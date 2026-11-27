mod bench;
mod lifecycle;
mod raw;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use usb4_rdma_core::{Service, config::Cluster};
use usb4_stream_hal::{
    LinkCapabilities, StreamTransport, TransportKind, configfs, stream::StreamLink,
};

#[derive(Parser)]
#[command(
    version,
    about = "Generic USB4 RDMA transport and routed fabric daemon"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum Backend {
    Usb4,
    Usb4BusyPoll,
    Emulated,
    EmulatedUring,
    Uring,
}

#[derive(Subcommand)]
enum Command {
    #[command(subcommand)]
    Bench(bench::Command),
    Doctor {
        #[arg(long, default_value = "/sys/bus/thunderbolt/devices")]
        sysfs: PathBuf,
        /// Require at least one link with two 20 Gbit/s lanes in each direction.
        #[arg(long)]
        require_40g: bool,
        #[arg(long, default_value = "/sys/kernel/config/thunderbolt/stream")]
        configfs: PathBuf,
    },
    Init {
        #[arg(long)]
        service: String,
        #[arg(long, default_value = "u4rd")]
        name: String,
        #[arg(long, default_value_t = -1, allow_hyphen_values = true)]
        in_hopid: i32,
        #[arg(long, default_value_t = -1, allow_hyphen_values = true)]
        out_hopid: i32,
        #[arg(long, default_value_t = 256)]
        ring_size: u32,
        /// Linux io_uring requires zero moderation.
        #[arg(long, default_value_t = 0)]
        throttling_ns: u32,
        /// Enable kernel ring polling on this endpoint.
        #[arg(long)]
        busy_poll: bool,
        #[arg(long)]
        apply: bool,
        #[arg(long, default_value = "/sys/kernel/config/thunderbolt/stream")]
        configfs: PathBuf,
    },
    Serve {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        node: u32,
        #[arg(long)]
        socket: PathBuf,
        #[arg(long, value_enum, default_value = "uring")]
        backend: Backend,
        #[arg(long)]
        emulated_dir: Option<PathBuf>,
        /// Maximum total bytes of daemon-mapped client remote MRs (default 2 GiB).
        #[arg(long, default_value_t = usb4_rdma_core::DEFAULT_MAX_MR_BYTES)]
        max_mr_bytes: usize,
        #[arg(long)]
        sqpoll: bool,
        /// Spin after progress in microseconds (USB4 default 200, busy-poll 500, otherwise 0).
        #[arg(long, value_parser = clap::value_parser!(u64).range(0..=10000))]
        spin_us: Option<u64>,
        /// Active probe interval in microseconds (USB4 default 100, busy-poll 50, otherwise 1000).
        #[arg(long, value_parser = clap::value_parser!(u64).range(50..=1000))]
        active_probe_us: Option<u64>,
        /// Run each link on a dedicated I/O and CRC32 worker (default true for USB4 and busy-poll).
        #[arg(long, action = clap::ArgAction::Set)]
        threaded: Option<bool>,
    },
    Monitor {
        #[arg(long)]
        socket: PathBuf,
        /// Include payload counters in local config link order.
        #[arg(long)]
        links: bool,
    },
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Bench(command) => return bench::run(command),
        Command::Doctor {
            sysfs,
            configfs,
            require_40g,
        } => {
            let report = configfs::doctor(&sysfs, &configfs)?;
            let two_lane_40g = report.links.iter().any(|link| {
                link.rx_lanes == Some(2)
                    && link.tx_lanes == Some(2)
                    && link.rx_gbps_per_lane == Some(20.0)
                    && link.tx_gbps_per_lane == Some(20.0)
            });
            let mut value = serde_json::to_value(report)?;
            value["two_lane_40g"] = two_lane_40g.into();
            value["thunderbolt_net"] = serde_json::json!({
                "module_loaded": Path::new("/sys/module/thunderbolt_net").is_dir(),
                "driver_registered": Path::new("/sys/bus/thunderbolt/drivers/thunderbolt-net").is_dir(),
            });
            println!("{}", serde_json::to_string_pretty(&value)?);
            anyhow::ensure!(
                !require_40g || two_lane_40g,
                "no bidirectional 20 Gbit/s × 2 link observed"
            );
        }
        Command::Init {
            service,
            name,
            in_hopid,
            out_hopid,
            ring_size,
            throttling_ns,
            busy_poll,
            apply,
            configfs,
        } => {
            let p = configfs::Provision {
                service,
                name,
                in_hopid,
                out_hopid,
                ring_size,
                throttling_ns,
            };
            p.validate()?;
            if busy_poll {
                let mut report = serde_json::to_value(&p)?;
                report["busy_poll"] = true.into();
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("{}", serde_json::to_string_pretty(&p)?);
            }
            if apply {
                println!(
                    "device={}",
                    p.apply_with_busy_poll(&configfs, busy_poll)?.display()
                );
            } else {
                println!("preview only; pass --apply to provision host USB4STREAM endpoints");
            }
        }
        Command::Serve {
            config,
            node,
            socket,
            backend,
            emulated_dir,
            max_mr_bytes,
            sqpoll,
            spin_us,
            active_probe_us,
            threaded,
        } => {
            let (default_spin_us, default_probe_us) = match backend {
                Backend::Usb4 => (200, 100),
                Backend::Usb4BusyPoll => (500, 50),
                _ => (0, 1000),
            };
            let spin_us = spin_us.unwrap_or(default_spin_us);
            let active_probe_us = active_probe_us.unwrap_or(default_probe_us);
            let threaded =
                threaded.unwrap_or(matches!(backend, Backend::Usb4 | Backend::Usb4BusyPoll));
            lifecycle::install()?;
            let c = Cluster::load(&config)?;
            let mut links = open_links(&c, node, backend, emulated_dir.as_deref(), sqpoll)?;
            if threaded {
                links = links
                    .into_iter()
                    .map(|l| {
                        Ok(
                            Box::new(usb4_stream_hal::threaded::ThreadedLink::spawn(l, spin_us)?)
                                as Box<dyn StreamTransport>,
                        )
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
            }
            let fabric = usb4_rdma_core::Fabric::new(&c, node, links)?
                .with_active_probe_interval(Duration::from_micros(active_probe_us));
            let server = usb4_rdma_core::ipc::Server::bind(
                &socket,
                usb4_rdma_core::ipc::Welcome {
                    node,
                    epoch: c.epoch,
                    digest: c.digest(),
                    client: 0,
                },
            )?;
            eprintln!(
                "usb4-rdma node {node} IPC={} backend={} epoch={} queue_depth={} spin_us={spin_us} active_probe_us={active_probe_us}",
                socket.display(),
                match backend {
                    Backend::Usb4 => "usb4",
                    Backend::Usb4BusyPoll => "usb4-busy-poll",
                    Backend::Emulated => "emulated (software only)",
                    Backend::Uring => "uring",
                    Backend::EmulatedUring => "emulated uring (software only)",
                },
                c.epoch,
                c.queue_depth
            );
            Service::with_max_mr_bytes(fabric, server, c.timeout_ms, max_mr_bytes)
                .run_with_link_failure_report(spin_us, lifecycle::stopping, |failure| {
                    // open_links supplies the backends in local configuration order.
                    let link = c
                        .links
                        .iter()
                        .filter(|link| link.a == node || link.b == node)
                        .nth(failure.port)
                        .map_or("<unknown>", |link| link.id.as_str());
                    eprintln!(
                        "usb4-rdma link failure node={node} epoch={} port={} peer={} link={link:?} cause={} local_stop_requested={}",
                        c.epoch,
                        failure.port,
                        failure.peer,
                        failure.cause,
                        lifecycle::stopping()
                    );
                })?;
        }
        Command::Monitor { socket, links } => {
            let mut c = usb4_rdma_core::ipc::Client::connect(&socket)?;
            let slot = c
                .post(
                    usb4_rdma_core::ipc::Descriptor {
                        op: usb4_rdma_core::ipc::OP_STATS,
                        flags: if links {
                            usb4_rdma_core::ipc::STATS_LINKS
                        } else {
                            0
                        },
                        ..Default::default()
                    },
                    &[],
                )?
                .ok_or_else(|| anyhow::anyhow!("monitor queue full"))?;
            let start = Instant::now();
            while !matches!(
                c.state(slot)?,
                usb4_rdma_core::ipc::COMPLETE | usb4_rdma_core::ipc::FAILED
            ) {
                anyhow::ensure!(
                    start.elapsed() < Duration::from_secs(2),
                    "monitor timed out"
                );
                c.alive()?;
                std::thread::sleep(Duration::from_millis(1));
            }
            let mut link_stats = Vec::new();
            if links && c.state(slot)? == usb4_rdma_core::ipc::COMPLETE {
                let len = c.response(slot)?.length as usize;
                anyhow::ensure!(
                    len > 0
                        && len <= usb4_rdma_core::ipc::WINDOW
                        && len.is_multiple_of(usb4_rdma_core::ipc::LINK_STATS_BYTES),
                    "daemon did not return per-link counters"
                );
                let mut bytes = vec![0; len];
                c.read_completed_stats(slot, &mut bytes)?;
                for link in bytes
                    .as_chunks::<{ usb4_rdma_core::ipc::LINK_STATS_BYTES }>()
                    .0
                {
                    let tx = u64::from_le_bytes(link[..8].try_into()?);
                    let rx = u64::from_le_bytes(link[8..].try_into()?);
                    link_stats.push(serde_json::json!({
                        "submitted_payload_bytes": tx,
                        "received_payload_bytes": rx,
                    }));
                }
            }
            let d = c.finish(slot)?;
            let mut report = serde_json::json!({
                "node": c.welcome.node,
                "epoch": c.welcome.epoch,
                "fabric_ready": d.flags == 1,
                "tx_frames": d.connection,
                "rx_frames": d.request,
                "forwarded_frames": d.listener,
                "submitted_payload_bytes": d.total,
                "received_payload_bytes": d.offset,
                "hardware_acceptance": false,
            });
            if links {
                report["links"] = link_stats.into();
                report["config_digest"] = c
                    .welcome
                    .digest
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
                    .into();
            }
            println!("{report}");
            anyhow::ensure!(d.status == 0 && d.flags == 1, "peer handshakes incomplete");
        }
    }
    Ok(())
}

fn open_links(
    c: &Cluster,
    node: u32,
    backend: Backend,
    dir: Option<&Path>,
    sqpoll: bool,
) -> anyhow::Result<Vec<Box<dyn StreamTransport>>> {
    c.node(node)?;
    anyhow::ensure!(
        !sqpoll || matches!(backend, Backend::Uring | Backend::EmulatedUring),
        "SQPOLL requires uring backend"
    );
    let mut out: Vec<Box<dyn StreamTransport>> = Vec::new();
    for l in &c.links {
        let (peer, endpoint, controller) = if l.a == node {
            (l.b, &l.endpoint_a, &l.controller_a)
        } else if l.b == node {
            (l.a, &l.endpoint_b, &l.controller_b)
        } else {
            continue;
        };
        use sha2::{Digest, Sha256};
        let group = u64::from_le_bytes(
            Sha256::digest(controller.as_bytes())[..8]
                .try_into()
                .unwrap(),
        );
        let emulated = matches!(backend, Backend::Emulated | Backend::EmulatedUring);
        let caps = LinkCapabilities {
            peer,
            speed_mbps: l.speed_mbps,
            controller_group: group,
            transport_kind: if emulated {
                TransportKind::Emulated
            } else {
                TransportKind::Usb4Stream
            },
            kernel_copies: !emulated,
            fixed_buffers: matches!(backend, Backend::Uring | Backend::EmulatedUring),
            sqpoll,
            zero_copy_dma: false,
        };
        if emulated {
            let dir = dir.ok_or_else(|| {
                anyhow::anyhow!("--emulated-dir is required for software stream testing")
            })?;
            std::fs::create_dir_all(dir)?;
            anyhow::ensure!(
                !l.id.contains('/') && !l.id.contains(".."),
                "invalid emulated link name"
            );
            let path = dir.join(format!("link-{}.sock", l.id));
            let stream =
                usb4_sim::unix::open(&path, l.a == node, Duration::from_millis(c.timeout_ms))?;
            if matches!(backend, Backend::EmulatedUring) {
                #[cfg(feature = "uring")]
                {
                    let fd = std::os::fd::OwnedFd::from(stream);
                    out.push(Box::new(
                        usb4_stream_hal::uring::UringLink::from_file(
                            std::fs::File::from(fd),
                            caps,
                            sqpoll,
                        )?
                        .with_receive_capacity(c.queue_depth * 2 + 8),
                    ));
                }
                #[cfg(not(feature = "uring"))]
                {
                    anyhow::bail!("emulated-uring requires the uring feature");
                }
            } else {
                let fd = std::os::fd::OwnedFd::from(stream);
                let file = usb4_stream_hal::stream::ReadyFile::from_file(std::fs::File::from(fd))?;
                let raw = file.as_raw_fd();
                let io: Box<dyn usb4_stream_hal::stream::StreamIo> = Box::new(file);
                out.push(Box::new(
                    StreamLink::new(io, caps)
                        .with_raw_fd(raw)
                        .with_tx_capacity(64)
                        .with_receive_capacity(c.queue_depth * 2 + 8),
                ));
            }
        } else {
            let device = native_device(Path::new(endpoint), Path::new("/dev"), backend)?;
            if matches!(backend, Backend::Uring) {
                #[cfg(feature = "uring")]
                {
                    let link = usb4_stream_hal::uring::UringLink::open(&device, caps, sqpoll)
                        .with_context(|| {
                            format!("open USB4STREAM device {} with io_uring", device.display())
                        })?;
                    out.push(Box::new(link.with_receive_capacity(c.queue_depth * 2 + 8)));
                }
                #[cfg(not(feature = "uring"))]
                {
                    anyhow::bail!("uring backend requires the uring feature");
                }
            } else {
                let link = if matches!(backend, Backend::Usb4BusyPoll) {
                    let file = usb4_stream_hal::stream::open_device(&device)
                        .with_context(|| format!("open USB4STREAM device {}", device.display()))?;
                    StreamLink::new(Box::new(file), caps)
                } else {
                    let file = usb4_stream_hal::stream::ReadyFile::open(&device)
                        .with_context(|| format!("open USB4STREAM device {}", device.display()))?;
                    let fd = file.as_raw_fd();
                    StreamLink::new(Box::new(file), caps).with_raw_fd(fd)
                };
                out.push(Box::new(
                    link.with_tx_capacity(64)
                        .with_receive_capacity(c.queue_depth * 2 + 8),
                ));
            }
        }
    }
    Ok(out)
}

fn native_device(endpoint: &Path, dev: &Path, backend: Backend) -> anyhow::Result<PathBuf> {
    let metadata = endpoint
        .metadata()
        .with_context(|| format!("inspect USB4STREAM endpoint {}", endpoint.display()))?;
    anyhow::ensure!(
        metadata.is_dir(),
        "native USB4 backends require a ConfigFS endpoint directory for profile readback: {}",
        endpoint.display()
    );
    if matches!(backend, Backend::Uring) {
        let path = endpoint.join("throttling");
        let throttling = std::fs::read_to_string(&path)
            .with_context(|| format!("read USB4STREAM moderation from {}", path.display()))?;
        anyhow::ensure!(
            throttling.trim() == "0",
            "io_uring backend requires zero ConfigFS moderation at {}",
            path.display()
        );
    }
    configfs::map_device_with_busy_poll(endpoint, dev, matches!(backend, Backend::Usb4BusyPoll))
        .with_context(|| format!("resolve the USB4STREAM device for {}", endpoint.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn serve_accepts_explicit_readiness_backend_and_keeps_uring_default() {
        let args = [
            "usb4-rdma-ctl",
            "serve",
            "--config",
            "fabric.toml",
            "--node",
            "0",
            "--socket",
            "node.sock",
        ];
        assert!(matches!(
            Cli::try_parse_from(args).unwrap().command,
            Command::Serve {
                backend: Backend::Uring,
                ..
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(args.into_iter().chain(["--backend", "usb4"]))
                .unwrap()
                .command,
            Command::Serve {
                backend: Backend::Usb4,
                ..
            }
        ));
    }

    #[test]
    fn native_backends_require_matching_configfs_profiles() {
        struct Fixture(PathBuf);
        impl Drop for Fixture {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }

        let endpoint = std::env::temp_dir().join(format!(
            "usb4-ctl-profile-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&endpoint).unwrap();
        let fixture = Fixture(endpoint);
        let endpoint = &fixture.0;
        let dev = endpoint.join("dev");
        let device = dev.join("tbstream7");
        let map = |backend| native_device(endpoint, &dev, backend);
        let busy_poll = endpoint.join("busy_poll");
        let throttling = endpoint.join("throttling");
        fs::write(endpoint.join("index"), "7\n").unwrap();
        fs::write(&busy_poll, "0\n").unwrap();
        fs::write(&throttling, "8192\n").unwrap();

        assert_eq!(map(Backend::Usb4).unwrap(), device);
        assert!(map(Backend::Usb4BusyPoll).is_err());
        assert!(map(Backend::Uring).is_err());
        assert!(native_device(&device, &dev, Backend::Usb4).is_err());

        fs::write(&busy_poll, "1\n").unwrap();
        fs::write(&throttling, "0\n").unwrap();
        assert!(map(Backend::Usb4).is_err());
        assert_eq!(map(Backend::Usb4BusyPoll).unwrap(), device);
        assert!(map(Backend::Uring).is_err());

        fs::remove_file(busy_poll).unwrap();
        assert_eq!(map(Backend::Usb4).unwrap(), device);
        assert!(map(Backend::Usb4BusyPoll).is_err());
        assert_eq!(map(Backend::Uring).unwrap(), device);
    }
}
