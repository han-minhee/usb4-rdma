mod proto;
mod stats;

use anyhow::{Context as _, Result, ensure};
use clap::{Parser, Subcommand, ValueEnum};
use proto::{ControlReq, ControlResp, recv_json, send_json};
use stats::{
    AllReduceResult, BandwidthResult, BenchReport, LatencyResult, print_report, summarize_latency,
};
use std::{
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    time::{Duration, Instant},
};
use usb4_rdma::{
    AccessFlags, CompletionQueue, Context, EndpointHandle, OwnedMr, QueuePair, WcOpcode, WcStatus,
};

#[derive(Parser)]
#[command(version, about = "USB4 RDMA vs Ethernet TCP cluster benchmark")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum Suite {
    All,
    Latency,
    Bandwidth,
    Allreduce,
}

#[derive(Clone, Copy, ValueEnum)]
enum TransportSelect {
    Both,
    Rdma,
    Tcp,
}

#[derive(Subcommand)]
enum Command {
    /// Run benchmark responder on the peer node.
    Server {
        /// Control TCP bind address (e.g. 0.0.0.0:19400 or 198.51.100.2:19400).
        #[arg(long, default_value = "0.0.0.0:19400")]
        bind: String,
        /// Optional local `usb4-rdma-daemon` Unix socket path.
        #[arg(long)]
        rdma_socket: Option<PathBuf>,
        /// Size of the pre-allocated RDMA memory region in bytes (default 64 MiB).
        #[arg(long, default_value_t = 64 * 1024 * 1024)]
        mr_bytes: usize,
        /// Handle one client session and exit cleanly.
        #[arg(long)]
        once: bool,
    },
    /// Run benchmark initiator and compare USB4 RDMA vs Ethernet TCP.
    Client {
        /// Server control TCP address (e.g. 198.51.100.2:19400).
        #[arg(long)]
        peer: String,
        /// Optional local `usb4-rdma-daemon` Unix socket path.
        #[arg(long)]
        rdma_socket: Option<PathBuf>,
        /// Which benchmark suites to execute.
        #[arg(long, value_enum, default_value = "all")]
        suite: Suite,
        /// Which transports to benchmark.
        #[arg(long, value_enum, default_value = "both")]
        only: TransportSelect,
        /// Comma-separated latency message sizes in bytes.
        #[arg(long, default_value = "64,4096,65536,262144,1048576")]
        lat_sizes: String,
        /// Comma-separated bandwidth message sizes in bytes.
        #[arg(long, default_value = "65536,1048576,4194304,16777216")]
        bw_sizes: String,
        /// Comma-separated f32 element counts for 2-node AllReduce.
        #[arg(long, default_value = "16384,262144,1048576")]
        allreduce_elems: String,
        /// Latency measured iterations per size.
        #[arg(long, default_value_t = 100)]
        lat_iters: usize,
        /// Latency warmup iterations per size.
        #[arg(long, default_value_t = 10)]
        warmup: usize,
        /// Target total bytes transferred per bandwidth test point (default 128 MiB).
        #[arg(long, default_value_t = 128 * 1024 * 1024)]
        bw_total_bytes: u64,
        /// Size of the local RDMA memory region in bytes (default 64 MiB).
        #[arg(long, default_value_t = 64 * 1024 * 1024)]
        mr_bytes: usize,
        /// Optional path to write structured JSON results.
        #[arg(long)]
        json: Option<PathBuf>,
    },
}

fn parse_sizes(s: &str) -> Result<Vec<usize>> {
    s.split(',')
        .map(|field| {
            let size = field
                .trim()
                .parse::<usize>()
                .context("expected a positive size")?;
            ensure!(size > 0, "benchmark sizes must be positive");
            Ok(size)
        })
        .collect()
}

fn fill_pattern(buf: &mut [u8], seed: u8) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b = seed.wrapping_add((i.wrapping_mul(31) & 0xFF) as u8);
    }
}

fn setup_tcp_stream(s: &TcpStream) -> Result<()> {
    s.set_nodelay(true)?;
    s.set_read_timeout(Some(Duration::from_secs(30)))?;
    s.set_write_timeout(Some(Duration::from_secs(30)))?;
    Ok(())
}

struct RdmaPeer {
    ctx: Context,
    cq: CompletionQueue,
    qp: QueuePair,
    mr: OwnedMr,
    dummy_mr: OwnedMr,
    remote_rkey: u64,
}

fn rdma_stats(rdma: Option<&RdmaPeer>) -> Result<(u64, u64)> {
    if let Some(r) = rdma {
        let s = r.ctx.stats()?;
        Ok((s.tx_payload_bytes, s.rx_payload_bytes))
    } else {
        Ok((0, 0))
    }
}

fn poll_exact(cq: &CompletionQueue, expected_wr_id: u64) -> Result<usb4_rdma::WorkCompletion> {
    let wc = cq.poll_blocking(Duration::from_secs(30))?;
    ensure!(
        wc.status == WcStatus::Success,
        "RDMA work completion failed: wr_id={} status={:?} opcode={:?}",
        wc.wr_id,
        wc.status,
        wc.opcode
    );
    ensure!(
        wc.wr_id == expected_wr_id,
        "unexpected wr_id: got {} expected {}",
        wc.wr_id,
        expected_wr_id
    );
    Ok(wc)
}

fn run_server(
    bind: &str,
    rdma_socket: Option<&PathBuf>,
    mr_bytes: usize,
    once: bool,
) -> Result<()> {
    let listener = TcpListener::bind(bind).with_context(|| format!("bind control {bind}"))?;
    let local_ip = listener.local_addr()?.ip();
    eprintln!(
        "usb4-rdma-bench server listening on {}",
        listener.local_addr()?
    );

    loop {
        let (ctrl, peer_addr) = listener.accept()?;
        setup_tcp_stream(&ctrl)?;
        eprintln!("client connected from {peer_addr}");
        if let Err(e) = handle_client(ctrl, local_ip, rdma_socket, mr_bytes) {
            eprintln!("client session ended with error: {e:#}");
            if once {
                return Err(e);
            }
        }
        if once {
            break;
        }
    }
    Ok(())
}

fn handle_client(
    ctrl: TcpStream,
    local_ip: std::net::IpAddr,
    rdma_socket: Option<&PathBuf>,
    mr_bytes: usize,
) -> Result<()> {
    let mut writer = ctrl.try_clone()?;
    let mut reader = BufReader::new(ctrl);

    let data_listener = TcpListener::bind((local_ip, 0))?;
    let data_port = data_listener.local_addr()?.port();

    let hello: ControlReq = recv_json(&mut reader)?;
    let (want_rdma, client_rkey) = match hello {
        ControlReq::Hello {
            want_rdma,
            client_rkey,
            ..
        } => (want_rdma, client_rkey.unwrap_or(0)),
        other => anyhow::bail!("expected Hello, got {other:?}"),
    };

    let mut rdma_state: Option<RdmaPeer> = None;
    let mut scratch = vec![0u8; 16 * 1024 * 1024];

    if want_rdma && let Some(sock) = rdma_socket {
        let ctx = Context::open(sock).with_context(|| format!("open RDMA {}", sock.display()))?;
        let pd = ctx.alloc_pd();
        let cq = ctx.create_cq()?;
        let all = AccessFlags::LOCAL_READ
            | AccessFlags::LOCAL_WRITE
            | AccessFlags::REMOTE_READ
            | AccessFlags::REMOTE_WRITE;
        let mr = pd.alloc_mr(mr_bytes, all)?;
        let dummy_mr = pd.alloc_mr(64, all)?;
        let r_listener = pd.listen()?;
        let handle = r_listener.handle().to_bytes();
        let server_rkey = mr.rkey();

        send_json(
            &mut writer,
            &ControlResp::HelloAck {
                rdma_available: true,
                server_handle: Some(handle),
                server_rkey: Some(server_rkey),
                server_mr_len: Some(mr_bytes),
                data_port,
            },
        )?;

        let qp = r_listener.accept(&cq, &cq, Duration::from_secs(15))?;
        rdma_state = Some(RdmaPeer {
            ctx,
            cq,
            qp,
            mr,
            dummy_mr,
            remote_rkey: client_rkey,
        });
    } else {
        send_json(
            &mut writer,
            &ControlResp::HelloAck {
                rdma_available: false,
                server_handle: None,
                server_rkey: None,
                server_mr_len: None,
                data_port,
            },
        )?;
    }

    loop {
        let req: ControlReq = recv_json(&mut reader)?;
        match req {
            ControlReq::Quit => {
                send_json(&mut writer, &ControlResp::Done { crc32: 0, bytes: 0 })?;
                break;
            }
            ControlReq::TcpPingPong {
                size,
                warmup,
                iters,
            } => {
                if scratch.len() < size {
                    scratch.resize(size, 0);
                }
                send_json(&mut writer, &ControlResp::Ready)?;
                let (mut data, _) = data_listener.accept()?;
                setup_tcp_stream(&data)?;
                let buf = &mut scratch[..size];
                for _ in 0..(warmup + iters) {
                    data.read_exact(buf)?;
                    data.write_all(buf)?;
                }
                let crc = crc32fast::hash(buf);
                send_json(
                    &mut writer,
                    &ControlResp::Done {
                        crc32: crc,
                        bytes: (size as u64) * ((warmup + iters) as u64),
                    },
                )?;
            }
            ControlReq::TcpUniRecv { size, iters } => {
                if scratch.len() < size {
                    scratch.resize(size, 0);
                }
                send_json(&mut writer, &ControlResp::Ready)?;
                let (mut data, _) = data_listener.accept()?;
                setup_tcp_stream(&data)?;
                let buf = &mut scratch[..size];
                let mut last_crc = 0u32;
                for _ in 0..iters {
                    data.read_exact(buf)?;
                    last_crc = crc32fast::hash(buf);
                }
                data.write_all(&[1u8])?;
                send_json(
                    &mut writer,
                    &ControlResp::Done {
                        crc32: last_crc,
                        bytes: (size as u64) * (iters as u64),
                    },
                )?;
            }
            ControlReq::TcpDuplex { size, iters } => {
                if scratch.len() < size {
                    scratch.resize(size, 0);
                }
                fill_pattern(&mut scratch[..size], 0x5A);
                send_json(&mut writer, &ControlResp::Ready)?;
                let (mut rx, _) = data_listener.accept()?;
                setup_tcp_stream(&rx)?;
                let mut tx = rx.try_clone()?;
                let send_buf = scratch[..size].to_vec();
                let mut recv_buf = vec![0u8; size];
                let (crc, total) = std::thread::scope(|s| -> Result<(u32, u64)> {
                    let sender = s.spawn(move || -> Result<()> {
                        for _ in 0..iters {
                            tx.write_all(&send_buf)?;
                        }
                        Ok(())
                    });
                    for _ in 0..iters {
                        rx.read_exact(&mut recv_buf)?;
                    }
                    sender.join().expect("tcp duplex sender thread")?;
                    Ok((crc32fast::hash(&recv_buf), (size as u64) * (iters as u64)))
                })?;
                send_json(
                    &mut writer,
                    &ControlResp::Done {
                        crc32: crc,
                        bytes: total,
                    },
                )?;
            }
            ControlReq::RdmaPreparePattern { size, seed } => {
                let r = rdma_state
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("RDMA not initialized"))?;
                ensure!(size <= r.mr.len(), "size exceeds server MR");
                let mut tmp = vec![0u8; size];
                fill_pattern(&mut tmp, seed);
                r.mr.write_bytes(0, &tmp)?;
                let crc = crc32fast::hash(&tmp);
                send_json(
                    &mut writer,
                    &ControlResp::Done {
                        crc32: crc,
                        bytes: size as u64,
                    },
                )?;
            }
            ControlReq::RdmaVerifyCrc { offset, size } => {
                let r = rdma_state
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("RDMA not initialized"))?;
                let mut tmp = vec![0u8; size];
                r.mr.read_bytes(offset, &mut tmp)?;
                let crc = crc32fast::hash(&tmp);
                send_json(
                    &mut writer,
                    &ControlResp::Done {
                        crc32: crc,
                        bytes: size as u64,
                    },
                )?;
            }
            ControlReq::RdmaSendPingPong {
                size,
                warmup,
                iters,
            } => {
                let r = rdma_state
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("RDMA not initialized"))?;
                ensure!(size <= r.mr.len(), "size exceeds server MR");
                send_json(&mut writer, &ControlResp::Ready)?;
                for i in 0..(warmup + iters) {
                    let wr = (i as u64 + 1) * 2;
                    r.qp.post_recv(wr, &r.mr, 0..size, 1)?;
                    poll_exact(&r.cq, wr)?;
                    r.qp.post_send(wr + 1, &r.mr, 0..size, 2)?;
                    poll_exact(&r.cq, wr + 1)?;
                }
                let mut tmp = vec![0u8; size];
                r.mr.read_bytes(0, &mut tmp)?;
                send_json(
                    &mut writer,
                    &ControlResp::Done {
                        crc32: crc32fast::hash(&tmp),
                        bytes: (size as u64) * ((warmup + iters) as u64),
                    },
                )?;
            }
            ControlReq::RdmaDuplexWrite {
                size,
                iters,
                peer_rkey,
            } => {
                let r = rdma_state
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("RDMA not initialized"))?;
                r.remote_rkey = peer_rkey;
                ensure!(size * 2 <= r.mr.len(), "MR must hold send + recv halves");
                let mut src = vec![0u8; size];
                fill_pattern(&mut src, 0x3C);
                r.mr.write_bytes(0, &src)?;
                send_json(&mut writer, &ControlResp::Ready)?;
                // Wait for the initiator's starting counters before sending any payload.
                ensure!(
                    matches!(recv_json(&mut reader)?, ControlReq::RdmaDuplexStart),
                    "expected RDMA duplex start"
                );
                for i in 0..iters {
                    let wr = 10_000 + i as u64;
                    r.qp.post_rdma_write(wr, &r.mr, 0..size, r.remote_rkey, size as u64)?;
                    poll_exact(&r.cq, wr)?;
                }
                ensure!(
                    matches!(recv_json(&mut reader)?, ControlReq::RdmaDuplexFinish),
                    "expected RDMA duplex finish"
                );
                let mut dst = vec![0u8; size];
                r.mr.read_bytes(size, &mut dst)?;
                send_json(
                    &mut writer,
                    &ControlResp::Done {
                        crc32: crc32fast::hash(&dst),
                        bytes: (size as u64) * (iters as u64),
                    },
                )?;
            }
            ControlReq::AllReduceTcp {
                elems,
                warmup,
                iters,
            } => {
                let bytes = elems * 4;
                let local_vals = vec![2.0f32; elems];
                let send_bytes: Vec<u8> = local_vals.iter().flat_map(|v| v.to_le_bytes()).collect();
                let mut recv_bytes = vec![0u8; bytes];
                let mut out_vals = vec![0.0f32; elems];
                send_json(&mut writer, &ControlResp::Ready)?;
                let (mut data, _) = data_listener.accept()?;
                setup_tcp_stream(&data)?;
                for _ in 0..(warmup + iters) {
                    let mut tx = data.try_clone()?;
                    std::thread::scope(|s| -> Result<()> {
                        let t = s.spawn(|| -> Result<()> {
                            tx.write_all(&send_bytes)?;
                            Ok(())
                        });
                        data.read_exact(&mut recv_bytes)?;
                        t.join().expect("allreduce tcp tx")?;
                        Ok(())
                    })?;
                    for (i, chunk) in recv_bytes.as_chunks::<4>().0.iter().enumerate() {
                        out_vals[i] = local_vals[i] + f32::from_le_bytes(*chunk);
                    }
                }
                ensure!(
                    out_vals.iter().all(|&v| (v - 3.0).abs() < 1e-6),
                    "TCP AllReduce numerical mismatch"
                );
                send_json(
                    &mut writer,
                    &ControlResp::Done {
                        crc32: 0,
                        bytes: bytes as u64,
                    },
                )?;
            }
            ControlReq::AllReduceRdma {
                elems,
                warmup,
                iters,
                peer_rkey,
            } => {
                let r = rdma_state
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("RDMA not initialized"))?;
                r.remote_rkey = peer_rkey;
                let bytes = elems * 4;
                ensure!(bytes * 2 <= r.mr.len(), "MR must hold send + recv halves");
                let local_vals = vec![2.0f32; elems];
                let send_bytes: Vec<u8> = local_vals.iter().flat_map(|v| v.to_le_bytes()).collect();
                r.mr.write_bytes(0, &send_bytes)?;
                let mut recv_bytes = vec![0u8; bytes];
                let mut out_vals = vec![0.0f32; elems];
                send_json(&mut writer, &ControlResp::Ready)?;
                for i in 0..(warmup + iters) {
                    let base_wr = 50_000 + (i as u64) * 2;
                    r.qp.post_recv(base_wr, &r.dummy_mr, 0..16, 0)?;
                    r.qp.post_rdma_write_imm(
                        base_wr + 1,
                        &r.mr,
                        0..bytes,
                        r.remote_rkey,
                        bytes as u64,
                        i as u32,
                    )?;
                    let mut got_send = false;
                    let mut got_recv = false;
                    while !(got_send && got_recv) {
                        let wc = r.cq.poll_blocking(Duration::from_secs(30))?;
                        ensure!(
                            wc.status == WcStatus::Success,
                            "RDMA AllReduce error: {wc:?}"
                        );
                        match wc.opcode {
                            WcOpcode::RdmaWrite => got_send = true,
                            WcOpcode::RecvRdmaWithImm => got_recv = true,
                            other => anyhow::bail!("unexpected opcode {other:?}"),
                        }
                    }
                    r.mr.read_bytes(bytes, &mut recv_bytes)?;
                    for (j, chunk) in recv_bytes.as_chunks::<4>().0.iter().enumerate() {
                        out_vals[j] = local_vals[j] + f32::from_le_bytes(*chunk);
                    }
                }
                ensure!(
                    out_vals.iter().all(|&v| (v - 3.0).abs() < 1e-6),
                    "RDMA AllReduce numerical mismatch"
                );
                send_json(
                    &mut writer,
                    &ControlResp::Done {
                        crc32: 0,
                        bytes: bytes as u64,
                    },
                )?;
            }
            ControlReq::Hello { .. } => {
                send_json(
                    &mut writer,
                    &ControlResp::Error {
                        message: "duplicate Hello".into(),
                    },
                )?;
            }
            ControlReq::RdmaDuplexStart | ControlReq::RdmaDuplexFinish => {
                anyhow::bail!("unexpected RDMA duplex barrier");
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_client(
    peer: &str,
    rdma_socket: Option<&PathBuf>,
    suite: Suite,
    only: TransportSelect,
    lat_sizes: &[usize],
    bw_sizes: &[usize],
    allreduce_elems: &[usize],
    lat_iters: usize,
    warmup: usize,
    bw_total_bytes: u64,
    mr_bytes: usize,
    json_out: Option<&PathBuf>,
) -> Result<()> {
    let want_rdma = !matches!(only, TransportSelect::Tcp) && rdma_socket.is_some();
    let want_tcp = !matches!(only, TransportSelect::Rdma);

    if matches!(only, TransportSelect::Rdma) && rdma_socket.is_none() {
        anyhow::bail!("--rdma-socket is required when --only rdma");
    }

    let ctrl = TcpStream::connect(peer).with_context(|| format!("connect control {peer}"))?;
    setup_tcp_stream(&ctrl)?;
    let peer_ip = ctrl.peer_addr()?.ip();
    let mut writer = ctrl.try_clone()?;
    let mut reader = BufReader::new(ctrl);

    let all_access = AccessFlags::LOCAL_READ
        | AccessFlags::LOCAL_WRITE
        | AccessFlags::REMOTE_READ
        | AccessFlags::REMOTE_WRITE;

    let mut local_ctx_mr: Option<(
        Context,
        CompletionQueue,
        usb4_rdma::ProtectionDomain,
        OwnedMr,
        OwnedMr,
    )> = None;
    let client_rkey = if want_rdma {
        let sock = rdma_socket.unwrap();
        let ctx = Context::open(sock).with_context(|| format!("open RDMA {}", sock.display()))?;
        let pd = ctx.alloc_pd();
        let cq = ctx.create_cq()?;
        let mr = pd.alloc_mr(mr_bytes, all_access)?;
        let dummy_mr = pd.alloc_mr(64, all_access)?;
        let rkey = mr.rkey();
        local_ctx_mr = Some((ctx, cq, pd, mr, dummy_mr));
        Some(rkey)
    } else {
        None
    };

    send_json(
        &mut writer,
        &ControlReq::Hello {
            want_rdma,
            client_handle: None,
            client_rkey,
            client_mr_len: client_rkey.map(|_| mr_bytes),
        },
    )?;

    let (rdma_avail, server_handle, server_rkey, data_port) = match recv_json(&mut reader)? {
        ControlResp::HelloAck {
            rdma_available,
            server_handle,
            server_rkey,
            data_port,
            ..
        } => (rdma_available, server_handle, server_rkey, data_port),
        other => anyhow::bail!("expected HelloAck, got {other:?}"),
    };

    let rdma: Option<RdmaPeer> = if want_rdma {
        ensure!(rdma_avail, "server does not have RDMA enabled");
        let (ctx, cq, pd, mr, dummy_mr) = local_ctx_mr.take().unwrap();
        let handle = EndpointHandle::from_bytes(&server_handle.expect("server handle"))?;
        let qp = pd.connect(&handle, &cq, &cq)?;
        Some(RdmaPeer {
            ctx,
            cq,
            qp,
            mr,
            dummy_mr,
            remote_rkey: server_rkey.expect("server rkey"),
        })
    } else {
        None
    };

    let data_addr = format!("{peer_ip}:{data_port}");
    let mut report = BenchReport {
        peer: peer.to_string(),
        rdma_socket: rdma_socket.map(|p| p.display().to_string()),
        latency: Vec::new(),
        bandwidth: Vec::new(),
        allreduce: Vec::new(),
    };

    // 1. Latency Suite
    if matches!(suite, Suite::All | Suite::Latency) {
        for &size in lat_sizes {
            if want_tcp {
                send_json(
                    &mut writer,
                    &ControlReq::TcpPingPong {
                        size,
                        warmup,
                        iters: lat_iters,
                    },
                )?;
                ensure!(matches!(recv_json(&mut reader)?, ControlResp::Ready));
                let mut data = TcpStream::connect(&data_addr)?;
                setup_tcp_stream(&data)?;
                let mut tx = vec![0u8; size];
                fill_pattern(&mut tx, 0x11);
                let mut rx = vec![0u8; size];
                let mut samples = Vec::with_capacity(lat_iters);
                for i in 0..(warmup + lat_iters) {
                    let t0 = Instant::now();
                    data.write_all(&tx)?;
                    data.read_exact(&mut rx)?;
                    let half_us = t0.elapsed().as_secs_f64() * 1e6 / 2.0;
                    if i >= warmup {
                        samples.push(half_us);
                    }
                }
                ensure!(rx == tx, "TCP ping-pong payload mismatch");
                let _done: ControlResp = recv_json(&mut reader)?;
                let (min_us, p50_us, p95_us, p99_us, mean_us) = summarize_latency(&mut samples);
                report.latency.push(LatencyResult {
                    transport: "ethernet".into(),
                    op: "tcp_pingpong".into(),
                    size_bytes: size,
                    iters: lat_iters,
                    min_us,
                    p50_us,
                    p95_us,
                    p99_us,
                    mean_us,
                    rdma_tx_bytes_delta: 0,
                    rdma_rx_bytes_delta: 0,
                });
            }

            if let Some(r) = rdma.as_ref() {
                // One-sided RDMA_WRITE latency (full hardware completion RTT, zero server involvement)
                let mut pat = vec![0u8; size];
                fill_pattern(&mut pat, 0x22);
                r.mr.write_bytes(0, &pat)?;
                let expected_crc = crc32fast::hash(&pat);

                let (tx0, rx0) = rdma_stats(Some(r))?;
                let mut samples = Vec::with_capacity(lat_iters);
                for i in 0..(warmup + lat_iters) {
                    let wr = 1000 + i as u64;
                    let t0 = Instant::now();
                    r.qp.post_rdma_write(wr, &r.mr, 0..size, r.remote_rkey, 0)?;
                    poll_exact(&r.cq, wr)?;
                    let us = t0.elapsed().as_secs_f64() * 1e6;
                    if i >= warmup {
                        samples.push(us);
                    }
                }
                let (tx1, rx1) = rdma_stats(Some(r))?;
                send_json(&mut writer, &ControlReq::RdmaVerifyCrc { offset: 0, size })?;
                if let ControlResp::Done { crc32, .. } = recv_json(&mut reader)? {
                    ensure!(crc32 == expected_crc, "RDMA_WRITE CRC mismatch");
                }
                let (min_us, p50_us, p95_us, p99_us, mean_us) = summarize_latency(&mut samples);
                report.latency.push(LatencyResult {
                    transport: "usb4-rdma".into(),
                    op: "rdma_write".into(),
                    size_bytes: size,
                    iters: lat_iters,
                    min_us,
                    p50_us,
                    p95_us,
                    p99_us,
                    mean_us,
                    rdma_tx_bytes_delta: tx1 - tx0,
                    rdma_rx_bytes_delta: rx1 - rx0,
                });

                // One-sided RDMA_READ latency (zero server involvement)
                send_json(
                    &mut writer,
                    &ControlReq::RdmaPreparePattern { size, seed: 0x44 },
                )?;
                let expected_read_crc = match recv_json(&mut reader)? {
                    ControlResp::Done { crc32, .. } => crc32,
                    other => anyhow::bail!("expected Done, got {other:?}"),
                };
                let (tx0, rx0) = rdma_stats(Some(r))?;
                let mut samples = Vec::with_capacity(lat_iters);
                for i in 0..(warmup + lat_iters) {
                    let wr = 2000 + i as u64;
                    let t0 = Instant::now();
                    r.qp.post_rdma_read(wr, &r.mr, 0..size, r.remote_rkey, 0)?;
                    poll_exact(&r.cq, wr)?;
                    let us = t0.elapsed().as_secs_f64() * 1e6;
                    if i >= warmup {
                        samples.push(us);
                    }
                }
                let (tx1, rx1) = rdma_stats(Some(r))?;
                let mut read_buf = vec![0u8; size];
                r.mr.read_bytes(0, &mut read_buf)?;
                ensure!(
                    crc32fast::hash(&read_buf) == expected_read_crc,
                    "RDMA_READ CRC mismatch"
                );
                let (min_us, p50_us, p95_us, p99_us, mean_us) = summarize_latency(&mut samples);
                report.latency.push(LatencyResult {
                    transport: "usb4-rdma".into(),
                    op: "rdma_read".into(),
                    size_bytes: size,
                    iters: lat_iters,
                    min_us,
                    p50_us,
                    p95_us,
                    p99_us,
                    mean_us,
                    rdma_tx_bytes_delta: tx1 - tx0,
                    rdma_rx_bytes_delta: rx1 - rx0,
                });

                // Two-sided SEND/RECV ping-pong (half-RTT)
                send_json(
                    &mut writer,
                    &ControlReq::RdmaSendPingPong {
                        size,
                        warmup,
                        iters: lat_iters,
                    },
                )?;
                ensure!(matches!(recv_json(&mut reader)?, ControlResp::Ready));
                let (tx0, rx0) = rdma_stats(Some(r))?;
                let mut samples = Vec::with_capacity(lat_iters);
                for i in 0..(warmup + lat_iters) {
                    let wr = 3000 + (i as u64) * 2;
                    let t0 = Instant::now();
                    r.qp.post_send(wr, &r.mr, 0..size, 1)?;
                    poll_exact(&r.cq, wr)?;
                    r.qp.post_recv(wr + 1, &r.mr, 0..size, 2)?;
                    poll_exact(&r.cq, wr + 1)?;
                    let half_us = t0.elapsed().as_secs_f64() * 1e6 / 2.0;
                    if i >= warmup {
                        samples.push(half_us);
                    }
                }
                let (tx1, rx1) = rdma_stats(Some(r))?;
                let _done: ControlResp = recv_json(&mut reader)?;
                let (min_us, p50_us, p95_us, p99_us, mean_us) = summarize_latency(&mut samples);
                report.latency.push(LatencyResult {
                    transport: "usb4-rdma".into(),
                    op: "rdma_send_recv".into(),
                    size_bytes: size,
                    iters: lat_iters,
                    min_us,
                    p50_us,
                    p95_us,
                    p99_us,
                    mean_us,
                    rdma_tx_bytes_delta: tx1 - tx0,
                    rdma_rx_bytes_delta: rx1 - rx0,
                });
            }
        }
    }

    // 2. Bandwidth Suite
    if matches!(suite, Suite::All | Suite::Bandwidth) {
        for &size in bw_sizes {
            let iters = (bw_total_bytes / (size as u64)).clamp(4, 2048) as usize;
            let total_bytes = (size as u64) * (iters as u64);

            if want_tcp {
                // Unidirectional TCP
                send_json(&mut writer, &ControlReq::TcpUniRecv { size, iters })?;
                ensure!(matches!(recv_json(&mut reader)?, ControlResp::Ready));
                let mut data = TcpStream::connect(&data_addr)?;
                setup_tcp_stream(&data)?;
                let mut buf = vec![0u8; size];
                fill_pattern(&mut buf, 0x77);
                let expected_crc = crc32fast::hash(&buf);
                let t0 = Instant::now();
                for _ in 0..iters {
                    data.write_all(&buf)?;
                }
                let mut ack = [0u8; 1];
                data.read_exact(&mut ack)?;
                let elapsed = t0.elapsed().as_secs_f64();
                let verified_crc32 = match recv_json(&mut reader)? {
                    ControlResp::Done { crc32, .. } => crc32,
                    other => anyhow::bail!("expected Done, got {other:?}"),
                };
                ensure!(verified_crc32 == expected_crc, "TCP uni CRC mismatch");
                report.bandwidth.push(BandwidthResult {
                    transport: "ethernet".into(),
                    op: "tcp_uni".into(),
                    size_bytes: size,
                    iters,
                    total_bytes,
                    elapsed_secs: elapsed,
                    gbytes_per_sec: (total_bytes as f64) / elapsed / 1e9,
                    gib_per_sec: (total_bytes as f64) / elapsed / (1024.0 * 1024.0 * 1024.0),
                    gbits_per_sec: (total_bytes as f64) * 8.0 / elapsed / 1e9,
                    verified_crc32,
                    rdma_tx_bytes_delta: 0,
                    rdma_rx_bytes_delta: 0,
                });

                // Full-duplex TCP
                send_json(&mut writer, &ControlReq::TcpDuplex { size, iters })?;
                ensure!(matches!(recv_json(&mut reader)?, ControlResp::Ready));
                let mut rx = TcpStream::connect(&data_addr)?;
                setup_tcp_stream(&rx)?;
                let mut tx = rx.try_clone()?;
                let mut recv_buf = vec![0u8; size];
                let t0 = Instant::now();
                std::thread::scope(|s| -> Result<()> {
                    let sender = s.spawn(move || -> Result<()> {
                        for _ in 0..iters {
                            tx.write_all(&buf)?;
                        }
                        Ok(())
                    });
                    for _ in 0..iters {
                        rx.read_exact(&mut recv_buf)?;
                    }
                    sender.join().expect("tcp duplex sender")?;
                    Ok(())
                })?;
                let elapsed = t0.elapsed().as_secs_f64();
                let _done: ControlResp = recv_json(&mut reader)?;
                let duplex_bytes = total_bytes * 2;
                report.bandwidth.push(BandwidthResult {
                    transport: "ethernet".into(),
                    op: "tcp_duplex".into(),
                    size_bytes: size,
                    iters,
                    total_bytes: duplex_bytes,
                    elapsed_secs: elapsed,
                    gbytes_per_sec: (duplex_bytes as f64) / elapsed / 1e9,
                    gib_per_sec: (duplex_bytes as f64) / elapsed / (1024.0 * 1024.0 * 1024.0),
                    gbits_per_sec: (duplex_bytes as f64) * 8.0 / elapsed / 1e9,
                    verified_crc32: crc32fast::hash(&recv_buf),
                    rdma_tx_bytes_delta: 0,
                    rdma_rx_bytes_delta: 0,
                });
            }

            if let Some(r) = rdma.as_ref() {
                // Unidirectional one-sided RDMA_WRITE
                let mut pat = vec![0u8; size];
                fill_pattern(&mut pat, 0x88);
                r.mr.write_bytes(0, &pat)?;
                let expected_crc = crc32fast::hash(&pat);

                let (tx0, rx0) = rdma_stats(Some(r))?;
                let t0 = Instant::now();
                for i in 0..iters {
                    let wr = 4000 + i as u64;
                    r.qp.post_rdma_write(wr, &r.mr, 0..size, r.remote_rkey, 0)?;
                    poll_exact(&r.cq, wr)?;
                }
                let elapsed = t0.elapsed().as_secs_f64();
                let (tx1, rx1) = rdma_stats(Some(r))?;
                send_json(&mut writer, &ControlReq::RdmaVerifyCrc { offset: 0, size })?;
                let verified_crc32 = match recv_json(&mut reader)? {
                    ControlResp::Done { crc32, .. } => crc32,
                    other => anyhow::bail!("expected Done, got {other:?}"),
                };
                ensure!(verified_crc32 == expected_crc, "RDMA_WRITE bw CRC mismatch");
                ensure!(
                    tx1 - tx0 >= total_bytes,
                    "USB4 RDMA tx_payload_bytes did not account for transfer: delta={} expected>={}",
                    tx1 - tx0,
                    total_bytes
                );
                report.bandwidth.push(BandwidthResult {
                    transport: "usb4-rdma".into(),
                    op: "rdma_write_uni".into(),
                    size_bytes: size,
                    iters,
                    total_bytes,
                    elapsed_secs: elapsed,
                    gbytes_per_sec: (total_bytes as f64) / elapsed / 1e9,
                    gib_per_sec: (total_bytes as f64) / elapsed / (1024.0 * 1024.0 * 1024.0),
                    gbits_per_sec: (total_bytes as f64) * 8.0 / elapsed / 1e9,
                    verified_crc32,
                    rdma_tx_bytes_delta: tx1 - tx0,
                    rdma_rx_bytes_delta: rx1 - rx0,
                });

                // Unidirectional one-sided RDMA_READ
                let (tx0, rx0) = rdma_stats(Some(r))?;
                let t0 = Instant::now();
                for i in 0..iters {
                    let wr = 5000 + i as u64;
                    r.qp.post_rdma_read(wr, &r.mr, 0..size, r.remote_rkey, 0)?;
                    poll_exact(&r.cq, wr)?;
                }
                let elapsed = t0.elapsed().as_secs_f64();
                let (tx1, rx1) = rdma_stats(Some(r))?;
                let mut check = vec![0u8; size];
                r.mr.read_bytes(0, &mut check)?;
                let verified_crc32 = crc32fast::hash(&check);
                ensure!(verified_crc32 == expected_crc, "RDMA_READ bw CRC mismatch");
                ensure!(
                    rx1 - rx0 >= total_bytes,
                    "USB4 RDMA rx_payload_bytes did not account for transfer"
                );
                report.bandwidth.push(BandwidthResult {
                    transport: "usb4-rdma".into(),
                    op: "rdma_read_uni".into(),
                    size_bytes: size,
                    iters,
                    total_bytes,
                    elapsed_secs: elapsed,
                    gbytes_per_sec: (total_bytes as f64) / elapsed / 1e9,
                    gib_per_sec: (total_bytes as f64) / elapsed / (1024.0 * 1024.0 * 1024.0),
                    gbits_per_sec: (total_bytes as f64) * 8.0 / elapsed / 1e9,
                    verified_crc32,
                    rdma_tx_bytes_delta: tx1 - tx0,
                    rdma_rx_bytes_delta: rx1 - rx0,
                });

                // Simultaneous full-duplex RDMA_WRITE (both nodes write to peer's upper half)
                if size * 2 <= r.mr.len() {
                    send_json(
                        &mut writer,
                        &ControlReq::RdmaDuplexWrite {
                            size,
                            iters,
                            peer_rkey: r.mr.rkey(),
                        },
                    )?;
                    ensure!(matches!(recv_json(&mut reader)?, ControlResp::Ready));
                    let (tx0, rx0) = rdma_stats(Some(r))?;
                    let t0 = Instant::now();
                    send_json(&mut writer, &ControlReq::RdmaDuplexStart)?;
                    for i in 0..iters {
                        let wr = 6000 + i as u64;
                        r.qp.post_rdma_write(wr, &r.mr, 0..size, r.remote_rkey, size as u64)?;
                        poll_exact(&r.cq, wr)?;
                    }
                    send_json(&mut writer, &ControlReq::RdmaDuplexFinish)?;
                    let done_resp: ControlResp = recv_json(&mut reader)?;
                    let elapsed = t0.elapsed().as_secs_f64();
                    let (tx1, rx1) = rdma_stats(Some(r))?;
                    let (server_crc, server_bytes) = match done_resp {
                        ControlResp::Done { crc32, bytes } => (crc32, bytes),
                        other => anyhow::bail!("expected Done, got {other:?}"),
                    };
                    ensure!(
                        server_crc == expected_crc,
                        "RDMA duplex responder CRC mismatch"
                    );
                    fill_pattern(&mut check, 0x3C);
                    let expected_peer_crc = crc32fast::hash(&check);
                    r.mr.read_bytes(size, &mut check)?;
                    ensure!(
                        crc32fast::hash(&check) == expected_peer_crc,
                        "RDMA duplex initiator CRC mismatch"
                    );
                    ensure!(
                        server_bytes == total_bytes
                            && tx1 - tx0 == total_bytes
                            && rx1 - rx0 == total_bytes,
                        "RDMA duplex byte mismatch: responder={server_bytes} tx={} rx={} expected={total_bytes}",
                        tx1 - tx0,
                        rx1 - rx0,
                    );
                    let duplex_bytes = total_bytes * 2;
                    report.bandwidth.push(BandwidthResult {
                        transport: "usb4-rdma".into(),
                        op: "rdma_duplex".into(),
                        size_bytes: size,
                        iters,
                        total_bytes: duplex_bytes,
                        elapsed_secs: elapsed,
                        gbytes_per_sec: (duplex_bytes as f64) / elapsed / 1e9,
                        gib_per_sec: (duplex_bytes as f64) / elapsed / (1024.0 * 1024.0 * 1024.0),
                        gbits_per_sec: (duplex_bytes as f64) * 8.0 / elapsed / 1e9,
                        verified_crc32: server_crc,
                        rdma_tx_bytes_delta: tx1 - tx0,
                        rdma_rx_bytes_delta: rx1 - rx0,
                    });
                }
            }
        }
    }

    // 3. 2-Node Host f32 SUM AllReduce Suite
    if matches!(suite, Suite::All | Suite::Allreduce) {
        let ar_iters = 20usize;
        let ar_warmup = 4usize;
        for &elems in allreduce_elems {
            let bytes = elems * 4;
            let local_vals = vec![1.0f32; elems];
            let send_bytes: Vec<u8> = local_vals.iter().flat_map(|v| v.to_le_bytes()).collect();
            let mut recv_bytes = vec![0u8; bytes];
            let mut out_vals = vec![0.0f32; elems];

            if want_tcp {
                send_json(
                    &mut writer,
                    &ControlReq::AllReduceTcp {
                        elems,
                        warmup: ar_warmup,
                        iters: ar_iters,
                    },
                )?;
                ensure!(matches!(recv_json(&mut reader)?, ControlResp::Ready));
                let mut data = TcpStream::connect(&data_addr)?;
                setup_tcp_stream(&data)?;
                let mut samples = Vec::with_capacity(ar_iters);
                for i in 0..(ar_warmup + ar_iters) {
                    let t0 = Instant::now();
                    let mut tx = data.try_clone()?;
                    std::thread::scope(|s| -> Result<()> {
                        let t = s.spawn(|| -> Result<()> {
                            tx.write_all(&send_bytes)?;
                            Ok(())
                        });
                        data.read_exact(&mut recv_bytes)?;
                        t.join().expect("allreduce tcp client tx")?;
                        Ok(())
                    })?;
                    for (j, chunk) in recv_bytes.as_chunks::<4>().0.iter().enumerate() {
                        out_vals[j] = local_vals[j] + f32::from_le_bytes(*chunk);
                    }
                    let us = t0.elapsed().as_secs_f64() * 1e6;
                    if i >= ar_warmup {
                        samples.push(us);
                    }
                }
                ensure!(
                    out_vals.iter().all(|&v| (v - 3.0).abs() < 1e-6),
                    "TCP AllReduce client mismatch"
                );
                let _done: ControlResp = recv_json(&mut reader)?;
                let (_, p50_us, _, p99_us, mean_us) = summarize_latency(&mut samples);
                report.allreduce.push(AllReduceResult {
                    transport: "ethernet".into(),
                    elems,
                    bytes,
                    iters: ar_iters,
                    mean_us,
                    p50_us,
                    p99_us,
                    algbw_gbytes_per_sec: (bytes as f64) / (mean_us * 1e-6) / 1e9,
                    rdma_tx_bytes_delta: 0,
                    rdma_rx_bytes_delta: 0,
                });
            }

            if let Some(r) = rdma.as_ref() {
                r.mr.write_bytes(0, &send_bytes)?;
                send_json(
                    &mut writer,
                    &ControlReq::AllReduceRdma {
                        elems,
                        warmup: ar_warmup,
                        iters: ar_iters,
                        peer_rkey: r.mr.rkey(),
                    },
                )?;
                ensure!(matches!(recv_json(&mut reader)?, ControlResp::Ready));
                let (tx0, rx0) = rdma_stats(Some(r))?;
                let mut samples = Vec::with_capacity(ar_iters);
                for i in 0..(ar_warmup + ar_iters) {
                    let base_wr = 60_000 + (i as u64) * 2;
                    let t0 = Instant::now();
                    r.qp.post_recv(base_wr, &r.dummy_mr, 0..16, 0)?;
                    r.qp.post_rdma_write_imm(
                        base_wr + 1,
                        &r.mr,
                        0..bytes,
                        r.remote_rkey,
                        bytes as u64,
                        i as u32,
                    )?;
                    let mut got_send = false;
                    let mut got_recv = false;
                    while !(got_send && got_recv) {
                        let wc = r.cq.poll_blocking(Duration::from_secs(30))?;
                        ensure!(
                            wc.status == WcStatus::Success,
                            "RDMA AllReduce error: {wc:?}"
                        );
                        match wc.opcode {
                            WcOpcode::RdmaWrite => got_send = true,
                            WcOpcode::RecvRdmaWithImm => got_recv = true,
                            other => anyhow::bail!("unexpected opcode {other:?}"),
                        }
                    }
                    r.mr.read_bytes(bytes, &mut recv_bytes)?;
                    for (j, chunk) in recv_bytes.as_chunks::<4>().0.iter().enumerate() {
                        out_vals[j] = local_vals[j] + f32::from_le_bytes(*chunk);
                    }
                    let us = t0.elapsed().as_secs_f64() * 1e6;
                    if i >= ar_warmup {
                        samples.push(us);
                    }
                }
                let (tx1, rx1) = rdma_stats(Some(r))?;
                ensure!(
                    out_vals.iter().all(|&v| (v - 3.0).abs() < 1e-6),
                    "RDMA AllReduce client mismatch"
                );
                let _done: ControlResp = recv_json(&mut reader)?;
                let (_, p50_us, _, p99_us, mean_us) = summarize_latency(&mut samples);
                report.allreduce.push(AllReduceResult {
                    transport: "usb4-rdma".into(),
                    elems,
                    bytes,
                    iters: ar_iters,
                    mean_us,
                    p50_us,
                    p99_us,
                    algbw_gbytes_per_sec: (bytes as f64) / (mean_us * 1e-6) / 1e9,
                    rdma_tx_bytes_delta: tx1 - tx0,
                    rdma_rx_bytes_delta: rx1 - rx0,
                });
            }
        }
    }

    send_json(&mut writer, &ControlReq::Quit)?;
    let _done: ControlResp = recv_json(&mut reader)?;

    print_report(&report);
    if let Some(path) = json_out {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_string_pretty(&report)?)?;
        eprintln!("wrote JSON report to {}", path.display());
    }
    Ok(())
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Server {
            bind,
            rdma_socket,
            mr_bytes,
            once,
        } => run_server(&bind, rdma_socket.as_ref(), mr_bytes, once),
        Command::Client {
            peer,
            rdma_socket,
            suite,
            only,
            lat_sizes,
            bw_sizes,
            allreduce_elems,
            lat_iters,
            warmup,
            bw_total_bytes,
            mr_bytes,
            json,
        } => {
            ensure!(lat_iters > 0, "latency iteration count must be positive");
            ensure!(
                warmup.checked_add(lat_iters).is_some(),
                "iteration count overflow"
            );
            let lat_sizes = parse_sizes(&lat_sizes)?;
            let bw_sizes = parse_sizes(&bw_sizes)?;
            let allreduce_elems = parse_sizes(&allreduce_elems)?;
            run_client(
                &peer,
                rdma_socket.as_ref(),
                suite,
                only,
                &lat_sizes,
                &bw_sizes,
                &allreduce_elems,
                lat_iters,
                warmup,
                bw_total_bytes,
                mr_bytes,
                json.as_ref(),
            )
        }
    }
}
