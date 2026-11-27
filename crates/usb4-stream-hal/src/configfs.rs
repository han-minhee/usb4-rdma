use serde::Serialize;
use std::{
    fs,
    path::{Component, Path, PathBuf},
};
use usb4_proto::{Error, Result};

#[derive(Debug, Serialize)]
pub struct Service {
    pub name: String,
    pub protocol: String,
    pub protocol_id: Option<u32>,
    pub protocol_version: Option<u32>,
    pub supported: bool,
    pub negotiated_speed: Option<String>,
    pub peer_uuid: Option<String>,
    pub controller: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Doctor {
    pub linux: bool,
    pub configfs_available: bool,
    pub services: Vec<Service>,
    /// Physical links, including peers without an advertised stream service.
    pub links: Vec<PhysicalLink>,
    pub streams: Vec<StreamReport>,
    pub kernel_release: Option<String>,
    pub memlock_soft: u64,
    pub memlock_hard: u64,
    pub notes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct StreamReport {
    pub endpoint: String,
    pub device: Option<String>,
    pub readable: bool,
    pub writable: bool,
    pub busy_poll: Option<bool>,
}

/// Sysfs speeds are per lane. Never sum RX and TX as one-direction bandwidth.
#[derive(Debug, Serialize)]
pub struct PhysicalLink {
    pub name: String,
    pub rx_gbps_per_lane: Option<f64>,
    pub tx_gbps_per_lane: Option<f64>,
    pub rx_lanes: Option<u32>,
    pub tx_lanes: Option<u32>,
    pub rx_gbps: Option<f64>,
    pub tx_gbps: Option<f64>,
    /// Decimal GB/s before encoding, framing, and transport overhead.
    pub rx_raw_ceiling_gbytes_sec: Option<f64>,
    pub tx_raw_ceiling_gbytes_sec: Option<f64>,
}

fn speed(value: &str) -> Option<f64> {
    let value = value.trim().strip_suffix(" Gb/s")?.parse::<f64>().ok()?;
    (value.is_finite() && value > 0.0).then_some(value)
}

pub fn physical_links(sysfs: &Path) -> Result<Vec<PhysicalLink>> {
    let mut links = Vec::new();
    if !sysfs.exists() {
        return Ok(links);
    }
    for entry in fs::read_dir(sysfs)? {
        let p = entry?.path();
        if !p.join("rx_speed").exists() && !p.join("tx_speed").exists() {
            continue;
        }
        let read = |name: &str| fs::read_to_string(p.join(name)).ok();
        let lanes = |name| {
            read(name)
                .and_then(|s| s.trim().parse::<u32>().ok())
                .filter(|n| (1..=3).contains(n))
        };
        let rx_gbps_per_lane = read("rx_speed").as_deref().and_then(speed);
        let tx_gbps_per_lane = read("tx_speed").as_deref().and_then(speed);
        let rx_lanes = lanes("rx_lanes");
        let tx_lanes = lanes("tx_lanes");
        let rx_gbps = rx_gbps_per_lane.zip(rx_lanes).map(|(s, n)| s * n as f64);
        let tx_gbps = tx_gbps_per_lane.zip(tx_lanes).map(|(s, n)| s * n as f64);
        links.push(PhysicalLink {
            name: p.file_name().unwrap().to_string_lossy().into(),
            rx_gbps_per_lane,
            tx_gbps_per_lane,
            rx_lanes,
            tx_lanes,
            rx_gbps,
            tx_gbps,
            rx_raw_ceiling_gbytes_sec: rx_gbps.map(|v| v / 8.0),
            tx_raw_ceiling_gbytes_sec: tx_gbps.map(|v| v / 8.0),
        });
    }
    links.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(links)
}

pub fn discover(sysfs: &Path) -> Result<Vec<Service>> {
    let mut out = Vec::new();
    if !sysfs.exists() {
        return Ok(out);
    }
    for e in fs::read_dir(sysfs)? {
        let p = e?.path();
        let read = |name: &str| {
            fs::read_to_string(p.join(name))
                .ok()
                .map(|s| s.trim().to_string())
        };
        if read("key").as_deref() == Some("stream") {
            let parent = p.canonicalize()?.parent().map(Path::to_path_buf);
            let uuid = parent
                .as_ref()
                .and_then(|x| fs::read_to_string(x.join("unique_id")).ok())
                .map(|s| s.trim().into());
            out.push(Service {
                name: p.file_name().unwrap().to_string_lossy().into(),
                protocol: "stream".into(),
                protocol_id: read("prtcid").and_then(|s| s.parse().ok()),
                protocol_version: read("prtcvers").and_then(|s| s.parse().ok()),
                supported: read("prtcid").as_deref() == Some("1")
                    && read("prtcvers").as_deref() == Some("1"),
                negotiated_speed: parent
                    .as_ref()
                    .and_then(|p| fs::read_to_string(p.join("rx_speed")).ok())
                    .map(|s| s.trim().into()),
                peer_uuid: uuid,
                controller: p.canonicalize().ok().and_then(|p| {
                    p.ancestors()
                        .find(|a| {
                            a.file_name()
                                .is_some_and(|s| s.to_string_lossy().starts_with("domain"))
                        })
                        .and_then(Path::parent)
                        .map(|p| p.display().to_string())
                }),
            });
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

pub fn doctor(sysfs: &Path, configfs: &Path) -> Result<Doctor> {
    let services = discover(sysfs)?;
    let mut streams = Vec::new();
    if configfs.exists() {
        for group in fs::read_dir(configfs)? {
            let g = group?.path();
            if !g.is_dir() {
                continue;
            }
            for e in fs::read_dir(g)? {
                let p = e?.path();
                if p.join("index").is_file() {
                    let index = fs::read_to_string(p.join("index"))
                        .ok()
                        .and_then(|s| s.trim().parse::<u32>().ok());
                    let device = index.map(|i| format!("/dev/tbstream{i}"));
                    let access = |mode| {
                        device.as_ref().is_some_and(|d| {
                            let c =
                                std::ffi::CString::new(d.as_str()).expect("numeric device path");
                            // SAFETY: numeric path is NUL-terminated; access is read-only.
                            unsafe { libc::access(c.as_ptr(), mode) == 0 }
                        })
                    };
                    let readable = access(libc::R_OK);
                    let writable = access(libc::W_OK);
                    let busy_poll = fs::read_to_string(p.join("busy_poll"))
                        .ok()
                        .map(|s| s.trim() != "0");
                    streams.push(StreamReport {
                        endpoint: p.display().to_string(),
                        device,
                        readable,
                        writable,
                        busy_poll,
                    });
                }
            }
        }
    }
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: limit points to a valid mutable rlimit structure.
    if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(Doctor {
        linux: true,
        configfs_available: configfs.exists(),
        kernel_release: fs::read_to_string("/proc/sys/kernel/osrelease")
            .ok()
            .map(|s| s.trim().into()),
        services,
        links: physical_links(sysfs)?,
        streams,
        memlock_soft: limit.rlim_cur,
        memlock_hard: limit.rlim_max,
        notes: vec![
            "Link rates are per direction; raw ceilings exclude framing and protocol overhead"
                .into(),
            "Physical throughput requires hardware qualification on the target link pair".into(),
            "ConfigFS mount and thunderbolt-stream module loading are explicit host prerequisites"
                .into(),
        ],
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct Provision {
    pub service: String,
    pub name: String,
    pub in_hopid: i32,
    pub out_hopid: i32,
    pub ring_size: u32,
    pub throttling_ns: u32,
}

fn component(s: &str) -> bool {
    let mut c = Path::new(s).components();
    matches!(c.next(), Some(Component::Normal(_))) && c.next().is_none() && !s.is_empty()
}

impl Provision {
    pub fn validate(&self) -> Result<()> {
        if !component(&self.service)
            || !component(&self.name)
            || !self.service.contains('.')
            || !(self.in_hopid == -1 || self.in_hopid >= 8)
            || !(self.out_hopid == -1 || self.out_hopid >= 8)
            || !(32..=4096).contains(&self.ring_size)
            || self.throttling_ns > 16_776_960
        {
            return Err(Error::Config("invalid provisioning attributes".into()));
        }
        Ok(())
    }

    pub fn apply(&self, root: &Path) -> Result<PathBuf> {
        self.apply_with_busy_poll(root, false)
    }

    pub fn apply_with_busy_poll(&self, root: &Path, busy_poll: bool) -> Result<PathBuf> {
        self.validate()?;
        if !root.is_dir() {
            return Err(Error::Config(
                "mount configfs and load thunderbolt-stream on the host first".into(),
            ));
        }
        let dir = root.join(&self.service).join(&self.name);
        fs::create_dir_all(&dir)?;
        let polling = dir.join("busy_poll");
        if busy_poll {
            let current = fs::read_to_string(&polling).map_err(|err| {
                Error::Config(format!("cannot read {}: {err}", polling.display()))
            })?;
            if current.trim() != "1" {
                fs::write(&polling, "1")?;
            }
        }
        check_busy_poll(&dir, busy_poll)?;
        for (name, value) in [
            ("ring_size", self.ring_size.to_string()),
            ("throttling", self.throttling_ns.to_string()),
            ("in_hopid", self.in_hopid.to_string()),
            ("out_hopid", self.out_hopid.to_string()),
        ] {
            let p = dir.join(name);
            let current = fs::read_to_string(&p)?;
            // Never replace an existing positive HopID with auto-allocation.
            if name.ends_with("hopid")
                && value == "-1"
                && current.trim().parse::<i32>().unwrap_or(0) > 0
            {
                continue;
            }
            if current.trim() != value {
                fs::write(p, value)?;
            }
        }
        map_device_with_busy_poll(&dir, Path::new("/dev"), busy_poll)
    }
}

pub fn map_device(endpoint: &Path, dev: &Path) -> Result<PathBuf> {
    map_device_with_busy_poll(endpoint, dev, false)
}

fn check_busy_poll(endpoint: &Path, busy_poll: bool) -> Result<()> {
    match fs::read_to_string(endpoint.join("busy_poll")) {
        Ok(value) if value.trim() == if busy_poll { "1" } else { "0" } => {}
        Err(err) if !busy_poll && err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => return Err(err.into()),
        _ => {
            let message = if busy_poll {
                "usb4-busy-poll requires a ConfigFS endpoint with busy_poll=1"
            } else {
                "busy_poll disables readiness polling; turn it off before opening streams"
            };
            return Err(Error::Config(message.into()));
        }
    }
    Ok(())
}

pub fn map_device_with_busy_poll(endpoint: &Path, dev: &Path, busy_poll: bool) -> Result<PathBuf> {
    check_busy_poll(endpoint, busy_poll)?;
    let index: u32 = fs::read_to_string(endpoint.join("index"))?
        .trim()
        .parse()
        .map_err(|_| Error::Config("invalid stream index".into()))?;
    Ok(dev.join(format!("tbstream{index}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_traversal() {
        let p = Provision {
            service: "../x".into(),
            name: "data".into(),
            in_hopid: -1,
            out_hopid: -1,
            ring_size: 256,
            throttling_ns: 8192,
        };
        assert!(p.validate().is_err());
    }
}
