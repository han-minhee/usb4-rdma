use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use usb4_stream_hal::configfs;
struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "u4rd-fixture-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn actual_service_attributes_and_mapping() {
    let f = Fixture::new();
    let service = f.0.join("0-1.0");
    fs::create_dir(&service).unwrap();
    for (name, value) in [("key", "stream"), ("prtcid", "1"), ("prtcvers", "1")] {
        fs::write(service.join(name), value).unwrap();
    }
    let discovered = configfs::discover(&f.0).unwrap();
    assert_eq!(discovered.len(), 1);
    assert!(discovered[0].supported);
    fs::write(service.join("prtcvers"), "999").unwrap();
    assert!(!configfs::discover(&f.0).unwrap()[0].supported);
    let endpoint = service.join("u4rd");
    fs::create_dir(&endpoint).unwrap();
    fs::write(endpoint.join("index"), "7").unwrap();
    assert_eq!(
        configfs::map_device(&endpoint, std::path::Path::new("/dev")).unwrap(),
        PathBuf::from("/dev/tbstream7")
    );
    assert!(configfs::map_device_with_busy_poll(&endpoint, Path::new("/dev"), true).is_err());
    fs::write(endpoint.join("busy_poll"), "0").unwrap();
    assert!(configfs::map_device_with_busy_poll(&endpoint, Path::new("/dev"), true).is_err());
    fs::write(endpoint.join("busy_poll"), "1").unwrap();
    assert!(configfs::map_device(&endpoint, std::path::Path::new("/dev")).is_err());
    assert_eq!(
        configfs::map_device_with_busy_poll(&endpoint, Path::new("/dev"), true).unwrap(),
        PathBuf::from("/dev/tbstream7")
    );
}

#[test]
fn auto_hopid_does_not_replace_existing_allocations() {
    let f = Fixture::new();
    let endpoint = f.0.join("0-1.0/u4rd");
    fs::create_dir_all(&endpoint).unwrap();
    for (name, value) in [
        ("index", "4"),
        ("in_hopid", "9"),
        ("out_hopid", "10"),
        ("ring_size", "256"),
        ("throttling", "8192"),
    ] {
        fs::write(endpoint.join(name), value).unwrap();
    }
    let p = configfs::Provision {
        service: "0-1.0".into(),
        name: "u4rd".into(),
        in_hopid: -1,
        out_hopid: -1,
        ring_size: 256,
        throttling_ns: 8192,
    };
    p.apply(&f.0).unwrap();
    assert_eq!(fs::read_to_string(endpoint.join("in_hopid")).unwrap(), "9");
    assert_eq!(
        fs::read_to_string(endpoint.join("out_hopid")).unwrap(),
        "10"
    );
    assert!(p.apply_with_busy_poll(&f.0, true).is_err());
    fs::write(endpoint.join("busy_poll"), "0").unwrap();
    assert_eq!(
        p.apply_with_busy_poll(&f.0, true).unwrap(),
        PathBuf::from("/dev/tbstream4")
    );
    assert_eq!(fs::read_to_string(endpoint.join("busy_poll")).unwrap(), "1");
}

#[test]
fn physical_rates_do_not_double_duplex_or_assume_missing_lanes() {
    let f = Fixture::new();
    let p = f.0.join("0-1");
    std::fs::create_dir(&p).unwrap();
    for (name, value) in [
        ("rx_speed", "10.0 Gb/s"),
        ("tx_speed", "20.0 Gb/s"),
        ("rx_lanes", "2"),
        ("tx_lanes", "1"),
    ] {
        std::fs::write(p.join(name), value).unwrap();
    }
    let links = configfs::physical_links(&f.0).unwrap();
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].rx_gbps, Some(20.0));
    assert_eq!(links[0].tx_gbps, Some(20.0));
    assert_eq!(links[0].rx_raw_ceiling_gbytes_sec, Some(2.5));
    std::fs::remove_file(p.join("rx_lanes")).unwrap();
    std::fs::write(p.join("tx_speed"), "NaN Gb/s").unwrap();
    let links = configfs::physical_links(&f.0).unwrap();
    assert_eq!(links[0].rx_gbps, None);
    assert_eq!(links[0].tx_gbps, None);
    assert_eq!(links[0].rx_gbps_per_lane, Some(10.0));
}
