use std::{
    fs,
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use usb4_rdma_core::{
    Error,
    protocol::{FrameHeader, HEADER, Kind, MAX_FRAME, Packet},
};
use usb4_rdma_transport::{
    Event, LinkBackend, LinkCapabilities, TransportKind, configfs, stream::StreamLink,
};

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

fn caps(peer: u32) -> LinkCapabilities {
    LinkCapabilities {
        peer,
        speed_mbps: 40000,
        controller_group: 0,
        transport_kind: TransportKind::Emulated,
        kernel_copies: false,
        fixed_buffers: false,
        sqpoll: false,
        zero_copy_dma: false,
    }
}

struct ShortIo(UnixStream);

impl Read for ShortIo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let limit = buf.len().min(7);
        self.0.read(&mut buf[..limit])
    }
}

impl Write for ShortIo {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(&buf[..buf.len().min(37)])
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
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
fn corruption_is_terminal() {
    let (mut writer, reader) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let mut h = FrameHeader::control(Kind::Data, 0, 1, 1);
    h.total = 4;
    let p = Packet::new(h, b"test").unwrap();
    let mut raw = [0; MAX_FRAME];
    let n = p.encode(&mut raw).unwrap();
    raw[HEADER] ^= 1;
    writer.write_all(&raw[..n]).unwrap();
    let mut receiver = StreamLink::new(Box::new(reader), caps(0));
    for _ in 0..3 {
        match receiver.poll() {
            Err(Error::Protocol("checksum")) => return,
            Ok(None) => {}
            _ => panic!("corruption was not rejected"),
        }
    }
    panic!("checksum failure did not arrive within bounded polling");
}

#[test]
fn disconnect_is_eof_not_completion() {
    let (writer, reader) = UnixStream::pair().unwrap();
    drop(writer);
    let mut receiver = StreamLink::new(Box::new(reader), caps(0));
    assert!(matches!(receiver.poll().unwrap(), Some(Event::Closed)));
}

#[test]
fn coalesced_frames_and_incomplete_tail_remain_ordered() {
    let mut wire = Vec::new();
    for tag in 0..9u32 {
        let mut h = FrameHeader::control(Kind::Data, 0, 1, 1);
        h.total = 32768;
        h.tag_or_imm = tag;
        let p = Packet::new(h, &vec![tag as u8; if tag == 0 { 32767 } else { 32768 }]).unwrap();
        let mut raw = [0; MAX_FRAME];
        let n = p.encode(&mut raw).unwrap();
        wire.extend_from_slice(&raw[..n]);
    }
    let mut receiver = StreamLink::new(Box::new(std::io::Cursor::new(wire)), caps(0));
    let mut count = 0u32;
    for _ in 0..100 {
        if let Some(Event::Received(p)) = receiver.poll().unwrap() {
            assert_eq!(p.header.tag_or_imm, count);
            assert_eq!(
                p.data(),
                &vec![count as u8; if count == 0 { 32767 } else { 32768 }]
            );
            count += 1;
            if count == 9 {
                break;
            }
        }
    }
    assert_eq!(count, 9);
}

#[test]
fn fragmented_batch_completes_every_id_once_and_keeps_order() {
    let (a, b) = UnixStream::pair().unwrap();
    a.set_nonblocking(true).unwrap();
    b.set_nonblocking(true).unwrap();
    let mut sender = StreamLink::new(Box::new(ShortIo(a)), caps(1));
    let mut receiver = StreamLink::new(Box::new(ShortIo(b)), caps(0));
    let mut h = FrameHeader::control(Kind::Data, 0, 1, 1);
    h.total = 65;
    for tag in 0..8u32 {
        h.tag_or_imm = tag;
        let p = Packet::new(h, &[tag as u8; 65]).unwrap();
        assert_eq!(sender.try_submit(&p).unwrap(), Some(tag as u64 + 1));
    }
    let packet = Packet::new(h, &[0; 65]).unwrap();
    assert!(sender.try_submit(&packet).unwrap().is_none());
    let (mut sent, mut received) = (0u64, 0u32);
    for _ in 0..1000 {
        if let Some(Event::Sent(id, bytes)) = sender.poll().unwrap() {
            sent += 1;
            assert_eq!(id, sent);
            assert_eq!(bytes, HEADER + 65);
        }
        if let Some(Event::Received(p)) = receiver.poll().unwrap() {
            assert_eq!(p.header.tag_or_imm, received);
            assert_eq!(p.data(), &[received as u8; 65]);
            received += 1;
        }
        if sent == 8 && received == 8 {
            break;
        }
    }
    assert_eq!((sent, received), (8, 8));
    assert!(sender.poll().unwrap().is_none());
    assert_eq!(sender.try_submit(&packet).unwrap(), Some(9));
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
