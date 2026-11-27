use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
};
use usb4_proto::Error;
use usb4_proto::protocol::{FrameHeader, HEADER, Kind, MAX_FRAME, Packet};
use usb4_stream_hal::{
    Event, LinkCapabilities, StreamTransport, TransportKind, stream::StreamLink,
};
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
        h.tag_or_imm = tag.into();
        let p = Packet::new(h, &vec![tag as u8; if tag == 0 { 32767 } else { 32768 }]).unwrap();
        let mut raw = [0; MAX_FRAME];
        let n = p.encode(&mut raw).unwrap();
        wire.extend_from_slice(&raw[..n]);
    }
    let mut receiver = StreamLink::new(Box::new(std::io::Cursor::new(wire)), caps(0));
    let mut count = 0u32;
    for _ in 0..100 {
        if let Some(Event::Received(p)) = receiver.poll().unwrap() {
            assert_eq!(p.header.tag_or_imm, u64::from(count));
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
    let mut sender = StreamLink::new(Box::new(ShortIo(a)), caps(1)).with_tx_capacity(9);
    let mut receiver = StreamLink::new(Box::new(ShortIo(b)), caps(0));
    let mut h = FrameHeader::control(Kind::Data, 0, 1, 1);
    h.total = 65;
    for tag in 0..8u32 {
        h.tag_or_imm = tag.into();
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
            assert_eq!(p.header.tag_or_imm, u64::from(received));
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
fn one_byte_fragments_across_header_and_payload() {
    use usb4_sim::{Cable, CableConfig};
    let (cable, [a, b]) = Cable::pair(CableConfig {
        read_limit: 1,
        write_limit: 1,
        latency_ticks: 3,
        ..CableConfig::default()
    });
    let (mut tx, mut rx) = (a.into_link(2, 40_000), b.into_link(1, 40_000));
    let count = 7;
    for tag in 0..count {
        let mut header = FrameHeader::control(Kind::Data, 1, 2, 1);
        header.total = 137;
        header.tag_or_imm = tag;
        tx.try_submit(&Packet::new(header, &[tag as u8; 137]).unwrap())
            .unwrap()
            .unwrap();
    }
    tx.flush().unwrap();
    assert!(rx.poll().unwrap().is_none());
    cable.advance(3);
    let mut seen = 0;
    for _ in 0..count as usize * (HEADER + 137) {
        if let Some(Event::Received(packet)) = rx.poll().unwrap() {
            assert_eq!(packet.header.tag_or_imm, seen);
            assert_eq!(packet.data(), &[seen as u8; 137]);
            seen += 1;
        }
    }
    assert_eq!(seen, count);
}

#[test]
fn unplug_preserves_terminal_io_error() {
    use usb4_sim::{Cable, CableConfig, Unplug};
    for mode in [Unplug::BrokenPipe, Unplug::PollError] {
        let (cable, [a, b]) = Cable::pair(CableConfig::default());
        let (mut tx, mut rx) = (a.into_link(2, 40_000), b.into_link(1, 40_000));
        tx.try_submit(&Packet::new(FrameHeader::control(Kind::Ping, 1, 2, 1), &[]).unwrap())
            .unwrap()
            .unwrap();
        cable.unplug(mode);
        assert!(matches!(tx.flush(), Err(Error::Io(e)) if e.raw_os_error() == Some(libc::EPIPE)));
        match mode {
            Unplug::BrokenPipe => assert!(matches!(rx.poll(), Ok(Some(Event::Closed)))),
            Unplug::PollError => assert!(
                matches!(rx.poll(), Err(Error::Io(e)) if e.raw_os_error() == Some(libc::EPIPE))
            ),
        }
    }
}
