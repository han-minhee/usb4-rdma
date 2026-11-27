use crate::{Error, NodeId, Result};
use std::sync::Arc;
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned, byteorder::little_endian as le};

pub const CHUNK: usize = 32 * 1024;
pub const HEADER: usize = 104;
pub const MAX_FRAME: usize = HEADER + CHUNK;
pub const VERSION: u16 = 2;
pub const MAGIC: [u8; 4] = *b"U4RD";

pub const FLAG_DIRECT: u32 = 1 << 31;
pub const FLAG_WITH_IMM: u32 = 1 << 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Kind {
    Hello = 1,
    Open = 2,
    OpenAck = 3,
    Rts = 4,
    Grant = 5,
    Data = 6,
    Ack = 7,
    Credit = 8,
    Close = 9,
    Error = 10,
    HelloAck = 11,
    Ping = 12,
    Pong = 13,
    RdmaWrite = 14,
    RdmaWriteAck = 15,
    RdmaReadReq = 16,
    RdmaReadResp = 17,
}

impl Kind {
    /// True for payload-carrying data-plane frames subject to hop credits and bonding.
    pub const fn is_data_plane(self) -> bool {
        matches!(self, Self::Data | Self::RdmaWrite | Self::RdmaReadResp)
    }

    pub const fn may_have_payload(self) -> bool {
        matches!(
            self,
            Self::Data | Self::RdmaWrite | Self::RdmaReadResp | Self::Hello | Self::HelloAck
        )
    }
}

impl TryFrom<u16> for Kind {
    type Error = Error;

    fn try_from(v: u16) -> Result<Self> {
        Ok(match v {
            1 => Self::Hello,
            2 => Self::Open,
            3 => Self::OpenAck,
            4 => Self::Rts,
            5 => Self::Grant,
            6 => Self::Data,
            7 => Self::Ack,
            8 => Self::Credit,
            9 => Self::Close,
            10 => Self::Error,
            11 => Self::HelloAck,
            12 => Self::Ping,
            13 => Self::Pong,
            14 => Self::RdmaWrite,
            15 => Self::RdmaWriteAck,
            16 => Self::RdmaReadReq,
            17 => Self::RdmaReadResp,
            _ => return Err(Error::Protocol("unknown frame kind")),
        })
    }
}

/// 104-byte little-endian `U4RD` v2 frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub kind: Kind,
    pub length: u32,
    pub flags: u32,
    pub source: NodeId,
    pub destination: NodeId,
    pub hops: u32,
    pub tag_or_imm: u64,
    pub epoch: u64,
    pub connection: u64,
    pub request: u64,
    pub offset: u64,
    pub total: u64,
    pub rkey: u64,
    pub remote_offset: u64,
    pub aux: u64,
}

impl FrameHeader {
    pub fn control(kind: Kind, source: NodeId, destination: NodeId, epoch: u64) -> Self {
        Self {
            kind,
            length: 0,
            flags: 0,
            source,
            destination,
            hops: 64,
            tag_or_imm: 0,
            epoch,
            connection: 0,
            request: 0,
            offset: 0,
            total: 0,
            rkey: 0,
            remote_offset: 0,
            aux: 0,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.hops == 0 || self.hops > 64 {
            return Err(Error::Protocol("hop limit"));
        }
        if self.length as usize > CHUNK {
            return Err(Error::Protocol("payload length"));
        }
        if self.kind.is_data_plane()
            && self
                .offset
                .checked_add(self.length as u64)
                .is_none_or(|end| end > self.total)
        {
            return Err(Error::Protocol("payload range"));
        }
        if matches!(
            self.kind,
            Kind::RdmaWrite | Kind::RdmaReadReq | Kind::RdmaReadResp
        ) && self.remote_offset.checked_add(self.total).is_none()
        {
            return Err(Error::Protocol("remote offset overflow"));
        }
        if !self.kind.may_have_payload() && self.length != 0 {
            return Err(Error::Protocol("control payload"));
        }
        Ok(())
    }
}

/// Small control frames are inline; data handles share immutable pooled storage.
#[derive(Clone)]
enum Payload {
    Inline([u8; 32], usize),
    Pooled(Arc<[u8; CHUNK]>, usize),
    Owned(Arc<[u8]>),
}

#[derive(Clone)]
pub struct Packet {
    pub header: FrameHeader,
    payload: Payload,
}

/// Setup-time storage. `Arc::get_mut` guarantees exclusive reuse after the last
/// packet clone retires, including clones moved to another thread.
pub struct PacketPool {
    pages: Vec<Arc<[u8; CHUNK]>>,
    cursor: usize,
}

impl PacketPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            pages: (0..capacity).map(|_| Arc::new([0; CHUNK])).collect(),
            cursor: 0,
        }
    }

    pub fn packet(&mut self, h: FrameHeader, data: &[u8]) -> Result<Packet> {
        self.packet_with(h, data.len(), |out| {
            out.copy_from_slice(data);
            Ok(())
        })
    }

    pub fn packet_with(
        &mut self,
        mut header: FrameHeader,
        length: usize,
        fill: impl FnOnce(&mut [u8]) -> Result<()>,
    ) -> Result<Packet> {
        if length > CHUNK {
            return Err(Error::Protocol("payload too large"));
        }
        header.length = length as u32;
        header.validate()?;
        if length <= 32 {
            let mut data = [0; 32];
            fill(&mut data[..length])?;
            return Ok(Packet {
                header,
                payload: Payload::Inline(data, length),
            });
        }
        for _ in 0..self.pages.len() {
            let index = self.cursor;
            self.cursor = (index + 1) % self.pages.len();
            if let Some(data) = Arc::get_mut(&mut self.pages[index]) {
                fill(&mut data[..length])?;
                return Ok(Packet {
                    header,
                    payload: Payload::Pooled(self.pages[index].clone(), length),
                });
            }
        }
        Err(Error::Exhausted)
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<Packet> {
        let h = Packet::checked_header(bytes)?;
        self.packet(h, &bytes[HEADER..])
    }
}

impl Packet {
    /// Convenience allocator for fixtures/tools. Live transports use `PacketPool`.
    pub fn new(mut header: FrameHeader, data: &[u8]) -> Result<Self> {
        if data.len() > CHUNK {
            return Err(Error::Protocol("payload too large"));
        }
        header.length = data.len() as u32;
        header.validate()?;
        let payload = if data.len() <= 32 {
            let mut bytes = [0; 32];
            bytes[..data.len()].copy_from_slice(data);
            Payload::Inline(bytes, data.len())
        } else {
            Payload::Owned(Arc::from(data))
        };
        Ok(Self { header, payload })
    }

    pub fn data(&self) -> &[u8] {
        match &self.payload {
            Payload::Inline(data, n) => &data[..*n],
            Payload::Pooled(data, n) => &data[..*n],
            Payload::Owned(data) => data,
        }
    }

    pub fn encode(&self, out: &mut [u8]) -> Result<usize> {
        self.header.validate()?;
        if self.header.length as usize != self.data().len() {
            return Err(Error::Protocol("payload length changed"));
        }
        Self::encode_with(&self.header, out, |payload| {
            payload.copy_from_slice(self.data());
            Ok(())
        })
    }

    /// Encode header and fill payload directly into `out`, writing CRC32 in place.
    pub fn encode_with(
        header: &FrameHeader,
        out: &mut [u8],
        fill: impl FnOnce(&mut [u8]) -> Result<()>,
    ) -> Result<usize> {
        header.validate()?;
        let n = HEADER + header.length as usize;
        if out.len() < n {
            return Err(Error::Protocol("output capacity"));
        }
        write_header(header, out);
        fill(&mut out[HEADER..n])?;
        let crc = crc32fast::hash(&out[..n]);
        put32(out, 96, crc);
        Ok(n)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Self::new(Self::checked_header(bytes)?, &bytes[HEADER..])
    }

    /// Verify framing and CRC32 in place, returning the header and borrowed payload slice.
    pub fn decode_raw(bytes: &[u8]) -> Result<(FrameHeader, &[u8])> {
        let h = Self::checked_header(bytes)?;
        Ok((h, &bytes[HEADER..]))
    }

    fn checked_header(bytes: &[u8]) -> Result<FrameHeader> {
        let h = decode_header(bytes)?;
        let n = HEADER + h.length as usize;
        if bytes.len() != n {
            return Err(Error::Protocol("frame length"));
        }
        verify_crc(bytes)?;
        Ok(h)
    }
}

pub(crate) fn verify_crc(bytes: &[u8]) -> Result<()> {
    let mut crc = crc32fast::Hasher::new();
    crc.update(&bytes[..96]);
    crc.update(&[0; 4]);
    crc.update(&bytes[100..]);
    if crc.finalize().to_le_bytes() != bytes[96..100] {
        return Err(Error::Protocol("checksum"));
    }
    Ok(())
}

#[repr(C)]
#[derive(FromBytes, Immutable, KnownLayout, Unaligned)]
struct WireHeader {
    magic: [u8; 4],
    version: le::U16,
    kind: le::U16,
    length: le::U32,
    flags: le::U32,
    source: le::U32,
    destination: le::U32,
    hops: le::U32,
    tag_or_imm: le::U32,
    epoch: le::U64,
    connection: le::U64,
    request: le::U64,
    offset: le::U64,
    total: le::U64,
    rkey: le::U64,
    remote_offset: le::U64,
    aux: le::U64,
    crc: le::U32,
    tag_high: le::U32,
}

const _: () = assert!(size_of::<WireHeader>() == HEADER);

pub fn decode_header(bytes: &[u8]) -> Result<FrameHeader> {
    let (wire, _) =
        WireHeader::ref_from_prefix(bytes).map_err(|_| Error::Protocol("short header"))?;
    if wire.magic != MAGIC || wire.version.get() != VERSION {
        return Err(Error::Protocol("magic/version"));
    }
    let header = FrameHeader {
        kind: Kind::try_from(wire.kind.get())?,
        length: wire.length.get(),
        flags: wire.flags.get(),
        source: wire.source.get(),
        destination: wire.destination.get(),
        hops: wire.hops.get(),
        tag_or_imm: u64::from(wire.tag_or_imm.get()) | (u64::from(wire.tag_high.get()) << 32),
        epoch: wire.epoch.get(),
        connection: wire.connection.get(),
        request: wire.request.get(),
        offset: wire.offset.get(),
        total: wire.total.get(),
        rkey: wire.rkey.get(),
        remote_offset: wire.remote_offset.get(),
        aux: wire.aux.get(),
    };
    header.validate()?;
    Ok(header)
}

fn write_header(h: &FrameHeader, out: &mut [u8]) {
    out[..HEADER].fill(0);
    out[..4].copy_from_slice(&MAGIC);
    put16(out, 4, VERSION);
    put16(out, 6, h.kind as u16);
    for (o, v) in [
        (8, h.length),
        (12, h.flags),
        (16, h.source),
        (20, h.destination),
        (24, h.hops),
        (28, h.tag_or_imm as u32),
        (100, (h.tag_or_imm >> 32) as u32),
    ] {
        put32(out, o, v);
    }
    for (o, v) in [
        (32, h.epoch),
        (40, h.connection),
        (48, h.request),
        (56, h.offset),
        (64, h.total),
        (72, h.rkey),
        (80, h.remote_offset),
        (88, h.aux),
    ] {
        put64(out, o, v);
    }
}

fn put16(b: &mut [u8], o: usize, v: u16) {
    b[o..o + 2].copy_from_slice(&v.to_le_bytes());
}
fn put32(b: &mut [u8], o: usize, v: u32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes());
}
fn put64(b: &mut [u8], o: usize, v: u64) {
    b[o..o + 8].copy_from_slice(&v.to_le_bytes());
}
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn round_trip_data(
            data in proptest::collection::vec(any::<u8>(), 0..=CHUNK),
            tag in any::<u32>()
        ) {
            let mut h = FrameHeader::control(Kind::Data, 1, 2, 3);
            h.total = data.len() as u64;
            h.tag_or_imm = tag.into();
            let p = Packet::new(h, &data).unwrap();
            let mut b = vec![0; MAX_FRAME];
            let n = p.encode(&mut b).unwrap();
            let q = Packet::decode(&b[..n]).unwrap();
            prop_assert_eq!(q.header, p.header);
            prop_assert_eq!(q.data(), data);
        }

        #[test]
        fn round_trip_rdma_write(
            data in proptest::collection::vec(any::<u8>(), 0..=CHUNK),
            rkey in any::<u64>(),
            remote_offset in 0u64..(1u64 << 40),
            imm in any::<u32>(),
        ) {
            let mut h = FrameHeader::control(Kind::RdmaWrite, 0, 1, 7);
            h.total = data.len() as u64;
            h.rkey = rkey;
            h.remote_offset = remote_offset;
            h.flags = FLAG_WITH_IMM;
            h.tag_or_imm = imm.into();
            let p = Packet::new(h, &data).unwrap();
            let mut b = vec![0; MAX_FRAME];
            let n = p.encode(&mut b).unwrap();
            let q = Packet::decode(&b[..n]).unwrap();
            prop_assert_eq!(q.header, p.header);
            prop_assert_eq!(q.data(), data);
        }

        #[test]
        fn malformed_never_panics(data in proptest::collection::vec(any::<u8>(), 0..MAX_FRAME + 8)) {
            let _ = Packet::decode(&data);
        }
    }

    #[test]
    fn pooled_lifetime_and_exhaustion() {
        let mut pool = PacketPool::new(1);
        let mut h = FrameHeader::control(Kind::Data, 1, 2, 1);
        h.total = CHUNK as u64;
        let packet = pool.packet(h, &[7; CHUNK]).unwrap();
        let retained = packet.clone();
        drop(packet);
        assert!(matches!(pool.packet(h, &[8; CHUNK]), Err(Error::Exhausted)));
        let control = FrameHeader::control(Kind::Credit, 1, 2, 1);
        assert!(pool.packet(control, &[]).is_ok());
        assert_eq!(retained.data(), &[7; CHUNK]);
        std::thread::spawn(move || {
            assert_eq!(retained.data(), &[7; CHUNK]);
            drop(retained);
        })
        .join()
        .unwrap();
        let packet = pool.packet(h, &[9; CHUNK]).unwrap();
        drop(pool);
        assert_eq!(packet.data(), &[9; CHUNK]);
    }

    #[test]
    fn pooled_decode_and_failed_fill() {
        let mut pool = PacketPool::new(1);
        let mut h = FrameHeader::control(Kind::RdmaWrite, 1, 2, 1);
        h.total = CHUNK as u64;
        h.rkey = 0xDEAD_BEEF;
        assert!(
            pool.packet_with(h, CHUNK, |_| Err(Error::Disconnected))
                .is_err()
        );
        let packet = pool.packet(h, &[3; CHUNK]).unwrap();
        let mut wire = [0; MAX_FRAME];
        let n = packet.encode(&mut wire).unwrap();
        drop(packet);
        let mut decoded = pool.decode(&wire[..n]).unwrap();
        assert_eq!(decoded.data(), &[3; CHUNK]);
        decoded.header.length -= 1;
        assert!(decoded.encode(&mut wire).is_err());
        drop(decoded);
        wire[n - 1] ^= 1;
        assert!(pool.decode(&wire[..n]).is_err());
        assert!(pool.packet(h, &[4; CHUNK]).is_ok());
        assert!(matches!(
            PacketPool::new(0).packet(h, &[0; 33]),
            Err(Error::Exhausted)
        ));
    }

    #[test]
    fn corruption_and_overflow() {
        let mut h = FrameHeader::control(Kind::Data, 1, 2, 1);
        h.total = 4;
        let p = Packet::new(h, b"test").unwrap();
        let mut b = [0; MAX_FRAME];
        let n = p.encode(&mut b).unwrap();
        b[n - 1] ^= 1;
        assert!(Packet::decode(&b[..n]).is_err());
        h.offset = u64::MAX;
        assert!(Packet::new(h, b"x").is_err());

        let mut rh = FrameHeader::control(Kind::RdmaWrite, 1, 2, 1);
        rh.remote_offset = u64::MAX;
        rh.total = 1;
        assert!(rh.validate().is_err());
    }
}
