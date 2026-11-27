use crate::{
    AccessFlags, CompletionQueue, EndpointHandle, OwnedMr, ProtectionDomain, QueuePair, WcOpcode,
    WcStatus,
};
use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
};

pub const ADDRESS_BYTES: usize = 40;
pub const HELLO_BYTES: usize = ADDRESS_BYTES + 8;
pub const MAX_MESSAGE: usize = 64 * 1024 * 1024;
pub const EAGER_LIMIT: usize = 16 * 1024;
const MAGIC: &[u8; 4] = b"U4F4";
const HEADER: usize = 32;
const UNIX_WINDOW: usize = 1024 * 1024;
const USB_BULK_CHUNK: usize = 4 * 1024 * 1024;
const USB_SMALL_SLOTS: usize = 64;
const USB_BULK_SLOTS: usize = 2;
const SMALL_STRIDE: usize = HEADER + EAGER_LIMIT + 32; // 16,448 B (64-byte aligned)
const BULK_STRIDE: usize = HEADER + USB_BULK_CHUNK + 32; // 4,194,368 B (64-byte aligned)
const BULK_BASE: usize = USB_SMALL_SLOTS * SMALL_STRIDE;
const CREDIT_SLOT_OFFSET: usize = BULK_BASE + USB_BULK_SLOTS * BULK_STRIDE;
const CREDIT_SLOT_IDX: usize = USB_SMALL_SLOTS + USB_BULK_SLOTS;
const RX_MR_BYTES: usize = CREDIT_SLOT_OFFSET + 64;
const READY_SLOT_OFFSET: usize = CREDIT_SLOT_OFFSET + 32;
const READY_MAGIC: u64 = 0x5534_4633_5244_5931;
const CREDIT_IMM_FLAG: u64 = 1 << 31;
const TX_MR_BYTES: usize = BULK_STRIDE * 2;
const STANDING_RECVS: usize = 8;
const HELLO_SDK_TAG: u64 = 0x4f46_4933;
const HELLO_RX_CAP: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Address(pub [u8; ADDRESS_BYTES]);

impl Address {
    pub fn new(handle: EndpointHandle) -> io::Result<Self> {
        let mut bytes = [0; ADDRESS_BYTES];
        bytes[..4].copy_from_slice(MAGIC);
        bytes[4..24].copy_from_slice(&handle.to_bytes());
        std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes[24..])?;
        Ok(Self(bytes))
    }

    pub fn parse(bytes: [u8; ADDRESS_BYTES]) -> io::Result<Self> {
        if &bytes[..4] != MAGIC {
            return Err(invalid());
        }
        let address = Self(bytes);
        address.handle()?;
        Ok(address)
    }

    pub fn handle(self) -> io::Result<EndpointHandle> {
        EndpointHandle::from_bytes(&self.0[4..24]).map_err(sdk)
    }

    pub fn abstract_name(self) -> String {
        use std::fmt::Write;
        let mut name = String::from("usb4-ofi-");
        for value in &self.0[24..] {
            write!(name, "{value:02x}").expect("writing to a String");
        }
        name
    }
}

#[track_caller]
pub fn invalid() -> io::Error {
    let loc = std::panic::Location::caller();
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid data at {}:{}", loc.file(), loc.line()),
    )
}

pub fn sdk(error: crate::Error) -> io::Error {
    io::Error::other(error)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Hello = 1,
    HelloAck = 2,
    Offer = 3,
    Grant = 4,
    Data = 5,
    Done = 6,
    Eager = 7,
    Credit = 8,
    RmaRead = 9,
    RmaWrite = 10,
    RmaResult = 11,
}

#[derive(Clone, Copy, Debug)]
pub struct Header {
    pub kind: Kind,
    pub tagged: bool,
    pub ack_small: u8,
    pub ack_bulk: u8,
    pub id: u64,
    pub value: u64,
    pub body: usize,
}

impl Header {
    pub fn control(kind: Kind, id: u64, value: u64) -> Self {
        Self {
            kind,
            tagged: false,
            ack_small: 0,
            ack_bulk: 0,
            id,
            value,
            body: 0,
        }
    }

    fn encode(self, buffer: &mut [u8]) {
        let header = &mut buffer[..HEADER];
        header.fill(0);
        header[..4].copy_from_slice(MAGIC);
        header[4] = self.kind as u8;
        header[5] = u8::from(self.tagged);
        header[6] = self.ack_small;
        header[7] = self.ack_bulk;
        header[8..16].copy_from_slice(&self.id.to_le_bytes());
        header[16..24].copy_from_slice(&self.value.to_le_bytes());
        header[24..28].copy_from_slice(&(self.body as u32).to_le_bytes());
    }

    fn parse(header: &[u8]) -> io::Result<Self> {
        if header.len() < HEADER
            || &header[..4] != MAGIC
            || header[5] > 1
            || header[28..32] != [0; 4]
        {
            return Err(invalid());
        }
        let kind = match header[4] {
            1 => Kind::Hello,
            2 => Kind::HelloAck,
            3 => Kind::Offer,
            4 => Kind::Grant,
            5 => Kind::Data,
            6 => Kind::Done,
            7 => Kind::Eager,
            8 => Kind::Credit,
            9 => Kind::RmaRead,
            10 => Kind::RmaWrite,
            11 => Kind::RmaResult,
            _ => return Err(invalid()),
        };
        let body = u32::from_le_bytes(header[24..28].try_into().expect("header")) as usize;
        match kind {
            Kind::Hello if body != HELLO_BYTES => return Err(invalid()),
            Kind::Offer if body != 8 => return Err(invalid()),
            Kind::RmaRead if body != 16 => return Err(invalid()),
            Kind::RmaWrite if !(8..=EAGER_LIMIT).contains(&body) => return Err(invalid()),
            Kind::Eager | Kind::RmaResult if body > EAGER_LIMIT => return Err(invalid()),
            Kind::Data if body == 0 || body > USB_BULK_CHUNK => return Err(invalid()),
            Kind::HelloAck | Kind::Grant | Kind::Done | Kind::Credit if body != 0 => {
                return Err(invalid());
            }
            _ => {}
        }
        Ok(Self {
            kind,
            tagged: header[5] == 1,
            ack_small: header[6],
            ack_bulk: header[7],
            id: u64::from_le_bytes(header[8..16].try_into().expect("header")),
            value: u64::from_le_bytes(header[16..24].try_into().expect("header")),
            body,
        })
    }
}

#[derive(Clone, Copy)]
struct StagedTx {
    tx_buf_idx: usize,
    len: usize,
    rx_offset: usize,
    imm: u64,
    send_tag: Option<u64>,
}

pub enum Transport {
    Unix(UnixStream),
    Usb(Box<Usb>),
}

pub struct Usb {
    pub pair: QueuePair,
    send: CompletionQueue,
    recv: CompletionQueue,
    tx: OwnedMr,
    rx: OwnedMr,
    hello_rx: OwnedMr,
    peer_rkey: u64,
    rdma_ready: bool,
    ready_written: bool,
    peer_ready: bool,
    send_pending: bool,
    active_tx_buf: usize,
    active_tx_tag: Option<u64>,
    completed_tx_tag: Option<u64>,
    staged: Option<StagedTx>,
    hello_recv_pending: bool,
    hello_received: bool,
    posted_imm_recvs: usize,
    next_imm_wr: u64,
    next_small_slot: usize,
    next_bulk_slot: usize,
    small_credits: usize,
    bulk_credits: usize,
    unacked_small: u8,
    unacked_bulk: u8,
}

impl Usb {
    pub fn new(
        pd: &ProtectionDomain,
        pair: QueuePair,
        send: CompletionQueue,
        recv: CompletionQueue,
    ) -> io::Result<Self> {
        let tx_access = AccessFlags::LOCAL_READ | AccessFlags::LOCAL_WRITE;
        let rx_access = AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE;
        Ok(Self {
            pair,
            send,
            recv,
            tx: pd.alloc_mr(TX_MR_BYTES, tx_access).map_err(sdk)?,
            rx: pd.alloc_mr(RX_MR_BYTES, rx_access).map_err(sdk)?,
            hello_rx: pd
                .alloc_mr(HELLO_RX_CAP, AccessFlags::LOCAL_WRITE)
                .map_err(sdk)?,
            peer_rkey: 0,
            rdma_ready: false,
            ready_written: false,
            peer_ready: false,
            send_pending: false,
            active_tx_buf: 0,
            active_tx_tag: None,
            completed_tx_tag: None,
            staged: None,
            hello_recv_pending: false,
            hello_received: false,
            posted_imm_recvs: 0,
            next_imm_wr: 100,
            next_small_slot: 0,
            next_bulk_slot: 0,
            small_credits: USB_SMALL_SLOTS,
            bulk_credits: USB_BULK_SLOTS,
            unacked_small: 0,
            unacked_bulk: 0,
        })
    }

    fn slot_bounds(slot: usize) -> Option<(usize, usize)> {
        if slot < USB_SMALL_SLOTS {
            Some((slot * SMALL_STRIDE, SMALL_STRIDE))
        } else if slot < CREDIT_SLOT_IDX {
            let b = slot - USB_SMALL_SLOTS;
            Some((BULK_BASE + b * BULK_STRIDE, BULK_STRIDE))
        } else if slot == CREDIT_SLOT_IDX {
            Some((CREDIT_SLOT_OFFSET, HEADER))
        } else {
            None
        }
    }
}

pub struct Link {
    pub transport: Transport,
    tx: Box<[u8]>,
    rx: Box<[u8]>,
    tx_len: usize,
    tx_pos: usize,
    rx_len: usize,
    rx_goal: usize,
    ready: Option<(Header, usize, usize)>, // (header, payload_offset_in_rx_mr, slot_idx)
}

impl Link {
    pub fn new(transport: Transport) -> Self {
        let local_window = if matches!(transport, Transport::Unix(_)) {
            UNIX_WINDOW
        } else {
            0
        };
        Self {
            transport,
            tx: vec![0; local_window].into_boxed_slice(),
            rx: vec![0; local_window].into_boxed_slice(),
            tx_len: 0,
            tx_pos: 0,
            rx_len: 0,
            rx_goal: HEADER,
            ready: None,
        }
    }

    pub fn usb_node(&self) -> Option<u32> {
        match &self.transport {
            Transport::Usb(usb) => Some(usb.pair.peer_node()),
            Transport::Unix(_) => None,
        }
    }

    pub fn local_rx_rkey(&self) -> u64 {
        match &self.transport {
            Transport::Usb(usb) => usb.rx.rkey(),
            Transport::Unix(_) => 0,
        }
    }

    pub fn set_peer_rkey(&mut self, rkey: u64) {
        if let Transport::Usb(usb) = &mut self.transport {
            usb.peer_rkey = rkey;
        }
    }

    pub fn is_rdma_ready(&self) -> bool {
        match &self.transport {
            Transport::Unix(_) => true,
            Transport::Usb(usb) => usb.rdma_ready,
        }
    }

    pub fn enable_rdma(&mut self) -> io::Result<()> {
        if let Transport::Usb(usb) = &mut self.transport {
            if usb.rdma_ready {
                return Ok(());
            }
            if usb.peer_rkey == 0 || usb.send_pending || usb.hello_recv_pending {
                return Err(invalid());
            }
            usb.rdma_ready = true;
        }
        Ok(())
    }

    pub fn payload_capacity(&self) -> usize {
        match self.transport {
            Transport::Unix(_) => UNIX_WINDOW - HEADER,
            Transport::Usb(_) => USB_BULK_CHUNK,
        }
    }

    pub fn idle(&self) -> bool {
        match &self.transport {
            Transport::Unix(_) => self.tx_len == 0,
            Transport::Usb(usb) => !usb.send_pending && self.tx_len == 0 && usb.staged.is_none(),
        }
    }

    pub fn can_stage(&self) -> bool {
        match &self.transport {
            Transport::Unix(_) => self.tx_len == 0,
            Transport::Usb(usb) => {
                if !usb.rdma_ready {
                    !usb.send_pending && self.tx_len == 0
                } else {
                    usb.ready_written && usb.peer_ready && usb.staged.is_none()
                }
            }
        }
    }

    pub fn can_send_small(&self) -> bool {
        match &self.transport {
            Transport::Unix(_) => true,
            Transport::Usb(usb) => usb.small_credits > 0,
        }
    }

    pub fn can_send_bulk(&self) -> bool {
        match &self.transport {
            Transport::Unix(_) => true,
            Transport::Usb(usb) => usb.bulk_credits > 0,
        }
    }

    pub fn needs_credit_return(&self) -> bool {
        match &self.transport {
            Transport::Unix(_) => false,
            Transport::Usb(usb) => usb.unacked_bulk > 0 || usb.unacked_small >= 8,
        }
    }

    pub fn stage(&mut self, header: Header, bytes: &[u8]) -> io::Result<()> {
        self.stage_tagged(header, bytes, None)
    }

    pub fn stage_tagged(
        &mut self,
        mut header: Header,
        bytes: &[u8],
        send_tag: Option<u64>,
    ) -> io::Result<()> {
        assert!(
            self.can_stage()
                && header.body == bytes.len()
                && bytes.len() <= self.payload_capacity()
        );
        let len = HEADER + bytes.len();
        match &mut self.transport {
            Transport::Unix(_) => {
                header.encode(&mut self.tx);
                self.tx[HEADER..len].copy_from_slice(bytes);
                self.tx_len = len;
                self.tx_pos = 0;
            }
            Transport::Usb(usb) => {
                if !usb.rdma_ready {
                    assert_eq!(header.kind, Kind::Hello);
                    let mut encoded = [0; HEADER];
                    header.encode(&mut encoded);
                    usb.tx.write_bytes(0, &encoded).map_err(sdk)?;
                    usb.tx.write_bytes(HEADER, bytes).map_err(sdk)?;
                    self.tx_len = len;
                    self.tx_pos = 0;
                } else {
                    header.ack_small = usb.unacked_small;
                    header.ack_bulk = usb.unacked_bulk;
                    usb.unacked_small = 0;
                    usb.unacked_bulk = 0;

                    let tx_buf_idx = if usb.send_pending {
                        1 - usb.active_tx_buf
                    } else {
                        0
                    };
                    let (staged_len, rx_offset, imm) = if header.kind == Kind::Credit {
                        (
                            8,
                            CREDIT_SLOT_OFFSET,
                            CREDIT_IMM_FLAG
                                | (u64::from(header.ack_small) << 8)
                                | u64::from(header.ack_bulk),
                        )
                    } else {
                        let (rx_slot, rx_offset) = match header.kind {
                            Kind::Data => {
                                if usb.bulk_credits == 0 {
                                    return Err(invalid());
                                }
                                usb.bulk_credits -= 1;
                                let idx = usb.next_bulk_slot;
                                usb.next_bulk_slot = (idx + 1) % USB_BULK_SLOTS;
                                (USB_SMALL_SLOTS + idx, BULK_BASE + idx * BULK_STRIDE)
                            }
                            _ => {
                                if usb.small_credits == 0 {
                                    return Err(invalid());
                                }
                                usb.small_credits -= 1;
                                let idx = usb.next_small_slot;
                                usb.next_small_slot = (idx + 1) % USB_SMALL_SLOTS;
                                (idx, idx * SMALL_STRIDE)
                            }
                        };
                        let base = tx_buf_idx * BULK_STRIDE;
                        let mut encoded = [0; HEADER];
                        header.encode(&mut encoded);
                        usb.tx.write_bytes(base, &encoded).map_err(sdk)?;
                        if !bytes.is_empty() {
                            usb.tx.write_bytes(base + HEADER, bytes).map_err(sdk)?;
                        }
                        (len, rx_offset, ((rx_slot as u64) << 24) | (len as u64))
                    };
                    usb.staged = Some(StagedTx {
                        tx_buf_idx,
                        len: staged_len,
                        rx_offset,
                        imm,
                        send_tag,
                    });
                }
            }
        }
        Ok(())
    }

    pub fn take_sent_tag(&mut self) -> Option<u64> {
        match &mut self.transport {
            Transport::Unix(_) => None,
            Transport::Usb(usb) => usb.completed_tx_tag.take(),
        }
    }

    pub fn incoming(&self) -> Option<(Header, &[u8])> {
        let (header, base, _) = self.ready?;
        let data = match &self.transport {
            Transport::Unix(_) => &self.rx[HEADER..HEADER + header.body],
            Transport::Usb(usb) => {
                let mr = if usb.rdma_ready {
                    &usb.rx
                } else {
                    &usb.hello_rx
                };
                // SAFETY: the SDK receive has completed. Progress does not overwrite
                // a live slot while `ready` is set or credit is withheld until `consume()`.
                unsafe { std::slice::from_raw_parts(mr.as_ptr().add(base + HEADER), header.body) }
            }
        };
        Some((header, data))
    }

    pub fn consume(&mut self) {
        if let Some((_, _, slot)) = self.ready.take()
            && let Transport::Usb(usb) = &mut self.transport
            && usb.rdma_ready
        {
            if slot < USB_SMALL_SLOTS {
                usb.unacked_small = usb.unacked_small.saturating_add(1);
            } else if slot < CREDIT_SLOT_IDX {
                usb.unacked_bulk = usb.unacked_bulk.saturating_add(1);
            }
        }
        self.rx_len = 0;
        self.rx_goal = HEADER;
    }

    pub fn progress(&mut self) -> io::Result<()> {
        match &mut self.transport {
            Transport::Unix(socket) => {
                if self.tx_len != 0 {
                    self.tx_pos += try_io(socket.write(&self.tx[self.tx_pos..self.tx_len]))?;
                    if self.tx_pos == self.tx_len {
                        self.tx_len = 0;
                    }
                }
                if self.ready.is_none() {
                    while self.rx_len < self.rx_goal {
                        let read = try_io(socket.read(&mut self.rx[self.rx_len..self.rx_goal]))?;
                        if read == 0 {
                            break;
                        }
                        self.rx_len += read;
                        if self.rx_len == HEADER {
                            self.rx_goal = HEADER + Header::parse(&self.rx)?.body;
                            if self.rx_goal > UNIX_WINDOW {
                                return Err(invalid());
                            }
                        }
                    }
                    if self.rx_len == self.rx_goal {
                        self.ready = Some((Header::parse(&self.rx)?, 0, 0));
                    }
                }
            }
            Transport::Usb(usb) => {
                if usb.send_pending
                    && let Some(wc) = usb.send.poll_one().map_err(sdk)?
                {
                    if wc.status != WcStatus::Success {
                        return Err(invalid());
                    }
                    usb.send_pending = false;
                    if let Some(tag) = usb.active_tx_tag.take() {
                        usb.completed_tx_tag = Some(tag);
                    }
                    self.tx_len = 0;
                }
                if !usb.rdma_ready {
                    if !usb.hello_recv_pending && !usb.hello_received && self.ready.is_none() {
                        match usb
                            .pair
                            .post_recv(1, &usb.hello_rx, 0..HELLO_RX_CAP, HELLO_SDK_TAG)
                        {
                            Ok(()) => {
                                usb.hello_recv_pending = true;
                            }
                            Err(crate::Error::Exhausted) => {}
                            Err(error) => return Err(sdk(error)),
                        }
                    }
                    if usb.hello_recv_pending
                        && let Some(wc) = usb.recv.poll_one().map_err(sdk)?
                    {
                        if wc.status != WcStatus::Success
                            || wc.opcode != WcOpcode::Recv
                            || !(HEADER as u64..=HELLO_RX_CAP as u64).contains(&wc.byte_len)
                        {
                            return Err(invalid());
                        }
                        let len = wc.byte_len as usize;
                        let mut encoded = [0; HEADER];
                        usb.hello_rx.read_bytes(0, &mut encoded).map_err(sdk)?;
                        let header = Header::parse(&encoded)?;
                        if len != HEADER + header.body || header.kind != Kind::Hello {
                            return Err(invalid());
                        }
                        self.ready = Some((header, 0, 0));
                        usb.hello_recv_pending = false;
                        usb.hello_received = true;
                    }
                    if !usb.send_pending && self.tx_len != 0 {
                        match usb
                            .pair
                            .post_send(2, &usb.tx, 0..self.tx_len, HELLO_SDK_TAG)
                        {
                            Ok(()) => usb.send_pending = true,
                            Err(crate::Error::Exhausted) => {}
                            Err(error) => return Err(sdk(error)),
                        }
                    }
                } else {
                    while usb.posted_imm_recvs < STANDING_RECVS {
                        let wr = usb.next_imm_wr;
                        usb.next_imm_wr = wr.wrapping_add(1).max(100);
                        match usb.pair.post_standing_recv(wr, &usb.rx, 0..0, 0) {
                            Ok(()) => usb.posted_imm_recvs += 1,
                            Err(crate::Error::Exhausted) => break,
                            Err(error) => return Err(sdk(error)),
                        }
                    }
                    if !usb.ready_written && !usb.send_pending {
                        usb.tx
                            .write_bytes(0, &READY_MAGIC.to_le_bytes())
                            .map_err(sdk)?;
                        match usb.pair.post_rdma_write(
                            2,
                            &usb.tx,
                            0..8,
                            usb.peer_rkey,
                            READY_SLOT_OFFSET as u64,
                        ) {
                            Ok(()) => {
                                usb.send_pending = true;
                                usb.active_tx_buf = 0;
                                usb.active_tx_tag = None;
                                usb.ready_written = true;
                            }
                            Err(crate::Error::Exhausted) => {}
                            Err(error) => return Err(sdk(error)),
                        }
                    }
                    if !usb.peer_ready {
                        let mut flag = [0u8; 8];
                        usb.rx
                            .read_bytes(READY_SLOT_OFFSET, &mut flag)
                            .map_err(sdk)?;
                        if u64::from_le_bytes(flag) == READY_MAGIC {
                            usb.peer_ready = true;
                        }
                    }
                    while self.ready.is_none() {
                        let Some(wc) = usb.recv.poll_one().map_err(sdk)? else {
                            break;
                        };
                        usb.posted_imm_recvs = usb.posted_imm_recvs.saturating_sub(1);
                        if wc.status != WcStatus::Success
                            || wc.opcode != WcOpcode::RecvRdmaWithImm
                            || !wc.has_imm
                        {
                            return Err(invalid());
                        }
                        let imm = wc.tag_or_imm;
                        if imm & CREDIT_IMM_FLAG != 0 {
                            let ack_small = ((imm >> 8) & 0xFF) as usize;
                            let ack_bulk = (imm & 0xFF) as usize;
                            usb.small_credits =
                                (usb.small_credits + ack_small).min(USB_SMALL_SLOTS);
                            usb.bulk_credits = (usb.bulk_credits + ack_bulk).min(USB_BULK_SLOTS);
                            continue;
                        }
                        let slot = (imm >> 24) as usize;
                        let len = (imm & 0x00FF_FFFF) as usize;
                        let Some((slot_off, slot_cap)) = Usb::slot_bounds(slot) else {
                            return Err(invalid());
                        };
                        if len < HEADER || len > slot_cap {
                            return Err(invalid());
                        }
                        let mut encoded = [0; HEADER];
                        usb.rx.read_bytes(slot_off, &mut encoded).map_err(sdk)?;
                        let header = Header::parse(&encoded)?;
                        if len != HEADER + header.body {
                            return Err(invalid());
                        }
                        usb.small_credits =
                            (usb.small_credits + header.ack_small as usize).min(USB_SMALL_SLOTS);
                        usb.bulk_credits =
                            (usb.bulk_credits + header.ack_bulk as usize).min(USB_BULK_SLOTS);
                        if header.kind != Kind::Credit {
                            self.ready = Some((header, slot_off, slot));
                        }
                    }
                    if !usb.send_pending
                        && let Some(staged) = usb.staged
                    {
                        let base = staged.tx_buf_idx * BULK_STRIDE;
                        match usb.pair.post_rdma_write_imm(
                            2,
                            &usb.tx,
                            base..base + staged.len,
                            usb.peer_rkey,
                            staged.rx_offset as u64,
                            staged.imm,
                        ) {
                            Ok(()) => {
                                usb.send_pending = true;
                                usb.active_tx_buf = staged.tx_buf_idx;
                                usb.active_tx_tag = staged.send_tag;
                                usb.staged = None;
                            }
                            Err(crate::Error::Exhausted) => {}
                            Err(error) => return Err(sdk(error)),
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn try_io(result: io::Result<usize>) -> io::Result<usize> {
    match result {
        Ok(0) => Err(io::ErrorKind::UnexpectedEof.into()),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok(0)
        }
        result => result,
    }
}
