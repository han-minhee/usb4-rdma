use super::rma::{self, Job as RmaJob, Request as RmaRequest};
use super::wire::{
    ADDRESS_BYTES, Address, EAGER_LIMIT, HELLO_BYTES, Header, Kind, Link, MAX_MESSAGE, Transport,
    Usb, invalid, sdk,
};
use crate::{
    AccessFlags, CompletionQueue, ConnectionRequest, Context, Listener, ProtectionDomain,
    host_memory::HostMemory,
};
use std::{
    collections::VecDeque,
    io,
    os::{
        fd::AsRawFd,
        linux::net::SocketAddrExt,
        unix::net::{SocketAddr, UnixListener, UnixStream},
    },
    path::Path,
    time::{Duration, Instant},
};

const PEERS: usize = 8;
const REQUESTS: usize = 256;
const CREDITS: usize = 32;
pub const ANY: u64 = u64::MAX;
pub const INJECT: usize = 256;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

pub const AGAIN: i32 = 1;
pub const BAD: i32 = 2;
pub const IO: i32 = 3;
pub const NO_MESSAGE: i32 = 4;
pub const CANCELED: i32 = 5;
pub const TRUNCATED: i32 = 6;
pub const ACCESS: i32 = 7;

#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct Completion {
    pub context: usize,
    pub address: u64,
    pub tag: u64,
    pub len: usize,
    pub overflow: usize,
    pub kind: u32,
    pub status: i32,
}

#[derive(Clone, Copy)]
pub struct Buffer {
    addr: usize,
    pub(crate) len: usize,
}

impl Buffer {
    /// Borrow host bytes for an asynchronous tagged or RMA operation.
    ///
    /// # Safety
    /// The caller retains the CPU mapping through all uses of this buffer. Reads
    /// and writes require the corresponding access and synchronization.
    pub unsafe fn from_raw(address: usize, len: usize) -> Option<Self> {
        if (address == 0 && len != 0)
            || len > isize::MAX as usize
            || address.checked_add(len).is_none()
        {
            None
        } else {
            Some(Self { addr: address, len })
        }
    }

    pub(crate) fn copy_from(self, offset: usize, bytes: &[u8]) {
        let count = self.len.saturating_sub(offset).min(bytes.len());
        if count != 0 {
            // SAFETY: OFI borrows this writable buffer until completion. The
            // slice comes from an owned transport buffer or injection buffer.
            unsafe { std::ptr::copy(bytes.as_ptr(), (self.addr as *mut u8).add(offset), count) };
        }
    }

    pub(crate) fn read(&self, offset: usize, len: usize) -> &[u8] {
        if len == 0 {
            return &[];
        }
        // SAFETY: callers retain read access to the submitted buffer until
        // completion. This slice is used only during a progress call.
        unsafe { std::slice::from_raw_parts((self.addr as *const u8).add(offset), len) }
    }
}

pub struct Send {
    id: u64,
    peer: usize,
    buffer: Buffer,
    tag: u64,
    tagged: bool,
    context: usize,
    notify: bool,
    inject: Option<[u8; INJECT]>,
    offered: bool,
    transmitted: bool,
    granted: bool,
    cursor: usize,
}

impl Send {
    fn bytes(&self, offset: usize, len: usize) -> &[u8] {
        match &self.inject {
            Some(bytes) => &bytes[offset..offset + len],
            None => self.buffer.read(offset, len),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Recv {
    pub source: u64,
    pub tag: u64,
    pub ignore: u64,
    pub tagged: bool,
    pub buffer: Buffer,
    pub context: usize,
    pub notify: bool,
}

#[derive(Clone, Copy)]
struct Offer {
    peer: usize,
    id: u64,
    tag: u64,
    tagged: bool,
    len: usize,
    claim: Option<usize>,
    eager_slot: Option<usize>,
}

impl Offer {
    fn matches(self, recv: Recv) -> bool {
        self.claim.is_none()
            && self.tagged == recv.tagged
            && (recv.source == ANY || recv.source == self.peer as u64)
            && ((self.tag ^ recv.tag) & !recv.ignore) == 0
    }
}

struct Incoming {
    offer: Offer,
    recv: Recv,
    cursor: usize,
}

struct Peer {
    address: Address,
    started: Option<Instant>,
    connected: bool,
    removed: bool,
    last_offer: u64,
    control: VecDeque<Header>,
    rma_reply: Option<Header>,
    reply_bytes: Box<[u8; EAGER_LIMIT]>,
}

struct Connection {
    peer: Option<usize>,
    send: CompletionQueue,
    recv: CompletionQueue,
    request: ConnectionRequest,
}

struct Session {
    link: Link,
    expected: Option<usize>,
    peer: Option<usize>,
    started: Instant,
    hello_sent: bool,
}

pub struct Endpoint {
    pub address: Address,
    pd: ProtectionDomain,
    sdk_listener: Listener,
    local_listener: UnixListener,
    context: Context,
    peers: Vec<Peer>,
    sessions: Vec<Option<Session>>,
    connecting: Vec<Connection>,
    next_send: u64,
    sends: VecDeque<Send>,
    memory: HostMemory,
    rma: VecDeque<RmaJob>,
    rma_scratch: Box<[u8; EAGER_LIMIT]>,
    posted: VecDeque<Recv>,
    offers: VecDeque<Offer>,
    eager_bytes: Box<[u8]>,
    free_eager_slots: Vec<usize>,
    incoming: Vec<Incoming>,
    pub completions: VecDeque<Completion>,
    pub failed: bool,
    pub enabled: bool,
}

impl Endpoint {
    pub fn new(socket: &Path) -> io::Result<Self> {
        let context = Context::open(socket).map_err(sdk)?;
        let pd = context.alloc_pd();
        let sdk_listener = pd.listen().map_err(sdk)?;
        let address = Address::new(sdk_listener.handle())?;
        let socket = SocketAddr::from_abstract_name(address.abstract_name())?;
        let local_listener = UnixListener::bind_addr(&socket)?;
        local_listener.set_nonblocking(true)?;
        Ok(Self {
            address,
            pd,
            sdk_listener,
            local_listener,
            context,
            peers: Vec::with_capacity(PEERS),
            sessions: Vec::with_capacity(PEERS),
            connecting: Vec::with_capacity(PEERS),
            next_send: 1,
            sends: VecDeque::with_capacity(REQUESTS),
            memory: HostMemory::new(256).map_err(sdk)?,
            rma: VecDeque::with_capacity(REQUESTS),
            rma_scratch: Box::new([0; EAGER_LIMIT]),
            posted: VecDeque::with_capacity(REQUESTS),
            offers: VecDeque::with_capacity(PEERS * CREDITS),
            eager_bytes: vec![0; PEERS * CREDITS * EAGER_LIMIT].into_boxed_slice(),
            free_eager_slots: (0..PEERS * CREDITS).rev().collect(),
            incoming: Vec::with_capacity(REQUESTS),
            completions: VecDeque::with_capacity(REQUESTS),
            failed: false,
            enabled: false,
        })
    }

    pub fn insert(&mut self, address: Address) -> Result<u64, i32> {
        if address.handle().map_err(|_| BAD)?.epoch != self.address.handle().map_err(|_| BAD)?.epoch
        {
            return Err(BAD);
        }
        if let Some(index) = self.peers.iter().position(|p| p.address == address) {
            self.peers[index].removed = false;
            return Ok(index as u64);
        }
        if self.peers.len() == PEERS {
            return Err(AGAIN);
        }
        self.peers.push(Peer {
            address,
            started: None,
            connected: address == self.address,
            removed: false,
            last_offer: 0,
            control: VecDeque::with_capacity(CREDITS * 2),
            rma_reply: None,
            reply_bytes: Box::new([0; EAGER_LIMIT]),
        });
        Ok((self.peers.len() - 1) as u64)
    }

    pub fn lookup(&self, address: u64) -> Option<Address> {
        self.peers
            .get(usize::try_from(address).ok()?)
            .filter(|p| !p.removed)
            .map(|p| p.address)
    }

    pub fn remove(&mut self, address: u64) -> Result<(), i32> {
        self.lookup(address).ok_or(BAD)?;
        self.peers[address as usize].removed = true;
        Ok(())
    }

    fn reserve(&self) -> Result<(), i32> {
        if self.failed {
            return Err(IO);
        }
        if !self.enabled {
            return Err(BAD);
        }
        if self.sends.len()
            + self.posted.len()
            + self.incoming.len()
            + self.completions.len()
            + self.rma.len()
            >= REQUESTS
        {
            return Err(AGAIN);
        }
        Ok(())
    }

    pub fn send(&mut self, dst: u64, operation: Recv, inject: bool) -> Result<(), i32> {
        self.reserve()?;
        self.lookup(dst).ok_or(BAD)?;
        let len = operation.buffer.len;
        if len > MAX_MESSAGE || inject && len > INJECT {
            return Err(BAD);
        }
        let bytes = inject.then(|| {
            let mut data = [0; INJECT];
            data[..len].copy_from_slice(operation.buffer.read(0, len));
            data
        });
        let id = self.next_send;
        self.next_send = id.checked_add(1).ok_or(IO)?;
        self.sends.push_back(Send {
            id,
            peer: dst as usize,
            buffer: operation.buffer,
            tag: operation.tag,
            tagged: operation.tagged,
            context: operation.context,
            notify: operation.notify && !inject,
            inject: bytes,
            offered: false,
            transmitted: false,
            granted: false,
            cursor: 0,
        });
        Ok(())
    }

    /// Register a writable or readable host mapping.
    ///
    /// # Safety
    /// The caller owns and retains the CPU allocation until deregistration.
    pub unsafe fn register(
        &mut self,
        address: *mut u8,
        len: usize,
        rights: u32,
    ) -> Result<u64, i32> {
        let address = std::ptr::NonNull::new(address).ok_or(BAD)?;
        let access = AccessFlags::from_bits(rights).map_err(|_| BAD)?;
        // SAFETY: the libfabric registration contract retains this host range through deregistration.
        unsafe { self.memory.register(address, len, access) }.map_err(|error| match error {
            crate::Error::Exhausted => AGAIN,
            crate::Error::InvalidArg(_) => BAD,
            _ => IO,
        })
    }

    pub fn deregister(&mut self, key: u64) -> Result<(), i32> {
        self.memory.deregister(key).map_err(|_| BAD)
    }

    pub fn post_rma(&mut self, peer: u64, request: RmaRequest) -> Result<(), i32> {
        self.reserve()?;
        self.lookup(peer).ok_or(BAD)?;
        if request.buffer.len > MAX_MESSAGE
            || request
                .offset
                .checked_add(request.buffer.len as u64)
                .is_none()
        {
            return Err(BAD);
        }
        let id = self.next_send;
        self.next_send = id.checked_add(1).ok_or(IO)?;
        self.rma.push_back(RmaJob::new(id, peer as usize, request));
        Ok(())
    }

    fn finish_rma(&mut self, index: usize, status: i32) {
        if let Some(completion) = self
            .rma
            .remove(index)
            .and_then(|job| job.completion(status))
        {
            self.completions.push_back(completion);
        }
    }

    pub fn recv(&mut self, recv: Recv, peek: bool, claim: bool, discard: bool) -> Result<(), i32> {
        self.reserve()?;
        if recv.source != ANY {
            self.lookup(recv.source).ok_or(BAD)?;
        }
        if recv.buffer.len > MAX_MESSAGE
            || discard && !(peek || claim)
            || claim && recv.context == 0
        {
            return Err(BAD);
        }
        if peek {
            if claim && self.offers.iter().any(|o| o.claim == Some(recv.context)) {
                return Err(BAD);
            }
            if let Some(index) = self.offers.iter().position(|o| o.matches(recv)) {
                let offer = self.offers[index];
                if discard {
                    self.offers.remove(index);
                    self.done(offer).map_err(|_| IO)?;
                } else if claim {
                    self.offers[index].claim = Some(recv.context);
                }
                self.completions
                    .push_back(recv_completion(recv, offer, 0, true));
            } else {
                self.completions.push_back(Completion {
                    context: recv.context,
                    kind: recv_kind(recv.tagged),
                    status: NO_MESSAGE,
                    ..Default::default()
                });
            }
        } else if claim {
            let index = self
                .offers
                .iter()
                .position(|o| o.claim == Some(recv.context))
                .ok_or(BAD)?;
            let offer = self.offers.remove(index).expect("offer exists");
            if discard {
                self.done(offer).map_err(|_| IO)?;
                self.completions
                    .push_back(recv_completion(recv, offer, 0, true));
            } else {
                self.start_receive(offer, recv).map_err(|_| IO)?;
            }
        } else {
            self.posted.push_back(recv);
        }
        Ok(())
    }

    pub fn cancel(&mut self, context: usize) -> Result<(), i32> {
        let index = self
            .posted
            .iter()
            .position(|r| r.context == context)
            .ok_or(NO_MESSAGE)?;
        let recv = self.posted.remove(index).expect("posted receive exists");
        self.completions.push_back(Completion {
            context,
            status: CANCELED,
            kind: recv_kind(recv.tagged),
            ..Default::default()
        });
        Ok(())
    }

    pub fn progress(&mut self) {
        if !self.enabled || self.failed {
            return;
        }
        if let Err(error) = self.drive() {
            eprintln!("usb4 OFI progress: {error}");
            self.failed = true;
            self.completions
                .extend(self.rma.drain(..).filter_map(|job| job.completion(IO)));
            for send in self.sends.drain(..).filter(|send| !send.transmitted) {
                self.completions.push_back(send_completion(&send, IO));
            }
            for recv in self
                .posted
                .drain(..)
                .chain(self.incoming.drain(..).map(|i| i.recv))
            {
                self.completions.push_back(Completion {
                    context: recv.context,
                    kind: recv_kind(recv.tagged),
                    status: IO,
                    ..Default::default()
                });
            }
        }
    }

    fn drive(&mut self) -> io::Result<()> {
        self.connect()?;
        for index in 0..self.sessions.len() {
            let Some(mut session) = self.sessions[index].take() else {
                continue;
            };
            session.link.progress()?;
            if let Some(id) = session.link.take_sent_tag()
                && let Some(send) = self.sends.iter_mut().find(|s| s.id == id && s.notify)
            {
                // One-sided RDMA_WRITE_WITH_IMM placed the payload directly in
                // the peer's receive MR; notify the caller while Done returns credit.
                self.completions.push_back(send_completion(send, 0));
                send.transmitted = true;
            }
            if let Some((header, body)) = session.link.incoming() {
                if let Some(peer) = session.peer {
                    if session.link.is_rdma_ready() {
                        self.inbound(peer, header, body)?;
                        session.link.consume();
                    }
                } else if let Some((peer, rkey)) = self.verify_hello(&session, header, body)? {
                    session.peer = Some(peer);
                    session.link.set_peer_rkey(rkey);
                    self.peers[peer].connected = true;
                    session.link.consume();
                }
            }
            if session.peer.is_none() && session.started.elapsed() > CONNECT_TIMEOUT {
                return Err(io::ErrorKind::TimedOut.into());
            }
            let is_usb = session.link.usb_node().is_some();
            if !session.hello_sent && session.link.can_stage() {
                let mut hello = [0u8; HELLO_BYTES];
                hello[..ADDRESS_BYTES].copy_from_slice(&self.address.0);
                hello[ADDRESS_BYTES..HELLO_BYTES]
                    .copy_from_slice(&session.link.local_rx_rkey().to_le_bytes());
                session.link.stage(
                    Header {
                        body: HELLO_BYTES,
                        ..Header::control(Kind::Hello, 0, 0)
                    },
                    &hello,
                )?;
                session.hello_sent = true;
            }
            if is_usb && !session.link.is_rdma_ready() {
                if session.hello_sent && session.peer.is_some() && session.link.idle() {
                    session.link.enable_rdma()?;
                }
                session.link.progress()?;
            }
            if session.link.is_rdma_ready()
                && let Some(peer) = session.peer
            {
                if session.link.can_stage() {
                    self.schedule(peer, &mut session.link)?;
                }
                session.link.progress()?;
            }
            self.sessions[index] = Some(session);
        }

        for i in 0..self.peers.len() {
            if self.peers[i].address == self.address {
                let rma_index = self.rma.iter().position(|job| job.peer == i);
                let rma_id = rma_index.map_or(u64::MAX, |index| self.rma[index].id);
                if let Some(index) = rma_index
                    && !self
                        .sends
                        .iter()
                        .any(|send| send.peer == i && send.id < rma_id)
                {
                    let job = &mut self.rma[index];
                    if let Some(header) = job.prepare(&mut self.rma_scratch) {
                        let slave = &mut self.peers[i].reply_bytes;
                        let result = rma::target(
                            &mut self.memory,
                            header,
                            &self.rma_scratch[..header.body],
                            slave,
                        )?;
                        if job.complete_chunk(result, &slave[..result.body])? {
                            self.finish_rma(index, result.value as i32);
                        }
                    }
                }
                let available =
                    CREDITS.saturating_sub(self.offers.iter().filter(|o| o.peer == i).count());
                for send in self
                    .sends
                    .iter_mut()
                    .filter(|s| s.peer == i && !s.offered && s.id < rma_id)
                    .take(available)
                {
                    send.offered = true;
                    self.offers.push_back(Offer {
                        peer: i,
                        id: send.id,
                        tag: send.tag,
                        tagged: send.tagged,
                        len: send.buffer.len,
                        claim: None,
                        eager_slot: None,
                    });
                }
            }
        }
        while let Some((rq, msg)) = self.posted.iter().enumerate().find_map(|(rq, r)| {
            self.offers
                .iter()
                .position(|m| m.matches(*r))
                .map(|msg| (rq, msg))
        }) {
            let recv = self.posted.remove(rq).expect("posted receive exists");
            let offer = self.offers.remove(msg).expect("offer exists");
            self.start_receive(offer, recv)?;
        }
        Ok(())
    }

    fn connect(&mut self) -> io::Result<()> {
        let mine = self.address.handle()?;
        for index in 0..self.peers.len() {
            let addr = self.peers[index].address;
            if addr == self.address || self.peers[index].connected || self.peers[index].removed {
                continue;
            }
            if let Some(started) = self.peers[index].started {
                if started.elapsed() > CONNECT_TIMEOUT {
                    return Err(io::ErrorKind::TimedOut.into());
                }
                continue;
            }
            if self.address < addr {
                if addr.handle()?.node_id == mine.node_id {
                    let socket = UnixStream::connect_addr(&SocketAddr::from_abstract_name(
                        addr.abstract_name(),
                    )?)?;
                    self.session(Transport::Unix(check_local(socket)?), Some(index))?;
                } else {
                    let send = self.context.create_cq().map_err(sdk)?;
                    let recv = self.context.create_cq().map_err(sdk)?;
                    let request = self
                        .pd
                        .begin_connect(&addr.handle()?, &send, &recv)
                        .map_err(sdk)?;
                    self.connecting.push(Connection {
                        peer: Some(index),
                        send,
                        recv,
                        request,
                    });
                }
            }
            self.peers[index].started = Some(Instant::now());
        }

        let expected = self
            .peers
            .iter()
            .filter(|p| {
                !p.connected
                    && !p.removed
                    && p.address < self.address
                    && p.address.handle().is_ok_and(|h| h.node_id != mine.node_id)
            })
            .count();
        let accepting = self.connecting.iter().filter(|c| c.peer.is_none()).count();
        let identifying = self
            .sessions
            .iter()
            .flatten()
            .filter(|s| s.expected.is_none() && s.peer.is_none() && s.link.usb_node().is_some())
            .count();
        for _ in accepting + identifying..expected {
            let send = self.context.create_cq().map_err(sdk)?;
            let recv = self.context.create_cq().map_err(sdk)?;
            let request = self.sdk_listener.begin_accept(&send, &recv).map_err(sdk)?;
            self.connecting.push(Connection {
                peer: None,
                send,
                recv,
                request,
            });
        }
        let mut i = 0;
        while i < self.connecting.len() {
            if let Some(pair) = self.connecting[i].request.poll().map_err(sdk)? {
                let pending = self.connecting.swap_remove(i);
                let usb = Usb::new(&self.pd, pair, pending.send, pending.recv)?;
                self.session(Transport::Usb(Box::new(usb)), pending.peer)?;
            } else {
                i += 1;
            }
        }
        if self.sessions.len() < PEERS {
            match self.local_listener.accept() {
                Ok((socket, _)) => self.session(Transport::Unix(check_local(socket)?), None)?,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn session(&mut self, transport: Transport, expected: Option<usize>) -> io::Result<()> {
        if self.sessions.len() == PEERS {
            return Err(invalid());
        }
        self.sessions.push(Some(Session {
            link: Link::new(transport),
            expected,
            peer: None,
            started: Instant::now(),
            hello_sent: false,
        }));
        Ok(())
    }

    fn verify_hello(
        &self,
        session: &Session,
        h: Header,
        body: &[u8],
    ) -> io::Result<Option<(usize, u64)>> {
        if h.kind != Kind::Hello
            || h.id != 0
            || h.value != 0
            || h.tagged
            || body.len() != HELLO_BYTES
        {
            return Err(invalid());
        }
        let address = Address::parse(body[..ADDRESS_BYTES].try_into().map_err(|_| invalid())?)?;
        let peer_rkey = u64::from_le_bytes(body[ADDRESS_BYTES..HELLO_BYTES].try_into().unwrap());
        let handle = address.handle()?;
        let mine = self.address.handle()?;
        if handle.epoch != mine.epoch
            || match session.link.usb_node() {
                Some(node) => node != handle.node_id || node == mine.node_id,
                None => handle.node_id != mine.node_id,
            }
        {
            return Err(invalid());
        }
        let Some(peer) = self
            .peers
            .iter()
            .position(|p| p.address == address && !p.removed)
        else {
            return Ok(None);
        };
        if self.peers[peer].connected
            || match session.expected {
                Some(expected) => expected != peer || self.address >= address,
                None => address >= self.address,
            }
        {
            return Err(invalid());
        }
        Ok(Some((peer, peer_rkey)))
    }

    fn schedule(&mut self, peer: usize, link: &mut Link) -> io::Result<()> {
        if link.can_send_small()
            && let Some(header) = self.peers[peer].control.pop_front()
        {
            link.stage(header, &[])?;
            return Ok(());
        }
        if link.needs_credit_return() {
            link.stage(Header::control(Kind::Credit, 0, 0), &[])?;
            return Ok(());
        }
        if link.can_send_small()
            && let Some(header) = self.peers[peer].rma_reply.take()
        {
            link.stage(header, &self.peers[peer].reply_bytes[..header.body])?;
            return Ok(());
        }
        let rma = self.rma.iter_mut().find(|job| job.peer == peer);
        let rma_id = rma.as_ref().map_or(u64::MAX, |job| job.id);
        if link.can_send_small()
            && let Some(job) = rma
            && !self
                .sends
                .iter()
                .any(|send| send.peer == peer && send.id < job.id)
            && let Some(header) = job.prepare(&mut self.rma_scratch)
        {
            link.stage(header, &self.rma_scratch[..header.body])?;
            return Ok(());
        }
        if link.can_send_bulk()
            && let Some(send) = self
                .sends
                .iter_mut()
                .find(|s| s.peer == peer && s.granted && s.cursor < s.buffer.len)
        {
            let len = link.payload_capacity().min(send.buffer.len - send.cursor);
            let done_offset = send.cursor + len;
            let tag = (send.notify && done_offset == send.buffer.len && link.usb_node().is_some())
                .then_some(send.id);
            link.stage_tagged(
                Header {
                    body: len,
                    ..Header::control(Kind::Data, send.id, send.cursor as u64)
                },
                send.bytes(send.cursor, len),
                tag,
            )?;
            send.cursor = done_offset;
            return Ok(());
        }
        if link.can_send_small()
            && self
                .sends
                .iter()
                .filter(|s| s.peer == peer && s.offered)
                .count()
                < CREDITS
            && let Some(send) = self
                .sends
                .iter_mut()
                .find(|s| s.peer == peer && !s.offered && s.id < rma_id)
        {
            let len = send.buffer.len;
            if len <= EAGER_LIMIT {
                let tag = (send.notify && link.usb_node().is_some()).then_some(send.id);
                link.stage_tagged(
                    Header {
                        kind: Kind::Eager,
                        id: send.id,
                        value: send.tag,
                        tagged: send.tagged,
                        body: len,
                        ..Header::control(Kind::Eager, send.id, send.tag)
                    },
                    send.bytes(0, len),
                    tag,
                )?;
                send.offered = true;
                send.granted = true;
                send.cursor = len;
            } else {
                link.stage(
                    Header {
                        kind: Kind::Offer,
                        id: send.id,
                        value: send.tag,
                        tagged: send.tagged,
                        body: 8,
                        ..Header::control(Kind::Offer, send.id, send.tag)
                    },
                    &(len as u64).to_le_bytes(),
                )?;
                send.offered = true;
            }
        }
        Ok(())
    }

    fn inbound(&mut self, peer: usize, h: Header, body: &[u8]) -> io::Result<()> {
        match h.kind {
            Kind::Offer | Kind::Eager => {
                let len = if h.kind == Kind::Eager {
                    body.len() as u64
                } else {
                    u64::from_le_bytes(body.try_into().map_err(|_| invalid())?)
                };
                let outstanding = self.offers.iter().filter(|o| o.peer == peer).count()
                    + self
                        .incoming
                        .iter()
                        .filter(|i| i.offer.peer == peer)
                        .count();
                if len > MAX_MESSAGE as u64
                    || h.id <= self.peers[peer].last_offer
                    || outstanding >= CREDITS
                {
                    return Err(invalid());
                }
                let eager_slot = if h.kind == Kind::Eager {
                    let slot = self.free_eager_slots.pop().ok_or_else(|| invalid())?;
                    let start = slot * EAGER_LIMIT;
                    self.eager_bytes[start..start + body.len()].copy_from_slice(body);
                    Some(slot)
                } else {
                    None
                };
                self.peers[peer].last_offer = h.id;
                self.offers.push_back(Offer {
                    peer,
                    id: h.id,
                    tag: h.value,
                    tagged: h.tagged,
                    len: len as usize,
                    claim: None,
                    eager_slot,
                });
            }
            Kind::Grant => {
                let send = self
                    .sends
                    .iter_mut()
                    .find(|s| s.peer == peer && s.id == h.id && s.offered && !s.granted)
                    .ok_or_else(|| invalid())?;
                if h.value != send.buffer.len as u64 {
                    return Err(invalid());
                }
                send.granted = true;
            }
            Kind::Done => {
                let index = self
                    .sends
                    .iter()
                    .position(|s| s.peer == peer && s.id == h.id && s.offered)
                    .ok_or_else(|| invalid())?;
                let send = self.sends.remove(index).expect("send exists");
                if send.granted && send.cursor != send.buffer.len || h.value != 0 {
                    return Err(invalid());
                }
                if send.notify && !send.transmitted {
                    self.completions.push_back(send_completion(&send, 0));
                }
            }
            Kind::Data => {
                let index = self
                    .incoming
                    .iter()
                    .position(|i| i.offer.peer == peer && i.offer.id == h.id)
                    .ok_or_else(|| invalid())?;
                let incoming = &mut self.incoming[index];
                if h.value != incoming.cursor as u64
                    || body.len() > incoming.offer.len - incoming.cursor
                {
                    return Err(invalid());
                }
                incoming.recv.buffer.copy_from(incoming.cursor, body);
                incoming.cursor += body.len();
                if incoming.cursor == incoming.offer.len {
                    let incoming = self.incoming.swap_remove(index);
                    self.done(incoming.offer)?;
                    self.complete_receive(incoming.recv, incoming.offer);
                }
            }
            Kind::RmaRead | Kind::RmaWrite => {
                let peer = &mut self.peers[peer];
                if peer.rma_reply.is_some() {
                    return Err(invalid());
                }
                peer.rma_reply = Some(rma::target(
                    &mut self.memory,
                    h,
                    body,
                    &mut peer.reply_bytes,
                )?);
            }
            Kind::RmaResult => {
                let index = self
                    .rma
                    .iter()
                    .position(|job| job.peer == peer && job.id == h.id)
                    .ok_or_else(invalid)?;
                if self.rma[index].complete_chunk(h, body)? {
                    self.finish_rma(index, h.value as i32);
                }
            }
            Kind::Hello | Kind::HelloAck | Kind::Credit => return Err(invalid()),
        }
        Ok(())
    }

    fn start_receive(&mut self, offer: Offer, recv: Recv) -> io::Result<()> {
        if self.peers[offer.peer].address == self.address {
            let index = self
                .sends
                .iter()
                .position(|s| s.id == offer.id)
                .ok_or_else(|| invalid())?;
            let send = self.sends.remove(index).expect("local send exists");
            recv.buffer
                .copy_from(0, send.bytes(0, send.buffer.len.min(recv.buffer.len)));
            if send.notify {
                self.completions.push_back(send_completion(&send, 0));
            }
            self.complete_receive(recv, offer);
        } else if let Some(slot) = offer.eager_slot {
            let start = slot * EAGER_LIMIT;
            recv.buffer
                .copy_from(0, &self.eager_bytes[start..start + offer.len]);
            self.done(offer)?;
            self.complete_receive(recv, offer);
        } else if offer.len == 0 {
            self.done(offer)?;
            self.complete_receive(recv, offer);
        } else {
            self.control(
                offer.peer,
                Header::control(Kind::Grant, offer.id, offer.len as u64),
            )?;
            self.incoming.push(Incoming {
                offer,
                recv,
                cursor: 0,
            });
        }
        Ok(())
    }

    fn done(&mut self, offer: Offer) -> io::Result<()> {
        if self.peers[offer.peer].address == self.address {
            let index = self
                .sends
                .iter()
                .position(|s| s.peer == offer.peer && s.id == offer.id)
                .ok_or_else(|| invalid())?;
            let send = self.sends.remove(index).expect("local send exists");
            if send.notify {
                self.completions.push_back(send_completion(&send, 0));
            }
        } else {
            self.control(offer.peer, Header::control(Kind::Done, offer.id, 0))?;
        }
        if let Some(slot) = offer.eager_slot {
            self.free_eager_slots.push(slot);
        }
        Ok(())
    }

    fn control(&mut self, peer: usize, header: Header) -> io::Result<()> {
        let pending = &mut self.peers[peer].control;
        if pending.len() >= 2 * CREDITS {
            return Err(invalid());
        }
        pending.push_back(header);
        Ok(())
    }

    fn complete_receive(&mut self, recv: Recv, offer: Offer) {
        let overflow = offer.len.saturating_sub(recv.buffer.len);
        if recv.notify || overflow > 0 {
            self.completions
                .push_back(recv_completion(recv, offer, overflow, false));
        }
    }
}

fn check_local(socket: UnixStream) -> io::Result<UnixStream> {
    // SAFETY: SO_PEERCRED receives a fixed-size Linux credential structure
    // from the live socket and does not retain the output pointer.
    let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
    let mut length = size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: both output arguments point to initialized objects of the declared size.
    if unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut length,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: geteuid has no preconditions.
    if length as usize != size_of::<libc::ucred>() || credentials.uid != unsafe { libc::geteuid() }
    {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    socket.set_nonblocking(true)?;
    Ok(socket)
}

fn recv_kind(tagged: bool) -> u32 {
    if tagged { 3 } else { 1 }
}

fn recv_completion(recv: Recv, offer: Offer, overflow: usize, peek: bool) -> Completion {
    Completion {
        context: recv.context,
        address: offer.peer as u64,
        tag: offer.tag,
        len: if peek {
            offer.len
        } else {
            offer.len - overflow
        },
        overflow,
        kind: recv_kind(recv.tagged),
        status: if overflow > 0 { TRUNCATED } else { 0 },
    }
}

fn send_completion(send: &Send, status: i32) -> Completion {
    Completion {
        context: send.context,
        address: send.peer as u64,
        tag: send.tag,
        len: send.buffer.len,
        kind: if send.tagged { 4 } else { 2 },
        status,
        ..Default::default()
    }
}
