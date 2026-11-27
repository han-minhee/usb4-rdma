use crate::{Event, FastPoll, LinkCapabilities, StreamTransport};
use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use usb4_proto::{
    Error, Result,
    protocol::{CHUNK, FrameHeader, Kind, Packet},
};

const RING_CAP: usize = 128;

#[repr(C, align(64))]
struct AlignedIndex {
    val: AtomicUsize,
    _pad: [u8; 64 - std::mem::size_of::<AtomicUsize>()],
}

impl AlignedIndex {
    const fn new(v: usize) -> Self {
        Self {
            val: AtomicUsize::new(v),
            _pad: [0; 64 - std::mem::size_of::<AtomicUsize>()],
        }
    }
}

struct TxSlot {
    id: u64,
    header: FrameHeader,
    len: usize,
    payload: Box<[u8]>,
}

impl TxSlot {
    fn new() -> Self {
        Self {
            id: 0,
            header: FrameHeader::control(Kind::Ping, 0, 0, 0),
            len: 0,
            payload: vec![0u8; CHUNK].into_boxed_slice(),
        }
    }
}

#[derive(Clone, Copy)]
enum RxKind {
    Frame,
    Sent,
    Closed,
}

struct RxSlot {
    id: u64,
    kind: RxKind,
    header: FrameHeader,
    len: usize,
    payload: Box<[u8]>,
}

impl RxSlot {
    fn new() -> Self {
        Self {
            id: 0,
            kind: RxKind::Frame,
            header: FrameHeader::control(Kind::Ping, 0, 0, 0),
            len: 0,
            payload: vec![0u8; CHUNK].into_boxed_slice(),
        }
    }
}

struct SharedRings {
    tx_head: AlignedIndex,
    tx_tail: AlignedIndex,
    tx_slots: Box<[UnsafeCell<TxSlot>]>,
    rx_head: AlignedIndex,
    rx_tail: AlignedIndex,
    rx_slots: Box<[UnsafeCell<RxSlot>]>,
    worker_sleeping: AtomicBool,
    service_sleeping: AtomicBool,
    stop: AtomicBool,
    error: AtomicBool,
    cause: Mutex<Option<Error>>,
    tx_wake_fd: OwnedFd,
    rx_wake_fd: OwnedFd,
}

// SAFETY: SPSC protocol guarantees tx_slots are written only by the producer (Service)
// and read only by the consumer (Worker), and rx_slots vice versa, synchronized via
// Release/Acquire index updates; atomic flags and OwnedFds are thread-safe.
unsafe impl Sync for SharedRings {}

fn create_eventfd() -> Result<OwnedFd> {
    // SAFETY: valid eventfd flags; returned FD is newly owned if non-negative.
    let raw = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if raw < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: raw is a live newly created eventfd descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

fn wake_eventfd(fd: RawFd) {
    let val: u64 = 1;
    // SAFETY: val is initialized 8-byte stack buffer written to live eventfd.
    let _ = unsafe { libc::write(fd, (&val as *const u64).cast(), 8) };
}

fn drain_eventfd(fd: RawFd) {
    let mut val: u64 = 0;
    // SAFETY: val is initialized 8-byte stack buffer read from nonblocking eventfd.
    let _ = unsafe { libc::read(fd, (&mut val as *mut u64).cast(), 8) };
}

/// Offloads per-link `read()`, `write()`, `flush()`, and CRC32 encode/decode onto a
/// dedicated OS thread communicating over lock-free cache-aligned SPSC rings.
pub struct ThreadedLink {
    caps: LinkCapabilities,
    rings: Arc<SharedRings>,
    next_tx_id: u64,
    pool: usb4_proto::protocol::PacketPool,
    handle: Option<JoinHandle<()>>,
}

impl ThreadedLink {
    pub fn spawn(mut backend: Box<dyn StreamTransport>, spin_us: u64) -> Result<Self> {
        let caps = backend.capabilities();
        let tx_wake_fd = create_eventfd()?;
        let rx_wake_fd = create_eventfd()?;
        let tx_slots: Vec<UnsafeCell<TxSlot>> = (0..RING_CAP)
            .map(|_| UnsafeCell::new(TxSlot::new()))
            .collect();
        let rx_slots: Vec<UnsafeCell<RxSlot>> = (0..RING_CAP)
            .map(|_| UnsafeCell::new(RxSlot::new()))
            .collect();
        let rings = Arc::new(SharedRings {
            tx_head: AlignedIndex::new(0),
            tx_tail: AlignedIndex::new(0),
            tx_slots: tx_slots.into_boxed_slice(),
            rx_head: AlignedIndex::new(0),
            rx_tail: AlignedIndex::new(0),
            rx_slots: rx_slots.into_boxed_slice(),
            worker_sleeping: AtomicBool::new(false),
            service_sleeping: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            error: AtomicBool::new(false),
            cause: Mutex::new(None),
            tx_wake_fd,
            rx_wake_fd,
        });
        let r = rings.clone();
        let handle = thread::Builder::new()
            .name(format!("u4rd-link-{}", caps.peer))
            .spawn(move || {
                if let Err(e) = worker_loop(&mut *backend, &r, spin_us) {
                    *r.cause.lock().unwrap() = Some(e);
                    r.error.store(true, Ordering::Release);
                    wake_eventfd(r.rx_wake_fd.as_raw_fd());
                }
                let _ = backend.shutdown();
            })?;
        Ok(Self {
            caps,
            rings,
            next_tx_id: 1,
            pool: usb4_proto::protocol::PacketPool::new(RING_CAP),
            handle: Some(handle),
        })
    }
}

impl Drop for ThreadedLink {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn worker_loop(backend: &mut dyn StreamTransport, r: &SharedRings, spin_us: u64) -> Result<()> {
    let budget = Duration::from_micros(spin_us);
    let mut active = Instant::now();
    let link_fd = backend.raw_fd();
    let busy_poll = link_fd.is_none();
    let tx_wake = r.tx_wake_fd.as_raw_fd();
    let rx_wake = r.rx_wake_fd.as_raw_fd();
    let mut idle_spins: u32 = 0;
    let mut pending_ids = VecDeque::with_capacity(RING_CAP);

    while !r.stop.load(Ordering::Acquire) {
        let mut progressed = false;

        // 1. Drain TX SPSC slots into backend and flush kernel DMA ring.
        let mut tx_h = r.tx_head.val.load(Ordering::Relaxed);
        let tx_t = r.tx_tail.val.load(Ordering::Acquire);
        let mut submitted_any = false;
        while tx_h != tx_t && pending_ids.len() < RING_CAP && backend.can_submit_direct() {
            let idx = tx_h & (RING_CAP - 1);
            // SAFETY: Worker exclusively reads slots between tx_head and tx_tail.
            let slot = unsafe { &*r.tx_slots[idx].get() };
            let header = slot.header;
            let len = slot.len;
            let payload = &slot.payload[..len];
            let res = backend.try_submit_with(&header, &mut |out| {
                out.copy_from_slice(payload);
                Ok(())
            })?;
            let Some(id) = res else { break };
            pending_ids.push_back((id, slot.id));
            tx_h = tx_h.wrapping_add(1);
            submitted_any = true;
            progressed = true;
        }
        if submitted_any {
            r.tx_head.val.store(tx_h, Ordering::Release);
        }
        backend.flush()?;

        // Under busy_poll=1, a read() on an empty RX ring spins briefly in the kernel
        // driver; if more TX slots are already waiting, keep feeding TX first.
        if busy_poll
            && submitted_any
            && r.tx_tail.val.load(Ordering::Acquire) != tx_h
            && backend.can_submit_direct()
        {
            idle_spins = 0;
            active = Instant::now();
            continue;
        }

        // 2. Drain RX frames and sent completions from backend into RX SPSC.
        let mut rx_t = r.rx_tail.val.load(Ordering::Relaxed);
        let mut pushed_rx = false;
        for _ in 0..64 {
            let rx_h = r.rx_head.val.load(Ordering::Acquire);
            if rx_t.wrapping_sub(rx_h) >= RING_CAP {
                break;
            }
            let idx = rx_t & (RING_CAP - 1);
            // SAFETY: Worker exclusively writes free rx_slots[rx_tail] before publishing rx_tail.
            let slot = unsafe { &mut *r.rx_slots[idx].get() };
            let mut delivered_inline = false;
            let mut fast_res = None;
            let fast = backend.poll_fast(&mut |h, payload| {
                if payload.len() > CHUNK {
                    return Ok(false);
                }
                slot.kind = RxKind::Frame;
                slot.header = h;
                slot.len = payload.len();
                slot.payload[..payload.len()].copy_from_slice(payload);
                delivered_inline = true;
                Ok(true)
            })?;
            if !delivered_inline {
                fast_res = Some(fast);
            }
            if delivered_inline {
                rx_t = rx_t.wrapping_add(1);
                pushed_rx = true;
                progressed = true;
                continue;
            }
            match fast_res.unwrap() {
                FastPoll::None => break,
                FastPoll::Delivered => unreachable!(),
                FastPoll::Event(Event::Sent(id, bytes)) => {
                    let (expected, outer) = pending_ids
                        .pop_front()
                        .ok_or(Error::Protocol("unexpected link completion"))?;
                    if id != expected {
                        return Err(Error::Protocol("link completion order"));
                    }
                    slot.id = outer;
                    slot.kind = RxKind::Sent;
                    slot.len = bytes;
                    rx_t = rx_t.wrapping_add(1);
                    pushed_rx = true;
                    progressed = true;
                }
                FastPoll::Event(Event::Received(p)) => {
                    let d = p.data();
                    slot.kind = RxKind::Frame;
                    slot.header = p.header;
                    slot.len = d.len();
                    slot.payload[..d.len()].copy_from_slice(d);
                    rx_t = rx_t.wrapping_add(1);
                    pushed_rx = true;
                    progressed = true;
                }
                FastPoll::Event(Event::Closed) => {
                    slot.kind = RxKind::Closed;
                    rx_t = rx_t.wrapping_add(1);
                    pushed_rx = true;
                    break;
                }
            }
        }
        if pushed_rx {
            r.rx_tail.val.store(rx_t, Ordering::Release);
            if r.service_sleeping.load(Ordering::Acquire) {
                wake_eventfd(rx_wake);
            }
        }

        if progressed {
            idle_spins = 0;
            if r.tx_tail.val.load(Ordering::Acquire) == tx_h {
                active = Instant::now();
            }
            continue;
        }
        idle_spins = idle_spins.wrapping_add(1);
        if (idle_spins & 31) != 0 || active.elapsed() < budget {
            if !busy_poll {
                // Brief backoff so IRQ-driven streams do not hammer kernel read() -> mutex_trylock.
                for _ in 0..4 {
                    std::hint::spin_loop();
                    if r.tx_tail.val.load(Ordering::Acquire) != tx_h {
                        break;
                    }
                }
            } else {
                std::hint::spin_loop();
            }
            continue;
        }
        idle_spins = 0;

        // Brief ppoll wait on link RX + TX wakeup eventfd.
        r.worker_sleeping.store(true, Ordering::Release);
        if r.tx_head.val.load(Ordering::Acquire) != r.tx_tail.val.load(Ordering::Acquire)
            || r.stop.load(Ordering::Acquire)
        {
            r.worker_sleeping.store(false, Ordering::Relaxed);
            continue;
        }
        let mut fds = [
            libc::pollfd {
                fd: tx_wake,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: link_fd.unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let nfds = if link_fd.is_some() { 2 } else { 1 };
        let ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 25_000,
        };
        // SAFETY: fds has length nfds and ts is initialized.
        let ready = unsafe { libc::ppoll(fds.as_mut_ptr(), nfds, &ts, std::ptr::null()) };
        r.worker_sleeping.store(false, Ordering::Release);
        if ready < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(error.into());
            }
        }
        if ready > 0 && fds[1].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
            return Err(std::io::Error::from_raw_os_error(libc::EPIPE).into());
        }
        if ready > 0 && (fds[0].revents & libc::POLLIN) != 0 {
            drain_eventfd(tx_wake);
        }
        active = Instant::now();
    }
    Ok(())
}

impl StreamTransport for ThreadedLink {
    fn capabilities(&self) -> LinkCapabilities {
        self.caps
    }

    fn raw_fd(&self) -> Option<RawFd> {
        Some(self.rings.rx_wake_fd.as_raw_fd())
    }

    fn set_consumer_sleeping(&self, sleeping: bool) {
        self.rings
            .service_sleeping
            .store(sleeping, Ordering::Release);
        if !sleeping {
            drain_eventfd(self.rings.rx_wake_fd.as_raw_fd());
        }
    }

    fn can_submit_direct(&self) -> bool {
        if self.rings.error.load(Ordering::Acquire) {
            return false;
        }
        let h = self.rings.tx_head.val.load(Ordering::Acquire);
        let t = self.rings.tx_tail.val.load(Ordering::Relaxed);
        t.wrapping_sub(h) < RING_CAP
    }

    fn can_submit_data(&self) -> bool {
        let h = self.rings.tx_head.val.load(Ordering::Acquire);
        let t = self.rings.tx_tail.val.load(Ordering::Relaxed);
        !self.rings.error.load(Ordering::Acquire) && t.wrapping_sub(h) < RING_CAP - 8
    }

    fn try_submit(&mut self, p: &Packet) -> Result<Option<u64>> {
        self.try_submit_with(&p.header, &mut |out| {
            out.copy_from_slice(p.data());
            Ok(())
        })
    }

    fn try_submit_with(
        &mut self,
        header: &FrameHeader,
        fill: &mut dyn FnMut(&mut [u8]) -> Result<()>,
    ) -> Result<Option<u64>> {
        if self.rings.error.load(Ordering::Acquire) {
            return Err(self
                .rings
                .cause
                .lock()
                .unwrap()
                .take()
                .unwrap_or(Error::Disconnected));
        }
        let len = header.length as usize;
        if len > CHUNK {
            return Err(Error::Protocol("payload exceeds chunk"));
        }
        let h = self.rings.tx_head.val.load(Ordering::Acquire);
        let t = self.rings.tx_tail.val.load(Ordering::Relaxed);
        if t.wrapping_sub(h) >= RING_CAP || (header.kind.is_data_plane() && !self.can_submit_data())
        {
            return Ok(None);
        }
        let id = self.next_tx_id;
        self.next_tx_id = id.checked_add(1).ok_or(Error::Exhausted)?;
        let idx = t & (RING_CAP - 1);
        // SAFETY: Service exclusively writes tx_slots[tx_tail] while ring is non-full.
        let slot = unsafe { &mut *self.rings.tx_slots[idx].get() };
        slot.id = id;
        slot.header = *header;
        slot.len = len;
        fill(&mut slot.payload[..len])?;
        self.rings
            .tx_tail
            .val
            .store(t.wrapping_add(1), Ordering::Release);
        if self.rings.worker_sleeping.load(Ordering::Acquire) {
            wake_eventfd(self.rings.tx_wake_fd.as_raw_fd());
        }
        Ok(Some(id))
    }

    fn poll(&mut self) -> Result<Option<Event>> {
        match self.poll_fast(&mut |_, _| Ok(false))? {
            FastPoll::Event(e) => Ok(Some(e)),
            FastPoll::Delivered | FastPoll::None => Ok(None),
        }
    }

    fn poll_fast(
        &mut self,
        on_rx: &mut dyn FnMut(FrameHeader, &[u8]) -> Result<bool>,
    ) -> Result<FastPoll> {
        if self.rings.error.load(Ordering::Acquire) {
            return Err(self
                .rings
                .cause
                .lock()
                .unwrap()
                .take()
                .unwrap_or(Error::Disconnected));
        }
        let h = self.rings.rx_head.val.load(Ordering::Relaxed);
        let t = self.rings.rx_tail.val.load(Ordering::Acquire);
        if h == t {
            return Ok(FastPoll::None);
        }
        let idx = h & (RING_CAP - 1);
        // SAFETY: Service exclusively reads rx_slots[rx_head] between rx_head and rx_tail.
        let slot = unsafe { &*self.rings.rx_slots[idx].get() };
        let res = match slot.kind {
            RxKind::Frame => {
                if on_rx(slot.header, &slot.payload[..slot.len])? {
                    FastPoll::Delivered
                } else {
                    FastPoll::Event(Event::Received(
                        self.pool.packet(slot.header, &slot.payload[..slot.len])?,
                    ))
                }
            }
            RxKind::Sent => FastPoll::Event(Event::Sent(slot.id, slot.len)),
            RxKind::Closed => FastPoll::Event(Event::Closed),
        };
        self.rings
            .rx_head
            .val
            .store(h.wrapping_add(1), Ordering::Release);
        Ok(res)
    }

    fn flush(&mut self) -> Result<()> {
        if self.rings.worker_sleeping.load(Ordering::Acquire)
            && self.rings.tx_head.val.load(Ordering::Acquire)
                != self.rings.tx_tail.val.load(Ordering::Acquire)
        {
            wake_eventfd(self.rings.tx_wake_fd.as_raw_fd());
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<()> {
        self.rings.stop.store(true, Ordering::Release);
        wake_eventfd(self.rings.tx_wake_fd.as_raw_fd());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        Ok(())
    }
}
