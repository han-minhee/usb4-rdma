//! Registered-buffer `io_uring` adapter. Kernel stream copies still occur on USB4STREAM.
use crate::{Event, FastPoll, LinkCapabilities, StreamTransport};
use io_uring::{IoUring, opcode, types};
use std::{
    collections::VecDeque,
    fs::File,
    ops::Range,
    os::fd::{AsRawFd, RawFd},
    ptr::NonNull,
    sync::Arc,
    time::{Duration, Instant},
};
use usb4_proto::{
    Error, Result,
    framer::Framer,
    protocol::{FrameHeader, HEADER, MAX_FRAME, Packet, PacketPool},
};

const TX_CAP: usize = 16;

// The pointer is stable and the allocation is never borrowed as a whole while the
// kernel owns a range. Only the ring owner accesses the data; extra Arcs are leases.
struct RegisteredBuffer {
    pointer: NonNull<u8>,
    size: usize,
    _allocation: std::cell::UnsafeCell<Box<[u8]>>,
}

// SAFETY: the allocation may move between threads with its single reactor owner.
unsafe impl Send for RegisteredBuffer {}
// SAFETY: all byte access is unsafe and serialized by the reactor and its CQE state.
unsafe impl Sync for RegisteredBuffer {}

impl RegisteredBuffer {
    fn new(size: usize) -> Arc<Self> {
        let mut allocation = vec![0; size].into_boxed_slice();
        let pointer = NonNull::new(allocation.as_mut_ptr()).expect("non-null allocation");
        Arc::new(Self {
            pointer,
            size,
            _allocation: std::cell::UnsafeCell::new(allocation),
        })
    }

    unsafe fn bytes(&self) -> &[u8] {
        // SAFETY: the caller excludes concurrent writers for the entire borrow.
        unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), self.size) }
    }

    unsafe fn mutate<T>(&self, range: Range<usize>, f: impl FnOnce(&mut [u8]) -> T) -> T {
        assert!(range.end <= self.size);
        // SAFETY: the caller excludes conflicting access to this initialized range.
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(self.pointer.as_ptr().add(range.start), range.len())
        };
        f(bytes)
    }
}

pub struct UringLink {
    pool: PacketPool,
    ring: Option<IoUring>,
    file: File,
    tx: Option<Arc<RegisteredBuffer>>,
    rx: Option<Arc<RegisteredBuffer>>,
    caps: LinkCapabilities,
    tx_len: usize,
    tx_pos: usize,
    framer: Framer,
    active: [bool; 4],
    leases: [Option<Arc<RegisteredBuffer>>; 2],
    wait: [bool; 2],
    next: u64,
    head_written: usize,
    pending: VecDeque<(u64, usize)>,
    completed: VecDeque<(u64, usize)>,
    closing: bool,
    closed: bool,
}

impl UringLink {
    pub fn with_receive_capacity(mut self, capacity: usize) -> Self {
        self.pool = PacketPool::new(capacity);
        self
    }

    pub fn open(path: &std::path::Path, caps: LinkCapabilities, sqpoll: bool) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(path)?;
        crate::claim_device(&f)?;
        Self::from_file(f, caps, sqpoll)
    }

    pub fn from_file(file: File, mut caps: LinkCapabilities, sqpoll: bool) -> Result<Self> {
        // SAFETY: FD remains owned; fcntl updates its file-status flags.
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: live FD and validated existing flags.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut builder = IoUring::builder();
        if sqpoll {
            builder.setup_sqpoll(2000);
        }
        let ring = builder.build(32)?;
        let tx = RegisteredBuffer::new(MAX_FRAME * TX_CAP);
        let rx = RegisteredBuffer::new(MAX_FRAME * 16);
        let iov = [
            libc::iovec {
                iov_base: tx.pointer.as_ptr().cast(),
                iov_len: tx.size,
            },
            libc::iovec {
                iov_base: rx.pointer.as_ptr().cast(),
                iov_len: rx.size,
            },
        ];
        // SAFETY: boxed allocations are stable and retained until every I/O is
        // canceled/drained; quarantine in Drop preserves them on shutdown failure.
        unsafe { ring.submitter().register_buffers(&iov) }?;
        caps.fixed_buffers = true;
        caps.sqpoll = sqpoll;
        Ok(Self {
            pool: PacketPool::new(64),
            ring: Some(ring),
            file,
            tx: Some(tx),
            rx: Some(rx),
            caps,
            tx_len: 0,
            tx_pos: 0,
            framer: Framer::default(),
            active: [false; 4],
            leases: [None, None],
            wait: [false; 2],
            next: 1,
            head_written: 0,
            pending: VecDeque::with_capacity(TX_CAP),
            completed: VecDeque::with_capacity(TX_CAP),
            closing: false,
            closed: false,
        })
    }

    fn has_tx_space(&mut self, needed: usize) -> bool {
        if self.closing || self.closed || self.pending.len() + self.completed.len() >= TX_CAP {
            return false;
        }
        let tx = self.tx.as_ref().unwrap();
        if self.tx_len + needed <= tx.size {
            return true;
        }
        if !self.active[1] && self.tx_pos > 0 {
            // SAFETY: no write SQE is active, and the reactor owns the entire TX allocation.
            unsafe {
                tx.mutate(0..tx.size, |bytes| {
                    bytes.copy_within(self.tx_pos..self.tx_len, 0)
                })
            };
            self.tx_len -= self.tx_pos;
            self.tx_pos = 0;
        }
        self.tx_len + needed <= tx.size
    }

    fn push(&mut self, entry: io_uring::squeue::Entry, index: usize) -> Result<()> {
        let ring = self.ring.as_mut().ok_or(Error::Disconnected)?;
        // SAFETY: entry pointers reference registered, stable buffers. Single owner
        // serializes the SQ and never reuses an active read/write buffer.
        unsafe { ring.submission().push(&entry) }.map_err(|_| Error::Exhausted)?;
        self.active[index] = true;
        match index {
            0 => self.leases[0] = self.rx.clone(),
            1 => self.leases[1] = self.tx.clone(),
            _ => {}
        }
        Ok(())
    }

    fn schedule(&mut self) -> Result<()> {
        if self.closing || self.closed {
            return Ok(());
        }
        let mut pushed = false;
        let fd = types::Fd(self.file.as_raw_fd());
        if !self.active[0] && !self.active[2] {
            if self.wait[0] {
                self.push(
                    opcode::PollAdd::new(fd, libc::POLLIN as u32)
                        .build()
                        .user_data(3),
                    2,
                )?;
                pushed = true;
            } else {
                let rx = self.rx.as_ref().unwrap();
                // SAFETY: this path has no active RX operation and no borrowed frames.
                unsafe { rx.mutate(0..rx.size, |bytes| self.framer.compact(bytes)) };
                let end = self.framer.end();
                let capacity = rx.size;
                let ptr = rx.pointer.as_ptr();
                // SAFETY: end is within this registered buffer and no read is active.
                let ptr = unsafe { ptr.add(end) };
                if capacity > end {
                    self.push(
                        opcode::ReadFixed::new(fd, ptr, (capacity - end) as u32, 1)
                            .offset(u64::MAX)
                            .build()
                            .user_data(1),
                        0,
                    )?;
                    pushed = true;
                }
            }
        }
        if self.tx_pos < self.tx_len && !self.active[1] && !self.active[3] {
            if self.wait[1] {
                self.push(
                    opcode::PollAdd::new(fd, libc::POLLOUT as u32)
                        .build()
                        .user_data(4),
                    3,
                )?;
                pushed = true;
            } else {
                let ptr = self.tx.as_ref().unwrap().pointer.as_ptr();
                // SAFETY: valid initialized TX range is retained until write completion.
                let ptr = unsafe { ptr.add(self.tx_pos) };
                self.push(
                    opcode::WriteFixed::new(fd, ptr, (self.tx_len - self.tx_pos) as u32, 0)
                        .offset(u64::MAX)
                        .build()
                        .user_data(2),
                    1,
                )?;
                pushed = true;
            }
        }
        if pushed {
            self.ring.as_ref().unwrap().submit()?;
        }
        Ok(())
    }

    fn drain(&mut self) -> Result<bool> {
        let mut completions = [(0u64, 0i32); 32];
        let mut n = 0;
        {
            let ring = self.ring.as_mut().ok_or(Error::Disconnected)?;
            for c in ring.completion() {
                completions[n] = (c.user_data(), c.result());
                n += 1;
                if n == 32 {
                    break;
                }
            }
        }
        let mut first_error = None;
        for &(id, result) in &completions[..n] {
            if !(1..=4).contains(&id) {
                continue;
            }
            self.active[id as usize - 1] = false;
            if id <= 2 {
                self.leases[id as usize - 1].take();
            }
            if result == -libc::ECANCELED && self.closing {
                continue;
            }
            if result < 0 {
                if result == -libc::EAGAIN && (id == 1 || id == 2) {
                    self.wait[id as usize - 1] = true;
                    continue;
                }
                first_error
                    .get_or_insert_with(|| std::io::Error::from_raw_os_error(-result).into());
                continue;
            }
            match id {
                1 => {
                    if result == 0 {
                        self.closed = true;
                        continue;
                    }
                    if let Err(error) = self
                        .framer
                        .filled(result as usize, self.rx.as_ref().unwrap().size)
                    {
                        first_error.get_or_insert(error);
                    }
                }
                2 => {
                    if result == 0 {
                        first_error.get_or_insert(Error::Disconnected);
                        continue;
                    }
                    let written = result as usize;
                    self.tx_pos += written;
                    self.head_written += written;
                    while let Some(&(pid, pbytes)) = self.pending.front() {
                        if self.head_written < pbytes {
                            break;
                        }
                        self.head_written -= pbytes;
                        self.pending.pop_front();
                        self.completed.push_back((pid, pbytes));
                    }
                    if self.tx_pos == self.tx_len {
                        self.tx_pos = 0;
                        self.tx_len = 0;
                    }
                }
                3 | 4 => {
                    self.wait[id as usize - 3] = false;
                    if result & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) as i32 != 0 {
                        first_error.get_or_insert_with(|| {
                            std::io::Error::from_raw_os_error(libc::EPIPE).into()
                        });
                    }
                }
                _ => {}
            }
        }
        first_error.map_or(Ok(n > 0), Err)
    }
}

impl StreamTransport for UringLink {
    fn capabilities(&self) -> LinkCapabilities {
        self.caps
    }

    fn raw_fd(&self) -> Option<RawFd> {
        Some(self.file.as_raw_fd())
    }

    fn can_submit_direct(&self) -> bool {
        !self.closing && !self.closed && self.pending.len() + self.completed.len() < TX_CAP
    }

    fn can_submit_data(&self) -> bool {
        self.can_submit_direct()
            && self.pending.len() + self.completed.len()
                < TX_CAP.saturating_sub((TX_CAP / 8).max(1)).max(1)
    }

    fn try_submit(&mut self, p: &Packet) -> Result<Option<u64>> {
        if self.closing || self.closed {
            return Err(Error::Disconnected);
        }
        let needed = HEADER + p.header.length as usize;
        if (p.header.kind.is_data_plane() && !self.can_submit_data()) || !self.has_tx_space(needed)
        {
            return Ok(None);
        }
        let id = self.next;
        self.next = id.checked_add(1).ok_or(Error::Exhausted)?;
        let tx = self.tx.as_ref().unwrap();
        // SAFETY: a submitted write can only read the prefix ending before this range.
        let n = unsafe { tx.mutate(self.tx_len..self.tx_len + needed, |bytes| p.encode(bytes)) }?;
        self.tx_len += n;
        self.pending.push_back((id, n));
        Ok(Some(id))
    }

    fn try_submit_with(
        &mut self,
        header: &FrameHeader,
        fill: &mut dyn FnMut(&mut [u8]) -> Result<()>,
    ) -> Result<Option<u64>> {
        if self.closing || self.closed {
            return Err(Error::Disconnected);
        }
        let needed = HEADER + header.length as usize;
        if (header.kind.is_data_plane() && !self.can_submit_data()) || !self.has_tx_space(needed) {
            return Ok(None);
        }
        let id = self.next;
        self.next = id.checked_add(1).ok_or(Error::Exhausted)?;
        let tx = self.tx.as_ref().unwrap();
        // SAFETY: a submitted write can only read the prefix ending before this range.
        let n = unsafe {
            tx.mutate(self.tx_len..self.tx_len + needed, |bytes| {
                Packet::encode_with(header, bytes, fill)
            })
        }?;
        self.tx_len += n;
        self.pending.push_back((id, n));
        Ok(Some(id))
    }

    fn poll(&mut self) -> Result<Option<Event>> {
        match self.poll_fast(&mut |_, _| Ok(false))? {
            FastPoll::Event(event) => Ok(Some(event)),
            FastPoll::Delivered | FastPoll::None => Ok(None),
        }
    }

    fn poll_fast(
        &mut self,
        on_rx: &mut dyn FnMut(FrameHeader, &[u8]) -> Result<bool>,
    ) -> Result<FastPoll> {
        self.drain()?;
        let frame = if self.active[0] {
            None
        } else {
            // SAFETY: the original read CQE retired before this borrow, and scheduling is deferred.
            let bytes = unsafe { self.rx.as_ref().unwrap().bytes() };
            self.framer.frame(bytes)?
        };
        if let Some((header, payload)) = frame {
            let result = if on_rx(header, payload)? {
                FastPoll::Delivered
            } else {
                FastPoll::Event(Event::Received(self.pool.packet(header, payload)?))
            };
            self.framer.consume();
            return Ok(result);
        }
        if let Some((id, bytes)) = self.completed.pop_front() {
            return Ok(FastPoll::Event(Event::Sent(id, bytes)));
        }
        if self.closed {
            if !self.framer.is_empty() {
                return Err(Error::Protocol("EOF inside stream frame"));
            }
            return Ok(FastPoll::Event(Event::Closed));
        }
        self.schedule()?;
        Ok(FastPoll::None)
    }

    fn flush(&mut self) -> Result<()> {
        if self.closing || self.closed {
            return Ok(());
        }
        if self.tx_pos < self.tx_len && !self.active[1] && !self.active[3] && !self.wait[1] {
            let fd = types::Fd(self.file.as_raw_fd());
            let ptr = self.tx.as_ref().unwrap().pointer.as_ptr();
            // SAFETY: valid initialized TX range is retained until write completion.
            let ptr = unsafe { ptr.add(self.tx_pos) };
            self.push(
                opcode::WriteFixed::new(fd, ptr, (self.tx_len - self.tx_pos) as u32, 0)
                    .offset(u64::MAX)
                    .build()
                    .user_data(2),
                1,
            )?;
            self.ring.as_ref().unwrap().submit()?;
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<()> {
        if self.ring.is_none() {
            return Ok(());
        }
        if !self.closing {
            self.closing = true;
            for index in 0..4 {
                if self.active[index] {
                    let entry = opcode::AsyncCancel::new(index as u64 + 1)
                        .build()
                        .user_data(16 + index as u64);
                    let ring = self.ring.as_mut().unwrap();
                    // SAFETY: cancellation targets opaque user-data IDs of our own active entries.
                    unsafe { ring.submission().push(&entry) }.map_err(|_| Error::Exhausted)?;
                }
            }
            self.ring.as_ref().unwrap().submit()?;
        }
        let start = Instant::now();
        let mut terminal = None;
        while self.active.iter().any(|&v| v) {
            if let Err(error) = self.drain() {
                terminal.get_or_insert(error);
            }
            if start.elapsed() > Duration::from_secs(1) {
                return Err(Error::Timeout);
            }
            std::thread::yield_now();
        }
        self.ring
            .as_ref()
            .unwrap()
            .submitter()
            .unregister_buffers()?;
        self.ring.take();
        terminal.map_or(Ok(()), Err)
    }
}

impl Drop for UringLink {
    fn drop(&mut self) {
        if self.shutdown().is_err() && self.ring.is_some() {
            // Kernel ownership is uncertain. Bounded per-link quarantine prevents
            // allocator reuse while outstanding operations may still access pages.
            if let Some(b) = self.tx.take() {
                std::mem::forget(b);
            }
            if let Some(b) = self.rx.take() {
                std::mem::forget(b);
            }
            self.ring.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransportKind;
    use std::os::{fd::OwnedFd, unix::net::UnixStream};
    use usb4_proto::protocol::Kind;

    fn pair() -> (UringLink, UringLink) {
        let (a, b) = UnixStream::pair().unwrap();
        let caps = LinkCapabilities {
            peer: 1,
            speed_mbps: 40_000,
            controller_group: 0,
            transport_kind: TransportKind::Emulated,
            kernel_copies: false,
            fixed_buffers: false,
            sqpoll: false,
            zero_copy_dma: false,
        };
        let link =
            |socket| UringLink::from_file(File::from(OwnedFd::from(socket)), caps, false).unwrap();
        (link(a), link(b))
    }

    #[test]
    fn fixed_buffer_completion_and_cancel() {
        let (mut tx, mut rx) = pair();
        let mut header = FrameHeader::control(Kind::Data, 0, 1, 1);
        header.total = 8192;
        let packet = Packet::new(header, &[17; 8192]).unwrap();
        assert_eq!(tx.try_submit(&packet).unwrap(), Some(1));
        tx.flush().unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let (mut sent, mut received) = (false, false);
        while !sent || !received {
            assert!(Instant::now() < deadline, "io_uring completion deadline");
            if let Some(Event::Sent(id, bytes)) = tx.poll().unwrap() {
                assert_eq!((id, bytes), (1, HEADER + 8192));
                sent = true;
            }
            if let Some(Event::Received(packet)) = rx.poll().unwrap() {
                assert_eq!(packet.data(), &[17; 8192]);
                received = true;
            }
            std::thread::yield_now();
        }
        // Leave a read or readiness operation outstanding on a live peer, then cancel.
        assert!(rx.poll().unwrap().is_none());
        assert!(rx.active[0] || rx.active[2]);
        rx.shutdown().unwrap();
        assert!(rx.ring.is_none() && rx.leases.iter().all(Option::is_none));
    }
    #[test]
    fn peer_loss_reports_poll_error() {
        let (mut left, right) = pair();
        left.wait[0] = true;
        assert!(left.poll().unwrap().is_none());
        assert!(left.active[2]);
        drop(right);
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            assert!(Instant::now() < deadline, "io_uring poll-error deadline");
            match left.poll() {
                Err(Error::Io(error)) if error.raw_os_error() == Some(libc::EPIPE) => break,
                Ok(None) => std::thread::yield_now(),
                _ => panic!("expected error readiness on the retired peer"),
            }
        }
        left.shutdown().unwrap();
    }
}
