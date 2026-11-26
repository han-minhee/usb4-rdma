//! Registered-buffer `io_uring` adapter. Kernel stream copies still occur on USB4STREAM.
use crate::{Event, FastPoll, LinkBackend, LinkCapabilities};
use io_uring::{IoUring, opcode, types};
use std::{
    collections::VecDeque,
    fs::File,
    os::fd::{AsRawFd, RawFd},
    time::{Duration, Instant},
};
use usb4_rdma_core::{
    Error, Result,
    protocol::{FrameHeader, HEADER, MAX_FRAME, Packet, PacketPool, decode_header},
};

const TX_CAP: usize = 16;

pub struct UringLink {
    pool: PacketPool,
    ring: Option<IoUring>,
    file: File,
    tx: Option<Box<[u8]>>,
    rx: Option<Box<[u8]>>,
    caps: LinkCapabilities,
    tx_len: usize,
    tx_pos: usize,
    rx_pos: usize,
    rx_start: usize,
    active: [bool; 4],
    wait: [bool; 2],
    next: u64,
    head_written: usize,
    pending: VecDeque<(u64, usize)>,
    completed: VecDeque<(u64, usize)>,
    closing: bool,
    received: Option<Packet>,
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
        let mut tx = vec![0; MAX_FRAME * TX_CAP].into_boxed_slice();
        let mut rx = vec![0; MAX_FRAME * 16].into_boxed_slice();
        let iov = [
            libc::iovec {
                iov_base: tx.as_mut_ptr().cast(),
                iov_len: tx.len(),
            },
            libc::iovec {
                iov_base: rx.as_mut_ptr().cast(),
                iov_len: rx.len(),
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
            rx_pos: 0,
            rx_start: 0,
            active: [false; 4],
            wait: [false; 2],
            next: 1,
            head_written: 0,
            pending: VecDeque::with_capacity(TX_CAP),
            completed: VecDeque::with_capacity(TX_CAP),
            closing: false,
            received: None,
            closed: false,
        })
    }

    fn has_tx_space(&mut self, needed: usize) -> bool {
        if self.closing || self.closed || self.pending.len() >= TX_CAP {
            return false;
        }
        let tx = self.tx.as_mut().unwrap();
        if self.tx_len + needed <= tx.len() {
            return true;
        }
        if !self.active[1] && self.tx_pos > 0 {
            tx.copy_within(self.tx_pos..self.tx_len, 0);
            self.tx_len -= self.tx_pos;
            self.tx_pos = 0;
        }
        self.tx_len + needed <= tx.len()
    }

    fn push(&mut self, entry: io_uring::squeue::Entry, index: usize) -> Result<()> {
        let ring = self.ring.as_mut().ok_or(Error::Disconnected)?;
        // SAFETY: entry pointers reference registered, stable buffers. Single owner
        // serializes the SQ and never reuses an active read/write buffer.
        unsafe { ring.submission().push(&entry) }.map_err(|_| Error::Exhausted)?;
        self.active[index] = true;
        Ok(())
    }

    fn schedule(&mut self) -> Result<()> {
        if self.closing || self.closed {
            return Ok(());
        }
        if !self.active[0] && self.received.is_none() {
            self.parse_received()?;
        }
        let mut pushed = false;
        let fd = types::Fd(self.file.as_raw_fd());
        if !self.active[0] && !self.active[2] && self.received.is_none() {
            if self.wait[0] {
                self.push(
                    opcode::PollAdd::new(fd, libc::POLLIN as u32)
                        .build()
                        .user_data(3),
                    2,
                )?;
                pushed = true;
            } else {
                let rx = self.rx.as_mut().unwrap();
                if self.rx_start > 0 {
                    if self.rx_start == self.rx_pos {
                        self.rx_pos = 0;
                        self.rx_start = 0;
                    } else {
                        rx.copy_within(self.rx_start..self.rx_pos, 0);
                        self.rx_pos -= self.rx_start;
                        self.rx_start = 0;
                    }
                }
                let capacity = rx.len();
                let ptr = rx.as_mut_ptr();
                // SAFETY: rx_pos <= allocation length, only one read is active.
                let ptr = unsafe { ptr.add(self.rx_pos) };
                if capacity > self.rx_pos {
                    self.push(
                        opcode::ReadFixed::new(fd, ptr, (capacity - self.rx_pos) as u32, 1)
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
                let ptr = self.tx.as_ref().unwrap().as_ptr();
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

    fn parse_received(&mut self) -> Result<()> {
        if self.received.is_none() && self.rx_pos - self.rx_start >= HEADER {
            let rx = self.rx.as_ref().unwrap();
            let needed = HEADER + decode_header(&rx[self.rx_start..self.rx_pos])?.length as usize;
            if self.rx_pos - self.rx_start >= needed {
                self.received = Some(
                    self.pool
                        .decode(&rx[self.rx_start..self.rx_start + needed])?,
                );
                self.rx_start += needed;
            }
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
        for &(id, result) in &completions[..n] {
            if !(1..=4).contains(&id) {
                continue;
            }
            self.active[id as usize - 1] = false;
            if result == -libc::ECANCELED && self.closing {
                continue;
            }
            if result < 0 {
                if result == -libc::EAGAIN && (id == 1 || id == 2) {
                    self.wait[id as usize - 1] = true;
                    continue;
                }
                return Err(std::io::Error::from_raw_os_error(-result).into());
            }
            match id {
                1 => {
                    if result == 0 {
                        self.closed = true;
                        continue;
                    }
                    self.rx_pos += result as usize;
                    self.parse_received()?;
                }
                2 => {
                    if result == 0 {
                        return Err(Error::Disconnected);
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
                3 => self.wait[0] = false,
                4 => self.wait[1] = false,
                _ => {}
            }
        }
        Ok(n > 0)
    }
}

impl LinkBackend for UringLink {
    fn capabilities(&self) -> LinkCapabilities {
        self.caps
    }

    fn raw_fd(&self) -> Option<RawFd> {
        Some(self.file.as_raw_fd())
    }

    fn can_submit_direct(&self) -> bool {
        !self.closing && !self.closed && self.pending.len() < TX_CAP
    }

    fn try_submit(&mut self, p: &Packet) -> Result<Option<u64>> {
        if self.closing || self.closed {
            return Err(Error::Disconnected);
        }
        let needed = HEADER + p.header.length as usize;
        if !self.has_tx_space(needed) {
            return Ok(None);
        }
        let id = self.next;
        self.next = id.checked_add(1).ok_or(Error::Exhausted)?;
        let n = p.encode(&mut self.tx.as_mut().unwrap()[self.tx_len..])?;
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
        if !self.has_tx_space(needed) {
            return Ok(None);
        }
        let id = self.next;
        self.next = id.checked_add(1).ok_or(Error::Exhausted)?;
        let n = Packet::encode_with(header, &mut self.tx.as_mut().unwrap()[self.tx_len..], fill)?;
        self.tx_len += n;
        self.pending.push_back((id, n));
        Ok(Some(id))
    }

    fn poll(&mut self) -> Result<Option<Event>> {
        self.drain()?;
        if !self.active[0] && self.received.is_none() && self.rx_pos - self.rx_start >= HEADER {
            self.parse_received()?;
        }
        if let Some(p) = self.received.take() {
            return Ok(Some(Event::Received(p)));
        }
        if let Some((id, bytes)) = self.completed.pop_front() {
            return Ok(Some(Event::Sent(id, bytes)));
        }
        if self.closed {
            return Ok(Some(Event::Closed));
        }
        self.schedule()?;
        Ok(None)
    }

    fn poll_fast(
        &mut self,
        on_rx: &mut dyn FnMut(FrameHeader, &[u8]) -> Result<bool>,
    ) -> Result<FastPoll> {
        self.drain()?;
        if !self.active[0] && self.received.is_none() && self.rx_pos - self.rx_start >= HEADER {
            let rx = self.rx.as_ref().unwrap();
            let needed = HEADER + decode_header(&rx[self.rx_start..self.rx_pos])?.length as usize;
            if self.rx_pos - self.rx_start >= needed {
                let frame = &rx[self.rx_start..self.rx_start + needed];
                let (h, payload) = Packet::decode_raw(frame)?;
                if on_rx(h, payload)? {
                    self.rx_start += needed;
                    return Ok(FastPoll::Delivered);
                }
            }
            self.parse_received()?;
        }
        if let Some(p) = self.received.take() {
            return Ok(FastPoll::Event(Event::Received(p)));
        }
        if let Some((id, bytes)) = self.completed.pop_front() {
            return Ok(FastPoll::Event(Event::Sent(id, bytes)));
        }
        if self.closed {
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
            let ptr = self.tx.as_ref().unwrap().as_ptr();
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
        while self.active.iter().any(|&v| v) {
            self.drain()?;
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
        Ok(())
    }
}

impl Drop for UringLink {
    fn drop(&mut self) {
        if self.shutdown().is_err() {
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
