use crate::{Event, FastPoll, LinkCapabilities, StreamTransport};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    os::fd::RawFd,
};
use usb4_proto::{
    Error, Result,
    framer::Framer,
    protocol::{FrameHeader, HEADER, MAX_FRAME, Packet, PacketPool},
};

type RxCallback<'a> = &'a mut dyn FnMut(FrameHeader, &[u8]) -> Result<bool>;

pub trait StreamIo: Read + Write + Send {}
impl<T: Read + Write + Send> StreamIo for T {}

/// Ordered pipelined TX batching; bounded RX read-ahead with zero-pool fast path.
pub struct StreamLink {
    io: Box<dyn StreamIo>,
    raw_fd: Option<RawFd>,
    pool: PacketPool,
    caps: LinkCapabilities,
    tx: Box<[u8]>,
    rx: Box<[u8]>,
    tx_cap: usize,
    tx_len: usize,
    tx_pos: usize,
    framer: Framer,
    next: u64,
    head_written: usize,
    pending: VecDeque<(u64, usize)>,
    completed: VecDeque<(u64, usize)>,
    closed: bool,
}

impl StreamLink {
    pub fn new(io: Box<dyn StreamIo>, caps: LinkCapabilities) -> Self {
        let tx_cap = 8;
        Self {
            io,
            raw_fd: None,
            pool: PacketPool::new(128),
            caps,
            tx: vec![0; MAX_FRAME * tx_cap].into_boxed_slice(),
            rx: vec![0; MAX_FRAME * 32].into_boxed_slice(),
            tx_cap,
            tx_len: 0,
            tx_pos: 0,
            framer: Framer::default(),
            next: 1,
            head_written: 0,
            pending: VecDeque::with_capacity(tx_cap),
            completed: VecDeque::with_capacity(tx_cap),
            closed: false,
        }
    }

    pub fn with_raw_fd(mut self, fd: RawFd) -> Self {
        self.raw_fd = Some(fd);
        self
    }

    /// Configure TX batch capacity before traffic (default 8 frames).
    pub fn with_tx_capacity(mut self, capacity: usize) -> Self {
        let cap = capacity.max(1);
        self.tx_cap = cap;
        self.tx = vec![0; MAX_FRAME * cap].into_boxed_slice();
        self.pending = VecDeque::with_capacity(cap);
        self.completed = VecDeque::with_capacity(cap);
        self
    }

    /// Configure before any traffic; retained packets keep their old buffers alive.
    pub fn with_receive_capacity(mut self, capacity: usize) -> Self {
        self.pool = PacketPool::new(capacity);
        self
    }

    fn has_tx_space(&mut self, needed: usize) -> bool {
        if self.pending.len() + self.completed.len() >= self.tx_cap {
            return false;
        }
        if self.tx_len + needed <= self.tx.len() {
            return true;
        }
        if self.tx_pos > 0 {
            self.tx.copy_within(self.tx_pos..self.tx_len, 0);
            self.tx_len -= self.tx_pos;
            self.tx_pos = 0;
        }
        self.tx_len + needed <= self.tx.len()
    }

    fn flush_tx(&mut self) -> Result<()> {
        while self.tx_pos < self.tx_len {
            match self.io.write(&self.tx[self.tx_pos..self.tx_len]) {
                Ok(0) => return Err(Error::Disconnected),
                Ok(n) => {
                    self.tx_pos += n;
                    self.head_written += n;
                    while let Some(&(id, bytes)) = self.pending.front() {
                        if self.head_written < bytes {
                            break;
                        }
                        self.head_written -= bytes;
                        self.pending.pop_front();
                        self.completed.push_back((id, bytes));
                    }
                    if self.tx_pos == self.tx_len {
                        self.tx_pos = 0;
                        self.tx_len = 0;
                        break;
                    }
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn poll_inner(&mut self, mut on_rx: Option<RxCallback<'_>>) -> Result<FastPoll> {
        if self.closed {
            return Ok(FastPoll::None);
        }
        self.flush_tx()?;

        if self.completed.len() >= self.tx_cap
            && let Some((id, bytes)) = self.completed.pop_front()
        {
            return Ok(FastPoll::Event(Event::Sent(id, bytes)));
        }

        // Parse buffered frames before reading. Compact only an incomplete tail;
        // complete packets leave by shared pool handle or synchronous borrow.
        for attempt in 0..2 {
            if let Some((header, payload)) = self.framer.frame(&self.rx)? {
                if let Some(cb) = on_rx.as_deref_mut()
                    && cb(header, payload)?
                {
                    self.framer.consume();
                    return Ok(FastPoll::Delivered);
                }
                let packet = self.pool.packet(header, payload)?;
                self.framer.consume();
                return Ok(FastPoll::Event(Event::Received(packet)));
            }
            if attempt == 1 {
                break;
            }
            self.framer.compact(&mut self.rx);
            let end = self.framer.end();
            if end < self.rx.len() {
                match self.io.read(&mut self.rx[end..]) {
                    Ok(0) => {
                        self.closed = true;
                        if !self.framer.is_empty() {
                            return Err(Error::Protocol("EOF inside stream frame"));
                        }
                        return Ok(FastPoll::Event(Event::Closed));
                    }
                    Ok(n) => self.framer.filled(n, self.rx.len())?,
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                        ) =>
                    {
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }

        if let Some((id, bytes)) = self.completed.pop_front() {
            return Ok(FastPoll::Event(Event::Sent(id, bytes)));
        }
        Ok(FastPoll::None)
    }
}

impl StreamTransport for StreamLink {
    fn capabilities(&self) -> LinkCapabilities {
        self.caps
    }

    fn raw_fd(&self) -> Option<RawFd> {
        self.raw_fd
    }

    fn can_submit_direct(&self) -> bool {
        !self.closed && self.pending.len() + self.completed.len() < self.tx_cap
    }

    fn can_submit_data(&self) -> bool {
        self.can_submit_direct()
            && self.pending.len() + self.completed.len()
                < self.tx_cap.saturating_sub((self.tx_cap / 8).max(1)).max(1)
    }

    fn try_submit(&mut self, p: &Packet) -> Result<Option<u64>> {
        if self.closed {
            return Err(Error::Disconnected);
        }
        let needed = HEADER + p.header.length as usize;
        if (p.header.kind.is_data_plane() && !self.can_submit_data()) || !self.has_tx_space(needed)
        {
            return Ok(None);
        }
        let id = self.next;
        self.next = id.checked_add(1).ok_or(Error::Exhausted)?;
        let n = p.encode(&mut self.tx[self.tx_len..])?;
        self.tx_len += n;
        self.pending.push_back((id, n));
        Ok(Some(id))
    }

    fn try_submit_with(
        &mut self,
        header: &FrameHeader,
        fill: &mut dyn FnMut(&mut [u8]) -> Result<()>,
    ) -> Result<Option<u64>> {
        if self.closed {
            return Err(Error::Disconnected);
        }
        let needed = HEADER + header.length as usize;
        if (header.kind.is_data_plane() && !self.can_submit_data()) || !self.has_tx_space(needed) {
            return Ok(None);
        }
        let id = self.next;
        self.next = id.checked_add(1).ok_or(Error::Exhausted)?;
        let n = Packet::encode_with(header, &mut self.tx[self.tx_len..], fill)?;
        self.tx_len += n;
        self.pending.push_back((id, n));
        Ok(Some(id))
    }

    fn poll(&mut self) -> Result<Option<Event>> {
        match self.poll_inner(None)? {
            FastPoll::Event(e) => Ok(Some(e)),
            FastPoll::Delivered | FastPoll::None => Ok(None),
        }
    }

    fn poll_fast(
        &mut self,
        on_rx: &mut dyn FnMut(FrameHeader, &[u8]) -> Result<bool>,
    ) -> Result<FastPoll> {
        self.poll_inner(Some(on_rx))
    }

    fn flush(&mut self) -> Result<()> {
        self.flush_tx()
    }

    fn shutdown(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }
}

pub struct ReadyFile {
    file: std::fs::File,
}

/// Open and claim a stream without using readiness syscalls.
pub fn open_device(path: &std::path::Path) -> Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)?;
    crate::claim_device(&file)?;
    Ok(file)
}

impl ReadyFile {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        Ok(Self {
            file: open_device(path)?,
        })
    }

    pub fn from_file(file: std::fs::File) -> Result<Self> {
        use std::os::fd::AsRawFd;
        // SAFETY: fcntl only changes flags on this owned, live descriptor.
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: the FD remains owned and valid throughout this call.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Self { file })
    }

    pub fn as_raw_fd(&self) -> RawFd {
        use std::os::fd::AsRawFd;
        self.file.as_raw_fd()
    }
}

impl Read for ReadyFile {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(b)
    }
}

impl Write for ReadyFile {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.file.write(b)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
