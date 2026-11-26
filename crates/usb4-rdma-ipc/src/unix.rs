use super::*;
use std::{
    cell::{Cell, UnsafeCell},
    fs::File,
    io::{Read, Write},
    marker::PhantomData,
    mem::{size_of, zeroed},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::{
            fs::PermissionsExt,
            net::{UnixListener, UnixStream},
        },
    },
    path::{Path, PathBuf},
    ptr::NonNull,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    time::Duration,
};
use usb4_rdma_core::rdma::AccessFlags;

const IPC_MAGIC: [u8; 4] = *b"U4IP";
const IPC_VERSION: u32 = 1;

const CTRL_REG_MR: u32 = 1;
const CTRL_DEREG_MR: u32 = 2;
const CTRL_MSG_BYTES: usize = 24;

#[repr(C, align(64))]
struct ArenaHeader {
    doorbell: AtomicU64,
    _pad: [u8; 56],
}

#[repr(C, align(64))]
struct Slot {
    state: AtomicU32,
    padding: [u8; 60],
    desc: UnsafeCell<Descriptor>,
    desc_pad: [u8; 32],
    data: UnsafeCell<[u8; WINDOW]>,
}

#[repr(C)]
struct Arena {
    header: ArenaHeader,
    slots: [Slot; SLOTS],
}

const MAP_BYTES: usize = size_of::<Arena>();

struct Mapping {
    ptr: NonNull<Arena>,
    _fd: OwnedFd,
    _single_owner: PhantomData<Cell<()>>,
}

// SAFETY: a mapping is moved between threads, never shared concurrently; each
// process has one serialized endpoint and state transitions transfer slot ownership.
unsafe impl Send for Mapping {}

impl Mapping {
    fn new() -> Result<Self> {
        let fd = {
            // SAFETY: static C string is NUL terminated; returned FD is newly owned.
            let raw = unsafe {
                libc::memfd_create(
                    c"u4rd-ipc-v1".as_ptr(),
                    libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
                )
            };
            if raw < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            // SAFETY: raw is a valid newly created FD with no other Rust owner.
            unsafe { OwnedFd::from_raw_fd(raw) }
        };
        // SAFETY: FD is live; length is a bounded positive off_t.
        if unsafe { libc::ftruncate(fd.as_raw_fd(), MAP_BYTES as libc::off_t) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: seals prohibit resizing and seal changes while keeping shared writes enabled.
        if unsafe {
            libc::fcntl(
                fd.as_raw_fd(),
                libc::F_ADD_SEALS,
                libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        Self::map(fd)
    }

    fn map(fd: OwnedFd) -> Result<Self> {
        let file = File::from(fd);
        if file.metadata()?.len() != MAP_BYTES as u64 {
            return Err(Error::Protocol("arena size"));
        }
        let fd = OwnedFd::from(file);
        // SAFETY: FD length was checked; mapping is shared, read/write, and owned until Drop.
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                MAP_BYTES,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if p == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error().into());
        }
        let ptr = NonNull::new(p.cast::<Arena>()).ok_or(Error::Exhausted)?;
        Ok(Self {
            ptr,
            _fd: fd,
            _single_owner: PhantomData,
        })
    }

    fn doorbell(&self) -> &AtomicU64 {
        // SAFETY: mmap is page-aligned and starts with a zero-initialized cache-aligned ArenaHeader.
        unsafe { &(*self.ptr.as_ptr()).header.doorbell }
    }

    fn slot(&self, i: usize) -> Result<&Slot> {
        if i >= SLOTS {
            return Err(Error::Stale);
        }
        // SAFETY: mmap is page-aligned and contains SLOTS zero-initialized valid
        // Slot representations. Atomic state is zero/FREE on creation.
        Ok(unsafe { &(*self.ptr.as_ptr()).slots[i] })
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: this exact mapping is owned and no reference escapes the endpoint.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), MAP_BYTES);
        }
    }
}

fn descriptor(slot: &Slot) -> Descriptor {
    // SAFETY: caller holds the ownership state for descriptor reads; its peer
    // cannot write again until a release transition transfers ownership back.
    unsafe { *slot.desc.get() }
}

fn write_descriptor(slot: &Slot, d: Descriptor) {
    // SAFETY: caller owns the slot in WRITING/BUSY/CONSUMED state exclusively.
    unsafe {
        *slot.desc.get() = d;
    }
}

#[derive(Debug)]
pub enum ControlEvent {
    None,
    Disconnected,
    RegMr {
        fd: OwnedFd,
        len: usize,
        access: AccessFlags,
    },
    DeregMr {
        mr_id: u64,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct ControlReply {
    pub status: i32,
    pub mr_id: u64,
    pub rkey: u64,
}

impl ControlReply {
    fn encode(self) -> [u8; CTRL_MSG_BYTES] {
        let mut b = [0u8; CTRL_MSG_BYTES];
        b[0..4].copy_from_slice(&self.status.to_le_bytes());
        b[8..16].copy_from_slice(&self.mr_id.to_le_bytes());
        b[16..24].copy_from_slice(&self.rkey.to_le_bytes());
        b
    }

    fn decode(b: &[u8; CTRL_MSG_BYTES]) -> Self {
        Self {
            status: i32::from_le_bytes(b[0..4].try_into().unwrap()),
            mr_id: u64::from_le_bytes(b[8..16].try_into().unwrap()),
            rkey: u64::from_le_bytes(b[16..24].try_into().unwrap()),
        }
    }
}

pub struct Client {
    map: Mapping,
    socket: UnixStream,
    pub welcome: Welcome,
    leased: [bool; SLOTS],
    next_slot: usize,
}

impl Client {
    pub fn connect(path: &Path) -> Result<Self> {
        let socket = UnixStream::connect(path)?;
        socket.set_read_timeout(Some(Duration::from_secs(5)))?;
        socket.set_write_timeout(Some(Duration::from_secs(5)))?;
        let (fd, welcome) = receive_welcome_fd(&socket)?;
        let map = Mapping::map(fd)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            map,
            socket,
            welcome,
            leased: [false; SLOTS],
            next_slot: 0,
        })
    }

    pub fn alive(&mut self) -> Result<()> {
        let mut byte = [0u8];
        match self.socket.read(&mut byte) {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => Ok(()),
            _ => Err(Error::Disconnected),
        }
    }

    /// Register a shared-memory FD with the daemon for direct local/remote RDMA access.
    pub fn register_mr(
        &mut self,
        fd: RawFd,
        len: usize,
        access: AccessFlags,
    ) -> Result<(u64, u64)> {
        let mut req = [0u8; CTRL_MSG_BYTES];
        req[0..4].copy_from_slice(&CTRL_REG_MR.to_le_bytes());
        req[4..8].copy_from_slice(&access.bits().to_le_bytes());
        req[8..16].copy_from_slice(&(len as u64).to_le_bytes());

        self.socket.set_nonblocking(false)?;
        self.socket.set_read_timeout(Some(Duration::from_secs(5)))?;
        self.socket
            .set_write_timeout(Some(Duration::from_secs(5)))?;
        let res = (|| -> Result<ControlReply> {
            send_bytes_with_fd(&self.socket, &req, fd)?;
            let mut reply = [0u8; CTRL_MSG_BYTES];
            self.socket.read_exact(&mut reply)?;
            Ok(ControlReply::decode(&reply))
        })();
        let _ = self.socket.set_nonblocking(true);
        let reply = res?;
        if reply.status != STATUS_OK {
            return Err(match reply.status {
                STATUS_REMOTE_ACCESS_ERR => Error::AccessViolation,
                _ => Error::Exhausted,
            });
        }
        Ok((reply.mr_id, reply.rkey))
    }

    /// Deregister a previously registered shared-memory MR on the daemon.
    pub fn deregister_mr(&mut self, mr_id: u64) -> Result<()> {
        let mut req = [0u8; CTRL_MSG_BYTES];
        req[0..4].copy_from_slice(&CTRL_DEREG_MR.to_le_bytes());
        req[8..16].copy_from_slice(&mr_id.to_le_bytes());

        self.socket.set_nonblocking(false)?;
        self.socket.set_read_timeout(Some(Duration::from_secs(5)))?;
        self.socket
            .set_write_timeout(Some(Duration::from_secs(5)))?;
        let res = (|| -> Result<ControlReply> {
            self.socket.write_all(&req)?;
            let mut reply = [0u8; CTRL_MSG_BYTES];
            self.socket.read_exact(&mut reply)?;
            Ok(ControlReply::decode(&reply))
        })();
        let _ = self.socket.set_nonblocking(true);
        let reply = res?;
        if reply.status != STATUS_OK {
            return Err(Error::Stale);
        }
        Ok(())
    }

    pub fn available(&self) -> usize {
        (0..SLOTS)
            .filter(|&i| {
                !self.leased[i]
                    && self
                        .map
                        .slot(i)
                        .is_ok_and(|s| s.state.load(Ordering::Acquire) == FREE)
            })
            .count()
    }

    pub fn post(&mut self, d: Descriptor, data: &[u8]) -> Result<Option<usize>> {
        d.validate()?;
        if data.len() > WINDOW {
            return Err(Error::Protocol("IPC payload exceeds window"));
        }
        if matches!(d.op, OP_SEND | OP_RDMA_WRITE)
            && d.local_mr_id == 0
            && data.len() != d.length as usize
        {
            return Err(Error::Protocol("IPC payload length mismatch"));
        }
        if d.local_mr_id != 0 && !data.is_empty() {
            return Err(Error::Protocol("unexpected inline payload with shared MR"));
        }
        for step in 0..SLOTS {
            let i = (self.next_slot + step) & (SLOTS - 1);
            if self.leased[i] {
                continue;
            }
            let s = self.map.slot(i)?;
            if s.state.load(Ordering::Relaxed) != FREE {
                continue;
            }
            if s.state
                .compare_exchange(FREE, 7, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                continue;
            }
            self.leased[i] = true;
            self.next_slot = (i + 1) & (SLOTS - 1);
            write_descriptor(s, d);
            // SAFETY: WRITING (7) is exclusively owned by this client; data is bounded.
            unsafe {
                (&mut *s.data.get())[..data.len()].copy_from_slice(data);
            }
            s.state.store(POSTED, Ordering::Release);
            self.map.doorbell().fetch_add(1, Ordering::Release);
            return Ok(Some(i));
        }
        Ok(None)
    }

    pub fn state(&self, i: usize) -> Result<u32> {
        if !self.leased.get(i).copied().unwrap_or(false) {
            return Err(Error::Stale);
        }
        Ok(self.map.slot(i)?.state.load(Ordering::Acquire))
    }

    pub fn response(&self, i: usize) -> Result<Descriptor> {
        if !matches!(self.state(i)?, READY | COMPLETE | FAILED) {
            return Err(Error::Protocol("response not published"));
        }
        Ok(descriptor(self.map.slot(i)?))
    }

    /// Borrow the published bytes until this client consumes the receive window.
    pub fn ready_payload(&self, i: usize) -> Result<(&[u8], Descriptor)> {
        if self.state(i)? != READY {
            return Err(Error::Protocol("payload not ready"));
        }
        self.payload(i)
    }

    pub fn read_payload(&self, i: usize, out: &mut [u8]) -> Result<Descriptor> {
        let (payload, d) = self.ready_payload(i)?;
        Self::copy_payload(payload, d, out)
    }

    pub fn read_completed_stats(&self, i: usize, out: &mut [u8]) -> Result<Descriptor> {
        if self.state(i)? != COMPLETE || descriptor(self.map.slot(i)?).op != OP_STATS {
            return Err(Error::Protocol("stats not complete"));
        }
        let (payload, d) = self.payload(i)?;
        Self::copy_payload(payload, d, out)
    }

    fn copy_payload(payload: &[u8], d: Descriptor, out: &mut [u8]) -> Result<Descriptor> {
        if out.len() < payload.len() {
            return Err(Error::Protocol("receive capacity"));
        }
        out[..payload.len()].copy_from_slice(payload);
        Ok(d)
    }

    fn payload(&self, i: usize) -> Result<(&[u8], Descriptor)> {
        let s = self.map.slot(i)?;
        let d = descriptor(s);
        d.validate()?;
        // SAFETY: both callers acquire the state publishing initialized bytes.
        // This borrow prevents consuming or finishing the slot. The server
        // leaves client-owned slots unchanged, including during failure.
        let payload = unsafe { &(&*s.data.get())[..d.length as usize] };
        Ok((payload, d))
    }

    /// Borrow the published bytes until this client finishes a completed inline slot.
    pub fn completed_payload(&self, i: usize) -> Result<(&[u8], Descriptor)> {
        if !matches!(self.state(i)?, COMPLETE | FAILED) {
            return Err(Error::Protocol("payload not complete"));
        }
        self.payload(i)
    }

    pub fn consume(&mut self, i: usize) -> Result<()> {
        if self.state(i)? != READY {
            return Err(Error::Protocol("consume state"));
        }
        self.map.slot(i)?.state.store(CONSUMED, Ordering::Release);
        self.map.doorbell().fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub fn finish(&mut self, i: usize) -> Result<Descriptor> {
        if !matches!(self.state(i)?, COMPLETE | FAILED) {
            return Err(Error::Protocol("request still active"));
        }
        let s = self.map.slot(i)?;
        let d = descriptor(s);
        s.state.store(FREE, Ordering::Release);
        self.leased[i] = false;
        Ok(d)
    }
}

pub struct ServerClient {
    map: Mapping,
    socket: UnixStream,
    pub namespace: u64,
    seen_doorbell: u64,
}

impl ServerClient {
    pub fn raw_fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }

    /// Returns true once whenever the client has published new slot activity.
    pub fn poll_doorbell(&mut self) -> bool {
        let current = self.map.doorbell().load(Ordering::Acquire);
        if current != self.seen_doorbell {
            self.seen_doorbell = current;
            true
        } else {
            false
        }
    }

    /// Non-blocking check for client liveness and incoming control-plane MR requests.
    pub fn poll_control(&mut self) -> Result<ControlEvent> {
        let mut buf = [0u8; CTRL_MSG_BYTES];
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr().cast(),
            iov_len: buf.len(),
        };
        let mut control = [0usize; 8];
        // SAFETY: zero is valid initial msghdr state.
        let mut msg: libc::msghdr = unsafe { zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = size_of_val(&control) as _;

        // SAFETY: buffers have valid sizes and remain live during nonblocking recvmsg.
        let n = unsafe { libc::recvmsg(self.socket.as_raw_fd(), &mut msg, 0) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            return match err.kind() {
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted => {
                    Ok(ControlEvent::None)
                }
                _ => Ok(ControlEvent::Disconnected),
            };
        }
        if n == 0 {
            return Ok(ControlEvent::Disconnected);
        }

        // Extract optional SCM_RIGHTS FD if present.
        let mut received_fd: Option<OwnedFd> = None;
        // SAFETY: kernel wrote ancillary control headers within `control`.
        let c = unsafe { libc::CMSG_FIRSTHDR(&msg) };
        if !c.is_null() {
            // SAFETY: c is non-null and within the initialized control buffer.
            let valid_fd = unsafe {
                (*c).cmsg_level == libc::SOL_SOCKET
                    && (*c).cmsg_type == libc::SCM_RIGHTS
                    && (*c).cmsg_len as usize == libc::CMSG_LEN(size_of::<RawFd>() as u32) as usize
            };
            if valid_fd {
                // SAFETY: validated SCM_RIGHTS carries one newly received owned FD.
                let fd = unsafe {
                    OwnedFd::from_raw_fd(std::ptr::read_unaligned(
                        libc::CMSG_DATA(c).cast::<RawFd>(),
                    ))
                };
                // SAFETY: newly owned FD; mark close-on-exec.
                let _ = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
                received_fd = Some(fd);
            }
        }
        if msg.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
            return Ok(ControlEvent::Disconnected);
        }

        let mut got = n as usize;
        if got < CTRL_MSG_BYTES {
            let _ = self.socket.set_nonblocking(false);
            let _ = self.socket.set_read_timeout(Some(Duration::from_secs(2)));
            while got < CTRL_MSG_BYTES {
                match self.socket.read(&mut buf[got..]) {
                    Ok(0) | Err(_) => {
                        let _ = self.socket.set_nonblocking(true);
                        return Ok(ControlEvent::Disconnected);
                    }
                    Ok(k) => got += k,
                }
            }
            let _ = self.socket.set_nonblocking(true);
        }

        let cmd = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let arg1 = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let arg2 = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        match cmd {
            CTRL_REG_MR => {
                let Some(fd) = received_fd else {
                    return Ok(ControlEvent::Disconnected);
                };
                let Ok(access) = AccessFlags::from_bits(arg1) else {
                    return Ok(ControlEvent::Disconnected);
                };
                let Ok(len) = usize::try_from(arg2) else {
                    return Ok(ControlEvent::Disconnected);
                };
                Ok(ControlEvent::RegMr { fd, len, access })
            }
            CTRL_DEREG_MR => Ok(ControlEvent::DeregMr { mr_id: arg2 }),
            _ => Ok(ControlEvent::Disconnected),
        }
    }

    pub fn reply_control(&mut self, reply: ControlReply) -> Result<()> {
        let bytes = reply.encode();
        let _ = self.socket.set_nonblocking(false);
        let _ = self.socket.set_write_timeout(Some(Duration::from_secs(2)));
        let res = self.socket.write_all(&bytes);
        let _ = self.socket.set_nonblocking(true);
        res.map_err(Error::from)
    }

    /// Read one wire fragment while the complete send window remains BUSY.
    pub fn read_range(&self, i: usize, offset: usize, out: &mut [u8]) -> Result<()> {
        if self.state(i)? != BUSY {
            return Err(Error::Protocol("server ownership"));
        }
        let s = self.map.slot(i)?;
        let d = descriptor(s);
        let end = offset.checked_add(out.len()).ok_or(Error::Exhausted)?;
        if end > d.length as usize || end > WINDOW {
            return Err(Error::Protocol("send window range"));
        }
        // SAFETY: BUSY belongs exclusively to the server, and the checked range
        // is initialized by the client's POSTED publication.
        unsafe { out.copy_from_slice(&(&*s.data.get())[offset..end]) };
        Ok(())
    }

    /// Stage a received fragment without publishing a partially filled window.
    pub fn write_range(&self, i: usize, offset: usize, data: &[u8]) -> Result<()> {
        if self.state(i)? != BUSY {
            return Err(Error::Protocol("server ownership"));
        }
        let end = offset.checked_add(data.len()).ok_or(Error::Exhausted)?;
        if end > WINDOW {
            return Err(Error::Protocol("receive window range"));
        }
        let s = self.map.slot(i)?;
        // SAFETY: BUSY gives this endpoint exclusive payload access. The client
        // cannot inspect it until the final READY release publication.
        unsafe { (&mut *s.data.get())[offset..end].copy_from_slice(data) };
        Ok(())
    }

    pub fn state(&self, i: usize) -> Result<u32> {
        Ok(self.map.slot(i)?.state.load(Ordering::Acquire))
    }

    pub fn claim(&mut self, i: usize) -> Result<Option<Descriptor>> {
        let s = self.map.slot(i)?;
        if s.state.load(Ordering::Relaxed) != POSTED {
            return Ok(None);
        }
        if s.state
            .compare_exchange(POSTED, BUSY, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Ok(None);
        }
        let d = descriptor(s);
        d.validate()?;
        Ok(Some(d))
    }

    pub fn read_payload(&self, i: usize, out: &mut [u8]) -> Result<Descriptor> {
        if self.state(i)? != BUSY {
            return Err(Error::Protocol("server ownership"));
        }
        let s = self.map.slot(i)?;
        let d = descriptor(s);
        d.validate()?;
        if out.len() < d.length as usize {
            return Err(Error::Protocol("copy capacity"));
        }
        // SAFETY: BUSY is owned by server; client waits for publication before reuse.
        unsafe {
            out[..d.length as usize].copy_from_slice(&(&*s.data.get())[..d.length as usize]);
        }
        Ok(d)
    }

    pub fn publish(&mut self, i: usize, d: Descriptor, data: &[u8], state: u32) -> Result<()> {
        if !matches!(self.state(i)?, BUSY | CONSUMED)
            || !matches!(state, READY | COMPLETE | FAILED | BUSY)
        {
            return Err(Error::Protocol("server publication state"));
        }
        d.validate()?;
        if data.len() > WINDOW {
            return Err(Error::Protocol("server payload"));
        }
        let s = self.map.slot(i)?;
        write_descriptor(s, d);
        // SAFETY: server exclusively owns BUSY/CONSUMED; client awaits release publication.
        unsafe {
            (&mut *s.data.get())[..data.len()].copy_from_slice(data);
        }
        s.state.store(state, Ordering::Release);
        Ok(())
    }

    pub fn consumed(&self, i: usize) -> Result<Option<Descriptor>> {
        Ok(if self.state(i)? == CONSUMED {
            Some(descriptor(self.map.slot(i)?))
        } else {
            None
        })
    }

    pub fn fail_all(&mut self) {
        for i in 0..SLOTS {
            if let Ok(s) = self.map.slot(i) {
                let state = s.state.load(Ordering::Acquire);
                let owned = if state == POSTED {
                    s.state
                        .compare_exchange(POSTED, BUSY, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                } else {
                    matches!(state, BUSY | CONSUMED)
                };
                if owned {
                    let mut d = descriptor(s);
                    d.status = STATUS_GENERAL_ERR;
                    write_descriptor(s, d);
                    s.state.store(FAILED, Ordering::Release);
                }
            }
        }
    }
}

pub struct Server {
    listener: UnixListener,
    _lock: File,
    path: PathBuf,
    identity: (u64, u64),
    welcome: Welcome,
    next: AtomicU64,
}

impl Drop for Server {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        if let Ok(m) = std::fs::symlink_metadata(&self.path)
            && (m.dev(), m.ino()) == self.identity
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl Server {
    pub fn raw_fd(&self) -> RawFd {
        self.listener.as_raw_fd()
    }

    pub fn bind(path: &Path, welcome: Welcome) -> Result<Self> {
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let mut lock = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(lock_path)?;
        // SAFETY: live owned lock FD; nonblocking advisory lock has no pointers.
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            if !metadata.file_type().is_socket() {
                return Err(Error::Protocol("IPC path is not a socket"));
            }
            match UnixStream::connect(path) {
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(path)?
                }
                _ => return Err(Error::Protocol("IPC socket is already owned")),
            }
        }
        let length = lock.metadata()?.len();
        if length > 65528 || length % 8 != 0 {
            return Err(Error::Protocol("IPC epoch journal invalid/full"));
        }
        let mut epochs = Vec::with_capacity(length as usize);
        lock.read_to_end(&mut epochs)?;
        if epochs
            .as_chunks::<8>()
            .0
            .iter()
            .any(|b| u64::from_le_bytes(*b) == welcome.epoch)
        {
            return Err(Error::Protocol("job epoch already used at IPC socket"));
        }
        lock.write_all(&welcome.epoch.to_le_bytes())?;
        lock.sync_all()?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        let listener = UnixListener::bind(path)?;
        let metadata = std::fs::symlink_metadata(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            _lock: lock,
            path: path.to_path_buf(),
            identity: (metadata.dev(), metadata.ino()),
            welcome,
            next: AtomicU64::new(1),
        })
    }

    pub fn accept(&self) -> Result<Option<ServerClient>> {
        let (socket, _) = match self.listener.accept() {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(self.accept_client(socket).ok())
    }

    fn accept_client(&self, socket: UnixStream) -> Result<ServerClient> {
        // SAFETY: cred is a POD struct zero-initialized before getsockopt.
        let mut cred: libc::ucred = unsafe { zeroed() };
        let mut length = size_of::<libc::ucred>() as libc::socklen_t;
        // SAFETY: live socket FD, correctly sized SO_PEERCRED output buffer.
        let rc = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut cred as *mut libc::ucred).cast(),
                &mut length,
            )
        };
        // SAFETY: geteuid takes no arguments and has no memory effects.
        if rc != 0 || cred.uid != unsafe { libc::geteuid() } {
            return Err(Error::Protocol("IPC peer UID"));
        }
        socket.set_write_timeout(Some(Duration::from_secs(3)))?;
        let map = Mapping::new()?;
        let mut welcome = self.welcome;
        welcome.client = self.next.fetch_add(1, Ordering::Relaxed);
        if welcome.client > u32::MAX as u64 {
            return Err(Error::Exhausted);
        }
        let b = welcome_bytes(welcome);
        send_bytes_with_fd(&socket, &b, map._fd.as_raw_fd())?;
        socket.set_nonblocking(true)?;
        Ok(ServerClient {
            map,
            socket,
            namespace: welcome.client,
            seen_doorbell: 0,
        })
    }
}

fn welcome_bytes(w: Welcome) -> [u8; 64] {
    let mut b = [0; 64];
    b[..4].copy_from_slice(&IPC_MAGIC);
    b[4..8].copy_from_slice(&IPC_VERSION.to_le_bytes());
    b[8..12].copy_from_slice(&w.node.to_le_bytes());
    b[12..16].copy_from_slice(&(SLOTS as u32).to_le_bytes());
    b[16..24].copy_from_slice(&w.epoch.to_le_bytes());
    b[24..56].copy_from_slice(&w.digest);
    b[56..64].copy_from_slice(&w.client.to_le_bytes());
    b
}

fn send_bytes_with_fd(socket: &UnixStream, bytes: &[u8], fd: RawFd) -> Result<()> {
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    };
    let mut control = [0usize; 8];
    // SAFETY: zero is the documented initialization for msghdr.
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    // SAFETY: CMSG_SPACE is a size calculation for one RawFd.
    msg.msg_controllen = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) } as _;
    // SAFETY: aligned control buffer has sufficient CMSG_SPACE; all pointers
    // remain live until sendmsg returns, and FD is borrowed only.
    let n = unsafe {
        let c = libc::CMSG_FIRSTHDR(&msg);
        (*c).cmsg_level = libc::SOL_SOCKET;
        (*c).cmsg_type = libc::SCM_RIGHTS;
        (*c).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as _;
        std::ptr::write_unaligned(libc::CMSG_DATA(c).cast::<RawFd>(), fd);
        libc::sendmsg(socket.as_raw_fd(), &msg, 0)
    };
    if n < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if (n as usize) < bytes.len() {
        let mut s = socket;
        s.write_all(&bytes[n as usize..])?;
    }
    Ok(())
}

fn receive_welcome_fd(socket: &UnixStream) -> Result<(OwnedFd, Welcome)> {
    let mut b = [0u8; 64];
    let mut iov = libc::iovec {
        iov_base: b.as_mut_ptr().cast(),
        iov_len: b.len(),
    };
    let mut control = [0usize; 8];
    // SAFETY: zero is valid initial msghdr state.
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = size_of_val(&control) as _;
    // SAFETY: writable buffers have the specified lengths and remain alive.
    let n = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut msg, libc::MSG_WAITALL) };
    if n < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: kernel initialized ancillary header within the aligned control buffer.
    let c = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if c.is_null()
        // SAFETY: short-circuiting excluded a null ancillary header.
        || unsafe {
            (*c).cmsg_level != libc::SOL_SOCKET
                || (*c).cmsg_type != libc::SCM_RIGHTS
                || (*c).cmsg_len as usize != libc::CMSG_LEN(size_of::<RawFd>() as u32) as usize
        }
    {
        return Err(Error::Protocol("missing arena FD"));
    }
    // SAFETY: validated SCM_RIGHTS payload contains one kernel-owned newly received FD.
    let fd = unsafe {
        OwnedFd::from_raw_fd(std::ptr::read_unaligned(libc::CMSG_DATA(c).cast::<RawFd>()))
    };
    if msg.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0 {
        return Err(Error::Protocol("truncated IPC handshake"));
    }
    if (n as usize) < b.len() {
        let mut s = socket;
        s.read_exact(&mut b[n as usize..])?;
    }
    if b[..4] != IPC_MAGIC
        || b[4..8] != IPC_VERSION.to_le_bytes()
        || b[12..16] != (SLOTS as u32).to_le_bytes()
    {
        return Err(Error::Protocol("IPC version/layout"));
    }
    // SAFETY: FD is newly received and owned; setting close-on-exec preserves ownership.
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let w = Welcome {
        node: u32::from_le_bytes(b[8..12].try_into().unwrap()),
        epoch: u64::from_le_bytes(b[16..24].try_into().unwrap()),
        digest: b[24..56].try_into().unwrap(),
        client: u64::from_le_bytes(b[56..64].try_into().unwrap()),
    };
    Ok((fd, w))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mr::SharedRegion;

    #[test]
    fn layout() {
        assert_eq!(size_of::<Descriptor>(), 96);
        assert_eq!(size_of::<Slot>(), 192 + WINDOW);
    }

    #[test]
    fn fd_passing_slot_ownership_and_mr_control() {
        let path = std::env::temp_dir().join(format!("u4rd-ipc-test-{}.sock", std::process::id()));
        let server = Server::bind(
            &path,
            Welcome {
                node: 7,
                epoch: 1,
                digest: [3; 32],
                client: 0,
            },
        )
        .unwrap();
        let (complete, allow_complete) = std::sync::mpsc::sync_channel(0);
        let worker = std::thread::spawn(move || {
            let mut s = loop {
                if let Some(s) = server.accept().unwrap() {
                    break s;
                }
                std::thread::yield_now();
            };
            let mut mapped_mr: Option<SharedRegion> = None;
            for _ in 0..200_000 {
                match s.poll_control().unwrap() {
                    ControlEvent::RegMr { fd, len, access } => {
                        let region = SharedRegion::map_fd(
                            fd,
                            len,
                            access.contains(AccessFlags::REMOTE_WRITE),
                        )
                        .unwrap();
                        region.write_range(0, b"daemon-wrote").unwrap();
                        mapped_mr = Some(region);
                        s.reply_control(ControlReply {
                            status: STATUS_OK,
                            mr_id: 1,
                            rkey: 0xABCD_1234,
                        })
                        .unwrap();
                    }
                    ControlEvent::DeregMr { mr_id } => {
                        assert_eq!(mr_id, 1);
                        mapped_mr = None;
                        s.reply_control(ControlReply {
                            status: STATUS_OK,
                            mr_id,
                            rkey: 0,
                        })
                        .unwrap();
                    }
                    ControlEvent::Disconnected => break,
                    ControlEvent::None => {}
                }
                if let Some(d) = s.claim(0).unwrap() {
                    assert_eq!(d.tag_or_imm, 42);
                    let mut data = [0; 4];
                    s.read_payload(0, &mut data).unwrap();
                    assert_eq!(&data, b"test");
                    allow_complete.recv_timeout(Duration::from_secs(2)).unwrap();
                    s.publish(0, d, &[], COMPLETE).unwrap();
                }
                std::thread::yield_now();
            }
            assert!(mapped_mr.is_none());
        });

        let mut c = Client::connect(&path).unwrap();
        assert_eq!(c.welcome.node, 7);

        let mr = SharedRegion::allocate(4096).unwrap();
        let (mr_id, rkey) = c
            .register_mr(
                mr.raw_fd(),
                mr.len(),
                AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE,
            )
            .unwrap();
        assert_eq!((mr_id, rkey), (1, 0xABCD_1234));
        let mut seen = [0u8; 12];
        mr.read_range(0, &mut seen).unwrap();
        assert_eq!(&seen, b"daemon-wrote");

        let i = c
            .post(
                Descriptor {
                    op: OP_SEND,
                    total: 4,
                    length: 4,
                    tag_or_imm: 42,
                    ..Default::default()
                },
                b"test",
            )
            .unwrap()
            .unwrap();
        assert!(c.finish(i).is_err());
        complete.send(()).unwrap();
        while c.state(i).unwrap() != COMPLETE {
            std::thread::yield_now();
        }
        c.finish(i).unwrap();
        c.deregister_mr(mr_id).unwrap();
        drop(c);
        worker.join().unwrap();
        assert!(!path.exists());
        std::fs::remove_file(format!("{}.lock", path.display())).unwrap();
    }

    #[test]
    fn socket_ownership_stale_recovery_and_foreign_path() {
        let path = std::env::temp_dir().join(format!("u4rd-ownership-{}.sock", std::process::id()));
        let welcome = Welcome {
            node: 0,
            epoch: 1,
            digest: [0; 32],
            client: 0,
        };
        let first = Server::bind(&path, welcome).unwrap();
        assert!(Server::bind(&path, welcome).is_err());
        assert!(path.exists());
        drop(first);
        assert!(!path.exists());
        assert!(Server::bind(&path, welcome).is_err());
        let stale = UnixListener::bind(&path).unwrap();
        drop(stale);
        let recovered = Server::bind(
            &path,
            Welcome {
                epoch: 2,
                ..welcome
            },
        )
        .unwrap();
        drop(recovered);
        std::fs::write(&path, b"preserve").unwrap();
        assert!(Server::bind(&path, welcome).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"preserve");
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(format!("{}.lock", path.display())).unwrap();
    }
}
