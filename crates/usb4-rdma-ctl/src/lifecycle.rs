use std::{
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::net::UnixStream,
    },
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn stop(_: libc::c_int) {
    // Only a lock-free atomic store runs in signal context; no I/O or allocation.
    STOP.store(true, Ordering::Relaxed);
}

pub fn install() -> std::io::Result<()> {
    for signal in [libc::SIGTERM, libc::SIGINT] {
        // SAFETY: static C-ABI handler remains live for the process lifetime and
        // only stores to a lock-free AtomicBool. No borrowed memory is retained.
        if unsafe { libc::signal(signal, stop as *const () as libc::sighandler_t) } == libc::SIG_ERR
        {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

pub fn stopping() -> bool {
    STOP.load(Ordering::Relaxed)
}

const FRAME_BYTES: usize = 52;
const PREPARE: u8 = 1;
const PREPARED: u8 = 2;
const RELEASE: u8 = 3;

pub struct ShutdownBarrier {
    control: UnixStream,
    identity: [u8; FRAME_BYTES],
    timeout: Duration,
}

impl ShutdownBarrier {
    pub fn from_inherited_fd(
        fd: RawFd,
        node: u32,
        epoch: u64,
        digest: [u8; 32],
        timeout: Duration,
    ) -> io::Result<Self> {
        if fd < 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid shutdown fd",
            ));
        }
        // SAFETY: fcntl either duplicates a live descriptor with exclusive ownership
        // of the new FD, or fails without producing a descriptor.
        let copy = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
        if copy < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: copy is the fresh, owned descriptor returned by fcntl above.
        let control = unsafe { UnixStream::from_raw_fd(copy) };
        let mut socket_type: libc::c_int = 0;
        let mut size = std::mem::size_of_val(&socket_type) as libc::socklen_t;
        // SAFETY: control keeps the descriptor live. Both out-parameters point
        // to initialized stack storage of the size requested by SO_TYPE.
        let result = unsafe {
            libc::getsockopt(
                control.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_TYPE,
                (&mut socket_type as *mut libc::c_int).cast(),
                &mut size,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        if socket_type != libc::SOCK_STREAM {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shutdown fd must be a Unix stream",
            ));
        }
        control.peer_addr()?;
        control.set_nonblocking(false)?;
        Ok(Self {
            control,
            identity: shutdown_frame(node, epoch, digest, 0),
            timeout,
        })
    }

    pub fn wait(mut self) -> io::Result<()> {
        let deadline = Instant::now() + self.timeout;
        self.identity[4] = PREPARE;
        receive(&mut self.control, &self.identity, deadline)?;
        self.identity[4] = PREPARED;
        send(&mut self.control, &self.identity, deadline)?;
        self.identity[4] = RELEASE;
        receive(&mut self.control, &self.identity, deadline)
    }
}

fn shutdown_frame(node: u32, epoch: u64, digest: [u8; 32], verb: u8) -> [u8; FRAME_BYTES] {
    let mut frame = [0; FRAME_BYTES];
    frame[..4].copy_from_slice(b"U4S1");
    frame[4] = verb;
    frame[8..12].copy_from_slice(&node.to_le_bytes());
    frame[12..20].copy_from_slice(&epoch.to_le_bytes());
    frame[20..].copy_from_slice(&digest);
    frame
}

fn transfer(
    deadline: Instant,
    mut next: impl FnMut(Duration) -> io::Result<bool>,
) -> io::Result<()> {
    loop {
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            break;
        }
        match next(timeout) {
            Ok(true) if Instant::now() < deadline => return Ok(()),
            Ok(true) => break,
            Ok(false) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(e) => return Err(e),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "shutdown barrier timed out",
    ))
}

fn receive(
    control: &mut UnixStream,
    expected: &[u8; FRAME_BYTES],
    deadline: Instant,
) -> io::Result<()> {
    let mut actual = [0; FRAME_BYTES];
    let mut offset = 0;
    transfer(deadline, |timeout| {
        control.set_read_timeout(Some(timeout))?;
        let len = control.read(&mut actual[offset..])?;
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "shutdown control closed",
            ));
        }
        let end = offset + len;
        if actual[offset..end] != expected[offset..end] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shutdown control frame differs",
            ));
        }
        offset = end;
        Ok(offset == FRAME_BYTES)
    })
}

fn send(control: &mut UnixStream, frame: &[u8; FRAME_BYTES], deadline: Instant) -> io::Result<()> {
    let mut offset = 0;
    transfer(deadline, |timeout| {
        control.set_write_timeout(Some(timeout))?;
        let len = control.write(&frame[offset..])?;
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "shutdown acknowledgment not sent",
            ));
        }
        offset += len;
        Ok(offset == FRAME_BYTES)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: [u8; 32] = [0xa5; 32];

    fn fixture(timeout: Duration) -> (ShutdownBarrier, UnixStream) {
        let (owner, daemon) = UnixStream::pair().unwrap();
        let barrier =
            ShutdownBarrier::from_inherited_fd(daemon.as_raw_fd(), 1, 10, DIGEST, timeout).unwrap();
        (barrier, owner)
    }

    #[test]
    fn stop_barrier_rejects_wrong_epoch() {
        let (barrier, mut owner) = fixture(Duration::from_secs(5));
        owner
            .write_all(&shutdown_frame(1, 11, DIGEST, PREPARE))
            .unwrap();
        assert_eq!(
            barrier.wait().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn stop_barrier_requires_release_before_deadline() {
        let (barrier, mut owner) = fixture(Duration::from_millis(100));
        owner
            .write_all(&shutdown_frame(1, 10, DIGEST, PREPARE))
            .unwrap();
        assert_eq!(barrier.wait().unwrap_err().kind(), io::ErrorKind::TimedOut);
        let mut acknowledgment = [0; FRAME_BYTES];
        owner.read_exact(&mut acknowledgment).unwrap();
        assert_eq!(acknowledgment, shutdown_frame(1, 10, DIGEST, PREPARED));
    }

    #[test]
    fn stop_barrier_rejects_closed_control() {
        let (barrier, owner) = fixture(Duration::from_secs(5));
        drop(owner);
        assert_eq!(
            barrier.wait().unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
}
