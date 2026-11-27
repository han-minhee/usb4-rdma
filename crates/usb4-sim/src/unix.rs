use std::{
    os::unix::net::UnixStream,
    path::Path,
    time::{Duration, Instant},
};
use usb4_proto::{Error, Result};

pub fn open(path: &Path, listen: bool, timeout: Duration) -> Result<UnixStream> {
    let start = Instant::now();
    let stream = if listen {
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
        let listener = std::os::unix::net::UnixListener::bind(path)?;
        listener.set_nonblocking(true)?;
        loop {
            match listener.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if start.elapsed() >= timeout {
                        return Err(Error::Timeout);
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(e) => return Err(e.into()),
            }
        }
    } else {
        loop {
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(s) => break s,
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    if start.elapsed() >= timeout {
                        return Err(Error::Timeout);
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(e) => return Err(e.into()),
            }
        }
    };
    stream.set_nonblocking(true)?;
    Ok(stream)
}
