pub mod configfs;
pub mod memory;
pub mod stream;
#[cfg(target_os = "linux")]
pub mod threaded;
#[cfg(all(feature = "uring", target_os = "linux"))]
pub mod uring;

use usb4_proto::{
    NodeId, Result,
    protocol::{FrameHeader, Packet},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportKind {
    Usb4Stream,
    Emulated,
}

#[derive(Clone, Copy, Debug)]
pub struct LinkCapabilities {
    pub peer: NodeId,
    pub speed_mbps: u32,
    pub controller_group: u64,
    pub transport_kind: TransportKind,
    /// True when the kernel stages payload through its own DMA buffers (e.g. USB4STREAM).
    pub kernel_copies: bool,
    pub fixed_buffers: bool,
    pub sqpoll: bool,
    /// True only for future kernel/hardware zero-copy DMA engines.
    pub zero_copy_dma: bool,
}

pub enum Event {
    /// Submission ID and number of wire bytes whose transport write completed.
    Sent(u64, usize),
    Received(Packet),
    Closed,
}

pub enum FastPoll {
    Event(Event),
    /// An incoming frame was consumed zero-copy directly from the transport buffer.
    Delivered,
    None,
}

/// Pluggable point-to-point reliable ordered link transport backend.
///
/// Linux `USB4STREAM` implements this via `StreamLink` (readiness) and `UringLink`
/// (`io_uring` fixed buffers). A future kernel zero-copy DMA backend can implement
/// this trait without altering router or IPC layers.
pub trait StreamTransport: Send {
    fn capabilities(&self) -> LinkCapabilities;
    fn try_submit(&mut self, packet: &Packet) -> Result<Option<u64>>;
    fn poll(&mut self) -> Result<Option<Event>>;
    /// Push staged outgoing wire bytes to the underlying transport without waiting for the next poll.
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn shutdown(&mut self) -> Result<()>;

    fn raw_fd(&self) -> Option<std::os::fd::RawFd> {
        None
    }

    fn can_submit_direct(&self) -> bool {
        false
    }

    fn can_submit_data(&self) -> bool {
        self.can_submit_direct()
    }

    fn try_submit_with(
        &mut self,
        _header: &FrameHeader,
        _fill: &mut dyn FnMut(&mut [u8]) -> Result<()>,
    ) -> Result<Option<u64>> {
        Ok(None)
    }

    fn poll_fast(
        &mut self,
        _on_rx: &mut dyn FnMut(FrameHeader, &[u8]) -> Result<bool>,
    ) -> Result<FastPoll> {
        Ok(match self.poll()? {
            Some(e) => FastPoll::Event(e),
            None => FastPoll::None,
        })
    }

    fn set_consumer_sleeping(&self, _sleeping: bool) {}
}

/// Cooperative exclusive ownership follows the opened device inode, including
/// across bind mounts. The kernel releases it when this descriptor closes.
pub fn claim_device(file: &std::fs::File) -> Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: flock operates only on the live owned FD and retains no pointers.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(test)]
mod device_lock_tests {
    #[test]
    fn exclusive_descriptor_and_release() {
        let path = std::env::temp_dir().join(format!("u4rd-device-lock-{}", std::process::id()));
        let first = std::fs::File::create_new(&path).unwrap();
        let second = std::fs::File::open(&path).unwrap();
        super::claim_device(&first).unwrap();
        assert!(super::claim_device(&second).is_err());
        drop(first);
        super::claim_device(&second).unwrap();
        drop(second);
        std::fs::remove_file(path).unwrap();
    }
}
