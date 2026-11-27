//! Single-client/single-server shared-memory ownership protocol and MR control channel.

pub mod mr;
mod unix;

use crate::{Error, Result};
pub use unix::*;

pub const SLOTS: usize = 64;
/// Bounded host transfer window; wire frames remain 32 KiB for routing/bonding.
pub const WINDOW: usize = 1024 * 1024;
/// Maximum simultaneously registered shared-memory MRs per local client.
pub const MAX_CLIENT_MRS: usize = 64;

// All mapped payload access is serialized through the ownership states below.
pub const FREE: u32 = 0;
pub const POSTED: u32 = 1;
pub const BUSY: u32 = 2;
pub const READY: u32 = 3;
pub const CONSUMED: u32 = 4;
pub const COMPLETE: u32 = 5;
pub const FAILED: u32 = 6;

pub const OP_LISTEN: u32 = 1;
pub const OP_CONNECT: u32 = 2;
pub const OP_ACCEPT: u32 = 3;
pub const OP_SEND: u32 = 4;
pub const OP_RECV: u32 = 5;
pub const OP_CLOSE: u32 = 6;
pub const OP_STATS: u32 = 7;
/// Advisory receive readiness; never grants or borrows an application buffer.
pub const OP_PEEK: u32 = 8;
/// One-sided RDMA write (with optional immediate value when `FLAG_IPC_WITH_IMM` is set).
pub const OP_RDMA_WRITE: u32 = 9;
/// One-sided RDMA read from a remote MR (`rkey`, `remote_offset`).
pub const OP_RDMA_READ: u32 = 10;

/// Request a payload of little-endian (submitted, received) u64 pairs.
pub const STATS_LINKS: u32 = 1;
pub const LINK_STATS_BYTES: usize = 16;

/// Descriptor flag bits for RDMA operations.
pub const FLAG_IPC_WITH_IMM: u32 = 1 << 0;
/// Set on a completed `OP_RECV` descriptor when it consumed an `RDMA_WRITE_WITH_IMM`.
pub const FLAG_IPC_RECV_IMM: u32 = 1 << 1;
/// Keep an `OP_RECV` posted until a matching message or disconnect.
pub const FLAG_IPC_RECV_STANDING: u32 = 1 << 2;
/// Send every window of a registered buffer from one `OP_SEND` descriptor.
pub const FLAG_IPC_SEND_REGISTERED: u32 = 1 << 3;
/// Payload for a completed single-window operation remains readable in the slot until `finish()`.
pub const FLAG_IPC_INLINE_READY: u32 = 1 << 4;

/// IPC completion error status codes.
pub const STATUS_OK: i32 = 0;
pub const STATUS_GENERAL_ERR: i32 = -1;
pub const STATUS_CONNECTION_CLOSED: i32 = -2;
pub const STATUS_REMOTE_ACCESS_ERR: i32 = -3;
pub const STATUS_REMOTE_INVALID_REQ: i32 = -4;
pub const STATUS_LOCAL_LEN_ERR: i32 = -5;
pub const STATUS_TRANSPORT_ERR: i32 = -6;

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
pub struct Descriptor {
    pub op: u32,
    pub status: i32,
    pub peer: u32,
    pub tag_or_imm: u64,
    pub connection: u64,
    pub request: u64,
    pub listener: u64,
    pub offset: u64,
    pub total: u64,
    pub rkey: u64,
    pub remote_offset: u64,
    /// Zero means payload is staged in the IPC slot; nonzero references a daemon-mapped shared MR.
    pub local_mr_id: u64,
    pub local_offset: u64,
    pub length: u32,
    pub flags: u32,
}

impl Descriptor {
    pub fn validate(&self) -> Result<()> {
        if !(OP_LISTEN..=OP_RDMA_READ).contains(&self.op) || self.length as usize > WINDOW {
            return Err(Error::Protocol("IPC descriptor"));
        }
        if matches!(self.op, OP_SEND | OP_RDMA_WRITE | OP_RDMA_READ)
            && self
                .offset
                .checked_add(self.length as u64)
                .is_none_or(|n| n > self.total)
        {
            return Err(Error::Protocol("IPC descriptor range"));
        }
        if matches!(self.op, OP_RDMA_WRITE | OP_RDMA_READ)
            && self.remote_offset.checked_add(self.total).is_none()
        {
            return Err(Error::Protocol("IPC remote offset overflow"));
        }
        if self.local_mr_id != 0 && self.local_offset.checked_add(self.length as u64).is_none() {
            return Err(Error::Protocol("IPC local MR offset overflow"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Welcome {
    pub node: u32,
    pub epoch: u64,
    pub digest: [u8; 32],
    pub client: u64,
}

#[cfg(test)]
mod tests {
    #[test]
    fn publication_model() {
        loom::model(|| {
            use loom::sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            };
            let data = Arc::new(AtomicUsize::new(0));
            let state = Arc::new(AtomicUsize::new(0));
            let (d, s) = (data.clone(), state.clone());
            let writer = loom::thread::spawn(move || {
                d.store(42, Ordering::Relaxed);
                s.store(1, Ordering::Release);
            });
            let reader = loom::thread::spawn(move || {
                if state.load(Ordering::Acquire) == 1 {
                    assert_eq!(data.load(Ordering::Relaxed), 42);
                }
            });
            writer.join().unwrap();
            reader.join().unwrap();
        });
    }
}

#[cfg(test)]
mod ownership_model;
