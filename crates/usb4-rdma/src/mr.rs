//! Local borrowed and daemon-shared Memory Region abstractions.

use crate::context::ContextInner;
use std::{
    os::fd::OwnedFd,
    sync::{Arc, Mutex},
};
use usb4_rdma_core::{Error, Result, rdma::AccessFlags};
use usb4_rdma_ipc::mr::SharedRegion;

/// Common trait implemented by both `OwnedMr` (shared) and `BorrowedMr` (local host pointer).
pub trait MrRef {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn access(&self) -> AccessFlags;
    fn lkey(&self) -> u64;
    fn rkey(&self) -> u64;
    /// Daemon-registered shared MR ID, or `0` for a borrowed local pointer.
    fn daemon_mr_id(&self) -> u64;
    /// Read bytes from this MR into a temporary slice (used when staging through IPC).
    fn read_local(&self, offset: usize, out: &mut [u8]) -> Result<()>;
    /// Write bytes into this MR from a temporary slice (used when draining from IPC).
    fn write_local(&self, offset: usize, data: &[u8]) -> Result<()>;
}

/// Shared-memory Memory Region mapped into both the application process and `usb4-rdma-daemon`.
///
/// Supports `LOCAL_READ`, `LOCAL_WRITE`, `REMOTE_READ`, and `REMOTE_WRITE`.
/// Remote peers can execute one-sided `RDMA_WRITE` and `RDMA_READ` directly against
/// this region using `(rkey(), offset)` without target-application involvement.
pub struct OwnedMr {
    pub(crate) ctx: Arc<Mutex<ContextInner>>,
    pub(crate) region: SharedRegion,
    pub(crate) access: AccessFlags,
    pub(crate) mr_id: u64,
    pub(crate) rkey: u64,
}

impl OwnedMr {
    pub(crate) fn allocate(
        ctx: Arc<Mutex<ContextInner>>,
        len: usize,
        access: AccessFlags,
    ) -> Result<Self> {
        let region = SharedRegion::allocate(len)?;
        Self::register_region(ctx, region, access)
    }

    pub(crate) fn from_fd(
        ctx: Arc<Mutex<ContextInner>>,
        fd: OwnedFd,
        len: usize,
        access: AccessFlags,
    ) -> Result<Self> {
        let writable =
            access.contains(AccessFlags::LOCAL_WRITE) || access.contains(AccessFlags::REMOTE_WRITE);
        let region = SharedRegion::map_fd(fd, len, writable)?;
        Self::register_region(ctx, region, access)
    }

    fn register_region(
        ctx: Arc<Mutex<ContextInner>>,
        region: SharedRegion,
        access: AccessFlags,
    ) -> Result<Self> {
        let (mr_id, rkey) = {
            let mut inner = ctx.lock().map_err(|_| Error::Disconnected)?;
            inner
                .client
                .register_mr(region.raw_fd(), region.len(), access)?
        };
        Ok(Self {
            ctx,
            region,
            access,
            mr_id,
            rkey,
        })
    }

    pub fn len(&self) -> usize {
        self.region.len()
    }

    pub fn is_empty(&self) -> bool {
        self.region.is_empty()
    }

    pub fn access(&self) -> AccessFlags {
        self.access
    }

    pub fn lkey(&self) -> u64 {
        self.mr_id
    }

    pub fn rkey(&self) -> u64 {
        self.rkey
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.region.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> Option<*mut u8> {
        self.region.as_mut_ptr()
    }

    /// Bounds-checked copy out of the shared memory region.
    pub fn read_bytes(&self, offset: usize, out: &mut [u8]) -> Result<()> {
        self.region.read_range(offset, out)
    }

    /// Bounds-checked copy into the shared memory region.
    pub fn write_bytes(&self, offset: usize, data: &[u8]) -> Result<()> {
        self.region.write_range(offset, data)
    }
}

impl MrRef for OwnedMr {
    fn len(&self) -> usize {
        self.region.len()
    }
    fn access(&self) -> AccessFlags {
        self.access
    }
    fn lkey(&self) -> u64 {
        self.mr_id
    }
    fn rkey(&self) -> u64 {
        self.rkey
    }
    fn daemon_mr_id(&self) -> u64 {
        self.mr_id
    }
    fn read_local(&self, offset: usize, out: &mut [u8]) -> Result<()> {
        self.region.read_range(offset, out)
    }
    fn write_local(&self, offset: usize, data: &[u8]) -> Result<()> {
        self.region.write_range(offset, data)
    }
}

impl Drop for OwnedMr {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.ctx.lock() {
            let _ = inner.client.deregister_mr(self.mr_id);
        }
    }
}

/// Borrowed host-pointer Memory Region for local operands (`LOCAL_READ | LOCAL_WRITE` only).
///
/// Requesting `REMOTE_READ` or `REMOTE_WRITE` fails closed with `Error::UnsupportedAccess`;
/// use `ProtectionDomain::alloc_mr` or `reg_mr_fd` for remotely accessible RDMA regions.
pub struct BorrowedMr {
    ptr: usize,
    len: usize,
    access: AccessFlags,
    lkey: u64,
}

impl BorrowedMr {
    /// # Safety
    /// `ptr` must remain valid for reads (and writes if `LOCAL_WRITE` is set) across
    /// `len` bytes until this `BorrowedMr` and every operation referencing it have completed.
    pub(crate) unsafe fn new(
        ptr: *mut u8,
        len: usize,
        access: AccessFlags,
        lkey: u64,
    ) -> Result<Self> {
        if access.has_remote() {
            return Err(Error::UnsupportedAccess);
        }
        if len > 0 && ptr.is_null() {
            return Err(Error::InvalidArg("null pointer with nonzero length"));
        }
        let base = ptr as usize;
        if base.checked_add(len).is_none() {
            return Err(Error::InvalidArg("pointer range overflow"));
        }
        Ok(Self {
            ptr: base,
            len,
            access,
            lkey,
        })
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr as *mut u8
    }
}

impl MrRef for BorrowedMr {
    fn len(&self) -> usize {
        self.len
    }
    fn access(&self) -> AccessFlags {
        self.access
    }
    fn lkey(&self) -> u64 {
        self.lkey
    }
    fn rkey(&self) -> u64 {
        0
    }
    fn daemon_mr_id(&self) -> u64 {
        0
    }
    fn read_local(&self, offset: usize, out: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(out.len())
            .ok_or(Error::AccessViolation)?;
        if end > self.len {
            return Err(Error::AccessViolation);
        }
        if out.is_empty() {
            return Ok(());
        }
        // SAFETY: caller guaranteed `self.ptr..self.ptr + self.len` stays valid for the
        // lifetime of `BorrowedMr` and all active operations; bounds were checked above.
        unsafe {
            std::ptr::copy_nonoverlapping(
                (self.ptr as *const u8).add(offset),
                out.as_mut_ptr(),
                out.len(),
            );
        }
        Ok(())
    }

    fn write_local(&self, offset: usize, data: &[u8]) -> Result<()> {
        if !self.access.contains(AccessFlags::LOCAL_WRITE) {
            return Err(Error::AccessViolation);
        }
        let end = offset
            .checked_add(data.len())
            .ok_or(Error::AccessViolation)?;
        if end > self.len {
            return Err(Error::AccessViolation);
        }
        if data.is_empty() {
            return Ok(());
        }
        // SAFETY: caller guaranteed writable range for the lifetime of `BorrowedMr` and
        // active operations; `LOCAL_WRITE` and bounds were checked above.
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                (self.ptr as *mut u8).add(offset),
                data.len(),
            );
        }
        Ok(())
    }
}
