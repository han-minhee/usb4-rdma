//! Cross-process shared memory regions backed by sealed `memfd` files.

use crate::{Error, Result};
use std::{
    fs::File,
    os::fd::{AsRawFd, OwnedFd},
    ptr::NonNull,
};

/// Page-aligned shared memory mapping suitable for daemon-served one-sided RDMA.
pub struct SharedRegion {
    ptr: NonNull<u8>,
    mapped_len: usize,
    len: usize,
    writable: bool,
    fd: OwnedFd,
}

// SAFETY: SharedRegion owns its mmap range and FD; callers synchronize concurrent
// remote/local byte accesses per the RDMA memory ordering contract.
unsafe impl Send for SharedRegion {}
// SAFETY: Read/write helpers perform bounds-checked raw pointer operations without
// exposing aliasing `&mut [u8]` references across threads or processes.
unsafe impl Sync for SharedRegion {}

fn page_size() -> Result<usize> {
    // SAFETY: sysconf queries a constant process property with no pointers.
    let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if p <= 0 {
        return Err(Error::Protocol("invalid page size"));
    }
    Ok(p as usize)
}

fn align_up(len: usize, page: usize) -> Result<usize> {
    let min_len = len.max(1);
    let rem = min_len % page;
    if rem == 0 {
        Ok(min_len)
    } else {
        min_len.checked_add(page - rem).ok_or(Error::Exhausted)
    }
}

impl SharedRegion {
    /// Allocate a new page-aligned shared memory region of at least `len` usable bytes.
    pub fn allocate(len: usize) -> Result<Self> {
        let page = page_size()?;
        let mapped_len = align_up(len, page)?;

        let fd = {
            use std::os::fd::FromRawFd;
            // SAFETY: static NUL-terminated C string; returned FD is newly owned.
            let raw = unsafe {
                libc::memfd_create(
                    c"u4rd-mr".as_ptr(),
                    libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
                )
            };
            if raw < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            // SAFETY: raw is a valid newly created FD with no other Rust owner.
            unsafe { OwnedFd::from_raw_fd(raw) }
        };

        // SAFETY: fd is live and mapped_len fits in off_t on 64-bit targets.
        if unsafe { libc::ftruncate(fd.as_raw_fd(), mapped_len as libc::off_t) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }

        // SAFETY: prevent peer resizing/truncation while keeping shared writes enabled.
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

        Self::map_fd(fd, len, true)
    }

    /// Map an existing shared-memory file descriptor with explicit byte length and writability.
    pub fn map_fd(fd: OwnedFd, len: usize, writable: bool) -> Result<Self> {
        let file = File::from(fd);
        let file_len = file.metadata()?.len();
        let mapped_len = usize::try_from(file_len).map_err(|_| Error::Exhausted)?;
        if mapped_len == 0 || len > mapped_len {
            return Err(Error::Protocol("MR backing size smaller than length"));
        }
        let fd = OwnedFd::from(file);
        let prot = if writable {
            libc::PROT_READ | libc::PROT_WRITE
        } else {
            libc::PROT_READ
        };
        // SAFETY: fd length was checked; mapping is owned exclusively until Drop.
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                mapped_len,
                prot,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if p == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error().into());
        }
        let ptr = NonNull::new(p.cast::<u8>()).ok_or(Error::Exhausted)?;
        Ok(Self {
            ptr,
            mapped_len,
            len,
            writable,
            fd,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn mapped_len(&self) -> usize {
        self.mapped_len
    }

    pub fn writable(&self) -> bool {
        self.writable
    }

    pub fn raw_fd(&self) -> std::os::fd::RawFd {
        self.fd.as_raw_fd()
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> Option<*mut u8> {
        self.writable.then_some(self.ptr.as_ptr())
    }

    /// Bounds-checked copy out of the shared mapping.
    pub fn read_range(&self, offset: usize, out: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(out.len())
            .ok_or(Error::AccessViolation)?;
        if end > self.len {
            return Err(Error::AccessViolation);
        }
        if out.is_empty() {
            return Ok(());
        }
        // SAFETY: `offset..end` is verified within `self.len <= self.mapped_len`
        // and `out` is a distinct Rust buffer not aliasing the shared mapping.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.ptr.as_ptr().add(offset),
                out.as_mut_ptr(),
                out.len(),
            );
        }
        Ok(())
    }

    /// Bounds-checked and permission-checked copy into the shared mapping.
    pub fn write_range(&self, offset: usize, data: &[u8]) -> Result<()> {
        if !self.writable {
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
        // SAFETY: `self.writable` guarantees `PROT_WRITE`, `offset..end` is within
        // `self.len <= self.mapped_len`, and `data` does not overlap this mapping.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr().add(offset), data.len());
        }
        Ok(())
    }
}

impl Drop for SharedRegion {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` and `self.mapped_len` came from a single successful `mmap`.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.mapped_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_read_write_and_bounds() {
        let r = SharedRegion::allocate(5000).unwrap();
        assert_eq!(r.len(), 5000);
        assert!(r.mapped_len() >= 5000);
        r.write_range(128, b"hello-rdma").unwrap();
        let mut out = [0u8; 10];
        r.read_range(128, &mut out).unwrap();
        assert_eq!(&out, b"hello-rdma");
        assert!(matches!(
            r.write_range(4995, b"123456"),
            Err(Error::AccessViolation)
        ));
    }
}
