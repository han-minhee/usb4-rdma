use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    ptr::NonNull,
};
use usb4_proto::{Error, Result};

/// Page-aligned host allocation owned until `Drop`.
pub struct AlignedRegion {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl AlignedRegion {
    pub fn new(length: usize) -> Result<Self> {
        if length == 0 {
            return Err(Error::Protocol("zero allocation"));
        }
        // SAFETY: sysconf takes no pointers and only queries this process's platform.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page <= 0 {
            return Err(Error::Protocol("page size"));
        }
        let layout =
            Layout::from_size_align(length, page as usize).map_err(|_| Error::Exhausted)?;
        // SAFETY: layout is nonzero and validated; ownership is retained until Drop.
        let ptr = NonNull::new(unsafe { alloc_zeroed(layout) }).ok_or(Error::Exhausted)?;
        Ok(Self { ptr, layout })
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: allocation is initialized, uniquely borrowed, and lives with self.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl Drop for AlignedRegion {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated once with this exact layout and remains owned.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

/// Validates a borrowed subrange without dereferencing or assuming CPU readability.
pub fn contains(base: usize, length: usize, address: usize, size: usize) -> bool {
    address >= base
        && address
            .checked_add(size)
            .zip(base.checked_add(length))
            .is_some_and(|(a, b)| a <= b)
}

/// Memory domain descriptor for future vendor-neutral accelerator / DMA-BUF plugins.
///
/// The baseline `usb4-rdma` fabric operates exclusively on host memory (`Host`).
/// Future accelerator plugins (CUDA, ROCm/HIP, Level Zero, Vulkan DMA-BUF) can
/// integrate through explicit external staging or kernel DMA-BUF hooks without
/// coupling the core fabric to any GPU vendor runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryDomain {
    Host,
    DmaBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unaligned_ranges() {
        assert!(contains(4096, 100, 4097, 3));
        assert!(!contains(4096, 100, usize::MAX, 2));
    }
}
