//! CPU access to a runtime-verified coherent UMA allocation.

use std::ptr::NonNull;
use usb4_proto::{Error, Result};

pub struct CoherentRegion<'a> {
    bytes: &'a mut [u8],
}

impl<'a> CoherentRegion<'a> {
    /// Borrow a range after the runtime has classified the allocation and synchronized its producer.
    ///
    /// # Safety
    /// This must be the CPU mapping of a live, cache-coherent allocation. The caller
    /// holds exclusive access through the borrow and synchronizes the next device use.
    pub unsafe fn from_mapping(mapping: NonNull<u8>, len: usize) -> Result<Self> {
        if len > isize::MAX as usize {
            return Err(Error::InvalidArg("UMA mapping length"));
        }
        // SAFETY: the verified CPU mapping and lifetime are guaranteed by the caller.
        let bytes = unsafe { std::slice::from_raw_parts_mut(mapping.as_ptr(), len) };
        Ok(Self { bytes })
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.bytes
    }
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        self.bytes
    }
}
