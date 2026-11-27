//! Level Zero pinned copy slots for caller-owned immediate command lists.

use crate::{MAX_SLOT, MIN_SLOT};
use libloading::Library;
use std::{ffi::c_void, ptr};
use usb4_proto::{Error, Result};

type Handle = *mut c_void;
type Allocate = unsafe extern "C" fn(Handle, *const HostAlloc, usize, usize, *mut Handle) -> u32;
type Free = unsafe extern "C" fn(Handle, Handle) -> u32;
type Copy =
    unsafe extern "C" fn(Handle, Handle, *const c_void, usize, Handle, u32, *const Handle) -> u32;
type Sync = unsafe extern "C" fn(Handle, u64) -> u32;

#[repr(C)]
struct HostAlloc {
    structure_type: u32,
    next: *const c_void,
    flags: u32,
}

pub struct Slot {
    context: Handle,
    queue: Handle,
    host: Handle,
    size: usize,
    pending: bool,
    free: Free,
    copy: Copy,
    sync: Sync,
    _runtime: Library,
}

impl Slot {
    /// Allocate a pinned slot on an initialized Level Zero context.
    ///
    /// # Safety
    /// `queue` is a live exclusive immediate command list within `context`. Both
    /// remain valid through the slot's destruction.
    pub unsafe fn new(context: Handle, queue: Handle, size: usize) -> Result<Self> {
        if context.is_null() || queue.is_null() || !(MIN_SLOT..=MAX_SLOT).contains(&size) {
            return Err(Error::InvalidArg("Level Zero staging slot"));
        }
        // SAFETY: the pinned library and symbol signatures use the Level Zero C ABI.
        unsafe {
            let runtime = Library::new("libze_loader.so.1")
                .map_err(|_| Error::Protocol("Level Zero unavailable"))?;
            let alloc = *runtime
                .get::<Allocate>(b"zeMemAllocHost\0")
                .map_err(|_| Error::Protocol("zeMemAllocHost unavailable"))?;
            let free = *runtime
                .get::<Free>(b"zeMemFree\0")
                .map_err(|_| Error::Protocol("zeMemFree unavailable"))?;
            let copy = *runtime
                .get::<Copy>(b"zeCommandListAppendMemoryCopy\0")
                .map_err(|_| Error::Protocol("Level Zero async copy unavailable"))?;
            let sync = *runtime
                .get::<Sync>(b"zeCommandListHostSynchronize\0")
                .map_err(|_| {
                    Error::Protocol("Level Zero immediate-list synchronization unavailable")
                })?;
            let desc = HostAlloc {
                structure_type: 0x16,
                next: ptr::null(),
                flags: 0,
            };
            let mut host = ptr::null_mut();
            if alloc(context, &desc, size, 64, &mut host) != 0 || host.is_null() {
                return Err(Error::Exhausted);
            }
            ptr::write_bytes(host.cast::<u8>(), 0, size);
            Ok(Self {
                context,
                queue,
                host,
                size,
                pending: false,
                free,
                copy,
                sync,
                _runtime: runtime,
            })
        }
    }

    pub fn host(&mut self) -> Result<&mut [u8]> {
        if self.pending {
            return Err(Error::Exhausted);
        }
        // SAFETY: the initialized slot is exclusively borrowed and no copy owns it.
        Ok(unsafe { std::slice::from_raw_parts_mut(self.host.cast(), self.size) })
    }

    /// Enqueue a GPU copy from or into this slot.
    ///
    /// # Safety
    /// The device range belongs to the slot context and remains valid until `poll` reports ready.
    pub unsafe fn submit(&mut self, device: Handle, len: usize, from_device: bool) -> Result<()> {
        if self.pending || device.is_null() || len > self.size {
            return Err(Error::InvalidArg("Level Zero copy range or busy slot"));
        }
        if len == 0 {
            return Ok(());
        }
        let (dst, src) = if from_device {
            (self.host, device)
        } else {
            (device, self.host)
        };
        // SAFETY: the caller retains the device range and this slot retains the host allocation.
        if unsafe { (self.copy)(self.queue, dst, src, len, ptr::null_mut(), 0, ptr::null()) } != 0 {
            return Err(Error::Protocol("Level Zero copy submission"));
        }
        self.pending = true;
        Ok(())
    }

    pub fn poll(&mut self) -> Result<bool> {
        if !self.pending {
            return Ok(true);
        }
        // SAFETY: the immediate queue remains live and is owned exclusively by the slot.
        match unsafe { (self.sync)(self.queue, 0) } {
            0 => {
                self.pending = false;
                Ok(true)
            }
            1 => Ok(false),
            _ => Err(Error::Protocol("Level Zero copy completion")),
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        // SAFETY: both handles remain live. A failed wait leaves the allocation quarantined.
        unsafe {
            if !self.pending || (self.sync)(self.queue, 1_000_000_000) == 0 {
                (self.free)(self.context, self.host);
            }
        }
    }
}
