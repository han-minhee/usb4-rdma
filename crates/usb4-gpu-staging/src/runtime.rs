//! CUDA and HIP asynchronous staging, loaded only on request.
//! No device pointer is ever dereferenced by the CPU.

use crate::{MAX_SLOT, MIN_SLOT};
use libloading::Library;
use std::ffi::c_void;
use usb4_proto::{Error, Result};

#[cfg(feature = "rocm")]
type Init = unsafe extern "C" fn(u32) -> i32;
type HostAlloc = unsafe extern "C" fn(*mut *mut c_void, usize, u32) -> i32;
type Free = unsafe extern "C" fn(*mut c_void) -> i32;
type Create = unsafe extern "C" fn(*mut *mut c_void, u32) -> i32;
type CopyAsync = unsafe extern "C" fn(*mut c_void, *const c_void, usize, i32, *mut c_void) -> i32;
type Record = unsafe extern "C" fn(*mut c_void, *mut c_void) -> i32;
type SetDevice = unsafe extern "C" fn(i32) -> i32;
type GetDevice = unsafe extern "C" fn(*mut i32) -> i32;
type PointerAttributes = unsafe extern "C" fn(*mut PointerInfo, *const c_void) -> i32;
type EventQuery = unsafe extern "C" fn(*mut c_void) -> i32;

const SMALL_SPIN_BYTES: usize = 64 * 1024;
const SMALL_SPIN_ITERS: usize = 64;

// HIP 10.2 extends the common CUDA attributes with two trailing fields.
#[repr(C)]
#[derive(Default)]
struct PointerInfo {
    memory_type: i32,
    device: i32,
    device_pointer: *mut c_void,
    host_pointer: *mut c_void,
    is_managed: i32,
    allocation_flags: u32,
}

#[derive(Clone, Copy)]
struct Functions {
    free: Free,
    set_device: SetDevice,
    pointer_attributes: PointerAttributes,
    stream_destroy: Free,
    stream_sync: Free,
    event_destroy: Free,
    copy: CopyAsync,
    record: Record,
    query: EventQuery,
}

#[derive(Clone, Copy)]
struct TxPending {
    request: u64,
    address: usize,
    length: usize,
    ready: bool,
}

#[derive(Clone, Copy)]
struct RxPending {
    request: u64,
}

pub struct Staging {
    slot_size: usize,
    device: i32,
    tx_hosts: [usize; 2],
    tx_stream: usize,
    tx_events: [usize; 2],
    tx_pending: [Option<TxPending>; 2],
    active_tx: Option<usize>,
    rx_hosts: [usize; 2],
    rx_events: [usize; 2],
    rx_pending: [Option<RxPending>; 2],
    rx_stream: usize,
    f: Functions,
    _library: Library,
}

fn check(code: i32) -> Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(Error::Protocol("GPU runtime staging failure"))
    }
}

#[derive(Clone, Copy)]
pub enum Runtime {
    #[cfg(feature = "rocm")]
    Rocm,
    #[cfg(feature = "cuda")]
    Cuda,
}

impl Staging {
    pub fn load(runtime: Runtime, slot_size: usize) -> Result<Self> {
        if !(MIN_SLOT..=MAX_SLOT).contains(&slot_size) {
            return Err(Error::InvalidArg("GPU staging slot size"));
        }
        let (library, prefix, host_alloc, host_free, host_flags, event_flags) = match runtime {
            #[cfg(feature = "rocm")]
            Runtime::Rocm => (
                "libamdhip64.so",
                "hip",
                "HostMalloc",
                "HostFree",
                0x4000_0000,
                0x8000_0002,
            ),
            #[cfg(feature = "cuda")]
            Runtime::Cuda => ("libcudart.so.13", "cuda", "HostAlloc", "FreeHost", 0, 2),
        };
        // SAFETY: dynamically loads the system ROCm HIP runtime only when explicitly enabled;
        // function pointers are bound to the live `Library` and follow the ROCm HIP C ABI.
        unsafe {
            let lib =
                Library::new(library).map_err(|_| Error::Protocol("GPU runtime unavailable"))?;
            let symbol = |name: &[u8]| -> Result<*const c_void> {
                Ok(*lib
                    .get::<*const c_void>(name)
                    .map_err(|_| Error::Protocol("GPU runtime symbol unavailable"))?)
            };
            macro_rules! load {
                ($n:expr, $t:ty) => {
                    std::mem::transmute::<*const c_void, $t>(symbol(
                        format!("{prefix}{}\0", $n).as_bytes(),
                    )?)
                };
            }
            #[cfg(feature = "rocm")]
            if matches!(runtime, Runtime::Rocm) {
                let init = load!("Init", Init);
                check(init(0))?;
            }
            let get_device = load!("GetDevice", GetDevice);
            let mut device = 0;
            check(get_device(&mut device))?;
            let alloc = load!(host_alloc, HostAlloc);
            let stream_create = load!("StreamCreateWithFlags", Create);
            let event_create = load!("EventCreateWithFlags", Create);
            let f = Functions {
                free: load!(host_free, Free),
                set_device: load!("SetDevice", SetDevice),
                pointer_attributes: load!("PointerGetAttributes", PointerAttributes),
                stream_destroy: load!("StreamDestroy", Free),
                stream_sync: load!("StreamSynchronize", Free),
                event_destroy: load!("EventDestroy", Free),
                copy: load!("MemcpyAsync", CopyAsync),
                record: load!("EventRecord", Record),
                query: load!("EventQuery", EventQuery),
            };

            let mut hosts: [*mut c_void; 4] = [std::ptr::null_mut(); 4];
            let mut streams: [*mut c_void; 2] = [std::ptr::null_mut(); 2];
            let mut events: [*mut c_void; 4] = [std::ptr::null_mut(); 4];
            let initialize = (|| {
                for host in &mut hosts {
                    check(alloc(host, slot_size, host_flags))?;
                }
                for stream in &mut streams {
                    check(stream_create(stream, 1))?;
                }
                for event in &mut events {
                    check(event_create(event, event_flags))?;
                }
                Ok(())
            })();
            if let Err(error) = initialize {
                for event in events.into_iter().filter(|p| !p.is_null()) {
                    (f.event_destroy)(event);
                }
                for stream in streams.into_iter().filter(|p| !p.is_null()) {
                    (f.stream_destroy)(stream);
                }
                for host in hosts.into_iter().filter(|p| !p.is_null()) {
                    (f.free)(host);
                }
                return Err(error);
            }
            Ok(Self {
                slot_size,
                device,
                tx_hosts: [hosts[0] as usize, hosts[1] as usize],
                tx_stream: streams[0] as usize,
                tx_events: [events[0] as usize, events[1] as usize],
                tx_pending: [None, None],
                active_tx: None,
                rx_hosts: [hosts[2] as usize, hosts[3] as usize],
                rx_events: [events[2] as usize, events[3] as usize],
                rx_pending: [None, None],
                rx_stream: streams[1] as usize,
                f,
                _library: lib,
            })
        }
    }

    /// Validate the vendor memory domain before an adapter retains the pointer.
    ///
    /// # Safety
    /// The pointer belongs to the caller's live allocation and may be classified by this runtime.
    pub unsafe fn validate_pointer(&self, address: *const c_void, device: bool) -> Result<()> {
        let mut attributes = PointerInfo::default();
        // SAFETY: both versions of the runtime use this common prefix, with enough space for HIP.
        let status = unsafe { (self.f.pointer_attributes)(&mut attributes, address) };
        let valid = if device {
            status == 0
                && matches!(attributes.memory_type, 2 | 3 | 11)
                && attributes.device == self.device
        } else {
            // Some HIP releases report InvalidValue for ordinary pageable system allocations.
            status == 1 || (status == 0 && matches!(attributes.memory_type, 0 | 1))
        };
        if !valid {
            return Err(Error::InvalidArg("GPU pointer domain or device"));
        }
        Ok(())
    }

    fn poll_event(&self, event: usize, stream: usize) -> Result<bool> {
        // SAFETY: these are owned GPU handles kept live by the runtime library.
        let result = unsafe {
            check((self.f.set_device)(self.device))?;
            (self.f.query)(event as *mut c_void)
        };
        if result == 600 {
            return Ok(false);
        }
        if result != 0 {
            // SAFETY: synchronize stream on error before returning.
            unsafe {
                (self.f.stream_sync)(stream as *mut c_void);
            }
            check(result)?;
        }
        Ok(true)
    }

    pub fn drain(&mut self) -> Result<()> {
        // SAFETY: synchronize only the streams owned by this staging pool.
        unsafe {
            check((self.f.set_device)(self.device))?;
            let tx = (self.f.stream_sync)(self.tx_stream as *mut c_void);
            let rx = (self.f.stream_sync)(self.rx_stream as *mut c_void);
            check(tx)?;
            check(rx)?;
        }
        self.tx_pending = [None, None];
        self.active_tx = None;
        self.rx_pending = [None, None];
        Ok(())
    }

    fn launch_d2h(
        &mut self,
        slot: usize,
        request: u64,
        address: usize,
        length: usize,
    ) -> Result<()> {
        if address == 0 || length == 0 || length > self.slot_size {
            return Err(Error::InvalidArg("GPU staging range"));
        }
        // SAFETY: caller retains the device pointer and the selected idle host slot is allocated.
        unsafe {
            check((self.f.set_device)(self.device))?;
            check((self.f.copy)(
                self.tx_hosts[slot] as *mut c_void,
                address as *const c_void,
                length,
                2,
                self.tx_stream as *mut c_void,
            ))?;
            if let Err(error) = check((self.f.record)(
                self.tx_events[slot] as *mut c_void,
                self.tx_stream as *mut c_void,
            )) {
                (self.f.stream_sync)(self.tx_stream as *mut c_void);
                return Err(error);
            }
        }
        self.tx_pending[slot] = Some(TxPending {
            request,
            address,
            length,
            ready: false,
        });
        Ok(())
    }

    /// Stage a device range for sending.
    ///
    /// # Safety
    /// The range belongs to the device context and must remain live and readable until retired.
    pub unsafe fn prepare_send(
        &mut self,
        request: u64,
        address: usize,
        length: usize,
    ) -> Result<bool> {
        self.active_tx = None;
        if length == 0 {
            return Ok(true);
        }
        if length > self.slot_size {
            return Err(Error::InvalidArg("GPU staging length"));
        }
        let slot = if let Some(slot) = self.tx_pending.iter().position(|state| {
            state
                .is_some_and(|p| p.request == request && p.address == address && p.length == length)
        }) {
            slot
        } else if let Some(slot) = self.tx_pending.iter().position(Option::is_none) {
            self.launch_d2h(slot, request, address, length)?;
            slot
        } else {
            return Ok(false);
        };
        let spins = if length <= SMALL_SPIN_BYTES {
            SMALL_SPIN_ITERS
        } else {
            1
        };
        for _ in 0..spins {
            if self.tx_pending[slot].is_some_and(|p| p.ready)
                || self.poll_event(self.tx_events[slot], self.tx_stream)?
            {
                self.tx_pending[slot]
                    .as_mut()
                    .expect("prepared staging slot")
                    .ready = true;
                self.active_tx = Some(slot);
                return Ok(true);
            }
            std::hint::spin_loop();
        }
        Ok(false)
    }

    pub fn tx_slice(&self, length: usize) -> Result<&[u8]> {
        if length == 0 {
            return Ok(&[]);
        }
        let slot = self
            .active_tx
            .filter(|&i| self.tx_pending[i].is_some_and(|p| p.ready && p.length == length))
            .ok_or(Error::Stale)?;
        // SAFETY: the requested length matches a live slot whose copy has completed.
        Ok(unsafe { std::slice::from_raw_parts(self.tx_hosts[slot] as *const u8, length) })
    }

    /// Consume the ready TX slot and optionally prefetch the next D2H window.
    ///
    /// # Safety
    /// The optional device range must remain live and readable until retired.
    pub unsafe fn finish_send_and_prefetch(
        &mut self,
        request: u64,
        next: Option<(usize, usize)>,
    ) -> Result<()> {
        if let Some(slot) = self.active_tx.take() {
            if self.tx_pending[slot].is_none_or(|pending| pending.request != request) {
                return Err(Error::Stale);
            }
            self.tx_pending[slot] = None;
            if let Some((address, length)) = next {
                self.launch_d2h(slot, request, address, length)?;
            }
        }
        Ok(())
    }

    /// Copy `data` into the pinned `slot_idx` host buffer and launch async H2D copy to GPU.
    /// Returns `Ok(false)` only if `slot_idx` is still occupied by an incomplete GPU H2D copy.
    ///
    /// # Safety
    /// The destination device range must remain live and writable until retired.
    pub unsafe fn launch_recv(
        &mut self,
        slot_idx: usize,
        request: u64,
        address: usize,
        data: &[u8],
    ) -> Result<bool> {
        if data.is_empty() {
            return Ok(true);
        }
        if slot_idx >= 2 || data.len() > self.slot_size {
            return Err(Error::InvalidArg("GPU receive slot or length"));
        }
        if self.rx_pending[slot_idx].is_some() {
            if !self.poll_event(self.rx_events[slot_idx], self.rx_stream)? {
                return Ok(false);
            }
            self.rx_pending[slot_idx] = None;
        }
        // SAFETY: `rx_hosts[slot_idx]` is idle and `WINDOW` bytes; `address` is retained GPU target.
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.rx_hosts[slot_idx] as *mut u8,
                data.len(),
            );
            check((self.f.set_device)(self.device))?;
            check((self.f.copy)(
                address as *mut c_void,
                self.rx_hosts[slot_idx] as *const c_void,
                data.len(),
                1,
                self.rx_stream as *mut c_void,
            ))?;
            if let Err(e) = check((self.f.record)(
                self.rx_events[slot_idx] as *mut c_void,
                self.rx_stream as *mut c_void,
            )) {
                (self.f.stream_sync)(self.rx_stream as *mut c_void);
                return Err(e);
            }
        }
        self.rx_pending[slot_idx] = Some(RxPending { request });
        Ok(true)
    }

    /// Check whether all async H2D copies launched on behalf of `request` have finished.
    pub fn rx_complete(&mut self, request: u64, allow_short_spin: bool) -> Result<bool> {
        let spins = if allow_short_spin {
            SMALL_SPIN_ITERS
        } else {
            1
        };
        for _ in 0..spins {
            let mut busy = false;
            for i in 0..2 {
                if self.rx_pending[i].is_some_and(|p| p.request == request) {
                    if self.poll_event(self.rx_events[i], self.rx_stream)? {
                        self.rx_pending[i] = None;
                    } else {
                        busy = true;
                    }
                }
            }
            if !busy {
                return Ok(true);
            }
            if allow_short_spin {
                std::hint::spin_loop();
            }
        }
        Ok(false)
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        // SAFETY: synchronize streams before freeing owned HIP resources while Library is live.
        unsafe {
            if (self.f.set_device)(self.device) != 0
                || (self.f.stream_sync)(self.tx_stream as *mut c_void) != 0
                || (self.f.stream_sync)(self.rx_stream as *mut c_void) != 0
            {
                // Runtime ownership is uncertain. Leave pinned resources quarantined.
                return;
            }
            for event in self.tx_events.into_iter().chain(self.rx_events) {
                (self.f.event_destroy)(event as *mut c_void);
            }
            (self.f.stream_destroy)(self.tx_stream as *mut c_void);
            (self.f.stream_destroy)(self.rx_stream as *mut c_void);
            for host in self.tx_hosts.into_iter().chain(self.rx_hosts) {
                (self.f.free)(host as *mut c_void);
            }
        }
    }
}
