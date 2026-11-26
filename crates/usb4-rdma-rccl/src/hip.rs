//! Optional ROCm HIP asynchronous staging (`libamdhip64.so` loaded dynamically at runtime).
//! No device pointer is ever dereferenced by the CPU.

use libloading::Library;
use std::ffi::c_void;
use usb4_rdma_core::{Error, Result};
use usb4_rdma_ipc::WINDOW;

type Init = unsafe extern "C" fn(u32) -> i32;
type HostAlloc = unsafe extern "C" fn(*mut *mut c_void, usize, u32) -> i32;
type Free = unsafe extern "C" fn(*mut c_void) -> i32;
type Create = unsafe extern "C" fn(*mut *mut c_void, u32) -> i32;
type CopyAsync = unsafe extern "C" fn(*mut c_void, *const c_void, usize, i32, *mut c_void) -> i32;
type Record = unsafe extern "C" fn(*mut c_void, *mut c_void) -> i32;
type EventQuery = unsafe extern "C" fn(*mut c_void) -> i32;

const SMALL_SPIN_BYTES: usize = 64 * 1024;
const SMALL_SPIN_ITERS: usize = 64;

#[derive(Clone, Copy)]
struct Functions {
    free: Free,
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

pub struct Hip {
    tx_host: usize,
    tx_stream: usize,
    tx_event: usize,
    tx_pending: Option<TxPending>,
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
        Err(Error::Protocol("HIP runtime staging failure"))
    }
}

impl Hip {
    pub fn load() -> Result<Self> {
        // SAFETY: dynamically loads the system ROCm HIP runtime only when explicitly enabled;
        // function pointers are bound to the live `Library` and follow the ROCm HIP C ABI.
        unsafe {
            let lib = Library::new("libamdhip64.so")
                .map_err(|_| Error::Protocol("libamdhip64.so unavailable"))?;
            let symbol = |name: &[u8]| -> Result<*const c_void> {
                Ok(*lib
                    .get::<*const c_void>(name)
                    .map_err(|_| Error::Protocol("HIP symbol unavailable"))?)
            };
            macro_rules! load {
                ($n:literal, $t:ty) => {
                    std::mem::transmute::<*const c_void, $t>(symbol(concat!($n, "\0").as_bytes())?)
                };
            }
            let init = load!("hipInit", Init);
            check(init(0))?;
            let alloc = load!("hipHostMalloc", HostAlloc);
            let stream_create = load!("hipStreamCreateWithFlags", Create);
            let event_create = load!("hipEventCreateWithFlags", Create);
            let f = Functions {
                free: load!("hipHostFree", Free),
                stream_destroy: load!("hipStreamDestroy", Free),
                stream_sync: load!("hipStreamSynchronize", Free),
                event_destroy: load!("hipEventDestroy", Free),
                copy: load!("hipMemcpyAsync", CopyAsync),
                record: load!("hipEventRecord", Record),
                query: load!("hipEventQuery", EventQuery),
            };

            let mut tx_host = std::ptr::null_mut();
            check(alloc(&mut tx_host, WINDOW, 0x4000_0000))?;

            let mut rx_host0 = std::ptr::null_mut();
            if let Err(e) = check(alloc(&mut rx_host0, WINDOW, 0x4000_0000)) {
                (f.free)(tx_host);
                return Err(e);
            }
            let mut rx_host1 = std::ptr::null_mut();
            if let Err(e) = check(alloc(&mut rx_host1, WINDOW, 0x4000_0000)) {
                (f.free)(rx_host0);
                (f.free)(tx_host);
                return Err(e);
            }

            let mut tx_stream = std::ptr::null_mut();
            let mut rx_stream = std::ptr::null_mut();
            let mut tx_event = std::ptr::null_mut();
            let mut rx_event0 = std::ptr::null_mut();
            let mut rx_event1 = std::ptr::null_mut();

            let mut init_sync = || -> Result<()> {
                check(stream_create(&mut tx_stream, 1))?;
                check(stream_create(&mut rx_stream, 1))?;
                check(event_create(&mut tx_event, 0x8000_0002))?;
                check(event_create(&mut rx_event0, 0x8000_0002))?;
                check(event_create(&mut rx_event1, 0x8000_0002))?;
                Ok(())
            };
            if let Err(e) = init_sync() {
                for ev in [tx_event, rx_event0, rx_event1] {
                    if !ev.is_null() {
                        (f.event_destroy)(ev);
                    }
                }
                for st in [tx_stream, rx_stream] {
                    if !st.is_null() {
                        (f.stream_destroy)(st);
                    }
                }
                (f.free)(rx_host1);
                (f.free)(rx_host0);
                (f.free)(tx_host);
                return Err(e);
            }

            Ok(Self {
                tx_host: tx_host as usize,
                tx_stream: tx_stream as usize,
                tx_event: tx_event as usize,
                tx_pending: None,
                rx_hosts: [rx_host0 as usize, rx_host1 as usize],
                rx_events: [rx_event0 as usize, rx_event1 as usize],
                rx_pending: [None, None],
                rx_stream: rx_stream as usize,
                f,
                _library: lib,
            })
        }
    }

    fn poll_event(&self, event: usize, stream: usize) -> Result<bool> {
        // SAFETY: `event` and `stream` are owned HIP handles kept live by `_library`.
        let result = unsafe { (self.f.query)(event as *mut c_void) };
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
        // SAFETY: `tx_stream` is an owned HIP handle kept live by `_library`.
        let c0 = unsafe { (self.f.stream_sync)(self.tx_stream as *mut c_void) };
        // SAFETY: `rx_stream` is an owned HIP handle kept live by `_library`.
        let c1 = unsafe { (self.f.stream_sync)(self.rx_stream as *mut c_void) };
        self.tx_pending = None;
        self.rx_pending = [None, None];
        check(c0)?;
        check(c1)
    }

    fn launch_d2h(&mut self, request: u64, address: usize, length: usize) -> Result<()> {
        // SAFETY: caller retains device pointer until request completes; `tx_host` is WINDOW bytes.
        unsafe {
            check((self.f.copy)(
                self.tx_host as *mut c_void,
                address as *const c_void,
                length,
                2,
                self.tx_stream as *mut c_void,
            ))?;
            if let Err(e) = check((self.f.record)(
                self.tx_event as *mut c_void,
                self.tx_stream as *mut c_void,
            )) {
                (self.f.stream_sync)(self.tx_stream as *mut c_void);
                return Err(e);
            }
        }
        self.tx_pending = Some(TxPending {
            request,
            address,
            length,
            ready: false,
        });
        Ok(())
    }

    /// Stage `length <= WINDOW` bytes from GPU memory into `tx_host` and borrow when ready.
    pub fn prepare_send(&mut self, request: u64, address: usize, length: usize) -> Result<bool> {
        if length == 0 {
            return Ok(true);
        }
        debug_assert!(length <= WINDOW);
        if let Some(mut p) = self.tx_pending {
            if !p.ready {
                if self.poll_event(self.tx_event, self.tx_stream)? {
                    p.ready = true;
                    self.tx_pending = Some(p);
                } else {
                    return Ok(false);
                }
            }
            if p.request == request && p.address == address && p.length == length {
                return Ok(true);
            }
            // Different stale/unrelated staged transfer was retired without consuming.
            self.tx_pending = None;
        }
        self.launch_d2h(request, address, length)?;
        if length <= SMALL_SPIN_BYTES {
            for _ in 0..SMALL_SPIN_ITERS {
                if self.poll_event(self.tx_event, self.tx_stream)? {
                    if let Some(p) = self.tx_pending.as_mut() {
                        p.ready = true;
                    }
                    return Ok(true);
                }
                std::hint::spin_loop();
            }
        }
        Ok(false)
    }

    /// Borrow staged TX bytes after `prepare_send` returned `true`.
    pub fn tx_slice(&self, length: usize) -> &[u8] {
        if length == 0 {
            return &[];
        }
        // SAFETY: `tx_host` holds `WINDOW` pinned bytes and caller synchronized via `prepare_send`.
        unsafe { std::slice::from_raw_parts(self.tx_host as *const u8, length) }
    }

    /// Consume the active TX staging buffer and optionally prefetch the next D2H window.
    pub fn finish_send_and_prefetch(
        &mut self,
        request: u64,
        next: Option<(usize, usize)>,
    ) -> Result<()> {
        self.tx_pending = None;
        if let Some((addr, len)) = next
            && len > 0
        {
            self.launch_d2h(request, addr, len)?;
        }
        Ok(())
    }

    /// Copy `data` into the pinned `slot_idx` host buffer and launch async H2D copy to GPU.
    /// Returns `Ok(false)` only if `slot_idx` is still occupied by an incomplete GPU H2D copy.
    pub fn launch_recv(
        &mut self,
        slot_idx: usize,
        request: u64,
        address: usize,
        data: &[u8],
    ) -> Result<bool> {
        if data.is_empty() {
            return Ok(true);
        }
        debug_assert!(slot_idx < 2 && data.len() <= WINDOW);
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

impl Drop for Hip {
    fn drop(&mut self) {
        // SAFETY: synchronize streams before freeing owned HIP resources while Library is live.
        unsafe {
            (self.f.stream_sync)(self.tx_stream as *mut c_void);
            (self.f.stream_sync)(self.rx_stream as *mut c_void);
            (self.f.event_destroy)(self.tx_event as *mut c_void);
            (self.f.event_destroy)(self.rx_events[0] as *mut c_void);
            (self.f.event_destroy)(self.rx_events[1] as *mut c_void);
            (self.f.stream_destroy)(self.tx_stream as *mut c_void);
            (self.f.stream_destroy)(self.rx_stream as *mut c_void);
            (self.f.free)(self.tx_host as *mut c_void);
            (self.f.free)(self.rx_hosts[0] as *mut c_void);
            (self.f.free)(self.rx_hosts[1] as *mut c_void);
        }
    }
}
