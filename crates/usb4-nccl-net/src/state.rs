use crate::bindings::*;
use std::{
    ffi::{c_int, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::{Mutex, MutexGuard, OnceLock, TryLockError},
    time::{Duration, Instant},
};
use usb4_rdma_core::ipc::*;
use usb4_rdma_core::{Error, pool::Pool};

const OK: c_int = 0;
const SYSTEM: c_int = 2;
const INTERNAL: c_int = 3;
const USAGE: c_int = 5;
const MAX_RECVS: usize = 8;
const HANDLE_MAGIC: [u8; 4] = *b"U4RN";

#[derive(Clone, Copy, PartialEq)]
enum CommKind {
    Listen,
    Send,
    Recv,
}

#[derive(Clone, Copy)]
struct Comm {
    kind: CommKind,
    peer: u32,
    connection: u64,
    listener: u64,
    pending: Option<usize>,
    accept: Option<usize>,
    next_send: u64,
    inflight: usize,
    registrations: usize,
}

#[derive(Clone, Copy)]
struct Mr {
    comm: u64,
    base: usize,
    length: usize,
    kind: c_int,
    refs: usize,
}

#[derive(Clone, Copy, Default)]
struct Buffer {
    address: usize,
    capacity: usize,
    mr: u64,
    tag: u64,
    offset: usize,
    total: usize,
    slot: Option<usize>,
    done: bool,
    h2d_flushing: bool,
    rx_seq: usize,
    sequence: u64,
}

#[derive(Clone, Copy)]
struct Request {
    comm: u64,
    send: bool,
    n: usize,
    buffers: [Buffer; MAX_RECVS],
    started: Instant,
}

struct State {
    client: Client,
    last_alive: Instant,
    contexts: Pool<()>,
    comms: Pool<Comm>,
    mrs: Pool<Mr>,
    requests: Pool<Request>,
    listener_counter: u32,
    speed: c_int,
    failed: bool,
    #[cfg(any(feature = "rocm", feature = "cuda"))]
    hip: Option<usb4_gpu_staging::Staging>,
}

static STATE: OnceLock<Mutex<Option<State>>> = OnceLock::new();

struct StateGuard {
    inner: MutexGuard<'static, Option<State>>,
}

impl std::ops::Deref for StateGuard {
    type Target = State;
    fn deref(&self) -> &State {
        self.inner
            .as_ref()
            .expect("initialized state held under mutex")
    }
}

impl std::ops::DerefMut for StateGuard {
    fn deref_mut(&mut self) -> &mut State {
        self.inner
            .as_mut()
            .expect("initialized state held under mutex")
    }
}

fn guard(inner: MutexGuard<'static, Option<State>>) -> std::result::Result<StateGuard, c_int> {
    if inner.as_ref().is_none_or(|s| s.failed) {
        Err(SYSTEM)
    } else {
        Ok(StateGuard { inner })
    }
}

unsafe extern "C" {
    fn u4rd_rccl_log(logger: Logger, message: *const std::ffi::c_char);
}

fn ffi(f: impl FnOnce() -> std::result::Result<(), c_int>) -> c_int {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => OK,
        Ok(Err(e)) => e,
        Err(_) => {
            if let Some(m) = STATE.get() {
                let mut s = m.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(s) = s.as_mut() {
                    s.failed = true;
                    #[cfg(any(feature = "rocm", feature = "cuda"))]
                    if let Some(hip) = s.hip.as_mut() {
                        let _ = hip.drain();
                    }
                }
            }
            INTERNAL
        }
    }
}

fn lock() -> std::result::Result<Option<StateGuard>, c_int> {
    match STATE.get().ok_or(SYSTEM)?.try_lock() {
        Ok(g) => guard(g).map(Some),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(_) => Err(SYSTEM),
    }
}

fn required() -> std::result::Result<StateGuard, c_int> {
    guard(STATE.get().ok_or(SYSTEM)?.lock().map_err(|_| SYSTEM)?)
}

fn id(p: *mut c_void) -> u64 {
    p as usize as u64
}

fn pointer(id: u64) -> *mut c_void {
    std::ptr::without_provenance_mut(id as usize)
}

fn map_error(e: Error) -> c_int {
    match e {
        Error::Stale | Error::Protocol(_) | Error::Exhausted => USAGE,
        _ => SYSTEM,
    }
}

fn empty_comm(kind: CommKind) -> Comm {
    Comm {
        kind,
        peer: 0,
        connection: 0,
        listener: 0,
        pending: None,
        accept: None,
        next_send: 1,
        inflight: 0,
        registrations: 0,
    }
}

pub unsafe extern "C" fn init(
    out: *mut *mut c_void,
    _comm_id: u64,
    _config: *mut c_void,
    logger: Logger,
    _profiler: Profiler,
) -> c_int {
    ffi(|| {
        if out.is_null() {
            return Err(USAGE);
        }
        // SAFETY: the caller provides a writable context output.
        unsafe { out.write(std::ptr::null_mut()) };
        let state = STATE.get_or_init(|| Mutex::new(None));
        let mut state = state.lock().map_err(|_| INTERNAL)?;
        if state.is_none() {
            let path = std::env::var("USB4_RDMA_SOCKET")
                .unwrap_or_else(|_| "/run/usb4-rdma/node.sock".into());
            let client = Client::connect(Path::new(&path)).map_err(map_error)?;
            let speed = std::env::var("USB4_RDMA_SPEED_MBPS")
                .ok()
                .and_then(|s| s.parse::<c_int>().ok())
                .filter(|&n| n > 0)
                .unwrap_or(40000);
            #[cfg(any(feature = "rocm", feature = "cuda"))]
            let hip = {
                let runtime = match std::env::var("USB4_RDMA_GPU_RUNTIME").as_deref() {
                    Ok("host") => None,
                    #[cfg(feature = "rocm")]
                    Ok("rocm") => Some(usb4_gpu_staging::Runtime::Rocm),
                    #[cfg(feature = "cuda")]
                    Ok("cuda") => Some(usb4_gpu_staging::Runtime::Cuda),
                    Ok(_) => return Err(USAGE),
                    #[cfg(all(feature = "rocm", not(feature = "cuda")))]
                    Err(_) => Some(usb4_gpu_staging::Runtime::Rocm),
                    #[cfg(all(feature = "cuda", not(feature = "rocm")))]
                    Err(_) => Some(usb4_gpu_staging::Runtime::Cuda),
                    #[cfg(all(feature = "rocm", feature = "cuda"))]
                    Err(_) => None,
                };
                runtime
                    .map(|runtime| usb4_gpu_staging::Staging::load(runtime, WINDOW))
                    .transpose()
                    .map_err(map_error)?
            };
            *state = Some(State {
                client,
                last_alive: Instant::now(),
                contexts: Pool::new(256),
                comms: Pool::new(256),
                mrs: Pool::new(2048),
                requests: Pool::new(256),
                listener_counter: 0,
                speed,
                failed: false,
                #[cfg(any(feature = "rocm", feature = "cuda"))]
                hip,
            });
        }
        let ctx = state
            .as_mut()
            .filter(|s| !s.failed)
            .ok_or(SYSTEM)?
            .contexts
            .insert(())
            .map_err(map_error)?;
        drop(state);
        // SAFETY: output remains writable and the logger follows the vendor variadic ABI.
        unsafe {
            out.write(pointer(ctx));
            u4rd_rccl_log(logger, c"Usb4Rdma network plugin initialized".as_ptr());
        }
        Ok(())
    })
}

pub unsafe extern "C" fn finalize(ctx: *mut c_void) -> c_int {
    ffi(|| {
        let mut state = STATE.get().ok_or(USAGE)?.lock().map_err(|_| INTERNAL)?;
        let s = state.as_mut().ok_or(USAGE)?;
        s.contexts.remove(id(ctx)).map_err(map_error)?;
        if s.contexts.available() == s.contexts.capacity() {
            let retired = state.take();
            drop(state);
            drop(retired);
        }
        Ok(())
    })
}

pub unsafe extern "C" fn set_net_attr(ctx: *mut c_void, _attributes: *mut c_void) -> c_int {
    ffi(|| {
        required()?
            .contexts
            .get(id(ctx))
            .map(|_| ())
            .map_err(map_error)
    })
}

pub unsafe extern "C" fn devices(out: *mut c_int) -> c_int {
    ffi(|| {
        if out.is_null() {
            return Err(USAGE);
        }
        let mut s = required()?;
        s.client.alive().map_err(map_error)?;
        // SAFETY: RCCL supplies a valid non-null writable int output.
        unsafe {
            *out = 1;
        }
        Ok(())
    })
}

fn properties(s: &State) -> ncclNetProperties_v12_t {
    #[cfg(any(feature = "rocm", feature = "cuda"))]
    let ptr_support = if s.hip.is_some() { 3 } else { 1 };
    #[cfg(not(any(feature = "rocm", feature = "cuda")))]
    let ptr_support = 1;
    ncclNetProperties_v12_t {
        name: c"Usb4Rdma".as_ptr().cast_mut(),
        pciPath: std::ptr::null_mut(),
        guid: s.client.welcome.node as u64,
        ptrSupport: ptr_support,
        regIsGlobal: 0,
        forceFlush: 0,
        speed: s.speed,
        port: 0,
        latency: 0.0,
        maxComms: 256,
        maxRecvs: 1,
        netDeviceType: 0,
        netDeviceVersion: 0,
        vProps: ncclNetVDeviceProps_v12_t {
            ndevs: 1,
            devs: [0; 8],
        },
        maxP2pBytes: i32::MAX as usize,
        maxCollBytes: i32::MAX as usize,
        maxMultiRequestSize: 1,
        railId: -1,
        planeId: -1,
    }
}

pub unsafe extern "C" fn get_properties(dev: c_int, out: *mut ncclNetProperties_v12_t) -> c_int {
    ffi(|| {
        if dev != 0 || out.is_null() {
            return Err(USAGE);
        }
        let s = required()?;
        // SAFETY: `out` is non-null and writable per net v12 ABI.
        unsafe {
            *out = properties(&s);
        }
        Ok(())
    })
}

pub unsafe extern "C" fn listen(
    ctx: *mut c_void,
    dev: c_int,
    handle: *mut c_void,
    out: *mut *mut c_void,
) -> c_int {
    ffi(|| {
        if out.is_null() || handle.is_null() || dev != 0 {
            return Err(USAGE);
        }
        // SAFETY: `out` is non-null and writable.
        unsafe {
            *out = std::ptr::null_mut();
        }
        let mut s = required()?;
        s.contexts.get(id(ctx)).map_err(map_error)?;
        if s.comms.available() == 0 {
            return Err(SYSTEM);
        }
        s.listener_counter = s.listener_counter.checked_add(1).ok_or(SYSTEM)?;
        let listener = (s.client.welcome.client << 32) | s.listener_counter as u64;
        let d = Descriptor {
            op: OP_LISTEN,
            listener,
            ..Default::default()
        };
        let slot = s.client.post(d, &[]).map_err(map_error)?.ok_or(SYSTEM)?;
        let mut comm = empty_comm(CommKind::Listen);
        comm.listener = listener;
        comm.pending = Some(slot);
        let cid = s.comms.insert(comm).map_err(map_error)?;
        // SAFETY: `handle` points to NCCL_NET_HANDLE_MAXSIZE (128) writable bytes.
        unsafe {
            let h = std::slice::from_raw_parts_mut(handle.cast::<u8>(), 128);
            h.fill(0);
            h[..4].copy_from_slice(&HANDLE_MAGIC);
            h[4..8].copy_from_slice(&1u32.to_le_bytes());
            h[8..12].copy_from_slice(&s.client.welcome.node.to_le_bytes());
            h[16..24].copy_from_slice(&s.client.welcome.epoch.to_le_bytes());
            h[24..32].copy_from_slice(&listener.to_le_bytes());
            h[32..64].copy_from_slice(&s.client.welcome.digest);
            *out = pointer(cid);
        }
        Ok(())
    })
}

pub unsafe extern "C" fn connect(
    ctx: *mut c_void,
    dev: c_int,
    handle: *mut c_void,
    out: *mut *mut c_void,
    offload: *mut *mut c_void,
) -> c_int {
    ffi(|| {
        clear_offload(offload);
        if dev != 0 || handle.is_null() || out.is_null() {
            return Err(USAGE);
        }
        // SAFETY: RCCL owns the 128-byte opaque handle and writable output.
        let h = unsafe {
            *out = std::ptr::null_mut();
            std::slice::from_raw_parts_mut(handle.cast::<u8>(), 128)
        };
        let Some(mut s) = lock()? else { return Ok(()) };
        s.contexts.get(id(ctx)).map_err(map_error)?;
        s.client.alive().map_err(map_error)?;
        if h[..4] != HANDLE_MAGIC
            || h[4..8] != 1u32.to_le_bytes()
            || h[16..24] != s.client.welcome.epoch.to_le_bytes()
            || h[32..64] != s.client.welcome.digest
        {
            return Err(USAGE);
        }
        let peer = u32::from_le_bytes(h[8..12].try_into().unwrap());
        let listener = u64::from_le_bytes(h[24..32].try_into().unwrap());
        let cookie = u64::from_le_bytes(h[64..72].try_into().unwrap());
        if cookie == 0 {
            if s.comms.available() == 0 {
                return Ok(());
            }
            let d = Descriptor {
                op: OP_CONNECT,
                peer,
                total: listener,
                ..Default::default()
            };
            if let Some(slot) = s.client.post(d, &[]).map_err(map_error)? {
                let mut c = empty_comm(CommKind::Send);
                c.peer = peer;
                c.listener = listener;
                c.pending = Some(slot);
                let cid = s.comms.insert(c).map_err(map_error)?;
                h[64..72].copy_from_slice(&cid.to_le_bytes());
            }
        } else {
            let mut c = *s.comms.get(cookie).map_err(map_error)?;
            if c.kind != CommKind::Send || c.peer != peer || c.listener != listener {
                return Err(USAGE);
            }
            let slot = c.pending.ok_or(USAGE)?;
            if matches!(s.client.state(slot).map_err(map_error)?, COMPLETE | FAILED) {
                let d = s.client.finish(slot).map_err(map_error)?;
                if d.status != STATUS_OK {
                    s.comms.remove(cookie).map_err(map_error)?;
                    h[64..72].fill(0);
                    return Err(SYSTEM);
                }
                c.connection = d.connection;
                c.pending = None;
                *s.comms.get_mut(cookie).map_err(map_error)? = c;
                h[64..72].fill(0);
                // SAFETY: `out` is validated writable output.
                unsafe {
                    *out = pointer(cookie);
                }
            }
        }
        Ok(())
    })
}

fn clear_offload(offload: *mut *mut c_void) {
    if !offload.is_null() {
        // SAFETY: non-null offload outputs are writable by the vendor contract.
        unsafe { offload.write(std::ptr::null_mut()) };
    }
}

pub unsafe extern "C" fn accept(
    listen: *mut c_void,
    out: *mut *mut c_void,
    offload: *mut *mut c_void,
) -> c_int {
    ffi(|| {
        clear_offload(offload);
        if out.is_null() {
            return Err(USAGE);
        }
        // SAFETY: `out` is non-null writable output.
        unsafe {
            *out = std::ptr::null_mut();
        }
        let Some(mut s) = lock()? else { return Ok(()) };
        let cid = id(listen);
        let mut c = *s.comms.get(cid).map_err(map_error)?;
        if c.kind != CommKind::Listen {
            return Err(USAGE);
        }
        if let Some(slot) = c.pending {
            if !matches!(s.client.state(slot).map_err(map_error)?, COMPLETE | FAILED) {
                return Ok(());
            }
            let d = s.client.finish(slot).map_err(map_error)?;
            if d.status != STATUS_OK {
                return Err(SYSTEM);
            }
            c.pending = None;
        }
        if let Some(slot) = c.accept {
            if matches!(s.client.state(slot).map_err(map_error)?, COMPLETE | FAILED) {
                if s.comms.available() == 0 {
                    return Ok(());
                }
                let d = s.client.finish(slot).map_err(map_error)?;
                c.accept = None;
                if d.status != STATUS_OK {
                    return Err(SYSTEM);
                }
                let mut recv = empty_comm(CommKind::Recv);
                recv.connection = d.connection;
                recv.peer = d.peer;
                let rid = s.comms.insert(recv).map_err(map_error)?;
                // SAFETY: `out` is validated writable output.
                unsafe {
                    *out = pointer(rid);
                }
            }
        } else {
            c.accept = s
                .client
                .post(
                    Descriptor {
                        op: OP_ACCEPT,
                        listener: c.listener,
                        ..Default::default()
                    },
                    &[],
                )
                .map_err(map_error)?;
        }
        *s.comms.get_mut(cid).map_err(map_error)? = c;
        Ok(())
    })
}

fn register(
    comm: *mut c_void,
    data: *mut c_void,
    size: usize,
    kind: c_int,
    out: *mut *mut c_void,
) -> c_int {
    ffi(|| {
        if out.is_null() || (data.is_null() && size != 0) || size > isize::MAX as usize {
            return Err(USAGE);
        }
        // SAFETY: `out` is non-null writable output.
        unsafe {
            *out = std::ptr::null_mut();
        }
        let mut s = required()?;
        let cid = id(comm);
        let c = *s.comms.get(cid).map_err(map_error)?;
        if c.kind == CommKind::Listen || c.pending.is_some() {
            return Err(USAGE);
        }
        #[cfg(any(feature = "rocm", feature = "cuda"))]
        let supported = kind == 1 || (kind == 2 && s.hip.is_some());
        #[cfg(not(any(feature = "rocm", feature = "cuda")))]
        let supported = kind == 1;
        if !supported || (data as usize).checked_add(size).is_none() {
            return Err(USAGE);
        }
        #[cfg(any(feature = "rocm", feature = "cuda"))]
        if size > 0
            && let Some(staging) = &s.hip
        {
            // SAFETY: the vendor registration call lends a live allocation to this adapter.
            unsafe { staging.validate_pointer(data, kind == 2) }.map_err(map_error)?;
        }
        let mid = s
            .mrs
            .insert(Mr {
                comm: cid,
                base: data as usize,
                length: size,
                kind,
                refs: 0,
            })
            .map_err(map_error)?;
        s.comms.get_mut(cid).map_err(map_error)?.registrations += 1;
        // SAFETY: `out` receives an opaque registration handle.
        unsafe {
            *out = pointer(mid);
        }
        Ok(())
    })
}

pub unsafe extern "C" fn reg_mr(
    c: *mut c_void,
    d: *mut c_void,
    n: usize,
    t: c_int,
    o: *mut *mut c_void,
) -> c_int {
    register(c, d, n, t, o)
}

pub unsafe extern "C" fn dereg(comm: *mut c_void, mr: *mut c_void) -> c_int {
    ffi(|| {
        let mut s = required()?;
        let mid = id(mr);
        let m = *s.mrs.get(mid).map_err(map_error)?;
        if m.comm != id(comm) || m.refs != 0 {
            return Err(USAGE);
        }
        s.mrs.remove(mid).map_err(map_error)?;
        s.comms.get_mut(m.comm).map_err(map_error)?.registrations -= 1;
        Ok(())
    })
}

fn valid_buffer(
    s: &State,
    cid: u64,
    address: usize,
    size: usize,
    mid: u64,
) -> std::result::Result<(), c_int> {
    let m = s.mrs.get(mid).map_err(map_error)?;
    if m.comm != cid
        || address < m.base
        || address
            .checked_add(size)
            .zip(m.base.checked_add(m.length))
            .is_none_or(|(a, b)| a > b)
    {
        return Err(USAGE);
    }
    Ok(())
}

impl State {
    fn post_buffer(
        &mut self,
        request: u64,
        comm: Comm,
        send: bool,
        b: &mut Buffer,
    ) -> std::result::Result<(), c_int> {
        if b.slot.is_some() || b.done || b.h2d_flushing {
            return Ok(());
        }
        let mut d = Descriptor {
            op: if send { OP_SEND } else { OP_RECV },
            peer: comm.peer,
            connection: comm.connection,
            request,
            tag_or_imm: b.tag,
            offset: b.offset as u64,
            total: b.total as u64,
            listener: b.sequence,
            ..Default::default()
        };
        let mr = *self.mrs.get(b.mr).map_err(map_error)?;
        let window = WINDOW;
        d.flags = if !send && mr.kind == 1 {
            window as u32 | FLAG_IPC_INLINE_READY
        } else {
            window as u32
        };
        if send {
            let n = (b.total - b.offset).min(window);
            d.length = n as u32;
            if mr.kind == 1 {
                let payload = if n == 0 {
                    &[]
                } else {
                    // SAFETY: valid_buffer checked this registered host subrange;
                    // caller retains immutable send memory until test() completes.
                    unsafe { std::slice::from_raw_parts((b.address + b.offset) as *const u8, n) }
                };
                b.slot = self.client.post(d, payload).map_err(map_error)?;
            } else {
                #[cfg(any(feature = "rocm", feature = "cuda"))]
                {
                    let Some(hip) = self.hip.as_mut() else {
                        return Err(USAGE);
                    };
                    // SAFETY: registration pins the device range contract until this request retires.
                    if !unsafe { hip.prepare_send(request, b.address + b.offset, n) }
                        .map_err(map_error)?
                    {
                        return Ok(());
                    }
                    let payload = hip.tx_slice(n).map_err(map_error)?;
                    b.slot = self.client.post(d, payload).map_err(map_error)?;
                    if b.slot.is_some() {
                        let next_off = b.offset + n;
                        let next = (next_off < b.total)
                            .then(|| (b.address + next_off, (b.total - next_off).min(window)));
                        // SAFETY: next lies within the same retained device MR.
                        unsafe { hip.finish_send_and_prefetch(request, next) }
                            .map_err(map_error)?;
                    }
                }
                #[cfg(not(any(feature = "rocm", feature = "cuda")))]
                return Err(USAGE);
            }
        } else {
            b.slot = self.client.post(d, &[]).map_err(map_error)?;
        }
        Ok(())
    }

    fn progress(&mut self, rid: u64) -> std::result::Result<Request, c_int> {
        let result = self.progress_inner(rid);
        if matches!(result, Err(SYSTEM)) {
            self.failed = true;
            #[cfg(any(feature = "rocm", feature = "cuda"))]
            if let Some(hip) = self.hip.as_mut() {
                let _ = hip.drain();
            }
        }
        result
    }

    fn progress_inner(&mut self, rid: u64) -> std::result::Result<Request, c_int> {
        let now = Instant::now();
        if now.saturating_duration_since(self.last_alive) >= Duration::from_millis(2) {
            self.client.alive().map_err(map_error)?;
            self.last_alive = now;
        }
        let mut r = *self.requests.get(rid).map_err(map_error)?;
        if r.buffers[..r.n].iter().all(|b| b.done) {
            return Ok(r);
        }
        if r.started.elapsed() > Duration::from_secs(30) {
            self.failed = true;
            return Err(SYSTEM);
        }
        let comm = *self.comms.get(r.comm).map_err(map_error)?;
        for b in &mut r.buffers[..r.n] {
            if b.h2d_flushing {
                #[cfg(any(feature = "rocm", feature = "cuda"))]
                {
                    let Some(hip) = self.hip.as_mut() else {
                        return Err(USAGE);
                    };
                    if hip
                        .rx_complete(rid, b.total <= 64 * 1024)
                        .map_err(map_error)?
                    {
                        b.h2d_flushing = false;
                        b.done = true;
                    }
                }
                #[cfg(not(any(feature = "rocm", feature = "cuda")))]
                {
                    b.h2d_flushing = false;
                    b.done = true;
                }
                continue;
            }
            self.post_buffer(rid, comm, r.send, b)?;
            let Some(slot) = b.slot else { continue };
            match self.client.state(slot).map_err(map_error)? {
                READY if !r.send => {
                    let d = self.client.response(slot).map_err(map_error)?;
                    d.validate().map_err(map_error)?;
                    if d.total > b.capacity as u64
                        || d.offset != b.offset as u64
                        || d.length as usize > b.capacity - b.offset
                    {
                        self.failed = true;
                        return Err(SYSTEM);
                    }
                    let m = *self.mrs.get(b.mr).map_err(map_error)?;
                    if m.kind == 1 {
                        if d.length > 0 {
                            let (payload, _) =
                                self.client.ready_payload(slot).map_err(map_error)?;
                            if payload.len() != d.length as usize {
                                self.failed = true;
                                return Err(SYSTEM);
                            }
                            // SAFETY: receive MR is writable and pinned by active refcount.
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    payload.as_ptr(),
                                    (b.address + b.offset) as *mut u8,
                                    d.length as usize,
                                );
                            }
                        }
                    } else {
                        #[cfg(any(feature = "rocm", feature = "cuda"))]
                        {
                            let Some(hip) = self.hip.as_mut() else {
                                return Err(USAGE);
                            };
                            let (payload, _) =
                                self.client.ready_payload(slot).map_err(map_error)?;
                            if payload.len() != d.length as usize {
                                self.failed = true;
                                return Err(SYSTEM);
                            }
                            // SAFETY: request retains the registered device destination.
                            if !unsafe {
                                hip.launch_recv(b.rx_seq & 1, rid, b.address + b.offset, payload)
                            }
                            .map_err(map_error)?
                            {
                                continue;
                            }
                            b.rx_seq += 1;
                        }
                        #[cfg(not(any(feature = "rocm", feature = "cuda")))]
                        return Err(USAGE);
                    }
                    b.offset += d.length as usize;
                    b.total = d.total as usize;
                    self.client.consume(slot).map_err(map_error)?;
                    r.started = Instant::now();
                }
                COMPLETE => {
                    r.started = Instant::now();
                    let m = *self.mrs.get(b.mr).map_err(map_error)?;
                    if !r.send && m.kind == 1 {
                        let (payload, inline_d) =
                            self.client.completed_payload(slot).map_err(map_error)?;
                        if inline_d.flags & FLAG_IPC_INLINE_READY != 0 {
                            if inline_d.total > b.capacity as u64
                                || payload.len() > b.capacity - b.offset
                            {
                                self.failed = true;
                                return Err(SYSTEM);
                            }
                            if !payload.is_empty() {
                                // SAFETY: receive MR is writable and pinned by active refcount.
                                unsafe {
                                    std::ptr::copy_nonoverlapping(
                                        payload.as_ptr(),
                                        (b.address + b.offset) as *mut u8,
                                        payload.len(),
                                    );
                                }
                            }
                            b.offset += payload.len();
                        }
                    }
                    let d = self.client.finish(slot).map_err(map_error)?;
                    b.slot = None;
                    if d.status != STATUS_OK {
                        self.failed = true;
                        return Err(SYSTEM);
                    }
                    if r.send {
                        b.offset += d.length as usize;
                        if b.offset == b.total {
                            b.done = true;
                        }
                    } else {
                        if b.offset != d.total as usize {
                            self.failed = true;
                            return Err(SYSTEM);
                        }
                        b.total = d.total as usize;
                        let m = *self.mrs.get(b.mr).map_err(map_error)?;
                        if m.kind == 1 {
                            b.done = true;
                        } else {
                            #[cfg(any(feature = "rocm", feature = "cuda"))]
                            {
                                let Some(hip) = self.hip.as_mut() else {
                                    return Err(USAGE);
                                };
                                if hip
                                    .rx_complete(rid, b.total <= 64 * 1024)
                                    .map_err(map_error)?
                                {
                                    b.done = true;
                                } else {
                                    b.h2d_flushing = true;
                                }
                            }
                            #[cfg(not(any(feature = "rocm", feature = "cuda")))]
                            {
                                b.done = true;
                            }
                        }
                    }
                }
                FAILED => {
                    let _ = self.client.finish(slot);
                    self.failed = true;
                    return Err(SYSTEM);
                }
                _ => {}
            }
        }
        *self.requests.get_mut(rid).map_err(map_error)? = r;
        Ok(r)
    }
}

pub unsafe extern "C" fn isend(
    comm: *mut c_void,
    data: *mut c_void,
    size: usize,
    tag: c_int,
    mr: *mut c_void,
    _profiler: *mut c_void,
    out: *mut *mut c_void,
) -> c_int {
    ffi(|| {
        if out.is_null() || size > i32::MAX as usize {
            return Err(USAGE);
        }
        // SAFETY: RCCL supplies a writable request output.
        unsafe {
            *out = std::ptr::null_mut();
        }
        let Some(mut s) = lock()? else { return Ok(()) };
        let cid = id(comm);
        let mut c = *s.comms.get(cid).map_err(map_error)?;
        if c.kind != CommKind::Send || c.pending.is_some() {
            return Err(USAGE);
        }
        valid_buffer(&s, cid, data as usize, size, id(mr))?;
        if s.requests.available() == 0 || s.client.available() == 0 {
            return Ok(());
        }
        let mut buffers = [Buffer::default(); MAX_RECVS];
        buffers[0] = Buffer {
            address: data as usize,
            capacity: size,
            total: size,
            mr: id(mr),
            tag: u64::from(tag as u32),
            sequence: c.next_send,
            ..Default::default()
        };
        let rid = s
            .requests
            .insert(Request {
                comm: cid,
                send: true,
                n: 1,
                buffers,
                started: Instant::now(),
            })
            .map_err(map_error)?;
        s.mrs.get_mut(id(mr)).map_err(map_error)?.refs += 1;
        c.inflight += 1;
        c.next_send += 1;
        *s.comms.get_mut(cid).map_err(map_error)? = c;
        // SAFETY: opaque registry token written to validated output.
        unsafe {
            *out = pointer(rid);
        }
        s.progress(rid)?;
        Ok(())
    })
}

pub unsafe extern "C" fn irecv(
    comm: *mut c_void,
    n: c_int,
    data: *mut *mut c_void,
    sizes: *mut usize,
    tags: *mut c_int,
    mrs: *mut *mut c_void,
    _profilers: *mut *mut c_void,
    out: *mut *mut c_void,
) -> c_int {
    ffi(|| {
        if out.is_null()
            || !(1..=MAX_RECVS as c_int).contains(&n)
            || data.is_null()
            || sizes.is_null()
            || tags.is_null()
            || mrs.is_null()
        {
            return Err(USAGE);
        }
        // SAFETY: `out` is validated non-null writable output.
        unsafe {
            *out = std::ptr::null_mut();
        }
        let Some(mut s) = lock()? else { return Ok(()) };
        let cid = id(comm);
        let mut c = *s.comms.get(cid).map_err(map_error)?;
        if c.kind != CommKind::Recv {
            return Err(USAGE);
        }
        // The daemon matches receives by tag while scanning reusable IPC slots.
        // Backpressure this communicator until RCCL retires the previous receive.
        // Otherwise equal-tag transfers can land in each other's destination.
        if c.inflight != 0 {
            return Ok(());
        }
        let count = n as usize;
        let mut buffers = [Buffer::default(); MAX_RECVS];
        for (i, b) in buffers[..count].iter_mut().enumerate() {
            // SAFETY: caller provides `n` valid entries in each array.
            let (address, size, tag, mid) = unsafe {
                (
                    *data.add(i) as usize,
                    *sizes.add(i),
                    *tags.add(i),
                    id(*mrs.add(i)),
                )
            };
            if size > i32::MAX as usize {
                return Err(USAGE);
            }
            valid_buffer(&s, cid, address, size, mid)?;
            *b = Buffer {
                address,
                capacity: size,
                total: size,
                tag: u64::from(tag as u32),
                mr: mid,
                ..Default::default()
            };
        }
        if s.requests.available() == 0 || s.client.available() < count {
            return Ok(());
        }
        let rid = s
            .requests
            .insert(Request {
                comm: cid,
                send: false,
                n: count,
                buffers,
                started: Instant::now(),
            })
            .map_err(map_error)?;
        for b in &buffers[..count] {
            s.mrs.get_mut(b.mr).map_err(map_error)?.refs += 1;
        }
        c.inflight += 1;
        *s.comms.get_mut(cid).map_err(map_error)? = c;
        // SAFETY: opaque token written to validated output.
        unsafe {
            *out = pointer(rid);
        }
        s.progress(rid)?;
        Ok(())
    })
}

pub unsafe extern "C" fn test(request: *mut c_void, done: *mut c_int, sizes: *mut c_int) -> c_int {
    ffi(|| {
        if done.is_null() {
            return Err(USAGE);
        }
        // SAFETY: `done` is non-null writable int output.
        unsafe {
            *done = 0;
        }
        let Some(mut s) = lock()? else { return Ok(()) };
        let rid = id(request);
        s.requests.get(rid).map_err(map_error)?;
        let mut ids = [0u64; 256];
        let count = s.requests.active_ids(&mut ids);
        for &active in &ids[..count] {
            s.progress(active)?;
        }
        let r = *s.requests.get(rid).map_err(map_error)?;
        if r.buffers[..r.n].iter().all(|b| b.done) {
            for (i, b) in r.buffers[..r.n].iter().enumerate() {
                if !sizes.is_null() {
                    // SAFETY: RCCL provides `n` writable ints when `sizes` is non-null.
                    unsafe {
                        *sizes.add(i) = b.total as c_int;
                    }
                }
                s.mrs.get_mut(b.mr).map_err(map_error)?.refs -= 1;
            }
            s.comms.get_mut(r.comm).map_err(map_error)?.inflight -= 1;
            s.requests.remove(rid).map_err(map_error)?;
            // SAFETY: `done` is validated writable output.
            unsafe {
                *done = 1;
            }
        }
        Ok(())
    })
}

pub unsafe extern "C" fn iflush(
    comm: *mut c_void,
    n: c_int,
    _data: *mut *mut c_void,
    _sizes: *mut c_int,
    _mrs: *mut *mut c_void,
    out: *mut *mut c_void,
) -> c_int {
    ffi(|| {
        if out.is_null() || !(1..=MAX_RECVS as c_int).contains(&n) {
            return Err(USAGE);
        }
        let s = required()?;
        if s.comms.get(id(comm)).map_err(map_error)?.kind != CommKind::Recv {
            return Err(USAGE);
        }
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        // SAFETY: null request indicates synchronous flush completion.
        unsafe {
            *out = std::ptr::null_mut();
        }
        Ok(())
    })
}

fn close(comm: *mut c_void, kind: CommKind) -> c_int {
    ffi(|| {
        let mut s = required()?;
        let cid = id(comm);
        let c = *s.comms.get(cid).map_err(map_error)?;
        if c.kind != kind || c.inflight != 0 || c.registrations != 0 || c.accept.is_some() {
            return Err(USAGE);
        }
        let start = Instant::now();
        if let Some(slot) = c.pending {
            while !matches!(s.client.state(slot).map_err(map_error)?, COMPLETE | FAILED) {
                if start.elapsed() > Duration::from_secs(3) {
                    return Err(SYSTEM);
                }
                std::thread::yield_now();
            }
            s.client.finish(slot).map_err(map_error)?;
        }
        let d = Descriptor {
            op: OP_CLOSE,
            peer: c.peer,
            connection: c.connection,
            listener: c.listener,
            flags: match kind {
                CommKind::Send => 0,
                CommKind::Recv => 1,
                CommKind::Listen => 2,
            },
            ..Default::default()
        };
        let slot = s.client.post(d, &[]).map_err(map_error)?.ok_or(SYSTEM)?;
        while !matches!(s.client.state(slot).map_err(map_error)?, COMPLETE | FAILED) {
            if start.elapsed() > Duration::from_secs(3) {
                return Err(SYSTEM);
            }
            s.client.alive().map_err(map_error)?;
            std::thread::yield_now();
        }
        let d = s.client.finish(slot).map_err(map_error)?;
        if d.status != STATUS_OK {
            return Err(SYSTEM);
        }
        s.comms.remove(cid).map_err(map_error)?;
        Ok(())
    })
}

pub unsafe extern "C" fn close_send(c: *mut c_void) -> c_int {
    close(c, CommKind::Send)
}

pub unsafe extern "C" fn close_recv(c: *mut c_void) -> c_int {
    close(c, CommKind::Recv)
}

pub unsafe extern "C" fn close_listen(c: *mut c_void) -> c_int {
    close(c, CommKind::Listen)
}
