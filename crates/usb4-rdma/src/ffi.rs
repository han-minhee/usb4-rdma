//! C11 FFI (`u4rd_*`) matching `include/usb4_rdma.h`.

use crate::{
    BorrowedMr, CompletionQueue, Context, EndpointHandle, Listener, MrRef, OwnedMr,
    ProtectionDomain, QueuePair,
};
use std::{
    ffi::{CStr, c_char, c_int, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    time::Duration,
};
use usb4_rdma_core::{Error, rdma::AccessFlags};

pub const U4RD_OK: c_int = 0;
pub const U4RD_ERR_INVALID_ARG: c_int = -1;
pub const U4RD_ERR_UNSUPPORTED_ACCESS: c_int = -2;
pub const U4RD_ERR_ACCESS_VIOLATION: c_int = -3;
pub const U4RD_ERR_EXHAUSTED: c_int = -4;
pub const U4RD_ERR_DISCONNECTED: c_int = -5;
pub const U4RD_ERR_TIMEOUT: c_int = -6;
pub const U4RD_ERR_PROTOCOL: c_int = -7;
pub const U4RD_ERR_IO: c_int = -8;
pub const U4RD_ERR_INTERNAL: c_int = -9;

#[repr(C)]
pub struct U4rdDeviceAttr {
    pub node_id: u32,
    pub max_window_bytes: u32,
    pub max_chunk_bytes: u32,
    pub max_mr_count: u32,
    pub epoch: u64,
    pub topology_digest: [u8; 32],
    pub supports_one_sided_rdma: u8,
    pub zero_copy_dma: u8,
    pub reserved: [u8; 6],
}

#[repr(C)]
pub struct U4rdEndpointHandle {
    pub bytes: [u8; EndpointHandle::SIZE],
}

#[repr(C)]
pub struct U4rdWc {
    pub wr_id: u64,
    pub byte_len: u64,
    pub qp_id: u64,
    pub status: u32,
    pub opcode: u32,
    pub tag_or_imm: u32,
    pub has_imm: u8,
    pub reserved: [u8; 3],
}

pub enum MrHandle {
    Owned(OwnedMr),
    Borrowed(BorrowedMr),
}

fn map_err(err: Error) -> c_int {
    match err {
        Error::InvalidArg(_) | Error::Config(_) | Error::Stale => U4RD_ERR_INVALID_ARG,
        Error::UnsupportedAccess => U4RD_ERR_UNSUPPORTED_ACCESS,
        Error::AccessViolation => U4RD_ERR_ACCESS_VIOLATION,
        Error::Exhausted => U4RD_ERR_EXHAUSTED,
        Error::Disconnected => U4RD_ERR_DISCONNECTED,
        Error::Timeout => U4RD_ERR_TIMEOUT,
        Error::Protocol(_) | Error::RemoteError(_) => U4RD_ERR_PROTOCOL,
        Error::Io(_) => U4RD_ERR_IO,
    }
}

fn guard(f: impl FnOnce() -> c_int) -> c_int {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(U4RD_ERR_INTERNAL)
}

#[unsafe(no_mangle)]
pub extern "C" fn u4rd_strerror(code: c_int) -> *const c_char {
    match code {
        U4RD_OK => c"ok".as_ptr(),
        U4RD_ERR_INVALID_ARG => c"invalid argument".as_ptr(),
        U4RD_ERR_UNSUPPORTED_ACCESS => c"unsupported access flags for registration mode".as_ptr(),
        U4RD_ERR_ACCESS_VIOLATION => c"memory access or bounds violation".as_ptr(),
        U4RD_ERR_EXHAUSTED => c"bounded resource exhausted".as_ptr(),
        U4RD_ERR_DISCONNECTED => c"peer or daemon disconnected".as_ptr(),
        U4RD_ERR_TIMEOUT => c"operation timed out".as_ptr(),
        U4RD_ERR_PROTOCOL => c"protocol or remote error".as_ptr(),
        U4RD_ERR_IO => c"I/O error".as_ptr(),
        _ => c"internal error".as_ptr(),
    }
}

/// # Safety
/// `socket_path` must be a valid NUL-terminated UTF-8 path (or NULL to use `USB4_RDMA_SOCKET`),
/// and `out_ctx` must be a valid writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_open(
    socket_path: *const c_char,
    out_ctx: *mut *mut Context,
) -> c_int {
    guard(|| {
        if out_ctx.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        let res = if socket_path.is_null() {
            Context::open_default()
        } else {
            // SAFETY: caller guarantees valid NUL-terminated C string when non-null.
            let cstr = unsafe { CStr::from_ptr(socket_path) };
            match cstr.to_str() {
                Ok(s) => Context::open(s),
                Err(_) => return U4RD_ERR_INVALID_ARG,
            }
        };
        match res {
            Ok(ctx) => {
                // SAFETY: `out_ctx` was checked non-null and is caller-owned.
                unsafe { *out_ctx = Box::into_raw(Box::new(ctx)) };
                U4RD_OK
            }
            Err(e) => map_err(e),
        }
    })
}

/// # Safety
/// `ctx` must be NULL or a live pointer returned by `u4rd_open`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_close(ctx: *mut Context) {
    let _ = guard(|| {
        if !ctx.is_null() {
            // SAFETY: `ctx` was allocated via `Box::into_raw` in `u4rd_open`.
            drop(unsafe { Box::from_raw(ctx) });
        }
        U4RD_OK
    });
}

/// # Safety
/// `ctx` and `out_attr` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_query_device(
    ctx: *const Context,
    out_attr: *mut U4rdDeviceAttr,
) -> c_int {
    guard(|| {
        if ctx.is_null() || out_attr.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        // SAFETY: `ctx` is live and non-null.
        let ctx_ref = unsafe { &*ctx };
        match ctx_ref.query_device() {
            Ok(a) => {
                // SAFETY: `out_attr` is non-null and writable.
                unsafe {
                    *out_attr = U4rdDeviceAttr {
                        node_id: a.node_id,
                        max_window_bytes: a.max_window_bytes,
                        max_chunk_bytes: a.max_chunk_bytes,
                        max_mr_count: a.max_mr_count,
                        epoch: a.epoch,
                        topology_digest: a.topology_digest,
                        supports_one_sided_rdma: u8::from(a.supports_one_sided_rdma),
                        zero_copy_dma: u8::from(a.zero_copy_dma),
                        reserved: [0; 6],
                    };
                }
                U4RD_OK
            }
            Err(e) => map_err(e),
        }
    })
}

/// # Safety
/// `ctx` and `out_pd` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_alloc_pd(
    ctx: *const Context,
    out_pd: *mut *mut ProtectionDomain,
) -> c_int {
    guard(|| {
        if ctx.is_null() || out_pd.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        // SAFETY: `ctx` is live and non-null.
        let pd = unsafe { &*ctx }.alloc_pd();
        // SAFETY: `out_pd` is non-null and writable.
        unsafe { *out_pd = Box::into_raw(Box::new(pd)) };
        U4RD_OK
    })
}

/// # Safety
/// `pd` must be NULL or a live pointer returned by `u4rd_alloc_pd`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_dealloc_pd(pd: *mut ProtectionDomain) {
    let _ = guard(|| {
        if !pd.is_null() {
            // SAFETY: `pd` came from `Box::into_raw` in `u4rd_alloc_pd`.
            drop(unsafe { Box::from_raw(pd) });
        }
        U4RD_OK
    });
}

/// # Safety
/// `ctx` and `out_cq` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_create_cq(
    ctx: *const Context,
    out_cq: *mut *mut CompletionQueue,
) -> c_int {
    guard(|| {
        if ctx.is_null() || out_cq.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        // SAFETY: `ctx` is live and non-null.
        match unsafe { &*ctx }.create_cq() {
            Ok(cq) => {
                // SAFETY: `out_cq` is non-null and writable.
                unsafe { *out_cq = Box::into_raw(Box::new(cq)) };
                U4RD_OK
            }
            Err(e) => map_err(e),
        }
    })
}

/// # Safety
/// `cq` must be NULL or returned by `u4rd_create_cq`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_destroy_cq(cq: *mut CompletionQueue) {
    let _ = guard(|| {
        if !cq.is_null() {
            // SAFETY: `cq` came from `Box::into_raw` in `u4rd_create_cq`.
            drop(unsafe { Box::from_raw(cq) });
        }
        U4RD_OK
    });
}

/// # Safety
/// `cq` must be valid; `out_wcs` must hold `max_entries` writable `U4rdWc` structs; `out_count` non-null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_poll_cq(
    cq: *const CompletionQueue,
    max_entries: usize,
    out_wcs: *mut U4rdWc,
    out_count: *mut usize,
) -> c_int {
    guard(|| {
        if cq.is_null() || out_count.is_null() || (max_entries > 0 && out_wcs.is_null()) {
            return U4RD_ERR_INVALID_ARG;
        }
        let capped = max_entries.min(64);
        let mut buf = [usb4_rdma_core::rdma::WorkCompletion {
            wr_id: 0,
            status: usb4_rdma_core::rdma::WcStatus::Success,
            opcode: usb4_rdma_core::rdma::WcOpcode::Send,
            byte_len: 0,
            tag_or_imm: 0,
            has_imm: false,
            qp_id: 0,
        }; 64];
        // SAFETY: `cq` is valid and non-null.
        match unsafe { &*cq }.poll(&mut buf[..capped]) {
            Ok(n) => {
                for (i, wc) in buf[..n].iter().enumerate() {
                    // SAFETY: `out_wcs` has capacity >= `max_entries >= n`.
                    unsafe {
                        *out_wcs.add(i) = U4rdWc {
                            wr_id: wc.wr_id,
                            byte_len: wc.byte_len,
                            qp_id: wc.qp_id,
                            status: wc.status as u32,
                            opcode: wc.opcode as u32,
                            tag_or_imm: wc.tag_or_imm,
                            has_imm: u8::from(wc.has_imm),
                            reserved: [0; 3],
                        };
                    }
                }
                // SAFETY: `out_count` is non-null and writable.
                unsafe { *out_count = n };
                U4RD_OK
            }
            Err(e) => map_err(e),
        }
    })
}

/// # Safety
/// `pd` and `out_mr` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_alloc_mr(
    pd: *const ProtectionDomain,
    len: usize,
    access_flags: u32,
    out_mr: *mut *mut MrHandle,
) -> c_int {
    guard(|| {
        if pd.is_null() || out_mr.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        let access = match AccessFlags::from_bits(access_flags) {
            Ok(a) => a,
            Err(e) => return map_err(e),
        };
        // SAFETY: `pd` is live and non-null.
        match unsafe { &*pd }.alloc_mr(len, access) {
            Ok(mr) => {
                // SAFETY: `out_mr` is non-null and writable.
                unsafe { *out_mr = Box::into_raw(Box::new(MrHandle::Owned(mr))) };
                U4RD_OK
            }
            Err(e) => map_err(e),
        }
    })
}

/// # Safety
/// `pd` and `out_mr` must be valid; `addr..addr+len` must stay valid until `u4rd_dereg_mr`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_reg_mr(
    pd: *const ProtectionDomain,
    addr: *mut c_void,
    len: usize,
    access_flags: u32,
    out_mr: *mut *mut MrHandle,
) -> c_int {
    guard(|| {
        if pd.is_null() || out_mr.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        let access = match AccessFlags::from_bits(access_flags) {
            Ok(a) => a,
            Err(e) => return map_err(e),
        };
        // SAFETY: `pd` is valid and caller upholds borrowed buffer lifetime contract.
        match unsafe { (&*pd).reg_mr_local(addr.cast::<u8>(), len, access) } {
            Ok(mr) => {
                // SAFETY: `out_mr` is non-null and writable.
                unsafe { *out_mr = Box::into_raw(Box::new(MrHandle::Borrowed(mr))) };
                U4RD_OK
            }
            Err(e) => map_err(e),
        }
    })
}

/// # Safety
/// `mr` must be NULL or returned by `u4rd_alloc_mr` / `u4rd_reg_mr`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_dereg_mr(mr: *mut MrHandle) {
    let _ = guard(|| {
        if !mr.is_null() {
            // SAFETY: `mr` was boxed in `u4rd_alloc_mr` or `u4rd_reg_mr`.
            drop(unsafe { Box::from_raw(mr) });
        }
        U4RD_OK
    });
}

/// # Safety
/// `mr` must be a valid pointer returned by `u4rd_alloc_mr` or `u4rd_reg_mr`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_mr_addr(mr: *const MrHandle) -> *mut c_void {
    if mr.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: `mr` is non-null and live.
    match unsafe { &*mr } {
        MrHandle::Owned(o) => o.as_ptr().cast_mut().cast(),
        MrHandle::Borrowed(b) => b.as_mut_ptr().cast(),
    }
}

/// # Safety
/// `mr` must be a valid pointer returned by `u4rd_alloc_mr` or `u4rd_reg_mr`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_mr_len(mr: *const MrHandle) -> usize {
    if mr.is_null() {
        return 0;
    }
    // SAFETY: `mr` is non-null and live.
    match unsafe { &*mr } {
        MrHandle::Owned(o) => o.len(),
        MrHandle::Borrowed(b) => b.len(),
    }
}

/// # Safety
/// `mr` must be a valid pointer returned by `u4rd_alloc_mr` or `u4rd_reg_mr`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_mr_lkey(mr: *const MrHandle) -> u64 {
    if mr.is_null() {
        return 0;
    }
    // SAFETY: `mr` is non-null and live.
    match unsafe { &*mr } {
        MrHandle::Owned(o) => o.lkey(),
        MrHandle::Borrowed(b) => b.lkey(),
    }
}

/// # Safety
/// `mr` must be a valid pointer returned by `u4rd_alloc_mr` or `u4rd_reg_mr`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_mr_rkey(mr: *const MrHandle) -> u64 {
    if mr.is_null() {
        return 0;
    }
    // SAFETY: `mr` is non-null and live.
    match unsafe { &*mr } {
        MrHandle::Owned(o) => o.rkey(),
        MrHandle::Borrowed(b) => b.rkey(),
    }
}

/// # Safety
/// `pd` and `out_listener` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_listen(
    pd: *const ProtectionDomain,
    out_listener: *mut *mut Listener,
) -> c_int {
    guard(|| {
        if pd.is_null() || out_listener.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        // SAFETY: `pd` is live and non-null.
        match unsafe { &*pd }.listen() {
            Ok(l) => {
                // SAFETY: `out_listener` is non-null and writable.
                unsafe { *out_listener = Box::into_raw(Box::new(l)) };
                U4RD_OK
            }
            Err(e) => map_err(e),
        }
    })
}

/// # Safety
/// `listener` and `out_handle` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_listener_handle(
    listener: *const Listener,
    out_handle: *mut U4rdEndpointHandle,
) -> c_int {
    guard(|| {
        if listener.is_null() || out_handle.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        // SAFETY: `listener` is non-null and live.
        let bytes = unsafe { &*listener }.handle().to_bytes();
        // SAFETY: `out_handle` is non-null and writable.
        unsafe { *out_handle = U4rdEndpointHandle { bytes } };
        U4RD_OK
    })
}

/// # Safety
/// `listener`, `send_cq`, `recv_cq`, and `out_qp` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_accept(
    listener: *const Listener,
    send_cq: *const CompletionQueue,
    recv_cq: *const CompletionQueue,
    timeout_ms: u64,
    out_qp: *mut *mut QueuePair,
) -> c_int {
    guard(|| {
        if listener.is_null() || send_cq.is_null() || recv_cq.is_null() || out_qp.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        // SAFETY: all input pointers were checked non-null.
        let res =
            unsafe { (&*listener).accept(&*send_cq, &*recv_cq, Duration::from_millis(timeout_ms)) };
        match res {
            Ok(qp) => {
                // SAFETY: `out_qp` is non-null and writable.
                unsafe { *out_qp = Box::into_raw(Box::new(qp)) };
                U4RD_OK
            }
            Err(e) => map_err(e),
        }
    })
}

/// # Safety
/// `listener` must be NULL or returned by `u4rd_listen`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_close_listener(listener: *mut Listener) {
    let _ = guard(|| {
        if !listener.is_null() {
            // SAFETY: `listener` came from `Box::into_raw` in `u4rd_listen`.
            drop(unsafe { Box::from_raw(listener) });
        }
        U4RD_OK
    });
}

/// # Safety
/// `pd`, `remote_handle`, `send_cq`, `recv_cq`, and `out_qp` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_connect(
    pd: *const ProtectionDomain,
    remote_handle: *const U4rdEndpointHandle,
    send_cq: *const CompletionQueue,
    recv_cq: *const CompletionQueue,
    out_qp: *mut *mut QueuePair,
) -> c_int {
    guard(|| {
        if pd.is_null()
            || remote_handle.is_null()
            || send_cq.is_null()
            || recv_cq.is_null()
            || out_qp.is_null()
        {
            return U4RD_ERR_INVALID_ARG;
        }
        // SAFETY: `remote_handle` is non-null and initialized.
        let handle = match EndpointHandle::from_bytes(&unsafe { &*remote_handle }.bytes) {
            Ok(h) => h,
            Err(e) => return map_err(e),
        };
        // SAFETY: `pd`, `send_cq`, and `recv_cq` are valid non-null pointers.
        match unsafe { (&*pd).connect(&handle, &*send_cq, &*recv_cq) } {
            Ok(qp) => {
                // SAFETY: `out_qp` is non-null and writable.
                unsafe { *out_qp = Box::into_raw(Box::new(qp)) };
                U4RD_OK
            }
            Err(e) => map_err(e),
        }
    })
}

/// # Safety
/// `qp` must be NULL or returned by `u4rd_connect` / `u4rd_accept`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_destroy_qp(qp: *mut QueuePair) {
    let _ = guard(|| {
        if !qp.is_null() {
            // SAFETY: `qp` came from `Box::into_raw`.
            drop(unsafe { Box::from_raw(qp) });
        }
        U4RD_OK
    });
}

/// # Safety
/// `qp` and `mr` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_post_send(
    qp: *const QueuePair,
    wr_id: u64,
    mr: *const MrHandle,
    offset: usize,
    len: usize,
    tag: u32,
) -> c_int {
    guard(|| {
        if qp.is_null() || mr.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        let Some(end) = offset.checked_add(len) else {
            return U4RD_ERR_INVALID_ARG;
        };
        // SAFETY: `qp` and `mr` are valid non-null pointers.
        let res = unsafe {
            match &*mr {
                MrHandle::Owned(o) => (&*qp).post_send(wr_id, o, offset..end, tag),
                MrHandle::Borrowed(b) => (&*qp).post_send(wr_id, b, offset..end, tag),
            }
        };
        res.map_or_else(map_err, |()| U4RD_OK)
    })
}

/// # Safety
/// `qp` and `mr` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_post_recv(
    qp: *const QueuePair,
    wr_id: u64,
    mr: *const MrHandle,
    offset: usize,
    len: usize,
    tag: u32,
) -> c_int {
    guard(|| {
        if qp.is_null() || mr.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        let Some(end) = offset.checked_add(len) else {
            return U4RD_ERR_INVALID_ARG;
        };
        // SAFETY: `qp` and `mr` are valid non-null pointers.
        let res = unsafe {
            match &*mr {
                MrHandle::Owned(o) => (&*qp).post_recv(wr_id, o, offset..end, tag),
                MrHandle::Borrowed(b) => (&*qp).post_recv(wr_id, b, offset..end, tag),
            }
        };
        res.map_or_else(map_err, |()| U4RD_OK)
    })
}

/// # Safety
/// `qp` and `out_ready` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_peek_recv(
    qp: *const QueuePair,
    tag: u32,
    out_ready: *mut c_int,
) -> c_int {
    guard(|| {
        if qp.is_null() || out_ready.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        // SAFETY: `qp` is valid and non-null.
        match unsafe { &*qp }.peek_recv(tag) {
            Ok(ready) => {
                // SAFETY: `out_ready` is non-null and writable.
                unsafe { *out_ready = c_int::from(ready) };
                U4RD_OK
            }
            Err(e) => map_err(e),
        }
    })
}

/// # Safety
/// `qp` and `mr` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_post_rdma_write(
    qp: *const QueuePair,
    wr_id: u64,
    mr: *const MrHandle,
    offset: usize,
    len: usize,
    rkey: u64,
    remote_offset: u64,
) -> c_int {
    guard(|| {
        if qp.is_null() || mr.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        let Some(end) = offset.checked_add(len) else {
            return U4RD_ERR_INVALID_ARG;
        };
        // SAFETY: `qp` and `mr` are valid non-null pointers.
        let res = unsafe {
            match &*mr {
                MrHandle::Owned(o) => {
                    (&*qp).post_rdma_write(wr_id, o, offset..end, rkey, remote_offset)
                }
                MrHandle::Borrowed(b) => {
                    (&*qp).post_rdma_write(wr_id, b, offset..end, rkey, remote_offset)
                }
            }
        };
        res.map_or_else(map_err, |()| U4RD_OK)
    })
}

/// # Safety
/// `qp` and `mr` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_post_rdma_write_imm(
    qp: *const QueuePair,
    wr_id: u64,
    mr: *const MrHandle,
    offset: usize,
    len: usize,
    rkey: u64,
    remote_offset: u64,
    imm_data: u32,
) -> c_int {
    guard(|| {
        if qp.is_null() || mr.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        let Some(end) = offset.checked_add(len) else {
            return U4RD_ERR_INVALID_ARG;
        };
        // SAFETY: `qp` and `mr` are valid non-null pointers.
        let res = unsafe {
            match &*mr {
                MrHandle::Owned(o) => {
                    (&*qp).post_rdma_write_imm(wr_id, o, offset..end, rkey, remote_offset, imm_data)
                }
                MrHandle::Borrowed(b) => {
                    (&*qp).post_rdma_write_imm(wr_id, b, offset..end, rkey, remote_offset, imm_data)
                }
            }
        };
        res.map_or_else(map_err, |()| U4RD_OK)
    })
}

/// # Safety
/// `qp` and `mr` must be valid non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn u4rd_post_rdma_read(
    qp: *const QueuePair,
    wr_id: u64,
    mr: *const MrHandle,
    offset: usize,
    len: usize,
    rkey: u64,
    remote_offset: u64,
) -> c_int {
    guard(|| {
        if qp.is_null() || mr.is_null() {
            return U4RD_ERR_INVALID_ARG;
        }
        let Some(end) = offset.checked_add(len) else {
            return U4RD_ERR_INVALID_ARG;
        };
        // SAFETY: `qp` and `mr` are valid non-null pointers.
        let res = unsafe {
            match &*mr {
                MrHandle::Owned(o) => {
                    (&*qp).post_rdma_read(wr_id, o, offset..end, rkey, remote_offset)
                }
                MrHandle::Borrowed(b) => {
                    (&*qp).post_rdma_read(wr_id, b, offset..end, rkey, remote_offset)
                }
            }
        };
        res.map_or_else(map_err, |()| U4RD_OK)
    })
}
