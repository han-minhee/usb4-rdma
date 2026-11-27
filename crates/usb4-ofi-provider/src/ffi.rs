use std::{
    ffi::{CStr, OsStr, c_char},
    os::unix::ffi::OsStrExt,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
};
use usb4_rdma_core::tagged::{
    ADDRESS_BYTES, AGAIN, Address, BAD, Buffer, Completion, Endpoint, IO, Recv, RmaRequest,
};

#[repr(C)]
struct Operation {
    addr: u64,
    tag: u64,
    ignore: u64,
    context: usize,
    data: usize,
    len: usize,
    flags: u32,
}
const TAGGED: u32 = 1;
const COMPLETION: u32 = 2;
const INJECT: u32 = 4;
const PEEK: u32 = 8;
const CLAIM: u32 = 16;
const DISCARD: u32 = 32;

fn run(handle: *mut Endpoint, f: impl FnOnce(&mut Endpoint) -> Result<(), i32>) -> i32 {
    if handle.is_null() {
        return -BAD;
    }
    // SAFETY: the FI_THREAD_DOMAIN shim serializes access and owns the boxed
    // endpoint from successful open until close.
    let endpoint = unsafe { &mut *handle };
    catch_unwind(AssertUnwindSafe(|| f(endpoint))).map_or(-IO, |r| r.map_or_else(|e| -e, |_| 0))
}

unsafe extern "C" fn u4off_open(socket: *const c_char) -> *mut Endpoint {
    if socket.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: the shim passes a live, NUL-terminated pathname.
    let socket = unsafe { CStr::from_ptr(socket) };
    let path = Path::new(OsStr::from_bytes(socket.to_bytes()));
    catch_unwind(AssertUnwindSafe(|| match Endpoint::new(path) {
        Ok(endpoint) => Box::into_raw(Box::new(endpoint)),
        Err(error) => {
            eprintln!("usb4 OFI open: {error}");
            std::ptr::null_mut()
        }
    }))
    .unwrap_or(std::ptr::null_mut())
}

unsafe extern "C" fn u4off_close(handle: *mut Endpoint) {
    if !handle.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: the shim transfers its unique pointer exactly once.
            drop(unsafe { Box::from_raw(handle) });
        }));
    }
}

unsafe extern "C" fn u4off_enable(handle: *mut Endpoint) -> i32 {
    run(handle, |endpoint| {
        endpoint.enabled = true;
        Ok(())
    })
}

unsafe extern "C" fn u4off_name(handle: *mut Endpoint, addr: *mut [u8; ADDRESS_BYTES]) -> i32 {
    if addr.is_null() {
        return -BAD;
    }
    run(handle, |endpoint| {
        // SAFETY: the shim supplies ADDRESS_BYTES writable bytes of address storage.
        unsafe { addr.write(endpoint.address.0) };
        Ok(())
    })
}

unsafe extern "C" fn u4off_insert(
    handle: *mut Endpoint,
    addr: *const [u8; ADDRESS_BYTES],
    out: *mut u64,
) -> i32 {
    if addr.is_null() || out.is_null() {
        return -BAD;
    }
    run(handle, |endpoint| {
        // SAFETY: the shim passes a readable fixed-width address.
        let addr = Address::parse(unsafe { addr.read() }).map_err(|_| BAD)?;
        let id = endpoint.insert(addr)?;
        // SAFETY: this points to the shim's writable address slot.
        unsafe { out.write(id) };
        Ok(())
    })
}

unsafe extern "C" fn u4off_lookup(
    handle: *mut Endpoint,
    id: u64,
    addr: *mut [u8; ADDRESS_BYTES],
) -> i32 {
    if addr.is_null() {
        return -BAD;
    }
    run(handle, |endpoint| {
        let address = endpoint.lookup(id).ok_or(BAD)?;
        // SAFETY: the shim supplies ADDRESS_BYTES writable bytes.
        unsafe { addr.write(address.0) };
        Ok(())
    })
}

unsafe extern "C" fn u4off_remove(handle: *mut Endpoint, id: u64) -> i32 {
    run(handle, |endpoint| endpoint.remove(id))
}

unsafe extern "C" fn u4off_submit(
    handle: *mut Endpoint,
    op: *const Operation,
    is_send: bool,
) -> i32 {
    if op.is_null() {
        return -BAD;
    }
    // SAFETY: the shim passes a live descriptor. OFI owns the buffer borrow
    // until completion and keeps send memory readable and receive memory writable.
    let op = unsafe { &*op };
    // SAFETY: the libfabric operation lends this buffer through its completion.
    let Some(buffer) = (unsafe { Buffer::from_raw(op.data, op.len) }) else {
        return -BAD;
    };
    let request = Recv {
        source: op.addr,
        tag: op.tag,
        ignore: op.ignore,
        tagged: op.flags & TAGGED != 0,
        buffer,
        context: op.context,
        notify: op.flags & COMPLETION != 0,
    };
    run(handle, |endpoint| {
        if is_send {
            endpoint.send(op.addr, request, op.flags & INJECT != 0)?;
            endpoint.progress();
            Ok(())
        } else {
            if op.flags & PEEK != 0 {
                endpoint.progress();
            }
            endpoint.recv(
                request,
                op.flags & PEEK != 0,
                op.flags & CLAIM != 0,
                op.flags & DISCARD != 0,
            )
        }
    })
}

unsafe extern "C" fn u4off_cancel(handle: *mut Endpoint, context: usize) -> i32 {
    run(handle, |endpoint| endpoint.cancel(context))
}

unsafe extern "C" fn u4off_next(
    handle: *mut Endpoint,
    out: *mut Completion,
    pop: bool,
    progress: bool,
) -> i32 {
    if out.is_null() {
        return -BAD;
    }
    run(handle, |endpoint| {
        if progress {
            endpoint.progress();
        }
        let entry = if pop {
            endpoint.completions.pop_front()
        } else {
            endpoint.completions.front().copied()
        };
        match entry {
            Some(entry) => {
                // SAFETY: the shim supplies one writable completion entry.
                unsafe { out.write(entry) };
                Ok(())
            }
            None if endpoint.failed => Err(IO),
            None => Err(AGAIN),
        }
    })
}

#[repr(C)]
struct RmaOperation {
    address: u64,
    offset: u64,
    key: u64,
    context: usize,
    data: usize,
    len: usize,
    notify: bool,
}

unsafe extern "C" fn u4off_mr_register(
    handle: *mut Endpoint,
    data: *mut u8,
    len: usize,
    rights: u32,
    out: *mut u64,
) -> i32 {
    run(handle, |endpoint| {
        if out.is_null() {
            return Err(BAD);
        }
        // SAFETY: the C caller retains the host mapping and provides a writable key output.
        unsafe { out.write(endpoint.register(data, len, rights)?) };
        Ok(())
    })
}

unsafe extern "C" fn u4off_mr_deregister(handle: *mut Endpoint, key: u64) -> i32 {
    run(handle, |endpoint| endpoint.deregister(key))
}

unsafe extern "C" fn u4off_rma(handle: *mut Endpoint, op: *const RmaOperation, write: bool) -> i32 {
    run(handle, |endpoint| {
        // SAFETY: the ABI supplies a live descriptor for the duration of the call.
        let op = unsafe { op.as_ref() }.ok_or(BAD)?;
        // SAFETY: the libfabric operation lends this buffer through its completion.
        let buffer = unsafe { Buffer::from_raw(op.data, op.len) }.ok_or(BAD)?;
        endpoint.post_rma(
            op.address,
            RmaRequest {
                buffer,
                offset: op.offset,
                key: op.key,
                context: op.context,
                write,
                notify: op.notify,
            },
        )?;
        endpoint.progress();
        Ok(())
    })
}

#[repr(C)]
struct RustApi {
    open: unsafe extern "C" fn(*const c_char) -> *mut Endpoint,
    close: unsafe extern "C" fn(*mut Endpoint) -> (),
    enable: unsafe extern "C" fn(*mut Endpoint) -> i32,
    name: unsafe extern "C" fn(*mut Endpoint, *mut [u8; ADDRESS_BYTES]) -> i32,
    insert: unsafe extern "C" fn(*mut Endpoint, *const [u8; ADDRESS_BYTES], *mut u64) -> i32,
    lookup: unsafe extern "C" fn(*mut Endpoint, u64, *mut [u8; ADDRESS_BYTES]) -> i32,
    remove: unsafe extern "C" fn(*mut Endpoint, u64) -> i32,
    submit: unsafe extern "C" fn(*mut Endpoint, *const Operation, bool) -> i32,
    cancel: unsafe extern "C" fn(*mut Endpoint, usize) -> i32,
    next: unsafe extern "C" fn(*mut Endpoint, *mut Completion, bool, bool) -> i32,
    mr_register: unsafe extern "C" fn(*mut Endpoint, *mut u8, usize, u32, *mut u64) -> i32,
    mr_deregister: unsafe extern "C" fn(*mut Endpoint, u64) -> i32,
    rma: unsafe extern "C" fn(*mut Endpoint, *const RmaOperation, bool) -> i32,
}

static API: RustApi = RustApi {
    open: u4off_open,
    close: u4off_close,
    enable: u4off_enable,
    name: u4off_name,
    insert: u4off_insert,
    lookup: u4off_lookup,
    remove: u4off_remove,
    submit: u4off_submit,
    cancel: u4off_cancel,
    next: u4off_next,
    mr_register: u4off_mr_register,
    mr_deregister: u4off_mr_deregister,
    rma: u4off_rma,
};

unsafe extern "C" {
    fn usb4_ofi_provider(api: *const RustApi) -> *const std::ffi::c_void;
}

#[unsafe(no_mangle)]
extern "C" fn fi_prov_ini() -> *const std::ffi::c_void {
    catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the C shim retains only this statically allocated dispatch table.
        unsafe { usb4_ofi_provider(&API) }
    }))
    .unwrap_or(std::ptr::null())
}
