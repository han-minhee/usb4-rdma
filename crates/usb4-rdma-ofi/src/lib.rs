//! Host-memory OFI session engine. The optional C shim owns libfabric's ABI.
mod endpoint;
mod wire;

use endpoint::{AGAIN, BAD, Buffer, Completion, Endpoint, IO, Recv};
use std::{
    ffi::{CStr, OsStr, c_char},
    os::unix::ffi::OsStrExt,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
};
use wire::{ADDRESS_BYTES, Address};

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

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
unsafe extern "C" fn u4off_close(handle: *mut Endpoint) {
    if !handle.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: the shim transfers its unique pointer exactly once.
            drop(unsafe { Box::from_raw(handle) });
        }));
    }
}

#[unsafe(no_mangle)]
unsafe extern "C" fn u4off_enable(handle: *mut Endpoint) -> i32 {
    run(handle, |endpoint| {
        endpoint.enabled = true;
        Ok(())
    })
}

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
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

#[unsafe(no_mangle)]
unsafe extern "C" fn u4off_remove(handle: *mut Endpoint, id: u64) -> i32 {
    run(handle, |endpoint| endpoint.remove(id))
}

#[unsafe(no_mangle)]
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
    if op.data == 0 && op.len != 0 {
        return -BAD;
    }
    let request = Recv {
        source: op.addr,
        tag: op.tag,
        ignore: op.ignore,
        tagged: op.flags & TAGGED != 0,
        buffer: Buffer {
            addr: op.data,
            len: op.len,
        },
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

#[unsafe(no_mangle)]
unsafe extern "C" fn u4off_cancel(handle: *mut Endpoint, context: usize) -> i32 {
    run(handle, |endpoint| endpoint.cancel(context))
}

#[unsafe(no_mangle)]
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
