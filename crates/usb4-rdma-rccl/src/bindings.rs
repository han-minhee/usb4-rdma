//! C ABI bindings matching ROCm RCCL / NVIDIA NCCL `ncclNet_v6_t` and `ncclNet_v8_t`.
#![allow(non_camel_case_types, non_snake_case)]

use std::ffi::{c_char, c_int, c_void};

pub type Logger =
    Option<unsafe extern "C" fn(c_int, libc::c_ulong, *const c_char, c_int, *const c_char, ...)>;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct ncclNetProperties_v6_t {
    pub name: *mut c_char,
    pub pciPath: *mut c_char,
    pub guid: u64,
    pub ptrSupport: c_int,
    pub speed: c_int,
    pub port: c_int,
    pub latency: f32,
    pub maxComms: c_int,
    pub maxRecvs: c_int,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct ncclNet_v6_t {
    pub name: *const c_char,
    pub init: Option<unsafe extern "C" fn(Logger) -> c_int>,
    pub devices: Option<unsafe extern "C" fn(*mut c_int) -> c_int>,
    pub getProperties: Option<unsafe extern "C" fn(c_int, *mut ncclNetProperties_v6_t) -> c_int>,
    pub listen: Option<unsafe extern "C" fn(c_int, *mut c_void, *mut *mut c_void) -> c_int>,
    pub connect: Option<unsafe extern "C" fn(c_int, *mut c_void, *mut *mut c_void) -> c_int>,
    pub accept: Option<unsafe extern "C" fn(*mut c_void, *mut *mut c_void) -> c_int>,
    pub regMr: Option<
        unsafe extern "C" fn(*mut c_void, *mut c_void, c_int, c_int, *mut *mut c_void) -> c_int,
    >,
    pub regMrDmaBuf: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut c_void,
            usize,
            c_int,
            u64,
            c_int,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub deregMr: Option<unsafe extern "C" fn(*mut c_void, *mut c_void) -> c_int>,
    pub isend: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut c_void,
            c_int,
            c_int,
            *mut c_void,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub irecv: Option<
        unsafe extern "C" fn(
            *mut c_void,
            c_int,
            *mut *mut c_void,
            *mut c_int,
            *mut c_int,
            *mut *mut c_void,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub iflush: Option<
        unsafe extern "C" fn(
            *mut c_void,
            c_int,
            *mut *mut c_void,
            *mut c_int,
            *mut *mut c_void,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub test: Option<unsafe extern "C" fn(*mut c_void, *mut c_int, *mut c_int) -> c_int>,
    pub closeSend: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub closeRecv: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub closeListen: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct ncclNetProperties_v8_t {
    pub name: *mut c_char,
    pub pciPath: *mut c_char,
    pub guid: u64,
    pub ptrSupport: c_int,
    pub regIsGlobal: c_int,
    pub speed: c_int,
    pub port: c_int,
    pub latency: f32,
    pub maxComms: c_int,
    pub maxRecvs: c_int,
    pub netDeviceType: c_int,
    pub netDeviceVersion: c_int,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct ncclNet_v8_t {
    pub name: *const c_char,
    pub init: Option<unsafe extern "C" fn(Logger) -> c_int>,
    pub devices: Option<unsafe extern "C" fn(*mut c_int) -> c_int>,
    pub getProperties: Option<unsafe extern "C" fn(c_int, *mut ncclNetProperties_v8_t) -> c_int>,
    pub listen: Option<unsafe extern "C" fn(c_int, *mut c_void, *mut *mut c_void) -> c_int>,
    pub connect: Option<
        unsafe extern "C" fn(c_int, *mut c_void, *mut *mut c_void, *mut *mut c_void) -> c_int,
    >,
    pub accept:
        Option<unsafe extern "C" fn(*mut c_void, *mut *mut c_void, *mut *mut c_void) -> c_int>,
    pub regMr: Option<
        unsafe extern "C" fn(*mut c_void, *mut c_void, usize, c_int, *mut *mut c_void) -> c_int,
    >,
    pub regMrDmaBuf: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut c_void,
            usize,
            c_int,
            u64,
            c_int,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub deregMr: Option<unsafe extern "C" fn(*mut c_void, *mut c_void) -> c_int>,
    pub isend: Option<
        unsafe extern "C" fn(
            *mut c_void,
            *mut c_void,
            c_int,
            c_int,
            *mut c_void,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub irecv: Option<
        unsafe extern "C" fn(
            *mut c_void,
            c_int,
            *mut *mut c_void,
            *mut c_int,
            *mut c_int,
            *mut *mut c_void,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub iflush: Option<
        unsafe extern "C" fn(
            *mut c_void,
            c_int,
            *mut *mut c_void,
            *mut c_int,
            *mut *mut c_void,
            *mut *mut c_void,
        ) -> c_int,
    >,
    pub test: Option<unsafe extern "C" fn(*mut c_void, *mut c_int, *mut c_int) -> c_int>,
    pub closeSend: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub closeRecv: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub closeListen: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub getDeviceMr:
        Option<unsafe extern "C" fn(*mut c_void, *mut c_void, *mut *mut c_void) -> c_int>,
    pub irecvConsumed: Option<unsafe extern "C" fn(*mut c_void, c_int, *mut c_void) -> c_int>,
}
