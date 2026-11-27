#![allow(non_snake_case, non_camel_case_types, clippy::type_complexity)]

use std::ffi::{c_char, c_int, c_void};

pub type Logger =
    Option<unsafe extern "C" fn(c_int, libc::c_ulong, *const c_char, c_int, *const c_char, ...)>;
pub type Profiler = Option<unsafe extern "C" fn()>;

type Handle = *mut c_void;
type Out = *mut Handle;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ncclNetVDeviceProps_v12_t {
    pub ndevs: c_int,
    pub devs: [c_int; 8],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ncclNetProperties_v12_t {
    pub name: *mut c_char,
    pub pciPath: *mut c_char,
    pub guid: u64,
    pub ptrSupport: c_int,
    pub regIsGlobal: c_int,
    pub forceFlush: c_int,
    pub speed: c_int,
    pub port: c_int,
    pub latency: f32,
    pub maxComms: c_int,
    pub maxRecvs: c_int,
    pub netDeviceType: c_int,
    pub netDeviceVersion: c_int,
    pub vProps: ncclNetVDeviceProps_v12_t,
    pub maxP2pBytes: usize,
    pub maxCollBytes: usize,
    pub maxMultiRequestSize: c_int,
    pub railId: i16,
    pub planeId: i16,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ncclNet_v12_t {
    pub name: *const c_char,
    pub init: Option<unsafe extern "C" fn(Out, u64, Handle, Logger, Profiler) -> c_int>,
    pub devices: Option<unsafe extern "C" fn(*mut c_int) -> c_int>,
    pub getProperties: Option<unsafe extern "C" fn(c_int, *mut ncclNetProperties_v12_t) -> c_int>,
    pub listen: Option<unsafe extern "C" fn(Handle, c_int, Handle, Out) -> c_int>,
    pub connect: Option<unsafe extern "C" fn(Handle, c_int, Handle, Out, Out) -> c_int>,
    pub accept: Option<unsafe extern "C" fn(Handle, Out, Out) -> c_int>,
    pub regMr: Option<unsafe extern "C" fn(Handle, Handle, usize, c_int, Out) -> c_int>,
    pub regMrDmaBuf:
        Option<unsafe extern "C" fn(Handle, Handle, usize, c_int, u64, c_int, Out) -> c_int>,
    pub deregMr: Option<unsafe extern "C" fn(Handle, Handle) -> c_int>,
    pub isend:
        Option<unsafe extern "C" fn(Handle, Handle, usize, c_int, Handle, Handle, Out) -> c_int>,
    pub irecv: Option<
        unsafe extern "C" fn(Handle, c_int, Out, *mut usize, *mut c_int, Out, Out, Out) -> c_int,
    >,
    pub iflush: Option<unsafe extern "C" fn(Handle, c_int, Out, *mut c_int, Out, Out) -> c_int>,
    pub test: Option<unsafe extern "C" fn(Handle, *mut c_int, *mut c_int) -> c_int>,
    pub closeSend: Option<unsafe extern "C" fn(Handle) -> c_int>,
    pub closeRecv: Option<unsafe extern "C" fn(Handle) -> c_int>,
    pub closeListen: Option<unsafe extern "C" fn(Handle) -> c_int>,
    pub getDeviceMr: Option<unsafe extern "C" fn(Handle, Handle, Out) -> c_int>,
    pub irecvConsumed: Option<unsafe extern "C" fn(Handle, c_int, Handle) -> c_int>,
    pub makeVDevice:
        Option<unsafe extern "C" fn(*mut c_int, *mut ncclNetVDeviceProps_v12_t) -> c_int>,
    pub finalize: Option<unsafe extern "C" fn(Handle) -> c_int>,
    pub setNetAttr: Option<unsafe extern "C" fn(Handle, Handle) -> c_int>,
}
