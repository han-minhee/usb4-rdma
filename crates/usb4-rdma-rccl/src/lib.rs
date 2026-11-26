//! ROCm RCCL / NCCL v6 & v8 network plugin adapter over `usb4-rdma`.

mod bindings;
#[cfg(feature = "hip-staging")]
mod hip;
mod state;

pub use bindings::*;
use state::*;

#[unsafe(no_mangle)]
pub static mut ncclNetPlugin_v6: ncclNet_v6_t = ncclNet_v6_t {
    name: c"Usb4Rdma".as_ptr(),
    init: Some(init),
    devices: Some(devices),
    getProperties: Some(properties6),
    listen: Some(listen),
    connect: Some(connect6),
    accept: Some(accept6),
    regMr: Some(reg6),
    regMrDmaBuf: Some(reg_dma),
    deregMr: Some(dereg),
    isend: Some(isend),
    irecv: Some(irecv),
    iflush: Some(iflush),
    test: Some(test),
    closeSend: Some(close_send),
    closeRecv: Some(close_recv),
    closeListen: Some(close_listen),
};

#[unsafe(no_mangle)]
pub static mut ncclNetPlugin_v8: ncclNet_v8_t = ncclNet_v8_t {
    name: c"Usb4Rdma".as_ptr(),
    init: Some(init),
    devices: Some(devices),
    getProperties: Some(properties8),
    listen: Some(listen),
    connect: Some(connect8),
    accept: Some(accept8),
    regMr: Some(reg8),
    regMrDmaBuf: Some(reg_dma),
    deregMr: Some(dereg),
    isend: Some(isend),
    irecv: Some(irecv),
    iflush: Some(iflush),
    test: Some(test),
    closeSend: Some(close_send),
    closeRecv: Some(close_recv),
    closeListen: Some(close_listen),
    getDeviceMr: None,
    irecvConsumed: None,
};
