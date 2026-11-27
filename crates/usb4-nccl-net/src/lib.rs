//! NCCL and RCCL host/staged network transport, using the vendor net v12 ABI.

mod bindings;
mod state;

pub use bindings::*;
use state::*;

#[unsafe(no_mangle)]
pub static mut ncclNetPlugin_v12: ncclNet_v12_t = ncclNet_v12_t {
    name: c"Usb4Rdma".as_ptr(),
    init: Some(init),
    devices: Some(devices),
    getProperties: Some(get_properties),
    listen: Some(listen),
    connect: Some(connect),
    accept: Some(accept),
    regMr: Some(reg_mr),
    regMrDmaBuf: None,
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
    makeVDevice: None,
    finalize: Some(finalize),
    setNetAttr: Some(set_net_attr),
};

#[cfg(test)]
mod tests;
