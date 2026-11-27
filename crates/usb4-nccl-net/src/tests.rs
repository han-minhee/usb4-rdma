use crate::{ncclNet_v12_t, ncclNetPlugin_v12, ncclNetProperties_v12_t};
use std::{
    ffi::CStr,
    mem::{offset_of, size_of},
};

#[test]
fn net_v12_layout_and_callbacks() {
    // SAFETY: the plugin exports its immutable-at-runtime vtable before loading any state.
    let vtable = unsafe { ncclNetPlugin_v12 };
    // SAFETY: the static plugin name is a NUL-terminated literal.
    let name = unsafe { CStr::from_ptr(vtable.name) };
    assert_eq!(name.to_bytes(), b"Usb4Rdma");
    assert!(vtable.init.is_some() && vtable.finalize.is_some());
    assert!(vtable.irecv.is_some() && vtable.isend.is_some() && vtable.iflush.is_some());
    assert!(vtable.getDeviceMr.is_none() && vtable.regMrDmaBuf.is_none());
    assert_eq!(size_of::<ncclNet_v12_t>(), 22 * size_of::<usize>());
    assert_eq!(offset_of!(ncclNetProperties_v12_t, maxP2pBytes), 104);
    assert_eq!(size_of::<ncclNetProperties_v12_t>(), 128);
}
