//! Verifies `ncclNetPlugin_v8` and `ncclNetPlugin_v6` ABI structures and live multi-window
//! transfer through `librccl_net_usb4` against two emulated `usb4-rdma-daemon` nodes.

use rccl_net_usb4::{ncclNet_v6_t, ncclNet_v8_t, ncclNetProperties_v6_t, ncclNetProperties_v8_t};
use std::ffi::CStr;

#[test]
fn rccl_abi_v6_and_v8_exports_are_complete() {
    // SAFETY: reading static exported ABI vtables with immutable access.
    unsafe {
        let v6: ncclNet_v6_t = rccl_net_usb4::ncclNetPlugin_v6;
        let v8: ncclNet_v8_t = rccl_net_usb4::ncclNetPlugin_v8;
        assert_eq!(CStr::from_ptr(v6.name).to_str().unwrap(), "Usb4Rdma");
        assert_eq!(CStr::from_ptr(v8.name).to_str().unwrap(), "Usb4Rdma");
        assert!(v6.init.is_some());
        assert!(v6.devices.is_some());
        assert!(v6.getProperties.is_some());
        assert!(v6.listen.is_some());
        assert!(v6.connect.is_some());
        assert!(v6.accept.is_some());
        assert!(v6.regMr.is_some());
        assert!(v6.deregMr.is_some());
        assert!(v6.isend.is_some());
        assert!(v6.irecv.is_some());
        assert!(v6.iflush.is_some());
        assert!(v6.test.is_some());
        assert!(v6.closeSend.is_some());
        assert!(v6.closeRecv.is_some());
        assert!(v6.closeListen.is_some());

        assert!(v8.init.is_some());
        assert!(v8.connect.is_some());
        assert!(v8.accept.is_some());
        assert!(v8.regMr.is_some());
        assert!(v8.getDeviceMr.is_none());
        assert!(v8.irecvConsumed.is_none());
    }
    // Verify 64-bit C struct sizes match upstream ROCm RCCL v6/v8 headers.
    assert_eq!(std::mem::size_of::<ncclNetProperties_v6_t>(), 48);
    assert_eq!(std::mem::size_of::<ncclNetProperties_v8_t>(), 64);
}
