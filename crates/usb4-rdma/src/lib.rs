//! Generic USB4 RDMA clustering client library (Rust SDK + C11 FFI).

mod context;
pub mod ffi;
mod mr;
mod qp;

pub use context::{CompletionQueue, Context, FabricStats, ProtectionDomain};
pub use mr::{BorrowedMr, MrRef, OwnedMr};
pub use qp::{ConnectionRequest, Listener, QueuePair};
pub use usb4_rdma_core::{
    Error, NodeId, Result,
    rdma::{AccessFlags, DeviceAttr, EndpointHandle, WcOpcode, WcStatus, WorkCompletion},
};
