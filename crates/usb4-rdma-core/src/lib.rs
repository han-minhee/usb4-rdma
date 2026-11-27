//! Virtual queue pairs, protected memory, and multi-rail progress over USB4 streams.

#[cfg(not(target_os = "linux"))]
compile_error!("usb4-rdma requires a Linux target");

pub mod config;
pub mod pool;
pub mod rdma;

pub use usb4_proto::{Error, NodeId, Result, protocol, reassembly};

mod context;
mod fabric;
pub mod host_memory;
pub mod ipc;
mod mr;
mod mr_table;
mod qp;
mod service;
pub mod tagged;

pub use context::{CompletionQueue, Context, FabricStats, ProtectionDomain};
pub use fabric::{Fabric, LinkFailure, LinkFailureCause, Routes};
pub use mr::{BorrowedMr, MrRef, OwnedMr};
pub use mr_table::DEFAULT_MAX_MR_BYTES;
pub use qp::{ConnectionRequest, Listener, QueuePair};
pub use rdma::{AccessFlags, DeviceAttr, EndpointHandle, WcOpcode, WcStatus, WorkCompletion};
pub use service::Service;
