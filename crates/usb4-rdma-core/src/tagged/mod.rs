//! Bounded host endpoint with tag masks, unexpected messages, and software RMA.

mod endpoint;
mod rma;
mod wire;

pub use endpoint::{ACCESS, AGAIN, BAD, Buffer, Completion, Endpoint, IO, Recv};
pub use rma::Request as RmaRequest;
pub use wire::{ADDRESS_BYTES, Address};
