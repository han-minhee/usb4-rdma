//! Device pointers are staged into CPU-accessible memory before crossing IPC or transport.

pub const MIN_SLOT: usize = 256 * 1024;
pub const MAX_SLOT: usize = 1024 * 1024;

#[cfg(any(feature = "cuda", feature = "rocm"))]
mod runtime;
#[cfg(any(feature = "cuda", feature = "rocm"))]
pub use runtime::{Runtime, Staging};

#[cfg(feature = "uma")]
pub mod uma;
#[cfg(feature = "ze")]
pub mod ze;
