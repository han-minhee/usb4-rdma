# usb4-rdma

Read `README.md`, `docs/DEVELOPMENT.md`, `docs/PROTOCOL.md`, and `docs/MEMORY.md` before changing code.

- Target Linux across the workspace, examples, and verification.
- Keep protocol, memory-ownership, and RPC C header (`include/usb4_rpc.h`) documentation consistent with the implementation.
- Keep steady-state data paths bounded with zero per-packet heap allocation.
- Document every `unsafe` block's lifetime and synchronization invariants (`unsafe_op_in_unsafe_fn` and `clippy::undocumented_unsafe_blocks` are denied workspace-wide).
- Keep the repository vendor-neutral and public-safe: do not add hardware-vendor lock-in to core crates or commit private hostnames, IPs, credentials, or internal deployment logs.
- Keep emulated multiprocess IPC tests, Linux `io_uring` checks, and physical Linux `USB4STREAM` hardware acceptance distinct in validation reports.
