# Memory and completion ownership

`usb4-rdma-ctl` runs in a separate process without `CAP_SYS_PTRACE` and supports two memory registration models:

- **Shared MRs (`alloc_mr` / `reg_mr_fd`)**: Page-aligned sealed `memfd` passed via `SCM_RIGHTS`. Handles one-sided `RDMA_WRITE` and `RDMA_READ` inside the daemon without target app involvement and skips app-to-IPC copies (`MAX_CLIENT_MRS = 64`, default `--max-mr-bytes = 2 GiB`).
- **Borrowed Local MRs (`reg_mr_local`)**: Borrows caller buffers (`LOCAL_READ | LOCAL_WRITE` only; remote access rejected) and stages payloads through a bounded 64-slot (`1 MiB`/slot) shared-memory IPC arena.

## Safety and dataflow contracts

- **Lifetime & ordering**: Callers must keep local buffers live until completion and synchronize concurrent reads/writes against in-flight one-sided `RDMA_WRITE` ranges (`RDMA_WRITE_WITH_IMM`, ordered `SEND`, or barrier).
- **Safe Rust access & deregistration**: Shared-memory wrappers perform bounds-checked copies without aliasing `&mut [u8]` references. `CTRL_DEREG_MR` revokes `rkey` immediately and defers `munmap` until active transfers finish.
- **IPC ownership**: Arena slots transition `FREE -> POSTED -> BUSY -> READY -> CONSUMED -> COMPLETE/FAILED` using release/acquire atomics (`loom`-verified).
- **Adapter staging**: Both the OFI (`mpi`) and `ggml-rpc` adapters keep registered per-peer send/receive windows for direct daemon payload assembly. Upstream Linux `USB4STREAM` currently stages through kernel DMA pages; direct GPU zero-copy (`DeviceAttr::zero_copy_dma`) remains on the [roadmap](ROADMAP.md).
