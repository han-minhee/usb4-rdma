# Memory and Completion Ownership

## Two Registration Modes (`alloc_mr` vs `reg_mr_local`)

`usb4-rdma-daemon` runs separately from applications without `CAP_SYS_PTRACE` or a custom module. Register shared memory or a borrowed pointer.

1. **Shared MRs (`pd.alloc_mr(len, access)` / `pd.reg_mr_fd(fd, len, access)` / `u4rd_alloc_mr`):**
   - SDK allocations use a page-aligned Linux `memfd` created with `MFD_CLOEXEC | MFD_ALLOW_SEALING` and sealed against shrinking, growing, and changes to the seals.
   - Passed once to `usb4-rdma-daemon` over Unix `SCM_RIGHTS`, mapped with `PROT_READ` (and `PROT_WRITE` only when write access is requested), and assigned a random 64-bit `rkey` when `REMOTE_READ` or `REMOTE_WRITE` is enabled.
   - The daemon handles one-sided `RDMA_WRITE` and `RDMA_READ` without target application progress. Shared operands skip the app-to-IPC copy for `SEND`, `RDMA_WRITE`, and `RDMA_READ`.
   - Bounded by `MAX_CLIENT_MRS = 64` per client and `--max-mr-bytes` (default 2 GiB) daemon-wide.

2. **Borrowed Local MRs (`pd.reg_mr_local(ptr, len, access)` / `u4rd_reg_mr`):**
   - Borrows any caller-owned host buffer (`LOCAL_READ | LOCAL_WRITE` only) until deregistration after all operations complete.
   - Requesting `REMOTE_READ` or `REMOTE_WRITE` fails closed with `Error::UnsupportedAccess` (`U4RD_ERR_UNSUPPORTED_ACCESS`).
   - Payload is staged through the bounded 64-slot shared-memory IPC arena (`1 MiB` per slot).

## Synchronization & Aliasing Contract

- **Operation lifetime:** Keep each local memory region and its backing storage live until its posted operations complete or its context shuts down.
- **IPC slot ownership:** Each slot transitions through `FREE -> POSTED -> BUSY -> READY -> CONSUMED -> COMPLETE/FAILED` using release/acquire atomics (`loom`-verified in `crates/usb4-rdma-ipc/src/ownership_model.rs`).
- **One-sided RDMA visibility:** While an `RDMA_WRITE` is in flight to `[remote_offset..remote_offset + len]`, the target application must not concurrently read or write those bytes until synchronization completes (e.g., via `RDMA_WRITE_WITH_IMM`, a subsequent ordered `SEND`, or an out-of-band barrier).
- **Safe Rust shared-memory access:** `SharedRegion` / `OwnedMr` never creates aliasing `&mut [u8]` references across process-shared memory; `read_bytes` / `write_bytes` perform bounds- and permission-checked copies.
- **Deregistration drain:** `CTRL_DEREG_MR` revokes `rkey` immediately so no new remote operations can reference it, and defers `munmap` until admitted RDMA read responders and registered transfers finish.

## Copy Accounting

Current path for `USB4STREAM` one-sided `RDMA_WRITE` using `alloc_mr` on both endpoints:
- Initiator: shared MR → daemon `PacketPool` → wire TX buffer → kernel USB4STREAM DMA pages.
- Target: kernel USB4STREAM DMA pages → daemon RX buffer → `PacketPool` → target shared MR.
- Forwarding adds userspace/kernel staging at intermediate nodes.

End-to-end zero-copy DMA needs kernel scatterlist and IOMMU support, plus DMA-BUF attachments and fences for accelerators. Advertise it via `DeviceAttr::zero_copy_dma` after benchmarking.


### Open MPI via OFI

The OFI adapter keeps a registered send and receive window per cross-host
peer. The daemon assembles a matched standing receive directly into the
receive region and publishes completion after writing the full frame. The
adapter copies the completed payload into the MPI buffer. Registered sends
keep one daemon request across their transport windows. Ordinary SDK
receives deliver chunks through leased IPC windows.
