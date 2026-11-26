# Architecture

`usb4-rdma` separates transport, routing, memory ownership, and workload adapters.

## Layer Stack

1. **`usb4-rdma-core`** (`#![forbid(unsafe_code)]`)
   - Cluster topology schema (`Cluster`, `Node`, `Link`) with connected-graph validation and canonical SHA-256 topology digest.
   - `U4RD` v1 wire header (104 bytes), `Packet`, and setup-allocated `PacketPool`.
   - Vendor-neutral RDMA primitives: `AccessFlags`, `WcOpcode`, `WcStatus`, `WorkCompletion`, `DeviceAttr`, `EndpointHandle`.

2. **`usb4-rdma-transport`**
   - Defines `LinkBackend` (`capabilities`, `try_submit`, `poll`, `shutdown`) and `LinkCapabilities`.
   - `configfs.rs` discovers and provisions Linux `USB4STREAM` links. `StreamLink` polls readiness or busy-polls, with up to 64 frames per TX batch and 32 frames of RX read-ahead.
   - `ThreadedLink` adds per-link workers. `UringLink` uses fixed `io_uring` buffers (`--features uring`). `Bond` stripes transfers above 32 KiB in 32 KiB chunks. `SimIo` injects deterministic faults.
   - `MemoryDomain` defines `Host` and reserves `DmaBuf`. The core has no GPU SDK dependency.

3. **`usb4-rdma-router`**
   - Deterministic BFS spanning-tree `Routes`: direct neighbors always use their direct edge; multi-hop pairs traverse an acyclic spanning tree rooted at the lowest `NodeId`.
   - `Fabric` state machine: incarnation-nonce `Hello`/`HelloAck`, active (100 µs for `usb4` readiness, 50 µs for `usb4-busy-poll`) and idle (`timeout / 4`) `Ping`/`Pong` liveness, separate bounded control / direct / transit queues, hop-credit flow control across `Data`, `RdmaWrite`, and `RdmaReadResp` frames, and strict fail-stop epoch semantics.

4. **`usb4-rdma-ipc`**
   - Lock-free 64-slot shared-memory IPC arena (`1 MiB` window per slot) with atomic `FREE -> POSTED -> BUSY -> READY -> CONSUMED -> COMPLETE/FAILED` ownership (`loom`-checked).
   - Per-client Unix domain socket carrying the initial `Welcome` + arena FD (`SCM_RIGHTS`) and control-plane `CTRL_REG_MR` / `CTRL_DEREG_MR` requests for shared-memory MRs.
   - Exclusive `.lock` inode and fsynced epoch journal rejecting epoch reuse at the same socket even after `SIGKILL`.

5. **`usb4-rdma-daemon`**
   - Single-process per-node daemon owning link FDs and mapped client MRs.
   - Full-duplex Reliable Connected (RC) Queue Pairs supporting simultaneous two-sided `SEND`/`RECV`/`PEEK` and one-sided `RDMA_WRITE` / `RDMA_WRITE_WITH_IMM` / `RDMA_READ`.
   - `MrTable` enforces per-client MR count and total mapped-byte budgets, random 64-bit `rkey` capabilities, connection-owner checks, `AccessFlags`, and deferred unmap draining for active `RDMA_READ` responders.

6. **`usb4-rdma` + `include/usb4_rdma.h`**
   - Rust SDK (`Context`, `ProtectionDomain`, `OwnedMr`, `BorrowedMr`, `CompletionQueue`, `Listener`, `QueuePair`) and C11 FFI (`u4rd_*`, `catch_unwind`-guarded).
   - Transparently chunks transfers larger than `WINDOW` (1 MiB), enforces per-QP send/write FIFO ordering, and advances all active ops on a `Context` whenever any `CompletionQueue` is polled.


7. **Optional workload adapters**
   - `usb4-rdma-ofi` and `adapters/ofi` provide the libfabric tagged RDM interface used by Open MPI. Ranks on one host exchange messages over same-user Unix sockets. Ranks on separate hosts use SDK queue pairs. The provider sends messages of up to 16 KiB eagerly. Bounded queues preserve tag matching and reserve space for unexpectedly arriving data. Large messages use offers and grants.
   - `usb4-rdma-rccl` implements the RCCL network plugin ABI with host staging for ROCm devices.
