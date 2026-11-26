# Roadmap

`usb4-rdma` v0.1 ships the core, daemon, Rust SDK, and C11 API (`include/usb4_rdma.h`).
Keep the core independent of hardware vendors and ML frameworks.

## 1. Clustering & Collective Adapters (Separate Adapter Crates)

- **Collective adapters:** The current RCCL adapter exports v6 and v8 and supports host pointers and HIP staging. Future work includes newer ABI versions and additional accelerator runtimes.
- **MPI and communication frameworks:** Extend the host-memory OFI provider beyond its pinned Open MPI reference build. Evaluate MPICH, MPI one-sided windows, UCX, GPU-aware MPI, and the integration paths used by PyTorch Distributed and Ray.
- **oneCCL / RPC bridges:** Optional adapters for heterogeneous CPU/GPU inference and distributed tensor RPC.

## 2. Pluggable Transport Backends (`LinkBackend`)

- **Custom Linux kernel zero-copy USB4 DMA interface:** Long-term user page pinning, scatterlist IOMMU mapping against the USB4 DMA device, bounded SQ/CQ rings, and safe unplug quiescing (flipping `DeviceAttr::zero_copy_dma = true` only when verified).
- **DMA-BUF / GPUDirect-style accelerator integration:** Explicit DMA-BUF attachment lifetime, scatterlist mapping, and `dma_resv` fences across vendors.

## 3. Fabric & Operations

- Dynamic topology discovery / automatic ConfigFS HopID pairing helper.
- Hardware qualification & multi-hour soak harness for single-cable, dual-cable bonded, and multi-hop physical USB4 clusters.
- Packaging (systemd units, container runtime profiles, pkg-config `.pc` file for `libusb4_rdma`).
