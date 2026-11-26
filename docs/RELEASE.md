# v0.1.0 Release

`v0.1.0` ships the Linux `USB4STREAM` daemon, Rust SDK, C11 ABI,
benchmarks, Open MPI OFI provider, and optional ROCm RCCL plugin. Verify on
Linux with `sh scripts/check.sh` and `cargo build --release --locked --workspace --all-features`.
Headers are in `include/`. Binaries and libraries are in `target/release/`.

## Build and verify the Linux bundle

Use a clean Git checkout on x86-64 Ubuntu 24.04 with Rust 1.99.0, rustfmt,
Clippy, a C/C++ toolchain, and Python 3. Install the pinned MPI and libfabric
reference stack and export its environment as shown in the [MPI guide](MPI.md).

~~~sh
sh scripts/check.sh
./scripts/release-linux.sh ../release-bundle
cd ../release-bundle
sha256sum -c usb4-rdma-linux-x86_64.tar.gz.sha256
tar -xzf usb4-rdma-linux-x86_64.tar.gz
cd usb4-rdma-linux-x86_64
sha256sum -c SHA256SUMS
cat SOURCE_REVISION
~~~

The bundle script runs the release C11 and four-rank OFI tests. The archive
includes the exact Git revision, dependency license notices, and the RCCL plugin
symlink. Install the matching libfabric runtime before loading the OFI plugin.
Physical operation requires compatible `USB4STREAM` kernels on both hosts.

## Release scope

[Benchmark data](../benchmarks/results) covers host memory and two-host MPI.
Cite inference results with the model, quantization, kernel, GPU runtime,
cable count, and data path.

Readiness polling (`--backend usb4`) defaults to per-link worker threads, a 200 µs post-progress spin budget, and a 100 µs active probe interval. Kernel busy polling (`--backend usb4-busy-poll`) also defaults to per-link worker threads with a 500 µs spin budget and 50 µs active probe interval. Operators who need lower idle CPU usage can set `--spin-us 0 --active-probe-us 1000` on both daemons. [Default selection measurements](../benchmarks/README.md#selecting-the-readiness-defaults)

The RCCL adapter advertises one receive per group and applies backpressure until the prior receive on that communicator is retired. This preserves receive ordering when RCCL reuses a tag. The two-host qualification exercised Open MPI, ROCm/RCCL, and real tensor-parallel inference over Ethernet and physical USB4 cables.
