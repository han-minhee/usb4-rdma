# Release

## Rust crates

The `0.1.0` crate release includes the Rust SDK, daemon CLI, wire codec,
transport backends, simulator, GPU staging, and the three native plugin
source packages. The release uses wire protocol and daemon IPC version 2.
See the [crate README](../crates/README.md) for installation and usage.

Use the pinned Rust toolchain and the Linux prerequisites in
[Development](DEVELOPMENT.md). From a clean release commit, run:

```sh
sh scripts/check.sh
cargo package --workspace --locked --all-features
cargo publish --dry-run --workspace --locked --all-features
```

Cargo verifies all nine archives in `target/package` using their normalized
manifests. Check that all archives contain the README and MIT license, and
that `usb4-llama-rpc` includes `include/usb4_rpc.h`.

After release approval, authenticate with crates.io via `cargo login` and
publish the workspace in dependency order:

```sh
cargo publish --workspace --locked --all-features
```

If publishing stops partway, check which `0.1.0` packages are visible in
the index before retrying. Exclude the versions already uploaded with
`--exclude <crate>` for each published workspace member. After the uploads,
verify the registry install in a fresh directory:

```sh
install_dir=$(mktemp -d)
cargo install usb4-rdma-ctl --version 0.1.0 --locked --root "$install_dir"
"$install_dir/bin/usb4-rdma-ctl" --version
```

## Linux bundle

Build at a clean Git revision on x86-64 Linux using the [development prerequisites](DEVELOPMENT.md). The current bundle contains `usb4-rdma-ctl`, the OFI and ggml-rpc libraries, the RPC C header, and independent CUDA and ROCm builds of the network plugin. Install matching libfabric before loading the OFI provider.

```sh
sh scripts/check.sh
./scripts/release-linux.sh ../release-bundle
cd ../release-bundle
sha256sum -c usb4-rdma-linux-x86_64.tar.gz.sha256
tar -xzf usb4-rdma-linux-x86_64.tar.gz
cd usb4-rdma-linux-x86_64
sha256sum -c SHA256SUMS
cat SOURCE_REVISION
```

Deploy the same source and protocol version to every participant in a fabric. Use a fresh epoch and task-owned endpoints for qualification. The [qualification record](QUALIFICATION.md) identifies the versions and device profiles in the historical measurements.
