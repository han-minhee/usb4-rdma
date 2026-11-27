# Release

## Rust crates

Release `0.1.0` uses wire protocol and daemon IPC version 2. [Crate list and setup](../crates/README.md)

Run these checks from a clean commit on Linux with the pinned Rust toolchain.
[Development prerequisites](DEVELOPMENT.md)

```sh
sh scripts/check.sh
cargo package --workspace --locked --all-features
cargo publish --dry-run --workspace --locked --all-features
```

Check the nine archives in `target/package` for the README and MIT license.
`usb4-llama-rpc` also needs `include/usb4_rpc.h`.

Run `cargo login`, then publish in dependency order.

```sh
cargo publish --workspace --locked --all-features
```

If publishing stops, check the index and retry with `--exclude <crate>` for
each published `0.1.0` crate. Then verify a clean install.

```sh
install_dir=$(mktemp -d)
cargo install usb4-rdma-ctl --version 0.1.0 --locked --root "$install_dir"
"$install_dir/bin/usb4-rdma-ctl" --version
```

## Linux bundle

Build from a clean commit on x86-64 Linux. The bundle contains `usb4-rdma-ctl`,
the OFI and ggml-rpc libraries, the RPC C header, and separate CUDA and ROCm
builds of the net plugin. Install matching libfabric before loading OFI.
[Development prerequisites](DEVELOPMENT.md)

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

Use the same source and protocol version on every host. For qualification,
use a fresh epoch and task-owned endpoints. [Tested versions and device profiles](QUALIFICATION.md)
