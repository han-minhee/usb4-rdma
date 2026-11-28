# Release

Version `0.1.0` uses wire protocol and daemon IPC v2 ([crate list](../crates/README.md)).

## Verify and publish crates

```sh
sh scripts/check.sh
cargo package --workspace --locked --all-features
cargo publish --dry-run --workspace --locked --all-features
cargo login && cargo publish --workspace --locked --all-features
```

Confirm each crate archive contains README/MIT license (and `include/usb4_rpc.h` for `usb4-llama-rpc`), then verify `cargo install usb4-rdma-ctl --version 0.1.0 --locked`.

## Build Linux release bundle

Includes `usb4-rdma-ctl`, OFI & `ggml-rpc` libraries, RPC C header, and CUDA/ROCm net plugins:

```sh
sh scripts/check.sh
./scripts/release-linux.sh ../release-bundle
(cd ../release-bundle && sha256sum -c usb4-rdma-linux-x86_64.tar.gz.sha256)
```
