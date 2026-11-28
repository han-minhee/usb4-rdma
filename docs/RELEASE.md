# Release

Version `0.1.0` uses wire protocol and daemon IPC v2 ([crate list](../crates/README.md)).

## Verify and publish crates

Run the preflight from the root of a clean Linux checkout with working
symlinks, Rust `1.100.0`, and the [development prerequisites](DEVELOPMENT.md).
The workspace tests require Linux to permit `io_uring` with registered buffers.
The dry run builds the nine packaged crates without uploading them.
Then Cargo writes archives to the target directory for inspection. Comparing
the archived files with the originals catches symlink paths stored as text.

```sh
(
    set -eu
    test -z "$(git status --porcelain)"
    export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"
    sh scripts/check.sh
    cargo publish --dry-run --workspace --locked --all-features --registry crates-io
    cargo package --workspace --locked --all-features --registry crates-io --no-verify

    version=0.1.0
    package_dir="$CARGO_TARGET_DIR/package"
    for manifest in crates/*/Cargo.toml; do
        crate=${manifest%/Cargo.toml}
        release=${crate##*/}-$version
        archive="$package_dir/$release.crate"
        printf 'Checking %s\n' "$archive"
        tar -xOzf "$archive" "$release/README.md" | cmp crates/README.md -
        tar -xOzf "$archive" "$release/LICENSE" | cmp LICENSE -
    done

    rpc=usb4-llama-rpc-$version
    tar -xOzf "$package_dir/$rpc.crate" "$rpc/include/usb4_rpc.h" | cmp include/usb4_rpc.h -
    ctl=usb4-rdma-ctl-$version
    tar -tzf "$package_dir/$ctl.crate" "$ctl/Cargo.lock" >/dev/null
)
```

After the preflight passes, publish from the verified revision.

```sh
cargo login --registry crates-io && cargo publish --workspace --locked --all-features --registry crates-io
```

If publishing is interrupted, check which `0.1.0` versions reached the crates.io
index. If any crates still need publishing, retry with an `--exclude <crate>`
for each already indexed `0.1.0` crate.

Once all nine versions are indexed, verify a fresh install. The binary should
report `usb4-rdma-ctl 0.1.0`.

```sh
(
    set -eu
    install_dir=$(mktemp -d)
    trap 'rm -rf -- "$install_dir"' EXIT
    cargo install --registry crates-io --version 0.1.0 --locked --root "$install_dir" usb4-rdma-ctl
    "$install_dir/bin/usb4-rdma-ctl" --version
)
```

## Build Linux release bundle

Includes `usb4-rdma-ctl`, OFI & `ggml-rpc` libraries, RPC C header, and CUDA/ROCm net plugins:

```sh
sh scripts/check.sh
./scripts/release-linux.sh ../release-bundle
(cd ../release-bundle && sha256sum -c usb4-rdma-linux-x86_64.tar.gz.sha256)
```
