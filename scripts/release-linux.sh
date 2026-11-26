#!/usr/bin/env bash
set -euo pipefail
umask 022

if [[ $# != 1 ]]; then
    echo "Usage: $0 OUTPUT_DIRECTORY" >&2
    exit 2
fi
if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
    echo "error: the release bundle requires x86-64 Linux" >&2
    exit 1
fi

mkdir -p -- "$1"
output=$(cd -- "$1" && pwd)
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

if [[ -n $(git status --porcelain) ]]; then
    echo "error: build the release from a clean checkout" >&2
    exit 1
fi
name=usb4-rdma-linux-x86_64
archive=$name.tar.gz
if [[ -e $output/$archive || -e $output/$archive.sha256 ]]; then
    echo "error: release files already exist in $output" >&2
    exit 1
fi
revision=$(git rev-parse HEAD)
epoch=$(git log -1 --format=%ct "$revision")
work=$(mktemp -d "$output/.$name.XXXXXX")
trap 'rm -rf -- "$work"' EXIT

mkdir -p -- "${CARGO_TARGET_DIR:-target}"
CARGO_TARGET_DIR=$(cd -- "${CARGO_TARGET_DIR:-target}" && pwd)
export CARGO_TARGET_DIR
cargo build --locked --release --workspace --all-features
U4RD_PROFILE=release sh scripts/test-c-api.sh
make -C adapters/ofi
sh adapters/ofi/test.sh --idle-check
sh adapters/ofi/test.sh --correctness

bundle=$work/$name
mkdir -p "$bundle"/{bin,lib}
git archive "$revision" -- README.md LICENSE NOTICE CONTRIBUTING.md SECURITY.md \
    include docs examples benchmarks scripts/bench-usb4-vs-eth.py scripts/bench-rocm-rccl.py |
    tar -xf - -C "$bundle"
cp "$CARGO_TARGET_DIR"/release/usb4-rdma-{daemon,bench} "$bundle/bin/"
cp "$CARGO_TARGET_DIR"/release/lib{usb4_rdma,usb4-fi,rccl_net_usb4}.so "$bundle/lib/"
ln -s librccl_net_usb4.so "$bundle/lib/librccl-net-usb4.so"
printf '%s\n' "$revision" > "$bundle/SOURCE_REVISION"
python3 scripts/bundle-licenses.py --target x86_64-unknown-linux-gnu --output "$bundle/licenses"

(
    cd "$bundle"
    find . \( -type f -o -type l \) -print0 | sort -z | xargs -0 sha256sum > "$work/SHA256SUMS"
    mv "$work/SHA256SUMS" .
    sha256sum --check --strict --quiet SHA256SUMS
)
(
    cd "$work"
    tar --sort=name --mtime="@$epoch" --owner=0 --group=0 --numeric-owner -cf - "$name" |
        gzip -n > "$archive"
    sha256sum "$archive" > "$archive.sha256"
    sha256sum --check --strict "$archive.sha256"
)
mv -- "$work/$archive" "$work/$archive.sha256" "$output/"
