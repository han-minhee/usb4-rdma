#!/usr/bin/env sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"
target=${CARGO_TARGET_DIR:-"$root/target"}
profile=${U4RD_PROFILE:-debug}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

python3 - "$target/$profile" <<'PY'
import ctypes
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
provider = ctypes.CDLL(str(root / 'libusb4_fi.so'), mode=ctypes.RTLD_LOCAL)
net = ctypes.CDLL(str(root / 'libusb4_net.so'), mode=ctypes.RTLD_LOCAL)
rpc = ctypes.CDLL(str(root / 'libusb4_rpc.so'), mode=ctypes.RTLD_LOCAL)
provider.fi_prov_ini.restype = ctypes.c_void_p
assert provider.fi_prov_ini()
assert ctypes.c_void_p.in_dll(net, 'ncclNetPlugin_v12').value
for legacy in ('ncclNetPlugin_v6', 'ncclNetPlugin_v8', 'ncclNetPlugin_v9'):
    assert not hasattr(net, legacy)
rpc.usb4_rpc_begin.argtypes = [ctypes.c_void_p]
rpc.usb4_rpc_begin.restype = ctypes.c_void_p
assert rpc.usb4_rpc_begin(None) is None
assert not hasattr(rpc, 'fi_prov_ini')
assert not hasattr(provider, 'u4off_submit')
PY

cbindgen --config crates/usb4-llama-rpc/cbindgen.toml --crate usb4-llama-rpc --output "$work/usb4_rpc.h"
diff -u include/usb4_rpc.h "$work/usb4_rpc.h"
printf '%s\n' '#include "usb4_rpc.h"' 'int main(void) { return usb4_rpc_begin(0) != 0; }' > "$work/rpc.c"
${CC:-cc} -std=c11 -Wall -Wextra -Werror "$work/rpc.c" -L"$target/$profile" -lusb4_rpc -o "$work/rpc"
LD_LIBRARY_PATH="$target/$profile${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" "$work/rpc"
