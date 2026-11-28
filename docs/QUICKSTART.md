# Connect two Linux hosts

## 1. Build or unpack tools

Build with Rust 1.100 and a C11 compiler, or unpack the release tarball:

```sh
cargo build --locked --release --workspace --all-features
```

## 2. Check and provision USB4STREAM endpoints

Run `usb4-rdma-ctl doctor` on both hosts after connecting the USB4 cable(s) to record each host's service ID. On host A, create the endpoint and read its hop IDs:

```sh
sudo usb4-rdma-ctl init --service "$SERVICE_A" --name u4rd --apply
endpoint="/sys/kernel/config/thunderbolt/stream/$SERVICE_A/u4rd"
cat "$endpoint/in_hopid" "$endpoint/out_hopid" "$endpoint/index"
```

Cross-wire host B's hop IDs (`in_hopid = A's out_hopid`, `out_hopid = A's in_hopid`):

```sh
sudo usb4-rdma-ctl init --service "$SERVICE_B" --name u4rd \
  --in-hopid="$A_OUT" --out-hopid="$A_IN" --apply
```

Run `usb4-rdma-ctl doctor` again to locate `/dev/tbstream*` and grant device access to the operating user. Repeat with a distinct endpoint name for a dual-cable bond.

## 3. Start the fabric

Copy [examples/two-node.toml](../examples/two-node.toml) or [examples/dual-link.toml](../examples/dual-link.toml) to both hosts with matching endpoint paths and a fresh positive `epoch`:

```sh
install -d -m 0700 "$HOME/.local/run/usb4-rdma"
export USB4_RDMA_SOCKET="$HOME/.local/run/usb4-rdma/node.sock"
usb4-rdma-ctl serve --config cluster.toml --node "$NODE_ID" \
  --socket "$USB4_RDMA_SOCKET" --backend uring
```

Use `NODE_ID=0` on host A and `NODE_ID=1` on host B (`--backend usb4` for readiness moderation, or `--backend usb4-busy-poll` for busy-poll endpoints). Verify readiness and byte counters:

```sh
usb4-rdma-ctl monitor --socket "$USB4_RDMA_SOCKET" --links
```

Containers need read access to ConfigFS, rw access to `/dev/tbstream*`, the mounted socket directory under the daemon UID, and `io_uring_*` syscalls permitted if using `--backend uring`.

## Troubleshooting

- **No service IDs in `doctor`**: Verify `CONFIG_USB4_STREAM`, `CONFIG_USB4_CONFIGFS`, `CONFIG_CONFIGFS_FS`, and cable negotiation.
- **Fabric stays unready**: Check matching `epoch`, crossed hop IDs, and device permissions.
- **After a link drop**: Increment the `epoch` across all nodes and restart with `SIGTERM`.
