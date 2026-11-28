# Connect two Linux hosts

## Build or unpack the tools

Build on Linux with Rust 1.100 and a C11 compiler, then install
the tools from `target/release`. You can also unpack the Linux x86-64
bundle into a directory you own.

~~~sh
cargo build --locked --release --workspace --all-features
~~~

Verify a downloaded bundle before adding its `bin` directory to PATH.

~~~sh
sha256sum -c usb4-rdma-linux-x86_64.tar.gz.sha256
tar -xzf usb4-rdma-linux-x86_64.tar.gz
cd usb4-rdma-linux-x86_64
sha256sum -c SHA256SUMS
export PATH="$PWD/bin:$PATH"
~~~

## Check the endpoints

Check the [kernel requirements](DEVELOPMENT.md). Connect the cable and run
`usb4-rdma-ctl doctor` on both hosts. Record service IDs, controllers,
lanes, and speeds. Run SSH over an existing private IP interface.

## Provision the pair

Set SERVICE_A and SERVICE_B to the reported service IDs. Get host A’s hop IDs.

~~~sh
sudo usb4-rdma-ctl init --service "$SERVICE_A" --name u4rd --apply
endpoint="/sys/kernel/config/thunderbolt/stream/$SERVICE_A/u4rd"
cat "$endpoint/in_hopid" "$endpoint/out_hopid" "$endpoint/index"
~~~

On host B set A_IN and A_OUT to the first two printed values. The transmit
hop on either side matches the receiving hop on its peer.

~~~sh
sudo usb4-rdma-ctl init --service "$SERVICE_B" --name u4rd \
  --in-hopid="$A_OUT" --out-hopid="$A_IN" --apply
~~~

Use doctor again to find each resulting character device. Grant access only
to the operating account on those devices. For a bond, repeat for the second
cable with a distinct endpoint name.

## Start the fabric

Copy [the two-node example](../examples/two-node.toml) or
[the bonded example](../examples/dual-link.toml) to both hosts. Set a fresh
positive epoch and replace endpoint paths, controller labels, and link rates
with the two hosts' actual results. Use the same configuration on both sides.
Create a private run directory and serve as the same user that runs the apps.

~~~sh
install -d -m 0700 "$HOME/.local/run/usb4-rdma"
export USB4_RDMA_SOCKET="$HOME/.local/run/usb4-rdma/node.sock"
usb4-rdma-ctl serve --config cluster.toml --node "$NODE_ID" \
  --socket "$USB4_RDMA_SOCKET" --backend uring
~~~

Use `NODE_ID=0` on host A and `NODE_ID=1` on host B. In a second shell,
check each daemon and its per-cable counters.

For an existing readiness-driven endpoint with kernel busy polling off,
use `--backend usb4` to retain its ConfigFS moderation. The default
`uring` backend requires zero moderation.

~~~sh
usb4-rdma-ctl monitor --socket "$USB4_RDMA_SOCKET" --links
~~~

Start clients when both peers report `fabric_ready: true` and the same epoch. Batch transfers can use both cables. Small messages may use a single
cable chosen by the scheduler.

For a containerized daemon, provision endpoints on the host. Give the daemon
read access to the selected ConfigFS endpoints and read-write access to their
corresponding character devices. Share the private Unix socket directory with
clients running under the daemon's UID. Check the container security policy
for device and socket access. The `uring` backend also needs
`io_uring_setup`, `io_uring_enter`, and `io_uring_register` in the daemon's
seccomp allowlist when that filter is enabled.

## Diagnose a connection

- If doctor shows no stream-service ID, verify the kernel configuration and
  the cable's negotiated link. Thunderbolt networking is a distinct interface.
- If the fabric fails to become ready, compare the epoch, topology digest,
  service pair, crossed hop IDs, and character-device ownership.
- Start a fresh epoch after a link failure. Stop test daemons with SIGTERM
  before changing endpoint settings. Keep the benchmark listener on a trusted
  test network. [Security notes](../SECURITY.md)
