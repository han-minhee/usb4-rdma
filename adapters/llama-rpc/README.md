# llama.cpp USB4STREAM transport

Use an isolated llama.cpp `v1.1.0` checkout. Build the Rust plugin and apply the transport adapter to that checkout.

```sh
cargo build --locked --release -p usb4-llama-rpc
export U4RD_LIB="$PWD/target/release"
python3 adapters/llama-rpc/prepare.py /path/to/llama.cpp
cmake -S /path/to/llama.cpp -B /path/to/build \
  -DGGML_RPC=ON -DGGML_RPC_RDMA=OFF -DCMAKE_LIBRARY_PATH="$U4RD_LIB"
cmake --build /path/to/build --target rpc-server llama-server
```

Set `GGML_RPC_USB4=1`, `USB4_RDMA_SOCKET`, and `LD_LIBRARY_PATH` on both hosts. Start `rpc-server` and point `llama-server --rpc` to its private IP endpoint. The TCP connection exchanges the USB4 listener handle. RPC payloads then use the USB4 queue pair. Start from [the USB4 fabric setup](../../docs/QUICKSTART.md).

Verify `USB4STREAM` selection in the transport log, load concurrent tensor ranges, and check payload-byte deltas on both nodes with `usb4-rdma-ctl monitor --links`. The same adapter can be exercised on two `--backend emulated` nodes for software verification.

Run the CPU fixture with `sh adapters/llama-rpc/test.sh /path/to/llama.cpp /path/to/build`. It uploads two tensors concurrently, reads them back, waits 35 seconds, executes a small graph, and verifies peer payload counters. The worker waits for a new command while the peer remains connected. The initial HELLO and each payload read retain the normal timeout.
