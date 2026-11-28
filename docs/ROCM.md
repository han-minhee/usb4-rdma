# ROCm collectives and inference

Complete the [USB4 fabric setup](QUICKSTART.md) and confirm `rocminfo` / PyTorch HIP visibility on each host before loading the RCCL plugin.

## Install and configure the RCCL plugin

Build the ROCm net v12 plugin (`ROCm 10.2` / `RCCL 2.32.3`; for CUDA use `--features cuda` and `libnccl-net-usb4.so`):

```sh
cargo build --locked --release -p usb4-nccl-net --no-default-features --features rocm
cp target/release/libusb4_net.so target/release/librccl-net-usb4.so
export LD_LIBRARY_PATH="$PWD/target/release${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export NCCL_NET=Usb4Rdma NCCL_NET_PLUGIN=usb4 NCCL_NET_GDR_LEVEL=LOC
export USB4_RDMA_SOCKET="$HOME/.local/run/usb4-rdma/node.sock"
```

When running in containers, pass GPU devices (`/dev/kfd`, `/dev/dri`), mount the daemon socket directory, and match the container UID to the daemon UID. For `gfx1151`, build SGLang from `sglang/docker/rocm-gfx1151.Dockerfile` (`v0.5.23`).

## Collectives and TP=2 serving

1. **Validate RCCL collectives**: Run `scripts/bench-rocm-rccl.py` (see [BENCHMARK.md](BENCHMARK.md)) and confirm `NCCL INFO Using network Usb4Rdma` and matching per-cable counters in `usb4-rdma-ctl monitor --links`.
2. **Serve TP=2 inference**: Pass `--disable-custom-all-reduce` so TP AllReduce traffic traverses RCCL over `usb4-rdma`.

Reference SGLang `gfx1151` fixtures:

| Model | Source / Mode | Allocator cap / rank | KV tokens / rank |
| --- | --- | ---: | ---: |
| Gemma 4 31B | BF16 (`842da3794eaa0b77d5f08bae87a17459d91ff475`) | 42 GiB | 4,096 |
| Qwen Flash Next | Bounded dummy (`Qwen/Qwen3.8-Flash-Next-FP8` config, `--num-experts 32`) | 24 GiB | 8,192 |
