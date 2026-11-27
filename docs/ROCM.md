# ROCm collectives and inference

Follow the [two-host USB4 setup](QUICKSTART.md) and install matching RCCL
and PyTorch builds. Check the GPU with `rocminfo` and
`torch.cuda.get_device_properties(0)`. PyTorch needs a usable device and a
nonempty `torch.version.hip`.

## Install the plugin

~~~sh
cargo build --locked --release -p usb4-nccl-net --no-default-features --features rocm
cd target/release
cp libusb4_net.so librccl-net-usb4.so
export LD_LIBRARY_PATH="$PWD${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export NCCL_NET=Usb4Rdma NCCL_NET_PLUGIN=usb4 NCCL_NET_GDR_LEVEL=LOC
export NCCL_DEBUG=INFO
export USB4_RDMA_SOCKET="$HOME/.local/run/usb4-rdma/node.sock"
~~~

Use ROCm 10.2 with RCCL 2.32.3 for the net v12 build. For the bundle, add its `lib` directory to `LD_LIBRARY_PATH`. The CUDA artifact uses the same net v12 interface with `--features cuda`, `libnccl-net-usb4.so`, and NCCL 2.32.6.

In a container, expose the ROCm GPU devices and mount the daemon socket
directory. Use the same Linux user ID for the container process and daemon.
Build the runtime for the target GPU architecture. For Strix Halo
(`gfx1151`), SGLang v0.5.23 ships a dedicated Dockerfile with a pinned
ROCm/PyTorch base digest.

~~~sh
git clone --depth 1 --branch v0.5.23 https://github.com/sgl-project/sglang.git
docker build -f sglang/docker/rocm-gfx1151.Dockerfile -t sglang-rocm:v0.5.23 sglang
~~~

Use the matching [AMD platform guide](https://github.com/sgl-project/sglang/blob/v0.5.23/docs/docs/hardware-platforms/amd_gpu.mdx)
for device access and serving flags. The Strix Halo benchmark receipts
identify the custom environment used for the reported measurements.
Record the base digest, runtime revision, and GPU configuration for
each new measurement.

## Run collectives before serving

Use torchrun or mpirun with one ROCm process per host. Pass the plugin
environment to both ranks, use a private rendezvous address, and run
scripts/bench-rocm-rccl.py from a path visible to each worker. The
[benchmark guide](../benchmarks/README.md#open-mpi-and-rccl) includes the
launcher commands. Verify that both ranks report the Usb4Rdma network and
that peer-matched per-cable bytes increase. Use the Socket plugin for a
separate 10 GbE control.

## Serve and measure

Follow the [serving recipe](../benchmarks/README.md#real-inference) with a
local model and one rank per host (TP=2). Use matching requests and flags
on both transports. Set `--disable-custom-all-reduce` to use RCCL.

The benchmark used BF16, Triton attention, PyTorch sampling, a 4,096-token
context, up to four active requests, and disabled graph capture and radix
caching. The GPU worker applied a PyTorch allocator cap before loading the
model. Set the same cap per rank for each transport.

| Fixture | Model input | Allocator cap per rank | Total KV tokens per rank |
| --- | --- | ---: | ---: |
| Gemma 4 31B | BF16 checkpoint at revision `842da3794eaa0b77d5f08bae87a17459d91ff475` | 42 GiB | 4,096 |
| Qwen Flash Next | Bounded metadata and seeded BF16 dummy tensors | 24 GiB | 8,192 |

Obtain the real Gemma checkpoint under the publisher's model terms.
[Pinned Gemma 4 31B revision](https://huggingface.co/google/gemma-4-31B-it/tree/842da3794eaa0b77d5f08bae87a17459d91ff475)

### Prepare the bounded Qwen fixture

Download the pinned configuration, tokenizer, template, and processors.
This workload loads BF16 dummy tensors and verifies deterministic output
from the bounded geometry.

~~~sh
hf download Qwen/Qwen3.8-Flash-Next-FP8 \
  config.json generation_config.json tokenizer.json tokenizer_config.json \
  chat_template.jinja preprocessor_config.json video_preprocessor_config.json \
  --revision 7fb48d708ca6cd8eeed8f4db0433be18af96984f --local-dir "$MODEL_PATH"

python3 - "$MODEL_PATH/config.json" <<'PYTHON'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
config = json.loads(path.read_text())
config.pop("quantization_config", None)
config["text_config"]["num_experts"] = 32
config["text_config"]["ngram_vocab_size_base"] = 1_000_000
path.write_text(json.dumps(config, indent=2) + "\n")
PYTHON
~~~

Serve Qwen with `--load-format dummy --random-seed 777` and
`--chunked-prefill-size 512`. Follow the
[dummy-weight benchmark](../benchmarks/README.md#dummy-weight-inference)
to record TTFT, decode, and concurrent throughput.
