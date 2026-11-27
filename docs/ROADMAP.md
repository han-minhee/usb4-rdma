# Qualification and follow-up

- Run the current direct USB4STREAM profiles on a physical single- and dual-cable pair. Record directional throughput, payload counters, CPU cost, and small-message tail latency.
- Qualify native OFI consumer releases individually. The provider exposes host message, tagged, and single-iovec host RMA operations. MPI windows, atomics, and accelerator-aware OFI each require their own acceptance.
- Run CUDA NCCL v12 and ROCm RCCL v12 collectives on their matching vendor runtimes. Separately qualify Level Zero staging and the coherent UMA fast path on the intended allocations.
- Measure the ggml-rpc transport with concurrent tensor loads and decode-sized requests using the pinned llama.cpp fixture. Track the source revision, backend, and link counters.
- Driver-level direct GPU DMA requires a separate USB4STREAM memory registration and synchronization contract.
