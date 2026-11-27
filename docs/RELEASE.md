# Linux release

Build at a clean Git revision on x86-64 Linux using the [development prerequisites](DEVELOPMENT.md). The current bundle contains `usb4-rdma-ctl`, the OFI and ggml-rpc libraries, the RPC C header, and independent CUDA and ROCm builds of the network plugin. Install matching libfabric before loading the OFI provider.

```sh
sh scripts/check.sh
./scripts/release-linux.sh ../release-bundle
cd ../release-bundle
sha256sum -c usb4-rdma-linux-x86_64.tar.gz.sha256
tar -xzf usb4-rdma-linux-x86_64.tar.gz
cd usb4-rdma-linux-x86_64
sha256sum -c SHA256SUMS
cat SOURCE_REVISION
```

Deploy the same source and protocol version to every participant in a fabric. Use a fresh epoch and task-owned endpoints for qualification. The [qualification record](QUALIFICATION.md) identifies the versions and device profiles in the historical measurements.
