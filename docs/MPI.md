# Open MPI over USB4

The `usb4` libfabric (`FI_EP_RDM`) provider bridges Open MPI (`mtl/ofi`) host-memory point-to-point messaging, collectives, and manual-progress RMA over `usb4-rdma-ctl`.

## Build and verify

Tested with [Libfabric 2.7.0](https://github.com/ofiwg/libfabric/releases/tag/v2.7.0) (qualification used 2.4.0) and [Open MPI 5.0.11](https://www.open-mpi.org/software/ompi/v5.0/) under the same prefix on all hosts:

```sh
MPI_PREFIX="$HOME/.local/usb4-mpi"
sh adapters/ofi/build-reference.sh "$MPI_PREFIX"
export PATH="$MPI_PREFIX/bin:$PATH"
export LD_LIBRARY_PATH="$MPI_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PKG_CONFIG_PATH="$MPI_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"

make -C adapters/ofi
export FI_PROVIDER_PATH="$PWD/target/release" FI_PROVIDER=usb4
fi_info -p usb4 -t FI_EP_RDM
```

Run the 4-rank correctness suite across two hosts (2 ranks/host):

```sh
mpicc -O2 -std=c11 -Wall -Wextra -Werror benchmarks/mpi-qualify.c -o /tmp/mpi-qualify
mpirun --hostfile hosts -np 4 --map-by ppr:2:node \
  --mca mpi_add_procs_cutoff 9 --mca pml cm --mca mtl ofi \
  --mca mtl_ofi_provider_include usb4 --mca mtl_ofi_av table \
  --mca mtl_ofi_disable_hmem 1 --mca mtl_ofi_tag_mode ofi_tag_1 \
  -x FI_PROVIDER -x FI_PROVIDER_PATH -x USB4_RDMA_SOCKET -x LD_LIBRARY_PATH \
  /tmp/mpi-qualify --correctness
```

For Ethernet controls, select `--mca pml ob1 --mca btl tcp,self`.

## Provider scope and limits

- Supports `MPI_THREAD_FUNNELED` with host buffers and registered per-peer send/receive windows (~8 MiB TX / 9 MiB RX per peer, messages up to 64 MiB; eager messages <= 16 KiB carry inline credits, larger transfers use rendezvous).
- Host `FI_RMA` supports single iovec, provider-assigned keys, and target manual progress (`FI_ATOMIC` and `FI_HMEM` are on the [roadmap](ROADMAP.md)).
