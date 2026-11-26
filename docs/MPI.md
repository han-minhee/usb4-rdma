# Open MPI over USB4

The usb4 libfabric provider handles host-memory messages and collectives via
Open MPI’s OFI MTL. Qualification used Open MPI 5.0.11 and libfabric 2.4.0
on two Linux hosts with one or two ranks per host. MPI launches over IP.
The provider uses the daemon between hosts and same-user Unix sockets locally.

## Build the reference stack

Install a C/C++ compiler, make, pkg-config, curl, bzip2, and Open MPI’s
build dependencies. Use the same prefix on both hosts. The script downloads
and checks both tarballs.

- [Libfabric 2.4.0](https://github.com/ofiwg/libfabric/releases/tag/v2.4.0)
- [Open MPI 5.0.11](https://www.open-mpi.org/software/ompi/v5.0/)

~~~sh
MPI_PREFIX="$HOME/.local/usb4-mpi"
sh adapters/ofi/build-reference.sh "$MPI_PREFIX"
export PATH="$MPI_PREFIX/bin:$PATH"
export LD_LIBRARY_PATH="$MPI_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PKG_CONFIG_PATH="$MPI_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
make -C adapters/ofi
export FI_PROVIDER_PATH="$PWD/target/release"
export FI_PROVIDER=usb4
fi_info -p usb4 -t FI_EP_RDM
~~~

When using the Linux bundle, point `FI_PROVIDER_PATH` to its `lib`
directory after installing the matching libfabric version.

Set USB4_RDMA_SOCKET to the local daemon socket from the
[USB4 setup guide](QUICKSTART.md). Make the provider library available at
FI_PROVIDER_PATH on each host. Use a private hostfile with two slots per
host and an MPI executable installed at the same path on both hosts.

~~~sh
mpicc -O2 -std=c11 -Wall -Wextra -Werror benchmarks/mpi-qualify.c -o /tmp/mpi-qualify
mpirun --hostfile hosts -np 4 --map-by ppr:2:node \
  --mca mpi_add_procs_cutoff 9 --mca pml cm --mca mtl ofi \
  --mca mtl_ofi_provider_include usb4 --mca mtl_ofi_av table \
  --mca mtl_ofi_disable_hmem 1 --mca mtl_ofi_tag_mode ofi_tag_1 \
  -x FI_PROVIDER -x FI_PROVIDER_PATH -x USB4_RDMA_SOCKET -x LD_LIBRARY_PATH \
  /tmp/mpi-qualify --correctness
~~~

The test checks matching, probes, cancellation, truncation, ordering,
payloads up to 64 MiB, and collectives. Save `monitor --links` output before
and after on both hosts. Confirm the peer byte deltas match. For emulation,
build the release daemon and provider, then run `sh adapters/ofi/test.sh`.

## Interface scope

Use MPI_THREAD_FUNNELED with host buffers in the reference configuration.
The mpi_add_procs_cutoff setting registers the job's addresses before MPI
operations begin. The provider uses that address list to pair connections,
including those that carry wildcard receives.
Start one provider endpoint per rank. Its resource bounds include eight
address entries, 256 outstanding operations and completion entries, and
32 unretired messages per peer. Up to 16 KiB, a cross-host message carries
the tag and payload in one frame, with credit returned when it is consumed.
Each rank has up to 4 MiB of bounded storage for unexpected eager messages.
Larger messages use the offer-and-grant path. Use the same OFI provider
build across ranks for the U4F3 provider protocol. The provider reserves about
8 MiB of transmit and 9 MiB of receive registered memory per cross-host peer and
accepts messages up to 64 MiB. Use MPI_TAG_UB from the running MPI
library for the application tag limit. Features that depend on FI_RMA,
FI_ATOMIC, or FI_HMEM remain on the [roadmap](ROADMAP.md).

If the OFI provider is unavailable, check that fi_info can find
libusb4-fi.so, inspect library dependencies with ldd, and verify the
host-memory and address-vector MCA parameters. For inter-host traffic on
Ethernet, keep the same rank and CPU placement and select the ob1 PML and
TCP BTL. If the hosts filter incoming connections, choose a TCP BTL port
range already permitted between the hosts. Use the `--bench` mode with two
ranks for latency and one-way goodput and `--soak 1800` with four ranks for a
30-minute data check.

See the [qualification record and release criteria](QUALIFICATION.md)
before citing performance or supported topologies.
