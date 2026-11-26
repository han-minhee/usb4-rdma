#!/bin/sh
# Install the reference host-memory MPI stack under an absolute user-owned prefix.
set -eu
if [ "$#" -ne 1 ]; then
    printf 'Usage: %s /absolute/install/prefix\n' "$0" >&2
    exit 2
fi
case "$1" in
    /*) prefix=$1 ;;
    *)  printf 'The install prefix must be absolute.\n' >&2; exit 2 ;;
esac

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cd "$work"
jobs=${BUILD_JOBS:-$(getconf _NPROCESSORS_ONLN)}
fabric=libfabric-2.4.0
mpi=openmpi-5.0.11

curl --fail --silent --show-error --location --retry 3 \
    "https://github.com/ofiwg/libfabric/releases/download/v2.4.0/$fabric.tar.bz2" -o "$fabric.tar.bz2"
printf '%s  %s\n' \
    0d3036a69c7f7d0e65f4ea9dbd96face3758e74f947842d3925e2b0a9a2ea99f8c2be394a1e532bba268ed9cfcab92d658bc536fe82911542159c64f5c667cc3 \
    "$fabric.tar.bz2" | sha512sum --check -
tar --no-same-owner -xjf "$fabric.tar.bz2"
(
    cd "$fabric"
    ./configure --prefix="$prefix" --libdir="$prefix/lib" --disable-static --disable-verbs --disable-efa
    make -j "$jobs"
    make install
)

curl --fail --silent --show-error --location --retry 3 \
    "https://download.open-mpi.org/release/open-mpi/v5.0/$mpi.tar.bz2" -o "$mpi.tar.bz2"
printf '%s  %s\n' e668a3c4acd50c41dc204c8a6dd98a611e0f26af89cf677577fa9be8a2698003 \
    "$mpi.tar.bz2" | sha256sum --check -
tar --no-same-owner -xjf "$mpi.tar.bz2"
(
    cd "$mpi"
    PKG_CONFIG_PATH="$prefix/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}" \
        ./configure --prefix="$prefix" --libdir="$prefix/lib" --with-libfabric="$prefix" \
        --disable-mpi-fortran --without-cuda --without-rocm --disable-oshmem
    make -j "$jobs"
    make install
)
LD_LIBRARY_PATH="$prefix/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    "$prefix/bin/ompi_info" --param mtl ofi | grep 'MCA mtl: ofi'
