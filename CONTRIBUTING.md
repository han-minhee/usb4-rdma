# Contributing

On Linux, use the pinned Rust toolchain, a C11 compiler, libfabric 2.7, and cbindgen. Run `sh scripts/check.sh` to exercise all features, load the plugins, and run the IPC examples. [Development guide](docs/DEVELOPMENT.md)

For fixes, include a reproduction. Document any protocol, memory ownership, or C ABI changes. Hardware reports need the revision, kernel, link setup, and test command. Remove private hostnames, addresses, and socket paths before sharing.
