# Contributing

On Linux, install Rust 1.99 or later and a C11 compiler. Run `sh scripts/check.sh` from a clean checkout to check all features, the C11 ABI, and IPC examples. [Development guide](docs/DEVELOPMENT.md)

For fixes, include a reproduction. Document any protocol, memory ownership, or C ABI changes. Hardware reports need the revision, kernel, link setup, and test command. Remove private hostnames, addresses, and socket paths before sharing.
