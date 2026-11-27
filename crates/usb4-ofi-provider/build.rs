fn main() {
    println!("cargo:rerun-if-changed=src/provider.c");
    if std::env::var_os("CARGO_FEATURE_NATIVE").is_none() {
        return;
    }
    let fabric = pkg_config::Config::new()
        .atleast_version("2.7.0")
        .probe("libfabric")
        .expect("native OFI requires libfabric 2.7 development headers");
    cc::Build::new()
        .file("src/provider.c")
        .includes(fabric.include_paths)
        .define("USB4_VERSION_MAJOR", env!("CARGO_PKG_VERSION_MAJOR"))
        .define("USB4_VERSION_MINOR", env!("CARGO_PKG_VERSION_MINOR"))
        .flag("-std=c11")
        .warnings_into_errors(true)
        .compile("usb4_ofi_shim");
}
