fn main() {
    cc::Build::new()
        .file("src/logger.c")
        .compile("u4rd_rccl_logger");
    println!("cargo:rerun-if-changed=src/logger.c");
}
