/* Fixed-format bridge for the upstream RCCL/NCCL variadic logging callback. */
typedef void (*u4rd_logger)(int, unsigned long, const char *, int, const char *, ...);

void u4rd_rccl_log(u4rd_logger logger, const char *message) {
    if (logger) {
        logger(4, 16, "usb4-rdma", 0, "%s", message);
    }
}
