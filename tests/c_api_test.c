/* SPDX-License-Identifier: MIT */
#define _POSIX_C_SOURCE 200809L
#include "usb4_rdma.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static void sleep_us(long usec) {
    struct timespec ts;
    ts.tv_sec = usec / 1000000L;
    ts.tv_nsec = (usec % 1000000L) * 1000L;
    nanosleep(&ts, NULL);
}

#define CHECK(expr)                                                            \
    do {                                                                       \
        int _rc = (expr);                                                      \
        if (_rc != U4RD_OK) {                                                  \
            fprintf(stderr, "FAIL %s:%d: %s -> %d (%s)\n", __FILE__, __LINE__, \
                    #expr, _rc, u4rd_strerror(_rc));                           \
            exit(1);                                                           \
        }                                                                      \
    } while (0)

typedef struct accept_args {
    u4rd_listener_t *listener;
    u4rd_cq_t *cq;
    u4rd_qp_t *qp;
} accept_args_t;

static void *accept_worker(void *arg) {
    accept_args_t *a = (accept_args_t *)arg;
    CHECK(u4rd_accept(a->listener, a->cq, a->cq, 5000, &a->qp));
    return NULL;
}

static u4rd_wc_t poll_one_blocking(const u4rd_cq_t *cq) {
    u4rd_wc_t wc;
    for (int i = 0; i < 50000; ++i) {
        size_t n = 0;
        CHECK(u4rd_poll_cq(cq, 1, &wc, &n));
        if (n == 1) {
            return wc;
        }
        sleep_us(100);
    }
    fprintf(stderr, "FAIL: poll_one_blocking timed out\n");
    exit(1);
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: %s <node0.sock> <node1.sock>\n", argv[0]);
        return 2;
    }

    u4rd_context_t *ctx0 = NULL, *ctx1 = NULL;
    CHECK(u4rd_open(argv[1], &ctx0));
    CHECK(u4rd_open(argv[2], &ctx1));

    u4rd_device_attr_t attr0;
    CHECK(u4rd_query_device(ctx0, &attr0));
    if (attr0.node_id != 0 || !attr0.supports_one_sided_rdma) {
        fprintf(stderr, "unexpected device attr\n");
        return 1;
    }

    u4rd_pd_t *pd0 = NULL, *pd1 = NULL;
    u4rd_cq_t *cq0 = NULL, *cq1 = NULL;
    CHECK(u4rd_alloc_pd(ctx0, &pd0));
    CHECK(u4rd_alloc_pd(ctx1, &pd1));
    CHECK(u4rd_create_cq(ctx0, &cq0));
    CHECK(u4rd_create_cq(ctx1, &cq1));

    /* Borrowed buffer must reject remote access flags fail-closed. */
    unsigned char stack_buf[64] = {0};
    u4rd_mr_t *bad_mr = NULL;
    if (u4rd_reg_mr(pd0, stack_buf, sizeof(stack_buf),
                    U4RD_ACCESS_LOCAL_WRITE | U4RD_ACCESS_REMOTE_WRITE,
                    &bad_mr) != U4RD_ERR_UNSUPPORTED_ACCESS) {
        fprintf(stderr, "expected U4RD_ERR_UNSUPPORTED_ACCESS for borrowed remote MR\n");
        return 1;
    }

    u4rd_listener_t *listener1 = NULL;
    u4rd_endpoint_handle_t handle1;
    CHECK(u4rd_listen(pd1, &listener1));
    CHECK(u4rd_listener_handle(listener1, &handle1));

    accept_args_t a = {.listener = listener1, .cq = cq1, .qp = NULL};
    pthread_t th;
    if (pthread_create(&th, NULL, accept_worker, &a) != 0) {
        perror("pthread_create");
        return 1;
    }

    u4rd_qp_t *qp0 = NULL;
    CHECK(u4rd_connect(pd0, &handle1, cq0, cq0, &qp0));
    pthread_join(th, NULL);
    u4rd_qp_t *qp1 = a.qp;

    uint32_t all_access = U4RD_ACCESS_LOCAL_READ | U4RD_ACCESS_LOCAL_WRITE |
                          U4RD_ACCESS_REMOTE_READ | U4RD_ACCESS_REMOTE_WRITE;
    u4rd_mr_t *mr0 = NULL, *mr1 = NULL;
    CHECK(u4rd_alloc_mr(pd0, 4096, all_access, &mr0));
    CHECK(u4rd_alloc_mr(pd1, 4096, all_access, &mr1));

    const char *msg = "c11-usb4-rdma-one-sided-test";
    size_t len = strlen(msg);
    memcpy(u4rd_mr_addr(mr0), msg, len);

    /* One-sided RDMA_WRITE */
    CHECK(u4rd_post_rdma_write(qp0, 11, mr0, 0, len, u4rd_mr_rkey(mr1), 64));
    u4rd_wc_t wc = poll_one_blocking(cq0);
    if (wc.status != U4RD_WC_SUCCESS || wc.opcode != U4RD_WC_RDMA_WRITE || wc.wr_id != 11) {
        fprintf(stderr, "RDMA_WRITE failed: status=%u opcode=%u\n", wc.status, wc.opcode);
        return 1;
    }
    if (memcmp((const char *)u4rd_mr_addr(mr1) + 64, msg, len) != 0) {
        fprintf(stderr, "RDMA_WRITE data mismatch\n");
        return 1;
    }

    /* One-sided RDMA_READ into borrowed local buffer */
    u4rd_mr_t *local_mr = NULL;
    CHECK(u4rd_reg_mr(pd0, stack_buf, sizeof(stack_buf),
                      U4RD_ACCESS_LOCAL_READ | U4RD_ACCESS_LOCAL_WRITE, &local_mr));
    CHECK(u4rd_post_rdma_read(qp0, 12, local_mr, 0, len, u4rd_mr_rkey(mr1), 64));
    wc = poll_one_blocking(cq0);
    if (wc.status != U4RD_WC_SUCCESS || wc.opcode != U4RD_WC_RDMA_READ || wc.wr_id != 12) {
        fprintf(stderr, "RDMA_READ failed: status=%u opcode=%u\n", wc.status, wc.opcode);
        return 1;
    }
    if (memcmp(stack_buf, msg, len) != 0) {
        fprintf(stderr, "RDMA_READ data mismatch\n");
        return 1;
    }

    /* Two-sided SEND/RECV (poll both CQs in this single-threaded test) */
    CHECK(u4rd_post_recv(qp1, 21, mr1, 512, len, 99));
    CHECK(u4rd_post_send(qp0, 22, mr0, 0, len, 99));
    int send_done = 0, recv_done = 0;
    for (int i = 0; i < 50000 && !(send_done && recv_done); ++i) {
        size_t n = 0;
        if (!send_done) {
            CHECK(u4rd_poll_cq(cq0, 1, &wc, &n));
            if (n == 1) {
                if (wc.status != U4RD_WC_SUCCESS || wc.opcode != U4RD_WC_SEND) {
                    fprintf(stderr, "SEND failed: status=%u\n", wc.status);
                    return 1;
                }
                send_done = 1;
            }
        }
        if (!recv_done) {
            CHECK(u4rd_poll_cq(cq1, 1, &wc, &n));
            if (n == 1) {
                if (wc.status != U4RD_WC_SUCCESS || wc.opcode != U4RD_WC_RECV) {
                    fprintf(stderr, "RECV failed: status=%u\n", wc.status);
                    return 1;
                }
                recv_done = 1;
            }
        }
        sleep_us(100);
    }
    if (!send_done || !recv_done) {
        fprintf(stderr, "SEND/RECV timed out\n");
        return 1;
    }
    if (memcmp((const char *)u4rd_mr_addr(mr1) + 512, msg, len) != 0) {
        fprintf(stderr, "SEND/RECV data mismatch\n");
        return 1;
    }

    u4rd_dereg_mr(local_mr);
    u4rd_dereg_mr(mr0);
    u4rd_dereg_mr(mr1);
    u4rd_destroy_qp(qp0);
    u4rd_destroy_qp(qp1);
    u4rd_close_listener(listener1);
    u4rd_destroy_cq(cq0);
    u4rd_destroy_cq(cq1);
    u4rd_dealloc_pd(pd0);
    u4rd_dealloc_pd(pd1);
    u4rd_close(ctx0);
    u4rd_close(ctx1);

    printf("c_api_test OK (RDMA_WRITE, RDMA_READ, SEND/RECV)\n");
    return 0;
}
