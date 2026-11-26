/* SPDX-License-Identifier: MIT */
#ifndef USB4_RDMA_H
#define USB4_RDMA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Status / error codes */
#define U4RD_OK                     0
#define U4RD_ERR_INVALID_ARG       -1
#define U4RD_ERR_UNSUPPORTED_ACCESS -2
#define U4RD_ERR_ACCESS_VIOLATION  -3
#define U4RD_ERR_EXHAUSTED         -4
#define U4RD_ERR_DISCONNECTED      -5
#define U4RD_ERR_TIMEOUT           -6
#define U4RD_ERR_PROTOCOL          -7
#define U4RD_ERR_IO                -8
#define U4RD_ERR_INTERNAL          -9

/* Memory access flags */
#define U4RD_ACCESS_LOCAL_READ    (1u << 0)
#define U4RD_ACCESS_LOCAL_WRITE   (1u << 1)
#define U4RD_ACCESS_REMOTE_READ   (1u << 2)
#define U4RD_ACCESS_REMOTE_WRITE  (1u << 3)

/* Work completion opcodes */
#define U4RD_WC_SEND              1u
#define U4RD_WC_RECV              2u
#define U4RD_WC_RDMA_WRITE        3u
#define U4RD_WC_RDMA_READ         4u
#define U4RD_WC_RECV_RDMA_WITH_IMM 5u

/* Work completion statuses */
#define U4RD_WC_SUCCESS            0u
#define U4RD_WC_LOCAL_LEN_ERR      1u
#define U4RD_WC_REMOTE_ACCESS_ERR  2u
#define U4RD_WC_REMOTE_INVALID_REQ 3u
#define U4RD_WC_TRANSPORT_ERR      4u
#define U4RD_WC_CONNECTION_CLOSED  5u
#define U4RD_WC_GENERAL_ERR        6u

typedef struct u4rd_context  u4rd_context_t;
typedef struct u4rd_pd       u4rd_pd_t;
typedef struct u4rd_cq       u4rd_cq_t;
typedef struct u4rd_mr       u4rd_mr_t;
typedef struct u4rd_listener u4rd_listener_t;
typedef struct u4rd_qp       u4rd_qp_t;

typedef struct u4rd_device_attr {
    uint32_t node_id;
    uint32_t max_window_bytes;
    uint32_t max_chunk_bytes;
    uint32_t max_mr_count;
    uint64_t epoch;
    uint8_t  topology_digest[32];
    uint8_t  supports_one_sided_rdma;
    uint8_t  zero_copy_dma;
    uint8_t  reserved[6];
} u4rd_device_attr_t;

typedef struct u4rd_endpoint_handle {
    uint8_t bytes[20];
} u4rd_endpoint_handle_t;

typedef struct u4rd_wc {
    uint64_t wr_id;
    uint64_t byte_len;
    uint64_t qp_id;
    uint32_t status;
    uint32_t opcode;
    uint32_t tag_or_imm;
    uint8_t  has_imm;
    uint8_t  reserved[3];
} u4rd_wc_t;

const char *u4rd_strerror(int code);

int  u4rd_open(const char *socket_path, u4rd_context_t **out_ctx);
void u4rd_close(u4rd_context_t *ctx);
int  u4rd_query_device(const u4rd_context_t *ctx, u4rd_device_attr_t *out_attr);

int  u4rd_alloc_pd(const u4rd_context_t *ctx, u4rd_pd_t **out_pd);
void u4rd_dealloc_pd(u4rd_pd_t *pd);

int  u4rd_create_cq(const u4rd_context_t *ctx, u4rd_cq_t **out_cq);
void u4rd_destroy_cq(u4rd_cq_t *cq);
int  u4rd_poll_cq(const u4rd_cq_t *cq, size_t max_entries, u4rd_wc_t *out_wcs, size_t *out_count);

/* Allocates a daemon-shared MR supporting LOCAL_* and REMOTE_* access flags. */
int      u4rd_alloc_mr(const u4rd_pd_t *pd, size_t len, uint32_t access_flags, u4rd_mr_t **out_mr);
/* Registers a caller-owned host buffer for LOCAL_* access only (fails closed on REMOTE_*). */
int      u4rd_reg_mr(const u4rd_pd_t *pd, void *addr, size_t len, uint32_t access_flags, u4rd_mr_t **out_mr);
void     u4rd_dereg_mr(u4rd_mr_t *mr);
void    *u4rd_mr_addr(const u4rd_mr_t *mr);
size_t   u4rd_mr_len(const u4rd_mr_t *mr);
uint64_t u4rd_mr_lkey(const u4rd_mr_t *mr);
uint64_t u4rd_mr_rkey(const u4rd_mr_t *mr);

int  u4rd_listen(const u4rd_pd_t *pd, u4rd_listener_t **out_listener);
int  u4rd_listener_handle(const u4rd_listener_t *listener, u4rd_endpoint_handle_t *out_handle);
int  u4rd_accept(const u4rd_listener_t *listener, const u4rd_cq_t *send_cq,
                 const u4rd_cq_t *recv_cq, uint64_t timeout_ms, u4rd_qp_t **out_qp);
void u4rd_close_listener(u4rd_listener_t *listener);

int  u4rd_connect(const u4rd_pd_t *pd, const u4rd_endpoint_handle_t *remote_handle,
                  const u4rd_cq_t *send_cq, const u4rd_cq_t *recv_cq, u4rd_qp_t **out_qp);
void u4rd_destroy_qp(u4rd_qp_t *qp);

int u4rd_post_send(const u4rd_qp_t *qp, uint64_t wr_id, const u4rd_mr_t *mr,
                   size_t offset, size_t len, uint32_t tag);
int u4rd_post_recv(const u4rd_qp_t *qp, uint64_t wr_id, const u4rd_mr_t *mr,
                   size_t offset, size_t len, uint32_t tag);
int u4rd_peek_recv(const u4rd_qp_t *qp, uint32_t tag, int *out_ready);

int u4rd_post_rdma_write(const u4rd_qp_t *qp, uint64_t wr_id, const u4rd_mr_t *mr,
                         size_t offset, size_t len, uint64_t rkey, uint64_t remote_offset);
int u4rd_post_rdma_write_imm(const u4rd_qp_t *qp, uint64_t wr_id, const u4rd_mr_t *mr,
                             size_t offset, size_t len, uint64_t rkey, uint64_t remote_offset,
                             uint32_t imm_data);
int u4rd_post_rdma_read(const u4rd_qp_t *qp, uint64_t wr_id, const u4rd_mr_t *mr,
                        size_t offset, size_t len, uint64_t rkey, uint64_t remote_offset);

#ifdef __cplusplus
}
#endif

#endif /* USB4_RDMA_H */
