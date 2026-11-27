/* SPDX-License-Identifier: MIT */
#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>

struct node {
    struct fi_info *info;
    struct fid_fabric *fabric;
    struct fid_domain *domain;
    struct fid_ep *ep;
    struct fid_av *av;
    struct fid_cq *cq;
    struct fid_mr *mr;
    fi_addr_t peer;
    char address[40];
};

static void check(int result)
{
    if (result) {
        fprintf(stderr, "libfabric error %d: %s\n", result, fi_strerror(-result));
        abort();
    }
}

static struct node initialize(const char *socket, void *target, size_t len)
{
    assert(setenv("USB4_RDMA_SOCKET", socket, 1) == 0);
    struct node n = {0};
    struct fi_info *hints = fi_allocinfo();
    assert(hints);
    hints->caps = FI_RMA | FI_MSG;
    hints->ep_attr->type = FI_EP_RDM;
    hints->fabric_attr->prov_name = strdup("usb4");
    assert(hints->fabric_attr->prov_name);
    check(fi_getinfo(FI_VERSION(2, 7), NULL, NULL, 0, hints, &n.info));
    fi_freeinfo(hints);
    assert(n.info->domain_attr->mr_mode & FI_MR_PROV_KEY);
    check(fi_fabric(n.info->fabric_attr, &n.fabric, NULL));
    check(fi_domain(n.fabric, n.info, &n.domain, NULL));
    check(fi_mr_reg(n.domain, target, len, FI_REMOTE_READ | FI_REMOTE_WRITE, 0, 0, 0, &n.mr, NULL));
    struct fi_av_attr av = {.type = FI_AV_TABLE};
    struct fi_cq_attr cq = {.format = FI_CQ_FORMAT_MSG, .wait_obj = FI_WAIT_NONE};
    check(fi_av_open(n.domain, &av, &n.av, NULL));
    check(fi_cq_open(n.domain, &cq, &n.cq, NULL));
    check(fi_endpoint(n.domain, n.info, &n.ep, NULL));
    check(fi_ep_bind(n.ep, &n.av->fid, 0));
    check(fi_ep_bind(n.ep, &n.cq->fid, FI_TRANSMIT | FI_RECV));
    check(fi_enable(n.ep));
    size_t size = sizeof(n.address);
    check(fi_getname(&n.ep->fid, n.address, &size));
    assert(size == sizeof(n.address));
    return n;
}

static void retire(struct node *n)
{
    check(fi_close(&n->mr->fid));
    check(fi_close(&n->ep->fid));
    check(fi_close(&n->cq->fid));
    check(fi_close(&n->av->fid));
    check(fi_close(&n->domain->fid));
    check(fi_close(&n->fabric->fid));
    fi_freeinfo(n->info);
}

static bool progress(struct node *n, void *context, uint64_t flags, int error)
{
    struct fi_cq_msg_entry completion;
    ssize_t result = fi_cq_read(n->cq, &completion, context ? 1 : 0);
    if (result == -FI_EAGAIN || (result == 0 && !context))
        return false;
    assert(context);
    if (result == -FI_EAVAIL) {
        struct fi_cq_err_entry entry = {0};
        assert(fi_cq_readerr(n->cq, &entry, 0) == 1);
        assert(error != 0 && entry.err == error && entry.op_context == context);
        return true;
    }
    assert(result == 1 && error == 0 && completion.op_context == context);
    assert((completion.flags & flags) == flags);
    return true;
}

static void wait_for(struct node *a, struct node *b, void *context, uint64_t flags, int error)
{
    struct timespec pause = {.tv_nsec = 100000};
    for (unsigned attempt = 0; attempt < 100000; ++attempt) {
        if (progress(a, context, flags, error))
            return;
        progress(b, NULL, 0, 0);
        nanosleep(&pause, NULL);
    }
    abort();
}

int main(int argc, char **argv)
{
    assert(argc == 3);
    enum { SIZE = 96 * 1024 };
    unsigned char source[SIZE], target[SIZE], readback[SIZE];
    for (size_t i = 0; i < SIZE; ++i)
        source[i] = (unsigned char)(31 * i + (i >> 8));
    memset(target, 0, sizeof(target));
    struct node a = initialize(argv[1], readback, sizeof(readback));
    struct node b = initialize(argv[2], target, sizeof(target));
    assert(fi_av_insert(a.av, b.address, 1, &a.peer, 0, NULL) == 1);
    assert(fi_av_insert(b.av, a.address, 1, &b.peer, 0, NULL) == 1);
    uint64_t key = fi_mr_key(b.mr);
    int write, read, invalid;
    check((int)fi_write(a.ep, source, SIZE, NULL, a.peer, 0, key, &write));
    wait_for(&a, &b, &write, FI_RMA | FI_WRITE, 0);
    assert(memcmp(source, target, SIZE) == 0);
    check((int)fi_read(a.ep, readback, SIZE, NULL, a.peer, 0, key, &read));
    wait_for(&a, &b, &read, FI_RMA | FI_READ, 0);
    assert(memcmp(source, readback, SIZE) == 0);
    check((int)fi_write(a.ep, source, 8, NULL, a.peer, SIZE - 4, key, &invalid));
    wait_for(&a, &b, &invalid, 0, FI_EACCES);
    assert(memcmp(source, target, SIZE) == 0);
    retire(&b);
    retire(&a);
    puts("OFI host RMA write, read, and remote bounds passed");
}
