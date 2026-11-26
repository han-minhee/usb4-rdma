/* SPDX-License-Identifier: MIT */
#define _POSIX_C_SOURCE 200809L
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_tagged.h>
#include <rdma/providers/fi_prov.h>

enum { ADDRESS_BYTES = 40, INJECT_BYTES = 256, MAX_MSG = 64 * 1024 * 1024 };
enum { TAGGED = 1, COMPLETE = 2, INJECT = 4, PEEK = 8, CLAIM = 16, DISCARD = 32 };

struct operation {
    uint64_t addr, tag, ignore;
    uintptr_t context, data;
    size_t len;
    uint32_t flags;
};

struct completion {
    uintptr_t context;
    uint64_t address, tag;
    size_t len, overflow;
    uint32_t kind;
    int32_t status;
};

void *u4off_open(const char *);
void u4off_close(void *);
int32_t u4off_enable(void *);
int32_t u4off_name(void *, void *);
int32_t u4off_insert(void *, const void *, uint64_t *);
int32_t u4off_lookup(void *, uint64_t, void *);
int32_t u4off_remove(void *, uint64_t);
int32_t u4off_submit(void *, const struct operation *, bool);
int32_t u4off_cancel(void *, uintptr_t);
int32_t u4off_next(void *, struct completion *, bool, bool);

struct fabric {
    struct fid_fabric fid;
    unsigned children;
};
struct domain {
    struct fid_domain fid;
    struct fabric *fabric;
    struct endpoint *endpoint;
    unsigned children;
};
struct av {
    struct fid_av fid;
    struct domain *domain;
    bool bound;
};
struct cq {
    struct fid_cq fid;
    struct domain *domain;
    bool bound;
    enum fi_cq_format format;
};
struct endpoint {
    struct fid_ep fid;
    struct domain *domain;
    struct av *av;
    struct cq *cq;
    void *engine;
    bool selective;
    bool enabled;
    uint64_t tx_flags, rx_flags;
};

static int status(int value)
{
    switch (value) {
    case 0: return 0;
    case -1: return -FI_EAGAIN;
    case -2: return -FI_EINVAL;
    case -4: return -FI_ENOMSG;
    case -5: return -FI_ECANCELED;
    case -6: return -FI_ETRUNC;
    default: return -FI_EIO;
    }
}

static int close_fid(struct fid *fid)
{
    switch (fid->fclass) {
    case FI_CLASS_FABRIC: {
        struct fabric *object = (struct fabric *)fid;
        if (object->children)
            return -FI_EBUSY;
        break;
    }
    case FI_CLASS_DOMAIN: {
        struct domain *object = (struct domain *)fid;
        if (object->children)
            return -FI_EBUSY;
        --object->fabric->children;
        break;
    }
    case FI_CLASS_EP: {
        struct endpoint *ep = (struct endpoint *)fid;
        if (ep->av)
            ep->av->bound = false;
        if (ep->cq)
            ep->cq->bound = false;
        u4off_close(ep->engine);
        ep->domain->endpoint = NULL;
        --ep->domain->children;
        break;
    }
    case FI_CLASS_AV: {
        struct av *object = (struct av *)fid;
        if (object->bound)
            return -FI_EBUSY;
        --object->domain->children;
        break;
    }
    case FI_CLASS_CQ: {
        struct cq *object = (struct cq *)fid;
        if (object->bound)
            return -FI_EBUSY;
        --object->domain->children;
        break;
    }
    default:
        return -FI_EINVAL;
    }
    free(fid);
    return 0;
}

static int bind_fid(struct fid *fid, struct fid *target, uint64_t flags)
{
    if (fid->fclass != FI_CLASS_EP || !target)
        return -FI_EINVAL;
    struct endpoint *ep = (struct endpoint *)fid;
    if (ep->enabled)
        return -FI_EOPBADSTATE;
    if (target->fclass == FI_CLASS_AV && !ep->av && flags == 0) {
        struct av *av = (struct av *)target;
        if (av->domain != ep->domain || av->bound)
            return -FI_EINVAL;
        ep->av = av;
        av->bound = true;
        return 0;
    }
    if (target->fclass == FI_CLASS_CQ && !ep->cq &&
        (flags & (FI_TRANSMIT | FI_RECV)) == (FI_TRANSMIT | FI_RECV) &&
        !(flags & ~(FI_TRANSMIT | FI_RECV | FI_SELECTIVE_COMPLETION))) {
        struct cq *cq = (struct cq *)target;
        if (cq->domain != ep->domain || cq->bound)
            return -FI_EINVAL;
        ep->cq = cq;
        cq->bound = true;
        ep->selective = (flags & FI_SELECTIVE_COMPLETION) != 0;
        return 0;
    }
    return -FI_EINVAL;
}

static int control_fid(struct fid *fid, int command, void *argument)
{
    (void)argument;
    if (fid->fclass != FI_CLASS_EP || command != FI_ENABLE)
        return -FI_ENOSYS;
    struct endpoint *ep = (struct endpoint *)fid;
    if (!ep->av || !ep->cq)
        return -FI_EOPBADSTATE;
    int result = status(u4off_enable(ep->engine));
    if (!result)
        ep->enabled = true;
    return result;
}

static int open_ops(struct fid *fid, const char *name, uint64_t flags,
                    void **out, void *context)
{
    (void)fid; (void)name; (void)flags; (void)out; (void)context;
    return -FI_ENOSYS;
}

static struct fi_ops fid_ops = {
    .size = sizeof(fid_ops), .close = close_fid, .bind = bind_fid,
    .control = control_fid, .ops_open = open_ops,
};

static int write_address(const uint8_t *source, void *dest, size_t *size)
{
    if (!size)
        return -FI_EINVAL;
    size_t capacity = *size;
    *size = ADDRESS_BYTES;
    if (!dest || capacity < ADDRESS_BYTES)
        return -FI_ETOOSMALL;
    memcpy(dest, source, ADDRESS_BYTES);
    return 0;
}

static int getname(fid_t fid, void *addr, size_t *len)
{
    if (fid->fclass != FI_CLASS_EP)
        return -FI_EINVAL;
    uint8_t address[ADDRESS_BYTES];
    int result = status(u4off_name(((struct endpoint *)fid)->engine, address));
    return result ? result : write_address(address, addr, len);
}

static struct fi_ops_cm cm_ops = { .size = sizeof(cm_ops), .getname = getname };

static int insert_addr(struct fid_av *fid, const void *address, size_t count,
                       fi_addr_t *out, uint64_t flags, void *context)
{
    (void)context;
    struct endpoint *ep = ((struct av *)fid)->domain->endpoint;
    if (!ep || !address || !out || flags)
        return -FI_EINVAL;
    const uint8_t *addresses = address;
    size_t inserted = 0;
    for (size_t i = 0; i < count; ++i) {
        out[i] = FI_ADDR_NOTAVAIL;
        if (!u4off_insert(ep->engine, addresses + i * ADDRESS_BYTES, &out[i]))
            ++inserted;
    }
    return (int)inserted;
}

static int remove_addr(struct fid_av *fid, fi_addr_t *addresses, size_t count, uint64_t flags)
{
    struct endpoint *ep = ((struct av *)fid)->domain->endpoint;
    if (!addresses || flags)
        return -FI_EINVAL;
    /* MPI may release its AV entries after it closes the endpoint. */
    if (!ep)
        return 0;
    for (size_t i = 0; i < count; ++i) {
        int result = status(u4off_remove(ep->engine, addresses[i]));
        if (result)
            return result;
    }
    return 0;
}

static int lookup_addr(struct fid_av *fid, fi_addr_t addr, void *out, size_t *size)
{
    struct endpoint *ep = ((struct av *)fid)->domain->endpoint;
    if (!ep)
        return -FI_EINVAL;
    uint8_t bytes[ADDRESS_BYTES];
    int result = status(u4off_lookup(ep->engine, addr, bytes));
    return result ? result : write_address(bytes, out, size);
}

static const char *print_addr(struct fid_av *fid, const void *addr, char *buf, size_t *len)
{
    (void)fid;
    const char digits[] = "0123456789abcdef";
    const uint8_t *bytes = addr;
    if (!len)
        return NULL;
    size_t capacity = *len;
    *len = 2 * ADDRESS_BYTES + 1;
    if (!buf || !addr || capacity < *len)
        return NULL;
    for (size_t i = 0; i < ADDRESS_BYTES; ++i) {
        buf[2 * i] = digits[bytes[i] >> 4];
        buf[2 * i + 1] = digits[bytes[i] & 15];
    }
    buf[*len - 1] = '\0';
    return buf;
}

static struct fi_ops_av av_ops = {
    .size = sizeof(av_ops), .insert = insert_addr, .remove = remove_addr,
    .lookup = lookup_addr, .straddr = print_addr,
};

static uint64_t cq_flags(const struct completion *result)
{
    uint64_t type = result->kind >= 3 ? FI_TAGGED : FI_MSG;
    return type | (result->kind % 2 ? FI_RECV : FI_SEND);
}

static ssize_t readfrom_cq(struct fid_cq *fid, void *buf, size_t count, fi_addr_t *source)
{
    struct cq *cq = (struct cq *)fid;
    struct endpoint *ep = cq->domain->endpoint;
    if (!ep)
        return -FI_EAGAIN;
    struct completion result;
    size_t i = 0;
    /* A zero-count read advances network progress without consuming entries. */
    do {
        int code = status(u4off_next(ep->engine, &result, false, i == 0));
        if (code)
            return i ? (ssize_t)i : code;
        if (result.status)
            return i ? (ssize_t)i : -FI_EAVAIL;
        if (count == 0)
            return 0;
        if (!buf)
            return -FI_EINVAL;
        code = status(u4off_next(ep->engine, &result, true, false));
        if (code)
            return i ? (ssize_t)i : code;
        switch (cq->format) {
        case FI_CQ_FORMAT_CONTEXT:
            ((struct fi_cq_entry *)buf)[i].op_context = (void *)result.context;
            break;
        case FI_CQ_FORMAT_MSG:
            ((struct fi_cq_msg_entry *)buf)[i] = (struct fi_cq_msg_entry){
                .op_context = (void *)result.context, .flags = cq_flags(&result), .len = result.len,
            };
            break;
        default:
            ((struct fi_cq_tagged_entry *)buf)[i] = (struct fi_cq_tagged_entry){
                .op_context = (void *)result.context, .flags = cq_flags(&result),
                .len = result.len, .tag = result.tag,
            };
            break;
        }
        if (source)
            source[i] = result.address;
        ++i;
    } while (i < count);
    return (ssize_t)i;
}

static ssize_t read_cq(struct fid_cq *fid, void *buf, size_t count)
{
    return readfrom_cq(fid, buf, count, NULL);
}

static ssize_t readerr_cq(struct fid_cq *fid, struct fi_cq_err_entry *entry, uint64_t flags)
{
    struct endpoint *ep = ((struct cq *)fid)->domain->endpoint;
    if (!entry || flags)
        return -FI_EINVAL;
    if (!ep)
        return -FI_EAGAIN;
    struct completion result;
    int code = status(u4off_next(ep->engine, &result, false, true));
    if (code)
        return code;
    if (!result.status)
        return -FI_EAGAIN;
    code = status(u4off_next(ep->engine, &result, true, false));
    if (code)
        return code;
    *entry = (struct fi_cq_err_entry){
        .op_context = (void *)result.context, .flags = cq_flags(&result),
        .len = result.len, .olen = result.overflow, .tag = result.tag,
        .err = -status(-result.status), .prov_errno = -status(-result.status),
    };
    return 1;
}

static const char *strerror_cq(struct fid_cq *fid, int err, const void *data, char *buf, size_t len)
{
    (void)fid; (void)data; (void)buf; (void)len;
    return fi_strerror(err);
}

static struct fi_ops_cq cq_ops = {
    .size = sizeof(cq_ops), .read = read_cq, .readfrom = readfrom_cq,
    .readerr = readerr_cq, .strerror = strerror_cq,
};

static ssize_t submit(struct fid_ep *fid, const void *buf, size_t len,
                      fi_addr_t addr, uint64_t tag, uint64_t ignore, void *context,
                      uint64_t flags, bool tagged, bool send)
{
    struct endpoint *ep = (struct endpoint *)fid;
    if (!ep->enabled)
        return -FI_EOPBADSTATE;
    const uint64_t allowed = FI_COMPLETION | (send ? FI_INJECT | FI_TRANSMIT_COMPLETE
                                            : FI_PEEK | FI_CLAIM | FI_DISCARD);
    if ((flags & ~allowed) || (!tagged && !send && (flags & (FI_PEEK | FI_CLAIM | FI_DISCARD))))
        return -FI_EOPNOTSUPP;
    struct operation op = {
        .addr = addr, .tag = tag, .ignore = ignore, .context = (uintptr_t)context,
        .data = (uintptr_t)buf, .len = len,
        .flags = (tagged ? TAGGED : 0) | ((!ep->selective || flags & FI_COMPLETION) ? COMPLETE : 0) |
                 (flags & FI_INJECT ? INJECT : 0) | (flags & FI_PEEK ? PEEK : 0) |
                 (flags & FI_CLAIM ? CLAIM : 0) | (flags & FI_DISCARD ? DISCARD : 0),
    };
    return status(u4off_submit(ep->engine, &op, send));
}

static ssize_t submit_iov(struct fid_ep *ep, const struct iovec *iov, size_t count,
                          fi_addr_t addr, uint64_t tag, uint64_t ignore, void *context,
                          uint64_t flags, bool tagged, bool send)
{
    if (count > 1 || (count && !iov))
        return -FI_EINVAL;
    return submit(ep, count ? iov->iov_base : NULL, count ? iov->iov_len : 0,
                  addr, tag, ignore, context, flags, tagged, send);
}

static ssize_t tsend(struct fid_ep *ep, const void *buf, size_t len, void *desc,
                     fi_addr_t addr, uint64_t tag, void *context)
{
    (void)desc;
    return submit(ep, buf, len, addr, tag, 0, context, ((struct endpoint *)ep)->tx_flags, true, true);
}

static ssize_t trecv(struct fid_ep *ep, void *buf, size_t len, void *desc,
                     fi_addr_t addr, uint64_t tag, uint64_t ignore, void *context)
{
    (void)desc;
    return submit(ep, buf, len, addr, tag, ignore, context, ((struct endpoint *)ep)->rx_flags, true, false);
}

static ssize_t tsendv(struct fid_ep *ep, const struct iovec *iov, void **desc, size_t count,
                      fi_addr_t addr, uint64_t tag, void *context)
{
    (void)desc;
    return submit_iov(ep, iov, count, addr, tag, 0, context, ((struct endpoint *)ep)->tx_flags, true, true);
}

static ssize_t trecvv(struct fid_ep *ep, const struct iovec *iov, void **desc, size_t count,
                      fi_addr_t addr, uint64_t tag, uint64_t ignore, void *context)
{
    (void)desc;
    return submit_iov(ep, iov, count, addr, tag, ignore, context, ((struct endpoint *)ep)->rx_flags, true, false);
}

static ssize_t tsendmsg(struct fid_ep *ep, const struct fi_msg_tagged *msg, uint64_t flags)
{
    return submit_iov(ep, msg->msg_iov, msg->iov_count, msg->addr, msg->tag,
                      msg->ignore, msg->context, flags, true, true);
}

static ssize_t trecvmsg(struct fid_ep *ep, const struct fi_msg_tagged *msg, uint64_t flags)
{
    return submit_iov(ep, msg->msg_iov, msg->iov_count, msg->addr, msg->tag,
                      msg->ignore, msg->context, flags, true, false);
}

static ssize_t tinject(struct fid_ep *ep, const void *buf, size_t len, fi_addr_t addr, uint64_t tag)
{
    return submit(ep, buf, len, addr, tag, 0, NULL, FI_INJECT, true, true);
}

static struct fi_ops_tagged tagged_ops = {
    .size = sizeof(tagged_ops), .send = tsend, .sendv = tsendv, .sendmsg = tsendmsg,
    .recv = trecv, .recvv = trecvv, .recvmsg = trecvmsg, .inject = tinject,
};

static ssize_t msg_send(struct fid_ep *ep, const void *buf, size_t len, void *desc,
                        fi_addr_t addr, void *context)
{
    (void)desc;
    return submit(ep, buf, len, addr, 0, 0, context, ((struct endpoint *)ep)->tx_flags, false, true);
}

static ssize_t msg_recv(struct fid_ep *ep, void *buf, size_t len, void *desc,
                        fi_addr_t addr, void *context)
{
    (void)desc;
    return submit(ep, buf, len, addr, 0, 0, context, ((struct endpoint *)ep)->rx_flags, false, false);
}

static ssize_t msg_sendv(struct fid_ep *ep, const struct iovec *iov, void **desc, size_t count,
                         fi_addr_t addr, void *context)
{
    (void)desc;
    return submit_iov(ep, iov, count, addr, 0, 0, context, ((struct endpoint *)ep)->tx_flags, false, true);
}

static ssize_t msg_recvv(struct fid_ep *ep, const struct iovec *iov, void **desc, size_t count,
                         fi_addr_t addr, void *context)
{
    (void)desc;
    return submit_iov(ep, iov, count, addr, 0, 0, context, ((struct endpoint *)ep)->rx_flags, false, false);
}

static ssize_t msg_sendmsg(struct fid_ep *ep, const struct fi_msg *msg, uint64_t flags)
{
    return submit_iov(ep, msg->msg_iov, msg->iov_count, msg->addr, 0, 0, msg->context, flags, false, true);
}

static ssize_t msg_recvmsg(struct fid_ep *ep, const struct fi_msg *msg, uint64_t flags)
{
    return submit_iov(ep, msg->msg_iov, msg->iov_count, msg->addr, 0, 0, msg->context, flags, false, false);
}

static ssize_t msg_inject(struct fid_ep *ep, const void *buf, size_t len, fi_addr_t addr)
{
    return submit(ep, buf, len, addr, 0, 0, NULL, FI_INJECT, false, true);
}

static struct fi_ops_msg msg_ops = {
    .size = sizeof(msg_ops), .send = msg_send, .sendv = msg_sendv, .sendmsg = msg_sendmsg,
    .recv = msg_recv, .recvv = msg_recvv, .recvmsg = msg_recvmsg, .inject = msg_inject,
};

static ssize_t cancel_recv(fid_t fid, void *context)
{
    if (fid->fclass != FI_CLASS_EP)
        return -FI_EINVAL;
    return status(u4off_cancel(((struct endpoint *)fid)->engine, (uintptr_t)context));
}

static int ep_getopt(fid_t fid, int level, int name, void *value, size_t *len)
{
    (void)fid; (void)level; (void)name; (void)value; (void)len;
    return -FI_ENOPROTOOPT;
}

static int ep_setopt(fid_t fid, int level, int name, const void *value, size_t len)
{
    (void)fid; (void)level; (void)name; (void)value; (void)len;
    return -FI_ENOPROTOOPT;
}

static struct fi_ops_ep ep_ops = {
    .size = sizeof(ep_ops), .cancel = cancel_recv, .getopt = ep_getopt, .setopt = ep_setopt,
};

static int open_ep(struct fid_domain *fid, struct fi_info *info, struct fid_ep **out, void *context)
{
    struct domain *domain = (struct domain *)fid;
    if (domain->endpoint || !info || !out)
        return -FI_EINVAL;
    const char *socket = getenv("USB4_RDMA_SOCKET");
    if (!socket || !*socket)
        return -FI_ENODEV;
    struct endpoint *ep = calloc(1, sizeof(*ep));
    if (!ep)
        return -FI_ENOMEM;
    ep->engine = u4off_open(socket);
    if (!ep->engine) {
        free(ep);
        return -FI_ENODEV;
    }
    ep->fid.fid = (struct fid){ .fclass = FI_CLASS_EP, .context = context, .ops = &fid_ops };
    ep->fid.ops = &ep_ops;
    ep->fid.cm = &cm_ops;
    ep->fid.tagged = &tagged_ops;
    ep->fid.msg = &msg_ops;
    ep->domain = domain;
    ep->tx_flags = info->tx_attr ? info->tx_attr->op_flags : FI_COMPLETION;
    ep->rx_flags = info->rx_attr ? info->rx_attr->op_flags : FI_COMPLETION;
    domain->endpoint = ep;
    ++domain->children;
    *out = &ep->fid;
    return 0;
}

static int open_av(struct fid_domain *fid, struct fi_av_attr *attr, struct fid_av **out, void *context)
{
    if (!attr || !out || (attr->type != FI_AV_UNSPEC && attr->type != FI_AV_TABLE) ||
        attr->flags || attr->rx_ctx_bits)
        return -FI_EINVAL;
    struct av *av = calloc(1, sizeof(*av));
    if (!av)
        return -FI_ENOMEM;
    av->fid.fid = (struct fid){ .fclass = FI_CLASS_AV, .context = context, .ops = &fid_ops };
    av->fid.ops = &av_ops;
    av->domain = (struct domain *)fid;
    ++av->domain->children;
    *out = &av->fid;
    return 0;
}

static int open_cq(struct fid_domain *fid, struct fi_cq_attr *attr, struct fid_cq **out, void *context)
{
    if (!attr || !out || attr->flags || (attr->wait_obj != FI_WAIT_NONE && attr->wait_obj != FI_WAIT_UNSPEC) ||
        (attr->format != FI_CQ_FORMAT_UNSPEC && attr->format != FI_CQ_FORMAT_TAGGED &&
         attr->format != FI_CQ_FORMAT_CONTEXT && attr->format != FI_CQ_FORMAT_MSG))
        return -FI_EINVAL;
    struct cq *cq = calloc(1, sizeof(*cq));
    if (!cq)
        return -FI_ENOMEM;
    cq->fid.fid = (struct fid){ .fclass = FI_CLASS_CQ, .context = context, .ops = &fid_ops };
    cq->fid.ops = &cq_ops;
    cq->format = attr->format;
    cq->domain = (struct domain *)fid;
    ++cq->domain->children;
    *out = &cq->fid;
    return 0;
}

static struct fi_ops_domain domain_ops = {
    .size = sizeof(domain_ops), .endpoint = open_ep, .cq_open = open_cq, .av_open = open_av,
};

static int open_domain(struct fid_fabric *fid, struct fi_info *info, struct fid_domain **out, void *context)
{
    (void)info;
    struct domain *domain = calloc(1, sizeof(*domain));
    if (!domain)
        return -FI_ENOMEM;
    domain->fid.fid = (struct fid){ .fclass = FI_CLASS_DOMAIN, .context = context, .ops = &fid_ops };
    domain->fid.ops = &domain_ops;
    domain->fabric = (struct fabric *)fid;
    ++domain->fabric->children;
    *out = &domain->fid;
    return 0;
}

static struct fi_ops_fabric fabric_ops = { .size = sizeof(fabric_ops), .domain = open_domain };

static int open_fabric(struct fi_fabric_attr *attr, struct fid_fabric **out, void *context)
{
    struct fabric *fabric = calloc(1, sizeof(*fabric));
    if (!fabric)
        return -FI_ENOMEM;
    fabric->fid.fid = (struct fid){ .fclass = FI_CLASS_FABRIC, .context = context, .ops = &fid_ops };
    fabric->fid.ops = &fabric_ops;
    fabric->fid.api_version = attr->api_version;
    *out = &fabric->fid;
    return 0;
}

static const uint64_t caps = FI_MSG | FI_TAGGED | FI_SEND | FI_RECV | FI_LOCAL_COMM |
                             FI_REMOTE_COMM | FI_DIRECTED_RECV | FI_SOURCE;

static int getinfo(uint32_t version, const char *node, const char *service, uint64_t flags,
                   const struct fi_info *hints, struct fi_info **result)
{
    (void)flags;
    const char *socket = getenv("USB4_RDMA_SOCKET");
    if (node || service || !socket || !*socket || version > FI_VERSION(2, 4))
        return -FI_ENODATA;
    if (hints && (hints->caps & ~caps || hints->addr_format != FI_FORMAT_UNSPEC ||
                 (hints->ep_attr && hints->ep_attr->type != FI_EP_UNSPEC &&
                  hints->ep_attr->type != FI_EP_RDM) ||
                 (hints->domain_attr && (hints->domain_attr->threading == FI_THREAD_SAFE ||
                  hints->domain_attr->cq_data_size ||
                  (hints->domain_attr->av_type != FI_AV_UNSPEC && hints->domain_attr->av_type != FI_AV_TABLE) ||
                  hints->domain_attr->data_progress == FI_PROGRESS_AUTO ||
                  hints->domain_attr->control_progress == FI_PROGRESS_AUTO)) ||
                 (hints->tx_attr && (hints->tx_attr->msg_order & ~FI_ORDER_SAS)) ||
                 (hints->rx_attr && (hints->rx_attr->msg_order & ~FI_ORDER_SAS))))
        return -FI_ENODATA;

    struct fi_info *info = fi_allocinfo();
    if (!info)
        return -FI_ENOMEM;
    info->caps = caps;
    info->ep_attr->type = FI_EP_RDM;
    info->ep_attr->max_msg_size = MAX_MSG;
    info->ep_attr->mem_tag_format = UINT64_MAX;
    info->ep_attr->tx_ctx_cnt = 1;
    info->ep_attr->rx_ctx_cnt = 1;
    info->domain_attr->threading = FI_THREAD_DOMAIN;
    info->domain_attr->av_type = FI_AV_TABLE;
    info->domain_attr->data_progress = FI_PROGRESS_MANUAL;
    info->domain_attr->control_progress = FI_PROGRESS_MANUAL;
    info->domain_attr->resource_mgmt = FI_RM_ENABLED;
    info->domain_attr->tx_ctx_cnt = 1;
    info->domain_attr->rx_ctx_cnt = 1;
    info->domain_attr->max_ep_tx_ctx = 1;
    info->domain_attr->max_ep_rx_ctx = 1;
    info->domain_attr->name = strdup("usb4");
    info->tx_attr->caps = caps & ~FI_RECV;
    info->tx_attr->msg_order = FI_ORDER_SAS;
    info->tx_attr->op_flags = hints && hints->tx_attr ? hints->tx_attr->op_flags : FI_COMPLETION;
    info->tx_attr->iov_limit = 1;
    info->tx_attr->size = 128;
    info->tx_attr->inject_size = INJECT_BYTES;
    info->rx_attr->caps = caps & ~FI_SEND;
    info->rx_attr->msg_order = FI_ORDER_SAS;
    info->rx_attr->op_flags = hints && hints->rx_attr ? hints->rx_attr->op_flags : FI_COMPLETION;
    info->rx_attr->iov_limit = 1;
    info->rx_attr->size = 128;
    info->fabric_attr->name = strdup("usb4");
    info->fabric_attr->prov_version = FI_VERSION(0, 1);
    info->fabric_attr->api_version = version;
    if (!info->domain_attr->name || !info->fabric_attr->name) {
        fi_freeinfo(info);
        return -FI_ENOMEM;
    }
    *result = info;
    return 0;
}

static void cleanup(void) {}

static struct fi_provider provider = {
    .name = "usb4", .version = FI_VERSION(0, 1), .fi_version = FI_VERSION(2, 4),
    .getinfo = getinfo, .fabric = open_fabric, .cleanup = cleanup,
};

FI_EXT_INI
{
    return &provider;
}
