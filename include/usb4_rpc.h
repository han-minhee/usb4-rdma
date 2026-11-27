#ifndef USB4_RPC_H
#define USB4_RPC_H

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#define USB4_RPC_HANDLE_BYTES 20

typedef struct Usb4RpcConnection Usb4RpcConnection;

typedef struct Usb4RpcListener Usb4RpcListener;

typedef struct Usb4RpcTransaction Usb4RpcTransaction;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * # Safety
 * The socket path is null or a live NUL-terminated path.
 */
struct Usb4RpcListener *usb4_rpc_listen(const char *socket);

/**
 * # Safety
 * `listener` is live and `out` holds `USB4_RPC_HANDLE_BYTES` writable bytes.
 */
bool usb4_rpc_listener_handle(const struct Usb4RpcListener *listener, uint8_t *out);

/**
 * # Safety
 * `listener` is a live handle. Only one caller may accept at a time.
 */
struct Usb4RpcConnection *usb4_rpc_accept(const struct Usb4RpcListener *listener);

/**
 * # Safety
 * `handle` holds `USB4_RPC_HANDLE_BYTES` readable bytes and socket follows `usb4_rpc_listen`.
 */
struct Usb4RpcConnection *usb4_rpc_connect(const char *socket, const uint8_t *handle);

/**
 * # Safety
 * `conn` is live until begin returns.
 */
struct Usb4RpcTransaction *usb4_rpc_begin(const struct Usb4RpcConnection *conn);

/**
 * # Safety
 * `txn` is live. `data` points to `len` readable bytes, or can be null when `len` is zero.
 */
bool usb4_rpc_stage_send(const struct Usb4RpcTransaction *txn, const uint8_t *data, size_t len);

/**
 * # Safety
 * `txn` is live for this call.
 */
bool usb4_rpc_flush(const struct Usb4RpcTransaction *txn);

/**
 * # Safety
 * `txn` is live. `data` points to `len` exclusively writable bytes, or can be null at length zero.
 */
bool usb4_rpc_recv_exact(const struct Usb4RpcTransaction *txn, uint8_t *data, size_t len);

/**
 * # Safety
 * `txn` is null or a uniquely owned live transaction. This consumes it exactly once.
 */
bool usb4_rpc_end(struct Usb4RpcTransaction *txn, bool commit);

/**
 * # Safety
 * `conn` is null or a uniquely owned connection handle, consumed exactly once.
 */
void usb4_rpc_close(struct Usb4RpcConnection *conn);

/**
 * # Safety
 * `listener` is null or a uniquely owned listener handle. No accept may be active.
 */
void usb4_rpc_close_listener(struct Usb4RpcListener *listener);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* USB4_RPC_H */
