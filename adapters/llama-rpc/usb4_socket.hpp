#pragma once

#include "usb4_rpc.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <sys/socket.h>
#include <sys/time.h>

class Usb4Socket {
public:
    static bool enabled() {
        const char *value = std::getenv("GGML_RPC_USB4");
        return value && std::strcmp(value, "1") == 0;
    }

    static std::unique_ptr<Usb4Socket> bootstrap(int fd, bool server) {
        static constexpr std::array<uint8_t, 4> magic{'U', '4', 'R', '2'};
        std::array<uint8_t, magic.size() + USB4_RPC_HANDLE_BYTES> wire{};
        const timeval timeout{30, 0};
        if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
            setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) {
            return nullptr;
        }
        Usb4RpcConnection *connection = nullptr;
        if (server) {
            using Listener = std::unique_ptr<Usb4RpcListener, decltype(&usb4_rpc_close_listener)>;
            Listener listener(usb4_rpc_listen(nullptr), usb4_rpc_close_listener);
            std::copy(magic.begin(), magic.end(), wire.begin());
            if (listener && usb4_rpc_listener_handle(listener.get(), wire.data() + magic.size()) &&
                transfer(fd, wire.data(), wire.size(), true)) {
                connection = usb4_rpc_accept(listener.get());
            }
        } else if (transfer(fd, wire.data(), wire.size(), false) &&
                   std::equal(magic.begin(), magic.end(), wire.begin())) {
            connection = usb4_rpc_connect(nullptr, wire.data() + magic.size());
        }
        return connection ? std::unique_ptr<Usb4Socket>(new Usb4Socket(connection)) : nullptr;
    }

    ~Usb4Socket() {
        if (transaction) {
            usb4_rpc_end(transaction, false);
        }
        usb4_rpc_close(connection);
    }

    bool send(const void *data, size_t size) {
        std::unique_lock<std::mutex> guard(mutex);
        if (!acquire(guard)) {
            return false;
        }
        return usb4_rpc_stage_send(transaction, static_cast<const uint8_t *>(data), size) || finish(false);
    }

    bool receive(void *data, size_t size) {
        return receive_impl(data, size, false);
    }

    bool receive_after_idle(void *data) {
        return receive_impl(data, 1, true);
    }

    bool flush() {
        std::unique_lock<std::mutex> guard(mutex);
        if (!wait(guard)) {
            return false;
        }
        return !transaction || finish(usb4_rpc_flush(transaction));
    }

private:
    explicit Usb4Socket(Usb4RpcConnection *conn) : connection(conn) {}

    static bool transfer(int fd, uint8_t *data, size_t len, bool sending) {
        while (len) {
            const ssize_t result = sending ? ::send(fd, data, len, MSG_NOSIGNAL) : ::recv(fd, data, len, 0);
            if (result < 0 && errno == EINTR) {
                continue;
            }
            if (result <= 0) {
                return false;
            }
            data += result;
            len -= static_cast<size_t>(result);
        }
        return true;
    }

    bool receive_impl(void *data, size_t size, bool allow_idle) {
        std::unique_lock<std::mutex> guard(mutex);
        if (!acquire(guard)) {
            return false;
        }
        auto *active = transaction;
        auto *bytes = static_cast<uint8_t *>(data);
        // The owner keeps the transaction while other callers wait with their own deadline.
        guard.unlock();
        const bool received = allow_idle ? usb4_rpc_recv_after_idle(active, bytes)
                                         : usb4_rpc_recv_exact(active, bytes, size);
        guard.lock();
        return finish(received);
    }

    bool wait(std::unique_lock<std::mutex> &guard) {
        return ready.wait_for(guard, std::chrono::seconds(30), [&] {
            return failed || !transaction || owner == std::this_thread::get_id();
        }) && !failed;
    }

    bool acquire(std::unique_lock<std::mutex> &guard) {
        if (!wait(guard)) {
            return false;
        }
        if (!transaction) {
            transaction = usb4_rpc_begin(connection);
            owner = std::this_thread::get_id();
        }
        return transaction != nullptr;
    }

    bool finish(bool ok) {
        const bool committed = usb4_rpc_end(transaction, ok);
        transaction = nullptr;
        failed = !ok || !committed;
        ready.notify_all();
        return !failed;
    }

    Usb4RpcConnection *connection;
    Usb4RpcTransaction *transaction = nullptr;
    std::mutex mutex;
    std::condition_variable ready;
    std::thread::id owner;
    bool failed = false;
};
