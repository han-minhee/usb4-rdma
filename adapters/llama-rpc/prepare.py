#!/usr/bin/env python3
"""Add the USB4STREAM transport to an isolated llama.cpp v1.1.0 source tree."""

import argparse
from pathlib import Path
import shutil


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f"unexpected llama.cpp v1.1.0 source near {old!r}")
    return text.replace(old, new)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tree", type=Path)
    args = parser.parse_args()
    base = args.tree / "ggml/src/ggml-rpc"
    source = base / "transport.cpp"
    build = base / "CMakeLists.txt"
    repository = Path(__file__).resolve().parents[2]
    transport = source.read_text()
    cmake = build.read_text()
    if "#include \"usb4_socket.hpp\"" in transport:
        raise SystemExit("USB4 transport is already installed in this tree")
    transport = replace_once(transport, '#include "log.h"', '#include "log.h"\n#include "usb4_socket.hpp"')
    transport = replace_once(transport, "struct socket_t::impl {", "struct socket_t::impl {\n    std::unique_ptr<Usb4Socket> usb4;")
    for signature, operation in (
        ("send_data(const void * data, size_t size)", "usb4->send(data, size)"),
        ("recv_data(void * data, size_t size)", "usb4->receive(data, size)"),
        ("flush()", "usb4->flush()"),
    ):
        start = f"bool socket_t::impl::{signature} {{"
        transport = replace_once(transport, start, start + f"\n    if (usb4) {{\n        return {operation};\n    }}")
    transport = replace_once(transport, 'return pimpl->use_rdma ? "RDMA" : "TCP";',
                             'return pimpl->usb4 ? "USB4STREAM" : (pimpl->use_rdma ? "RDMA" : "TCP");')
    for factory, fd, responder in (("accept()", "client_socket_fd", "true"), ("connect(const char * host, int port)", "sockfd", "false")):
        start = transport.index(f"socket_ptr socket_t::{factory} {{")
        end = transport.index("\n}\n", start)
        old = f"    return socket_ptr(new socket_t(std::make_unique<impl>({fd})));"
        block = replace_once(transport[start:end], old, f"""    auto socket = std::make_unique<impl>({fd});
    if (Usb4Socket::enabled()) {{
        socket->usb4 = Usb4Socket::bootstrap({fd}, {responder});
        if (!socket->usb4) {{
            LOG_ERROR("USB4STREAM bootstrap failed\\n");
            return nullptr;
        }}
    }}
    return socket_ptr(new socket_t(std::move(socket)));""")
        transport = transport[:start] + block + transport[end:]
    cmake += """
find_library(USB4_RPC_LIBRARY usb4_rpc REQUIRED)
target_link_libraries(ggml-rpc PRIVATE ${USB4_RPC_LIBRARY})
"""
    shutil.copyfile(Path(__file__).with_name("usb4_socket.hpp"), base / "usb4_socket.hpp")
    shutil.copyfile(repository / "include/usb4_rpc.h", base / "usb4_rpc.h")
    source.write_text(transport)
    build.write_text(cmake)


if __name__ == "__main__":
    main()
