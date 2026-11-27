#!/usr/bin/env python3
"""Run a multi-window host transfer through the loaded net v12 vtable."""

import ctypes as c
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


VOID = c.c_void_p
OUT = c.POINTER(VOID)
INT = c.c_int
COUNT = 2 * 1024 * 1024 + 17
TAG = 42
ABI = "name init devices getProperties listen connect accept regMr regMrDmaBuf deregMr isend irecv iflush test closeSend closeRecv closeListen getDeviceMr irecvConsumed makeVDevice finalize setNetAttr"


class Net(c.Structure):
    _fields_ = [(name, VOID) for name in ABI.split()]

    def call(self, name, types, *args):
        callback = c.CFUNCTYPE(INT, *types)(getattr(self, name))
        result = callback(*args)
        if result:
            raise RuntimeError(f"{name} returned {result}")


def wait_for(step):
    deadline = time.monotonic() + 10
    while not step():
        if time.monotonic() >= deadline:
            raise TimeoutError("net v12 request did not retire")
        time.sleep(0.0001)


def payload():
    unit = bytes((i * 31 + 7) % 256 for i in range(256))
    return (unit * (COUNT // 256 + 1))[:COUNT]


def worker(artifact, receiver):
    net = Net.in_dll(c.CDLL(artifact, mode=c.RTLD_LOCAL), "ncclNetPlugin_v12")
    ctx, comm, offload, request, mr = VOID(), VOID(), VOID(), VOID(), VOID()
    net.call("init", (OUT, c.c_uint64, VOID, VOID, VOID), c.byref(ctx), 1, None, None, None)
    handle = c.create_string_buffer(128)
    if receiver:
        listener = VOID()
        net.call("listen", (VOID, INT, VOID, OUT), ctx, 0, handle, c.byref(listener))
        print(handle.raw.hex(), flush=True)
        def connect():
            net.call("accept", (VOID, OUT, OUT), listener, c.byref(comm), c.byref(offload))
            return comm.value
        wait_for(connect)
    else:
        raw = bytes.fromhex(sys.stdin.readline())
        assert len(raw) == 128
        handle.raw = raw
        def connect():
            net.call("connect", (VOID, INT, VOID, OUT, OUT), ctx, 0, handle, c.byref(comm), c.byref(offload))
            return comm.value
        wait_for(connect)
    data = c.create_string_buffer(COUNT) if receiver else c.create_string_buffer(payload(), COUNT)
    net.call("regMr", (VOID, VOID, c.c_size_t, INT, OUT), comm, data, COUNT, 1, c.byref(mr))
    if receiver:
        arrays = ((VOID * 1)(c.addressof(data)), (c.c_size_t * 1)(COUNT), (INT * 1)(TAG), (VOID * 1)(mr.value))
        def post():
            net.call("irecv", (VOID, INT, OUT, c.POINTER(c.c_size_t), c.POINTER(INT), OUT, OUT, OUT),
                     comm, 1, *arrays, None, c.byref(request))
            return request.value
    else:
        def post():
            net.call("isend", (VOID, VOID, c.c_size_t, INT, VOID, VOID, OUT),
                     comm, data, COUNT, TAG, mr, None, c.byref(request))
            return request.value
    wait_for(post)
    done, size = INT(), INT()
    def test():
        net.call("test", (VOID, c.POINTER(INT), c.POINTER(INT)), request, c.byref(done), c.byref(size))
        return done.value
    wait_for(test)
    assert size.value == COUNT
    if receiver:
        assert data.raw == payload()
    net.call("deregMr", (VOID, VOID), comm, mr)
    net.call("closeRecv" if receiver else "closeSend", (VOID,), comm)
    if receiver:
        net.call("closeListen", (VOID,), listener)
    net.call("finalize", (VOID,), ctx)


def run():
    root = Path(__file__).resolve().parents[2]
    target = Path(os.environ.get("CARGO_TARGET_DIR", root / "target")) / os.environ.get("U4RD_PROFILE", "debug")
    ctl = target / "usb4-rdma-ctl"
    artifact = target / "libusb4_net.so"
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        (work / "links").mkdir()
        sockets = [work / f"node{rank}" for rank in range(2)]
        children = []
        try:
            for rank, socket in enumerate(sockets):
                children.append(subprocess.Popen([
                    ctl, "serve", "--config", root / "examples/two-node.toml", "--node", str(rank),
                    "--socket", socket, "--backend", "emulated", "--emulated-dir", work / "links",
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            def ready():
                return all(path.exists() and subprocess.run([ctl, "monitor", "--socket", path],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0 for path in sockets)
            wait_for(ready)
            env = dict(os.environ, USB4_RDMA_GPU_RUNTIME="host", USB4_RDMA_SOCKET=str(sockets[1]))
            command = [sys.executable, __file__, str(artifact)]
            receiver = subprocess.Popen([*command, "recv"], env=env, stdout=subprocess.PIPE, text=True)
            children.append(receiver)
            import select
            assert select.select([receiver.stdout], [], [], 10)[0], "listener handle timeout"
            token = receiver.stdout.readline()
            assert len(token.strip()) == 256
            env["USB4_RDMA_SOCKET"] = str(sockets[0])
            subprocess.run([*command, "send"], env=env, input=token, text=True, timeout=20, check=True)
            assert receiver.wait(20) == 0
            print(f"Net v12 verified a {COUNT}-byte host transfer")
        finally:
            for child in reversed(children):
                if child.poll() is None:
                    child.terminate()
            for child in children:
                child.wait(10)


if __name__ == "__main__":
    if len(sys.argv) == 3:
        worker(sys.argv[1], sys.argv[2] == "recv")
    else:
        run()
