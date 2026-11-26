# Wire Protocol (`U4RD` v1)

Multi-byte integers are little-endian. A frame has a 104-byte header and
`0..=32768` payload bytes (`CHUNK = 32 KiB`, `MAX_FRAME = 32872`).

## Header Layout (104 bytes)

| Offset | Size | Field | Notes |
| --- | --- | --- | --- |
| `0` | 4 | `magic` | ASCII `U4RD` |
| `4` | 2 | `version` | `1` |
| `6` | 2 | `kind` | Frame type (`1..=17`) |
| `8` | 4 | `length` | Payload byte length (`0..=32768`) |
| `12` | 4 | `flags` | Bit 31: `FLAG_DIRECT`; Bit 0: `FLAG_WITH_IMM` |
| `16` | 4 | `source` | Source `NodeId` |
| `20` | 4 | `destination` | Destination `NodeId` |
| `24` | 4 | `hops` | Remaining hop limit (`1..=64`) |
| `28` | 4 | `tag_or_imm` | Two-sided message tag or 32-bit `RDMA_WRITE_WITH_IMM` immediate value |
| `32` | 8 | `epoch` | Immutable cluster session epoch (`> 0`) |
| `40` | 8 | `connection` | Globally unique bidirectional QP ID (`initiator_node << 32 | counter`) |
| `48` | 8 | `request` | Per-operation ID (or link incarnation nonce in `Hello`/`HelloAck`/`Ping`/`Pong`) |
| `56` | 8 | `offset` | Byte offset within this frame's transfer window |
| `64` | 8 | `total` | Total byte length of this transfer / window |
| `72` | 8 | `rkey` | Remote MR capability key (`0` for two-sided ops) |
| `80` | 8 | `remote_offset` | Base byte offset inside the remote MR, or the send sequence for `Rts`. Other two-sided frames use `0`. |
| `88` | 8 | `aux` | Window grant/ACK size, echoed nonce, listener ID, full op length for `WITH_IMM`, or error status |
| `96` | 4 | `crc32` | CRC32 over header (with bytes `96..100` zeroed) + payload |
| `100` | 4 | `reserved` | Must be `0` |

## Frame Kinds

- **Link lifecycle & flow control:** `Hello (1)`, `HelloAck (11)`, `Ping (12)`, `Pong (13)`, `Credit (8)`
- **Queue Pair lifecycle:** `Open (2)`, `OpenAck (3)`, `Close (9)`, `Error (10)`
- **Two-sided messaging:** `Rts (4)`, `Grant (5)`, `Data (6)`, `Ack (7)`
- **One-sided RDMA:** `RdmaWrite (14)`, `RdmaWriteAck (15)`, `RdmaReadReq (16)`, `RdmaReadResp (17)`

`Data`, `RdmaWrite`, and `RdmaReadResp` are **data-plane** frames: they consume per-hop direct/transit receive credits, carry payload bytes, and are eligible for parallel-link bonding (32 KiB striping across parallel edges above 32 KiB; least-queued edge at or below 32 KiB). All other frames use reserved control queues and never block on data credits. At each hop, a connection's `Rts` frames use a single configured outgoing link and wait when its control queue is full or the link is still coming up.

## Handshake, Liveness & Fail-Stop Epochs

- Every link generates a random nonzero 64-bit incarnation nonce and retries `Hello` (carrying the 32-byte SHA-256 topology digest) until `HelloAck` echoes its nonce.
- Matching duplicate `Hello`/`HelloAck` frames are idempotent; any epoch, identity, digest, or incarnation-nonce change fails the session.
- Idle links probe with monotonic `Ping`/`Pong` challenges at `timeout_ms / 4` with a 1 ms minimum interval. For one second after non-health traffic, the daemon uses its recent-transfer interval to keep isolated control completions progressing. USB4 readiness defaults to 100 µs. Other daemon backends default to 1 ms. `--active-probe-us` accepts 50–1,000 µs.
- A cable or node failure is terminal for the epoch. Operators recover by starting a fresh epoch across participating nodes.

## Two-Sided & One-Sided Flows

- **Two-sided `SEND` / `RECV`:** The sender sends `Rts`. The receiver matches `(connection, tag)` in send order, checks capacity, and sends `Grant` for up to `1 MiB`. The sender streams `Data` chunks. The receiver sends `Ack` once its application consumes the window.
- **One-sided `RDMA_WRITE`:** Initiator streams bounded (`<= 1 MiB`) `RdmaWrite` chunks directly to the target daemon (1 RTT, no `Rts`/`Grant`). Target validates `(connection, rkey, REMOTE_WRITE, bounds)`, writes directly into the daemon-mapped target MR, tracks chunks with a 32-bit window bitmap, and sends `RdmaWriteAck`.
- **One-sided `RDMA_WRITE_WITH_IMM`:** Same as `RDMA_WRITE`, except on completion of the final window the target daemon enqueues an immediate notice consumed by a posted `OP_RECV` on the target QP (producing `WcOpcode::RecvRdmaWithImm`) before sending the final `RdmaWriteAck`.
- **One-sided `RDMA_READ`:** Initiator sends `RdmaReadReq` for each bounded window (`<= 1 MiB`); target validates `(connection, rkey, REMOTE_READ, bounds)` and streams 32 KiB `RdmaReadResp` chunks directly from its mapped MR back to the initiator.
- **Access errors:** Invalid `rkey`, missing `REMOTE_READ`/`REMOTE_WRITE`, or out-of-bounds offsets return a targeted `Error` frame (`STATUS_REMOTE_ACCESS_ERR`) that fails only the offending work request (`WcStatus::RemoteAccessErr`) without poisoning unrelated clients or the fabric.
