# Wire protocol (`U4RD` v2)

Frames use little-endian fields, a **104-byte header**, and `0..=32768` bytes of payload (`CHUNK = 32 KiB`, `MAX_FRAME = 32872`), protected end-to-end by CRC32 over header + payload.

## Header layout

| Offset | Size | Field | Purpose |
| --- | --- | --- | --- |
| `0` | 4 | `magic` | ASCII `U4RD` |
| `4` | 2 | `version` | `2` |
| `6` | 2 | `kind` | Frame type (`1..=17`) |
| `8` | 4 | `length` | Payload bytes (`0..=32768`) |
| `12` | 4 | `flags` | Bit 31: `FLAG_DIRECT`; Bit 0: `FLAG_WITH_IMM` |
| `16` / `20` | 4 / 4 | `source` / `destination` | Source and destination `NodeId` |
| `24` | 4 | `hops` | Remaining hop limit (`1..=64`) |
| `28` / `100` | 4 / 4 | `tag_low` / `tag_high` | 64-bit tag or immediate value |
| `32` | 8 | `epoch` | Immutable cluster session epoch (`> 0`) |
| `40` | 8 | `connection` | Bidirectional QP ID (`initiator << 32 \| counter`) |
| `48` | 8 | `request` | Request ID or link incarnation nonce |
| `56` / `64` | 8 / 8 | `offset` / `total` | Byte offset and total length of transfer window |
| `72` / `80` | 8 / 8 | `rkey` / `remote_offset` | Remote MR capability key and byte offset (or `Rts` sequence) |
| `88` | 8 | `aux` | Grant/ACK size, echoed nonce, listener ID, `WITH_IMM` total, or status |
| `96` | 4 | `crc32` | CRC32 checksum (header bytes `96..100` zeroed) |

## Flow and liveness

- **Control vs. data frames**: `Data (6)`, `RdmaWrite (14)`, and `RdmaReadResp (17)` consume per-hop receive credits and stripe across bonded links (`32 KiB` above `64 KiB`). Link/QP control frames (`Hello`, `HelloAck`, `Ping`, `Pong`, `Credit`, `Open`, `OpenAck`, `Close`, `Error`, `Rts`, `Grant`, `Ack`, `RdmaWriteAck`, `RdmaReadReq`) use reserved control queues.
- **One-sided RDMA**: `RDMA_WRITE` / `RDMA_READ` stream bounded `<= 1 MiB` windows directly against daemon-validated `(connection, rkey, access, bounds)` without `Rts`/`Grant` round trips; invalid access returns `STATUS_REMOTE_ACCESS_ERR` for that work request alone.
- **Fail-stop epochs**: Links exchange 64-bit incarnation nonces + SHA-256 topology digests (`Hello`/`HelloAck`) and monitor health via `Ping`/`Pong`. Any epoch/digest mismatch or link loss ends the epoch cleanly.
