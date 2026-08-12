# 16kbTCP

Lightweight TCP‑based protocol with random magic, 16 KB fragmentation, optional zlib compression, CRC32 integrity check, and built‑in keep‑alive.

---

## Technical features

- **Dynamic Magic Number** – a random 16‑bit signature generated once per process (`rand::random::<u16>() | 1`). Every packet starts with this magic, making spoofing harder.

- **Fixed 17‑byte header** – contains magic, flags, 64‑bit message ID, fragment index, total fragments, and payload length. All fields are big‑endian.

- **16 KB packet limit** – maximum packet size is 16 KiB; 17 bytes are reserved for the header, leaving 16 367 bytes for payload per fragment. This matches typical network MTU constraints.

- **Automatic fragmentation & reassembly** – messages larger than the payload limit are split into fragments. Each fragment carries the same message ID and total fragment count. The receiver collects fragments until all are received, then reassembles the original message.

- **Optional zlib compression** – compression can be enabled per message. The protocol compares compressed size vs. original and uses the smaller one. Compression level is configurable via `ProtocolConfig`.

- **Reliability via TCP** – the protocol relies on TCP’s built‑in reliability; no application‑level ACKs or retransmissions are used. This avoids redundant traffic and simplifies the logic.

- **CRC32 integrity check** – a 32‑bit CRC of the original data is prepended to the payload before compression (if enabled). On the receiver side, after reassembly and decompression, the CRC is verified. Mismatch causes an error, guaranteeing data integrity.

- **Read timeout** – all read operations are wrapped with `tokio::time::timeout` (configurable via `read_timeout`), preventing indefinite blocking on a stuck connection.

- **Keep‑Alive** – TCP keep‑alive is enabled via `socket2` with a configurable interval (default 30 s), allowing early detection of broken connections.

- **Denial‑of‑Service protection** – limits are placed on the number of active assemblers (`max_assemblers`) and completed message entries (`max_completed`). When exceeded, the oldest entries are evicted. This prevents memory exhaustion from malicious or misbehaving peers.

- **Unique message IDs** – 64‑bit message IDs are used, with the initial value generated randomly (`rand::random::<u64>()`) per `Protocol` instance. This eliminates collisions across different connections and simplifies reassembly.

- **Duplex communication** – a single `Protocol` instance can both send and receive messages over the same TCP stream.

- **Asynchronous I/O (Tokio)** – all network operations are non‑blocking, enabling high concurrency and scalability.

- **Configurable** – `ProtocolConfig` allows tuning of compression, timeouts, limits, and keep‑alive interval.

- **Simple public API** – just `send_message(data, compress)` and `receive_message()` hide all fragmentation, compression, CRC, and reassembly details.

---

## Example logs

```cmd
[2026-08-12T19:24:55.245Z INFO  protocol] Server started on 127.0.0.1:8080
[2026-08-12T19:24:55.453Z INFO  protocol] Client connecting...
[2026-08-12T19:24:55.454Z INFO  protocol] Connection from 127.0.0.1:54376
[2026-08-12T19:24:55.454Z INFO  protocol] Client: sending 30000 bytes with compression enabled
[2026-08-12T19:24:55.455Z INFO  protocol] Sending message 15876000817515482866 (size: 30000 bytes, compression: on)
[2026-08-12T19:24:55.456Z INFO  protocol] Message 15876000817515482866 compressed: 30004 -> 58 bytes
[2026-08-12T19:24:55.456Z INFO  protocol] Message 15876000817515482866 split into 1 fragments (max payload 16367 bytes)
[2026-08-12T19:24:55.456Z INFO  protocol] All fragments for message 15876000817515482866 sent
[2026-08-12T19:24:55.456Z INFO  protocol] Created assembler for message 15876000817515482866 (1 fragments)
[2026-08-12T19:24:55.457Z INFO  protocol] Message 15876000817515482866 assembled from 1 fragments, size 58 bytes, compressed: yes
[2026-08-12T19:24:55.457Z INFO  protocol] Message 15876000817515482866 decompressed: 58 -> 30004 bytes
[2026-08-12T19:24:55.457Z INFO  protocol] Message 15876000817515482866 fully assembled
[2026-08-12T19:24:55.457Z INFO  protocol] CRC check passed for message 15876000817515482866
[2026-08-12T19:24:55.458Z INFO  protocol] Server received message of 30000 bytes
[2026-08-12T19:24:55.458Z INFO  protocol] Sending message 12446259305763433799 (size: 30000 bytes, compression: on)
[2026-08-12T19:24:55.459Z INFO  protocol] Message 12446259305763433799 compressed: 30004 -> 58 bytes
[2026-08-12T19:24:55.459Z INFO  protocol] Message 12446259305763433799 split into 1 fragments (max payload 16367 bytes)
[2026-08-12T19:24:55.459Z INFO  protocol] All fragments for message 12446259305763433799 sent
[2026-08-12T19:24:55.459Z INFO  protocol] Created assembler for message 12446259305763433799 (1 fragments)
[2026-08-12T19:24:55.460Z INFO  protocol] Message 12446259305763433799 assembled from 1 fragments, size 58 bytes, compressed: yes
[2026-08-12T19:24:55.460Z INFO  protocol] Message 12446259305763433799 decompressed: 58 -> 30004 bytes
[2026-08-12T19:24:55.460Z INFO  protocol] Message 12446259305763433799 fully assembled
[2026-08-12T19:24:55.460Z INFO  protocol] CRC check passed for message 12446259305763433799
[2026-08-12T19:24:55.461Z INFO  protocol] Client: received response of 30000 bytes
[2026-08-12T19:24:55.461Z INFO  protocol] Client: data matches, test passed!
[2026-08-12T19:24:55.972Z INFO  protocol] Client closed connection
```
