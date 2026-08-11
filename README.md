# 16kbTCP

Light weight TCP based protocol with random MAGIC, 16kb fragmentation, optional zlib compression.

---

## Technical features

- **Dynamic Magic Number** – a random 16-bit signature is generated once per process (`rand::random::<u16>() | 1`). Each packet starts with this magic, making the protocol less predictable and harder to spoof.

- **Fixed 13‑byte header** – contains magic, flags, message ID, fragment index, total fragments, and payload length. Header is encoded in big‑endian.

- **16 KB packet limit** – maximum packet size is 16 KiB, with 13 bytes reserved for the header, leaving 16 371 bytes for payload per fragment. This matches typical network MTU constraints.

- **Automatic fragmentation & reassembly** – messages larger than the payload limit are split into fragments. Each fragment carries the same message ID and total fragment count. The receiver collects fragments until all are received, then reassembles the original message.

- **Optional zlib compression** – compression can be enabled per message. The protocol compares compressed size vs. original and uses the smaller one. Compression level is configurable via `ProtocolConfig`.

- **Reliable delivery with ACK** – after sending all fragments of a message, the sender waits for a dedicated ACK packet (flag `FLAG_ACK`). If no ACK is received within the timeout, the whole message is retransmitted (configurable retry count).

- **Timeout and cleanup** – incomplete assemblers expire after `assembler_timeout` (default 30s); completed messages are kept for `completed_timeout` (default 60s) to avoid duplicate processing. Cleanup runs when the number of entries exceeds a threshold.

- **Duplex communication** – a single `Protocol` instance can both send and receive messages over the same TCP stream. The `receive_raw()` loop handles both data fragments and ACKs transparently.

- **Asynchronous I/O (Tokio)** – all network operations are non‑blocking, allowing high concurrency and scalability.

- **Configurable** – `ProtocolConfig` allows tuning of retry parameters, compression level, timeouts, fragment limits, message size limits, and cleanup thresholds.

- **Simple public API** – just `send_message(data, compress)` and `receive_message()` hide all complexity of fragmentation, compression, and retries.

---

## Example logs 
```cmd
[2026-08-11T16:38:12.645Z INFO protocol] Server started on 127.0.0.1:8080
[2026-08-11T16:38:12.857Z INFO protocol] Client connecting...
[2026-08-11T16:38:12.858Z INFO protocol] Connection from 127.0.0.1:60975
[2026-08-11T16:38:12.858Z INFO protocol] Client: sending 30000 bytes with compression enabled
[2026-08-11T16:38:12.859Z INFO protocol] Sending message 1 (size: 30000 bytes, compression: on)
[2026-08-11T16:38:12.860Z INFO protocol] Message 1 compressed: 30000 -> 53 bytes
[2026-08-11T16:38:12.860Z INFO protocol] Message 1 split into 1 fragments (max payload 16371 bytes)
[2026-08-11T16:38:12.860Z INFO protocol] Sending message 1 (attempt 1/3)
[2026-08-11T16:38:12.860Z INFO protocol] All fragments for message 1 sent, waiting for ACK
[2026-08-11T16:38:12.860Z INFO protocol] Created assembler for message 1 (1 fragments)
[2026-08-11T16:38:12.860Z INFO protocol] Message 1 assembled from 1 fragments, size 53 bytes, compressed: yes
[2026-08-11T16:38:12.861Z INFO protocol] Message 1 decompressed: 53 -> 30000 bytes
[2026-08-11T16:38:12.861Z INFO protocol] Message 1 fully assembled and acknowledged
[2026-08-11T16:38:12.861Z INFO protocol] Server received message of 30000 bytes
[2026-08-11T16:38:12.861Z INFO protocol] Sending message 1 (size: 30000 bytes, compression: on)
[2026-08-11T16:38:12.862Z INFO protocol] Message 1 compressed: 30000 -> 53 bytes
[2026-08-11T16:38:12.862Z INFO protocol] Message 1 split into 1 fragments (max payload 16371 bytes)
[2026-08-11T16:38:12.862Z INFO protocol] Sending message 1 (attempt 1/3)
[2026-08-11T16:38:12.862Z INFO protocol] All fragments for message 1 sent, waiting for ACK
[2026-08-11T16:38:12.863Z INFO protocol] Received ACK for message 1 in 4.0309ms
[2026-08-11T16:38:12.863Z INFO protocol] Client: message sent and acknowledged
[2026-08-11T16:38:12.863Z INFO protocol] Created assembler for message 1 (1 fragments)
[2026-08-11T16:38:12.863Z INFO protocol] Message 1 assembled from 1 fragments, size 53 bytes, compressed: yes
[2026-08-11T16:38:12.864Z INFO protocol] Message 1 decompressed: 53 -> 30000 bytes
[2026-08-11T16:38:12.864Z INFO protocol] Message 1 fully assembled and acknowledged
[2026-08-11T16:38:12.864Z INFO protocol] Received ACK for message 1 in 2.6288ms
[2026-08-11T16:38:12.864Z INFO protocol] Client: received response of 30000 bytes
[2026-08-11T16:38:12.864Z INFO protocol] Client: data matches, test passed!
[2026-08-11T16:38:13.374Z INFO protocol] Client closed connection
```
