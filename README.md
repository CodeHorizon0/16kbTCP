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
[2026-08-12T19:24:55.245Z INFO  protocol] Server started on 127.0.0.1:8080
[2026-08-12T19:24:55.453Z INFO  protocol] Client connecting...
[2026-08-12T19:24:55.454Z INFO  protocol] Connection from 127.0.0.1:54376
[2026-08-12T19:24:55.454Z INFO  protocol] Client: sending 30000 bytes with compression enabled
[2026-08-12T19:24:55.455Z INFO  protocol] Sending message 15876000817515482866 (size: 30000 bytes, compression: on)
[2026-08-12T19:24:55.456Z INFO  protocol] Message 15876000817515482866 compressed: 30004 -> 58 bytes
[2026-08-12T19:24:55.456Z INFO  protocol] Message 15876000817515482866 split into 1 fragments (max payload 16367 bytes)
[2026-08-12T19:24:55.456Z INFO  protocol] Created assembler for message 15876000817515482866 (1 fragments)
[2026-08-12T19:24:55.456Z INFO  protocol] All fragments for message 15876000817515482866 sent
[2026-08-12T19:24:55.457Z INFO  protocol] Message 15876000817515482866 assembled from 1 fragments, size 58 bytes, compressed: yes
[2026-08-12T19:24:55.457Z INFO  protocol] Client: message sent
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
[2026-08-12T19:24:55.461Z INFO  protocol] CRC check passed for message 12446259305763433799
[2026-08-12T19:24:55.461Z INFO  protocol] Client: received response of 30000 bytes
[2026-08-12T19:24:55.461Z INFO  protocol] Client: data matches, test passed!
[2026-08-12T19:24:55.972Z INFO  protocol] Client closed connection
```
