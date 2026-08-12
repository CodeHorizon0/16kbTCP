// src/lib.rs

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;

use anyhow::{Result, anyhow, bail};
use log::{info, warn, debug};

pub const MAX_PACKET_SIZE: usize = 16 * 1024;
pub const HEADER_SIZE: usize = 17;
pub const MAX_PAYLOAD_SIZE: usize = MAX_PACKET_SIZE - HEADER_SIZE;
pub const FLAG_COMPRESSED: u8 = 0x01;

const DEFAULT_MAX_TOTAL_FRAGMENTS: u16 = 1024;
const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_ASSEMBLER_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_ASSEMBLERS: usize = 1000;
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_COMPLETED_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_MAX_COMPLETED: usize = 1000;

fn get_magic() -> u16 {
    static MAGIC: OnceLock<u16> = OnceLock::new();
    *MAGIC.get_or_init(|| rand::random::<u16>() | 1)
}

#[derive(Debug, Clone, Copy)]
pub struct FragmentHeader {
    pub magic: u16,
    pub flags: u8,
    pub message_id: u64,
    pub fragment_index: u16,
    pub total_fragments: u16,
    pub payload_len: u16,
}

impl FragmentHeader {
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..2].copy_from_slice(&self.magic.to_be_bytes());
        buf[2] = self.flags;
        buf[3..11].copy_from_slice(&self.message_id.to_be_bytes());
        buf[11..13].copy_from_slice(&self.fragment_index.to_be_bytes());
        buf[13..15].copy_from_slice(&self.total_fragments.to_be_bytes());
        buf[15..17].copy_from_slice(&self.payload_len.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8; HEADER_SIZE]) -> Result<Self> {
        let magic = u16::from_be_bytes([buf[0], buf[1]]);
        let expected = get_magic();
        if magic != expected {
            bail!("Invalid magic number: {:#x} (expected {:#x})", magic, expected);
        }
        Ok(Self {
            magic,
            flags: buf[2],
            message_id: u64::from_be_bytes([
                buf[3], buf[4], buf[5], buf[6],
                buf[7], buf[8], buf[9], buf[10],
            ]),
            fragment_index: u16::from_be_bytes([buf[11], buf[12]]),
            total_fragments: u16::from_be_bytes([buf[13], buf[14]]),
            payload_len: u16::from_be_bytes([buf[15], buf[16]]),
        })
    }
}

struct MessageAssembler {
    message_id: u64,
    total_fragments: u16,
    fragments: Vec<Vec<u8>>,
    received: Vec<bool>,
    received_count: usize,
    compressed: bool,
    created_at: Instant,
}

impl MessageAssembler {
    fn new(message_id: u64, total_fragments: u16, compressed: bool) -> Self {
        let cap = total_fragments as usize;
        Self {
            message_id,
            total_fragments,
            fragments: vec![Vec::new(); cap],
            received: vec![false; cap],
            received_count: 0,
            compressed,
            created_at: Instant::now(),
        }
    }

    fn add_fragment(&mut self, index: u16, data: Vec<u8>) -> bool {
        let idx = index as usize;
        if idx >= self.fragments.len() || self.received[idx] {
            return false;
        }
        self.fragments[idx] = data;
        self.received[idx] = true;
        self.received_count += 1;
        debug!(
            "Message {}: got fragment {}/{}, total {}/{}",
            self.message_id,
            index + 1,
            self.total_fragments,
            self.received_count,
            self.total_fragments
        );
        self.received_count == self.total_fragments as usize
    }

    fn assemble(self, max_message_size: usize) -> Result<Vec<u8>> {
        if self.received_count != self.total_fragments as usize {
            bail!(
                "Not all fragments received for message {} (received {}/{})",
                self.message_id,
                self.received_count,
                self.total_fragments
            );
        }

        let total_len: usize = self.fragments.iter().map(|v| v.len()).sum();
        if total_len > max_message_size {
            bail!(
                "Assembled message {} size {} exceeds limit {}",
                self.message_id,
                total_len,
                max_message_size
            );
        }

        let mut full_data = Vec::with_capacity(total_len);
        for frag in self.fragments {
            full_data.extend_from_slice(&frag);
        }

        info!(
            "Message {} assembled from {} fragments, size {} bytes, compressed: {}",
            self.message_id,
            self.total_fragments,
            full_data.len(),
            if self.compressed { "yes" } else { "no" }
        );

        if self.compressed {
            let mut decoder = ZlibDecoder::new(&full_data[..]);
            let mut out = Vec::new();
            decoder.read_to_end(&mut out)?;
            info!(
                "Message {} decompressed: {} -> {} bytes",
                self.message_id,
                full_data.len(),
                out.len()
            );
            Ok(out)
        } else {
            Ok(full_data)
        }
    }

    fn is_expired(&self, timeout: Duration) -> bool {
        self.created_at.elapsed() > timeout
    }
}

#[derive(Debug, Clone)]
pub struct ProtocolConfig {
    pub compression_level: Compression,
    pub assembler_timeout: Duration,
    pub max_total_fragments: u16,
    pub max_message_size: usize,
    pub max_assemblers: usize,
    pub read_timeout: Duration,
    pub completed_timeout: Duration,
    pub max_completed: usize,
    pub keepalive_interval: Option<Duration>,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            compression_level: Compression::default(),
            assembler_timeout: DEFAULT_ASSEMBLER_TIMEOUT,
            max_total_fragments: DEFAULT_MAX_TOTAL_FRAGMENTS,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            max_assemblers: DEFAULT_MAX_ASSEMBLERS,
            read_timeout: DEFAULT_READ_TIMEOUT,
            completed_timeout: DEFAULT_COMPLETED_TIMEOUT,
            max_completed: DEFAULT_MAX_COMPLETED,
            keepalive_interval: Some(Duration::from_secs(30)),
        }
    }
}

pub struct Protocol {
    stream: TcpStream,
    assemblers: HashMap<u64, MessageAssembler>,
    completed: HashMap<u64, Instant>,
    next_id: AtomicU64,
    config: ProtocolConfig,
}

impl Protocol {
    pub async fn new(stream: TcpStream) -> Self {
        let proto = Self {
            stream,
            assemblers: HashMap::new(),
            completed: HashMap::new(),
            next_id: AtomicU64::new(rand::random::<u64>()),
            config: ProtocolConfig::default(),
        };
        proto.apply_keepalive();
        proto
    }

    pub fn with_config(mut self, config: ProtocolConfig) -> Self {
        self.config = config;
        self.apply_keepalive();
        self
    }

    fn apply_keepalive(&self) {
        if let Some(interval) = self.config.keepalive_interval {
            use socket2::{SockRef, TcpKeepalive};
            let sock = SockRef::from(&self.stream);
            let _ = sock.set_keepalive(true);
            let _ = sock.set_tcp_keepalive(&TcpKeepalive::new().with_time(interval));
        }
    }

    fn cleanup(&mut self) {
        let to_remove: Vec<u64> = self.assemblers
            .iter()
            .filter_map(|(id, assembler)| {
                if assembler.is_expired(self.config.assembler_timeout) {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        for id in to_remove {
            self.assemblers.remove(&id);
            warn!("Assembler for message {} removed by timeout", id);
        }

        let to_remove_completed: Vec<u64> = self.completed
            .iter()
            .filter_map(|(id, &time)| {
                if time.elapsed() > self.config.completed_timeout {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        for id in to_remove_completed {
            self.completed.remove(&id);
            debug!("Completed entry for message {} removed by timeout", id);
        }

        if self.assemblers.len() > self.config.max_assemblers {
            let mut entries: Vec<_> = self.assemblers
                .iter()
                .map(|(id, a)| (*id, a.created_at))
                .collect();
            entries.sort_by_key(|(_, t)| *t);
            let to_remove_count = entries.len() - self.config.max_assemblers;
            for (id, _) in entries.into_iter().take(to_remove_count) {
                self.assemblers.remove(&id);
                warn!("Assembler for message {} removed due to limit", id);
            }
        }

        if self.completed.len() > self.config.max_completed {
            let mut entries: Vec<_> = self.completed
                .iter()
                .map(|(id, &t)| (*id, t))
                .collect();
            entries.sort_by_key(|(_, t)| *t);
            let to_remove_count = entries.len() - self.config.max_completed;
            for (id, _) in entries.into_iter().take(to_remove_count) {
                self.completed.remove(&id);
                debug!("Completed entry for message {} removed due to limit", id);
            }
        }
    }

    pub async fn send_message(&mut self, data: &[u8], compress: bool) -> Result<()> {
        let crc = crc32fast::hash(data);
        let mut payload_with_crc = Vec::with_capacity(data.len() + 4);
        payload_with_crc.extend_from_slice(&crc.to_be_bytes());
        payload_with_crc.extend_from_slice(data);

        let msg_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        info!(
            "Sending message {} (size: {} bytes, compression: {})",
            msg_id,
            data.len(),
            if compress { "on" } else { "off" }
        );

        let payload = if compress {
            let mut encoder = ZlibEncoder::new(Vec::new(), self.config.compression_level);
            encoder.write_all(&payload_with_crc)?;
            let compressed = encoder.finish()?;
            if compressed.len() < payload_with_crc.len() {
                info!(
                    "Message {} compressed: {} -> {} bytes",
                    msg_id,
                    payload_with_crc.len(),
                    compressed.len()
                );
                compressed
            } else {
                info!(
                    "Compression not beneficial for message {} ({} -> {}), using original",
                    msg_id,
                    payload_with_crc.len(),
                    compressed.len()
                );
                payload_with_crc
            }
        } else {
            payload_with_crc
        };

        let compressed_actually = compress && payload.len() < data.len() + 4;
        let total_len = payload.len();

        if total_len > self.config.max_message_size + 4 {
            bail!(
                "Message size {} exceeds limit {}",
                total_len,
                self.config.max_message_size + 4
            );
        }

        let total_fragments = if total_len == 0 {
            1
        } else {
            (total_len + MAX_PAYLOAD_SIZE - 1) / MAX_PAYLOAD_SIZE
        } as u16;

        if total_fragments > self.config.max_total_fragments {
            bail!(
                "Fragment count {} exceeds limit {}",
                total_fragments,
                self.config.max_total_fragments
            );
        }

        info!(
            "Message {} split into {} fragments (max payload {} bytes)",
            msg_id,
            total_fragments,
            MAX_PAYLOAD_SIZE
        );

        let flags = if compressed_actually { FLAG_COMPRESSED } else { 0 };

        for idx in 0..total_fragments {
            let start = (idx as usize) * MAX_PAYLOAD_SIZE;
            let end = std::cmp::min(start + MAX_PAYLOAD_SIZE, total_len);
            let frag_data = &payload[start..end];

            let header = FragmentHeader {
                magic: get_magic(),
                flags,
                message_id: msg_id,
                fragment_index: idx,
                total_fragments,
                payload_len: frag_data.len() as u16,
            };
            self.stream.write_all(&header.encode()).await?;
            self.stream.write_all(frag_data).await?;
            debug!(
                "Sent fragment {}/{} for message {} ({} bytes)",
                idx + 1,
                total_fragments,
                msg_id,
                frag_data.len()
            );
        }
        self.stream.flush().await?;
        info!("All fragments for message {} sent", msg_id);
        Ok(())
    }

    async fn read_exact_timeout(&mut self, buf: &mut [u8]) -> Result<()> {
        timeout(self.config.read_timeout, self.stream.read_exact(buf))
            .await
            .map_err(|_| anyhow!("Read timeout"))?
            .map_err(Into::into)
            .map(|_| ())
    }

    async fn receive_raw(&mut self) -> Result<Vec<u8>> {
        loop {
            if self.assemblers.len() > self.config.max_assemblers
                || self.completed.len() > self.config.max_completed
            {
                self.cleanup();
            }

            let mut header_buf = [0u8; HEADER_SIZE];
            self.read_exact_timeout(&mut header_buf).await?;
            let header = FragmentHeader::decode(&header_buf)?;

            if header.payload_len as usize > MAX_PAYLOAD_SIZE {
                bail!("Payload too large: {} > {}", header.payload_len, MAX_PAYLOAD_SIZE);
            }

            let mut payload = vec![0u8; header.payload_len as usize];
            if !payload.is_empty() {
                self.read_exact_timeout(&mut payload).await?;
            }

            let msg_id = header.message_id;
            let compressed = (header.flags & FLAG_COMPRESSED) != 0;

            debug!(
                "Received fragment {}/{} for message {} ({} bytes, compression: {})",
                header.fragment_index + 1,
                header.total_fragments,
                msg_id,
                payload.len(),
                if compressed { "yes" } else { "no" }
            );

            if self.completed.contains_key(&msg_id) {
                debug!("Message {} already completed, ignoring fragment", msg_id);
                continue;
            }

            if header.total_fragments > self.config.max_total_fragments {
                bail!(
                    "Too many fragments {} (limit {})",
                    header.total_fragments,
                    self.config.max_total_fragments
                );
            }

            let assembler = self.assemblers.entry(msg_id).or_insert_with(|| {
                info!(
                    "Created assembler for message {} ({} fragments)",
                    msg_id,
                    header.total_fragments
                );
                MessageAssembler::new(msg_id, header.total_fragments, compressed)
            });

            if assembler.is_expired(self.config.assembler_timeout) {
                self.assemblers.remove(&msg_id);
                bail!(
                    "Assembly timeout for message {} ({} sec)",
                    msg_id,
                    self.config.assembler_timeout.as_secs()
                );
            }

            let complete = assembler.add_fragment(header.fragment_index, payload);
            if complete {
                let assembled = self.assemblers.remove(&msg_id)
                    .ok_or_else(|| anyhow!("Assembler disappeared"))?
                    .assemble(self.config.max_message_size + 4)?;
                self.completed.insert(msg_id, Instant::now());
                info!("Message {} fully assembled", msg_id);

                if assembled.len() < 4 {
                    bail!("Assembled message too short (missing CRC)");
                }
                let crc_bytes = &assembled[0..4];
                let expected_crc = u32::from_be_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
                let data = &assembled[4..];
                let actual_crc = crc32fast::hash(data);
                if actual_crc != expected_crc {
                    bail!(
                        "CRC mismatch: expected {:#x}, got {:#x}",
                        expected_crc,
                        actual_crc
                    );
                }
                info!("CRC check passed for message {}", msg_id);
                return Ok(data.to_vec());
            }
        }
    }

    pub async fn receive_message(&mut self) -> Result<Vec<u8>> {
        self.receive_raw().await
    }
}