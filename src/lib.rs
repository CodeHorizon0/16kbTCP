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
pub const HEADER_SIZE: usize = 13;
pub const MAX_PAYLOAD_SIZE: usize = MAX_PACKET_SIZE - HEADER_SIZE;

pub const FLAG_COMPRESSED: u8 = 0x01;
pub const FLAG_ACK: u8 = 0x02;

const DEFAULT_MAX_TOTAL_FRAGMENTS: u16 = 1024;
const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_COMPLETED_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_ASSEMBLER_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CLEANUP_INTERVAL: usize = 100;

// Returns the dynamic magic number, generating it once.
fn get_magic() -> u16 {
    static MAGIC: OnceLock<u16> = OnceLock::new();
    *MAGIC.get_or_init(|| rand::random::<u16>() | 1)
}

// Fragment header structure.
#[derive(Debug, Clone, Copy)]
pub struct FragmentHeader {
    pub magic: u16,
    pub flags: u8,
    pub message_id: u32,
    pub fragment_index: u16,
    pub total_fragments: u16,
    pub payload_len: u16,
}

impl FragmentHeader {
    // Encodes header to bytes.
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..2].copy_from_slice(&self.magic.to_be_bytes());
        buf[2] = self.flags;
        buf[3..7].copy_from_slice(&self.message_id.to_be_bytes());
        buf[7..9].copy_from_slice(&self.fragment_index.to_be_bytes());
        buf[9..11].copy_from_slice(&self.total_fragments.to_be_bytes());
        buf[11..13].copy_from_slice(&self.payload_len.to_be_bytes());
        buf
    }

    // Decodes header from bytes.
    pub fn decode(buf: &[u8; HEADER_SIZE]) -> Result<Self> {
        let magic = u16::from_be_bytes([buf[0], buf[1]]);
        let expected = get_magic();
        if magic != expected {
            bail!("Invalid magic number: {:#x} (expected {:#x})", magic, expected);
        }
        Ok(Self {
            magic,
            flags: buf[2],
            message_id: u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]),
            fragment_index: u16::from_be_bytes([buf[7], buf[8]]),
            total_fragments: u16::from_be_bytes([buf[9], buf[10]]),
            payload_len: u16::from_be_bytes([buf[11], buf[12]]),
        })
    }

    // Returns true if this is an ACK packet.
    pub fn is_ack(&self) -> bool {
        (self.flags & FLAG_ACK) != 0
    }
}

// MessageAssembler collects fragments until complete.
struct MessageAssembler {
    message_id: u32,
    total_fragments: u16,
    fragments: Vec<Vec<u8>>,
    received: Vec<bool>,
    received_count: usize,
    compressed: bool,
    created_at: Instant,
}

impl MessageAssembler {
    // Creates a new assembler.
    fn new(message_id: u32, total_fragments: u16, compressed: bool) -> Self {
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

    // Adds a fragment, returns true if complete.
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

    // Assembles all fragments into a message.
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

    // Checks if assembler expired.
    fn is_expired(&self, timeout: Duration) -> bool {
        self.created_at.elapsed() > timeout
    }
}

// Received types.
#[derive(Debug)]
pub enum Received {
    Message(Vec<u8>),
    Ack(u32),
}

// Retry configuration.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub timeout: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            timeout: Duration::from_secs(5),
        }
    }
}

// Protocol configuration.
#[derive(Debug, Clone)]
pub struct ProtocolConfig {
    pub retry_config: RetryConfig,
    pub compression_level: Compression,
    pub assembler_timeout: Duration,
    pub completed_timeout: Duration,
    pub max_total_fragments: u16,
    pub max_message_size: usize,
    pub cleanup_threshold: usize,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            retry_config: RetryConfig::default(),
            compression_level: Compression::default(),
            assembler_timeout: DEFAULT_ASSEMBLER_TIMEOUT,
            completed_timeout: DEFAULT_COMPLETED_TIMEOUT,
            max_total_fragments: DEFAULT_MAX_TOTAL_FRAGMENTS,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            cleanup_threshold: DEFAULT_CLEANUP_INTERVAL,
        }
    }
}

// Protocol instance.
pub struct Protocol {
    stream: TcpStream,
    assemblers: HashMap<u32, MessageAssembler>,
    completed: HashMap<u32, Instant>,
    next_id: AtomicU64,
    config: ProtocolConfig,
}

impl Protocol {
    // Creates a new protocol instance.
    pub async fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            assemblers: HashMap::new(),
            completed: HashMap::new(),
            next_id: AtomicU64::new(1),
            config: ProtocolConfig::default(),
        }
    }

    // Sets configuration.
    pub fn with_config(mut self, config: ProtocolConfig) -> Self {
        self.config = config;
        self
    }

    // Cleans up expired entries.
    fn cleanup(&mut self) {
        let to_remove: Vec<u32> = self.completed
            .iter()
            .filter_map(|(id, &time)| {
                if time.elapsed() > self.config.completed_timeout {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        for id in to_remove {
            self.completed.remove(&id);
            debug!("Cleaned completed for message {}", id);
        }

        let to_remove_assemblers: Vec<u32> = self.assemblers
            .iter()
            .filter_map(|(id, assembler)| {
                if assembler.is_expired(self.config.assembler_timeout) {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        for id in to_remove_assemblers {
            self.assemblers.remove(&id);
            warn!("Assembler for message {} removed by timeout", id);
        }
    }

    // Sends an ACK for a message.
    async fn send_ack(&mut self, msg_id: u32) -> Result<()> {
        let header = FragmentHeader {
            magic: get_magic(),
            flags: FLAG_ACK,
            message_id: msg_id,
            fragment_index: 0,
            total_fragments: 0,
            payload_len: 0,
        };
        self.stream.write_all(&header.encode()).await?;
        self.stream.flush().await?;
        debug!("Sent ACK for message {}", msg_id);
        Ok(())
    }

    // Sends a complete message, possibly fragmented.
    pub async fn send_message(&mut self, data: &[u8], compress: bool) -> Result<()> {
        let msg_id = self.next_id.fetch_add(1, Ordering::SeqCst) as u32;
        info!(
            "Sending message {} (size: {} bytes, compression: {})",
            msg_id,
            data.len(),
            if compress { "on" } else { "off" }
        );

        let start_time = Instant::now();

        // Optionally compress
        let payload = if compress {
            let mut encoder = ZlibEncoder::new(Vec::new(), self.config.compression_level);
            encoder.write_all(data)?;
            let compressed = encoder.finish()?;
            if compressed.len() < data.len() {
                info!(
                    "Message {} compressed: {} -> {} bytes",
                    msg_id,
                    data.len(),
                    compressed.len()
                );
                compressed
            } else {
                info!(
                    "Compression not beneficial for message {} ({} -> {}), using original",
                    msg_id,
                    data.len(),
                    compressed.len()
                );
                data.to_vec()
            }
        } else {
            data.to_vec()
        };

        let compressed_actually = compress && payload.len() < data.len();
        let total_len = payload.len();

        if total_len > self.config.max_message_size {
            bail!(
                "Message size {} exceeds limit {}",
                total_len,
                self.config.max_message_size
            );
        }

        // Split into fragments
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

        let fragments: Vec<Vec<u8>> = (0..total_fragments)
            .map(|idx| {
                let start = (idx as usize) * MAX_PAYLOAD_SIZE;
                let end = std::cmp::min(start + MAX_PAYLOAD_SIZE, total_len);
                payload[start..end].to_vec()
            })
            .collect();

        // Retry loop waiting for ACK
        for attempt in 0..self.config.retry_config.max_retries {
            info!(
                "Sending message {} (attempt {}/{})",
                msg_id,
                attempt + 1,
                self.config.retry_config.max_retries
            );

            for (idx, frag_data) in fragments.iter().enumerate() {
                let header = FragmentHeader {
                    magic: get_magic(),
                    flags,
                    message_id: msg_id,
                    fragment_index: idx as u16,
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
            info!("All fragments for message {} sent, waiting for ACK", msg_id);

            match timeout(self.config.retry_config.timeout, self.receive_raw()).await {
                Ok(Ok(Received::Ack(id))) if id == msg_id => {
                    info!("Received ACK for message {} in {:?}", msg_id, start_time.elapsed());
                    return Ok(());
                }
                Ok(Ok(Received::Ack(id))) => {
                    bail!("Received ACK for different message {}", id);
                }
                Ok(Ok(Received::Message(_))) => {
                    bail!("Received data instead of ACK");
                }
                Ok(Err(e)) => {
                    if Self::is_connection_closed(&e) {
                        bail!("Connection closed while waiting for ACK: {}", e);
                    }
                    warn!("Error waiting for ACK (attempt {}): {}", attempt + 1, e);
                }
                Err(_) => {
                    warn!("Timeout waiting for ACK (attempt {})", attempt + 1);
                }
            }

            if attempt == self.config.retry_config.max_retries - 1 {
                bail!("Failed to get ACK after {} attempts", self.config.retry_config.max_retries);
            }
        }

        unreachable!()
    }

    // Checks if error indicates connection closed.
    fn is_connection_closed(e: &anyhow::Error) -> bool {
        let msg = e.to_string();
        msg.contains("early eof") || msg.contains("read_exact") || msg.contains("Connection reset")
    }

    // Receives raw packets and assembles if needed.
    async fn receive_raw(&mut self) -> Result<Received> {
        loop {
            if self.assemblers.len() > self.config.cleanup_threshold
                || self.completed.len() > self.config.cleanup_threshold
            {
                self.cleanup();
            }

            let mut header_buf = [0u8; HEADER_SIZE];
            if let Err(e) = self.stream.read_exact(&mut header_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    bail!("Connection closed (early eof)");
                }
                return Err(e.into());
            }
            let header = FragmentHeader::decode(&header_buf)?;

            // ACK packet
            if header.is_ack() {
                if header.payload_len > 0 {
                    let mut junk = vec![0u8; header.payload_len as usize];
                    self.stream.read_exact(&mut junk).await?;
                }
                debug!("Received ACK packet for message {}", header.message_id);
                return Ok(Received::Ack(header.message_id));
            }

            if header.payload_len as usize > MAX_PAYLOAD_SIZE {
                bail!("Payload too large: {} > {}", header.payload_len, MAX_PAYLOAD_SIZE);
            }

            let mut payload = vec![0u8; header.payload_len as usize];
            if !payload.is_empty() {
                if let Err(e) = self.stream.read_exact(&mut payload).await {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        bail!("Connection closed while reading payload (early eof)");
                    }
                    return Err(e.into());
                }
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

            // Already completed – just ACK and ignore
            if self.completed.contains_key(&msg_id) {
                debug!("Message {} already completed, sending ACK and ignoring", msg_id);
                self.send_ack(msg_id).await?;
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
                    .assemble(self.config.max_message_size)?;
                self.send_ack(msg_id).await?;
                self.completed.insert(msg_id, Instant::now());
                info!("Message {} fully assembled and acknowledged", msg_id);
                return Ok(Received::Message(assembled));
            }
        }
    }

    // Public receive function – returns a complete message.
    pub async fn receive_message(&mut self) -> Result<Vec<u8>> {
        loop {
            match self.receive_raw().await? {
                Received::Message(data) => return Ok(data),
                Received::Ack(_) => continue,
            }
        }
    }
}