use std::time::Duration;

use anyhow::Result;
use log::{info, error};

use protocol::*;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let server_handle = tokio::spawn(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await.unwrap();
        info!("Server started on 127.0.0.1:8080");

        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            info!("Connection from {}", addr);
            tokio::spawn(async move {
                let config = ProtocolConfig::default();
                let mut protocol = Protocol::new(stream).await.with_config(config);
                loop {
                    match protocol.receive_message().await {
                        Ok(data) => {
                            info!("Server received message of {} bytes", data.len());
                            if let Err(e) = protocol.send_message(&data, true).await {
                                error!("Echo send error: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            if e.to_string().contains("early eof") || e.to_string().contains("read_exact") {
                                info!("Client closed connection");
                            } else {
                                error!("Server receive error: {}", e);
                            }
                            break;
                        }
                    }
                }
            });
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    info!("Client connecting...");
    let stream = tokio::net::TcpStream::connect("127.0.0.1:8080").await?;
    let config = ProtocolConfig::default();
    let mut client = Protocol::new(stream).await.with_config(config);

    let large_data = vec![b'X'; 30_000];
    info!("Client: sending {} bytes with compression enabled", large_data.len());

    client.send_message(&large_data, true).await?;
    info!("Client: message sent");

    let response = client.receive_message().await?;
    info!("Client: received response of {} bytes", response.len());

    if large_data == response {
        info!("Client: data matches, test passed!");
    } else {
        error!(
            "Client: data mismatch! Expected {} bytes, got {}",
            large_data.len(),
            response.len()
        );
        return Err(anyhow::anyhow!("Data mismatch"));
    }

    server_handle.abort();
    let _ = server_handle.await;
    Ok(())
}
