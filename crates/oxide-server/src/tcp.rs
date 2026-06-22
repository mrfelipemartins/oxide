use crate::{OxideConfig, OxideRuntime};
use anyhow::Context;
use bytes::BytesMut;
use oxide_wire::{encode, try_decode_with_max};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

pub async fn serve(config: OxideConfig) -> anyhow::Result<()> {
    let runtime = OxideRuntime::open(config.clone()).context("open runtime")?;
    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .with_context(|| format!("bind {}", config.listen_addr))?;
    info!(listen_addr = %config.listen_addr, "oxide server listening");

    loop {
        tokio::select! {
            incoming = listener.accept() => {
                let (stream, peer_addr) = incoming.context("accept connection")?;
                let runtime = runtime.clone();
                let max_frame_bytes = config.max_frame_bytes;
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, runtime, max_frame_bytes).await {
                        warn!(%peer_addr, error = %err, "connection closed with error");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("listen for shutdown signal")?;
                info!("shutdown signal received");
                runtime.flush().await.context("flush wal")?;
                break;
            }
        }
    }
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    runtime: OxideRuntime,
    max_frame_bytes: usize,
) -> anyhow::Result<()> {
    let mut buffer = BytesMut::with_capacity(8 * 1024);
    loop {
        let read = stream.read_buf(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        loop {
            let frame = match try_decode_with_max(&mut buffer, max_frame_bytes) {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(err) => {
                    error!(error = %err, "wire decode error");
                    return Err(err.into());
                }
            };
            let response = runtime.handle_frame(frame).await;
            let encoded = encode(&response)?;
            stream.write_all(&encoded).await?;
        }
    }
}
