use anyhow::{Context, bail};
use bytes::BytesMut;
use clap::{Parser, Subcommand};
use oxide_core::{
    ClaimManyRequest, ClaimRequest, ClearStreamRequest, ReleaseRequest, StreamConfig, SubmitWork,
};
use oxide_server::{OxideConfig, serve};
use oxide_wal::Wal;
use oxide_wire::{Frame, ResponseStatus, encode, try_decode};
use std::path::PathBuf;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "oxide")]
#[command(about = "A durable Rust work-stream engine for backend platforms.")]
struct Cli {
    /// Auth token sent as a Bearer token to protected Oxide servers.
    #[arg(long, global = true, env = "OXIDE_AUTH_TOKEN")]
    auth_token: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start an Oxide server.
    Serve {
        /// Path to a TOML config file.
        #[arg(short, long)]
        config: Option<PathBuf>,
        /// Override the listen address.
        #[arg(long)]
        listen_addr: Option<String>,
        /// Override the data directory.
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Validate a TOML config file.
    ValidateConfig {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Query server health.
    Health {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
    },
    /// Query server stats as JSON.
    Stats {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
    },
    /// Query Prometheus-style metrics text.
    Metrics {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
    },
    /// Create a stream.
    StreamCreate {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
        #[arg(long)]
        name: String,
    },
    /// Inspect stream stats.
    StreamInfo {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
        #[arg(long)]
        name: String,
    },
    /// Clear ready and delayed work from a stream.
    StreamClear {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        include_claimed: bool,
    },
    /// Submit one work item.
    Submit {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
        #[arg(long)]
        stream: String,
        #[arg(long)]
        payload: String,
        #[arg(long, default_value_t = 0)]
        priority: i32,
        #[arg(long)]
        idempotency_key: Option<String>,
        #[arg(long)]
        lane_key: Option<String>,
    },
    /// Claim work with long polling.
    Claim {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
        #[arg(long)]
        stream: String,
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long, default_value = "cli")]
        worker: String,
        #[arg(long, default_value_t = 1)]
        max: usize,
        #[arg(long, default_value_t = 1_000)]
        wait_ms: u64,
    },
    /// Claim work from several streams in priority order.
    ClaimMany {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
        #[arg(long, value_delimiter = ',')]
        streams: Vec<String>,
        #[arg(long, default_value = "default")]
        group: String,
        #[arg(long, default_value = "cli")]
        worker: String,
        #[arg(long, default_value_t = 1)]
        max: usize,
        #[arg(long, default_value_t = 1_000)]
        wait_ms: u64,
    },
    /// Release a claimed work item back to ready or delayed state.
    Release {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
        #[arg(long)]
        claim_id: u64,
        #[arg(long)]
        work_id: u64,
        #[arg(long)]
        fence_token: u64,
        #[arg(long, default_value_t = 0)]
        delay_ms: u64,
        #[arg(long)]
        priority: Option<i32>,
    },
    /// Inspect local WAL segment files.
    WalInspect {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Run a simple protocol throughput benchmark against a live server.
    Bench {
        #[arg(long, default_value = "127.0.0.1:7379")]
        addr: String,
        #[arg(long, default_value = "bench")]
        stream: String,
        #[arg(long, default_value_t = 10_000)]
        total: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let auth_token = cli.auth_token.clone();
    match cli.command {
        Command::Serve {
            config,
            listen_addr,
            data_dir,
        } => {
            let mut config = OxideConfig::load(config.as_deref())?;
            if let Some(listen_addr) = listen_addr {
                config.listen_addr = listen_addr;
            }
            if let Some(data_dir) = data_dir {
                config.data_dir = data_dir;
            }
            if let Some(auth_token) = auth_token {
                config.auth_token = Some(auth_token);
            }
            config.validate()?;
            init_tracing(&config.log_level);
            serve(config).await
        }
        Command::ValidateConfig { config } => {
            let config = OxideConfig::from_path(config)?;
            config.validate()?;
            println!("{}", serde_json::to_string_pretty(&config)?);
            Ok(())
        }
        Command::Health { addr } => {
            print_response(send(addr, Frame::new("HEALTH"), auth_token.as_deref()).await?)
        }
        Command::Stats { addr } => {
            print_response(send(addr, Frame::new("STATS"), auth_token.as_deref()).await?)
        }
        Command::Metrics { addr } => {
            let response = send(addr, Frame::new("METRICS"), auth_token.as_deref()).await?;
            if response
                .headers
                .get("status")
                .is_some_and(|value| value == "error")
            {
                bail!(
                    response
                        .headers
                        .get("error")
                        .cloned()
                        .unwrap_or_else(|| "server error".to_owned())
                );
            }
            println!("{}", String::from_utf8_lossy(&response.payload));
            Ok(())
        }
        Command::StreamCreate { addr, name } => {
            let frame = Frame::new("STREAM.CREATE")
                .with_payload(serde_json::to_vec(&StreamConfig::new(name))?);
            print_response(send(addr, frame, auth_token.as_deref()).await?)
        }
        Command::StreamInfo { addr, name } => {
            let frame = Frame::new("STREAM.INFO").with_header("stream", name);
            print_response(send(addr, frame, auth_token.as_deref()).await?)
        }
        Command::StreamClear {
            addr,
            name,
            include_claimed,
        } => {
            let request = ClearStreamRequest {
                stream: name,
                include_claimed,
            };
            let frame = Frame::new("STREAM.CLEAR").with_payload(serde_json::to_vec(&request)?);
            print_response(send(addr, frame, auth_token.as_deref()).await?)
        }
        Command::Submit {
            addr,
            stream,
            payload,
            priority,
            idempotency_key,
            lane_key,
        } => {
            let mut work = SubmitWork::new(stream, payload.into_bytes());
            work.priority = priority;
            work.idempotency_key = idempotency_key;
            work.lane_key = lane_key;
            let frame = Frame::new("WORK.SUBMIT").with_payload(serde_json::to_vec(&work)?);
            print_response(send(addr, frame, auth_token.as_deref()).await?)
        }
        Command::Claim {
            addr,
            stream,
            group,
            worker,
            max,
            wait_ms,
        } => {
            let request = ClaimRequest {
                stream,
                consumer_group: group,
                worker_id: worker,
                max,
                lease_ms: 30_000,
            };
            let frame = Frame::new("WORK.CLAIM")
                .with_header("wait_ms", wait_ms.to_string())
                .with_payload(serde_json::to_vec(&request)?);
            print_response(send(addr, frame, auth_token.as_deref()).await?)
        }
        Command::ClaimMany {
            addr,
            streams,
            group,
            worker,
            max,
            wait_ms,
        } => {
            let request = ClaimManyRequest {
                streams,
                consumer_group: group,
                worker_id: worker,
                max,
                lease_ms: 30_000,
            };
            let frame = Frame::new("WORK.CLAIM_MANY")
                .with_header("wait_ms", wait_ms.to_string())
                .with_payload(serde_json::to_vec(&request)?);
            print_response(send(addr, frame, auth_token.as_deref()).await?)
        }
        Command::Release {
            addr,
            claim_id,
            work_id,
            fence_token,
            delay_ms,
            priority,
        } => {
            let request = ReleaseRequest {
                claim_id,
                work_id,
                fence_token,
                delay_ms,
                priority,
            };
            let frame = Frame::new("WORK.RELEASE").with_payload(serde_json::to_vec(&request)?);
            print_response(send(addr, frame, auth_token.as_deref()).await?)
        }
        Command::WalInspect { data_dir } => {
            let segments = Wal::inspect(data_dir.join("wal"))?;
            println!("{}", serde_json::to_string_pretty(&segments)?);
            Ok(())
        }
        Command::Bench {
            addr,
            stream,
            total,
        } => run_bench(addr, stream, total, auth_token.as_deref()).await,
    }
}

fn init_tracing(log_level: &str) {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

async fn send(addr: String, frame: Frame, auth_token: Option<&str>) -> anyhow::Result<Frame> {
    let mut stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("connect {addr}"))?;
    let frame = with_auth(frame, auth_token);
    let encoded = encode(&frame)?;
    stream.write_all(&encoded).await?;
    let mut buffer = BytesMut::with_capacity(8 * 1024);
    loop {
        let read = stream.read_buf(&mut buffer).await?;
        if read == 0 {
            bail!("server closed connection before response");
        }
        if let Some(frame) = try_decode(&mut buffer)? {
            return Ok(frame);
        }
    }
}

fn with_auth(frame: Frame, auth_token: Option<&str>) -> Frame {
    if let Some(auth_token) = auth_token {
        frame.with_header("authorization", format!("Bearer {auth_token}"))
    } else {
        frame
    }
}

fn print_response(response: Frame) -> anyhow::Result<()> {
    if response
        .headers
        .get("status")
        .is_some_and(|value| value == "error")
    {
        bail!(
            response
                .headers
                .get("error")
                .cloned()
                .unwrap_or_else(|| "server error".to_owned())
        );
    }
    if response
        .headers
        .get("status")
        .is_some_and(|value| value == ResponseStatus::Empty.as_str())
    {
        println!("[]");
        return Ok(());
    }
    if response.payload.is_empty() {
        println!("{}", serde_json::to_string_pretty(&response.headers)?);
    } else if response
        .headers
        .get("content-type")
        .is_some_and(|value| value == "application/json")
    {
        let value: serde_json::Value = serde_json::from_slice(&response.payload)?;
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        println!("{}", String::from_utf8_lossy(&response.payload));
    }
    Ok(())
}

async fn run_bench(
    addr: String,
    stream: String,
    total: usize,
    auth_token: Option<&str>,
) -> anyhow::Result<()> {
    let _ = send(
        addr.clone(),
        Frame::new("STREAM.CREATE").with_payload(serde_json::to_vec(&StreamConfig::new(&stream))?),
        auth_token,
    )
    .await;

    let started = Instant::now();
    for i in 0..total {
        let mut work = SubmitWork::new(&stream, format!("payload-{i}").into_bytes());
        work.idempotency_key = Some(format!("bench-{i}"));
        let response = send(
            addr.clone(),
            Frame::new("WORK.SUBMIT").with_payload(serde_json::to_vec(&work)?),
            auth_token,
        )
        .await?;
        if response
            .headers
            .get("status")
            .is_some_and(|value| value == "error")
        {
            bail!(
                response
                    .headers
                    .get("error")
                    .cloned()
                    .unwrap_or_else(|| "server error".to_owned())
            );
        }
    }
    let elapsed = started.elapsed();
    let ops = total as f64 / elapsed.as_secs_f64();
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "submitted": total,
            "elapsed_ms": elapsed.as_millis(),
            "ops_per_sec": ops
        }))?
    );
    Ok(())
}
