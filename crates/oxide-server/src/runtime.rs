use crate::OxideConfig;
use bytes::Bytes;
use oxide_core::{
    ClaimManyRequest, ClaimRequest, ClearStreamRequest, ClearStreamResult, CompleteRequest,
    CompletionRecord, Engine, EngineError, EngineEvent, EngineResult, ExtendRequest, FailRequest,
    ReleaseRequest, StreamConfig, SubmitWork, WorkId, now_ms,
};
use oxide_wal::{Wal, WalError};
use oxide_wire::{Frame, ResponseStatus, WireError, error_frame, json_frame};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("{0}")]
    Engine(#[from] EngineError),
    #[error("{0}")]
    Wal(#[from] WalError),
    #[error("{0}")]
    Wire(#[from] WireError),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("unauthorized")]
    Unauthorized,
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, Clone)]
pub struct OxideRuntime {
    config: OxideConfig,
    engine: Arc<Mutex<Engine>>,
    wal: Arc<Mutex<Wal>>,
    started_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: &'static str,
    pub now_ms: u64,
    pub started_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub engine: oxide_core::EngineStats,
    pub uptime_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CancelRequest {
    work_id: WorkId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StreamConfigResponse {
    stream: StreamConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkSubmitResponse {
    work_id: WorkId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkSubmitBatchRequest {
    items: Vec<SubmitWork>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkSubmitBatchResponse {
    count: usize,
    work_ids: Vec<WorkId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkAckBatchRequest {
    items: Vec<CompleteRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkAckBatchResponse {
    count: usize,
    records: Vec<CompletionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkIdResponse {
    work_id: WorkId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkFailResponse {
    work_id: WorkId,
    retry_at_ms: Option<u64>,
    reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkReleaseResponse {
    work_id: WorkId,
    available_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StreamPauseResponse {
    stream: String,
    until_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StreamResumeResponse {
    stream: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClaimResponse {
    count: usize,
    items: Vec<oxide_core::ClaimedWork>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClearStreamResponse {
    stream: String,
    removed_ready: usize,
    removed_scheduled: usize,
    removed_claimed: usize,
    total_removed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PauseRequest {
    stream: String,
    duration_ms: Option<u64>,
    until_ms: Option<u64>,
}

impl OxideRuntime {
    pub fn open(config: OxideConfig) -> RuntimeResult<Self> {
        std::fs::create_dir_all(&config.data_dir)?;
        // Open before replaying: opening truncates a torn trailing record, so
        // replay and later appends both see the same clean log.
        let wal = Wal::open(config.wal_config())?;
        if wal.truncated_tail_bytes() > 0 {
            tracing::warn!(
                bytes = wal.truncated_tail_bytes(),
                "truncated torn record at the end of the WAL (unclean shutdown)"
            );
        }
        let mut engine = Engine::new();
        for event in Wal::replay(config.wal_dir())? {
            engine.apply_event(event)?;
        }
        Ok(Self {
            config,
            engine: Arc::new(Mutex::new(engine)),
            wal: Arc::new(Mutex::new(wal)),
            started_at_ms: now_ms(),
        })
    }

    pub fn config(&self) -> &OxideConfig {
        &self.config
    }

    pub async fn handle_frame(&self, frame: Frame) -> Frame {
        match self.handle_frame_inner(frame.clone()).await {
            Ok(response) => response,
            Err(err) => error_frame(frame.request_id, frame.op, err.to_string()),
        }
    }

    pub async fn flush(&self) -> RuntimeResult<()> {
        self.wal.lock().await.flush()?;
        Ok(())
    }

    pub async fn health(&self) -> HealthStatus {
        HealthStatus {
            status: "ok",
            now_ms: now_ms(),
            started_at_ms: self.started_at_ms,
        }
    }

    pub async fn metrics(&self) -> MetricsSnapshot {
        let engine = self.engine.lock().await;
        MetricsSnapshot {
            engine: engine.stats(),
            uptime_ms: now_ms().saturating_sub(self.started_at_ms),
        }
    }

    pub async fn metrics_text(&self) -> String {
        let metrics = self.metrics().await;
        format!(
            concat!(
                "# TYPE oxide_streams gauge\n",
                "oxide_streams {}\n",
                "# TYPE oxide_work_total gauge\n",
                "oxide_work_total {}\n",
                "# TYPE oxide_work_ready gauge\n",
                "oxide_work_ready {}\n",
                "# TYPE oxide_work_claimed gauge\n",
                "oxide_work_claimed {}\n",
                "# TYPE oxide_live_claims gauge\n",
                "oxide_live_claims {}\n",
                "# TYPE oxide_uptime_ms counter\n",
                "oxide_uptime_ms {}\n"
            ),
            metrics.engine.streams,
            metrics.engine.total_work,
            metrics.engine.ready,
            metrics.engine.claimed,
            metrics.engine.live_claims,
            metrics.uptime_ms
        )
    }

    async fn handle_frame_inner(&self, frame: Frame) -> RuntimeResult<Frame> {
        self.authorize(&frame)?;
        match frame.op.as_str() {
            "HEALTH" => json_frame(
                frame.request_id,
                "HEALTH.OK",
                ResponseStatus::Ok,
                &self.health().await,
            )
            .map_err(RuntimeError::from),
            "STATS" => {
                let metrics = self.metrics().await;
                json_frame(frame.request_id, "STATS.OK", ResponseStatus::Ok, &metrics)
                    .map_err(RuntimeError::from)
            }
            "METRICS" => Ok(Frame::new("METRICS.OK")
                .with_request_id(frame.request_id)
                .with_header("status", ResponseStatus::Ok.as_str())
                .with_header("content-type", "text/plain; version=0.0.4")
                .with_payload(Bytes::from(self.metrics_text().await))),
            "STREAM.CREATE" => {
                let config: StreamConfig = frame.payload_json()?;
                let event = self.mutate(|engine| engine.create_stream(config)).await?;
                let stream = match event {
                    EngineEvent::StreamCreated(stream) => stream,
                    _ => unreachable!("create_stream returns StreamCreated"),
                };
                json_frame(
                    frame.request_id,
                    "STREAM.CREATE.OK",
                    ResponseStatus::Ok,
                    &StreamConfigResponse { stream },
                )
                .map_err(RuntimeError::from)
            }
            "STREAM.UPDATE" => {
                let config: StreamConfig = frame.payload_json()?;
                let event = self.mutate(|engine| engine.update_stream(config)).await?;
                let stream = match event {
                    EngineEvent::StreamUpdated(stream) => stream,
                    _ => unreachable!("update_stream returns StreamUpdated"),
                };
                json_frame(
                    frame.request_id,
                    "STREAM.UPDATE.OK",
                    ResponseStatus::Ok,
                    &StreamConfigResponse { stream },
                )
                .map_err(RuntimeError::from)
            }
            "STREAM.INFO" => {
                let stream = required_header(&frame, "stream")?;
                let engine = self.engine.lock().await;
                let stats = engine.stream_stats(stream)?;
                json_frame(
                    frame.request_id,
                    "STREAM.INFO.OK",
                    ResponseStatus::Ok,
                    &stats,
                )
                .map_err(RuntimeError::from)
            }
            "STREAM.CLEAR" | "QUEUE.CLEAR" => {
                let request: ClearStreamRequest = frame.payload_json()?;
                let (_, result) = self
                    .mutate_with(|engine| {
                        engine.clear_stream(request.stream, request.include_claimed)
                    })
                    .await?;
                json_frame(
                    frame.request_id,
                    "STREAM.CLEAR.OK",
                    ResponseStatus::Ok,
                    &clear_stream_response(result),
                )
                .map_err(RuntimeError::from)
            }
            "WORK.SUBMIT" => {
                let submit = frame.payload_json()?;
                let work_id = self.submit_work(submit).await?;
                json_frame(
                    frame.request_id,
                    "WORK.SUBMIT.OK",
                    ResponseStatus::Ok,
                    &WorkSubmitResponse { work_id },
                )
                .map_err(RuntimeError::from)
            }
            "WORK.SUBMIT_BATCH" => {
                let request: WorkSubmitBatchRequest = frame.payload_json()?;
                let work_ids = self.submit_work_batch(request.items).await?;
                json_frame(
                    frame.request_id,
                    "WORK.SUBMIT_BATCH.OK",
                    ResponseStatus::Ok,
                    &WorkSubmitBatchResponse {
                        count: work_ids.len(),
                        work_ids,
                    },
                )
                .map_err(RuntimeError::from)
            }
            "WORK.CLAIM" => {
                let request: ClaimRequest = frame.payload_json()?;
                let wait_ms = frame
                    .headers
                    .get("wait_ms")
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(self.config.default_claim_wait_ms);
                let claimed = self
                    .claim_with_wait(request.into(), Duration::from_millis(wait_ms))
                    .await?;
                let status = if claimed.is_empty() {
                    ResponseStatus::Empty
                } else {
                    ResponseStatus::Ok
                };
                json_frame(
                    frame.request_id,
                    "WORK.CLAIM.OK",
                    status,
                    &ClaimResponse {
                        count: claimed.len(),
                        items: claimed,
                    },
                )
                .map_err(RuntimeError::from)
            }
            "WORK.CLAIM_MANY" => {
                let request: ClaimManyRequest = frame.payload_json()?;
                let wait_ms = frame
                    .headers
                    .get("wait_ms")
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(self.config.default_claim_wait_ms);
                let claimed = self
                    .claim_with_wait(request, Duration::from_millis(wait_ms))
                    .await?;
                let status = if claimed.is_empty() {
                    ResponseStatus::Empty
                } else {
                    ResponseStatus::Ok
                };
                json_frame(
                    frame.request_id,
                    "WORK.CLAIM_MANY.OK",
                    status,
                    &ClaimResponse {
                        count: claimed.len(),
                        items: claimed,
                    },
                )
                .map_err(RuntimeError::from)
            }
            "WORK.ACK" => {
                let request: CompleteRequest = frame.payload_json()?;
                let record = self.ack_work(request).await?;
                json_frame(frame.request_id, "WORK.ACK.OK", ResponseStatus::Ok, &record)
                    .map_err(RuntimeError::from)
            }
            "WORK.ACK_BATCH" => {
                let request: WorkAckBatchRequest = frame.payload_json()?;
                let records = self.ack_work_batch(request.items).await?;
                json_frame(
                    frame.request_id,
                    "WORK.ACK_BATCH.OK",
                    ResponseStatus::Ok,
                    &WorkAckBatchResponse {
                        count: records.len(),
                        records,
                    },
                )
                .map_err(RuntimeError::from)
            }
            "WORK.FAIL" => {
                let request: FailRequest = frame.payload_json()?;
                let event = self.mutate(|engine| engine.fail(request)).await?;
                let response = match event {
                    EngineEvent::WorkFailed {
                        work_id,
                        retry_at_ms,
                        reason,
                    } => WorkFailResponse {
                        work_id,
                        retry_at_ms,
                        reason,
                    },
                    _ => unreachable!("fail returns WorkFailed"),
                };
                json_frame(
                    frame.request_id,
                    "WORK.FAIL.OK",
                    ResponseStatus::Ok,
                    &response,
                )
                .map_err(RuntimeError::from)
            }
            "WORK.RELEASE" => {
                let request: ReleaseRequest = frame.payload_json()?;
                let event = self.mutate(|engine| engine.release(request)).await?;
                let response = match event {
                    EngineEvent::WorkReleased {
                        work_id,
                        available_at_ms,
                        ..
                    } => WorkReleaseResponse {
                        work_id,
                        available_at_ms,
                    },
                    _ => unreachable!("release returns WorkReleased"),
                };
                json_frame(
                    frame.request_id,
                    "WORK.RELEASE.OK",
                    ResponseStatus::Ok,
                    &response,
                )
                .map_err(RuntimeError::from)
            }
            "WORK.EXTEND" => {
                let request: ExtendRequest = frame.payload_json()?;
                let event = self.mutate(|engine| engine.extend(request)).await?;
                let claim = match event {
                    EngineEvent::WorkExtended(claim) => claim,
                    _ => unreachable!("extend returns WorkExtended"),
                };
                json_frame(
                    frame.request_id,
                    "WORK.EXTEND.OK",
                    ResponseStatus::Ok,
                    &claim,
                )
                .map_err(RuntimeError::from)
            }
            "WORK.CANCEL" => {
                let request: CancelRequest = frame.payload_json()?;
                let event = self.mutate(|engine| engine.cancel(request.work_id)).await?;
                let work_id = match event {
                    EngineEvent::WorkCancelled { work_id } => work_id,
                    _ => unreachable!("cancel returns WorkCancelled"),
                };
                json_frame(
                    frame.request_id,
                    "WORK.CANCEL.OK",
                    ResponseStatus::Ok,
                    &WorkIdResponse { work_id },
                )
                .map_err(RuntimeError::from)
            }
            "WORK.INSPECT" => {
                let work_id = required_header(&frame, "work_id")?
                    .parse::<WorkId>()
                    .map_err(|_| {
                        RuntimeError::InvalidRequest("work_id must be a u64".to_owned())
                    })?;
                let engine = self.engine.lock().await;
                let work = engine.inspect_work(work_id)?;
                json_frame(
                    frame.request_id,
                    "WORK.INSPECT.OK",
                    ResponseStatus::Ok,
                    &work,
                )
                .map_err(RuntimeError::from)
            }
            "GROUP.INFO" => {
                let group = required_header(&frame, "group")?;
                json_frame(
                    frame.request_id,
                    "GROUP.INFO.OK",
                    ResponseStatus::Ok,
                    &json!({ "group": group, "mode": "pull", "state": "stateless_v1" }),
                )
                .map_err(RuntimeError::from)
            }
            "PAUSE" => {
                let request: PauseRequest = frame.payload_json()?;
                let until_ms = request.until_ms.unwrap_or_else(|| {
                    now_ms().saturating_add(
                        request
                            .duration_ms
                            .unwrap_or(self.config.default_claim_wait_ms),
                    )
                });
                let event = self
                    .mutate(|engine| engine.pause_stream(request.stream, until_ms))
                    .await?;
                let response = match event {
                    EngineEvent::StreamPaused { stream, until_ms } => {
                        StreamPauseResponse { stream, until_ms }
                    }
                    _ => unreachable!("pause_stream returns StreamPaused"),
                };
                json_frame(frame.request_id, "PAUSE.OK", ResponseStatus::Ok, &response)
                    .map_err(RuntimeError::from)
            }
            "RESUME" => {
                let stream = required_header(&frame, "stream")?.to_owned();
                let event = self.mutate(|engine| engine.resume_stream(stream)).await?;
                let response = match event {
                    EngineEvent::StreamResumed { stream } => StreamResumeResponse { stream },
                    _ => unreachable!("resume_stream returns StreamResumed"),
                };
                json_frame(frame.request_id, "RESUME.OK", ResponseStatus::Ok, &response)
                    .map_err(RuntimeError::from)
            }
            "DRAIN" => {
                self.flush().await?;
                let metrics = self.metrics().await;
                json_frame(frame.request_id, "DRAIN.OK", ResponseStatus::Ok, &metrics)
                    .map_err(RuntimeError::from)
            }
            op => Err(RuntimeError::InvalidRequest(format!("unknown op: {op}"))),
        }
    }

    async fn submit_work(&self, submit: SubmitWork) -> RuntimeResult<WorkId> {
        let mut engine = self.engine.lock().await;
        let (event, work_id) = engine.prepare_submit(submit)?;
        self.wal.lock().await.append(&event)?;
        engine.apply_event(event)?;

        Ok(work_id)
    }

    async fn submit_work_batch(&self, items: Vec<SubmitWork>) -> RuntimeResult<Vec<WorkId>> {
        let mut engine = self.engine.lock().await;
        let mut wal = self.wal.lock().await;
        let mut work_ids = Vec::with_capacity(items.len());

        for submit in items {
            let (event, work_id) = engine.prepare_submit(submit)?;
            wal.append(&event)?;
            engine.apply_event(event)?;
            work_ids.push(work_id);
        }

        Ok(work_ids)
    }

    async fn ack_work(&self, request: CompleteRequest) -> RuntimeResult<CompletionRecord> {
        let mut engine = self.engine.lock().await;
        let event = engine.prepare_ack(request)?;
        self.wal.lock().await.append(&event)?;
        engine.apply_event(event.clone())?;

        match event {
            EngineEvent::WorkAcknowledged(record) => Ok(record),
            _ => unreachable!("prepare_ack returns WorkAcknowledged"),
        }
    }

    async fn ack_work_batch(
        &self,
        items: Vec<CompleteRequest>,
    ) -> RuntimeResult<Vec<CompletionRecord>> {
        let mut engine = self.engine.lock().await;
        let mut wal = self.wal.lock().await;
        let mut records = Vec::with_capacity(items.len());

        for request in items {
            let event = engine.prepare_ack(request)?;
            wal.append(&event)?;
            engine.apply_event(event.clone())?;
            match event {
                EngineEvent::WorkAcknowledged(record) => records.push(record),
                _ => unreachable!("prepare_ack returns WorkAcknowledged"),
            }
        }

        Ok(records)
    }

    async fn claim_with_wait(
        &self,
        request: ClaimManyRequest,
        wait: Duration,
    ) -> RuntimeResult<Vec<oxide_core::ClaimedWork>> {
        let deadline = Instant::now() + wait;
        loop {
            let claimed = {
                let mut engine = self.engine.lock().await;
                let now = now_ms();
                engine.promote_due(now);
                engine.requeue_expired(now);
                Engine::validate_claim_request_shape(&request)?;

                if self.config.lazy_claim_stream_creation {
                    let mut wal = self.wal.lock().await;
                    for stream in &request.streams {
                        if matches!(
                            engine.stream_config(stream),
                            Err(EngineError::StreamNotFound(_))
                        ) {
                            let event = engine
                                .prepare_create_stream(self.config.default_stream_config(stream))?;
                            wal.append(&event)?;
                            engine.apply_event(event)?;
                        }
                    }
                }

                let (event, claimed) = engine.prepare_claim_many(request.clone(), now)?;

                if !claimed.is_empty() {
                    self.wal.lock().await.append(&event)?;
                    engine.apply_event(event)?;
                }

                claimed
            };

            if !claimed.is_empty() {
                return Ok(claimed);
            }
            if Instant::now() >= deadline {
                return Ok(Vec::new());
            }
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn mutate<F>(&self, f: F) -> RuntimeResult<EngineEvent>
    where
        F: FnOnce(&mut Engine) -> EngineResult<EngineEvent>,
    {
        self.mutate_with(|engine| {
            let event = f(engine)?;
            Ok((event.clone(), event))
        })
        .await
        .map(|(_, event)| event)
    }

    async fn mutate_with<F, T>(&self, f: F) -> RuntimeResult<(EngineEvent, T)>
    where
        F: FnOnce(&mut Engine) -> EngineResult<(EngineEvent, T)>,
    {
        let mut guard = self.engine.lock().await;
        let mut draft = guard.clone();
        let (event, value) = f(&mut draft)?;
        if is_durable_event(&event) {
            self.wal.lock().await.append(&event)?;
        }
        *guard = draft;
        Ok((event, value))
    }

    fn authorize(&self, frame: &Frame) -> RuntimeResult<()> {
        let Some(expected) = self.config.auth_token.as_deref() else {
            return Ok(());
        };
        if frame.op == "HEALTH" {
            return Ok(());
        }
        let provided = frame
            .headers
            .get("authorization")
            .and_then(|value| value.strip_prefix("Bearer ").or(Some(value.as_str())))
            .or_else(|| frame.headers.get("auth-token").map(String::as_str));
        if provided == Some(expected) {
            Ok(())
        } else {
            Err(RuntimeError::Unauthorized)
        }
    }
}

fn required_header<'a>(frame: &'a Frame, key: &str) -> RuntimeResult<&'a str> {
    frame
        .headers
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| RuntimeError::InvalidRequest(format!("missing header: {key}")))
}

fn is_durable_event(event: &EngineEvent) -> bool {
    match event {
        EngineEvent::Noop => false,
        EngineEvent::WorkClaimed(claims) => !claims.is_empty(),
        _ => true,
    }
}

fn clear_stream_response(result: ClearStreamResult) -> ClearStreamResponse {
    ClearStreamResponse {
        stream: result.stream,
        removed_ready: result.removed_ready,
        removed_scheduled: result.removed_scheduled,
        removed_claimed: result.removed_claimed,
        total_removed: result.total_removed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_core::{ClaimManyRequest, ClaimRequest, StreamConfig, SubmitWork};
    use oxide_wire::encode;
    use tempfile::tempdir;

    #[tokio::test]
    async fn runtime_persists_and_recovers_submitted_work() {
        let dir = tempdir().unwrap();
        let config = OxideConfig {
            data_dir: dir.path().to_path_buf(),
            ..OxideConfig::default()
        };
        let runtime = OxideRuntime::open(config.clone()).unwrap();
        let create = Frame::new("STREAM.CREATE")
            .with_payload(serde_json::to_vec(&StreamConfig::new("email")).unwrap());
        let response = runtime.handle_frame(create).await;
        assert_eq!(response.headers.get("status").unwrap(), "ok");
        let submit = SubmitWork::new("email", b"hello".to_vec());
        let response = runtime
            .handle_frame(
                Frame::new("WORK.SUBMIT").with_payload(serde_json::to_vec(&submit).unwrap()),
            )
            .await;
        assert_eq!(response.headers.get("status").unwrap(), "ok");
        runtime.flush().await.unwrap();
        drop(runtime);

        let recovered = OxideRuntime::open(config).unwrap();
        let stats = recovered.metrics().await;
        assert_eq!(stats.engine.ready, 1);
    }

    #[tokio::test]
    async fn claim_returns_empty_after_wait() {
        let dir = tempdir().unwrap();
        let runtime = OxideRuntime::open(OxideConfig {
            data_dir: dir.path().to_path_buf(),
            default_claim_wait_ms: 1,
            ..OxideConfig::default()
        })
        .unwrap();
        runtime
            .handle_frame(
                Frame::new("STREAM.CREATE")
                    .with_payload(serde_json::to_vec(&StreamConfig::new("email")).unwrap()),
            )
            .await;
        let claim = ClaimRequest::one("email", "workers", "worker-1");
        let response = runtime
            .handle_frame(
                Frame::new("WORK.CLAIM")
                    .with_header("wait_ms", "1")
                    .with_payload(serde_json::to_vec(&claim).unwrap()),
            )
            .await;
        assert_eq!(response.headers.get("status").unwrap(), "empty");
    }

    #[tokio::test]
    async fn claim_lazily_creates_missing_stream() {
        let dir = tempdir().unwrap();
        let runtime = OxideRuntime::open(OxideConfig {
            data_dir: dir.path().to_path_buf(),
            default_claim_wait_ms: 1,
            ..OxideConfig::default()
        })
        .unwrap();
        let claim = ClaimRequest::one("transcribe", "workers", "worker-1");

        let response = runtime
            .handle_frame(
                Frame::new("WORK.CLAIM")
                    .with_header("wait_ms", "1")
                    .with_payload(serde_json::to_vec(&claim).unwrap()),
            )
            .await;

        assert_eq!(response.headers.get("status").unwrap(), "empty");
        let info = runtime
            .handle_frame(Frame::new("STREAM.INFO").with_header("stream", "transcribe"))
            .await;
        assert_eq!(info.headers.get("status").unwrap(), "ok");
        let stats: serde_json::Value = serde_json::from_slice(&info.payload).unwrap();
        assert_eq!(stats["name"].as_str(), Some("transcribe"));
    }

    #[tokio::test]
    async fn lazy_claim_stream_is_recovered_from_wal() {
        let dir = tempdir().unwrap();
        let config = OxideConfig {
            data_dir: dir.path().to_path_buf(),
            default_claim_wait_ms: 1,
            ..OxideConfig::default()
        };
        let runtime = OxideRuntime::open(config.clone()).unwrap();
        let claim = ClaimRequest::one("transcribe", "workers", "worker-1");
        let response = runtime
            .handle_frame(
                Frame::new("WORK.CLAIM")
                    .with_header("wait_ms", "1")
                    .with_payload(serde_json::to_vec(&claim).unwrap()),
            )
            .await;
        assert_eq!(response.headers.get("status").unwrap(), "empty");
        runtime.flush().await.unwrap();
        drop(runtime);

        let recovered = OxideRuntime::open(config).unwrap();
        let info = recovered
            .handle_frame(Frame::new("STREAM.INFO").with_header("stream", "transcribe"))
            .await;
        assert_eq!(info.headers.get("status").unwrap(), "ok");
    }

    #[tokio::test]
    async fn claim_many_lazily_creates_missing_streams() {
        let dir = tempdir().unwrap();
        let runtime = OxideRuntime::open(OxideConfig {
            data_dir: dir.path().to_path_buf(),
            default_claim_wait_ms: 1,
            ..OxideConfig::default()
        })
        .unwrap();
        let claim = ClaimManyRequest {
            streams: vec!["high".to_owned(), "default".to_owned(), "low".to_owned()],
            consumer_group: "workers".to_owned(),
            worker_id: "worker-1".to_owned(),
            max: 1,
            lease_ms: 30_000,
        };

        let response = runtime
            .handle_frame(
                Frame::new("WORK.CLAIM_MANY")
                    .with_header("wait_ms", "1")
                    .with_payload(serde_json::to_vec(&claim).unwrap()),
            )
            .await;

        assert_eq!(response.headers.get("status").unwrap(), "empty");
        let metrics = runtime.metrics().await;
        assert_eq!(metrics.engine.streams, 3);
        for stream in ["high", "default", "low"] {
            let info = runtime
                .handle_frame(Frame::new("STREAM.INFO").with_header("stream", stream))
                .await;
            assert_eq!(info.headers.get("status").unwrap(), "ok");
        }
    }

    #[tokio::test]
    async fn claim_missing_stream_errors_when_lazy_creation_is_disabled() {
        let dir = tempdir().unwrap();
        let runtime = OxideRuntime::open(OxideConfig {
            data_dir: dir.path().to_path_buf(),
            default_claim_wait_ms: 1,
            lazy_claim_stream_creation: false,
            ..OxideConfig::default()
        })
        .unwrap();
        let claim = ClaimRequest::one("transcribe", "workers", "worker-1");

        let response = runtime
            .handle_frame(
                Frame::new("WORK.CLAIM")
                    .with_header("wait_ms", "1")
                    .with_payload(serde_json::to_vec(&claim).unwrap()),
            )
            .await;

        assert_eq!(response.headers.get("status").unwrap(), "error");
        assert_eq!(
            response.headers.get("error").map(String::as_str),
            Some("stream does not exist: transcribe"),
        );
    }

    #[tokio::test]
    async fn invalid_claim_does_not_create_stream() {
        let dir = tempdir().unwrap();
        let runtime = OxideRuntime::open(OxideConfig {
            data_dir: dir.path().to_path_buf(),
            ..OxideConfig::default()
        })
        .unwrap();
        let claim = ClaimManyRequest {
            streams: vec!["transcribe".to_owned()],
            consumer_group: "workers".to_owned(),
            worker_id: "worker-1".to_owned(),
            max: 0,
            lease_ms: 30_000,
        };

        let response = runtime
            .handle_frame(
                Frame::new("WORK.CLAIM_MANY").with_payload(serde_json::to_vec(&claim).unwrap()),
            )
            .await;

        assert_eq!(response.headers.get("status").unwrap(), "error");
        assert_eq!(runtime.metrics().await.engine.streams, 0);
    }

    #[tokio::test]
    async fn submit_missing_stream_stays_strict() {
        let dir = tempdir().unwrap();
        let runtime = OxideRuntime::open(OxideConfig {
            data_dir: dir.path().to_path_buf(),
            ..OxideConfig::default()
        })
        .unwrap();
        let submit = SubmitWork::new("transcribe", b"hello".to_vec());

        let response = runtime
            .handle_frame(
                Frame::new("WORK.SUBMIT").with_payload(serde_json::to_vec(&submit).unwrap()),
            )
            .await;

        assert_eq!(response.headers.get("status").unwrap(), "error");
        assert_eq!(
            response.headers.get("error").map(String::as_str),
            Some("stream does not exist: transcribe"),
        );
    }

    #[tokio::test]
    async fn auth_token_protects_non_health_operations() {
        let dir = tempdir().unwrap();
        let runtime = OxideRuntime::open(OxideConfig {
            data_dir: dir.path().to_path_buf(),
            auth_token: Some("secret".to_owned()),
            ..OxideConfig::default()
        })
        .unwrap();

        let health = runtime.handle_frame(Frame::new("HEALTH")).await;
        assert_eq!(health.headers.get("status").unwrap(), "ok");

        let denied = runtime.handle_frame(Frame::new("STATS")).await;
        assert_eq!(denied.headers.get("status").unwrap(), "error");

        let allowed = runtime
            .handle_frame(Frame::new("STATS").with_header("authorization", "Bearer secret"))
            .await;
        assert_eq!(allowed.headers.get("status").unwrap(), "ok");
    }

    #[tokio::test]
    async fn release_and_clear_return_stable_json_shapes() {
        let dir = tempdir().unwrap();
        let runtime = OxideRuntime::open(OxideConfig {
            data_dir: dir.path().to_path_buf(),
            default_claim_wait_ms: 1,
            ..OxideConfig::default()
        })
        .unwrap();
        runtime
            .handle_frame(
                Frame::new("STREAM.CREATE")
                    .with_payload(serde_json::to_vec(&StreamConfig::new("email")).unwrap()),
            )
            .await;
        let submit = SubmitWork::new("email", b"hello".to_vec());
        runtime
            .handle_frame(
                Frame::new("WORK.SUBMIT").with_payload(serde_json::to_vec(&submit).unwrap()),
            )
            .await;
        let claim = ClaimRequest::one("email", "workers", "worker-1");
        let claimed = runtime
            .handle_frame(
                Frame::new("WORK.CLAIM")
                    .with_header("wait_ms", "1")
                    .with_payload(serde_json::to_vec(&claim).unwrap()),
            )
            .await;
        let claimed: serde_json::Value = serde_json::from_slice(&claimed.payload).unwrap();
        let claim = &claimed["items"][0]["claim"];

        let release = oxide_core::ReleaseRequest {
            claim_id: claim["claim_id"].as_u64().unwrap(),
            work_id: claim["work_id"].as_u64().unwrap(),
            fence_token: claim["fence_token"].as_u64().unwrap(),
            delay_ms: 0,
            priority: None,
        };
        let response = runtime
            .handle_frame(
                Frame::new("WORK.RELEASE").with_payload(serde_json::to_vec(&release).unwrap()),
            )
            .await;
        let released: serde_json::Value = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(released["work_id"].as_u64(), Some(1));
        assert!(released["available_at_ms"].as_u64().is_some());

        let clear = ClearStreamRequest {
            stream: "email".to_owned(),
            include_claimed: false,
        };
        let response = runtime
            .handle_frame(
                Frame::new("STREAM.CLEAR").with_payload(serde_json::to_vec(&clear).unwrap()),
            )
            .await;
        let cleared: serde_json::Value = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(cleared["stream"].as_str(), Some("email"));
        assert_eq!(cleared["total_removed"].as_u64(), Some(1));
    }

    #[test]
    fn encoded_response_is_wire_compatible() {
        let frame = json_frame(1, "HEALTH.OK", ResponseStatus::Ok, &json!({"ok": true})).unwrap();
        let encoded = encode(&frame).unwrap();
        assert!(!encoded.is_empty());
    }
}
