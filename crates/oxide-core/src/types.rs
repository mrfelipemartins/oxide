use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub type StreamName = String;
pub type ConsumerGroup = String;
pub type PartitionId = u32;
pub type LaneKey = String;
pub type IdempotencyKey = String;
pub type WorkId = u64;
pub type ClaimId = u64;
pub type FenceToken = u64;
pub type TimestampMs = u64;

pub const DEFAULT_PARTITION: PartitionId = 0;

pub fn now_ms() -> TimestampMs {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

pub fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerPolicy {
    Fifo,
    #[default]
    Priority,
    Deadline,
    FairShare,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: StreamName,
    pub partitions: u32,
    pub policy: SchedulerPolicy,
    pub max_payload_bytes: usize,
    pub default_ttr_ms: u64,
    pub dedupe_ttl_ms: u64,
    pub dead_letter_stream: Option<StreamName>,
}

impl StreamConfig {
    pub fn new(name: impl Into<StreamName>) -> Self {
        Self {
            name: name.into(),
            partitions: 1,
            policy: SchedulerPolicy::Priority,
            max_payload_bytes: 1024 * 1024,
            default_ttr_ms: 30_000,
            dedupe_ttl_ms: 24 * 60 * 60 * 1000,
            dead_letter_stream: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff_ms: 1_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitWork {
    pub stream: StreamName,
    pub priority: i32,
    pub schedule_at_ms: TimestampMs,
    pub deadline_ms: Option<TimestampMs>,
    pub ttr_ms: u64,
    pub idempotency_key: Option<IdempotencyKey>,
    pub lane_key: Option<LaneKey>,
    pub headers: BTreeMap<String, String>,
    pub retry: RetryPolicy,
    #[serde(deserialize_with = "deserialize_payload")]
    pub payload: Vec<u8>,
}

impl SubmitWork {
    pub fn new(stream: impl Into<StreamName>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            stream: stream.into(),
            priority: 0,
            schedule_at_ms: now_ms(),
            deadline_ms: None,
            ttr_ms: 30_000,
            idempotency_key: None,
            lane_key: None,
            headers: BTreeMap::new(),
            retry: RetryPolicy::default(),
            payload: payload.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItem {
    pub id: WorkId,
    pub stream: StreamName,
    pub partition: PartitionId,
    pub priority: i32,
    pub submitted_at_ms: TimestampMs,
    pub schedule_at_ms: TimestampMs,
    pub deadline_ms: Option<TimestampMs>,
    pub ttr_ms: u64,
    pub idempotency_key: Option<IdempotencyKey>,
    pub lane_key: Option<LaneKey>,
    pub headers: BTreeMap<String, String>,
    pub retry: RetryPolicy,
    pub attempts: u32,
    pub payload: Vec<u8>,
    pub state: WorkState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkState {
    Scheduled,
    Ready,
    Claimed,
    Completed,
    Failed,
    Cancelled,
    DeadLettered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub claim_id: ClaimId,
    pub work_id: WorkId,
    pub stream: StreamName,
    pub consumer_group: ConsumerGroup,
    pub worker_id: String,
    pub fence_token: FenceToken,
    pub claimed_at_ms: TimestampMs,
    pub expires_at_ms: TimestampMs,
    pub lane_key: Option<LaneKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimedWork {
    pub claim: Claim,
    pub work: WorkItem,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub stream: StreamName,
    pub consumer_group: ConsumerGroup,
    pub worker_id: String,
    pub max: usize,
    pub lease_ms: u64,
}

impl ClaimRequest {
    pub fn one(
        stream: impl Into<StreamName>,
        consumer_group: impl Into<ConsumerGroup>,
        worker_id: impl Into<String>,
    ) -> Self {
        Self {
            stream: stream.into(),
            consumer_group: consumer_group.into(),
            worker_id: worker_id.into(),
            max: 1,
            lease_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimManyRequest {
    pub streams: Vec<StreamName>,
    pub consumer_group: ConsumerGroup,
    pub worker_id: String,
    pub max: usize,
    pub lease_ms: u64,
}

impl From<ClaimRequest> for ClaimManyRequest {
    fn from(request: ClaimRequest) -> Self {
        Self {
            streams: vec![request.stream],
            consumer_group: request.consumer_group,
            worker_id: request.worker_id,
            max: request.max,
            lease_ms: request.lease_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteRequest {
    pub claim_id: ClaimId,
    pub work_id: WorkId,
    pub fence_token: FenceToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailRequest {
    pub claim_id: ClaimId,
    pub work_id: WorkId,
    pub fence_token: FenceToken,
    pub retry: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtendRequest {
    pub claim_id: ClaimId,
    pub work_id: WorkId,
    pub fence_token: FenceToken,
    pub extend_by_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseRequest {
    pub claim_id: ClaimId,
    pub work_id: WorkId,
    pub fence_token: FenceToken,
    pub delay_ms: u64,
    pub priority: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClearStreamRequest {
    pub stream: StreamName,
    #[serde(default)]
    pub include_claimed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClearStreamResult {
    pub stream: StreamName,
    pub removed_ready: usize,
    pub removed_scheduled: usize,
    pub removed_claimed: usize,
    pub total_removed: usize,
    pub work_ids: Vec<WorkId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionRecord {
    pub work_id: WorkId,
    pub stream: StreamName,
    pub idempotency_key: Option<IdempotencyKey>,
    pub completed_at_ms: TimestampMs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineCommand {
    CreateStream(StreamConfig),
    UpdateStream(StreamConfig),
    SubmitWork(SubmitWork),
    ClaimWork(ClaimRequest),
    ClaimMany(ClaimManyRequest),
    Ack(CompleteRequest),
    Fail(FailRequest),
    Extend(ExtendRequest),
    Release(ReleaseRequest),
    ClearStream(ClearStreamRequest),
    Cancel {
        work_id: WorkId,
    },
    PauseStream {
        stream: StreamName,
        until_ms: TimestampMs,
    },
    ResumeStream {
        stream: StreamName,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineEvent {
    StreamCreated(StreamConfig),
    StreamUpdated(StreamConfig),
    WorkSubmitted(WorkItem),
    WorkClaimed(Vec<Claim>),
    WorkAcknowledged(CompletionRecord),
    WorkFailed {
        work_id: WorkId,
        retry_at_ms: Option<TimestampMs>,
        reason: Option<String>,
    },
    WorkReleased {
        work_id: WorkId,
        available_at_ms: TimestampMs,
        priority: Option<i32>,
    },
    WorkExtended(Claim),
    WorkCancelled {
        work_id: WorkId,
    },
    StreamCleared(ClearStreamResult),
    StreamPaused {
        stream: StreamName,
        until_ms: TimestampMs,
    },
    StreamResumed {
        stream: StreamName,
    },
    Noop,
}

impl fmt::Display for SchedulerPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fifo => f.write_str("fifo"),
            Self::Priority => f.write_str("priority"),
            Self::Deadline => f.write_str("deadline"),
            Self::FairShare => f.write_str("fair_share"),
        }
    }
}

fn deserialize_payload<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    struct PayloadVisitor;

    impl<'de> Visitor<'de> for PayloadVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a UTF-8 string or an array of byte values")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.as_bytes().to_vec())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.into_bytes())
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut bytes = Vec::with_capacity(sequence.size_hint().unwrap_or(0));

            while let Some(byte) = sequence.next_element::<u8>()? {
                bytes.push(byte);
            }

            Ok(bytes)
        }
    }

    deserializer.deserialize_any(PayloadVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_work_payload_deserializes_from_string_or_byte_array() {
        let from_string: SubmitWork = serde_json::from_value(serde_json::json!({
            "stream": "email",
            "priority": 0,
            "schedule_at_ms": 1,
            "deadline_ms": null,
            "ttr_ms": 30000,
            "idempotency_key": null,
            "lane_key": null,
            "headers": {},
            "retry": {"max_attempts": 3, "backoff_ms": 1000},
            "payload": "hello"
        }))
        .unwrap();
        let from_bytes: SubmitWork = serde_json::from_value(serde_json::json!({
            "stream": "email",
            "priority": 0,
            "schedule_at_ms": 1,
            "deadline_ms": null,
            "ttr_ms": 30000,
            "idempotency_key": null,
            "lane_key": null,
            "headers": {},
            "retry": {"max_attempts": 3, "backoff_ms": 1000},
            "payload": [104, 101, 108, 108, 111]
        }))
        .unwrap();

        assert_eq!(from_string.payload, b"hello");
        assert_eq!(from_bytes.payload, b"hello");
    }
}
