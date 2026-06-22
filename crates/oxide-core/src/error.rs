use crate::{ClaimId, StreamName, WorkId};
use thiserror::Error;

pub type EngineResult<T> = Result<T, EngineError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EngineError {
    #[error("stream already exists: {0}")]
    StreamAlreadyExists(StreamName),
    #[error("stream does not exist: {0}")]
    StreamNotFound(StreamName),
    #[error("work item does not exist: {0}")]
    WorkNotFound(WorkId),
    #[error("claim does not exist: {0}")]
    ClaimNotFound(ClaimId),
    #[error("invalid fencing token for claim: {0}")]
    InvalidFence(ClaimId),
    #[error("work item is not claimed: {0}")]
    WorkNotClaimed(WorkId),
    #[error("work item is not ready: {0}")]
    WorkNotReady(WorkId),
    #[error("deduplicated idempotency key: {0}")]
    DuplicateIdempotencyKey(String),
    #[error("payload exceeds configured limit")]
    PayloadTooLarge,
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}
