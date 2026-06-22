use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use thiserror::Error;

const MAGIC: &[u8; 4] = b"OXD1";
const HEADER_LEN: usize = 28;
pub const VERSION: u16 = 1;
pub const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WireError {
    #[error("invalid frame magic")]
    InvalidMagic,
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u16),
    #[error("frame exceeds configured max bytes")]
    FrameTooLarge,
    #[error("operation name is too long")]
    OperationTooLong,
    #[error("header bytes are too large")]
    HeadersTooLarge,
    #[error("headers are not valid UTF-8")]
    HeaderUtf8,
    #[error("headers are not valid JSON: {0}")]
    HeaderJson(String),
    #[error("payload JSON error: {0}")]
    PayloadJson(String),
}

pub type WireResult<T> = Result<T, WireError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub version: u16,
    pub request_id: u64,
    pub op: String,
    pub headers: BTreeMap<String, String>,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(op: impl Into<String>) -> Self {
        Self {
            version: VERSION,
            request_id: 0,
            op: op.into(),
            headers: BTreeMap::new(),
            payload: Bytes::new(),
        }
    }

    pub fn with_request_id(mut self, request_id: u64) -> Self {
        self.request_id = request_id;
        self
    }

    pub fn with_payload(mut self, payload: impl Into<Bytes>) -> Self {
        self.payload = payload.into();
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn payload_json<T: DeserializeOwned>(&self) -> WireResult<T> {
        serde_json::from_slice(&self.payload).map_err(|err| WireError::PayloadJson(err.to_string()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseStatus {
    Ok,
    Error,
    Empty,
}

impl ResponseStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Empty => "empty",
        }
    }
}

pub fn json_frame<T: Serialize>(
    request_id: u64,
    op: impl Into<String>,
    status: ResponseStatus,
    value: &T,
) -> WireResult<Frame> {
    let payload = Bytes::from(
        serde_json::to_vec(value).map_err(|err| WireError::PayloadJson(err.to_string()))?,
    );
    Ok(Frame::new(op)
        .with_request_id(request_id)
        .with_header("status", status.as_str())
        .with_header("content-type", "application/json")
        .with_payload(payload))
}

pub fn error_frame(request_id: u64, op: impl Into<String>, message: impl Into<String>) -> Frame {
    Frame::new(op)
        .with_request_id(request_id)
        .with_header("status", ResponseStatus::Error.as_str())
        .with_header("error", message.into())
}

pub fn encode(frame: &Frame) -> WireResult<Bytes> {
    let op = frame.op.as_bytes();
    let op_len = u16::try_from(op.len()).map_err(|_| WireError::OperationTooLong)?;
    let headers =
        serde_json::to_vec(&frame.headers).map_err(|err| WireError::HeaderJson(err.to_string()))?;
    let headers_len = u32::try_from(headers.len()).map_err(|_| WireError::HeadersTooLarge)?;
    let payload_len = u32::try_from(frame.payload.len()).map_err(|_| WireError::FrameTooLarge)?;
    let frame_len = HEADER_LEN
        .saturating_add(op.len())
        .saturating_add(headers.len())
        .saturating_add(frame.payload.len());
    if frame_len > DEFAULT_MAX_FRAME_BYTES {
        return Err(WireError::FrameTooLarge);
    }

    let mut out = BytesMut::with_capacity(frame_len);
    out.put_slice(MAGIC);
    out.put_u16(frame.version);
    out.put_u64(frame.request_id);
    out.put_u16(op_len);
    out.put_u32(headers_len);
    out.put_u32(payload_len);
    out.put_u32(0);
    out.put_slice(op);
    out.put_slice(&headers);
    out.put_slice(&frame.payload);
    Ok(out.freeze())
}

pub fn try_decode(src: &mut BytesMut) -> WireResult<Option<Frame>> {
    try_decode_with_max(src, DEFAULT_MAX_FRAME_BYTES)
}

pub fn try_decode_with_max(
    src: &mut BytesMut,
    max_frame_bytes: usize,
) -> WireResult<Option<Frame>> {
    if src.len() < HEADER_LEN {
        return Ok(None);
    }
    if &src[..4] != MAGIC {
        return Err(WireError::InvalidMagic);
    }
    let version = u16::from_be_bytes(src[4..6].try_into().expect("version"));
    if version != VERSION {
        return Err(WireError::UnsupportedVersion(version));
    }
    let op_len = u16::from_be_bytes(src[14..16].try_into().expect("op len")) as usize;
    let headers_len = u32::from_be_bytes(src[16..20].try_into().expect("headers len")) as usize;
    let payload_len = u32::from_be_bytes(src[20..24].try_into().expect("payload len")) as usize;
    let total_len = HEADER_LEN
        .checked_add(op_len)
        .and_then(|len| len.checked_add(headers_len))
        .and_then(|len| len.checked_add(payload_len))
        .ok_or(WireError::FrameTooLarge)?;
    if total_len > max_frame_bytes {
        return Err(WireError::FrameTooLarge);
    }
    if src.len() < total_len {
        return Ok(None);
    }

    let mut frame = src.split_to(total_len);
    frame.advance(4);
    let version = frame.get_u16();
    let request_id = frame.get_u64();
    let op_len = frame.get_u16() as usize;
    let headers_len = frame.get_u32() as usize;
    let payload_len = frame.get_u32() as usize;
    let _reserved = frame.get_u32();

    let op_bytes = frame.split_to(op_len);
    let op = String::from_utf8(op_bytes.to_vec()).map_err(|_| WireError::HeaderUtf8)?;
    let headers_bytes = frame.split_to(headers_len);
    let headers: BTreeMap<String, String> = serde_json::from_slice(&headers_bytes)
        .map_err(|err| WireError::HeaderJson(err.to_string()))?;
    let payload = frame.split_to(payload_len).freeze();

    Ok(Some(Frame {
        version,
        request_id,
        op,
        headers,
        payload,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_frame() {
        let frame = Frame::new("WORK.SUBMIT")
            .with_request_id(42)
            .with_header("stream", "email")
            .with_payload(Bytes::from_static(b"payload"));

        let encoded = encode(&frame).unwrap();
        let mut buffer = BytesMut::from(&encoded[..]);
        let decoded = try_decode(&mut buffer).unwrap().unwrap();

        assert_eq!(decoded, frame);
        assert!(buffer.is_empty());
    }

    #[test]
    fn waits_for_partial_frame() {
        let frame = Frame::new("STATS").with_request_id(7);
        let encoded = encode(&frame).unwrap();
        let split = encoded.len() - 2;
        let mut buffer = BytesMut::from(&encoded[..split]);

        assert_eq!(try_decode(&mut buffer).unwrap(), None);
        buffer.extend_from_slice(&encoded[split..]);

        assert_eq!(try_decode(&mut buffer).unwrap().unwrap(), frame);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut buffer = BytesMut::from(&b"NOPE000000000000000000000000"[..]);
        assert_eq!(try_decode(&mut buffer), Err(WireError::InvalidMagic));
    }

    #[test]
    fn enforces_max_frame_bytes() {
        let frame = Frame::new("WORK.SUBMIT").with_payload(vec![0_u8; 128]);
        let encoded = encode(&frame).unwrap();
        let mut buffer = BytesMut::from(&encoded[..]);

        assert_eq!(
            try_decode_with_max(&mut buffer, 32),
            Err(WireError::FrameTooLarge)
        );
    }
}
