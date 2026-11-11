use crc32fast::Hasher;
use oxide_core::EngineEvent;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use thiserror::Error;

const WAL_MAGIC: &[u8; 4] = b"OXW1";
const HEADER_LEN: usize = 12;
const DEFAULT_SEGMENT_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_BATCH_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("record payload is too large")]
    RecordTooLarge,
    #[error("corrupt WAL record in {path} at offset {offset}: {reason}")]
    Corrupt {
        path: PathBuf,
        offset: u64,
        reason: String,
    },
}

pub type WalResult<T> = Result<T, WalError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsyncMode {
    Always,
    Batch,
    Async,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalConfig {
    pub dir: PathBuf,
    pub segment_bytes: u64,
    pub fsync_mode: FsyncMode,
    pub batch_bytes: u64,
    pub batch_interval_ms: u64,
}

impl WalConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            fsync_mode: FsyncMode::Batch,
            batch_bytes: DEFAULT_BATCH_BYTES,
            batch_interval_ms: 10,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentInfo {
    pub index: u64,
    pub path: PathBuf,
    pub bytes: u64,
}

#[derive(Debug)]
pub struct Wal {
    config: WalConfig,
    active_index: u64,
    active_path: PathBuf,
    active: File,
    active_len: u64,
    unsynced_bytes: u64,
    last_sync: Instant,
    truncated_tail_bytes: u64,
}

impl Wal {
    /// Opens the WAL for appending.
    ///
    /// If the active segment ends in a torn record (a crash mid-write), the
    /// partial bytes are truncated so new records are not appended after
    /// garbage. If valid records follow the bad bytes, this is real
    /// corruption rather than a torn write and an error is returned instead.
    pub fn open(config: WalConfig) -> WalResult<Self> {
        fs::create_dir_all(&config.dir)?;
        let active_index = latest_segment_index(&config.dir)?.unwrap_or(0);
        let active_path = segment_path(&config.dir, active_index);
        let truncated_tail_bytes = repair_torn_tail(&active_path)?;
        let active = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&active_path)?;
        let active_len = active.metadata()?.len();
        Ok(Self {
            config,
            active_index,
            active_path,
            active,
            active_len,
            unsynced_bytes: 0,
            last_sync: Instant::now(),
            truncated_tail_bytes,
        })
    }

    /// Bytes of a torn trailing record removed by [`Wal::open`], if any.
    pub fn truncated_tail_bytes(&self) -> u64 {
        self.truncated_tail_bytes
    }

    pub fn append(&mut self, event: &EngineEvent) -> WalResult<()> {
        let record = encode_record(event)?;
        if self.active_len > 0
            && self
                .active_len
                .saturating_add(record.len() as u64)
                .gt(&self.config.segment_bytes)
        {
            self.rotate()?;
        }

        self.active.write_all(&record)?;
        self.active_len = self.active_len.saturating_add(record.len() as u64);
        self.unsynced_bytes = self.unsynced_bytes.saturating_add(record.len() as u64);
        self.sync_if_needed()?;
        Ok(())
    }

    pub fn flush(&mut self) -> WalResult<()> {
        self.active.flush()?;
        self.active.sync_data()?;
        self.unsynced_bytes = 0;
        self.last_sync = Instant::now();
        Ok(())
    }

    /// Replays every record in segment order.
    ///
    /// A torn trailing record is tolerated only in the last segment; sealed
    /// segments must be complete.
    pub fn replay(dir: impl AsRef<Path>) -> WalResult<Vec<EngineEvent>> {
        let segments = list_segments(dir.as_ref())?;
        let last = segments.len().saturating_sub(1);
        let mut events = Vec::new();
        for (position, segment) in segments.iter().enumerate() {
            let bytes = fs::read(&segment.path)?;
            let valid = valid_prefix_len(&bytes, &segment.path)?;
            if valid < bytes.len() && position != last {
                return Err(WalError::Corrupt {
                    path: segment.path.clone(),
                    offset: valid as u64,
                    reason: "incomplete record in a sealed segment".to_owned(),
                });
            }
            let mut offset = 0;
            while offset < valid {
                let len = record_payload_len(&bytes[offset..]);
                let start = offset + HEADER_LEN;
                events.push(serde_json::from_slice(&bytes[start..start + len])?);
                offset = start + len;
            }
        }
        Ok(events)
    }

    pub fn inspect(dir: impl AsRef<Path>) -> WalResult<Vec<WalSegmentInfo>> {
        list_segments(dir.as_ref())
    }

    fn rotate(&mut self) -> WalResult<()> {
        self.flush()?;
        self.active_index = self.active_index.saturating_add(1);
        self.active_path = segment_path(&self.config.dir, self.active_index);
        self.active = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.active_path)?;
        self.active_len = self.active.metadata()?.len();
        Ok(())
    }

    fn sync_if_needed(&mut self) -> WalResult<()> {
        match self.config.fsync_mode {
            FsyncMode::Always => self.flush(),
            FsyncMode::Async => Ok(()),
            FsyncMode::Batch => {
                let due_by_bytes = self.unsynced_bytes >= self.config.batch_bytes;
                let due_by_time = self.last_sync.elapsed()
                    >= Duration::from_millis(self.config.batch_interval_ms);
                if due_by_bytes || due_by_time {
                    self.flush()
                } else {
                    Ok(())
                }
            }
        }
    }
}

fn encode_record(event: &EngineEvent) -> WalResult<Vec<u8>> {
    let payload = serde_json::to_vec(event)?;
    let len = u32::try_from(payload.len()).map_err(|_| WalError::RecordTooLarge)?;
    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let crc = hasher.finalize();

    let mut record = Vec::with_capacity(HEADER_LEN + payload.len());
    record.extend_from_slice(WAL_MAGIC);
    record.extend_from_slice(&len.to_le_bytes());
    record.extend_from_slice(&crc.to_le_bytes());
    record.extend_from_slice(&payload);
    Ok(record)
}

enum RecordCheck {
    /// A complete record with a matching checksum, of this total length.
    Valid(usize),
    /// Not a valid record; the reason is used in error messages.
    Invalid(&'static str),
}

fn record_payload_len(buf: &[u8]) -> usize {
    u32::from_le_bytes(buf[4..8].try_into().expect("header slice")) as usize
}

fn check_record(buf: &[u8]) -> RecordCheck {
    if buf.len() < HEADER_LEN {
        return RecordCheck::Invalid("incomplete header");
    }
    if &buf[0..4] != WAL_MAGIC {
        return RecordCheck::Invalid("invalid magic");
    }
    let len = record_payload_len(buf);
    let Some(total) = HEADER_LEN.checked_add(len).filter(|total| *total <= buf.len()) else {
        return RecordCheck::Invalid("incomplete payload");
    };
    let expected_crc = u32::from_le_bytes(buf[8..12].try_into().expect("header slice"));
    let mut hasher = Hasher::new();
    hasher.update(&buf[HEADER_LEN..total]);
    if hasher.finalize() != expected_crc {
        return RecordCheck::Invalid("checksum mismatch");
    }
    RecordCheck::Valid(total)
}

/// Returns the length of the longest prefix made of valid records.
///
/// Bytes after that prefix are treated as a torn write only when no valid
/// record can be found anywhere after them. Otherwise the damage is in the
/// middle of the log and truncating would drop durable records, so this
/// returns [`WalError::Corrupt`].
fn valid_prefix_len(bytes: &[u8], path: &Path) -> WalResult<usize> {
    let mut offset = 0;
    while offset < bytes.len() {
        match check_record(&bytes[offset..]) {
            RecordCheck::Valid(len) => offset += len,
            RecordCheck::Invalid(reason) => {
                if let Some(next) = find_valid_record(bytes, offset + 1) {
                    return Err(WalError::Corrupt {
                        path: path.to_path_buf(),
                        offset: offset as u64,
                        reason: format!(
                            "{reason}; a valid record follows at offset {next}, refusing to truncate"
                        ),
                    });
                }
                return Ok(offset);
            }
        }
    }
    Ok(offset)
}

fn find_valid_record(bytes: &[u8], from: usize) -> Option<usize> {
    (from..bytes.len().saturating_sub(HEADER_LEN - 1)).find(|&position| {
        bytes[position..].starts_with(WAL_MAGIC)
            && matches!(check_record(&bytes[position..]), RecordCheck::Valid(_))
    })
}

/// Truncates a torn trailing record from `path`, returning the bytes removed.
fn repair_torn_tail(path: &Path) -> WalResult<u64> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err.into()),
    };
    let valid = valid_prefix_len(&bytes, path)?;
    if valid == bytes.len() {
        return Ok(0);
    }
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(valid as u64)?;
    file.sync_all()?;
    Ok((bytes.len() - valid) as u64)
}

fn list_segments(dir: &Path) -> WalResult<Vec<WalSegmentInfo>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(index) = parse_segment_index(&path) else {
            continue;
        };
        segments.push(WalSegmentInfo {
            index,
            bytes: entry.metadata()?.len(),
            path,
        });
    }
    segments.sort_by_key(|segment| segment.index);
    Ok(segments)
}

fn latest_segment_index(dir: &Path) -> WalResult<Option<u64>> {
    Ok(list_segments(dir)?.last().map(|segment| segment.index))
}

fn segment_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("{index:020}.wal"))
}

fn parse_segment_index(path: &Path) -> Option<u64> {
    let file_name = path.file_name()?.to_str()?;
    let index = file_name.strip_suffix(".wal")?;
    index.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_core::{Engine, StreamConfig, SubmitWork};
    use std::io::{Seek, SeekFrom};
    use tempfile::tempdir;

    fn always(dir: &Path) -> WalConfig {
        WalConfig {
            fsync_mode: FsyncMode::Always,
            ..WalConfig::new(dir)
        }
    }

    fn write_events(wal: &mut Wal, engine: &mut Engine, payloads: &[&str]) {
        for payload in payloads {
            let (event, _) = engine
                .submit(SubmitWork::new("email", payload.as_bytes().to_vec()))
                .unwrap();
            wal.append(&event).unwrap();
        }
    }

    fn engine_with_stream(wal: &mut Wal) -> Engine {
        let mut engine = Engine::new();
        let event = engine.create_stream(StreamConfig::new("email")).unwrap();
        wal.append(&event).unwrap();
        engine
    }

    fn truncate_by(path: &Path, bytes: u64) {
        let len = fs::metadata(path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(len - bytes)
            .unwrap();
    }

    #[test]
    fn torn_tail_is_truncated_and_later_appends_replay() {
        let dir = tempdir().unwrap();
        let mut wal = Wal::open(always(dir.path())).unwrap();
        let mut engine = engine_with_stream(&mut wal);
        write_events(&mut wal, &mut engine, &["one"]);
        drop(wal);
        let path = Wal::inspect(dir.path()).unwrap()[0].path.clone();
        truncate_by(&path, 7);

        let mut wal = Wal::open(always(dir.path())).unwrap();
        assert!(wal.truncated_tail_bytes() > 0);
        write_events(&mut wal, &mut engine, &["two", "three"]);
        drop(wal);

        assert_eq!(Wal::replay(dir.path()).unwrap().len(), 3);
        let reopened = Wal::open(always(dir.path())).unwrap();
        assert_eq!(reopened.truncated_tail_bytes(), 0);
    }

    #[test]
    fn torn_header_is_truncated() {
        let dir = tempdir().unwrap();
        let mut wal = Wal::open(always(dir.path())).unwrap();
        let mut engine = engine_with_stream(&mut wal);
        drop(wal);
        let path = Wal::inspect(dir.path()).unwrap()[0].path.clone();
        let clean_len = fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&WAL_MAGIC[..3]).unwrap();
        drop(file);

        let mut wal = Wal::open(always(dir.path())).unwrap();
        assert_eq!(wal.truncated_tail_bytes(), 3);
        assert_eq!(fs::metadata(&path).unwrap().len(), clean_len);
        write_events(&mut wal, &mut engine, &["after"]);
        assert_eq!(Wal::replay(dir.path()).unwrap().len(), 2);
    }

    #[test]
    fn incomplete_sealed_segment_is_reported() {
        let dir = tempdir().unwrap();
        let mut wal = Wal::open(WalConfig {
            segment_bytes: 64,
            ..always(dir.path())
        })
        .unwrap();
        let mut engine = engine_with_stream(&mut wal);
        write_events(&mut wal, &mut engine, &["one", "two"]);
        drop(wal);
        let segments = Wal::inspect(dir.path()).unwrap();
        assert!(segments.len() >= 2);
        truncate_by(&segments[0].path, 5);

        let err = Wal::replay(dir.path()).unwrap_err();
        assert!(matches!(err, WalError::Corrupt { .. }));
    }

    #[test]
    fn appends_and_replays_events() {
        let dir = tempdir().unwrap();
        let mut wal = Wal::open(WalConfig {
            dir: dir.path().to_path_buf(),
            fsync_mode: FsyncMode::Always,
            ..WalConfig::new(dir.path())
        })
        .unwrap();

        let mut engine = Engine::new();
        let event = engine.create_stream(StreamConfig::new("email")).unwrap();
        wal.append(&event).unwrap();
        let (event, _) = engine
            .submit(SubmitWork::new("email", b"hello".to_vec()))
            .unwrap();
        wal.append(&event).unwrap();
        wal.flush().unwrap();

        let events = Wal::replay(dir.path()).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn rotates_segments() {
        let dir = tempdir().unwrap();
        let mut wal = Wal::open(WalConfig {
            dir: dir.path().to_path_buf(),
            segment_bytes: 64,
            fsync_mode: FsyncMode::Always,
            ..WalConfig::new(dir.path())
        })
        .unwrap();

        let mut engine = Engine::new();
        let event = engine.create_stream(StreamConfig::new("email")).unwrap();
        wal.append(&event).unwrap();
        let (event, _) = engine
            .submit(SubmitWork::new("email", vec![b'x'; 128]))
            .unwrap();
        wal.append(&event).unwrap();

        let segments = Wal::inspect(dir.path()).unwrap();
        assert!(segments.len() >= 2);
    }

    #[test]
    fn corruption_before_valid_records_is_reported_not_truncated() {
        let dir = tempdir().unwrap();
        let mut wal = Wal::open(always(dir.path())).unwrap();
        let mut engine = engine_with_stream(&mut wal);
        write_events(&mut wal, &mut engine, &["after"]);
        drop(wal);
        let path = Wal::inspect(dir.path()).unwrap()[0].path.clone();
        let len_before = fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start((HEADER_LEN + 3) as u64)).unwrap();
        file.write_all(b"x").unwrap();
        file.sync_all().unwrap();

        let err = Wal::replay(dir.path()).unwrap_err();
        assert!(matches!(err, WalError::Corrupt { .. }));
        let err = Wal::open(always(dir.path())).unwrap_err();
        assert!(matches!(err, WalError::Corrupt { .. }));
        assert_eq!(fs::metadata(&path).unwrap().len(), len_before);
    }
}
