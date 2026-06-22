use crc32fast::Hasher;
use oxide_core::EngineEvent;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
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
}

impl Wal {
    pub fn open(config: WalConfig) -> WalResult<Self> {
        fs::create_dir_all(&config.dir)?;
        let active_index = latest_segment_index(&config.dir)?.unwrap_or(0);
        let active_path = segment_path(&config.dir, active_index);
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
        })
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

    pub fn replay(dir: impl AsRef<Path>) -> WalResult<Vec<EngineEvent>> {
        let mut events = Vec::new();
        for segment in list_segments(dir.as_ref())? {
            let mut file = File::open(&segment.path)?;
            let mut offset = 0_u64;
            loop {
                match read_record(&mut file, &segment.path, offset) {
                    Ok(Some((event, bytes_read))) => {
                        offset = offset.saturating_add(bytes_read);
                        events.push(event);
                    }
                    Ok(None) => break,
                    Err(WalError::Io(err)) if err.kind() == ErrorKind::UnexpectedEof => break,
                    Err(err) => return Err(err),
                }
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

fn read_record(file: &mut File, path: &Path, offset: u64) -> WalResult<Option<(EngineEvent, u64)>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut header = [0_u8; HEADER_LEN];
    let mut read = 0_usize;
    while read < HEADER_LEN {
        let n = file.read(&mut header[read..])?;
        if n == 0 {
            if read == 0 {
                return Ok(None);
            }
            return Ok(None);
        }
        read += n;
    }

    if &header[0..4] != WAL_MAGIC {
        return Err(WalError::Corrupt {
            path: path.to_path_buf(),
            offset,
            reason: "invalid magic".to_owned(),
        });
    }
    let len = u32::from_le_bytes(header[4..8].try_into().expect("header slice"));
    let expected_crc = u32::from_le_bytes(header[8..12].try_into().expect("header slice"));
    let mut payload = vec![0_u8; len as usize];
    file.read_exact(&mut payload)?;

    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let actual_crc = hasher.finalize();
    if expected_crc != actual_crc {
        return Err(WalError::Corrupt {
            path: path.to_path_buf(),
            offset,
            reason: "checksum mismatch".to_owned(),
        });
    }
    let event = serde_json::from_slice(&payload)?;
    Ok(Some((event, HEADER_LEN as u64 + u64::from(len))))
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
    use tempfile::tempdir;

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
    fn detects_checksum_corruption() {
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
        wal.flush().unwrap();
        let path = Wal::inspect(dir.path()).unwrap()[0].path.clone();
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start((HEADER_LEN + 3) as u64)).unwrap();
        file.write_all(b"x").unwrap();
        file.sync_all().unwrap();

        let err = Wal::replay(dir.path()).unwrap_err();
        assert!(matches!(err, WalError::Corrupt { .. }));
    }
}
