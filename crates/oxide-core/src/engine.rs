use crate::{
    Claim, ClaimId, ClaimManyRequest, ClaimRequest, ClaimedWork, ClearStreamResult,
    CompleteRequest, CompletionRecord, DEFAULT_PARTITION, EngineError, EngineEvent, EngineResult,
    ExtendRequest, FailRequest, FenceToken, LaneKey, ReleaseRequest, SchedulerPolicy, StreamConfig,
    StreamName, SubmitWork, TimestampMs, WorkId, WorkItem, WorkState, now_ms,
};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineStats {
    pub streams: usize,
    pub total_work: usize,
    pub ready: usize,
    pub scheduled: usize,
    pub claimed: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub dead_lettered: usize,
    pub live_claims: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamStats {
    pub name: StreamName,
    pub policy: SchedulerPolicy,
    pub total_work: usize,
    pub pending: usize,
    pub delayed: usize,
    pub reserved: usize,
    pub ready: usize,
    pub scheduled: usize,
    pub claimed: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub dead_lettered: usize,
    pub terminal: usize,
    pub oldest_pending_submitted_at_ms: Option<TimestampMs>,
    pub paused_until_ms: Option<TimestampMs>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Engine {
    streams: BTreeMap<StreamName, StreamRuntime>,
    work: HashMap<WorkId, WorkItem>,
    claims: HashMap<ClaimId, Claim>,
    work_claims: HashMap<WorkId, ClaimId>,
    active_lanes: HashSet<(StreamName, LaneKey)>,
    #[serde(default)]
    active_idempotency_keys: HashSet<String>,
    completion_ledger: HashMap<String, CompletionRecord>,
    next_work_id: WorkId,
    next_claim_id: ClaimId,
    next_fence_token: FenceToken,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamRuntime {
    config: StreamConfig,
    ready: BTreeSet<QueueEntry>,
    scheduled: BTreeSet<ScheduledEntry>,
    paused_until_ms: Option<TimestampMs>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct QueueEntry {
    work_id: WorkId,
    priority: i32,
    submitted_at_ms: TimestampMs,
    deadline_ms: Option<TimestampMs>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ScheduledEntry {
    schedule_at_ms: TimestampMs,
    work_id: WorkId,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    pub fn new() -> Self {
        Self {
            streams: BTreeMap::new(),
            work: HashMap::new(),
            claims: HashMap::new(),
            work_claims: HashMap::new(),
            active_lanes: HashSet::new(),
            active_idempotency_keys: HashSet::new(),
            completion_ledger: HashMap::new(),
            next_work_id: 1,
            next_claim_id: 1,
            next_fence_token: 1,
        }
    }

    pub fn create_stream(&mut self, config: StreamConfig) -> EngineResult<EngineEvent> {
        validate_stream_config(&config)?;
        if self.streams.contains_key(&config.name) {
            return Err(EngineError::StreamAlreadyExists(config.name));
        }
        self.streams.insert(
            config.name.clone(),
            StreamRuntime {
                config: config.clone(),
                ready: BTreeSet::new(),
                scheduled: BTreeSet::new(),
                paused_until_ms: None,
            },
        );
        Ok(EngineEvent::StreamCreated(config))
    }

    pub fn update_stream(&mut self, config: StreamConfig) -> EngineResult<EngineEvent> {
        validate_stream_config(&config)?;
        let stream = self
            .streams
            .get_mut(&config.name)
            .ok_or_else(|| EngineError::StreamNotFound(config.name.clone()))?;
        stream.config = config.clone();
        Ok(EngineEvent::StreamUpdated(config))
    }

    pub fn stream_config(&self, stream: &str) -> EngineResult<StreamConfig> {
        self.streams
            .get(stream)
            .map(|runtime| runtime.config.clone())
            .ok_or_else(|| EngineError::StreamNotFound(stream.to_owned()))
    }

    pub fn list_streams(&self) -> Vec<StreamConfig> {
        self.streams
            .values()
            .map(|runtime| runtime.config.clone())
            .collect()
    }

    pub fn prepare_submit(&self, mut submit: SubmitWork) -> EngineResult<(EngineEvent, WorkId)> {
        let stream = self
            .streams
            .get(&submit.stream)
            .ok_or_else(|| EngineError::StreamNotFound(submit.stream.clone()))?;
        if submit.payload.len() > stream.config.max_payload_bytes {
            return Err(EngineError::PayloadTooLarge);
        }
        if submit.ttr_ms == 0 {
            submit.ttr_ms = stream.config.default_ttr_ms;
        }
        if let Some(key) = &submit.idempotency_key {
            if self.completion_ledger.contains_key(key)
                || self.active_idempotency_keys.contains(key)
            {
                return Err(EngineError::DuplicateIdempotencyKey(key.clone()));
            }
        }

        let now = now_ms();
        let work_id = self.next_work_id;
        let state = if submit.schedule_at_ms > now {
            WorkState::Scheduled
        } else {
            WorkState::Ready
        };
        let item = WorkItem {
            id: work_id,
            stream: submit.stream.clone(),
            partition: DEFAULT_PARTITION,
            priority: submit.priority,
            submitted_at_ms: now,
            schedule_at_ms: submit.schedule_at_ms,
            deadline_ms: submit.deadline_ms,
            ttr_ms: submit.ttr_ms,
            idempotency_key: submit.idempotency_key,
            lane_key: submit.lane_key,
            headers: submit.headers,
            retry: submit.retry,
            attempts: 0,
            payload: submit.payload,
            state,
        };

        Ok((EngineEvent::WorkSubmitted(item), work_id))
    }

    pub fn submit(&mut self, submit: SubmitWork) -> EngineResult<(EngineEvent, WorkId)> {
        let (event, work_id) = self.prepare_submit(submit)?;
        self.apply_event(event.clone())?;

        Ok((event, work_id))
    }

    pub fn claim(
        &mut self,
        request: ClaimRequest,
    ) -> EngineResult<(EngineEvent, Vec<ClaimedWork>)> {
        self.claim_many(request.into())
    }

    pub fn claim_many(
        &mut self,
        request: ClaimManyRequest,
    ) -> EngineResult<(EngineEvent, Vec<ClaimedWork>)> {
        if request.max == 0 {
            return Err(EngineError::InvalidArgument(
                "claim max must be greater than zero".to_owned(),
            ));
        }
        if request.streams.is_empty() {
            return Err(EngineError::InvalidArgument(
                "at least one stream is required".to_owned(),
            ));
        }
        if request.lease_ms == 0 {
            return Err(EngineError::InvalidArgument(
                "lease must be greater than zero".to_owned(),
            ));
        }
        self.promote_due(now_ms());
        self.requeue_expired(now_ms());

        for stream in &request.streams {
            if !self.streams.contains_key(stream) {
                return Err(EngineError::StreamNotFound(stream.clone()));
            }
        }
        let now = now_ms();

        let mut claimed = Vec::new();
        let mut claim_records = Vec::new();
        while claimed.len() < request.max {
            let Some((stream_name, work_id)) =
                self.next_claimable_from_streams(&request.streams, now)
            else {
                break;
            };
            let item = self
                .work
                .get(&work_id)
                .cloned()
                .ok_or(EngineError::WorkNotFound(work_id))?;
            let stream = self
                .streams
                .get_mut(&stream_name)
                .ok_or_else(|| EngineError::StreamNotFound(stream_name.clone()))?;
            stream.ready.remove(&QueueEntry::from_work(&item));

            let claim = Claim {
                claim_id: self.allocate_claim_id(),
                work_id,
                stream: stream_name,
                consumer_group: request.consumer_group.clone(),
                worker_id: request.worker_id.clone(),
                fence_token: self.allocate_fence_token(),
                claimed_at_ms: now,
                expires_at_ms: now.saturating_add(request.lease_ms),
                lane_key: item.lane_key.clone(),
            };

            let work = self.work.get_mut(&work_id).expect("work exists");
            work.state = WorkState::Claimed;
            work.attempts = work.attempts.saturating_add(1);
            if let Some(lane) = &work.lane_key {
                self.active_lanes
                    .insert((work.stream.clone(), lane.clone()));
            }
            self.work_claims.insert(work_id, claim.claim_id);
            self.claims.insert(claim.claim_id, claim.clone());
            claim_records.push(claim.clone());
            claimed.push(ClaimedWork {
                claim,
                work: work.clone(),
            });
        }

        Ok((EngineEvent::WorkClaimed(claim_records), claimed))
    }

    pub fn prepare_claim_many(
        &self,
        request: ClaimManyRequest,
        now: TimestampMs,
    ) -> EngineResult<(EngineEvent, Vec<ClaimedWork>)> {
        if request.max == 0 {
            return Err(EngineError::InvalidArgument(
                "claim max must be greater than zero".to_owned(),
            ));
        }
        if request.streams.is_empty() {
            return Err(EngineError::InvalidArgument(
                "at least one stream is required".to_owned(),
            ));
        }
        if request.lease_ms == 0 {
            return Err(EngineError::InvalidArgument(
                "lease must be greater than zero".to_owned(),
            ));
        }

        for stream in &request.streams {
            if !self.streams.contains_key(stream) {
                return Err(EngineError::StreamNotFound(stream.clone()));
            }
        }

        let mut claimed = Vec::new();
        let mut claim_records = Vec::new();
        let mut selected_work_ids = HashSet::new();
        let mut selected_lanes = HashSet::new();

        while claimed.len() < request.max {
            let Some((stream_name, work_id)) = self.next_claimable_from_streams_excluding(
                &request.streams,
                now,
                &selected_work_ids,
                &selected_lanes,
            ) else {
                break;
            };
            let mut work = self
                .work
                .get(&work_id)
                .cloned()
                .ok_or(EngineError::WorkNotFound(work_id))?;
            let offset = claim_records.len() as u64;
            let claim = Claim {
                claim_id: self.next_claim_id.saturating_add(offset),
                work_id,
                stream: stream_name,
                consumer_group: request.consumer_group.clone(),
                worker_id: request.worker_id.clone(),
                fence_token: self.next_fence_token.saturating_add(offset),
                claimed_at_ms: now,
                expires_at_ms: now.saturating_add(request.lease_ms),
                lane_key: work.lane_key.clone(),
            };

            work.state = WorkState::Claimed;
            work.attempts = work.attempts.saturating_add(1);
            selected_work_ids.insert(work_id);
            if let Some(lane) = &work.lane_key {
                selected_lanes.insert((work.stream.clone(), lane.clone()));
            }
            claim_records.push(claim.clone());
            claimed.push(ClaimedWork { claim, work });
        }

        Ok((EngineEvent::WorkClaimed(claim_records), claimed))
    }

    pub fn ack(&mut self, request: CompleteRequest) -> EngineResult<EngineEvent> {
        let claim = self.valid_claim(request.claim_id, request.work_id, request.fence_token)?;
        self.claims.remove(&claim.claim_id);
        self.work_claims.remove(&request.work_id);

        let work = self
            .work
            .get_mut(&request.work_id)
            .ok_or(EngineError::WorkNotFound(request.work_id))?;
        work.state = WorkState::Completed;
        if let Some(lane) = &work.lane_key {
            self.active_lanes
                .remove(&(work.stream.clone(), lane.clone()));
        }
        let record = CompletionRecord {
            work_id: work.id,
            stream: work.stream.clone(),
            idempotency_key: work.idempotency_key.clone(),
            completed_at_ms: now_ms(),
        };
        if let Some(key) = &record.idempotency_key {
            self.active_idempotency_keys.remove(key);
            self.completion_ledger.insert(key.clone(), record.clone());
        }
        Ok(EngineEvent::WorkAcknowledged(record))
    }

    pub fn prepare_ack(&self, request: CompleteRequest) -> EngineResult<EngineEvent> {
        self.valid_claim(request.claim_id, request.work_id, request.fence_token)?;
        let work = self
            .work
            .get(&request.work_id)
            .ok_or(EngineError::WorkNotFound(request.work_id))?;

        Ok(EngineEvent::WorkAcknowledged(CompletionRecord {
            work_id: work.id,
            stream: work.stream.clone(),
            idempotency_key: work.idempotency_key.clone(),
            completed_at_ms: now_ms(),
        }))
    }

    pub fn fail(&mut self, request: FailRequest) -> EngineResult<EngineEvent> {
        let claim = self.valid_claim(request.claim_id, request.work_id, request.fence_token)?;
        self.claims.remove(&claim.claim_id);
        self.work_claims.remove(&request.work_id);

        let now = now_ms();
        let work = self
            .work
            .get_mut(&request.work_id)
            .ok_or(EngineError::WorkNotFound(request.work_id))?;
        if let Some(lane) = &work.lane_key {
            self.active_lanes
                .remove(&(work.stream.clone(), lane.clone()));
        }

        let should_retry = request.retry && work.attempts < work.retry.max_attempts;
        let retry_at_ms = should_retry.then(|| now.saturating_add(work.retry.backoff_ms));
        if let Some(retry_at_ms) = retry_at_ms {
            work.schedule_at_ms = retry_at_ms;
            work.state = WorkState::Scheduled;
            let stream = self.streams.get_mut(&work.stream).expect("stream exists");
            stream.scheduled.insert(ScheduledEntry {
                schedule_at_ms: retry_at_ms,
                work_id: work.id,
            });
        } else {
            if let Some(key) = &work.idempotency_key {
                self.active_idempotency_keys.remove(key);
            }
            work.state = WorkState::Failed;
        }

        Ok(EngineEvent::WorkFailed {
            work_id: request.work_id,
            retry_at_ms,
            reason: request.reason,
        })
    }

    pub fn release(&mut self, request: ReleaseRequest) -> EngineResult<EngineEvent> {
        let claim = self.valid_claim(request.claim_id, request.work_id, request.fence_token)?;
        self.claims.remove(&claim.claim_id);
        self.work_claims.remove(&request.work_id);

        let now = now_ms();
        let work = self
            .work
            .get_mut(&request.work_id)
            .ok_or(EngineError::WorkNotFound(request.work_id))?;
        if let Some(lane) = &work.lane_key {
            self.active_lanes
                .remove(&(work.stream.clone(), lane.clone()));
        }
        if let Some(priority) = request.priority {
            work.priority = priority;
        }
        let available_at_ms = now.saturating_add(request.delay_ms);
        work.schedule_at_ms = available_at_ms;
        let stream = self.streams.get_mut(&work.stream).expect("stream exists");
        if request.delay_ms == 0 {
            work.state = WorkState::Ready;
            stream.ready.insert(QueueEntry::from_work(work));
        } else {
            work.state = WorkState::Scheduled;
            stream.scheduled.insert(ScheduledEntry {
                schedule_at_ms: available_at_ms,
                work_id: work.id,
            });
        }

        Ok(EngineEvent::WorkReleased {
            work_id: request.work_id,
            available_at_ms,
            priority: request.priority,
        })
    }

    pub fn extend(&mut self, request: ExtendRequest) -> EngineResult<EngineEvent> {
        let existing = self.valid_claim(request.claim_id, request.work_id, request.fence_token)?;
        let new_expiry = existing.expires_at_ms.saturating_add(request.extend_by_ms);
        let new_token = self.allocate_fence_token();
        let claim = self
            .claims
            .get_mut(&request.claim_id)
            .ok_or(EngineError::ClaimNotFound(request.claim_id))?;
        claim.expires_at_ms = new_expiry;
        claim.fence_token = new_token;
        Ok(EngineEvent::WorkExtended(claim.clone()))
    }

    pub fn clear_stream(
        &mut self,
        stream: impl AsRef<str>,
        include_claimed: bool,
    ) -> EngineResult<(EngineEvent, ClearStreamResult)> {
        let stream_name = stream.as_ref();
        if !self.streams.contains_key(stream_name) {
            return Err(EngineError::StreamNotFound(stream_name.to_owned()));
        }

        let mut ready_ids = Vec::new();
        let mut scheduled_ids = Vec::new();
        if let Some(runtime) = self.streams.get(stream_name) {
            ready_ids.extend(runtime.ready.iter().map(|entry| entry.work_id));
            scheduled_ids.extend(runtime.scheduled.iter().map(|entry| entry.work_id));
        }
        let claimed_ids: Vec<_> = if include_claimed {
            self.claims
                .values()
                .filter(|claim| claim.stream == stream_name)
                .map(|claim| claim.work_id)
                .collect()
        } else {
            Vec::new()
        };

        let mut work_ids =
            Vec::with_capacity(ready_ids.len() + scheduled_ids.len() + claimed_ids.len());
        work_ids.extend(ready_ids.iter().copied());
        work_ids.extend(scheduled_ids.iter().copied());
        work_ids.extend(claimed_ids.iter().copied());
        self.apply_clear_stream(stream_name, &work_ids);

        let result = ClearStreamResult {
            stream: stream_name.to_owned(),
            removed_ready: ready_ids.len(),
            removed_scheduled: scheduled_ids.len(),
            removed_claimed: claimed_ids.len(),
            total_removed: work_ids.len(),
            work_ids,
        };
        Ok((EngineEvent::StreamCleared(result.clone()), result))
    }

    pub fn cancel(&mut self, work_id: WorkId) -> EngineResult<EngineEvent> {
        let work = self
            .work
            .get_mut(&work_id)
            .ok_or(EngineError::WorkNotFound(work_id))?;
        if let Some(stream) = self.streams.get_mut(&work.stream) {
            stream.ready.remove(&QueueEntry::from_work(work));
            stream.scheduled.remove(&ScheduledEntry::from_work(work));
        }
        if let Some(claim_id) = self.work_claims.remove(&work_id) {
            if let Some(claim) = self.claims.remove(&claim_id) {
                if let Some(lane) = claim.lane_key {
                    self.active_lanes.remove(&(claim.stream, lane));
                }
            }
        }
        if let Some(key) = &work.idempotency_key {
            self.active_idempotency_keys.remove(key);
        }
        work.state = WorkState::Cancelled;
        Ok(EngineEvent::WorkCancelled { work_id })
    }

    pub fn pause_stream(
        &mut self,
        stream: impl AsRef<str>,
        until_ms: TimestampMs,
    ) -> EngineResult<EngineEvent> {
        let stream_name = stream.as_ref();
        let runtime = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| EngineError::StreamNotFound(stream_name.to_owned()))?;
        runtime.paused_until_ms = Some(until_ms);
        Ok(EngineEvent::StreamPaused {
            stream: stream_name.to_owned(),
            until_ms,
        })
    }

    pub fn resume_stream(&mut self, stream: impl AsRef<str>) -> EngineResult<EngineEvent> {
        let stream_name = stream.as_ref();
        let runtime = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| EngineError::StreamNotFound(stream_name.to_owned()))?;
        runtime.paused_until_ms = None;
        Ok(EngineEvent::StreamResumed {
            stream: stream_name.to_owned(),
        })
    }

    pub fn promote_due(&mut self, now: TimestampMs) {
        let streams: Vec<_> = self.streams.keys().cloned().collect();
        for stream_name in streams {
            let mut due = Vec::new();
            if let Some(stream) = self.streams.get_mut(&stream_name) {
                while let Some(entry) = stream.scheduled.first().copied() {
                    if entry.schedule_at_ms > now {
                        break;
                    }
                    stream.scheduled.remove(&entry);
                    due.push(entry.work_id);
                }
            }
            for work_id in due {
                if let Some(work) = self.work.get_mut(&work_id) {
                    if work.state == WorkState::Scheduled {
                        work.state = WorkState::Ready;
                        if let Some(stream) = self.streams.get_mut(&work.stream) {
                            stream.ready.insert(QueueEntry::from_work(work));
                        }
                    }
                }
            }
        }
    }

    pub fn requeue_expired(&mut self, now: TimestampMs) -> usize {
        let expired: Vec<_> = self
            .claims
            .values()
            .filter(|claim| claim.expires_at_ms <= now)
            .map(|claim| claim.claim_id)
            .collect();
        let mut count = 0;
        for claim_id in expired {
            let Some(claim) = self.claims.remove(&claim_id) else {
                continue;
            };
            self.work_claims.remove(&claim.work_id);
            if let Some(lane) = &claim.lane_key {
                self.active_lanes
                    .remove(&(claim.stream.clone(), lane.clone()));
            }
            if let Some(work) = self.work.get_mut(&claim.work_id) {
                if work.state == WorkState::Claimed {
                    work.state = WorkState::Ready;
                    if let Some(stream) = self.streams.get_mut(&work.stream) {
                        stream.ready.insert(QueueEntry::from_work(work));
                    }
                    count += 1;
                }
            }
        }
        count
    }

    pub fn inspect_work(&self, work_id: WorkId) -> EngineResult<WorkItem> {
        self.work
            .get(&work_id)
            .cloned()
            .ok_or(EngineError::WorkNotFound(work_id))
    }

    pub fn stats(&self) -> EngineStats {
        let mut stats = EngineStats {
            streams: self.streams.len(),
            total_work: self.work.len(),
            live_claims: self.claims.len(),
            ..EngineStats::default()
        };
        for work in self.work.values() {
            match work.state {
                WorkState::Scheduled => stats.scheduled += 1,
                WorkState::Ready => stats.ready += 1,
                WorkState::Claimed => stats.claimed += 1,
                WorkState::Completed => stats.completed += 1,
                WorkState::Failed => stats.failed += 1,
                WorkState::Cancelled => stats.cancelled += 1,
                WorkState::DeadLettered => stats.dead_lettered += 1,
            }
        }
        stats
    }

    pub fn stream_stats(&self, stream_name: impl AsRef<str>) -> EngineResult<StreamStats> {
        let stream_name = stream_name.as_ref();
        let stream = self
            .streams
            .get(stream_name)
            .ok_or_else(|| EngineError::StreamNotFound(stream_name.to_owned()))?;
        let mut stats = StreamStats {
            name: stream_name.to_owned(),
            policy: stream.config.policy,
            paused_until_ms: stream.paused_until_ms,
            ..StreamStats::default()
        };
        for work in self.work.values().filter(|work| work.stream == stream_name) {
            stats.total_work += 1;
            match work.state {
                WorkState::Scheduled => {
                    stats.scheduled += 1;
                    stats.delayed += 1;
                }
                WorkState::Ready => {
                    stats.ready += 1;
                    stats.pending += 1;
                    stats.oldest_pending_submitted_at_ms = Some(
                        stats
                            .oldest_pending_submitted_at_ms
                            .map_or(work.submitted_at_ms, |current| {
                                current.min(work.submitted_at_ms)
                            }),
                    );
                }
                WorkState::Claimed => {
                    stats.claimed += 1;
                    stats.reserved += 1;
                }
                WorkState::Completed => {
                    stats.completed += 1;
                    stats.terminal += 1;
                }
                WorkState::Failed => {
                    stats.failed += 1;
                    stats.terminal += 1;
                }
                WorkState::Cancelled => {
                    stats.cancelled += 1;
                    stats.terminal += 1;
                }
                WorkState::DeadLettered => {
                    stats.dead_lettered += 1;
                    stats.terminal += 1;
                }
            }
        }
        Ok(stats)
    }

    pub fn apply_event(&mut self, event: EngineEvent) -> EngineResult<()> {
        match event {
            EngineEvent::StreamCreated(config) => {
                if !self.streams.contains_key(&config.name) {
                    self.create_stream(config)?;
                }
            }
            EngineEvent::StreamUpdated(config) => {
                if self.streams.contains_key(&config.name) {
                    self.update_stream(config)?;
                } else {
                    self.create_stream(config)?;
                }
            }
            EngineEvent::WorkSubmitted(item) => {
                let idempotency_key = item.idempotency_key.clone();
                self.next_work_id = self.next_work_id.max(item.id.saturating_add(1));
                let stream = self
                    .streams
                    .get_mut(&item.stream)
                    .ok_or_else(|| EngineError::StreamNotFound(item.stream.clone()))?;
                match item.state {
                    WorkState::Scheduled => {
                        stream.scheduled.insert(ScheduledEntry::from_work(&item));
                    }
                    WorkState::Ready => {
                        stream.ready.insert(QueueEntry::from_work(&item));
                    }
                    _ => {}
                }
                self.work.insert(item.id, item);
                if let Some(key) = idempotency_key {
                    self.active_idempotency_keys.insert(key);
                }
            }
            EngineEvent::WorkClaimed(claims) => {
                for claim in claims {
                    self.next_claim_id = self.next_claim_id.max(claim.claim_id.saturating_add(1));
                    self.next_fence_token = self
                        .next_fence_token
                        .max(claim.fence_token.saturating_add(1));
                    if let Some(work) = self.work.get_mut(&claim.work_id) {
                        if let Some(stream) = self.streams.get_mut(&work.stream) {
                            stream.ready.remove(&QueueEntry::from_work(work));
                        }
                        work.state = WorkState::Claimed;
                        work.attempts = work.attempts.saturating_add(1);
                        if let Some(lane) = &work.lane_key {
                            self.active_lanes
                                .insert((work.stream.clone(), lane.clone()));
                        }
                    }
                    self.work_claims.insert(claim.work_id, claim.claim_id);
                    self.claims.insert(claim.claim_id, claim);
                }
            }
            EngineEvent::WorkAcknowledged(record) => {
                if let Some(work) = self.work.get_mut(&record.work_id) {
                    if let Some(claim_id) = self.work_claims.remove(&record.work_id) {
                        self.claims.remove(&claim_id);
                    }
                    if let Some(lane) = &work.lane_key {
                        self.active_lanes
                            .remove(&(work.stream.clone(), lane.clone()));
                    }
                    work.state = WorkState::Completed;
                }
                if let Some(key) = &record.idempotency_key {
                    self.active_idempotency_keys.remove(key);
                    self.completion_ledger.insert(key.clone(), record);
                }
            }
            EngineEvent::WorkFailed {
                work_id,
                retry_at_ms,
                ..
            } => {
                if let Some(work) = self.work.get_mut(&work_id) {
                    if let Some(claim_id) = self.work_claims.remove(&work_id) {
                        self.claims.remove(&claim_id);
                    }
                    if let Some(lane) = &work.lane_key {
                        self.active_lanes
                            .remove(&(work.stream.clone(), lane.clone()));
                    }
                    if let Some(retry_at_ms) = retry_at_ms {
                        work.schedule_at_ms = retry_at_ms;
                        work.state = WorkState::Scheduled;
                        if let Some(stream) = self.streams.get_mut(&work.stream) {
                            stream.scheduled.insert(ScheduledEntry {
                                schedule_at_ms: retry_at_ms,
                                work_id,
                            });
                        }
                    } else {
                        if let Some(key) = &work.idempotency_key {
                            self.active_idempotency_keys.remove(key);
                        }
                        work.state = WorkState::Failed;
                    }
                }
            }
            EngineEvent::WorkReleased {
                work_id,
                available_at_ms,
                priority,
            } => {
                if let Some(work) = self.work.get_mut(&work_id) {
                    if let Some(claim_id) = self.work_claims.remove(&work_id) {
                        self.claims.remove(&claim_id);
                    }
                    if let Some(lane) = &work.lane_key {
                        self.active_lanes
                            .remove(&(work.stream.clone(), lane.clone()));
                    }
                    if let Some(priority) = priority {
                        work.priority = priority;
                    }
                    work.schedule_at_ms = available_at_ms;
                    if available_at_ms <= now_ms() {
                        work.state = WorkState::Ready;
                        if let Some(stream) = self.streams.get_mut(&work.stream) {
                            stream.ready.insert(QueueEntry::from_work(work));
                        }
                    } else {
                        work.state = WorkState::Scheduled;
                        if let Some(stream) = self.streams.get_mut(&work.stream) {
                            stream.scheduled.insert(ScheduledEntry {
                                schedule_at_ms: available_at_ms,
                                work_id,
                            });
                        }
                    }
                }
            }
            EngineEvent::WorkExtended(claim) => {
                self.next_fence_token = self
                    .next_fence_token
                    .max(claim.fence_token.saturating_add(1));
                self.claims.insert(claim.claim_id, claim);
            }
            EngineEvent::WorkCancelled { work_id } => {
                self.cancel(work_id)?;
            }
            EngineEvent::StreamCleared(result) => {
                self.apply_clear_stream(&result.stream, &result.work_ids);
            }
            EngineEvent::StreamPaused { stream, until_ms } => {
                self.pause_stream(stream, until_ms)?;
            }
            EngineEvent::StreamResumed { stream } => {
                self.resume_stream(stream)?;
            }
            EngineEvent::Noop => {}
        }
        Ok(())
    }

    fn next_claimable_from_streams(
        &self,
        streams: &[StreamName],
        now: TimestampMs,
    ) -> Option<(StreamName, WorkId)> {
        self.next_claimable_from_streams_excluding(streams, now, &HashSet::new(), &HashSet::new())
    }

    fn next_claimable_from_streams_excluding(
        &self,
        streams: &[StreamName],
        now: TimestampMs,
        excluded_work_ids: &HashSet<WorkId>,
        excluded_lanes: &HashSet<(StreamName, LaneKey)>,
    ) -> Option<(StreamName, WorkId)> {
        streams.iter().find_map(|stream| {
            let paused = self
                .streams
                .get(stream)
                .and_then(|runtime| runtime.paused_until_ms)
                .is_some_and(|until| until > now);
            if paused {
                return None;
            }
            self.next_claimable_work_id_excluding(stream, excluded_work_ids, excluded_lanes)
                .map(|work_id| (stream.clone(), work_id))
        })
    }

    fn next_claimable_work_id_excluding(
        &self,
        stream_name: &str,
        excluded_work_ids: &HashSet<WorkId>,
        excluded_lanes: &HashSet<(StreamName, LaneKey)>,
    ) -> Option<WorkId> {
        let stream = self.streams.get(stream_name)?;
        stream
            .ready
            .iter()
            .find(|entry| {
                if excluded_work_ids.contains(&entry.work_id) {
                    return false;
                }
                let Some(work) = self.work.get(&entry.work_id) else {
                    return false;
                };
                if work.state != WorkState::Ready {
                    return false;
                }
                if let Some(lane) = &work.lane_key {
                    let lane_key = (work.stream.clone(), lane.clone());

                    !self.active_lanes.contains(&lane_key) && !excluded_lanes.contains(&lane_key)
                } else {
                    true
                }
            })
            .map(|entry| entry.work_id)
    }

    fn apply_clear_stream(&mut self, stream_name: &str, work_ids: &[WorkId]) {
        for work_id in work_ids {
            let Some(work) = self.work.get_mut(work_id) else {
                continue;
            };
            if work.stream != stream_name {
                continue;
            }
            if let Some(stream) = self.streams.get_mut(&work.stream) {
                stream.ready.remove(&QueueEntry::from_work(work));
                stream.scheduled.remove(&ScheduledEntry::from_work(work));
            }
            if let Some(claim_id) = self.work_claims.remove(work_id) {
                if let Some(claim) = self.claims.remove(&claim_id) {
                    if let Some(lane) = claim.lane_key {
                        self.active_lanes.remove(&(claim.stream, lane));
                    }
                }
            }
            if let Some(lane) = &work.lane_key {
                self.active_lanes
                    .remove(&(work.stream.clone(), lane.clone()));
            }
            if let Some(key) = &work.idempotency_key {
                self.active_idempotency_keys.remove(key);
            }
            work.state = WorkState::Cancelled;
        }
    }

    fn valid_claim(
        &self,
        claim_id: ClaimId,
        work_id: WorkId,
        fence_token: FenceToken,
    ) -> EngineResult<Claim> {
        let claim = self
            .claims
            .get(&claim_id)
            .ok_or(EngineError::ClaimNotFound(claim_id))?;
        if claim.work_id != work_id {
            return Err(EngineError::WorkNotClaimed(work_id));
        }
        if claim.fence_token != fence_token {
            return Err(EngineError::InvalidFence(claim_id));
        }
        Ok(claim.clone())
    }

    fn allocate_claim_id(&mut self) -> ClaimId {
        let id = self.next_claim_id;
        self.next_claim_id = self.next_claim_id.saturating_add(1);
        id
    }

    fn allocate_fence_token(&mut self) -> FenceToken {
        let token = self.next_fence_token;
        self.next_fence_token = self.next_fence_token.saturating_add(1);
        token
    }
}

impl QueueEntry {
    fn from_work(work: &WorkItem) -> Self {
        Self {
            work_id: work.id,
            priority: work.priority,
            submitted_at_ms: work.submitted_at_ms,
            deadline_ms: work.deadline_ms,
        }
    }
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .reverse()
            .then_with(|| {
                self.deadline_ms
                    .unwrap_or(u64::MAX)
                    .cmp(&other.deadline_ms.unwrap_or(u64::MAX))
            })
            .then_with(|| self.submitted_at_ms.cmp(&other.submitted_at_ms))
            .then_with(|| self.work_id.cmp(&other.work_id))
    }
}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl ScheduledEntry {
    fn from_work(work: &WorkItem) -> Self {
        Self {
            schedule_at_ms: work.schedule_at_ms,
            work_id: work.id,
        }
    }
}

impl Ord for ScheduledEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.schedule_at_ms
            .cmp(&other.schedule_at_ms)
            .then_with(|| self.work_id.cmp(&other.work_id))
    }
}

impl PartialOrd for ScheduledEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl WorkItem {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            WorkState::Completed
                | WorkState::Failed
                | WorkState::Cancelled
                | WorkState::DeadLettered
        )
    }
}

fn validate_stream_config(config: &StreamConfig) -> EngineResult<()> {
    if config.name.trim().is_empty() {
        return Err(EngineError::InvalidArgument(
            "stream name cannot be empty".to_owned(),
        ));
    }
    if config.partitions == 0 {
        return Err(EngineError::InvalidArgument(
            "stream must have at least one partition".to_owned(),
        ));
    }
    if config.max_payload_bytes == 0 {
        return Err(EngineError::InvalidArgument(
            "max payload bytes must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CompleteRequest, RetryPolicy};

    fn engine_with_stream() -> Engine {
        let mut engine = Engine::new();
        engine
            .create_stream(StreamConfig::new("email"))
            .expect("stream is created");
        engine
    }

    #[test]
    fn claims_highest_priority_first() {
        let mut engine = engine_with_stream();
        let mut low = SubmitWork::new("email", b"low".to_vec());
        low.priority = 1;
        engine.submit(low).unwrap();
        let mut high = SubmitWork::new("email", b"high".to_vec());
        high.priority = 100;
        let (_, high_id) = engine.submit(high).unwrap();

        let (_, claimed) = engine
            .claim(ClaimRequest::one("email", "workers", "worker-1"))
            .unwrap();

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].work.id, high_id);
    }

    #[test]
    fn lane_keys_block_parallel_claims_for_same_lane() {
        let mut engine = engine_with_stream();
        for payload in [b"first".to_vec(), b"second".to_vec()] {
            let mut submit = SubmitWork::new("email", payload);
            submit.lane_key = Some("account-1".to_owned());
            engine.submit(submit).unwrap();
        }

        let (_, claimed) = engine
            .claim(ClaimRequest {
                stream: "email".to_owned(),
                consumer_group: "workers".to_owned(),
                worker_id: "worker-1".to_owned(),
                max: 2,
                lease_ms: 30_000,
            })
            .unwrap();

        assert_eq!(claimed.len(), 1);
        assert_eq!(engine.stats().ready, 1);
    }

    #[test]
    fn ack_requires_current_fence_token() {
        let mut engine = engine_with_stream();
        engine
            .submit(SubmitWork::new("email", b"body".to_vec()))
            .unwrap();
        let (_, claimed) = engine
            .claim(ClaimRequest::one("email", "workers", "worker-1"))
            .unwrap();
        let claim = &claimed[0].claim;

        let err = engine
            .ack(CompleteRequest {
                claim_id: claim.claim_id,
                work_id: claim.work_id,
                fence_token: claim.fence_token + 1,
            })
            .unwrap_err();

        assert_eq!(err, EngineError::InvalidFence(claim.claim_id));
    }

    #[test]
    fn completion_ledger_deduplicates_idempotency_keys() {
        let mut engine = engine_with_stream();
        let mut submit = SubmitWork::new("email", b"body".to_vec());
        submit.idempotency_key = Some("abc".to_owned());
        engine.submit(submit).unwrap();
        let (_, claimed) = engine
            .claim(ClaimRequest::one("email", "workers", "worker-1"))
            .unwrap();
        let claim = &claimed[0].claim;
        engine
            .ack(CompleteRequest {
                claim_id: claim.claim_id,
                work_id: claim.work_id,
                fence_token: claim.fence_token,
            })
            .unwrap();

        let mut duplicate = SubmitWork::new("email", b"body".to_vec());
        duplicate.idempotency_key = Some("abc".to_owned());
        let err = engine.submit(duplicate).unwrap_err();

        assert_eq!(err, EngineError::DuplicateIdempotencyKey("abc".to_owned()));
    }

    #[test]
    fn failed_work_retries_until_max_attempts() {
        let mut engine = engine_with_stream();
        let mut submit = SubmitWork::new("email", b"body".to_vec());
        submit.retry = RetryPolicy {
            max_attempts: 2,
            backoff_ms: 1,
        };
        engine.submit(submit).unwrap();
        let (_, claimed) = engine
            .claim(ClaimRequest::one("email", "workers", "worker-1"))
            .unwrap();
        let claim = &claimed[0].claim;

        engine
            .fail(FailRequest {
                claim_id: claim.claim_id,
                work_id: claim.work_id,
                fence_token: claim.fence_token,
                retry: true,
                reason: Some("boom".to_owned()),
            })
            .unwrap();

        assert_eq!(engine.stats().scheduled, 1);
    }

    #[test]
    fn release_requeues_claimed_work_with_same_fence_token() {
        let mut engine = engine_with_stream();
        let mut submit = SubmitWork::new("email", b"body".to_vec());
        submit.priority = 1;
        let (_, work_id) = engine.submit(submit).unwrap();
        let (_, claimed) = engine
            .claim(ClaimRequest::one("email", "workers", "worker-1"))
            .unwrap();
        let claim = &claimed[0].claim;

        engine
            .release(ReleaseRequest {
                claim_id: claim.claim_id,
                work_id,
                fence_token: claim.fence_token,
                delay_ms: 0,
                priority: Some(10),
            })
            .unwrap();

        assert_eq!(engine.stats().ready, 1);
        let (_, claimed_again) = engine
            .claim(ClaimRequest::one("email", "workers", "worker-2"))
            .unwrap();
        assert_eq!(claimed_again[0].work.id, work_id);
        assert_eq!(claimed_again[0].work.priority, 10);
    }

    #[test]
    fn claim_many_checks_streams_in_order() {
        let mut engine = engine_with_stream();
        engine
            .create_stream(StreamConfig::new("webhooks"))
            .expect("second stream");
        let (_, email_id) = engine
            .submit(SubmitWork::new("email", b"email".to_vec()))
            .unwrap();
        let (_, webhook_id) = engine
            .submit(SubmitWork::new("webhooks", b"webhook".to_vec()))
            .unwrap();

        let (_, claimed) = engine
            .claim_many(ClaimManyRequest {
                streams: vec!["webhooks".to_owned(), "email".to_owned()],
                consumer_group: "workers".to_owned(),
                worker_id: "worker-1".to_owned(),
                max: 1,
                lease_ms: 30_000,
            })
            .unwrap();

        assert_eq!(claimed[0].work.id, webhook_id);
        assert_ne!(claimed[0].work.id, email_id);
    }

    #[test]
    fn clear_stream_cancels_ready_scheduled_and_optionally_claimed_work() {
        let mut engine = engine_with_stream();
        engine
            .submit(SubmitWork::new("email", b"claimed".to_vec()))
            .unwrap();
        let (_, ready_id) = engine
            .submit(SubmitWork::new("email", b"ready".to_vec()))
            .unwrap();
        let mut delayed = SubmitWork::new("email", b"delayed".to_vec());
        delayed.schedule_at_ms = now_ms().saturating_add(60_000);
        let (_, delayed_id) = engine.submit(delayed).unwrap();
        let (_, claimed) = engine
            .claim(ClaimRequest::one("email", "workers", "worker-1"))
            .unwrap();
        let claimed_id = claimed[0].work.id;

        let (_, result) = engine.clear_stream("email", true).unwrap();

        assert_eq!(result.total_removed, 3);
        assert!(result.work_ids.contains(&delayed_id));
        assert!(result.work_ids.contains(&claimed_id));
        assert!(result.work_ids.contains(&ready_id));
        assert_eq!(engine.stats().cancelled, 3);
        assert_eq!(engine.stats().live_claims, 0);
    }

    #[test]
    fn stream_stats_exposes_pending_delayed_and_reserved_counts() {
        let mut engine = engine_with_stream();
        engine
            .submit(SubmitWork::new("email", b"ready".to_vec()))
            .unwrap();
        let mut delayed = SubmitWork::new("email", b"delayed".to_vec());
        delayed.schedule_at_ms = now_ms().saturating_add(60_000);
        engine.submit(delayed).unwrap();
        engine
            .claim(ClaimRequest::one("email", "workers", "worker-1"))
            .unwrap();

        let stats = engine.stream_stats("email").unwrap();

        assert_eq!(stats.pending, 0);
        assert_eq!(stats.delayed, 1);
        assert_eq!(stats.reserved, 1);
    }
}
