//! Storage contracts for replayable Triggerlane events.

use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard, PoisonError},
};

use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use triggerlane_core::{EventEnvelope, EventId};

/// Acquire a mutex guard, recovering from poisoning rather than propagating a
/// panic. The store's invariants hold across every operation that runs under a
/// lock (push, clone, scan, swap), so a poisoned-but-consistent value is safe to
/// resume — a transient fault in one operation must not render the store unusable
/// for the life of the process.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How a durable store decides which records to keep when pruned. Both forms are
/// operator-driven; the store never prunes on its own.
#[derive(Debug, Clone, Copy)]
pub enum ManualRetention {
    /// Keep records whose timestamp is at or after the cutoff; remove older ones.
    OlderThan(DateTime<Utc>),
    /// Keep only the most recent N records by append order; remove the rest.
    KeepMostRecent(usize),
}

impl ManualRetention {
    /// Partition `items` into kept (in order) given each item's timestamp,
    /// returning the kept records. `timestamp` extracts the record's age key.
    fn keep<T: Clone>(&self, items: &[T], timestamp: impl Fn(&T) -> DateTime<Utc>) -> Vec<T> {
        match *self {
            ManualRetention::OlderThan(cutoff) => items
                .iter()
                .filter(|item| timestamp(item) >= cutoff)
                .cloned()
                .collect(),
            ManualRetention::KeepMostRecent(max) => {
                let skip = items.len().saturating_sub(max);
                items.iter().skip(skip).cloned().collect()
            }
        }
    }
}

/// An automatic, bounded retention policy for the event store. Every bound is
/// optional; the default (all `None`) is unbounded and removes nothing. A
/// *delivered* event is removed once it is older than `delivered_grace`. Any event
/// — delivered or not — is removed once it crosses a hard bound (`max_age`,
/// `max_count`, or `max_bytes`); the hard bounds are the safety valve that keeps an
/// always-on store from growing without limit even if delivery stalls.
#[derive(Debug, Clone, Copy, Default)]
pub struct AutoRetention {
    /// Remove a delivered event once it is older than this.
    pub delivered_grace: Option<Duration>,
    /// Remove any event older than this, regardless of delivery state.
    pub max_age: Option<Duration>,
    /// Keep at most this many of the most recent events by append order.
    pub max_count: Option<usize>,
    /// Keep at most roughly this many serialized bytes, dropping oldest first.
    pub max_bytes: Option<u64>,
}

impl AutoRetention {
    /// The unbounded policy (retain everything) — the default.
    pub fn unbounded() -> Self {
        Self::default()
    }

    /// Whether the policy imposes no bound, so the store can skip retention work.
    pub fn is_unbounded(&self) -> bool {
        self.delivered_grace.is_none()
            && self.max_age.is_none()
            && self.max_count.is_none()
            && self.max_bytes.is_none()
    }
}

/// Decide which events to keep, in append order, under an [`AutoRetention`] policy
/// evaluated at `now`. Pure, so it is testable without touching disk. An event is
/// dropped when it is past `max_age`, or delivered and past `delivered_grace`;
/// the surviving set is then bounded by `max_count` and `max_bytes`.
fn retain_events(
    events: &[EventEnvelope],
    delivered: &HashSet<EventId>,
    policy: &AutoRetention,
    now: DateTime<Utc>,
) -> Vec<EventEnvelope> {
    let kept = events
        .iter()
        .filter(|event| {
            let age = now - event.timestamp;
            if policy.max_age.is_some_and(|max_age| age > max_age) {
                return false;
            }
            if let Some(grace) = policy.delivered_grace
                && delivered.contains(&event.id)
                && age > grace
            {
                return false;
            }
            true
        })
        .cloned()
        .collect();
    apply_size_bounds(kept, policy)
}

/// Decide which dead-trigger records to keep, in append order, under `policy`.
/// The delivered-grace bound does not apply — a dead-trigger has no delivery
/// state — so records are bounded only by the hard `max_age`/`max_count`/
/// `max_bytes` valves that keep the queue from growing without limit.
fn retain_dead_triggers(
    records: &[DeadTriggerRecord],
    policy: &AutoRetention,
    now: DateTime<Utc>,
) -> Vec<DeadTriggerRecord> {
    let kept = records
        .iter()
        .filter(|record| {
            policy
                .max_age
                .is_none_or(|max| now - record.event.timestamp <= max)
        })
        .cloned()
        .collect();
    apply_size_bounds(kept, policy)
}

/// Bound an ordered, already-age-filtered set by `max_count` then `max_bytes`,
/// dropping the oldest (front) first. Shared by event and dead-trigger retention.
fn apply_size_bounds<T: Serialize>(mut kept: Vec<T>, policy: &AutoRetention) -> Vec<T> {
    if let Some(max) = policy.max_count
        && kept.len() > max
    {
        kept.drain(0..kept.len() - max);
    }
    if let Some(max_bytes) = policy.max_bytes {
        // Walk newest-to-oldest, keeping records while within the byte budget;
        // drop the oldest that overflow it.
        let mut budget = max_bytes;
        let mut first_kept = kept.len();
        for (index, record) in kept.iter().enumerate().rev() {
            let size = serialized_len(record);
            if size > budget {
                break;
            }
            budget -= size;
            first_kept = index;
        }
        kept.drain(0..first_kept);
    }
    kept
}

/// The approximate on-disk footprint of one record: its serialized JSON plus the
/// trailing newline. Used only to bound `max_bytes` retention.
fn serialized_len<T: Serialize>(record: &T) -> u64 {
    serde_json::to_vec(record)
        .map(|bytes| bytes.len() as u64 + 1)
        .unwrap_or(0)
}

pub trait EventStore: Send + Sync {
    /// Persist an accepted event. A durable backend returns an error on a
    /// storage failure (disk full, permission, interrupted write) rather than
    /// aborting the process.
    fn append(&self, event: EventEnvelope) -> io::Result<()>;
    fn get(&self, id: EventId) -> Option<EventEnvelope>;
    fn all(&self) -> Vec<EventEnvelope>;

    /// Durably mark an event delivered — every trigger matched for it has had its
    /// job accepted by Worklane or recorded to the dead-trigger queue, so it is
    /// eligible for grace-based automatic retention. The default is a no-op for
    /// backends that do not track delivery.
    fn mark_delivered(&self, _id: EventId) -> io::Result<()> {
        Ok(())
    }

    /// Retained events that are not yet delivered, in append order — the in-flight
    /// set a restart replays. The default conservatively treats every retained
    /// event as undelivered.
    fn undelivered(&self) -> Vec<EventEnvelope> {
        self.all()
    }

    /// Apply an automatic, bounded [`AutoRetention`] policy evaluated at `now`,
    /// returning the number of events removed. The default retains everything.
    fn enforce_retention(&self, _policy: &AutoRetention, _now: DateTime<Utc>) -> io::Result<usize> {
        Ok(0)
    }

    /// Number of retained events. The default counts `all()`; a backend that
    /// tracks a length cheaply may override it.
    fn len(&self) -> usize {
        self.all().len()
    }

    /// Whether the store currently holds no events.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate on-disk size of the store in bytes; `0` for non-durable
    /// backends. Used for the store-size metric.
    fn size_bytes(&self) -> u64 {
        0
    }

    /// Return the first stored event whose idempotency key equals `key`, if any.
    /// The default is a linear scan over `all()`; a backend with an index may
    /// override it.
    fn find_by_idempotency_key(&self, key: &str) -> Option<EventEnvelope> {
        self.all()
            .into_iter()
            .find(|event| event.metadata.idempotency_key.as_deref() == Some(key))
    }

    /// Prune stored events per `policy`, returning the number removed. Pruning is
    /// operator-driven and never happens automatically. A durable backend returns
    /// an error if the compaction fails, leaving the store unchanged.
    fn prune(&self, policy: &ManualRetention) -> io::Result<usize>;
}

#[derive(Default)]
pub struct InMemoryEventStore {
    events: Mutex<Vec<EventEnvelope>>,
    delivered: Mutex<HashSet<EventId>>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl EventStore for InMemoryEventStore {
    fn append(&self, event: EventEnvelope) -> io::Result<()> {
        lock(&self.events).push(event);
        Ok(())
    }

    fn get(&self, id: EventId) -> Option<EventEnvelope> {
        lock(&self.events)
            .iter()
            .find(|event| event.id == id)
            .cloned()
    }

    fn all(&self) -> Vec<EventEnvelope> {
        lock(&self.events).clone()
    }

    fn mark_delivered(&self, id: EventId) -> io::Result<()> {
        lock(&self.delivered).insert(id);
        Ok(())
    }

    fn undelivered(&self) -> Vec<EventEnvelope> {
        let delivered = lock(&self.delivered);
        lock(&self.events)
            .iter()
            .filter(|event| !delivered.contains(&event.id))
            .cloned()
            .collect()
    }

    fn enforce_retention(&self, policy: &AutoRetention, now: DateTime<Utc>) -> io::Result<usize> {
        if policy.is_unbounded() {
            return Ok(0);
        }
        let mut events = lock(&self.events);
        let mut delivered = lock(&self.delivered);
        let kept = retain_events(&events, &delivered, policy, now);
        let removed = events.len() - kept.len();
        if removed > 0 {
            let kept_ids: HashSet<EventId> = kept.iter().map(|event| event.id).collect();
            delivered.retain(|id| kept_ids.contains(id));
            *events = kept;
        }
        Ok(removed)
    }

    fn prune(&self, policy: &ManualRetention) -> io::Result<usize> {
        let mut events = lock(&self.events);
        let kept = policy.keep(&events, |event| event.timestamp);
        let removed = events.len() - kept.len();
        *events = kept;
        Ok(removed)
    }

    fn len(&self) -> usize {
        lock(&self.events).len()
    }
}

/// Durable, append-only JSONL store: one JSON record per line, reread on open,
/// atomically compactable. Shared mechanism behind the event store and the
/// dead-trigger queue; each wraps a `JsonlStore<T>` and adds its record-specific
/// accessors. Durable mutations are fallible and reflect in memory only after
/// the durable write succeeds.
struct JsonlStore<T> {
    path: PathBuf,
    records: Mutex<Vec<T>>,
    file: Mutex<File>,
}

impl<T> JsonlStore<T>
where
    T: Clone + Serialize + DeserializeOwned,
{
    fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let records = read_jsonl(&path)?;
        // A brand-new file's directory entry must be fsynced too, or a crash
        // could lose the file itself even after its data is durable.
        let newly_created = !path.exists();
        let file = open_locked_append(&path)?;
        if newly_created {
            sync_parent_dir(&path)?;
        }
        Ok(Self {
            path,
            records: Mutex::new(records),
            file: Mutex::new(file),
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn append(&self, record: T) -> io::Result<()> {
        // Build body + newline as one buffer and write it with a single
        // `write_all`: a torn line cannot result from a partial first write, and
        // an `O_APPEND` write up to `PIPE_BUF` is atomic on Linux. The advisory
        // lock taken in `open` is the real guard for larger records.
        let mut encoded = serde_json::to_vec(&record).map_err(io::Error::other)?;
        encoded.push(b'\n');
        let mut file = lock(&self.file);

        file.write_all(&encoded)?;
        // Durable-by-acknowledgement: persist the bytes and the file-size
        // metadata needed to read them back before reporting success. `sync_data`
        // (fdatasync) is cheaper than `sync_all`, which would also flush unrelated
        // inode metadata.
        file.sync_data()?;

        // Reflect in memory only after the durable append succeeds.
        lock(&self.records).push(record);
        Ok(())
    }

    fn all(&self) -> Vec<T> {
        lock(&self.records).clone()
    }

    fn find(&self, predicate: impl Fn(&T) -> bool) -> Option<T> {
        lock(&self.records)
            .iter()
            .find(|record| predicate(record))
            .cloned()
    }

    fn len(&self) -> usize {
        lock(&self.records).len()
    }

    /// Current on-disk size of the backing file in bytes (`0` if it is missing).
    fn size_bytes(&self) -> u64 {
        std::fs::metadata(&self.path)
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    }

    fn drain(&self) -> io::Result<Vec<T>> {
        // Lock file then records, matching `append`'s order, so a concurrent
        // append cannot deadlock against a drain. Clear the durable backing
        // first; take the in-memory records only after truncation succeeds, so a
        // storage failure leaves the store intact.
        let file = lock(&self.file);
        let mut records = lock(&self.records);
        file.set_len(0)?;
        file.sync_all()?;
        Ok(std::mem::take(&mut *records))
    }

    /// Atomically rewrite the store to contain exactly `kept`, swapping the
    /// durable file and in-memory set only after the rewrite succeeds.
    fn replace_all(&self, kept: Vec<T>) -> io::Result<()> {
        let mut file = lock(&self.file);
        let mut records = lock(&self.records);
        *file = compact_jsonl(&self.path, &kept)?;
        *records = kept;
        Ok(())
    }

    /// Recompute the retained set from the *live* records and compact to it, all
    /// under one held lock. `keep` receives the current records and returns the
    /// survivors. Holding the lock across compute-and-swap is what makes this
    /// safe against a concurrent `append`: unlike snapshot-then-`replace_all`, an
    /// event appended during retention is seen by `keep` (and kept) rather than
    /// overwritten away by a stale survivor set. Mirrors `prune`'s discipline.
    fn retain(&self, keep: impl FnOnce(&[T]) -> Vec<T>) -> io::Result<usize> {
        // Lock file then records, matching `append`'s order, so a concurrent
        // append cannot deadlock against retention.
        let mut file = lock(&self.file);
        let mut records = lock(&self.records);
        let kept = keep(&records);
        let removed = records.len() - kept.len();
        if removed > 0 {
            *file = compact_jsonl(&self.path, &kept)?;
            *records = kept;
        }
        Ok(removed)
    }

    fn prune(
        &self,
        policy: &ManualRetention,
        timestamp: impl Fn(&T) -> DateTime<Utc>,
    ) -> io::Result<usize> {
        // Lock file then records, matching `append`'s order, so a concurrent
        // append cannot deadlock against a prune.
        let mut file = lock(&self.file);
        let mut records = lock(&self.records);
        let kept = policy.keep(&records, timestamp);
        let removed = records.len() - kept.len();
        if removed > 0 {
            // Swap in the compacted file and in-memory set only after the durable
            // rewrite succeeds; on error the store is left unchanged.
            *file = compact_jsonl(&self.path, &kept)?;
            *records = kept;
        }
        Ok(removed)
    }
}

/// Durable, file-backed event store: a `JsonlStore` of `EventEnvelope` records,
/// plus a sibling `JsonlStore` of delivered event ids and an in-memory membership
/// set so delivery state survives a restart and drives automatic retention.
pub struct FileEventStore {
    inner: JsonlStore<EventEnvelope>,
    delivered: JsonlStore<EventId>,
    delivered_set: Mutex<HashSet<EventId>>,
}

/// The path of the delivered-ids journal that sits beside an event store file.
fn delivered_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".delivered");
    PathBuf::from(name)
}

impl FileEventStore {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let inner = JsonlStore::open(path)?;
        // The delivered-ids journal is a sibling file with the same durability and
        // single-writer guarantees; the in-memory set is rebuilt from it on open.
        let delivered = JsonlStore::open(delivered_path(inner.path()))?;
        let delivered_set = delivered.all().into_iter().collect();
        Ok(Self {
            inner,
            delivered,
            delivered_set: Mutex::new(delivered_set),
        })
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }
}

impl EventStore for FileEventStore {
    fn append(&self, event: EventEnvelope) -> io::Result<()> {
        self.inner.append(event)
    }

    fn get(&self, id: EventId) -> Option<EventEnvelope> {
        self.inner.find(|event| event.id == id)
    }

    fn all(&self) -> Vec<EventEnvelope> {
        self.inner.all()
    }

    fn mark_delivered(&self, id: EventId) -> io::Result<()> {
        // Append to the durable journal only the first time, then cache in memory.
        if lock(&self.delivered_set).contains(&id) {
            return Ok(());
        }
        self.delivered.append(id)?;
        lock(&self.delivered_set).insert(id);
        Ok(())
    }

    fn undelivered(&self) -> Vec<EventEnvelope> {
        let delivered = lock(&self.delivered_set);
        self.inner
            .all()
            .into_iter()
            .filter(|event| !delivered.contains(&event.id))
            .collect()
    }

    fn enforce_retention(&self, policy: &AutoRetention, now: DateTime<Utc>) -> io::Result<usize> {
        if policy.is_unbounded() {
            return Ok(0);
        }
        // Snapshot the delivered set: it only governs the grace decision on
        // already-delivered events, so staleness here can never drop a recent
        // undelivered event. The survivor set itself is computed from the *live*
        // event log under `retain`'s lock, so a concurrent append is kept.
        let delivered = lock(&self.delivered_set).clone();
        let mut kept_ids: HashSet<EventId> = HashSet::new();
        let removed = self.inner.retain(|events| {
            let kept = retain_events(events, &delivered, policy, now);
            kept_ids = kept.iter().map(|event| event.id).collect();
            kept
        })?;
        if removed > 0 {
            // The event log is compacted; now drop the now-orphaned ids from the
            // delivered journal. Losing a delivered id is at worst a redundant
            // re-delivery, never data loss, so it need not share the lock.
            let kept_delivered: Vec<EventId> = delivered
                .iter()
                .copied()
                .filter(|id| kept_ids.contains(id))
                .collect();
            self.delivered.replace_all(kept_delivered.clone())?;
            *lock(&self.delivered_set) = kept_delivered.into_iter().collect();
        }
        Ok(removed)
    }

    fn prune(&self, policy: &ManualRetention) -> io::Result<usize> {
        self.inner.prune(policy, |event| event.timestamp)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn size_bytes(&self) -> u64 {
        // Both the event log and the delivered-ids journal count toward footprint.
        self.inner.size_bytes() + self.delivered.size_bytes()
    }
}

/// A terminal trigger-handling failure: the failed event, the trigger that
/// produced it, and the error. Persisted so trigger-side failures survive a
/// restart and can be inspected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadTriggerRecord {
    pub event: EventEnvelope,
    pub trigger_name: String,
    pub error: String,
}

/// A store for terminal trigger-handling failures (the dead-trigger queue). The
/// trigger plane's counterpart to a Worklane dead-letter store.
pub trait DeadTriggerQueue: Send + Sync {
    /// Persist a terminal trigger-handling failure. A durable backend returns an
    /// error on a storage failure rather than aborting, so the caller can report
    /// that dead-trigger persistence itself failed.
    fn record(&self, record: DeadTriggerRecord) -> io::Result<()>;
    fn all(&self) -> Vec<DeadTriggerRecord>;
    /// Atomically return all records and empty the queue, including its durable
    /// backing. Used by retry to take work out so only records that fail again
    /// are written back via [`DeadTriggerQueue::record`]. In-memory records are
    /// taken only after the durable backing is cleared, so a storage failure
    /// leaves the queue intact.
    fn drain(&self) -> io::Result<Vec<DeadTriggerRecord>>;

    /// Prune records per `policy` (by the record's event timestamp), returning
    /// the number removed. Operator-driven; never automatic. A durable backend
    /// returns an error if compaction fails, leaving the queue unchanged.
    fn prune(&self, policy: &ManualRetention) -> io::Result<usize>;

    /// Current number of records, without cloning them. Used for the
    /// dead-trigger-depth metric.
    fn len(&self) -> usize;

    /// Approximate on-disk size of the queue in bytes; `0` for non-durable
    /// backends. Used for the store-size metric.
    fn size_bytes(&self) -> u64 {
        0
    }

    /// Apply automatic, hard-bound retention (max age/count/bytes) so the queue
    /// cannot grow without limit when triggers keep failing. The delivered-grace
    /// bound does not apply to dead-triggers. Returns the number removed; the
    /// default retains everything.
    fn enforce_retention(&self, _policy: &AutoRetention, _now: DateTime<Utc>) -> io::Result<usize> {
        Ok(0)
    }

    /// Whether the queue is currently empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Default)]
pub struct InMemoryDeadTriggerQueue {
    records: Mutex<Vec<DeadTriggerRecord>>,
}

impl InMemoryDeadTriggerQueue {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DeadTriggerQueue for InMemoryDeadTriggerQueue {
    fn record(&self, record: DeadTriggerRecord) -> io::Result<()> {
        lock(&self.records).push(record);
        Ok(())
    }

    fn all(&self) -> Vec<DeadTriggerRecord> {
        lock(&self.records).clone()
    }

    fn drain(&self) -> io::Result<Vec<DeadTriggerRecord>> {
        Ok(std::mem::take(&mut *lock(&self.records)))
    }

    fn prune(&self, policy: &ManualRetention) -> io::Result<usize> {
        let mut records = lock(&self.records);
        let kept = policy.keep(&records, |record| record.event.timestamp);
        let removed = records.len() - kept.len();
        *records = kept;
        Ok(removed)
    }

    fn enforce_retention(&self, policy: &AutoRetention, now: DateTime<Utc>) -> io::Result<usize> {
        if policy.is_unbounded() {
            return Ok(0);
        }
        let mut records = lock(&self.records);
        let kept = retain_dead_triggers(&records, policy, now);
        let removed = records.len() - kept.len();
        if removed > 0 {
            *records = kept;
        }
        Ok(removed)
    }

    fn len(&self) -> usize {
        lock(&self.records).len()
    }
}

/// Durable, file-backed dead-trigger queue: a `JsonlStore` of `DeadTriggerRecord`
/// records, reread on open so failures survive a process restart.
pub struct FileDeadTriggerQueue {
    inner: JsonlStore<DeadTriggerRecord>,
}

impl FileDeadTriggerQueue {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self {
            inner: JsonlStore::open(path)?,
        })
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }
}

impl DeadTriggerQueue for FileDeadTriggerQueue {
    fn record(&self, record: DeadTriggerRecord) -> io::Result<()> {
        self.inner.append(record)
    }

    fn all(&self) -> Vec<DeadTriggerRecord> {
        self.inner.all()
    }

    fn drain(&self) -> io::Result<Vec<DeadTriggerRecord>> {
        self.inner.drain()
    }

    fn prune(&self, policy: &ManualRetention) -> io::Result<usize> {
        self.inner.prune(policy, |record| record.event.timestamp)
    }

    fn enforce_retention(&self, policy: &AutoRetention, now: DateTime<Utc>) -> io::Result<usize> {
        if policy.is_unbounded() {
            return Ok(0);
        }
        // Compute the survivor set from the live queue under `retain`'s lock so a
        // concurrent `record` is kept rather than overwritten by a stale set.
        self.inner
            .retain(|records| retain_dead_triggers(records, policy, now))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn size_bytes(&self) -> u64 {
        self.inner.size_bytes()
    }
}

/// Open a store file for appending and take an advisory exclusive lock on it for
/// the lifetime of the returned handle. A second opener of the same file fails
/// fast instead of interleaving writes or appending into a renamed-away inode.
/// The lock is released when the handle is dropped.
fn open_locked_append(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    file.try_lock_exclusive().map_err(|error| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "store {} is already locked by another writer: {error}",
                path.display()
            ),
        )
    })?;
    Ok(file)
}

/// Fsync the parent directory of `path` so a newly created or renamed file's
/// directory entry is itself durable, not just the file's contents.
fn sync_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Atomically rewrite a JSONL store to contain exactly `kept`: write to a sibling
/// temp file, fsync, then rename over the original (atomic on the same
/// filesystem, so a crash mid-prune leaves either the old or new file, never a
/// half-written one), then fsync the directory so the rename is durable. Returns
/// a fresh, freshly-locked append handle on the rewritten file.
fn compact_jsonl<T: Serialize>(path: &Path, kept: &[T]) -> io::Result<File> {
    let tmp = path.with_extension("compact");
    {
        let mut writer = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        for item in kept {
            let encoded = serde_json::to_vec(item).map_err(io::Error::other)?;
            writer.write_all(&encoded)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        writer.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    sync_parent_dir(path)?;
    open_locked_append(path)
}

/// Read a JSONL file into records, skipping blank lines. A missing file is an
/// empty store. A single torn trailing record — a final line that fails to parse
/// when the file does not end in a newline, the signature of a crash between an
/// append's write and its completion — is discarded so the store still opens. Any
/// other malformed line (not the last, or last but newline terminated) is an
/// `InvalidData` error naming the path and line. Shared by every `JsonlStore`.
fn read_jsonl<T: DeserializeOwned>(path: &Path) -> io::Result<Vec<T>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let ends_with_newline = bytes.last() == Some(&b'\n');
    let text = std::str::from_utf8(&bytes).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid UTF-8 in {}: {error}", path.display()),
        )
    })?;

    // (original line number, content), blank lines dropped.
    let lines: Vec<(usize, &str)> = text
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .collect();
    let last = lines.len().saturating_sub(1);

    let mut records = Vec::with_capacity(lines.len());
    for (position, (index, line)) in lines.iter().enumerate() {
        match serde_json::from_str(line) {
            Ok(record) => records.push(record),
            // Tolerate exactly one torn trailing fragment from a crash mid-append.
            Err(_) if position == last && !ends_with_newline => break,
            Err(error) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid JSON in {} on line {}: {error}",
                        path.display(),
                        index + 1
                    ),
                ));
            }
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use bytes::Bytes;
    use triggerlane_core::{
        EVENT_CRON_DAILY, EVENT_GITHUB_ISSUE_CREATED, EventEnvelope, EventMetadata, Source,
    };

    use super::*;

    #[test]
    fn in_memory_store_preserves_event_for_replay() {
        let store = InMemoryEventStore::new();
        let event = EventEnvelope::new(Source::Cron, EVENT_CRON_DAILY, Bytes::from_static(b"{}"));
        let id = event.id;

        store.append(event.clone()).unwrap();

        assert_eq!(store.get(id), Some(event));
        assert_eq!(store.all().len(), 1);
    }

    #[test]
    fn file_store_reopens_persisted_event_for_replay() {
        let path = test_store_path("reopen");
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        )
        .with_metadata(EventMetadata {
            trace_id: Some("trace-1".to_owned()),
            correlation_id: Some("delivery-1".to_owned()),
            tenant_id: Some("tenant-1".to_owned()),
            idempotency_key: Some("idem-1".to_owned()),
            causation_id: None,
        });
        let id = event.id;

        {
            let store = FileEventStore::open(&path).expect("store should open");
            store.append(event.clone()).expect("append");
        }

        let reopened = FileEventStore::open(&path).expect("store should reopen");

        assert_eq!(reopened.get(id), Some(event));
        remove_test_store(path);
    }

    #[test]
    fn file_store_lists_reopened_events_in_append_order() {
        let path = test_store_path("ordered");
        let first = EventEnvelope::new(
            Source::Manual,
            EVENT_CRON_DAILY,
            Bytes::from_static(b"first"),
        );
        let second = EventEnvelope::new(
            Source::Cron,
            EVENT_CRON_DAILY,
            Bytes::from_static(b"second"),
        );

        {
            let store = FileEventStore::open(&path).expect("store should open");
            store.append(first.clone()).unwrap();
            store.append(second.clone()).unwrap();
        }

        let reopened = FileEventStore::open(&path).expect("store should reopen");

        assert_eq!(reopened.all(), [first, second]);
        remove_test_store(path);
    }

    #[test]
    fn file_dead_trigger_queue_rereads_after_reopen() {
        let path = test_store_path("dtq");
        let event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(br#"{"issue":1}"#),
        );
        let record = DeadTriggerRecord {
            event,
            trigger_name: "github-issue".to_owned(),
            error: "submission failed".to_owned(),
        };

        {
            let queue = FileDeadTriggerQueue::open(&path).expect("queue should open");
            queue.record(record.clone()).unwrap();
            assert_eq!(queue.all(), vec![record.clone()]);
        }

        let reopened = FileDeadTriggerQueue::open(&path).expect("queue should reopen");
        assert_eq!(reopened.all(), vec![record]);
        remove_test_store(path);
    }

    #[test]
    fn in_memory_drain_returns_all_and_empties() {
        let queue = InMemoryDeadTriggerQueue::new();
        queue.record(dead_record("a")).unwrap();
        queue.record(dead_record("b")).unwrap();

        let drained = queue.drain().unwrap();

        assert_eq!(drained.len(), 2);
        assert!(queue.all().is_empty());
    }

    #[test]
    fn file_drain_truncates_durable_backing_and_allows_reappend() {
        let path = test_store_path("dtq-drain");
        // Each handle holds the single-writer lock, so it must be dropped before
        // the file is reopened.
        let after = dead_record("c");
        {
            let queue = FileDeadTriggerQueue::open(&path).expect("queue should open");
            queue.record(dead_record("a")).unwrap();
            queue.record(dead_record("b")).unwrap();

            let drained = queue.drain().unwrap();
            assert_eq!(drained.len(), 2);
            assert!(queue.all().is_empty());
        }

        {
            // A reopen after draining reads an empty durable backing.
            let reopened = FileDeadTriggerQueue::open(&path).expect("queue should reopen");
            assert!(reopened.all().is_empty());

            // Records appended after a drain still persist.
            reopened.record(after.clone()).unwrap();
        }

        let reopened_again = FileDeadTriggerQueue::open(&path).expect("queue should reopen");
        assert_eq!(reopened_again.all(), vec![after]);

        remove_test_store(path);
    }

    #[test]
    fn torn_trailing_line_is_recovered_on_open() {
        let path = test_store_path("torn");
        let good = event_at("2026-06-20T00:00:00Z");
        {
            let store = FileEventStore::open(&path).expect("store should open");
            store.append(good.clone()).unwrap();
        }
        // Simulate a crash mid-append: a partial, unterminated JSON fragment with
        // no trailing newline appended after the last complete record.
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(br#"{"id":"trunc"#).unwrap();
        }

        let reopened =
            FileEventStore::open(&path).expect("torn final line should be tolerated on open");
        assert_eq!(reopened.all(), vec![good]);
        remove_test_store(path);
    }

    #[test]
    fn corrupt_non_final_line_fails_open() {
        let path = test_store_path("corrupt-mid");
        {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap();
            // A corrupt line that is NOT the torn tail: it is followed by another.
            file.write_all(b"not json\n{\"also\":\"bad\"}\n").unwrap();
        }
        let error = match FileEventStore::open(&path) {
            Ok(_) => panic!("open must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        remove_test_store(path);
    }

    #[test]
    fn corrupt_newline_terminated_final_line_fails_open() {
        let path = test_store_path("corrupt-final");
        {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap();
            // Bad JSON, but newline terminated — a fully written corrupt record,
            // not a torn tail, so it must not be silently dropped.
            file.write_all(b"{\"broken\": \n").unwrap();
        }
        let error = match FileEventStore::open(&path) {
            Ok(_) => panic!("open must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        remove_test_store(path);
    }

    #[test]
    fn second_open_of_same_file_fails_fast() {
        let path = test_store_path("locked");
        let first = FileEventStore::open(&path).expect("first open should succeed");
        let error = match FileEventStore::open(&path) {
            Ok(_) => panic!("open must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        drop(first);
        // Once the lock is released, reopening succeeds.
        let _reopened = FileEventStore::open(&path).expect("reopen after drop should succeed");
        remove_test_store(path);
    }

    #[test]
    fn lock_helper_recovers_from_poisoning() {
        let mutex = Mutex::new(vec![1, 2, 3]);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mutex.lock().unwrap();
            panic!("poison the mutex");
        }));
        assert!(mutex.is_poisoned());
        // The helper recovers the still-consistent value instead of panicking.
        assert_eq!(*lock(&mutex), vec![1, 2, 3]);
    }

    #[test]
    fn in_memory_event_store_prunes_by_age_and_count() {
        let store = InMemoryEventStore::new();
        let old = event_at("2020-01-01T00:00:00Z");
        let mid = event_at("2026-06-01T00:00:00Z");
        let new = event_at("2026-06-20T00:00:00Z");
        store.append(old).unwrap();
        store.append(mid.clone()).unwrap();
        store.append(new.clone()).unwrap();

        // Older-than keeps the cutoff boundary and newer, drops older.
        let cutoff: DateTime<Utc> = "2026-06-01T00:00:00Z".parse().expect("cutoff parses");
        let removed = store.prune(&ManualRetention::OlderThan(cutoff)).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(store.all(), vec![mid, new.clone()]);

        // Keep-most-recent bounds the count.
        let removed = store.prune(&ManualRetention::KeepMostRecent(1)).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(store.all(), vec![new]);
    }

    #[test]
    fn file_event_store_prune_compacts_durable_backing() {
        let path = test_store_path("evt-prune");
        let keep = event_at("2026-06-20T00:00:00Z");
        {
            let store = FileEventStore::open(&path).expect("store should open");
            store.append(event_at("2020-01-01T00:00:00Z")).unwrap();
            store.append(keep.clone()).unwrap();
            let removed = store.prune(&ManualRetention::KeepMostRecent(1)).unwrap();
            assert_eq!(removed, 1);
            // Appends after a prune still persist.
            store.append(event_at("2026-06-21T00:00:00Z")).unwrap();
        }

        let reopened = FileEventStore::open(&path).expect("store should reopen");
        let ids: Vec<_> = reopened.all();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0].id, keep.id);
        remove_test_store(path);
    }

    #[test]
    fn file_dead_trigger_queue_prune_compacts_durable_backing() {
        let path = test_store_path("dtq-prune");
        {
            let queue = FileDeadTriggerQueue::open(&path).expect("queue should open");
            queue
                .record(dead_record_at("old", "2020-01-01T00:00:00Z"))
                .unwrap();
            queue
                .record(dead_record_at("new", "2026-06-20T00:00:00Z"))
                .unwrap();
            let cutoff: DateTime<Utc> = "2026-06-01T00:00:00Z".parse().expect("cutoff parses");
            let removed = queue.prune(&ManualRetention::OlderThan(cutoff)).unwrap();
            assert_eq!(removed, 1);
        }

        let reopened = FileDeadTriggerQueue::open(&path).expect("queue should reopen");
        let all = reopened.all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].trigger_name, "new");
        remove_test_store(path);
    }

    fn now() -> DateTime<Utc> {
        "2026-06-20T00:00:00Z".parse().expect("now parses")
    }

    #[test]
    fn auto_retention_removes_delivered_aged_events_keeps_undelivered() {
        let store = InMemoryEventStore::new();
        let old_delivered = event_at("2026-06-01T00:00:00Z");
        let old_undelivered = event_at("2026-06-01T00:00:00Z");
        let recent = event_at("2026-06-19T18:00:00Z");
        store.append(old_delivered.clone()).unwrap();
        store.append(old_undelivered.clone()).unwrap();
        store.append(recent.clone()).unwrap();
        store.mark_delivered(old_delivered.id).unwrap();

        let policy = AutoRetention {
            delivered_grace: Some(Duration::days(7)),
            ..AutoRetention::unbounded()
        };
        let removed = store.enforce_retention(&policy, now()).unwrap();

        // Only the delivered, aged event goes; the aged-but-undelivered one stays.
        assert_eq!(removed, 1);
        let ids: Vec<_> = store.all().into_iter().map(|event| event.id).collect();
        assert_eq!(ids, vec![old_undelivered.id, recent.id]);
    }

    #[test]
    fn auto_retention_hard_max_age_removes_undelivered() {
        let store = InMemoryEventStore::new();
        let old = event_at("2026-06-01T00:00:00Z");
        let recent = event_at("2026-06-19T18:00:00Z");
        store.append(old).unwrap();
        store.append(recent.clone()).unwrap();

        let policy = AutoRetention {
            max_age: Some(Duration::days(7)),
            ..AutoRetention::unbounded()
        };
        let removed = store.enforce_retention(&policy, now()).unwrap();

        assert_eq!(removed, 1);
        assert_eq!(store.all(), vec![recent]);
    }

    #[test]
    fn auto_retention_unbounded_removes_nothing() {
        let store = InMemoryEventStore::new();
        let old = event_at("2020-01-01T00:00:00Z");
        store.append(old.clone()).unwrap();
        store.mark_delivered(old.id).unwrap();

        let removed = store
            .enforce_retention(&AutoRetention::unbounded(), now())
            .unwrap();
        assert_eq!(removed, 0);
        assert_eq!(store.all(), vec![old]);
    }

    #[test]
    fn auto_retention_max_count_keeps_most_recent() {
        let store = InMemoryEventStore::new();
        let a = event_at("2026-06-01T00:00:00Z");
        let b = event_at("2026-06-02T00:00:00Z");
        let c = event_at("2026-06-03T00:00:00Z");
        store.append(a).unwrap();
        store.append(b.clone()).unwrap();
        store.append(c.clone()).unwrap();

        let policy = AutoRetention {
            max_count: Some(2),
            ..AutoRetention::unbounded()
        };
        let removed = store.enforce_retention(&policy, now()).unwrap();

        assert_eq!(removed, 1);
        assert_eq!(store.all(), vec![b, c]);
    }

    #[test]
    fn file_delivery_state_survives_reopen() {
        let path = test_store_path("delivery");
        let delivered = event_at("2026-06-19T00:00:00Z");
        let pending = event_at("2026-06-19T00:00:00Z");
        {
            let store = FileEventStore::open(&path).expect("store should open");
            store.append(delivered.clone()).unwrap();
            store.append(pending.clone()).unwrap();
            store.mark_delivered(delivered.id).unwrap();
            // Marking twice does not double-write the journal.
            store.mark_delivered(delivered.id).unwrap();
        }

        let reopened = FileEventStore::open(&path).expect("store should reopen");
        let undelivered: Vec<_> = reopened.undelivered().into_iter().map(|e| e.id).collect();
        assert_eq!(undelivered, vec![pending.id]);

        remove_test_store_with_sidecar(path);
    }

    #[test]
    fn file_auto_retention_compacts_event_log_and_journal() {
        let path = test_store_path("file-retention");
        let old_delivered = event_at("2026-06-01T00:00:00Z");
        let recent = event_at("2026-06-19T18:00:00Z");
        let policy = AutoRetention {
            delivered_grace: Some(Duration::days(7)),
            ..AutoRetention::unbounded()
        };
        {
            let store = FileEventStore::open(&path).expect("store should open");
            store.append(old_delivered.clone()).unwrap();
            store.append(recent.clone()).unwrap();
            store.mark_delivered(old_delivered.id).unwrap();
            assert_eq!(store.enforce_retention(&policy, now()).unwrap(), 1);
        }

        // The compacted event log AND delivered journal persist across reopen.
        let reopened = FileEventStore::open(&path).expect("store should reopen");
        let ids: Vec<_> = reopened.all().into_iter().map(|e| e.id).collect();
        assert_eq!(ids, vec![recent.id]);
        // The orphaned delivered id was dropped, so the survivor reads undelivered.
        let undelivered: Vec<_> = reopened.undelivered().into_iter().map(|e| e.id).collect();
        assert_eq!(undelivered, vec![recent.id]);

        remove_test_store_with_sidecar(path);
    }

    #[test]
    fn file_store_reports_len_and_nonzero_size() {
        let path = test_store_path("metrics");
        let store = FileEventStore::open(&path).expect("store should open");
        assert_eq!(store.len(), 0);
        assert_eq!(store.size_bytes(), 0);

        store.append(event_at("2026-06-20T00:00:00Z")).unwrap();
        store.append(event_at("2026-06-20T01:00:00Z")).unwrap();

        assert_eq!(store.len(), 2);
        assert!(store.size_bytes() > 0, "a written store has nonzero size");

        remove_test_store_with_sidecar(path);
    }

    #[test]
    fn in_memory_store_reports_len_and_zero_size() {
        let store = InMemoryEventStore::new();
        store.append(event_at("2026-06-20T00:00:00Z")).unwrap();
        assert_eq!(store.len(), 1);
        // Non-durable backends report no on-disk footprint.
        assert_eq!(store.size_bytes(), 0);
    }

    #[test]
    fn in_memory_dead_trigger_queue_retention_bounds_by_count() {
        let queue = InMemoryDeadTriggerQueue::new();
        queue.record(dead_record("a")).unwrap();
        queue.record(dead_record("b")).unwrap();
        queue.record(dead_record("c")).unwrap();

        let policy = AutoRetention {
            max_count: Some(2),
            ..AutoRetention::unbounded()
        };
        let removed = queue.enforce_retention(&policy, now()).unwrap();

        assert_eq!(removed, 1);
        let names: Vec<_> = queue
            .all()
            .into_iter()
            .map(|record| record.trigger_name)
            .collect();
        assert_eq!(names, vec!["b".to_owned(), "c".to_owned()]);
    }

    #[test]
    fn dead_trigger_queue_retention_ignores_delivered_grace() {
        // A grace-only policy has no effect on dead-triggers (no delivery state).
        let queue = InMemoryDeadTriggerQueue::new();
        queue
            .record(dead_record_at("old", "2020-01-01T00:00:00Z"))
            .unwrap();
        let policy = AutoRetention {
            delivered_grace: Some(Duration::days(1)),
            ..AutoRetention::unbounded()
        };
        assert_eq!(queue.enforce_retention(&policy, now()).unwrap(), 0);
        assert_eq!(queue.all().len(), 1);
    }

    #[test]
    fn file_dead_trigger_queue_retention_compacts_by_age() {
        let path = test_store_path("dtq-retention");
        let policy = AutoRetention {
            max_age: Some(Duration::days(7)),
            ..AutoRetention::unbounded()
        };
        {
            let queue = FileDeadTriggerQueue::open(&path).expect("queue should open");
            queue
                .record(dead_record_at("old", "2020-01-01T00:00:00Z"))
                .unwrap();
            queue
                .record(dead_record_at("new", "2026-06-20T00:00:00Z"))
                .unwrap();
            assert_eq!(queue.enforce_retention(&policy, now()).unwrap(), 1);
        }

        let reopened = FileDeadTriggerQueue::open(&path).expect("queue should reopen");
        let names: Vec<_> = reopened
            .all()
            .into_iter()
            .map(|record| record.trigger_name)
            .collect();
        assert_eq!(names, vec!["new".to_owned()]);
        remove_test_store(path);
    }

    #[test]
    fn file_auto_retention_does_not_drop_events_appended_during_a_sweep() {
        // Regression: retention must compute its survivor set from the *live* log
        // under lock, not from a snapshot it then overwrites. With the snapshot
        // bug, an event appended while a sweep is mid-flight is silently dropped
        // even though its append already returned (and fsynced) success — a
        // durability violation. Seed removable old-delivered events so every sweep
        // actually compacts (removed > 0, firing the rewrite), then hammer appends
        // of recent undelivered events concurrently and assert none are lost.
        use std::{sync::Arc, thread};

        let path = test_store_path("retention-race");
        let store = Arc::new(FileEventStore::open(&path).expect("store should open"));

        // 100 old, delivered events that each sweep is entitled to remove.
        let mut removable = Vec::new();
        for _ in 0..100 {
            let event = event_at("2020-01-01T00:00:00Z");
            store.append(event.clone()).unwrap();
            store.mark_delivered(event.id).unwrap();
            removable.push(event.id);
        }

        let policy = AutoRetention {
            delivered_grace: Some(Duration::days(7)),
            ..AutoRetention::unbounded()
        };

        // Appender: 200 recent, undelivered events that retention must keep.
        let appender = {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let mut ids = Vec::new();
                for _ in 0..200 {
                    let event = event_at("2026-06-19T18:00:00Z");
                    store.append(event.clone()).unwrap();
                    ids.push(event.id);
                }
                ids
            })
        };

        // Sweeper: keep enforcing retention while the appender runs.
        for _ in 0..200 {
            store.enforce_retention(&policy, now()).unwrap();
        }
        let appended: HashSet<EventId> = appender
            .join()
            .expect("appender thread")
            .into_iter()
            .collect();
        // A final sweep to settle.
        store.enforce_retention(&policy, now()).unwrap();

        let surviving: HashSet<EventId> = store.all().into_iter().map(|event| event.id).collect();
        // Every recent, undelivered event survived — none was overwritten away.
        let lost: Vec<_> = appended.difference(&surviving).collect();
        assert!(
            lost.is_empty(),
            "{} acknowledged events were lost",
            lost.len()
        );
        // The old delivered events were eligible for removal (sweeps did compact).
        assert!(removable.iter().all(|id| !surviving.contains(id)));

        remove_test_store_with_sidecar(path);
    }

    fn remove_test_store_with_sidecar(path: PathBuf) {
        let _ = fs::remove_file(delivered_path(&path));
        remove_test_store(path);
    }

    fn event_at(ts: &str) -> EventEnvelope {
        let timestamp: DateTime<Utc> = ts.parse().expect("timestamp parses");
        let mut event = EventEnvelope::new(
            Source::GitHub,
            EVENT_GITHUB_ISSUE_CREATED,
            Bytes::from_static(b"{}"),
        );
        event.timestamp = timestamp;
        event
    }

    fn dead_record_at(trigger: &str, ts: &str) -> DeadTriggerRecord {
        DeadTriggerRecord {
            event: event_at(ts),
            trigger_name: trigger.to_owned(),
            error: "submission failed".to_owned(),
        }
    }

    fn dead_record(trigger: &str) -> DeadTriggerRecord {
        DeadTriggerRecord {
            event: EventEnvelope::new(
                Source::GitHub,
                EVENT_GITHUB_ISSUE_CREATED,
                Bytes::from_static(br#"{"issue":1}"#),
            ),
            trigger_name: trigger.to_owned(),
            error: "submission failed".to_owned(),
        }
    }

    fn test_store_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "triggerlane-storage-{name}-{}-{nonce}.jsonl",
            std::process::id()
        ))
    }

    fn remove_test_store(path: PathBuf) {
        fs::remove_file(path).expect("test store should be removed");
    }
}
