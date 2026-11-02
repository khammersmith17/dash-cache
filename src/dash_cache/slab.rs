use crate::core::{CacheError, GetResult, SlabShard, SlabShardBuilder};
use crate::queue::{self, CacheQueue};
use crate::shared_vec::SharedVecRef;
use crate::stats::{AtomicStats, CacheStats};
use std::collections::HashSet;
use std::hash::{BuildHasher, Hash};
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::RwLock;

// The initial capacity provided to the overflow queues when used.
const OVERFLOW_QUEUE_INITIAL_SIZE: usize = 32_usize;

// Multiplier that determines how much the queued cache state updates can overflow.
// Used to bound the memory of the eviction and promotion queues.
const FORCED_DRAIN_MULTIPLE: usize = 2_usize;

// Slab type couples the cache shard and the promotion queue together.
//
// On read, the write lock would only be required to perform the promotion of the touched entry.
// Given this, all reads are queued and the promotion only occurs when the write lock is acquired.
// This amortizes the cost reads, and eliminates lock contention on read, otherwise the RwLock
// effectively becomes Mutex. This allows for semantics better inline with read/write locking a
// critical section.
//
// Queued promotions ar fired every call that requires the write lock. Firing them off all
// sequentially allows for all promotions to be performed with a single write lock acquistion and
// may improve the cache locality, and potentially lowering the overhead of cache invalidation on
// other cores, given that all promotions in a single queued batch will result in a single
// observed cache invalidation on another core, per MESI.
#[derive(Debug)]
struct Slab<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync,
{
    shard: SlabShard<K, V, S, AtomicStats, SharedVecRef<K, V>>,
    promotion: CacheQueue,
    eviction: CacheQueue,
    promotion_overflow: Mutex<Vec<u32>>,
    eviction_overflow: Mutex<Vec<u32>>,
    eviction_keys: HashSet<K>,
}

impl<K, V, S> Slab<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync,
{
    fn new(
        shard: SlabShard<K, V, S, AtomicStats, SharedVecRef<K, V>>,
        eviction_queue_size: usize,
        promotion_queue_size: usize,
    ) -> Slab<K, V, S> {
        Slab {
            shard,
            promotion: CacheQueue::new(promotion_queue_size),
            eviction: CacheQueue::new(eviction_queue_size),
            promotion_overflow: Mutex::new(Vec::new()),
            eviction_overflow: Mutex::new(Vec::new()),
            eviction_keys: HashSet::with_capacity(queue::DEFAULT_BUFFER_SIZE),
        }
    }

    /// Decide whether an early drain should occur to bound consumed memory.
    /// Only one caller will be assigned the early promotion for each type.
    /// In the case that two callers trigger a drain from the promotion queue and eviction queue
    /// separately, then two callers will trigger the drain.
    ///
    /// In all other cases, only one caller will attempt the lock upgrade.
    fn should_drain_early(&self, handle_size: usize, capacity: usize) -> bool {
        let drain_factor = capacity * (FORCED_DRAIN_MULTIPLE - 1);
        // If the queue is at capacity, and a multiple of the capacity.
        // In this case, only one held read lock attempts to acquire the write lock.
        // If the task is canceled, then the next caller at multiple of `drain_factor`
        // will perform the drain.
        (handle_size >= drain_factor) && handle_size.is_multiple_of(drain_factor)
    }

    /// Queue an entries promotion. If the concurrent queue is full, write to the overflow queue.
    /// The overflow queue is only allocated capacity when it is needed.
    /// Returns to the caller if an early drain should occur.
    #[must_use]
    fn queue_promotion(&self, entry_idx: u32) -> bool {
        if self.promotion.try_push(entry_idx).is_err() {
            let mut handle = self.promotion_overflow.lock().unwrap();

            // Lazily provide the overflow queue with capacity.
            if handle.is_empty() {
                handle.reserve(OVERFLOW_QUEUE_INITIAL_SIZE);
            }
            handle.push(entry_idx);
            return self.should_drain_early(handle.len(), self.promotion.capacity());
        }
        false
    }

    /// Queue an entries eviction. If the concurrent queue is full, write to the overflow queue.
    /// The overflow queue is only allocated capacity when it is needed.
    /// Returns to the caller if an early drain should occur.
    #[must_use]
    fn queue_eviction(&self, entry_idx: u32) -> bool {
        if self.eviction.try_push(entry_idx).is_err() {
            let mut handle = self.eviction_overflow.lock().unwrap();

            // Lazily provide the overflow queue with capacity.
            if handle.is_empty() {
                handle.reserve(OVERFLOW_QUEUE_INITIAL_SIZE);
            }
            handle.push(entry_idx);

            return self.should_drain_early(handle.len(), self.eviction.capacity());
        }
        false
    }

    fn drain_queues(&mut self) {
        // Replace with an empty Vec so space is only allocated when it is needed.
        let promotion_overflow = std::mem::take(self.promotion_overflow.get_mut().unwrap());
        let eviction_overflow = std::mem::take(self.eviction_overflow.get_mut().unwrap());

        perform_queued_actions(
            self.promotion.drain().chain(promotion_overflow),
            self.eviction.drain().chain(eviction_overflow),
            &mut self.shard,
            &mut self.eviction_keys,
        );
    }
}

/// Fewest entries a shard holds when the shard count is chosen by default. Per-shard LRU needs some
/// room to work: with only one or two entries per shard, eviction is decided by which keys hash to
/// the same shard rather than by recency.
pub(super) const MIN_DEFAULT_SHARD_CAPACITY: usize = 8;

// 4 shards per CPU by default, capped so each shard still holds at least
// `MIN_DEFAULT_SHARD_CAPACITY` entries. A small cache gets fewer shards, down to one. Without the
// cap, a cache with fewer entries than shards would round every shard up to one entry and hold far
// more than the requested capacity.
pub(super) fn default_shard_count(capacity: usize) -> usize {
    (num_cpus::get() * 4)
        .min(capacity / MIN_DEFAULT_SHARD_CAPACITY)
        .max(1)
}

// Performs all required state updates queued on the acquisition of the write lock.
fn perform_queued_actions<K, V, S, I>(
    promotions: I,
    evictions: I,
    slab: &mut SlabShard<K, V, S, AtomicStats, SharedVecRef<K, V>>,
    keys: &mut HashSet<K>,
) where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync,
    I: Iterator<Item = u32>,
{
    // Perform promotions first, so entries are not invalidated by eviction.
    promote_on_writes(promotions, slab);
    evict_on_writes(evictions, slab, keys);
}

fn promote_on_writes<K, V, S, I>(
    promotions: I,
    slab: &mut SlabShard<K, V, S, AtomicStats, SharedVecRef<K, V>>,
) where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync,
    I: Iterator<Item = u32>,
{
    // Promotions do not need to kept track of. If the same key is promote twice in a row, then the
    // promotion is a no-op. Duplicate promotions in this queue also maintain correct state. A
    // promotion does not mutate the slab, rather only mutates the list pointers.
    for entry in promotions {
        slab.promote_existing_entry(entry);
    }
}

fn evict_on_writes<K, V, S, I>(
    evictions: I,
    slab: &mut SlabShard<K, V, S, AtomicStats, SharedVecRef<K, V>>,
    keys: &mut HashSet<K>,
) where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync,
    I: Iterator<Item = u32>,
{
    /*
     * Eviction can invalidate other entries in the slab, thus we first need to resolve the keys so
     * the entry indexes can be resolved through a lookup in the map. Otherwise there is a risk of
     * removing entries that have been moved, but not expired.
     *
     * Thus, the keys need to first be resolved before any mutation occurs, then evict is performed
     * using the key.
     *
     * This does incur a clone, but keys are clone and _should_ be cheap to clone.
     * */
    keys.clear();
    for entry in evictions {
        let key = slab.get_key_from_entry_position(entry);
        if keys.contains(&key) {
            continue;
        }
        keys.insert(key);
    }

    for key in keys.iter() {
        slab.evict(key);
    }
}

// wrap CacheShard in RwLock for better type semantics
#[derive(Debug)]
pub(super) struct LockedCache<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync,
{
    handle: RwLock<Slab<K, V, S>>,
}

// wrapper methods around the CacheShard shard internal to a shard
// This level on the type abstraction contains all concurrency primitives present in the type
// All write operations flush the shards promotion promotion, as explained above, to amortize the cost
// of promotion on a read.
impl<K, V, S> LockedCache<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    pub(super) fn new(
        slab_ref: SharedVecRef<K, V>,
        cap: NonZeroUsize,
        hasher: S,
        promotion_queue_size: usize,
        eviction_queue_size: usize,
        default_ttl: Option<Duration>,
    ) -> LockedCache<K, V, S> {
        let mut builder = SlabShardBuilder::new(cap).with_hasher(hasher);
        if let Some(ttl) = default_ttl {
            builder = builder.with_default_ttl(ttl);
        }
        let shard = builder.build_with_backend::<SharedVecRef<K, V>, AtomicStats>(slab_ref);
        let handle = RwLock::new(Slab::new(shard, eviction_queue_size, promotion_queue_size));
        LockedCache { handle }
    }

    pub(super) async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) {
        let mut guard = self.handle.write().await;
        guard.drain_queues();
        guard.shard.insert_with_ttl(key, value, ttl);
    }

    pub(super) async fn checkout<Q>(&self, key: &Q) -> Option<(K, V, u64)>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        // This is not a write operation, but does take a write lock.
        // Given the held write lock, any reads are promoted.

        guard.drain_queues();
        guard.shard.evict_full_entry(key)
    }

    pub(super) async fn insert_with_expires(&self, key: K, value: V, expires: u64) {
        let mut guard = self.handle.write().await;
        guard.drain_queues();
        guard.shard.insert_with_expires(key, value, expires);
    }

    pub(super) async fn update_with_ttl<Q>(
        &self,
        key: &Q,
        value: V,
        ttl: Duration,
    ) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        guard.drain_queues();
        guard.shard.update_with_ttl(key, value, ttl)?;

        Ok(())
    }

    pub(super) async fn len(&self) -> usize {
        let guard = self.handle.read().await;
        let Slab { ref shard, .. } = *guard;
        shard.len()
    }

    pub(super) async fn insert(&self, key: K, value: V) {
        let mut guard = self.handle.write().await;
        guard.drain_queues();
        guard.shard.insert(key, value)
    }

    pub(super) async fn drain(&self) {
        let mut guard = self.handle.write().await;
        guard.drain_queues();
        guard.shard.drain();
    }

    pub(super) async fn statistics(&self) -> CacheStats {
        let guard = self.handle.read().await;
        let Slab { ref shard, .. } = *guard;
        shard.statistics()
    }

    pub(super) async fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let guard = self.handle.read().await;

        // Attempt to get without promoting/evicting.
        // If either queue is full, drop the read lock, acquire the write lock and performed the
        // queued operations.
        match guard.shard.get_no_promote(key) {
            GetResult::Miss => None,
            GetResult::Hit(v, entry_idx) => {
                if guard.queue_promotion(entry_idx) {
                    drop(guard);
                    self.early_drain().await;
                };
                Some(v)
            }
            GetResult::Expired(entry_idx) => {
                if guard.queue_eviction(entry_idx) {
                    drop(guard);
                    self.early_drain().await;
                };
                None
            }
        }
    }

    // When the queue has reached overflow capacity, perform all the queued actions on a read
    // action.
    async fn early_drain(&self) {
        let mut guard = self.handle.write().await;
        guard.drain_queues();
    }

    pub(super) async fn evict<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        guard.drain_queues();
        guard.shard.evict(key)
    }

    pub(super) async fn update<Q>(&self, key: &Q, value: V) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        guard.drain_queues();

        guard.shard.update(key, value)?;
        Ok(())
    }

    pub(super) async fn contains<Q>(&self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let guard = self.handle.read().await;
        let Slab { ref shard, .. } = *guard;
        shard.contains(key)
    }
}

#[cfg(test)]
mod early_drain_tests {
    use super::*;
    use crate::shared_vec::SharedVec;
    use std::sync::Arc;

    const CAP: usize = 8;
    const QUEUE: usize = 4;
    // Miri's clock advances with the instructions it interprets, so it needs a far longer timeout.
    const TIMEOUT: Duration = Duration::from_secs(if cfg!(miri) { 600 } else { 5 });
    const READS_PER_TASK: u64 = if cfg!(miri) { 50 } else { 500 };

    // Overflow length at which a reader drains both queues early.
    const DRAIN_AT: usize = QUEUE * (FORCED_DRAIN_MULTIPLE - 1);

    // A single shard with its backing allocation. Fields drop in order, so the shard is dropped
    // before the memory it points into.
    struct TestShard {
        cache: LockedCache<u64, u64>,
        _backing: SharedVec<u64, u64>,
    }

    fn shard() -> TestShard {
        let cap = NonZeroUsize::new(CAP).unwrap();
        let backing = SharedVec::new(cap, NonZeroUsize::new(1).unwrap());
        let slab_ref = backing.make_shards().pop().unwrap();
        let cache = LockedCache::new(
            slab_ref,
            cap,
            ahash::RandomState::with_seeds(1, 2, 3, 4),
            QUEUE,
            QUEUE,
            None,
        );
        TestShard {
            cache,
            _backing: backing,
        }
    }

    async fn promotion_overflow_len(cache: &LockedCache<u64, u64>) -> usize {
        cache
            .handle
            .read()
            .await
            .promotion_overflow
            .lock()
            .unwrap()
            .len()
    }

    // Regression test: the reader that triggered the early drain asked for the write lock while
    // still holding its read lock, and waited forever. Reads only, so nothing but the early drain
    // empties the overflow.
    #[tokio::test]
    async fn read_only_promotions_drain_early() {
        let s = shard();
        for k in 0..CAP as u64 {
            s.cache.insert(k, k).await;
        }

        tokio::time::timeout(TIMEOUT, async {
            for i in 0..10 * (QUEUE + DRAIN_AT) {
                assert_eq!(s.cache.get(&0).await, Some(0));
                let overflow = promotion_overflow_len(&s.cache).await;
                assert!(
                    overflow < DRAIN_AT,
                    "read {i}: overflow holds {overflow} promotions, never drained at {DRAIN_AT}"
                );
            }
        })
        .await
        .expect("early drain deadlocked");

        // Key 0 was inserted first, so it was least recently used until its promotions were
        // applied. The next eviction must take key 1 instead.
        s.cache.insert(100, 100).await;
        assert_eq!(s.cache.get(&0).await, Some(0));
        assert_eq!(s.cache.get(&1).await, None);
    }

    // Expired entries found by reads are queued for eviction. With no writes, only the early drain
    // can remove them.
    #[tokio::test]
    async fn read_only_expirations_drain_early() {
        let s = shard();
        for k in 0..CAP as u64 {
            s.cache
                .insert_with_ttl(k, k, Duration::from_millis(1))
                .await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Fill the eviction queue, then the overflow up to the drain point.
        tokio::time::timeout(TIMEOUT, async {
            for _ in 0..QUEUE + DRAIN_AT {
                assert_eq!(s.cache.get(&0).await, None);
            }
        })
        .await
        .expect("early drain deadlocked");

        assert_eq!(
            s.cache.len().await,
            CAP - 1,
            "the expired key was not evicted by an early drain"
        );
    }

    // Many readers overflow the same queue at once. Only the reader that reaches each drain point
    // takes the write lock; nothing may hang, and once every read has returned the overflow is
    // below the drain point.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_readers_drain_early() {
        let s = Arc::new(shard());
        for k in 0..CAP as u64 {
            s.cache.insert(k, k).await;
        }

        let readers: Vec<_> = (0..8)
            .map(|t| {
                let s = Arc::clone(&s);
                tokio::spawn(async move {
                    for i in 0..READS_PER_TASK {
                        let k = (t + i) % CAP as u64;
                        assert_eq!(s.cache.get(&k).await, Some(k));
                    }
                })
            })
            .collect();

        tokio::time::timeout(TIMEOUT, async {
            for reader in readers {
                reader.await.unwrap();
            }
        })
        .await
        .expect("early drain deadlocked");

        let overflow = promotion_overflow_len(&s.cache).await;
        assert!(
            overflow < DRAIN_AT,
            "overflow holds {overflow} promotions after all reads returned"
        );
    }
}
