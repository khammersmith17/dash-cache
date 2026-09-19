use crate::core::{CacheError, GetResult, SlabShard, SlabShardBuilder};
use crate::queue::{self, CacheQueue};
use crate::shared_vec::SharedVecRef;
use crate::stats::{AtomicStats, CacheStats};
use std::collections::HashSet;
use std::hash::{BuildHasher, Hash};
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::sync::RwLock;

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
            promotion: CacheQueue::new(promotion_queue_size),
            eviction: CacheQueue::new(eviction_queue_size),
            eviction_keys: HashSet::with_capacity(queue::DEFAULT_BUFFER_SIZE),
            shard,
        }
    }
}

// 4 shards per cpu by default.
pub(super) fn default_shard_count() -> usize {
    num_cpus::get() * 4
}

// Performs all required state updates queued on the acquisition of the write lock.
fn perform_cleanup<K, V, S, I>(
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
        slab.promote(entry);
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
        slab.evict(&key);
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
        let Slab {
            ref mut promotion,
            ref mut shard,
            ref mut eviction,
            ref mut eviction_keys,
        } = *guard;
        perform_cleanup(promotion.drain(), eviction.drain(), shard, eviction_keys);
        shard.insert_with_ttl(key, value, ttl);
    }

    pub(super) async fn checkout<Q>(&self, key: &Q) -> Option<(K, V, u64)>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut promotion,
            ref mut shard,
            ref mut eviction,
            ref mut eviction_keys,
        } = *guard;

        // This is not a write operation, but does take a write lock.
        // Given the held write lock, any reads are promoted.
        perform_cleanup(promotion.drain(), eviction.drain(), shard, eviction_keys);
        shard.evict_full_entry(key)
    }

    pub(super) async fn insert_with_expires(&self, key: K, value: V, expires: u64) {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut promotion,
            ref mut shard,
            ref mut eviction,
            ref mut eviction_keys,
        } = *guard;
        perform_cleanup(promotion.drain(), eviction.drain(), shard, eviction_keys);
        shard.insert_with_expires(key, value, expires);
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
        let Slab {
            ref mut promotion,
            ref mut shard,
            ref mut eviction,
            ref mut eviction_keys,
        } = *guard;
        perform_cleanup(promotion.drain(), eviction.drain(), shard, eviction_keys);
        shard.update_with_ttl(key, value, ttl)?;

        Ok(())
    }

    pub(super) async fn len(&self) -> usize {
        let guard = self.handle.read().await;
        let Slab { ref shard, .. } = *guard;
        shard.len()
    }

    pub(super) async fn insert(&self, key: K, value: V) {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut promotion,
            ref mut shard,
            ref mut eviction,
            ref mut eviction_keys,
        } = *guard;
        perform_cleanup(promotion.drain(), eviction.drain(), shard, eviction_keys);
        shard.insert(key, value)
    }

    pub(super) async fn drain(&self) {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut promotion,
            ref mut shard,
            ref mut eviction,
            ref mut eviction_keys,
        } = *guard;
        perform_cleanup(promotion.drain(), eviction.drain(), shard, eviction_keys);
        shard.drain();
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
        let Slab {
            ref shard,
            ref promotion,
            ref eviction,
            ..
        } = *guard;
        match shard.get_no_promote(key) {
            GetResult::Miss => None,
            GetResult::Hit(v, entry_idx) => {
                promotion.push(entry_idx);
                Some(v)
            }
            GetResult::Expired(entry_idx) => {
                eviction.push(entry_idx);
                None
            }
        }
    }

    pub(super) async fn evict<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut shard,
            ref mut promotion,
            ref mut eviction,
            ref mut eviction_keys,
        } = *guard;
        perform_cleanup(promotion.drain(), eviction.drain(), shard, eviction_keys);
        shard.evict(key)
    }

    pub(super) async fn update<Q>(&self, key: &Q, value: V) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut shard,
            ref mut promotion,
            ref mut eviction,
            ref mut eviction_keys,
        } = *guard;
        perform_cleanup(promotion.drain(), eviction.drain(), shard, eviction_keys);

        shard.update(key, value)?;
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
