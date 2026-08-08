use crate::buffer::PromotionBuffer;
use crate::core::{CacheError, SlabShard};
use crate::guard::CacheEntryGuard;
use crate::stats::{AtomicStats, CacheStats};
use ahash::AHasher;
use futures::stream::{self, StreamExt};
use std::hash::Hasher;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
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
    shard: SlabShard<K, V, S, AtomicStats>,
    buffer: PromotionBuffer,
}

fn default_shard_count() -> usize {
    num_cpus::get() * 8
}

fn promote_on_writes<K, V, S>(promotions: &[u32], slab: &mut SlabShard<K, V, S, AtomicStats>)
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync,
{
    for entry in promotions {
        slab.promote(*entry);
    }
}

// wrap CacheShard in RwLock for better type semantics
#[derive(Debug)]
struct LockedCache<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync,
{
    handle: RwLock<Slab<K, V, S>>,
}

// wrapper methods around the CacheShard shard internal to a shard
// This level on the type abstraction contains all concurrency primitives present in the type
// All write operations flush the shards promotion buffer, as explained above, to amortize the cost
// of promotion on a read.
impl<K, V, S> LockedCache<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    fn with_capacity_and_hasher(cap: NonZeroUsize, hasher: S) -> LockedCache<K, V, S> {
        let shard: SlabShard<K, V, S, AtomicStats> =
            SlabShard::with_capacity_and_hasher(cap, hasher);
        let handle = RwLock::new(Slab {
            buffer: PromotionBuffer::default(),
            shard,
        });
        LockedCache { handle }
    }

    fn with_capacity_and_hasher_and_default_ttl(
        cap: NonZeroUsize,
        hasher: S,
        default_ttl: Duration,
    ) -> LockedCache<K, V, S> {
        let shard: SlabShard<K, V, S, AtomicStats> =
            SlabShard::with_capacity_and_hasher_and_default_ttl(cap, hasher, default_ttl);
        let handle = RwLock::new(Slab {
            buffer: PromotionBuffer::default(),
            shard,
        });

        LockedCache { handle }
    }

    async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut buffer,
            ref mut shard,
        } = *guard;

        let entries = buffer.drain();
        promote_on_writes(&entries, shard);
        shard.insert_with_ttl(key, value, ttl);
    }

    async fn checkout<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut shard,
            ref mut buffer,
        } = *guard;
        // This is not a write operation, but does take a write lock.
        // Given the held write lock, any reads are promoted.
        let entries = buffer.drain();
        promote_on_writes(&entries, shard);
        shard.evict_full_entry(key)
    }

    async fn update_with_ttl<Q>(&self, key: &Q, value: V, ttl: Duration) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut buffer,
            ref mut shard,
        } = *guard;
        let entries = buffer.drain();
        promote_on_writes(&entries, shard);
        shard.update_with_ttl(key, value, ttl)?;

        Ok(())
    }

    async fn len(&self) -> usize {
        let guard = self.handle.read().await;
        let Slab { ref shard, .. } = *guard;
        shard.len()
    }

    async fn insert(&self, key: K, value: V) {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut shard,
            ref mut buffer,
        } = *guard;
        let entries = buffer.drain();
        promote_on_writes(&entries, shard);
        shard.insert(key, value)
    }

    async fn drain(&self) {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut shard,
            ref mut buffer,
        } = *guard;
        let entries = buffer.drain();
        promote_on_writes(&entries, shard);
        shard.drain();
    }

    async fn statistics(&self) -> CacheStats {
        let guard = self.handle.read().await;
        let Slab { ref shard, .. } = *guard;
        shard.statistics()
    }

    async fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let guard = self.handle.read().await;
        let Slab {
            ref shard,
            ref buffer,
        } = *guard;
        let Some((v, entry_idx)) = shard.get_no_promote(key) else {
            return None;
        };
        buffer.push(entry_idx);
        Some(v)
    }

    async fn evict<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut shard,
            ref mut buffer,
        } = *guard;
        let entries = buffer.drain();
        promote_on_writes(&entries, shard);
        shard.evict(key)
    }

    async fn update<Q>(&self, key: &Q, value: V) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut guard = self.handle.write().await;
        let Slab {
            ref mut shard,
            ref mut buffer,
        } = *guard;
        let entries = buffer.drain();
        promote_on_writes(&entries, shard);
        shard.update(key, value)?;
        Ok(())
    }

    async fn contains<Q>(&self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let guard = self.handle.read().await;
        let Slab { ref shard, .. } = *guard;
        shard.contains(key)
    }
}

/// Builder for [`DashCache`].
///
/// Construct one via [`DashCacheBuilder::new`], optionally set [`with_num_shards`] and/or
/// [`with_hasher`], then call [`build`] to obtain a [`DashCache`].
///
/// If `with_num_shards` is not called, the cache defaults to one shard per logical CPU core and
/// distributes `capacity` evenly across them (ceiling division). If `with_hasher` is not called,
/// `ahash::RandomState` is used.
///
/// [`with_num_shards`]: DashCacheBuilder::with_num_shards
/// [`with_hasher`]: DashCacheBuilder::with_hasher
/// [`build`]: DashCacheBuilder::build
#[derive(Debug)]
pub struct DashCacheBuilder<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync,
{
    cap: NonZeroUsize,
    num_shards: Option<NonZeroUsize>,
    hasher: S,
    default_ttl: Option<Duration>,
    _type_marker: PhantomData<(K, V)>,
}

impl<K, V> DashCacheBuilder<K, V, ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Creates a builder with the given total capacity and `ahash::RandomState` as the default
    /// hasher. Call [`with_num_shards`](DashCacheBuilder::with_num_shards) and/or
    /// [`with_hasher`](DashCacheBuilder::with_hasher) to customise, then
    /// [`build`](DashCacheBuilder::build) to construct the cache.
    pub fn new(capacity: NonZeroUsize) -> DashCacheBuilder<K, V> {
        DashCacheBuilder {
            cap: capacity,
            num_shards: None,
            hasher: ahash::RandomState::new(),
            default_ttl: None,
            _type_marker: PhantomData,
        }
    }
}

impl<K, V, S> DashCacheBuilder<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync,
{
    /// Sets the number of shards. Per-shard capacity is computed as `ceil(total_cap / num_shards)`
    /// when [`build`](DashCacheBuilder::build) is called, so total capacity may be slightly above
    /// the value passed to [`new`](DashCacheBuilder::new) when it is not evenly divisible.
    pub fn with_num_shards(mut self, num_shards: NonZeroUsize) -> DashCacheBuilder<K, V, S> {
        self.num_shards = Some(num_shards);
        self
    }

    /// Sets a default TTL applied to every entry inserted via [`DashCache::insert`]. Entries
    /// inserted via [`DashCache::insert_with_ttl`] override this per-call.
    pub fn with_default_ttl(mut self, ttl: Duration) -> DashCacheBuilder<K, V, S> {
        self.default_ttl = Some(ttl);
        self
    }

    /// Replaces the hasher, retyping the builder. Any state set before this call (capacity,
    /// num_shards) is preserved. Because this changes the `S` type parameter, the returned builder
    /// is `DashCacheBuilder<K, V, H>` rather than `DashCacheBuilder<K, V, S>`.
    ///
    /// The hasher must implement `Clone` because each shard receives its own clone of the state.
    pub fn with_hasher<H: BuildHasher + Clone + Send + Sync>(
        self,
        hasher: H,
    ) -> DashCacheBuilder<K, V, H> {
        let DashCacheBuilder {
            cap,
            num_shards,
            default_ttl,
            ..
        } = self;
        DashCacheBuilder {
            cap,
            num_shards,
            hasher,
            default_ttl,
            _type_marker: PhantomData,
        }
    }

    /// Consumes the builder and returns a [`DashCache`].
    ///
    /// If [`with_num_shards`](DashCacheBuilder::with_num_shards) was called, the cache has exactly
    /// that many shards, each with capacity `ceil(total_cap / num_shards)`. Otherwise, the number
    /// of shards defaults to the number of logical CPU cores.
    pub fn build(self) -> DashCache<K, V, S> {
        let DashCacheBuilder {
            cap,
            num_shards,
            hasher,
            default_ttl,
            ..
        } = self;
        let inner = match num_shards {
            Some(n) => {
                let shard_capacity = (cap.get() as f64 / n.get() as f64).ceil() as usize;
                InnerCacheShards::with_num_shards_and_capacity_and_hasher(
                    n,
                    NonZeroUsize::new(shard_capacity).unwrap(),
                    hasher,
                    default_ttl,
                )
            }
            None => InnerCacheShards::new_with_hasher(cap, hasher, default_ttl),
        };
        DashCache {
            inner: Arc::new(inner),
        }
    }
}

/// A concurrent, sharded LRU cache inspired by `DashMap`.
///
/// Keys are routed to shards by hash, so lock contention under concurrent access is proportional
/// to the number of shards rather than the total capacity. Each shard is a `SlabShard` wrapped in
/// a `tokio::RwLock`.
///
/// LRU ordering is per-shard: eviction picks the least recently used entry within a shard, not
/// globally across the whole cache.
///
/// `DashCache` wraps its shards in an `Arc` internally, so cloning is cheap and no external `Arc`
/// is required.
#[derive(Clone, Debug)]
pub struct DashCache<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync + 'static,
{
    inner: Arc<InnerCacheShards<K, V, S>>,
}

impl<K, V> DashCache<K, V, ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(capacity: NonZeroUsize) -> DashCache<K, V, ahash::RandomState> {
        DashCache::new_with_hasher(capacity, ahash::RandomState::new())
    }

    /// Creates a `DashCache` with an explicit shard count and per-shard capacity.
    ///
    /// Total capacity is `num_shards * shard_capacity`.
    pub fn with_num_shards_and_capacity(
        num_shards: NonZeroUsize,
        shard_capacity: NonZeroUsize,
    ) -> DashCache<K, V, ahash::RandomState> {
        DashCache::with_num_shards_and_capacity_and_hasher(
            num_shards,
            shard_capacity,
            ahash::RandomState::new(),
        )
    }
}

impl<K, V, S> DashCache<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync,
{
    /// Creates a `DashCache` with one shard per logical CPU core.
    ///
    /// Shard capacity is `ceil(cap / cpu_count)`, with a minimum of 1. If `cap` is not evenly
    /// divisible the total capacity will be slightly above `cap`.
    pub fn new_with_hasher(cap: NonZeroUsize, hasher: S) -> DashCache<K, V, S> {
        let inner = Arc::new(InnerCacheShards::<K, V, S>::new_with_hasher(
            cap, hasher, None,
        ));

        DashCache { inner }
    }

    /// Creates a `DashCache` with an explicit shard count and per-shard capacity.
    ///
    /// Total capacity is `num_shards * shard_capacity`.
    pub fn with_num_shards_and_capacity_and_hasher(
        num_shards: NonZeroUsize,
        shard_capacity: NonZeroUsize,
        hasher: S,
    ) -> DashCache<K, V, S> {
        let inner = Arc::new(InnerCacheShards::with_num_shards_and_capacity_and_hasher(
            num_shards,
            shard_capacity,
            hasher,
            None,
        ));

        DashCache { inner }
    }

    /// Returns a clone of the value for the given key and promotes it to most recently used within
    /// its shard. Acquires a write lock on the key's shard. Returns `None` on a cache miss.
    pub async fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(key).await
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, its value is updated and it is promoted to most recently used
    /// within its shard. If the shard is full and the key is new, the shard's least recently used
    /// entry is evicted. Acquires a write lock on the key's shard.
    pub async fn insert(&self, key: K, value: V) {
        self.inner.insert(key, value).await;
    }

    /// Inserts a key-value pair with an explicit TTL, overriding any default TTL set on the cache.
    pub async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) {
        self.inner.insert_with_ttl(key, value, ttl).await;
    }

    /// Updates the value for an existing key with an explicit TTL and promotes it to most recently
    /// used within its shard. Returns `Err(CacheError::KeyNotExist)` if the key is not present.
    pub async fn update_with_ttl<Q>(
        &self,
        key: &Q,
        value: V,
        ttl: Duration,
    ) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.update_with_ttl(key, value, ttl).await?;
        Ok(())
    }

    /// Returns `true` if the key exists in the cache without promoting it or recording a hit.
    ///
    /// This is the only read-only method — it acquires a read lock and does not modify the shard.
    pub async fn contains<Q>(&self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains(key).await
    }

    /// Returns a snapshot of hit, miss, and eviction counts aggregated across all shards.
    /// Acquires a read lock on each shard sequentially.
    pub async fn statistics(&self) -> CacheStats {
        self.inner.statistics().await
    }

    /// Removes the entry for the given key and returns its value, or `None` if the key is not
    /// present. Acquires a write lock on the key's shard.
    pub async fn evict<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.evict(key).await
    }

    /// Returns the total number of entries across all shards.
    pub async fn len(&self) -> usize {
        self.inner.len().await
    }

    pub async fn checkout<Q>(&self, key: &Q) -> Option<CacheEntryGuard<K, V, S>>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (key, value) = self.inner.checkout(key).await?;
        Some(CacheEntryGuard::new(key, value, self.clone()))
    }

    /// Returns whether the entire cache is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0_usize
    }

    /// Updates the value for an existing key and promotes it to most recently used within its shard.
    ///
    /// Returns `Err(CacheError::KeyNotExist)` if the key is not in the cache — use `insert` to
    /// write a new key. There is no `get_mut`. Acquires a write lock on the key's shard.
    pub async fn update<Q>(&self, key: &Q, value: V) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.update(key, value).await?;
        Ok(())
    }

    /// Removes all entries from every shard.
    pub async fn drain(&self) {
        self.inner.drain().await;
    }

    /// Returns the number of shards.
    pub fn num_shards(&self) -> usize {
        usize::from(self.inner.num_shards)
    }
}

// Owns the shard array and routes all operations to the correct shard via key hash.
#[derive(Debug)]
struct InnerCacheShards<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync,
{
    cache_shards: Box<[LockedCache<K, V, S>]>,
    num_shards: NonZeroUsize,
}

impl<K, T, S> InnerCacheShards<K, T, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    fn new_with_hasher(
        capacity: NonZeroUsize,
        hasher: S,
        default_ttl: Option<Duration>,
    ) -> InnerCacheShards<K, T, S> {
        let shard_count = default_shard_count();
        let cap = capacity.get();

        let shard_capacity =
            NonZeroUsize::new(((cap as f32 / shard_count as f32).ceil() as usize).max(1_usize))
                .unwrap();

        let shards_vec: Vec<LockedCache<K, T, S>> = (0..shard_count)
            .map(|_| {
                let h = hasher.clone();
                match default_ttl {
                    Some(ttl) => LockedCache::with_capacity_and_hasher_and_default_ttl(
                        shard_capacity,
                        h,
                        ttl,
                    ),
                    None => LockedCache::with_capacity_and_hasher(shard_capacity, h),
                }
            })
            .collect();

        let cache_shards = shards_vec.into_boxed_slice();
        let num_shards = unsafe { NonZeroUsize::new_unchecked(shard_count) };

        InnerCacheShards {
            cache_shards,
            num_shards,
        }
    }

    fn with_num_shards_and_capacity_and_hasher(
        num_shards: NonZeroUsize,
        shard_capacity: NonZeroUsize,
        hasher: S,
        default_ttl: Option<Duration>,
    ) -> InnerCacheShards<K, T, S> {
        let shard_count = num_shards.get();
        let shards_vec: Vec<LockedCache<K, T, S>> = (0..num_shards.get())
            .map(|_| {
                let h = hasher.clone();
                match default_ttl {
                    Some(ttl) => LockedCache::with_capacity_and_hasher_and_default_ttl(
                        shard_capacity,
                        h,
                        ttl,
                    ),
                    None => LockedCache::with_capacity_and_hasher(shard_capacity, h),
                }
            })
            .collect();

        let cache_shards = shards_vec.into_boxed_slice();
        let num_shards = unsafe { NonZeroUsize::new_unchecked(shard_count) };

        InnerCacheShards {
            cache_shards,
            num_shards,
        }
    }

    async fn checkout<Q>(&self, key: &Q) -> Option<(K, T)>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        /*
         * Evict then return it back
         * */
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.checkout(key).await
    }

    async fn len(&self) -> usize {
        let len_iter = self.cache_shards.iter().map(|s| s.len());
        let mut len_stream = stream::iter(len_iter);
        let mut len = 0_usize;
        while let Some(l) = len_stream.next().await {
            len += l.await
        }

        len
    }

    async fn get<Q>(&self, key: &Q) -> Option<T>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.get(key).await
    }

    async fn drain(&self) {
        for shard in self.cache_shards.iter() {
            shard.drain().await;
        }
    }

    async fn insert(&self, key: K, value: T) {
        let shard_key = self.compute_shard(&key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.insert(key, value).await;
    }

    async fn insert_with_ttl(&self, key: K, value: T, ttl: Duration) {
        let shard_key = self.compute_shard(&key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.insert_with_ttl(key, value, ttl).await;
    }

    async fn update_with_ttl<Q>(&self, key: &Q, value: T, ttl: Duration) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.update_with_ttl(key, value, ttl).await?;
        Ok(())
    }

    fn compute_shard<Q>(&self, key: &Q) -> usize
    where
        Q: Hash + ?Sized,
    {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        hash_value as usize % usize::from(self.num_shards)
    }

    async fn contains<Q>(&self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.contains(key).await
    }

    async fn update<Q>(&self, key: &Q, value: T) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.update(key, value).await?;
        Ok(())
    }

    async fn evict<Q>(&self, key: &Q) -> Option<T>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.evict(key).await
    }

    async fn statistics(&self) -> CacheStats {
        let mut stats = CacheStats::default();

        for shard in self.cache_shards.iter() {
            let shard_stats = shard.statistics().await;
            stats += shard_stats;
        }

        stats
    }
}

#[cfg(test)]
mod builder_tests {
    use super::*;

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    #[tokio::test]
    async fn default_build_is_functional() {
        let cache = DashCacheBuilder::<u64, u64>::new(nz(100)).build();
        cache.insert(1, 10).await;
        assert_eq!(cache.get(&1).await, Some(10));
    }

    #[tokio::test]
    async fn with_num_shards_sets_shard_count() {
        let cache = DashCacheBuilder::<u64, u64>::new(nz(100))
            .with_num_shards(nz(8))
            .build();
        assert_eq!(cache.num_shards(), 8);
    }

    #[tokio::test]
    async fn shard_capacity_is_ceil_divided() {
        // 10 total / 3 shards = ceil(3.33) = 4 per shard → total capacity = 12
        let cache = DashCacheBuilder::<u64, u64>::new(nz(10))
            .with_num_shards(nz(3))
            .build();
        // Insert 12 entries — should fit (3 shards * 4 cap each).
        // Keys are routed by hash so we just verify no panic and len <= 12.
        for i in 0..12u64 {
            cache.insert(i, i).await;
        }
        assert!(cache.len().await <= 12);
    }

    #[tokio::test]
    async fn with_num_shards_one_works() {
        let cache = DashCacheBuilder::<u64, u64>::new(nz(4))
            .with_num_shards(nz(1))
            .build();
        assert_eq!(cache.num_shards(), 1);
        for i in 0..4u64 {
            cache.insert(i, i).await;
        }
        assert_eq!(cache.len().await, 4);
    }

    #[tokio::test]
    async fn with_hasher_retypes_builder() {
        // Verifies that with_hasher compiles and produces a working cache.
        let hasher = ahash::RandomState::with_seeds(1, 2, 3, 4);
        let cache = DashCacheBuilder::<u64, u64>::new(nz(100))
            .with_hasher(hasher)
            .build();
        cache.insert(42, 99).await;
        assert_eq!(cache.get(&42).await, Some(99));
    }

    #[tokio::test]
    async fn with_hasher_then_num_shards_composes() {
        let hasher = ahash::RandomState::with_seeds(5, 6, 7, 8);
        let cache = DashCacheBuilder::<u64, u64>::new(nz(40))
            .with_hasher(hasher)
            .with_num_shards(nz(4))
            .build();
        assert_eq!(cache.num_shards(), 4);
        cache.insert(1, 1).await;
        assert_eq!(cache.get(&1).await, Some(1));
    }

    #[tokio::test]
    async fn build_without_num_shards_uses_cpu_count() {
        // No num_shards set → defaults to cpu_count shards via new_with_hasher.
        let cache = DashCacheBuilder::<u64, u64>::new(nz(100)).build();
        assert!(cache.num_shards() >= 1);
    }

    #[tokio::test]
    async fn builder_cap_equals_one_per_shard() {
        // Edge case: total cap == num_shards → shard_cap = 1 each.
        let n = 4usize;
        let cache = DashCacheBuilder::<u64, u64>::new(nz(n))
            .with_num_shards(nz(n))
            .build();
        assert_eq!(cache.num_shards(), n);
        // Each shard holds exactly 1 entry; inserting n+1 distinct-shard keys
        // must trigger at least one eviction.
        for i in 0..(n * 4) as u64 {
            cache.insert(i, i).await;
        }
        assert!(cache.len().await <= n);
    }
}

#[cfg(test)]
mod dash_cache_tests {
    use super::*;

    // Fixed shard count and capacity for deterministic tests — avoids CPU-count variance.
    fn make_cache(shard_cap: usize) -> DashCache<u64, u64> {
        DashCache::with_num_shards_and_capacity(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(shard_cap).unwrap(),
        )
    }

    #[tokio::test]
    async fn insert_get_roundtrip() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        assert_eq!(cache.get(&1).await, Some(100));
    }

    #[tokio::test]
    async fn get_miss_returns_none() {
        let cache = make_cache(10);
        assert_eq!(cache.get(&99).await, None);
    }

    #[tokio::test]
    async fn insert_updates_existing_key() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        cache.insert(1, 200).await;
        assert_eq!(cache.get(&1).await, Some(200));
    }

    #[tokio::test]
    async fn contains_returns_correct_values() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        assert!(cache.contains(&1).await);
        assert!(!cache.contains(&2).await);
    }

    #[tokio::test]
    async fn contains_does_not_count_as_hit_in_stats() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        cache.contains(&1).await;
        // contains is a read-only check — stats should show no hits
        let stats = cache.statistics().await;
        assert_eq!(stats.hits, 0);
    }

    #[tokio::test]
    async fn evict_removes_key_and_returns_value() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        let val = cache.evict(&1).await;
        assert_eq!(val, Some(100));
        assert!(!cache.contains(&1).await);
    }

    #[tokio::test]
    async fn evict_missing_key_returns_none() {
        let cache = make_cache(10);
        assert_eq!(cache.evict(&99).await, None);
    }

    #[tokio::test]
    async fn update_existing_key_changes_value() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        cache.update(&1, 999).await.unwrap();
        assert_eq!(cache.get(&1).await, Some(999));
    }

    #[tokio::test]
    async fn update_missing_key_returns_err() {
        let cache = make_cache(10);
        assert!(cache.update(&99, 1).await.is_err());
    }

    #[tokio::test]
    async fn drain_empties_all_shards() {
        let cache = make_cache(10);
        for i in 0..20u64 {
            cache.insert(i, i).await;
        }
        cache.drain().await;
        assert_eq!(cache.len().await, 0);
        for i in 0..20u64 {
            assert_eq!(cache.get(&i).await, None);
        }
    }

    #[tokio::test]
    async fn statistics_aggregates_hits_across_shards() {
        let cache = make_cache(10);
        for i in 0..8u64 {
            cache.insert(i, i).await;
        }
        // hit every key once
        for i in 0..8u64 {
            cache.get(&i).await;
        }
        let stats = cache.statistics().await;
        assert_eq!(stats.hits, 8);
    }

    #[tokio::test]
    async fn statistics_counts_misses() {
        let cache = make_cache(10);
        cache.get(&1).await;
        cache.get(&2).await;
        let stats = cache.statistics().await;
        assert_eq!(stats.misses, 2);
    }

    #[tokio::test]
    async fn len_reflects_inserted_keys() {
        let cache = make_cache(10);
        for i in 0..8u64 {
            cache.insert(i, i).await;
        }
        assert_eq!(cache.len().await, 8);
    }

    #[tokio::test]
    async fn num_shards_matches_constructor() {
        let cache = make_cache(10);
        assert_eq!(cache.num_shards(), 4);
    }

    #[tokio::test]
    async fn concurrent_inserts_all_keys_present() {
        let cache = DashCache::<u64, u64>::with_num_shards_and_capacity(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(1000).unwrap(),
        );
        let n = 200u64;
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let cache_c = cache.clone();
            handles.push(tokio::spawn(async move {
                let start = t * (n / 4);
                let end = start + (n / 4);
                for i in start..end {
                    cache_c.insert(i, i * 10).await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        for i in 0..n {
            assert_eq!(cache.get(&i).await, Some(i * 10), "missing key {i}");
        }
    }

    #[tokio::test]
    async fn get_accepts_borrowed_key() {
        let cache: DashCache<String, u32> = DashCache::with_num_shards_and_capacity(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(10).unwrap(),
        );
        cache.insert("hello".to_string(), 1).await;
        cache.insert("world".to_string(), 2).await;
        // &str is accepted where K = String via Borrow<str>
        assert_eq!(cache.get("hello").await, Some(1));
        assert_eq!(cache.get("world").await, Some(2));
        assert_eq!(cache.get("missing").await, None);
        let stats = cache.statistics().await;
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test]
    async fn contains_accepts_borrowed_key() {
        let cache: DashCache<String, u32> = DashCache::with_num_shards_and_capacity(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(10).unwrap(),
        );
        cache.insert("hello".to_string(), 1).await;
        assert!(cache.contains("hello").await);
        assert!(!cache.contains("missing").await);
    }

    #[tokio::test]
    async fn evict_accepts_borrowed_key() {
        let cache: DashCache<String, u32> = DashCache::with_num_shards_and_capacity(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(10).unwrap(),
        );
        cache.insert("hello".to_string(), 1).await;
        cache.insert("world".to_string(), 2).await;
        assert_eq!(cache.evict("hello").await, Some(1));
        assert_eq!(cache.evict("hello").await, None);
        assert!(cache.contains("world").await);
    }

    #[tokio::test]
    async fn update_accepts_borrowed_key() {
        let cache: DashCache<String, u32> = DashCache::with_num_shards_and_capacity(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(10).unwrap(),
        );
        cache.insert("hello".to_string(), 1).await;
        cache.update("hello", 99).await.unwrap();
        assert_eq!(cache.get("hello").await, Some(99));
        assert!(cache.update("missing", 0).await.is_err());
    }

    // TTL tests

    #[tokio::test]
    async fn insert_with_ttl_accessible_before_expiry() {
        let cache = make_cache(10);
        cache.insert_with_ttl(1, 100, Duration::from_secs(60)).await;
        assert_eq!(cache.get(&1).await, Some(100));
        assert!(cache.contains(&1).await);
    }

    #[tokio::test]
    async fn insert_with_ttl_expires() {
        let cache = make_cache(10);
        cache
            .insert_with_ttl(1, 100, Duration::from_millis(50))
            .await;
        assert_eq!(cache.get(&1).await, Some(100));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, None);
        assert!(!cache.contains(&1).await);
    }

    #[tokio::test]
    async fn expiration_stat_incremented() {
        let cache = make_cache(10);
        cache
            .insert_with_ttl(1, 100, Duration::from_millis(50))
            .await;
        cache
            .insert_with_ttl(2, 200, Duration::from_millis(50))
            .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, None);
        assert_eq!(cache.get(&2).await, None);
        let stats = cache.statistics().await;
        assert_eq!(stats.expirations, 2);
        assert_eq!(stats.hits, 0);
    }

    #[tokio::test]
    async fn update_with_ttl_resets_expiration() {
        let cache = make_cache(10);
        cache
            .insert_with_ttl(1, 100, Duration::from_millis(50))
            .await;
        cache
            .update_with_ttl(&1, 200, Duration::from_secs(60))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, Some(200));
    }

    #[tokio::test]
    async fn default_ttl_applied_via_builder() {
        let cache = DashCacheBuilder::<u64, u64>::new(NonZeroUsize::new(40).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .with_default_ttl(Duration::from_millis(50))
            .build();
        cache.insert(1, 100).await;
        assert_eq!(cache.get(&1).await, Some(100));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, None);
    }

    #[tokio::test]
    async fn insert_with_ttl_overrides_default_ttl() {
        let cache = DashCacheBuilder::<u64, u64>::new(NonZeroUsize::new(40).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .with_default_ttl(Duration::from_millis(50))
            .build();
        // explicit long TTL overrides the short default
        cache.insert_with_ttl(1, 100, Duration::from_secs(60)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, Some(100));
    }

    #[tokio::test]
    async fn no_default_ttl_entries_do_not_expire() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, Some(100));
    }

    #[tokio::test]
    async fn eviction_does_not_affect_other_shards() {
        // Fill the cache past capacity to trigger evictions across shards.
        // Hash routing is not uniform so we can't assert an exact len, but total
        // len must never exceed total capacity and evictions must have occurred.
        let shards = 4usize;
        let shard_cap = 4usize;
        let total_cap = shards * shard_cap;
        let cache = DashCache::<u64, u64>::with_num_shards_and_capacity(
            NonZeroUsize::new(shards).unwrap(),
            NonZeroUsize::new(shard_cap).unwrap(),
        );
        for i in 0..(total_cap * 4) as u64 {
            cache.insert(i, i).await;
        }
        assert!(cache.len().await <= total_cap);
        let stats = cache.statistics().await;
        assert!(stats.evictions > 0);
    }

    #[tokio::test]
    async fn checkout_removes_entry_from_cache() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        let guard = cache.checkout(&1).await;
        assert!(guard.is_some());
        assert!(!cache.contains(&1).await);
    }

    #[tokio::test]
    async fn checkout_miss_returns_none() {
        let cache = make_cache(10);
        assert!(cache.checkout(&99u64).await.is_none());
    }

    #[tokio::test]
    async fn checkout_deref_returns_value() {
        let cache = make_cache(10);
        cache.insert(1, 100u64).await;
        let guard = cache.checkout(&1).await.unwrap();
        assert_eq!(*guard, 100);
        drop(guard);
    }

    #[tokio::test]
    async fn checkout_drop_reinserts_value() {
        let cache = make_cache(10);
        cache.insert(1, 100u64).await;
        let guard = cache.checkout(&1).await.unwrap();
        drop(guard);
        // yield to let the spawned re-insert task run
        tokio::task::yield_now().await;
        assert_eq!(cache.get(&1).await, Some(100));
    }

    #[tokio::test]
    async fn checkout_drop_reinserts_modified_value() {
        let cache = make_cache(10);
        cache.insert(1, 100u64).await;
        let mut guard = cache.checkout(&1).await.unwrap();
        *guard = 999;
        drop(guard);
        tokio::task::yield_now().await;
        assert_eq!(cache.get(&1).await, Some(999));
    }

    #[tokio::test]
    async fn checkout_other_keys_unaffected() {
        let cache = make_cache(10);
        cache.insert(1, 100u64).await;
        cache.insert(2, 200u64).await;
        let _guard = cache.checkout(&1).await.unwrap();
        assert_eq!(cache.get(&2).await, Some(200));
    }

    #[tokio::test]
    async fn checkout_then_insert_new_value_while_checked_out() {
        // If the caller re-inserts a new value while the guard is live, the guard's
        // drop re-inserts the original (checked-out) value, overwriting the interim one.
        let cache = make_cache(10);
        cache.insert(1, 100u64).await;
        let guard = cache.checkout(&1).await.unwrap();
        cache.insert(1, 777).await;
        assert_eq!(cache.get(&1).await, Some(777));
        drop(guard);
        tokio::task::yield_now().await;
        // guard re-inserted the original value, overwriting the interim insert
        assert_eq!(cache.get(&1).await, Some(100));
    }
}
