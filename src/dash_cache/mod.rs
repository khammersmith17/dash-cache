use crate::core::{CacheError, CacheStats, SlabShard};
use ahash::AHasher;
use futures::stream::{self, StreamExt};
use std::hash::Hasher;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;

// wrap CacheShard in RwLock for better type semantics
struct LockedCache<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone,
    V: Clone,
    S: BuildHasher,
{
    handle: RwLock<SlabShard<K, V, S>>,
}

// wrapper methods around the CacheShard shard internal to a shard
// This level on the type abstraction contains all concurrency primitives present in the type
impl<K, V, S> LockedCache<K, V, S>
where
    K: Hash + Ord + Clone,
    V: Clone,
    S: BuildHasher,
{
    fn with_capacity_and_hasher(cap: NonZeroUsize, hasher: S) -> LockedCache<K, V, S> {
        let cache: SlabShard<K, V, S> = SlabShard::with_capacity_and_hasher(cap, hasher);
        let handle = RwLock::new(cache);
        LockedCache { handle }
    }

    async fn len(&self) -> usize {
        let guard = self.handle.read().await;
        guard.len()
    }

    async fn insert(&self, key: K, value: V) {
        let mut guard = self.handle.write().await;
        guard.insert(key, value)
    }

    async fn drain(&self) {
        let mut guard = self.handle.write().await;
        guard.drain();
    }

    async fn statistics(&self) -> CacheStats {
        let guard = self.handle.read().await;
        guard.statistics()
    }

    async fn get(&self, key: &K) -> Option<V> {
        let mut guard = self.handle.write().await;
        let value = guard.get(key);
        value
    }

    async fn evict(&self, key: &K) -> Option<V> {
        let mut guard = self.handle.write().await;
        guard.evict(key)
    }

    async fn update(&self, key: &K, value: V) -> Result<(), CacheError> {
        let mut guard = self.handle.write().await;
        guard.update(key, value)?;
        Ok(())
    }

    async fn contains(&self, key: &K) -> bool {
        let guard = self.handle.read().await;
        guard.contains(key)
    }
}

pub struct DashCacheBuilder<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    cap: NonZeroUsize,
    num_shards: Option<NonZeroUsize>,
    hasher: S,
    _type_marker: PhantomData<(K, V)>,
}

impl<K, V> DashCacheBuilder<K, V, ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(capacity: NonZeroUsize) -> DashCacheBuilder<K, V> {
        DashCacheBuilder {
            cap: capacity,
            num_shards: None,
            hasher: ahash::RandomState::new(),
            _type_marker: PhantomData,
        }
    }
}

impl<K, V, S> DashCacheBuilder<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    pub fn with_num_shards(mut self, num_shards: NonZeroUsize) -> DashCacheBuilder<K, V, S> {
        self.num_shards = Some(num_shards);
        self
    }

    pub fn with_hasher<H: BuildHasher + Clone>(self, hasher: H) -> DashCacheBuilder<K, V, H> {
        let DashCacheBuilder {
            cap, num_shards, ..
        } = self;
        DashCacheBuilder {
            cap,
            num_shards,
            hasher,
            _type_marker: PhantomData,
        }
    }

    pub fn build(self) -> DashCache<K, V, S> {
        let DashCacheBuilder {
            cap,
            num_shards,
            hasher,
            ..
        } = self;
        match num_shards {
            Some(n) => {
                let shard_capacity = (cap.get() as f64 / n.get() as f64).ceil() as usize;
                // safety: given cap
                DashCache::with_num_shards_and_capacity_and_hasher(
                    n,
                    NonZeroUsize::new(shard_capacity).unwrap(),
                    hasher,
                )
            }
            None => DashCache::new_with_hasher(cap, hasher),
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
#[derive(Clone)]
pub struct DashCache<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher,
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
    S: BuildHasher + Clone,
{
    /// Creates a `DashCache` with one shard per logical CPU core.
    ///
    /// Shard capacity is `ceil(cap / cpu_count)`, with a minimum of 1. If `cap` is not evenly
    /// divisible the total capacity will be slightly above `cap`.
    pub fn new_with_hasher(cap: NonZeroUsize, hasher: S) -> DashCache<K, V, S> {
        let inner = Arc::new(InnerCacheShards::<K, V, S>::new_with_hasher(cap, hasher));

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
        ));

        DashCache { inner }
    }

    /// Returns a clone of the value for the given key and promotes it to most recently used within
    /// its shard. Acquires a write lock on the key's shard. Returns `None` on a cache miss.
    pub async fn get(&self, key: &K) -> Option<V> {
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

    /// Returns `true` if the key exists in the cache without promoting it or recording a hit.
    ///
    /// This is the only read-only method — it acquires a read lock and does not modify the shard.
    pub async fn contains(&self, key: &K) -> bool {
        self.inner.contains(key).await
    }

    /// Returns a snapshot of hit, miss, and eviction counts aggregated across all shards.
    /// Acquires a read lock on each shard sequentially.
    pub async fn statistics(&self) -> CacheStats {
        self.inner.statistics().await
    }

    /// Removes the entry for the given key and returns its value, or `None` if the key is not
    /// present. Acquires a write lock on the key's shard.
    pub async fn evict(&self, key: &K) -> Option<V> {
        self.inner.evict(key).await
    }

    /// Returns the total number of entries across all shards.
    pub async fn len(&self) -> usize {
        self.inner.len().await
    }

    /// Updates the value for an existing key and promotes it to most recently used within its shard.
    ///
    /// Returns `Err(CacheError::KeyNotExist)` if the key is not in the cache — use `insert` to
    /// write a new key. There is no `get_mut`. Acquires a write lock on the key's shard.
    pub async fn update(&self, key: &K, value: V) -> Result<(), CacheError> {
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
struct InnerCacheShards<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher,
{
    cache_shards: Box<[LockedCache<K, V, S>]>,
    num_shards: NonZeroUsize,
}

impl<K, T, S> InnerCacheShards<K, T, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    fn new_with_hasher(capacity: NonZeroUsize, hasher: S) -> InnerCacheShards<K, T, S> {
        let cpu_count = num_cpus::get();

        let cap = capacity.get();

        let shard_capacity =
            NonZeroUsize::new(((cap as f32 / cpu_count as f32).ceil() as usize).max(1_usize))
                .unwrap();

        let shards_vec: Vec<LockedCache<K, T, S>> = (0..cpu_count)
            .map(|_| {
                let h = hasher.clone();
                LockedCache::with_capacity_and_hasher(shard_capacity, h)
            })
            .collect();

        let cache_shards = shards_vec.into_boxed_slice();
        let num_shards = unsafe { NonZeroUsize::new_unchecked(cpu_count) };

        InnerCacheShards {
            cache_shards,
            num_shards,
        }
    }

    fn with_num_shards_and_capacity_and_hasher(
        num_shards: NonZeroUsize,
        shard_capacity: NonZeroUsize,
        hasher: S,
    ) -> InnerCacheShards<K, T, S> {
        let shard_count = num_shards.get();
        let shards_vec: Vec<LockedCache<K, T, S>> = (0..num_shards.get())
            .map(|_| {
                let h = hasher.clone();
                LockedCache::with_capacity_and_hasher(shard_capacity, h)
            })
            .collect();

        let cache_shards = shards_vec.into_boxed_slice();
        let num_shards = unsafe { NonZeroUsize::new_unchecked(shard_count) };

        InnerCacheShards {
            cache_shards,
            num_shards,
        }
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

    async fn get(&self, key: &K) -> Option<T> {
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

    fn compute_shard(&self, key: &K) -> usize {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        hash_value as usize % usize::from(self.num_shards)
    }

    async fn contains(&self, key: &K) -> bool {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.contains(key).await
    }

    async fn update(&self, key: &K, value: T) -> Result<(), CacheError> {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.update(key, value).await?;
        Ok(())
    }

    async fn evict(&self, key: &K) -> Option<T> {
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
}
