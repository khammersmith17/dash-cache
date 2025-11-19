use crate::core::{CacheError, CacheShard, CacheStats};
use ahash::AHasher;
use futures::stream::{self, StreamExt};
use std::hash::Hash;
use std::hash::Hasher;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;

// wrap CacheShard in RwLock for better type semantics
struct LockedCache<K, T> {
    handle: RwLock<CacheShard<K, T>>,
}

// wrapper methods around the CacheShard shard internal to a shard
// This level on the type abstraction contains all concurrency primitives present in the type
impl<K, T> LockedCache<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    fn with_capacity(cap: usize) -> LockedCache<K, T> {
        let cache: CacheShard<K, T> = CacheShard::with_capacity(cap);
        let handle = RwLock::new(cache);
        LockedCache { handle }
    }

    async fn len(&self) -> usize {
        let guard = self.handle.read().await;
        guard.len()
    }

    async fn insert(&self, key: K, value: T) {
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

    async fn get(&self, key: &K) -> Option<T> {
        let mut guard = self.handle.write().await;
        let value = guard.get(key);
        value
    }

    async fn pop(&self, key: &K) -> Option<T> {
        let mut guard = self.handle.write().await;
        guard.pop(key)
    }

    async fn update(&self, key: &K, value: T) -> Result<(), CacheError> {
        let mut guard = self.handle.write().await;
        guard.update(key, value)?;
        Ok(())
    }

    async fn contains(&self, key: &K) -> bool {
        let guard = self.handle.read().await;
        guard.contains(key)
    }
}

///This lru cache implementation is an omage to dashmap::DashMap.
///Interally keys are sharded base on key hash to minimize locking access across threads. Each internal shard cache
///is an instance of a thread safe cache table.
///Each shard is wrapped in a tokio::RwLock.
///Most APIs are locking on each shard, aside from the contains method, which is read only access.
///All other methods mutate cache stat and thus require exclusive access.
///All other APIs may mutate the cache shard, thus requiring locking mutable references.
///This type wraps all shared internally in an tokio::sync::Arc, so wrapping this type in Arc is
///not required by users.
#[derive(Clone)]
pub struct DashCache<K, T> {
    inner: Arc<InnerCacheShards<K, T>>,
}

impl<K, T> DashCache<K, T>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// Use default sharding logic. By default the shard count will be equal to the number of cpu
    /// cores available on the machine. When request capacity < cpu core count, each shard
    /// will have capacity of size 1. In the case where capacity is not evenly divided by the
    /// number of cpu cores available, each shard capacity rounds up to the next unsigned integer
    /// value.
    pub fn new(cap: u64) -> DashCache<K, T> {
        let inner = Arc::new(InnerCacheShards::new(cap));

        DashCache { inner }
    }

    /// Explicility define the number of shards and capacity per shard, total capacity will then be
    /// num_shards * shard_capacity.
    pub fn with_num_shards_and_capacity(
        num_shards: usize,
        shard_capacity: usize,
    ) -> DashCache<K, T> {
        let inner = Arc::new(InnerCacheShards::with_num_shards_and_capacity(
            num_shards,
            shard_capacity,
        ));

        DashCache { inner }
    }

    /// Method to fetch a value for a given key. This method will return `Some<T>` when there is a
    /// cache hit, a copy of the value stored with the associated key. Given the internal borrowing
    /// semantics as currently implemented, returning a copy is the most ergonomic approach. This
    /// will promote the fetched key to the most recently used in the local shard the key is stored
    /// in. This method locks the accessed shard, given the promotion semantics on a cache hit.
    /// Again, given the borrowing semantics, a clone of the value is returned.
    pub async fn get(&self, key: &K) -> Option<T> {
        self.inner.get(key).await
    }

    /// Given there is not get_mut to acquire mutable access to the value stored at a key, this is
    /// the method to use when attempting to mutate the value for a given key. This method will
    /// update the value for a key that already exists in the cache, on a cache hit. This method
    /// locks the key local shard. This will promote the inserted/updated key to the most recently
    /// used in the local shard the key is stored in. When the cache is full, the least recently
    /// used item in the local cache is evicted.
    pub async fn insert(&self, key: K, value: T) {
        self.inner.insert(key, value).await;
    }

    /// This is the only pure read method, and thus will not fully lock the local shard on a cache
    /// hit. This method also will not promote the keyed value on a cache hit. This is the only
    /// exposed API that does not require mutable access.
    pub async fn contains(&self, key: &K) -> bool {
        self.inner.contains(key).await
    }

    /// Statistics are kept interally to the nature of cache hits, misses, and evictions. This
    /// methods provides a snapshot aggregated across all shards. This method will acquire a read
    /// lock on all shards, blocking any mutable access during the aggregation.
    pub async fn statistics(&self) -> CacheStats {
        self.inner.statistics().await
    }

    /// Pop an entry from the cache.
    pub async fn pop(&self, key: &K) -> Option<T> {
        self.inner.pop(key).await
    }

    pub async fn len(&self) -> usize {
        self.inner.len().await
    }

    /// This method will update the value for a key. For similar borrowing semantic limitations,
    /// there is no provided get_mut method. Thus, this is the most appropriate method to update a
    /// value that exists in the cache. This method will not write a new key on a cache miss for a
    /// particular key, thus when the key does not exists in the cache, an error will be returned.
    /// On success a unit type value is returned.
    pub async fn update(&self, key: &K, value: T) -> Result<(), CacheError> {
        self.inner.update(key, value).await?;
        Ok(())
    }

    /// Empty the entire cache, and thus all shards will be cleared.
    pub async fn drain(&self) {
        self.inner.drain().await;
    }

    /// Utility to fetch the number of shards.
    pub async fn num_shards(&self) -> usize {
        usize::from(self.inner.num_shards)
    }
}

// encapsulates the cache shards and handles all concurrent access/mutation operations
struct InnerCacheShards<K, T> {
    cache_shards: Box<[LockedCache<K, T>]>,
    num_shards: NonZeroUsize,
}

unsafe impl<K, T> Send for InnerCacheShards<K, T>
where
    K: Send + 'static,
    T: Send + 'static,
{
}

unsafe impl<K, T> Sync for InnerCacheShards<K, T>
where
    K: Sync + 'static,
    T: Sync + 'static,
{
}

impl<K, T> InnerCacheShards<K, T>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    fn new(cap: u64) -> InnerCacheShards<K, T> {
        if cap == 0 {
            panic!("reqeusted capacity must be greater than zero");
        }
        let cpu_count = num_cpus::get();
        let mut shards_vec: Vec<LockedCache<K, T>> = Vec::with_capacity(cpu_count);

        let shard_size = ((cap as f32 / cpu_count as f32).ceil() as usize).max(1_usize);

        for _ in 0..cpu_count {
            let shard: LockedCache<K, T> = LockedCache::with_capacity(shard_size as usize);
            shards_vec.push(shard);
        }

        let cache_shards = shards_vec.into_boxed_slice();
        let num_shards = unsafe { NonZeroUsize::new_unchecked(cpu_count) };

        InnerCacheShards {
            cache_shards,
            num_shards,
        }
    }

    fn with_num_shards_and_capacity(
        num_shards: usize,
        shard_capacity: usize,
    ) -> InnerCacheShards<K, T> {
        if num_shards == 0 || shard_capacity == 0 {
            panic!("num_chards and shard_capacity must non zero")
        }
        let mut shards_vec: Vec<LockedCache<K, T>> = Vec::with_capacity(num_shards);

        for _ in 0..num_shards {
            let shard: LockedCache<K, T> = LockedCache::with_capacity(shard_capacity);
            shards_vec.push(shard);
        }

        // in the case that the vec is over allocated
        // make the heap shard allocation more compact
        shards_vec.shrink_to_fit();
        let cache_shards = shards_vec.into_boxed_slice();
        let num_shards = unsafe { NonZeroUsize::new_unchecked(num_shards) };

        InnerCacheShards {
            cache_shards,
            num_shards,
        }
    }

    // TODO: revist this
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

    async fn pop(&self, key: &K) -> Option<T> {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.pop(key).await
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
