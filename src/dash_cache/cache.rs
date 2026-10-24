use super::inner::InnerCacheShards;
use crate::core::CacheError;
use crate::guard::CacheEntryGuard;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use std::time::Duration;

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
    pub(super) inner: Arc<InnerCacheShards<K, V, S>>,
}

impl<K, V, S> DashCache<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync,
{
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
    pub async fn statistics(&self) -> crate::stats::CacheStats {
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

    /// Removes the entry from the cache and returns a [`CacheEntryGuard`] that dereferences to its
    /// value. When the guard is dropped, the value is written back with its
    /// original expiry. Returns `None` if the key is not present.
    ///
    /// The write-back is asynchronous: inside a Tokio runtime it is spawned as a task, so it
    /// completes some time after the guard is dropped. Until it does:
    ///
    /// - the key is absent: [`get`](DashCache::get) and [`contains`](DashCache::contains) miss,
    ///   and other callers may [`insert`](DashCache::insert) it;
    /// - if another caller inserts the key, the write-back later **overwrites** that newer value;
    /// - if the runtime shuts down before the task runs, the write-back is **lost**.
    ///
    /// Outside a Tokio runtime, dropping the guard blocks the current thread until the write-back
    /// completes. Drop it immediately after it is mutated.
    ///
    /// Use `checkout` for values that are not written concurrently, where losing a write-back at
    /// shutdown is acceptable.
    ///
    /// # Example
    ///
    /// Keep the guard's scope as short as possible: mutate the value, then let the guard drop
    /// straight away. The key is missing from the cache for as long as the guard is alive, so
    /// don't hold it across unrelated `.await`s.
    ///
    /// ```
    /// use dash_cache::DashCacheBuilder;
    /// use std::num::NonZeroUsize;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let cache = DashCacheBuilder::<u64, Vec<u64>>::new(NonZeroUsize::new(16).unwrap()).build();
    /// cache.insert(1, vec![10]).await;
    ///
    /// if let Some(mut values) = cache.checkout(&1).await {
    ///     values.push(20);
    /// } // `values` is dropped here, which starts the write-back.
    ///
    /// # }
    /// ```
    pub async fn checkout<Q>(&self, key: &Q) -> Option<CacheEntryGuard<K, V, S>>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (key, value, expires) = self.inner.checkout(key).await?;
        Some(CacheEntryGuard::new(key, value, expires, self.clone()))
    }

    pub(crate) async fn insert_with_expires(&self, key: K, value: V, expires: u64) {
        self.inner.insert_with_expires(key, value, expires).await;
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
