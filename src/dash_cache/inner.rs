use super::slab::{LockedCache, default_shard_count};
use crate::core::CacheError;
use crate::shared_vec::{SharedVec, SharedVecRef};
use crate::stats::CacheStats;
use std::hash::{BuildHasher, Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

// Owns the shard array and routes all operations to the correct shard via key hash.
#[derive(Debug)]
pub(super) struct InnerCacheShards<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Send + Sync,
{
    pub(super) cache_shards: Box<[LockedCache<K, V, S>]>,
    pub(super) num_shards: NonZeroUsize,
    hasher: S,
    _shared_vec: Arc<SharedVec<K, V>>,
}

impl<K, T, S> InnerCacheShards<K, T, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    pub(super) fn new(
        cap: NonZeroUsize,
        num_shards: Option<NonZeroUsize>,
        hasher: S,
        promotion_queue_size: usize,
        eviction_queue_size: usize,
        default_ttl: Option<Duration>,
    ) -> InnerCacheShards<K, T, S> {
        let shard_count = num_shards
            .map(|n| n.get())
            .unwrap_or_else(default_shard_count);

        // Round up so total capacity is evenly divisible by shard_count.
        let shard_capacity = (cap.get() + shard_count - 1) / shard_count;
        let total_capacity = shard_capacity * shard_count;

        let shared_vec = Arc::new(SharedVec::new(
            NonZeroUsize::new(total_capacity).unwrap(),
            NonZeroUsize::new(shard_count).unwrap(),
        ));

        let shard_refs: Vec<SharedVecRef<K, T>> = shared_vec.make_shards();
        let shard_capacity_nz = NonZeroUsize::new(shard_capacity).unwrap();

        let shards_vec: Vec<LockedCache<K, T, S>> = shard_refs
            .into_iter()
            .map(|slab_ref| {
                LockedCache::new(
                    slab_ref,
                    shard_capacity_nz,
                    hasher.clone(),
                    promotion_queue_size,
                    eviction_queue_size,
                    default_ttl,
                )
            })
            .collect();

        let cache_shards = shards_vec.into_boxed_slice();
        let num_shards = unsafe { NonZeroUsize::new_unchecked(shard_count) };
        InnerCacheShards {
            cache_shards,
            num_shards,
            hasher,
            _shared_vec: shared_vec,
        }
    }

    pub(super) async fn checkout<Q>(&self, key: &Q) -> Option<(K, T, u64)>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.checkout(key).await
    }

    pub(super) async fn insert_with_expires(&self, key: K, value: T, expires: u64) {
        let shard_key = self.compute_shard(&key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.insert_with_expires(key, value, expires).await;
    }

    pub(super) async fn len(&self) -> usize {
        let mut len = 0_usize;
        for shard in self.cache_shards.iter() {
            len += shard.len().await;
        }
        len
    }

    pub(super) async fn get<Q>(&self, key: &Q) -> Option<T>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.get(key).await
    }

    pub(super) async fn drain(&self) {
        for shard in self.cache_shards.iter() {
            shard.drain().await;
        }
    }

    pub(super) async fn insert(&self, key: K, value: T) {
        let shard_key = self.compute_shard(&key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.insert(key, value).await;
    }

    pub(super) async fn insert_with_ttl(&self, key: K, value: T, ttl: Duration) {
        let shard_key = self.compute_shard(&key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.insert_with_ttl(key, value, ttl).await;
    }

    pub(super) async fn update_with_ttl<Q>(
        &self,
        key: &Q,
        value: T,
        ttl: Duration,
    ) -> Result<(), CacheError>
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
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        hash_value as usize % usize::from(self.num_shards)
    }

    pub(super) async fn contains<Q>(&self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.contains(key).await
    }

    pub(super) async fn update<Q>(&self, key: &Q, value: T) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.update(key, value).await?;
        Ok(())
    }

    pub(super) async fn evict<Q>(&self, key: &Q) -> Option<T>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.evict(key).await
    }

    pub(super) async fn statistics(&self) -> CacheStats {
        let mut stats = CacheStats::default();

        for shard in self.cache_shards.iter() {
            let shard_stats = shard.statistics().await;
            stats += shard_stats;
        }

        stats
    }
}
