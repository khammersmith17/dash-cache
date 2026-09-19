use super::cache::DashCache;
use super::inner::InnerCacheShards;
use crate::queue;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

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
    pub(super) cap: NonZeroUsize,
    pub(super) num_shards: Option<NonZeroUsize>,
    pub(super) hasher: S,
    pub(super) default_ttl: Option<Duration>,
    pub(super) promotion_queue_size: Option<usize>,
    pub(super) eviction_queue_size: Option<usize>,
    pub(super) _type_marker: PhantomData<(K, V)>,
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
            promotion_queue_size: None,
            eviction_queue_size: None,
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
    pub fn with_num_shards(&mut self, num_shards: NonZeroUsize) -> &mut DashCacheBuilder<K, V, S> {
        self.num_shards = Some(num_shards);
        self
    }

    /// Sets a default TTL applied to every entry inserted via [`DashCache::insert`]. Entries
    /// inserted via [`DashCache::insert_with_ttl`] override this per-call.
    pub fn with_default_ttl(&mut self, ttl: Duration) -> &mut DashCacheBuilder<K, V, S> {
        self.default_ttl = Some(ttl);
        self
    }

    /// Sets the size of the promotion queue. Promotions in the priority list are queued for all
    /// read/get operations to allow a read to acquired a shared read lock, and amortize the cost
    /// of all list mutations to when a write lock is required.
    ///
    /// The value for the promotion queue size should be decided by evaluating how many reads might
    /// be made to the same shard before a write. If the number of reads in a row to the same shard
    /// exceeds this value, then some promotions may be dropped.
    pub fn with_promotion_queue_size(
        &mut self,
        size: NonZeroUsize,
    ) -> &mut DashCacheBuilder<K, V, S> {
        self.promotion_queue_size = Some(size.get());
        self
    }

    /// Sets the size of the eviction queue. Evictions due to entry expirations are queued for all
    /// read/get operations to allow a read to acquired a shared read lock, and amortize the cost
    /// of all list mutations to when a write lock is required.
    ///
    /// The value for the eviction queue size should be decided by evaluating how many reads might
    /// be made to the same shard before a write. If the number of reads in a row to the same shard
    /// exceeds this value, then some promotions may be dropped.
    pub fn with_eviction_queue_size(
        &mut self,
        size: NonZeroUsize,
    ) -> &mut DashCacheBuilder<K, V, S> {
        self.eviction_queue_size = Some(size.get());
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
            promotion_queue_size,
            eviction_queue_size,
            ..
        } = self;
        DashCacheBuilder {
            cap,
            num_shards,
            hasher,
            default_ttl,
            promotion_queue_size,
            eviction_queue_size,
            _type_marker: PhantomData,
        }
    }

    /// Returns a [`DashCache`] from the current builder configuration.
    ///
    /// If [`with_num_shards`](DashCacheBuilder::with_num_shards) was called, the cache has exactly
    /// that many shards, each with capacity `ceil(total_cap / num_shards)`. Otherwise, the number
    /// of shards defaults to the number of logical CPU cores.
    pub fn build(&self) -> DashCache<K, V, S> {
        let promotion_queue_size = self
            .promotion_queue_size
            .unwrap_or(queue::DEFAULT_BUFFER_SIZE);
        let eviction_queue_size = self
            .eviction_queue_size
            .unwrap_or(queue::DEFAULT_BUFFER_SIZE);
        let inner = InnerCacheShards::new(
            self.cap,
            self.num_shards,
            self.hasher.clone(),
            promotion_queue_size,
            eviction_queue_size,
            self.default_ttl,
        );
        DashCache {
            inner: Arc::new(inner),
        }
    }
}
