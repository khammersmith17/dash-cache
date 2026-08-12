use crate::stats::{CacheStats, LocalStats, Stats};
use crate::util;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::time::Duration;
use thiserror::Error;

/// Covers the three cases for a get operation that is queued in [crate::dash_cache::DashCache].
pub(crate) enum GetResult<V: Clone> {
    Hit(V, u32),
    Miss,
    Expired(u32),
}

/// The only supported error is when an operation that requires a key to be present, in the case of
/// an update, is performed for a key not cached.
#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Key does not exist in cache")]
    KeyNotExist,
}

#[derive(Debug)]
pub(crate) struct CacheEntry<K: Hash + Eq, V: Clone> {
    key: K,
    value: V,
    list_neighbors: u64,
    expires: u64,
}

impl<K: Hash + Eq, V: Clone> CacheEntry<K, V> {
    #[inline]
    fn is_live(&self) -> bool {
        !util::is_expired(self.expires)
    }

    // All list pointer bit math.

    #[inline]
    fn clear_neighbors(&mut self) {
        self.list_neighbors = util::pointer_idx::null_neighbors();
    }

    #[inline]
    fn set_next(&mut self, next: Option<u32>) {
        self.list_neighbors = util::pointer_idx::set_next_pointer(self.list_neighbors, next);
    }

    #[inline]
    fn set_prev(&mut self, prev: Option<u32>) {
        self.list_neighbors = util::pointer_idx::set_prev_pointer(self.list_neighbors, prev);
    }

    #[inline]
    fn prev(&self) -> Option<u32> {
        util::pointer_idx::get_prev_pointer(self.list_neighbors)
    }

    #[inline]
    fn next(&self) -> Option<u32> {
        util::pointer_idx::get_next_pointer(self.list_neighbors)
    }
}

/// Builder type to configure and instance of [SlabShard].
#[derive(Debug)]
pub struct SlabShardBuilder<K: Hash + Ord + Clone, V: Clone, S: BuildHasher> {
    capacity: NonZeroUsize,
    default_ttl: Option<Duration>,
    hasher: S,
    _type_marker: PhantomData<(K, V)>,
}

impl<K: Hash + Ord + Clone, V: Clone> SlabShardBuilder<K, V, ahash::RandomState> {
    pub fn new(capacity: NonZeroUsize) -> SlabShardBuilder<K, V, ahash::RandomState> {
        SlabShardBuilder {
            capacity,
            default_ttl: None,
            hasher: ahash::RandomState::new(),
            _type_marker: PhantomData,
        }
    }
}

impl<K: Hash + Ord + Clone, V: Clone, S: BuildHasher> SlabShardBuilder<K, V, S> {
    pub fn with_hasher<H: BuildHasher>(self, hasher: H) -> SlabShardBuilder<K, V, H> {
        let Self {
            capacity,
            default_ttl,
            ..
        } = self;
        SlabShardBuilder {
            capacity,
            default_ttl,
            hasher,
            _type_marker: PhantomData,
        }
    }

    pub fn with_default_ttl(mut self, default_ttl: Duration) -> SlabShardBuilder<K, V, S> {
        self.default_ttl = Some(default_ttl);
        self
    }

    pub fn build(self) -> SlabShard<K, V, S, LocalStats> {
        let Self {
            capacity,
            default_ttl,
            hasher,
            ..
        } = self;
        let cap = capacity.get();
        if cap > (u32::MAX >> 1) as usize {
            panic!("capacity must be <= {}", u32::MAX >> 1);
        }

        let node_map: HashMap<K, u32, S> = HashMap::with_capacity_and_hasher(cap, hasher);

        SlabShard {
            cap,
            node_map,
            slab: Vec::with_capacity(cap),
            head: None,
            tail: None,
            stats: LocalStats::default(),
            default_ttl,
        }
    }
}

/// An LRU cache backed by a contiguous slab allocation.
///
/// All entries live in a pre-allocated `Vec` and the recency list uses `u32` slab indices instead
/// of heap pointers. This gives significantly better cache locality, especially on read-heavy
/// workloads.
///
/// The `St` type parameter controls the statistics backend, and are not configurable.
///
///
/// Safety invariants are documented inline and heavily asserted in debug builds, so debug builds
/// may see reduced performance, but my review bugs. In this case please file an issue. Capacity is
/// limited to `u32::MAX >> 1` entries, reserving `u32::MAX` as the null pointer sentinel.
/// All values returned are clones.
#[derive(Debug)]
pub struct SlabShard<K, V, S = ahash::RandomState, St = LocalStats>
where
    K: Hash + Ord + Clone,
    V: Clone,
    S: BuildHasher,
    St: Stats,
{
    cap: usize,
    slab: Vec<CacheEntry<K, V>>,
    node_map: HashMap<K, u32, S>,
    head: Option<u32>,
    tail: Option<u32>,
    stats: St,
    default_ttl: Option<Duration>,
}

impl<K, V> SlabShard<K, V, ahash::RandomState, LocalStats>
where
    K: Hash + Ord + Clone,
    V: Clone,
{
    pub fn with_capacity(
        capacity: NonZeroUsize,
    ) -> SlabShard<K, V, ahash::RandomState, LocalStats> {
        SlabShard::with_capacity_and_hasher(capacity, ahash::RandomState::new())
    }

    pub fn with_capacity_and_default_ttl(
        capacity: NonZeroUsize,
        default_ttl: Duration,
    ) -> SlabShard<K, V, ahash::RandomState, LocalStats> {
        SlabShard::with_capacity_and_hasher_and_default_ttl(
            capacity,
            ahash::RandomState::new(),
            default_ttl,
        )
    }
}

impl<K, V, S, St> SlabShard<K, V, S, St>
where
    K: Hash + Ord + Clone,
    V: Clone,
    S: BuildHasher,
    St: Stats,
{
    /// Creates a new `SlabShard` with the given capacity. Panics if capacity exceeds `u32::MAX >> 1`.
    pub fn with_capacity_and_hasher(capacity: NonZeroUsize, hasher: S) -> SlabShard<K, V, S, St>
    where
        St: Default,
    {
        let cap = capacity.get();
        if cap > (u32::MAX >> 1) as usize {
            panic!("capacity must be <= {}", u32::MAX >> 1);
        }

        let node_map: HashMap<K, u32, S> = HashMap::with_capacity_and_hasher(cap, hasher);

        SlabShard {
            cap,
            node_map,
            slab: Vec::with_capacity(cap),
            head: None,
            tail: None,
            stats: St::default(),
            default_ttl: None,
        }
    }

    pub fn with_capacity_and_hasher_and_default_ttl(
        capacity: NonZeroUsize,
        hasher: S,
        default_ttl: Duration,
    ) -> SlabShard<K, V, S, St>
    where
        St: Default,
    {
        let cap = capacity.get();
        if cap > (u32::MAX >> 1) as usize {
            panic!("capacity must be <= {}", u32::MAX >> 1);
        }

        let node_map: HashMap<K, u32, S> = HashMap::with_capacity_and_hasher(cap, hasher);

        SlabShard {
            cap,
            node_map,
            slab: Vec::with_capacity(cap),
            head: None,
            tail: None,
            stats: St::default(),
            default_ttl: Some(default_ttl),
        }
    }

    /// Performs a liveness check on a particular entry, to determine if the TTL on the entry has
    /// expired.
    #[inline]
    fn is_entry_valid(&self, entry_idx: u32) -> bool {
        debug_assert!((entry_idx as usize) < self.slab.len());
        unsafe { self.slab.get_unchecked(entry_idx as usize).is_live() }
    }

    /// Returns `true` if the key exists in the cache without promoting it or recording a hit.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let Some(entry_idx) = self.node_map.get(key) else {
            return false;
        };
        self.is_entry_valid(*entry_idx)
    }

    /// Returns the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.slab.len()
    }

    /// Promote an entry to head.
    pub(crate) fn promote(&mut self, entry_idx: u32) {
        debug_assert!(self.head.is_some());
        let head_idx = self.head.unwrap();
        if head_idx != entry_idx {
            self.unlink_node(entry_idx);
            self.push_node_to_head(entry_idx);
        }
    }

    // Promotes the entry at `entry_idx` to the head of the recency list and overwrites its value.
    // Safety: `entry_idx` must be a valid index into `self.slab`, obtained from `node_map`.
    // The cache must be non-empty when this is called (debug-asserted).
    #[inline(always)]
    fn update_cache_entry(&mut self, entry_idx: u32, value: V, expires: u64) {
        debug_assert!(self.head.is_some());
        let head_idx = self.head.unwrap();
        if head_idx != entry_idx {
            self.unlink_node(entry_idx);
            self.push_node_to_head(entry_idx);
        }

        // SAFETY: at this point we have validated that the pointer is non null
        // and a mutable update is safe
        debug_assert!((entry_idx as usize) < self.slab.len());
        unsafe {
            let entry = self.slab.get_unchecked_mut(entry_idx as usize);
            entry.value = value;
            entry.expires = expires;
        };
    }

    /// Updates the value for an existing key and promotes it to most recently used.
    ///
    /// Returns `Err(CacheError::KeyNotExist)` if the key is not in the cache. Use `insert` to
    /// write a new key. There is no `get_mut` — this is the correct method for mutating a stored
    /// value.
    pub fn update<Q>(&mut self, key: &Q, value: V) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.update_entry(key, value, self.default_ttl)
    }

    #[inline]
    fn update_entry<Q>(
        &mut self,
        key: &Q,
        value: V,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        debug_assert!(
            (self.head.is_some() && self.tail.is_some())
                || (self.head.is_none() && self.tail.is_none())
        );
        let Some(entry_idx) = self.node_map.get(key) else {
            return Err(CacheError::KeyNotExist);
        };
        self.stats.hit();
        self.update_cache_entry(*entry_idx, value, util::expires_from_ttl(ttl));

        Ok(())
    }
    /// Updates the value for an existing key along with the item's ttl and promotes it to
    /// most recently used.
    ///
    /// Returns `Err(CacheError::KeyNotExist)` if the key is not in the cache. Use `insert` to
    /// write a new key. There is no `get_mut` — this is the correct method for mutating a stored
    /// value.
    pub fn update_with_ttl<Q>(&mut self, key: &Q, value: V, ttl: Duration) -> Result<(), CacheError>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.update_entry(key, value, Some(ttl))
    }

    #[inline]
    fn default_expiration(&self) -> u64 {
        util::expires_from_ttl(self.default_ttl)
    }

    pub(crate) fn get_key_from_entry_position(&self, entry_idx: u32) -> K {
        debug_assert!((entry_idx as usize) < self.slab.len());
        unsafe { self.slab.get_unchecked(entry_idx as usize).key.clone() }
    }

    /// Removes the entry for the given key and returns its value, or `None` if the key is not
    /// present. Does not count as a miss in statistics.
    ///
    /// Uses `swap_remove` internally: the last slab entry is moved into the evicted slot, and all
    /// index references to it (in `node_map` and the recency list) are updated accordingly.
    pub fn evict<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (_, v, _) = self.evict_full_entry(key)?;
        Some(v)
    }

    pub(crate) fn evict_full_entry<Q>(&mut self, key: &Q) -> Option<(K, V, u64)>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let entry_idx = self.node_map.remove(key)?;
        Some(self.evict_entry(entry_idx))
    }

    pub(crate) fn evict_entry(&mut self, entry_idx: u32) -> (K, V, u64) {
        self.unlink_node(entry_idx);

        #[cfg(debug_assertions)]
        {
            debug_assert!(
                (self.head.is_some() && self.tail.is_some())
                    || (self.head.is_none() && self.tail.is_none())
            )
        }
        let len = self.len();
        if len > 1 && entry_idx as usize != len - 1 {
            let (swap_key, swap_prev, swap_next) = unsafe {
                let entry = &self.slab.get_unchecked(len - 1);
                (&entry.key, entry.prev(), entry.next())
            };
            *self.node_map.get_mut::<K>(swap_key).unwrap() = entry_idx;
            if let Some(swap_prev) = swap_prev {
                debug_assert!((swap_prev as usize) < self.slab.len());
                let entry = unsafe { self.slab.get_unchecked_mut(swap_prev as usize) };
                entry.set_next(Some(entry_idx));
            }
            if let Some(swap_next) = swap_next {
                debug_assert!((swap_next as usize) < self.slab.len());
                let entry = unsafe { self.slab.get_unchecked_mut(swap_next as usize) };
                entry.set_prev(Some(entry_idx));
            }
            if self.head == Some(len as u32 - 1) {
                self.head = Some(entry_idx)
            }
            if self.tail == Some(len as u32 - 1) {
                self.tail = Some(entry_idx)
            }
        }

        let CacheEntry {
            key,
            value,
            expires,
            ..
        } = self.slab.swap_remove(entry_idx as usize);
        (key, value, expires)
    }

    pub(crate) fn evict_from_idx(&mut self, entry_idx: u32) {
        debug_assert!((entry_idx as usize) < self.slab.len());
        let key = unsafe { &self.slab.get_unchecked(entry_idx as usize).key };
        let _ = self.node_map.remove(key);
        let _ = self.evict_entry(entry_idx);
    }

    /// Returns `true` if the cache contains no entries.
    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.head.is_none() && self.tail.is_none() && self.len() == 0
    }

    /// Returns `true` if the cache is at capacity. The next insert of a new key will evict the
    /// least recently used entry.
    pub fn is_full(&self) -> bool {
        self.node_map.len() == self.cap
    }

    /// Insert a new item into the cache with a non default TTL. If the item already exists, then
    /// the value will be update, TTL will be update, and the entry will be promoted.
    pub fn insert_with_ttl(&mut self, key: K, value: V, ttl: Duration) {
        let _ = self.insert_entry(key, value, util::expires_from_ttl(Some(ttl)));
    }

    pub(crate) fn insert_with_expires(&mut self, key: K, value: V, expires: u64) {
        let _ = self.insert_entry(key, value, expires);
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, its value is updated and it is promoted to most recently used.
    /// If the cache is full and the key is new, the least recently used entry is evicted and its
    /// slab slot is reused in-place for the new entry — no allocation or deallocation occurs.
    pub fn insert(&mut self, key: K, value: V) {
        self.insert_entry(key, value, self.default_expiration())
    }

    fn insert_entry(&mut self, key: K, value: V, expires: u64) {
        let idx = if self.is_full() {
            match self.node_map.get(&key) {
                Some(entry_idx) => {
                    self.update_cache_entry(*entry_idx, value, expires);
                    return;
                }
                None => {
                    let stale_idx = self.pop_tail();
                    debug_assert!((stale_idx as usize) < self.slab.len());
                    let stale_entry = unsafe { self.slab.get_unchecked_mut(stale_idx as usize) };
                    stale_entry.key = key.clone();
                    stale_entry.value = value;
                    // next pointer should already be null;
                    stale_entry.clear_neighbors();
                    stale_entry.expires = expires;
                    self.node_map.insert(key, stale_idx);
                    stale_idx
                }
            }
        } else {
            match self.node_map.entry(key.clone()) {
                Entry::Occupied(occ_entry_idx) => {
                    let entry_idx = *occ_entry_idx.get();
                    self.update_cache_entry(entry_idx, value, expires);
                    return;
                }
                Entry::Vacant(vac_entry) => {
                    let new_entry = CacheEntry {
                        key,
                        value,
                        list_neighbors: util::pointer_idx::null_neighbors(),
                        expires,
                    };
                    let entry_idx = self.slab.len();
                    self.slab.push(new_entry);
                    vac_entry.insert_entry(entry_idx as u32);
                    entry_idx as u32
                }
            }
        };

        self.push_node_to_head(idx);
    }

    /// Empty the cache.
    /// After this call, cache will be empty.
    pub fn drain(&mut self) {
        self.head = None;
        self.tail = None;
        self.node_map.clear();
        self.slab.clear();
    }

    #[inline(always)]
    fn unlink_node(&mut self, entry_idx: u32) {
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                (self.head.is_some() && self.tail.is_some())
                    || (self.head.is_none() && self.tail.is_none())
            );
            debug_assert!((entry_idx as usize) < self.slab.len());
        }
        let (prev_opt, next_opt) = {
            let entry: &mut CacheEntry<K, V> =
                unsafe { self.slab.get_unchecked_mut(entry_idx as usize) };
            let (prev_opt, next_opt) = (entry.prev(), entry.next());
            entry.list_neighbors = util::pointer_idx::null_neighbors();
            (prev_opt, next_opt)
        };

        match (prev_opt, next_opt) {
            (Some(prev), Some(next)) => {
                debug_assert!((prev as usize) < self.slab.len());
                debug_assert!((next as usize) < self.slab.len());
                // node is in the middle of the list
                unsafe {
                    self.slab
                        .get_unchecked_mut(prev as usize)
                        .set_next(next_opt)
                }
                unsafe {
                    self.slab
                        .get_unchecked_mut(next as usize)
                        .set_prev(prev_opt)
                }
            }
            (Some(prev), None) => {
                debug_assert!((prev as usize) < self.slab.len());
                // node is current the tail
                unsafe { self.slab.get_unchecked_mut(prev as usize).set_next(None) }
                self.tail = prev_opt
            }
            (None, Some(next)) => {
                debug_assert!((next as usize) < self.slab.len());
                // node is current head
                unsafe { self.slab.get_unchecked_mut(next as usize).set_prev(None) }
                self.head = next_opt
            }
            (None, None) => {
                // node is both head and tail
                // no unlinking required
                self.head = None;
                self.tail = None;
            }
        }
    }

    #[inline(always)]
    fn push_node_to_head(&mut self, entry_idx: u32) {
        /*
         * Assume the node is unlinked here.
         * Take the current head, set its prev to new head idx
         * Set new head idx to prev head, update the head idx
         * */
        // this method assumes that a node is fully unlinked before being pushed to the head
        #[cfg(debug_assertions)]
        {
            // assert general invariants
            // also assert this node is transiently unlinked
            // unlink op should always happen before pushing to head
            debug_assert!(
                (self.head.is_some() && self.tail.is_some())
                    || (self.head.is_none() && self.tail.is_none())
            );

            debug_assert!((entry_idx as usize) < self.slab.len());
            let node: &mut CacheEntry<K, V> = &mut self.slab[entry_idx as usize];
            // if we get a cache hit on the key, then head should be Some
            let (prev, next) = (node.prev(), node.next());
            debug_assert!(prev.is_none() && next.is_none());
        }

        let node: &mut CacheEntry<K, V> =
            unsafe { self.slab.get_unchecked_mut(entry_idx as usize) };

        // if the list is non-empty, update the current head prev pointer to point to node
        // update node next to point to current head
        // if the list is empty set the node to be the head and tail
        if let Some(head_idx) = self.head {
            node.set_next(self.head);
            node.set_prev(None);
            unsafe {
                self.slab
                    .get_unchecked_mut(head_idx as usize)
                    .set_prev(Some(entry_idx))
            }
            self.head = Some(entry_idx);
        } else {
            // when list is currently empty, ensure that both node pointers are null

            // head and tail both get set to current node when cache is empty
            self.head = Some(entry_idx);
            self.tail = Some(entry_idx)
        }
    }

    #[inline(always)]
    fn pop_tail(&mut self) -> u32 {
        // method assumes that it is only called when the cache is at capacity, requiring an
        // eviction
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                (self.head.is_some() && self.tail.is_some())
                    || (self.head.is_none() && self.tail.is_none())
            );

            debug_assert!(self.node_map.len() == self.cap);
            debug_assert!(self.tail.is_some());
        }

        // safe unwrap, validate above that tail is Some
        let tail_idx = self.tail.unwrap();
        self.unlink_node(tail_idx);
        self.stats.eviction();

        unsafe {
            self.node_map
                .remove(&self.slab.get_unchecked(tail_idx as usize).key)
                .unwrap()
        }
    }

    /// Returns a clone of the value for the given key and promotes it to most recently used.
    ///
    /// Returns `None` on a cache miss.
    pub fn get<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let Some(entry_idx_ref) = self.node_map.get(key) else {
            self.stats.miss();
            return None;
        };
        #[cfg(debug_assertions)]
        {
            // if key is cache hit, head and tail must be Some
            debug_assert!(self.head.is_some() && self.tail.is_some());
        }
        self.get_and_promote(*entry_idx_ref)
    }

    // Get an entry without promoting. Only called by [crate::dash_cache::DashCache]. In this case
    // it is queued for next write lock acquisition.
    pub(crate) fn get_no_promote<Q>(&self, key: &Q) -> GetResult<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let Some(entry_idx_ref) = self.node_map.get(key) else {
            self.stats.miss();
            return GetResult::Miss;
        };

        let entry_idx = *entry_idx_ref;

        if !self.is_entry_valid(entry_idx) {
            self.stats.expiration();
            return GetResult::Expired(entry_idx);
        }
        self.stats.hit();

        debug_assert!((entry_idx as usize) < self.slab.len());
        let value = unsafe { self.slab.get_unchecked(entry_idx as usize).value.clone() };
        GetResult::Hit(value, entry_idx)
    }

    fn get_and_promote(&mut self, entry_idx: u32) -> Option<V> {
        if !self.is_entry_valid(entry_idx) {
            self.stats.expiration();
            self.evict_from_idx(entry_idx);
            return None;
        }

        self.stats.hit();

        // Promote entry to head.
        let head_idx = self.head.unwrap();
        if head_idx != entry_idx {
            self.unlink_node(entry_idx);
            self.push_node_to_head(entry_idx);
        }

        let value = unsafe { self.slab.get_unchecked(entry_idx as usize).value.clone() };

        Some(value)
    }

    /// Returns a snapshot of cache hit, miss, expirations, eviction counts.
    pub fn statistics(&self) -> CacheStats {
        self.stats.snapshot()
    }
}

#[cfg(test)]
mod indexed_shard_cache_test {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn constructs_empty() {
        let c: SlabShard<i32, i32, _> = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        assert_eq!(c.node_map.len(), 0);
        assert!(c.head.is_none());
        assert!(c.tail.is_none());
    }

    #[test]
    fn insert_get_contains() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("a", 1);
        c.insert("b", 2);
        assert!(c.contains(&"a"));
        assert!(c.contains(&"b"));
        assert_eq!(c.get(&"a"), Some(1));
        assert_eq!(c.get(&"b"), Some(2));
        assert_eq!(c.node_map.len(), 2);
        assert!(c.head.is_some());
        assert!(c.tail.is_some());
    }

    #[test]
    fn get_promotes_to_head() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);

        assert_eq!(c.get(&"a"), Some(1));

        c.insert("d", 4);

        assert!(c.contains(&"a"));
        assert!(c.contains(&"c"));
        assert!(c.contains(&"d"));
        assert!(!c.contains(&"b"));
    }

    #[test]
    fn update_changes_value_and_promotes() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(2).unwrap());
        c.insert("x", 10);
        c.insert("y", 20);

        c.update(&"x", 11).unwrap();
        assert_eq!(c.get(&"x"), Some(11));

        c.insert("z", 30);
        assert!(c.contains(&"x"));
        assert!(c.contains(&"z"));
        assert!(!c.contains(&"y"));
    }

    #[test]
    fn drain_empties_cache() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        c.drain();
        assert_eq!(c.node_map.len(), 0);
        assert!(c.head.is_none());
        assert!(c.tail.is_none());
        assert_eq!(c.get(&"a"), None);
    }

    #[test]
    fn many_inserts_and_accesses_preserve_invariants() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(5).unwrap());
        for i in 0..10 {
            c.insert(i, i * 10);
            assert_eq!(c.head.is_some(), c.tail.is_some());
            assert!(c.node_map.len() <= 5);
        }

        for i in [7, 8, 9] {
            assert_eq!(c.get(&i), Some(i * 10));
            assert_eq!(c.head.is_some(), c.tail.is_some());
        }

        let keys: HashSet<_> = c.node_map.keys().cloned().collect();
        assert_eq!(keys.len(), c.node_map.len());
        assert!(c.node_map.len() <= 5);
    }

    #[test]
    fn len() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert_eq!(c.len(), 3)
    }

    #[test]
    fn evict() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert_eq!(c.evict(&"a"), Some(1));
        assert_eq!(c.evict(&"c"), Some(3));
        assert_eq!(c.evict(&"a"), None);
        assert_eq!(c.evict(&"b"), Some(2));
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn full_and_empty() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert!(c.is_full());
        assert_eq!(c.evict(&"a"), Some(1));
        assert_eq!(c.evict(&"c"), Some(3));
        assert_eq!(c.evict(&"a"), None);
        assert_eq!(c.evict(&"b"), Some(2));
        assert!(c.is_empty());
    }

    // The following tests target the non-full insert path (Entry API occupied branch).
    // A prior bug called push_node_to_head after update_cache_entry already handled
    // promotion, corrupting the list. None of the tests above cover a re-insert into
    // a non-full cache.

    #[test]
    fn insert_existing_non_head_key_non_full_updates_value() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(5).unwrap());
        c.insert(1, 10);
        c.insert(2, 20);
        c.insert(3, 30);
        // cache is non-full, re-insert a non-head key with a new value
        c.insert(1, 99);
        assert_eq!(c.get(&1), Some(99));
        assert_eq!(c.get(&2), Some(20));
        assert_eq!(c.get(&3), Some(30));
        assert_eq!(c.len(), 3);
    }

    #[test]
    fn insert_existing_non_head_key_non_full_promotes_and_evicts_correctly() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(5).unwrap());
        c.insert(1, 10); // tail
        c.insert(2, 20);
        c.insert(3, 30); // head
        // re-insert 1 (tail) — should become head, 2 becomes new tail
        c.insert(1, 99);
        // fill to capacity
        c.insert(4, 40);
        c.insert(5, 50);
        // now full; inserting 6 should evict 2, the new LRU
        c.insert(6, 60);
        assert!(c.contains(&1));
        assert!(!c.contains(&2));
        assert!(c.contains(&3));
        assert_eq!(c.get(&1), Some(99));
    }

    #[test]
    fn insert_existing_head_key_non_full_updates_value() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(5).unwrap());
        c.insert(1, 10);
        c.insert(2, 20); // head
        // re-insert the current head — list structure must stay consistent
        c.insert(2, 99);
        assert_eq!(c.get(&2), Some(99));
        assert_eq!(c.get(&1), Some(10));
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn insert_existing_head_key_non_full_eviction_order_unchanged() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(5).unwrap());
        c.insert(1, 10); // tail
        c.insert(2, 20);
        c.insert(3, 30); // head
        c.insert(3, 99); // re-insert head — 1 should remain tail
        c.insert(4, 40);
        c.insert(5, 50); // now full
        c.insert(6, 60); // evicts 1
        assert!(!c.contains(&1));
        assert!(c.contains(&2));
        assert_eq!(c.get(&3), Some(99));
    }

    #[test]
    fn repeated_reinserts_non_full_maintain_integrity() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(10).unwrap());
        for i in 0..5 {
            c.insert(i, i * 10);
        }
        // repeatedly re-insert the same key with updated values
        for v in 0..5 {
            c.insert(2, v);
        }
        assert_eq!(c.get(&2), Some(4));
        assert_eq!(c.len(), 5);
        // all other keys still reachable
        assert_eq!(c.get(&0), Some(0));
        assert_eq!(c.get(&1), Some(10));
        assert_eq!(c.get(&3), Some(30));
        assert_eq!(c.get(&4), Some(40));
    }

    #[test]
    fn get_accepts_borrowed_key() {
        let mut c: SlabShard<String, u32, _> =
            SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("hello".to_string(), 1);
        c.insert("world".to_string(), 2);
        // &str is accepted where K = String via Borrow<str>
        assert_eq!(c.get("hello"), Some(1));
        assert_eq!(c.get("world"), Some(2));
        assert_eq!(c.get("missing"), None);
        let stats = c.statistics();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn contains_accepts_borrowed_key() {
        let mut c: SlabShard<String, u32, _> =
            SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("hello".to_string(), 1);
        assert!(c.contains("hello"));
        assert!(!c.contains("missing"));
    }

    #[test]
    fn evict_accepts_borrowed_key() {
        let mut c: SlabShard<String, u32, _> =
            SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("hello".to_string(), 1);
        c.insert("world".to_string(), 2);
        assert_eq!(c.evict("hello"), Some(1));
        assert_eq!(c.evict("hello"), None);
        assert!(c.contains("world"));
    }

    #[test]
    fn update_accepts_borrowed_key() {
        let mut c: SlabShard<String, u32, _> =
            SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("hello".to_string(), 1);
        c.update("hello", 99).unwrap();
        assert_eq!(c.get("hello"), Some(99));
        assert!(c.update("missing", 0).is_err());
    }

    // TTL tests

    #[test]
    fn insert_with_ttl_accessible_before_expiry() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert_with_ttl("a", 1, Duration::from_secs(60));
        assert_eq!(c.get(&"a"), Some(1));
        assert!(c.contains(&"a"));
        let stats = c.statistics();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.expirations, 0);
    }

    #[test]
    fn get_returns_none_after_expiry() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert_with_ttl("a", 1, Duration::from_millis(50));
        assert_eq!(c.get(&"a"), Some(1));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(c.get(&"a"), None);
    }

    #[test]
    fn expiration_stat_incremented_on_get() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert_with_ttl("a", 1, Duration::from_millis(50));
        c.insert_with_ttl("b", 2, Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(c.get(&"a"), None);
        assert_eq!(c.get(&"b"), None);
        let stats = c.statistics();
        assert_eq!(stats.expirations, 2);
        assert_eq!(stats.hits, 0);
    }

    #[test]
    fn expired_entry_evicted_from_slab_on_get() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert_with_ttl("a", 1, Duration::from_millis(50));
        c.insert("b", 2);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(c.get(&"a"), None);
        assert!(!c.contains(&"a"));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn contains_returns_false_for_expired_entry() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert_with_ttl("a", 1, Duration::from_millis(50));
        assert!(c.contains(&"a"));
        std::thread::sleep(Duration::from_millis(100));
        assert!(!c.contains(&"a"));
    }

    #[test]
    fn contains_does_not_evict_get_does() {
        // contains checks liveness but does not evict; get does both
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(2).unwrap());
        c.insert_with_ttl("a", 1, Duration::from_millis(50));
        c.insert("b", 2);
        std::thread::sleep(Duration::from_millis(100));
        assert!(!c.contains(&"a"));
        assert_eq!(c.len(), 2); // still occupies a slot
        assert_eq!(c.get(&"a"), None);
        assert_eq!(c.len(), 1); // evicted on get
    }

    #[test]
    fn update_with_ttl_resets_expiration() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert_with_ttl("a", 1, Duration::from_millis(50));
        c.update_with_ttl(&"a", 2, Duration::from_secs(60)).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(c.get(&"a"), Some(2));
    }

    #[test]
    fn expired_slot_freed_for_new_insert() {
        // After an expired entry is evicted via get, a new insert must succeed
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(2).unwrap());
        c.insert_with_ttl("a", 1, Duration::from_millis(50));
        c.insert("b", 2);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(c.get(&"a"), None); // evicts "a"
        c.insert("c", 3);
        assert_eq!(c.get(&"c"), Some(3));
        assert_eq!(c.get(&"b"), Some(2));
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn no_ttl_entry_never_expires() {
        let mut c = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("a", 1); // no TTL
        c.insert_with_ttl("b", 2, Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(c.get(&"a"), Some(1));
        assert_eq!(c.get(&"b"), None);
        let stats = c.statistics();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.expirations, 1);
    }
}
