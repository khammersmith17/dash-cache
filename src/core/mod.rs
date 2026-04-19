use core::ptr::NonNull;
use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::{BuildHasher, Hash};
use std::num::NonZeroUsize;
use std::rc::{Rc, Weak};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Key does not exist in cache")]
    KeyNotExist,
    #[error("Cache is internally corrupt, file a bug report")]
    CorruptedCacheError,
}

#[derive(Default, Debug, Clone)]
pub struct CacheStats {
    pub hits: usize,
    pub misses: usize,
    pub evictions: usize,
}

impl std::ops::Add<CacheStats> for CacheStats {
    type Output = Self;
    fn add(self, other: Self) -> Self::Output {
        CacheStats {
            hits: self.hits + other.hits,
            misses: self.misses + other.misses,
            evictions: self.evictions + other.evictions,
        }
    }
}

impl std::ops::AddAssign<CacheStats> for CacheStats {
    fn add_assign(&mut self, other: Self) {
        self.hits += other.hits;
        self.misses += other.misses;
        self.evictions += other.evictions;
    }
}

impl CacheStats {
    fn miss(&mut self) {
        self.misses += 1;
    }

    fn hit(&mut self) {
        self.hits += 1;
    }

    fn eviction(&mut self) {
        self.evictions += 1
    }
}

pub struct CacheItem<K, T> {
    pub key: K,
    pub value: T,
}

#[derive(Debug)]
struct CacheEntry<K, T> {
    value: T,
    key: K,
    prev: Option<Weak<RefCell<CacheEntry<K, T>>>>,
    next: Option<Weak<RefCell<CacheEntry<K, T>>>>,
}

/// A single-threaded LRU cache. Not `Send` or `Sync` — use `DashCache` for concurrent access.
///
/// Internally uses `Rc<RefCell<T>>` for linked list nodes, making this a fully safe implementation.
/// A "use" is defined as any read or write — both promote the key to most recently used.
/// When the cache is full, the least recently used item is evicted on the next insert.
/// All values returned are clones due to the internal borrowing mechanics of `RefCell`.
///
/// Debug builds run invariant assertions on every operation. Release performance is good but
/// slower than `CacheShard` or `SlabShard` due to reference counting overhead.
#[derive(Debug)]
pub struct LruCache<K, T> {
    cap: usize,
    node_map: HashMap<K, Rc<RefCell<CacheEntry<K, T>>>>,
    head: Option<Weak<RefCell<CacheEntry<K, T>>>>,
    tail: Option<Weak<RefCell<CacheEntry<K, T>>>>,
    stats: CacheStats,
}

impl<K, T> LruCache<K, T>
where
    K: Hash + Eq + Clone + std::fmt::Debug,
    T: Clone + std::fmt::Debug,
{
    /// Creates a new `LruCache` with the given capacity.
    pub fn with_capacity(capacity: NonZeroUsize) -> LruCache<K, T> {
        let cap = capacity.get();
        let node_map: HashMap<K, Rc<RefCell<CacheEntry<K, T>>>> = HashMap::with_capacity(cap);
        LruCache {
            cap,
            node_map,
            head: None,
            tail: None,
            stats: CacheStats::default(),
        }
    }

    /// Returns the number of items currently in the cache.
    pub fn len(&self) -> usize {
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!(self.node_map.len(), self.linked_list_len());
        }
        self.node_map.len()
    }

    /// Returns a clone of the key at the head of the recency list (most recently used entry),
    /// or `None` if the cache is empty.
    pub fn head(&self) -> Option<K> {
        match self.head {
            Some(ref weak_head) => {
                let head = weak_head.upgrade()?;
                Some(head.borrow().key.clone())
            }
            None => None,
        }
    }

    /// Removes the entry for the given key from the cache and returns its value, or `None` if the
    /// key is not present. Does not count as a miss in statistics.
    pub fn evict(&mut self, key: &K) -> Option<T> {
        let cache_entry = self.node_map.remove(key)?;

        self.unlink_node(&cache_entry);

        #[cfg(debug_assertions)]
        {
            debug_assert!(
                (self.head.is_some() && self.tail.is_some())
                    || (self.head.is_none() && self.tail.is_none())
            )
        }

        // node unlinked
        // only strong ref is stored in map
        // thus, strong count should be one and we can safely unwrap the Rc
        debug_assert!(Rc::strong_count(&cache_entry) == 1);

        let deref_entry = Rc::try_unwrap(cache_entry).unwrap();
        let entry_inner = deref_entry.into_inner();
        let CacheEntry { value, .. } = entry_inner;

        Some(value)
    }

    /// Returns a snapshot of cache hit, miss, and eviction counts.
    pub fn statistics(&self) -> CacheStats {
        self.stats.clone()
    }

    /// Returns `true` if the key exists in the cache without promoting it or recording a hit.
    pub fn contains(&self, key: &K) -> bool {
        self.node_map.contains_key(key)
    }

    /// Updates the value for an existing key and promotes it to most recently used.
    ///
    /// Returns `Err(CacheError::KeyNotExist)` if the key is not in the cache. Use `insert` to
    /// write a new key. There is no `get_mut` — this is the correct method for mutating a stored
    /// value.
    pub fn update(&mut self, key: &K, value: T) -> Result<(), CacheError> {
        #[cfg(debug_assertions)]
        {
            self.assert_invariants();
        }
        let node_rc = {
            let Some(rc) = self.node_map.get(key) else {
                self.stats.miss();
                return Err(CacheError::KeyNotExist);
            };
            rc.clone()
        };

        self.stats.hit();

        if let Some(head_clone) = self.head.clone() {
            if let Some(head_rc) = head_clone.upgrade() {
                if !Rc::ptr_eq(&head_rc, &node_rc) {
                    self.unlink_node(&node_rc);
                    let node_weak_ref = Rc::downgrade(&node_rc);
                    self.push_node_to_head(node_weak_ref);
                }
            }
        }
        let mut node_ref: RefMut<CacheEntry<K, T>> = node_rc.as_ref().borrow_mut();
        node_ref.value = value;
        Ok(())
    }

    /// Returns `true` if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.head.is_none() && self.tail.is_none() && self.len() == 0
    }

    /// Returns `true` if the cache is at capacity. The next insert of a new key will evict the
    /// least recently used entry.
    pub fn is_full(&self) -> bool {
        self.len() == self.cap
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, its value is updated and it is promoted to most recently used.
    /// If the cache is full and the key is new, the least recently used entry is evicted first.
    pub fn insert(&mut self, key: K, value: T) {
        #[cfg(debug_assertions)]
        {
            self.assert_invariants();
        }
        match self.node_map.contains_key(&key) {
            true => {
                let _ = self.update(&key, value);
            }
            false => {
                let new_node = Rc::new(RefCell::new(CacheEntry {
                    key: key.clone(),
                    value,
                    prev: None,
                    next: None,
                }));

                // always call pop tail
                // returns early if there is no need to evict
                if self.is_full() {
                    self.pop_tail();
                }

                self.push_node_to_head(Rc::downgrade(&new_node));
                self.node_map.insert(key, new_node);
            }
        }
    }

    /// Empty the cache.
    /// After this call, cache will be empty.
    pub fn drain(&mut self) {
        self.head = None;
        self.tail = None;
        self.node_map.clear();
    }

    // internal method to run in debug mode and assert that invariants are always consistent.
    #[cfg(debug_assertions)]
    fn assert_invariants(&self) {
        debug_assert!(
            (self.head.is_some() && self.tail.is_some())
                || (self.head.is_none() && self.tail.is_none())
        )
    }

    // remove the node from its current position in the list determining usage recency.
    #[inline]
    fn unlink_node(&mut self, node: &Rc<RefCell<CacheEntry<K, T>>>) {
        #[cfg(debug_assertions)]
        {
            self.assert_invariants();
        }
        // if the list is empty, then no movement needs to happen
        if self.is_empty() {
            return;
        }

        // get weak reference to prev and next
        // pull immutable reference from RefCell
        // scope shared borrow
        let (prev_weak, next_weak) = {
            let node_ref = node.borrow();
            (node_ref.prev.clone(), node_ref.next.clone())
        };

        // 4 variant cases
        // prev, next
        // Some, Some -> this node is somewhere in the middle of the list
        // Some, None -> This node is the current tail
        // None, Some -> this node is the current head
        // None, None -> cache is of size 1, current node is both head and tail

        match (&prev_weak, &next_weak) {
            (Some(prev), Some(next)) => {
                // node is in the middle of the list
                debug_assert!(prev.strong_count() >= 1 && next.strong_count() >= 1);

                // assertion validates that strong count >= 1, upgrade is safe
                let prev_rc_opt = prev.upgrade();
                let next_rc_opt = next.upgrade();
                debug_assert!(prev_rc_opt.is_some() && next_rc_opt.is_some());

                if let (Some(prev_rc), Some(next_rc)) = (prev_rc_opt, next_rc_opt) {
                    prev_rc.borrow_mut().next = next_weak.clone();
                    next_rc.borrow_mut().prev = prev_weak.clone();
                }
            }
            (None, Some(next)) => {
                // node is current head
                debug_assert!(next.strong_count() >= 1);
                let next_rc = next.upgrade().unwrap();
                next_rc.borrow_mut().prev = None;
                self.head = next_weak.clone();
            }
            (Some(prev), None) => {
                // node is current tail
                debug_assert!(prev.strong_count() >= 1);
                let prev_rc = prev.upgrade().unwrap();
                prev_rc.borrow_mut().next = None;
                self.tail = prev_weak.clone();
            }
            (None, None) => {
                // current node is both head and tail
                // both list refs are none, no need to clear, thus can return
                // no unlinking required
                self.head = None;
                self.tail = None;
            }
        }

        let mut node_ref = node.borrow_mut();
        node_ref.prev = None;
        node_ref.next = None;
    }

    // pushed node to the front of the list determining how recently an entry was used/inserted.
    #[inline]
    fn push_node_to_head(&mut self, node: Weak<RefCell<CacheEntry<K, T>>>) {
        #[cfg(debug_assertions)]
        {
            self.assert_invariants();
        }
        // if the list is empty set the node to be the head and tail
        if self.is_empty() {
            self.head = Some(node.clone());
            self.tail = Some(node);
            return;
        }

        // upgrade head to Rc to get mutable access
        // set the curr head prev pointer to be a weak referene to the current node
        if let Some(curr_head) = self.head.clone() {
            if let Some(curr_head_rc) = curr_head.upgrade() {
                let mut curr_head_mut = curr_head_rc.as_ref().borrow_mut();
                curr_head_mut.prev = Some(node.clone());
            }
        }

        // upgrade current node to RC to get mutable access
        // set prev to None
        // set next on current node to current head
        if let Some(new_head_rc) = node.upgrade() {
            let mut new_head_mut = new_head_rc.as_ref().borrow_mut();
            new_head_mut.prev = None;
            new_head_mut.next = self.head.clone();
        }

        // promote node to head
        self.head = Some(node)
    }

    // debug only method to get the length of the internal list. This is to assert that the number
    // of nodes in the internal map equals the number of nodes in the recency list.
    #[cfg(debug_assertions)]
    fn linked_list_len(&self) -> usize {
        let mut len = 0_usize;
        let mut curr = if let Some(ref head_weak) = self.head {
            head_weak.upgrade()
        } else {
            return len;
        };

        while curr.is_some() {
            len += 1;

            let curr_inner = curr.clone().unwrap();
            let curr_ref = curr_inner.borrow();

            curr = if let Some(ref next) = curr_ref.next {
                next.upgrade()
            } else {
                None
            };
        }

        len
    }

    // Upon eviction, pop the entry off the recency list.
    #[inline]
    fn pop_tail(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.assert_invariants();
        }
        self.stats.eviction();

        let curr_tail_weak_ref = self.tail.clone().unwrap();
        let curr_tail_rc = curr_tail_weak_ref.upgrade().unwrap();
        self.unlink_node(&curr_tail_rc);

        self.node_map.remove(&curr_tail_rc.as_ref().borrow().key);
    }

    /// Returns a clone of the value for the given key and promotes it to most recently used.
    ///
    /// Returns `None` on a cache miss.
    pub fn get(&mut self, key: &K) -> Option<T> {
        #[cfg(debug_assertions)]
        {
            self.assert_invariants();
        }

        // Smoke check to for is cache is empty.
        // Cache is empty if head is None.
        let _ = self.head.as_ref()?;
        let node_rc = if let Some(src_node_rc) = self.node_map.get(key) {
            src_node_rc.clone()
        } else {
            self.stats.miss();
            return None;
        };

        self.stats.hit();
        let node_ref: RefMut<CacheEntry<K, T>> = node_rc.as_ref().borrow_mut();
        let value = node_ref.value.clone();
        let res = Some(value);
        drop(node_ref);

        if let Some(head_clone) = self.head.clone() {
            if let Some(head_rc) = head_clone.upgrade() {
                if !Rc::ptr_eq(&head_rc, &node_rc) {
                    self.unlink_node(&node_rc);
                    let node_weak_ref = Rc::downgrade(&node_rc);
                    self.push_node_to_head(node_weak_ref);
                }
            }
        }
        res
    }
}

// Internal linked list node for CacheShard. Uses NonNull raw pointers for prev/next to avoid
// the reference-counting overhead of Rc, at the cost of requiring manual safety invariants.
#[derive(Clone)]
struct ShardCacheEntry<K, T> {
    key: K,
    value: T,
    next: Option<NonNull<ShardCacheEntry<K, T>>>,
    prev: Option<NonNull<ShardCacheEntry<K, T>>>,
}

/// An unsafe, single-threaded LRU cache using `NonNull` raw pointers and `Box`-heap-allocated
/// nodes for the recency list.
///
/// Safety invariants are documented inline and heavily asserted in debug builds. Release builds
/// skip these checks and perform consistently with comparable crates such as `lru`.
///
/// This type is `!Send + !Sync` due to its raw pointer fields and is intended for single-threaded
/// use only. It was the original internal shard type for `DashCache` but has since been superseded
/// by `SlabShard` for better cache locality. It is retained as a standalone cache type.
pub struct CacheShard<K, T, S = ahash::RandomState>
where
    K: Hash + Eq + Clone,
    T: Clone,
    S: BuildHasher,
{
    cap: usize,
    node_map: HashMap<K, Box<ShardCacheEntry<K, T>>, S>,
    head: Option<NonNull<ShardCacheEntry<K, T>>>,
    tail: Option<NonNull<ShardCacheEntry<K, T>>>,
    stats: CacheStats,
}

impl<K, T> CacheShard<K, T, ahash::RandomState>
where
    K: Hash + Ord + Clone,
    T: Clone,
{
    pub fn with_capacity(capacity: NonZeroUsize) -> CacheShard<K, T, ahash::RandomState> {
        CacheShard::with_capacity_and_hasher(capacity, ahash::RandomState::new())
    }
}

impl<K, T, S> CacheShard<K, T, S>
where
    K: Hash + Eq + Clone,
    T: Clone,
    S: BuildHasher,
{
    /// Creates a new `CacheShard` with the given capacity.
    pub fn with_capacity_and_hasher(capacity: NonZeroUsize, hasher: S) -> CacheShard<K, T, S> {
        let cap = capacity.get();
        let node_map: HashMap<K, Box<ShardCacheEntry<K, T>>, S> =
            HashMap::with_capacity_and_hasher(cap, hasher);

        CacheShard {
            cap,
            node_map,
            head: None,
            tail: None,
            stats: CacheStats::default(),
        }
    }

    /// Returns `true` if the key exists in the cache without promoting it or recording a hit.
    pub fn contains(&self, key: &K) -> bool {
        self.node_map.contains_key(key)
    }

    /// Returns the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.node_map.len()
    }

    // Promotes the entry at `entry_ptr` to the head of the recency list and overwrites its value.
    // Safety: must only be called with a pointer obtained from a live entry in `node_map`.
    // The cache must be non-empty when this is called (debug-asserted).
    #[inline(always)]
    fn update_cache_entry(&mut self, mut entry_ptr: NonNull<ShardCacheEntry<K, T>>, value: T) {
        debug_assert!(self.head.is_some());
        let curr_head = self.head.unwrap();
        if !curr_head.eq(&entry_ptr) {
            self.unlink_node(entry_ptr);
            self.push_node_to_head(entry_ptr);
        }

        // at this point we have validated that the pointer is non null
        // and a mutable update is safe
        unsafe { entry_ptr.as_mut().value = value };
    }

    /// Updates the value for an existing key and promotes it to most recently used.
    ///
    /// Returns `Err(CacheError::KeyNotExist)` if the key is not in the cache. Use `insert` to
    /// write a new key. There is no `get_mut` — this is the correct method for mutating a stored
    /// value.
    pub fn update(&mut self, key: &K, value: T) -> Result<(), CacheError> {
        debug_assert!(
            (self.head.is_some() && self.tail.is_some())
                || (self.head.is_none() && self.tail.is_none())
        );
        let entry_ptr = {
            let Some(cache_entry) = self.node_map.get(key) else {
                return Err(CacheError::KeyNotExist);
            };
            NonNull::from(cache_entry.as_ref())
        };
        self.stats.hit();
        self.update_cache_entry(entry_ptr, value);

        Ok(())
    }

    /// Removes the entry for the given key and returns its value, or `None` if the key is not
    /// present. Does not count as a miss in statistics.
    pub fn evict(&mut self, key: &K) -> Option<T> {
        let mut cache_entry = self.node_map.remove(key)?;
        let cache_entry_ptr = NonNull::from(cache_entry.as_mut());
        self.unlink_node(cache_entry_ptr);

        #[cfg(debug_assertions)]
        {
            debug_assert!(
                (self.head.is_some() && self.tail.is_some())
                    || (self.head.is_none() && self.tail.is_none())
            )
        }

        let ShardCacheEntry { value, .. } = *cache_entry;
        Some(value)
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

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, its value is updated and it is promoted to most recently used.
    /// If the cache is full and the key is new, the least recently used entry is evicted and its
    /// `Box` allocation is reused for the new entry to avoid an extra allocation.
    pub fn insert(&mut self, key: K, value: T) {
        let ptr = if self.is_full() {
            match self.node_map.get(&key) {
                Some(cache_entry) => {
                    self.update_cache_entry(NonNull::from(cache_entry.as_ref()), value);
                    return;
                }
                None => {
                    let mut stale_entry = self.pop_tail();
                    stale_entry.key = key.clone();
                    stale_entry.value = value;
                    stale_entry.prev = None;
                    // next should already be None
                    stale_entry.next = None;
                    let ptr = NonNull::from(stale_entry.as_mut());
                    self.node_map.insert(key, stale_entry);
                    ptr
                }
            }
        } else {
            match self.node_map.entry(key.clone()) {
                Entry::Occupied(occ_entry) => {
                    let ptr = NonNull::from(occ_entry.get().as_ref());
                    self.update_cache_entry(ptr, value);
                    return;
                }
                Entry::Vacant(vac_entry) => {
                    let mut boxed_node = Box::new(ShardCacheEntry {
                        key,
                        value,
                        prev: None,
                        next: None,
                    });
                    let ptr = NonNull::from(boxed_node.as_mut());
                    vac_entry.insert_entry(boxed_node);
                    ptr
                }
            }
        };

        self.push_node_to_head(ptr);
    }

    /// Empty the cache.
    /// After this call, cache will be empty.
    pub fn drain(&mut self) {
        self.head = None;
        self.tail = None;
        self.node_map.clear();
    }

    #[inline(always)]
    fn unlink_node(&mut self, mut node: NonNull<ShardCacheEntry<K, T>>) {
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                (self.head.is_some() && self.tail.is_some())
                    || (self.head.is_none() && self.tail.is_none())
            );
        }

        let (prev_opt, next_opt) = unsafe {
            let curr = node.as_ref();
            (curr.prev, curr.next)
        };

        match (prev_opt, next_opt) {
            (Some(mut prev), Some(mut next)) => {
                // node is in the middle of the list
                unsafe { prev.as_mut().next = next_opt }
                unsafe { next.as_mut().prev = prev_opt }
            }
            (Some(mut prev), None) => {
                // node is current the tail
                unsafe { prev.as_mut().next = None }
                self.tail = prev_opt
            }
            (None, Some(mut next)) => {
                // node is current head
                unsafe { next.as_mut().prev = None }
                self.head = next_opt
            }
            (None, None) => {
                // node is both head and tail
                // no unlinking required
                self.head = None;
                self.tail = None;
            }
        }

        unsafe {
            node.as_mut().prev = None;
            node.as_mut().next = None;
        }
    }

    #[inline(always)]
    fn push_node_to_head(&mut self, mut node: NonNull<ShardCacheEntry<K, T>>) {
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
            // if we get a cache hit on the key, then head should be Some
            let (prev, next) = unsafe {
                let node_ref = node.as_ref();
                (node_ref.prev, node_ref.next)
            };
            debug_assert!(prev.is_none() && next.is_none());
        }

        // if the list is non-empty, update the current head prev pointer to point to node
        // update node next to point to current head
        // if the list is empty set the node to be the head and tail
        if let Some(mut curr_head) = self.head {
            unsafe {
                let node_ref = node.as_mut();
                node_ref.next = self.head;
                node_ref.prev = None;
            }
            unsafe { curr_head.as_mut().prev = Some(node) }
            self.head = Some(node);
        } else {
            // when list is currently empty, ensure that both node pointers are null
            unsafe {
                let node_ref = node.as_mut();
                node_ref.next = None;
                node_ref.prev = None;
            }

            // head and tail both get set to current node when cache is empty
            self.head = Some(node);
            self.tail = Some(node)
        }
    }

    #[inline(always)]
    fn pop_tail(&mut self) -> Box<ShardCacheEntry<K, T>> {
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
        let tail_ptr = self.tail.unwrap();
        self.unlink_node(tail_ptr);
        self.stats.eviction();

        unsafe { self.node_map.remove(&tail_ptr.as_ref().key).unwrap() }
    }

    /// Returns a clone of the value for the given key and promotes it to most recently used.
    ///
    /// Returns `None` on a cache miss.
    pub fn get(&mut self, key: &K) -> Option<T> {
        let entry_ptr = {
            let Some(entry_box) = self.node_map.get(key) else {
                self.stats.miss();
                return None;
            };
            NonNull::from(entry_box.as_ref())
        };

        #[cfg(debug_assertions)]
        {
            // if key is cache hit, head and tail must be Some
            debug_assert!(self.head.is_some() && self.tail.is_some());
        }

        self.stats.hit();

        let head_ptr = self.head.unwrap();

        if !head_ptr.eq(&entry_ptr) {
            self.unlink_node(entry_ptr);
            self.push_node_to_head(entry_ptr);
        }

        let value = unsafe { entry_ptr.as_ref().value.clone() };

        Some(value)
    }

    /// Returns a snapshot of cache hit, miss, and eviction counts.
    pub fn statistics(&self) -> CacheStats {
        self.stats.clone()
    }
}

struct CacheSlabEntry<K: Hash + Eq, V: Clone> {
    key: K,
    value: V,
    prev: Option<u32>,
    next: Option<u32>,
}

/// An unsafe, single-threaded LRU cache backed by a contiguous slab allocation.
///
/// Unlike `CacheShard`, all entries live in a pre-allocated `Vec` and the recency list uses `u32`
/// slab indices instead of heap pointers. This gives significantly better cache locality,
/// especially on read-heavy workloads.
///
/// This is `Send + Sync` and is the internal shard type used by `DashCache`.
///
/// Safety invariants are documented inline and heavily asserted in debug builds. Capacity is
/// limited to `u32::MAX` entries. All values returned are clones.
pub struct SlabShard<K, V, S = ahash::RandomState>
where
    K: Hash + Ord + Clone,
    V: Clone,
    S: BuildHasher,
{
    cap: usize,
    slab: Vec<CacheSlabEntry<K, V>>,
    node_map: HashMap<K, u32, S>,
    head: Option<u32>,
    tail: Option<u32>,
    stats: CacheStats,
}

impl<K, V> SlabShard<K, V, ahash::RandomState>
where
    K: Hash + Ord + Clone,
    V: Clone,
{
    pub fn with_capacity(capacity: NonZeroUsize) -> SlabShard<K, V, ahash::RandomState> {
        SlabShard::with_capacity_and_hasher(capacity, ahash::RandomState::new())
    }
}

impl<K, V, S> SlabShard<K, V, S>
where
    K: Hash + Ord + Clone,
    V: Clone,
    S: BuildHasher,
{
    /// Creates a new `SlabShard` with the given capacity. Panics if capacity exceeds `u32::MAX`.
    pub fn with_capacity_and_hasher(capacity: NonZeroUsize, hasher: S) -> SlabShard<K, V, S> {
        let cap = capacity.get();
        if cap > u32::MAX as usize {
            panic!("capacity must be <= {}", u32::MAX);
        }

        let node_map: HashMap<K, u32, S> = HashMap::with_capacity_and_hasher(cap, hasher);

        SlabShard {
            cap,
            node_map,
            slab: Vec::with_capacity(cap),
            head: None,
            tail: None,
            stats: CacheStats::default(),
        }
    }

    /// Returns `true` if the key exists in the cache without promoting it or recording a hit.
    pub fn contains(&self, key: &K) -> bool {
        self.node_map.contains_key(key)
    }

    /// Returns the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.slab.len()
    }

    // Promotes the entry at `entry_idx` to the head of the recency list and overwrites its value.
    // Safety: `entry_idx` must be a valid index into `self.slab`, obtained from `node_map`.
    // The cache must be non-empty when this is called (debug-asserted).
    #[inline(always)]
    fn update_cache_entry(&mut self, entry_idx: u32, value: V) {
        debug_assert!(self.head.is_some());
        let head_idx = self.head.unwrap();
        if head_idx != entry_idx {
            self.unlink_node(entry_idx);
            self.push_node_to_head(entry_idx);
        }

        // at this point we have validated that the pointer is non null
        // and a mutable update is safe
        unsafe { self.slab.get_unchecked_mut(entry_idx as usize).value = value };
    }

    /// Updates the value for an existing key and promotes it to most recently used.
    ///
    /// Returns `Err(CacheError::KeyNotExist)` if the key is not in the cache. Use `insert` to
    /// write a new key. There is no `get_mut` — this is the correct method for mutating a stored
    /// value.
    pub fn update(&mut self, key: &K, value: V) -> Result<(), CacheError> {
        debug_assert!(
            (self.head.is_some() && self.tail.is_some())
                || (self.head.is_none() && self.tail.is_none())
        );
        let Some(entry_idx) = self.node_map.get(key) else {
            return Err(CacheError::KeyNotExist);
        };
        self.stats.hit();
        self.update_cache_entry(*entry_idx, value);

        Ok(())
    }

    /// Removes the entry for the given key and returns its value, or `None` if the key is not
    /// present. Does not count as a miss in statistics.
    ///
    /// Uses `swap_remove` internally: the last slab entry is moved into the evicted slot, and all
    /// index references to it (in `node_map` and the recency list) are updated accordingly.
    pub fn evict(&mut self, key: &K) -> Option<V> {
        let entry_idx = self.node_map.remove(key)?;
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
                (&entry.key, entry.prev, entry.next)
            };
            *self.node_map.get_mut(swap_key).unwrap() = entry_idx;
            if let Some(swap_prev) = swap_prev {
                unsafe { self.slab.get_unchecked_mut(swap_prev as usize).next = Some(entry_idx) };
            }
            if let Some(swap_next) = swap_next {
                unsafe { self.slab.get_unchecked_mut(swap_next as usize).prev = Some(entry_idx) };
            }
            if self.head == Some(len as u32 - 1) {
                self.head = Some(entry_idx)
            }
            if self.tail == Some(len as u32 - 1) {
                self.tail = Some(entry_idx)
            }
        }

        let CacheSlabEntry { value, .. } = self.slab.swap_remove(entry_idx as usize);
        Some(value)
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

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, its value is updated and it is promoted to most recently used.
    /// If the cache is full and the key is new, the least recently used entry is evicted and its
    /// slab slot is reused in-place for the new entry — no allocation or deallocation occurs.
    pub fn insert(&mut self, key: K, value: V) {
        let idx = if self.is_full() {
            match self.node_map.get(&key) {
                Some(entry_idx) => {
                    self.update_cache_entry(*entry_idx, value);
                    return;
                }
                None => {
                    let stale_idx = self.pop_tail();
                    let stale_entry = unsafe { self.slab.get_unchecked_mut(stale_idx as usize) };
                    stale_entry.key = key.clone();
                    stale_entry.value = value;
                    stale_entry.prev = None;
                    // next should already be None
                    stale_entry.next = None;
                    self.node_map.insert(key, stale_idx);
                    stale_idx
                }
            }
        } else {
            match self.node_map.entry(key.clone()) {
                Entry::Occupied(occ_entry_idx) => {
                    let entry_idx = *occ_entry_idx.get();
                    self.update_cache_entry(entry_idx, value);
                    return;
                }
                Entry::Vacant(vac_entry) => {
                    let new_entry = CacheSlabEntry {
                        key,
                        value,
                        prev: None,
                        next: None,
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
        }

        let (prev_opt, next_opt) = {
            let entry: &mut CacheSlabEntry<K, V> =
                unsafe { self.slab.get_unchecked_mut(entry_idx as usize) };
            let (prev_opt, next_opt) = (entry.prev, entry.next);
            entry.prev = None;
            entry.next = None;
            (prev_opt, next_opt)
        };

        match (prev_opt, next_opt) {
            (Some(prev), Some(next)) => {
                // node is in the middle of the list
                unsafe { self.slab.get_unchecked_mut(prev as usize).next = next_opt }
                unsafe { self.slab.get_unchecked_mut(next as usize).prev = prev_opt }
            }
            (Some(prev), None) => {
                // node is current the tail
                unsafe { self.slab.get_unchecked_mut(prev as usize).next = None }
                self.tail = prev_opt
            }
            (None, Some(next)) => {
                // node is current head
                unsafe { self.slab.get_unchecked_mut(next as usize).prev = None }
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

            let node: &mut CacheSlabEntry<K, V> =
                unsafe { self.slab.get_unchecked_mut(entry_idx as usize) };
            // if we get a cache hit on the key, then head should be Some
            let (prev, next) = (node.prev, node.next);
            debug_assert!(prev.is_none() && next.is_none());
        }

        let node: &mut CacheSlabEntry<K, V> =
            unsafe { self.slab.get_unchecked_mut(entry_idx as usize) };

        // if the list is non-empty, update the current head prev pointer to point to node
        // update node next to point to current head
        // if the list is empty set the node to be the head and tail
        if let Some(head_idx) = self.head {
            node.next = self.head;
            node.prev = None;
            unsafe { self.slab.get_unchecked_mut(head_idx as usize).prev = Some(entry_idx) }
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
    pub fn get(&mut self, key: &K) -> Option<V> {
        let Some(entry_idx_ref) = self.node_map.get(key) else {
            self.stats.miss();
            return None;
        };
        let entry_idx = *entry_idx_ref;

        #[cfg(debug_assertions)]
        {
            // if key is cache hit, head and tail must be Some
            debug_assert!(self.head.is_some() && self.tail.is_some());
        }

        self.stats.hit();

        let head_idx = self.head.unwrap();
        if head_idx != entry_idx {
            self.unlink_node(entry_idx);
            self.push_node_to_head(entry_idx);
        }

        let value = unsafe { self.slab.get_unchecked(entry_idx as usize).value.clone() };

        Some(value)
    }

    /// Returns a snapshot of cache hit, miss, and eviction counts.
    pub fn statistics(&self) -> CacheStats {
        self.stats.clone()
    }
}

#[cfg(test)]
mod single_threaded_test {

    use super::*;
    use std::collections::HashSet;

    #[test]
    fn constructs_empty() {
        let c: LruCache<i32, i32> = LruCache::with_capacity(NonZeroUsize::new(3).unwrap());
        assert_eq!(c.node_map.len(), 0);
        assert!(c.head.is_none());
        assert!(c.tail.is_none());
    }

    #[test]
    fn insert_get_contains() {
        let mut c = LruCache::with_capacity(NonZeroUsize::new(3).unwrap());
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
        let mut c = LruCache::with_capacity(NonZeroUsize::new(3).unwrap());
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
        let mut c = LruCache::with_capacity(NonZeroUsize::new(2).unwrap());
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
    fn eviction_order_at_capacity() {
        let mut c = LruCache::with_capacity(NonZeroUsize::new(2).unwrap());
        c.insert(1, "a");
        c.insert(2, "b");

        assert_eq!(c.get(&1), Some("a"));

        c.insert(3, "c");

        assert!(c.contains(&1));
        assert!(c.contains(&3));
        assert!(!c.contains(&2));
    }

    #[test]
    fn drain_empties_cache() {
        let mut c = LruCache::with_capacity(NonZeroUsize::new(3).unwrap());
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
        let mut c = LruCache::with_capacity(NonZeroUsize::new(5).unwrap());
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
        let mut c = LruCache::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert_eq!(c.len(), 3)
    }

    #[test]
    fn evict() {
        let mut c = LruCache::with_capacity(NonZeroUsize::new(3).unwrap());
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
        let mut c = LruCache::with_capacity(NonZeroUsize::new(3).unwrap());
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
}

#[cfg(test)]
mod shard_cache_test {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn constructs_empty() {
        let c: CacheShard<i32, i32> = CacheShard::with_capacity(NonZeroUsize::new(3).unwrap());
        assert_eq!(c.node_map.len(), 0);
        assert!(c.head.is_none());
        assert!(c.tail.is_none());
    }

    #[test]
    fn insert_get_contains() {
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(3).unwrap());
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
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(3).unwrap());
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
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(2).unwrap());
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
    fn eviction_order_at_capacity() {
        let mut c = LruCache::with_capacity(NonZeroUsize::new(2).unwrap());
        c.insert(1, "a");
        c.insert(2, "b");

        assert_eq!(c.get(&1), Some("a"));

        c.insert(3, "c");

        assert!(c.contains(&1));
        assert!(c.contains(&3));
        assert!(!c.contains(&2));
    }

    #[test]
    fn drain_empties_cache() {
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(3).unwrap());
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
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(5).unwrap());
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
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(3).unwrap());
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert_eq!(c.len(), 3)
    }

    #[test]
    fn evict() {
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(3).unwrap());
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
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(3).unwrap());
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
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(5).unwrap());
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
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(5).unwrap());
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
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(5).unwrap());
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
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(5).unwrap());
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
        let mut c = CacheShard::with_capacity(NonZeroUsize::new(10).unwrap());
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
}

#[cfg(test)]
mod indexed_shard_cache_test {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn constructs_empty() {
        let c: SlabShard<i32, i32> = SlabShard::with_capacity(NonZeroUsize::new(3).unwrap());
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
}
