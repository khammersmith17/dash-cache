use core::ptr::NonNull;
use std::cell::{Ref, RefCell, RefMut};
//use std::collections::HashMap;
use ahash::{HashMap, HashMapExt};
use std::hash::Hash;
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
    hits: usize,
    misses: usize,
    evictions: usize,
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

/// LruCache is desgined for single threaded access or to be used in a non async context.
/// Initializing with capacity is required.
/// When the cache is full, the least recently used item will be evicted.
/// A use is defined as a write to the cache or a read from the cache. This type is fully safe.
/// Performance in debug mode may not be optimal, due to the internal variant assertions to ensure
/// correct behavior. All data accesses return copies due to internal borrowing mechanics.
/// Hopefully this can be improved in later versions.
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
    /// This is the only provided constructor.
    /// Will initialize an LruCache with the requested capacity
    pub fn with_capacity(cap: usize) -> LruCache<K, T> {
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

    /// Get a refernce to the key that is currently the most recently used entry in the cache, which is
    /// also the head.
    pub fn head(&self) -> Option<K> {
        match self.head {
            Some(ref weak_head) => {
                let Some(head) = weak_head.upgrade() else {
                    return None;
                };

                Some(head.borrow().key.clone())
            }
            None => None,
        }
    }

    /// Pop the least recently used entry in the cache.
    pub fn pop(&mut self, key: &K) -> Option<T> {
        let Some(cache_entry) = self.node_map.remove(key) else {
            return None;
        };

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

    /// Cache hits, misses, and evictions are stored internally. This method exposes a snapshot of
    /// the current cache locality performance.
    pub fn statistics(&self) -> CacheStats {
        self.stats.clone()
    }

    /// Returns whether or not a key exists in the cache.
    /// This method is not defined as a use, thus accessing this key will not promote the item.
    pub fn contains(&self, key: &K) -> bool {
        self.node_map.contains_key(key)
    }

    /// Update an item that exists in the cache.
    /// If the requested key does not exist in the cache, a CacheError will be returned.
    /// On success, a unit type value is returned.
    /// When the value of a key value pair is updated, this key value pair is promoted to most
    /// recently used. There is no get_mut method on this type due to borrowing semantic
    /// limitations, use this method any time you would like to mutate the value stored with a
    /// given key.
    pub fn update(&mut self, key: &K, value: T) -> Result<(), CacheError> {
        #[cfg(debug_assertions)]
        {
            self.assert_invariants();
        }
        let node_rc = {
            let Some(rc) = self.node_map.get(&key) else {
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

    /// Returns whether the cache is empty or not. Empty is defined as both head and tail of the internal linked list are
    /// None and the internal entry table is empty. The valid variants are that both are true or
    /// neither is true.
    pub fn is_empty(&self) -> bool {
        self.head.is_none() && self.tail.is_none() && self.len() == 0
    }

    /// Returns whether or not the cache is at capacity. When the cache is at capacity, the next
    /// insert into the cache will lead to eviction.
    pub fn is_full(&self) -> bool {
        self.len() == self.cap
    }

    /// Insert value into the cache.
    /// If the key currently exists in the cache, the associated value is updated.
    /// Key value pair is either promoted to most recently used if it exists, or inserted as most
    /// recently used.
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
        let curr_tail_ref: Ref<CacheEntry<K, T>> = curr_tail_rc.as_ref().borrow();

        let key_to_pop = curr_tail_ref.key.clone();
        if let Some(new_tail) = curr_tail_ref.prev.clone() {
            let new_tail_rc = new_tail.upgrade().unwrap();
            let mut new_tail_ref = new_tail_rc.borrow_mut();
            new_tail_ref.next = None;
            drop(new_tail_ref);

            self.tail = Some(new_tail)
        } else {
            self.head = None;
            self.tail = None;
        }

        self.node_map.remove(&key_to_pop);
    }

    /// Fetch value from the cache for associated key.
    /// Key value pair will then be promoted to most recently used.
    /// When they Key does not exist in the cache, None will be returned.
    pub fn get(&mut self, key: &K) -> Option<T> {
        #[cfg(debug_assertions)]
        {
            self.assert_invariants();
        }
        if self.head.is_none() {
            return None;
        }
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

// internal cache table entry for safely sharing across threads
// leveraging NonNull safe pointers rather than
#[derive(Clone)]
struct ShardCacheEntry<K, T> {
    key: K,
    value: T,
    next: Option<NonNull<ShardCacheEntry<K, T>>>,
    prev: Option<NonNull<ShardCacheEntry<K, T>>>,
}

/// This is unsafe implementation of an LRU Cache, and it the cache type used in the shards of the
/// 'DashCache' type. Debug performance is non-optimal. Given the unsafe nature, invariant
/// assertions are run on every cache operation to maintain correct state. In release builds, the
/// performance is quite good, consist with other crates such as lru.
/// Hopefully this can be improved in later versions.
pub struct CacheShard<K, T> {
    cap: usize,
    node_map: HashMap<K, Box<ShardCacheEntry<K, T>>>,
    head: Option<NonNull<ShardCacheEntry<K, T>>>,
    tail: Option<NonNull<ShardCacheEntry<K, T>>>,
    stats: CacheStats,
}

unsafe impl<K, T> Send for CacheShard<K, T>
where
    K: Send + 'static,
    T: Send + 'static,
{
}
unsafe impl<K, T> Sync for CacheShard<K, T>
where
    K: Sync + 'static,
    T: Sync + 'static,
{
}

impl<K, T> CacheShard<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    /// This is the Only provided constructor.
    /// Will initialize an LruCache with the requested capacity
    pub fn with_capacity(cap: usize) -> CacheShard<K, T> {
        if cap == 0 {
            panic!("capacity must be > 0");
        }

        let node_map: HashMap<K, Box<ShardCacheEntry<K, T>>> = HashMap::with_capacity(cap);

        CacheShard {
            cap,
            node_map,
            head: None,
            tail: None,
            stats: CacheStats::default(),
        }
    }

    /// Returns whether or not a key exists in the cache.
    /// This method is not defined as a use, thus accessing this key will not promote the item.
    pub fn contains(&self, key: &K) -> bool {
        self.node_map.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.node_map.len()
    }

    /// Update an item that exists in the cache.
    /// If the requested key does not exist in the cache, a CacheError will be returned.
    /// On success, a unit type value is returned.
    /// When the value of a key value pair is updated, this key value pair is promoted to most
    /// recently used. There is no get_mut method on this type due to borrowing semantic
    /// limitations, use this method any time you would like to mutate the value stored with a
    /// given key.
    pub fn update(&mut self, key: &K, value: T) -> Result<(), CacheError> {
        debug_assert!(
            (self.head.is_some() && self.tail.is_some())
                || (self.head.is_none() && self.tail.is_none())
        );
        let mut entry_ptr = {
            let Some(cache_entry) = self.node_map.get(key) else {
                self.stats.miss();
                return Err(CacheError::KeyNotExist);
            };
            let ptr = NonNull::from(cache_entry.as_ref());
            ptr
        };
        self.stats.hit();

        let Some(curr_head) = self.head else {
            return Err(CacheError::CorruptedCacheError);
        };

        // only promote node when it is not current head
        if !curr_head.eq(&entry_ptr) {
            self.unlink_node(entry_ptr);
            self.push_node_to_head(entry_ptr);
        }

        // at this point we have validated that the pointer is non null
        // and a mutable update is safe
        unsafe { entry_ptr.as_mut().value = value };

        Ok(())
    }

    /// Pop an entry from the cache, forcing an eviction. Returns the value associated with the key
    /// is the key is currently in the cache, None otherwise.
    pub fn pop(&mut self, key: &K) -> Option<T> {
        let Some(mut cache_entry) = self.node_map.remove(key) else {
            return None;
        };

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

    /// Returns whether or not the cache is currently empty. Empty is defined as both head and tail are None and the internal
    /// entry table is empty.
    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.head.is_none() && self.tail.is_none() && self.len() == 0
    }

    /// Returns whether the cache is full. When the cache is full, the next insert will force an
    /// eviction.
    pub fn is_full(&self) -> bool {
        self.node_map.len() == self.cap
    }

    /// Insert value into the cache.
    /// If the key currently exists in the cache, the value is updated
    /// Key value pair is promoted to most recently used.
    pub fn insert(&mut self, key: K, value: T) {
        if self.contains(&key) {
            let _ = self.update(&key, value);
            return;
        }

        // if eviction is needed, reuse already allocated entry
        let ptr = if self.is_full() {
            let mut stale_entry = self.pop_tail();
            stale_entry.key = key.clone();
            stale_entry.value = value;
            stale_entry.prev = None;
            // next should already be None
            stale_entry.next = None;
            let ptr = NonNull::from(stale_entry.as_mut());
            self.node_map.insert(key, stale_entry);
            ptr
        } else {
            // if there is still capacity available, allocate a new entry
            let node = ShardCacheEntry {
                key: key.clone(),
                value,
                prev: None,
                next: None,
            };
            let mut boxed_node = Box::new(node);
            let ptr = NonNull::from(boxed_node.as_mut());
            self.node_map.insert(key, boxed_node);
            ptr
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
            (curr.prev.clone(), curr.next.clone())
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

        unsafe { self.node_map.remove(&tail_ptr.as_ref().key).unwrap() }
    }

    /// Fetch value from the cache for associated key.
    /// Key value pair will then be promoted to most recently used.
    /// When they Key does not exist in the cache, None will be returned.
    pub fn get(&mut self, key: &K) -> Option<T> {
        let entry_ptr = {
            let Some(entry_box) = self.node_map.get(key) else {
                self.stats.miss();
                return None;
            };
            let ptr = NonNull::from(entry_box.as_ref());
            ptr
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

    /// Statistics are kept internally detailing the number of cache hits, misses, and evictions
    /// for promoting operations.
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
        let c: LruCache<i32, i32> = LruCache::with_capacity(3);
        assert_eq!(c.node_map.len(), 0);
        assert!(c.head.is_none());
        assert!(c.tail.is_none());
    }

    #[test]
    fn insert_get_contains() {
        let mut c = LruCache::with_capacity(3);
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
        let mut c = LruCache::with_capacity(3);
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
        let mut c = LruCache::with_capacity(2);
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
        let mut c = LruCache::with_capacity(2);
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
        let mut c = LruCache::with_capacity(3);
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
        let mut c = LruCache::with_capacity(5);
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
        let mut c = LruCache::with_capacity(3);
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert_eq!(c.len(), 3)
    }

    #[test]
    fn pop() {
        let mut c = LruCache::with_capacity(3);
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert_eq!(c.pop(&"a"), Some(1));
        assert_eq!(c.pop(&"c"), Some(3));
        assert_eq!(c.pop(&"a"), None);
        assert_eq!(c.pop(&"b"), Some(2));
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn full_and_empty() {
        let mut c = LruCache::with_capacity(3);
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert!(c.is_full());
        assert_eq!(c.pop(&"a"), Some(1));
        assert_eq!(c.pop(&"c"), Some(3));
        assert_eq!(c.pop(&"a"), None);
        assert_eq!(c.pop(&"b"), Some(2));
        assert!(c.is_empty());
    }
}

#[cfg(test)]
mod shard_cache_test {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn constructs_empty() {
        let c: CacheShard<i32, i32> = CacheShard::with_capacity(3);
        assert_eq!(c.node_map.len(), 0);
        assert!(c.head.is_none());
        assert!(c.tail.is_none());
    }

    #[test]
    fn insert_get_contains() {
        let mut c = CacheShard::with_capacity(3);
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
        let mut c = CacheShard::with_capacity(3);
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
        let mut c = CacheShard::with_capacity(2);
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
        let mut c = LruCache::with_capacity(2);
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
        let mut c = CacheShard::with_capacity(3);
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
        let mut c = CacheShard::with_capacity(5);
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
        let mut c = CacheShard::with_capacity(3);
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert_eq!(c.len(), 3)
    }

    #[test]
    fn pop() {
        let mut c = CacheShard::with_capacity(3);
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert_eq!(c.pop(&"a"), Some(1));
        assert_eq!(c.pop(&"c"), Some(3));
        assert_eq!(c.pop(&"a"), None);
        assert_eq!(c.pop(&"b"), Some(2));
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn full_and_empty() {
        let mut c = CacheShard::with_capacity(3);
        c.insert("a", 1);
        c.insert("b", 2);
        c.insert("c", 3);
        assert!(c.is_full());
        assert_eq!(c.pop(&"a"), Some(1));
        assert_eq!(c.pop(&"c"), Some(3));
        assert_eq!(c.pop(&"a"), None);
        assert_eq!(c.pop(&"b"), Some(2));
        assert!(c.is_empty());
    }
}
