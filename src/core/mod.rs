use core::ptr::NonNull;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::{Rc, Weak};
use thiserror::Error;

//TODO:
//implement thread safe methods for thread safe version
//implement cache stats and add reporting methods

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Key does not exist in cache")]
    KeyNotExist,
    #[error("Cache is internally corrupt, file a bug report")]
    CorruptedCacheError,
}

#[derive(Default, Debug)]
pub(crate) struct CacheStats {
    hits: usize,
    misses: usize,
    evictions: usize,
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
/// A use is defined as a write to the cache or a read from the cache.
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
    K: Hash + Eq + Clone,
    T: Clone,
{
    /// Only provided constructor
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
                    self.unlink_node(Rc::clone(&node_rc));
                    let node_weak_ref = Rc::downgrade(&node_rc);
                    self.push_node_to_head(node_weak_ref);
                }
            }
        }
        let mut node_ref: RefMut<CacheEntry<K, T>> = node_rc.as_ref().borrow_mut();
        node_ref.value = value;
        Ok(())
    }

    // empty is defined as both head and tail are None and the internal node_map is empty
    fn is_empty(&self) -> bool {
        self.head.is_none() && self.tail.is_none() && self.node_map.len() == 0
    }

    fn is_full(&self) -> bool {
        self.node_map.len() == self.cap
    }

    /// Insert value into the cache.
    /// If the key currently exists in the cache, the value is updated
    /// Key value pair is promoted to most recently used.
    pub fn insert(&mut self, key: K, value: T) {
        self.assert_invariants();
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

    #[cfg(debug_assertions)]
    fn assert_invariants(&self) {
        debug_assert!(
            (self.head.is_some() && self.tail.is_some())
                || (self.head.is_none() && self.tail.is_none())
        )
    }

    fn unlink_node(&mut self, node: Rc<RefCell<CacheEntry<K, T>>>) {
        self.assert_invariants();
        // if the list is empty, then no movement needs to happen
        if self.is_empty() {
            return;
        }

        // get weak reference to prev and next
        // pull mutable reference from RefCell
        // mutable reference to enfore poth list pointers are null after unlinking
        let mut node_ref = node.borrow_mut();
        let prev_weak = node_ref.prev.clone();
        let next_weak = node_ref.next.clone();

        // set prev next to node next
        if let Some(ref prev) = prev_weak {
            if let Some(prev_rc) = prev.upgrade() {
                let mut prev_ref = prev_rc.borrow_mut();
                prev_ref.next = next_weak.clone();
            }
        } else {
            self.head = next_weak.clone();
        }

        // set next prev to node prev
        if let Some(ref next) = next_weak {
            if let Some(next_rc) = next.upgrade() {
                let mut next_ref = next_rc.borrow_mut();
                next_ref.prev = prev_weak.clone();
            }
        } else {
            self.tail = prev_weak.clone();
        }

        node_ref.prev = None;
        node_ref.next = None;
    }

    fn push_node_to_head(&mut self, node: Weak<RefCell<CacheEntry<K, T>>>) {
        self.assert_invariants();
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

    fn pop_tail(&mut self) {
        self.assert_invariants();
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

    /// Get method without associated checks for key existance.
    /// Will panic when value does not exist in the map.
    pub fn get_unchecked(&mut self, key: &K) -> T {
        self.assert_invariants();
        let node_rc = self.node_map[key].clone();
        let node_ref: RefMut<CacheEntry<K, T>> = node_rc.as_ref().borrow_mut();
        let value = node_ref.value.clone();
        let res = value;
        drop(node_ref);

        if let Some(head_clone) = self.head.clone() {
            if let Some(head_rc) = head_clone.upgrade() {
                if !Rc::ptr_eq(&head_rc, &node_rc) {
                    self.unlink_node(Rc::clone(&node_rc));
                    let node_weak_ref = Rc::downgrade(&node_rc);
                    self.push_node_to_head(node_weak_ref);
                }
            }
        }
        res
    }

    /// Fetch value from the cache for associated key.
    /// Key value pair will then be promoted to most recently used.
    /// When they Key does not exist in the cache, None will be returned.
    pub fn get(&mut self, key: &K) -> Option<T> {
        self.assert_invariants();
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
                    self.unlink_node(Rc::clone(&node_rc));
                    let node_weak_ref = Rc::downgrade(&node_rc);
                    self.push_node_to_head(node_weak_ref);
                }
            }
        }

        res
    }
}

#[derive(Clone)]
struct ShardCacheEntry<K, T> {
    key: K,
    value: T,
    next: Option<NonNull<ShardCacheEntry<K, T>>>,
    prev: Option<NonNull<ShardCacheEntry<K, T>>>,
}

pub(crate) struct CacheShard<K, T> {
    cap: usize,
    node_map: HashMap<K, Box<ShardCacheEntry<K, T>>>,
    head: Option<NonNull<ShardCacheEntry<K, T>>>,
    tail: Option<NonNull<ShardCacheEntry<K, T>>>,
    stats: CacheStats,
}

impl<K, T> CacheShard<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    /// Only provided constructor
    /// Will initialize an LruCache with the requested capacity
    pub fn with_capacity(cap: usize) -> CacheShard<K, T> {
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

    // empty is defined as both head and tail are None and the internal node_map is empty
    #[allow(unused)]
    fn is_empty(&self) -> bool {
        self.head.is_none() && self.tail.is_none() && self.node_map.len() == 0
    }

    fn is_full(&self) -> bool {
        self.node_map.len() == self.cap
    }

    /// Insert value into the cache.
    /// If the key currently exists in the cache, the value is updated
    /// Key value pair is promoted to most recently used.
    pub fn insert(&mut self, key: K, value: T) {
        if self.contains(&key) {
            // ignoring error here as path only taken when the key exists
            let _ = self.update(&key, value);
        } else {
            let node = ShardCacheEntry {
                key: key.clone(),
                value,
                prev: None,
                next: None,
            };

            let mut boxed_node = Box::new(node);
            self.node_map.insert(key, boxed_node.clone());
            if self.is_full() {
                self.pop_tail();
            }

            let ptr = NonNull::from(boxed_node.as_mut());
            self.push_node_to_head(ptr);
        }
    }

    /// Empty the cache.
    /// After this call, cache will be empty.
    pub fn drain(&mut self) {
        self.head = None;
        self.tail = None;
        self.node_map.clear();
    }

    #[inline]
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

        if let Some(mut prev) = prev_opt {
            unsafe { prev.as_mut().next = next_opt }
        } else {
            self.head = next_opt
        }

        if let Some(mut next) = next_opt {
            unsafe { next.as_mut().prev = prev_opt }
        } else {
            self.tail = prev_opt
        }

        unsafe {
            node.as_mut().prev = None;
            node.as_mut().next = None;
        }

        #[cfg(debug_assertions)]
        {
            // validating that node is unlinked after op
            let (prev, next) = unsafe {
                let node_ref = node.as_ref();
                (node_ref.prev, node_ref.next)
            };

            debug_assert!(prev.is_none() && next.is_none());
        }
    }

    #[inline]
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
        //      update node next to point to current head
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

    #[inline]
    fn pop_tail(&mut self) {
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

        let key = unsafe { tail_ptr.as_ref().key.clone() };
        self.node_map.remove(&key);
    }

    /// Get method without associated checks for key existance.
    /// Will panic when value does not exist in the map.
    pub fn get_unchecked(&mut self, key: &K) -> T {
        #[cfg(debug_assertions)]
        {
            // if key is cache hit, head and tail must be Some
            debug_assert!(self.head.is_some() && self.tail.is_some());
        }

        let entry_ref = &self.node_map[key];

        let head_ptr = self.head.unwrap();
        let entry_ptr = NonNull::from(entry_ref.as_ref());

        if !head_ptr.eq(&entry_ptr) {
            self.unlink_node(entry_ptr);
            self.push_node_to_head(entry_ptr);
        }

        let entry_ref = &self.node_map[key];
        let value = entry_ref.value.clone();

        value
    }

    /// Fetch value from the cache for associated key.
    /// Key value pair will then be promoted to most recently used.
    /// When they Key does not exist in the cache, None will be returned.
    pub fn get(&mut self, key: &K) -> Option<T> {
        #[cfg(debug_assertions)]
        {
            // if key is cache hit, head and tail must be Some
            debug_assert!(self.head.is_some() && self.tail.is_some());
        }

        let entry_ptr = {
            let Some(entry_box) = self.node_map.get(key) else {
                self.stats.miss();
                return None;
            };
            let ptr = NonNull::from(entry_box.as_ref());
            ptr
        };
        self.stats.hit();

        let head_ptr = self.head.unwrap();

        if !head_ptr.eq(&entry_ptr) {
            self.unlink_node(entry_ptr);
            self.push_node_to_head(entry_ptr);
        }

        let value = unsafe { entry_ptr.as_ref().value.clone() };

        Some(value)
    }
}
