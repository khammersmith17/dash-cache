use ahash::AHasher;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::num::NonZeroUsize;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Key does not exist in cache")]
    KeyNotExist,
}

#[derive(Debug)]
struct ListNode<K, T> {
    value: T,
    key: K,
    prev: Option<Weak<RefCell<ListNode<K, T>>>>,
    next: Option<Weak<RefCell<ListNode<K, T>>>>,
}

/// LruCache is desgined for single threaded access or to be used in a non async context
/// Initialize with capacity
#[derive(Debug)]
pub struct LruCache<K, T> {
    cap: usize,
    node_map: HashMap<K, Rc<RefCell<ListNode<K, T>>>>,
    head: Option<Weak<RefCell<ListNode<K, T>>>>,
    tail: Option<Weak<RefCell<ListNode<K, T>>>>,
}

impl<K, T> LruCache<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    pub fn with_capacity(cap: usize) -> LruCache<K, T> {
        let node_map: HashMap<K, Rc<RefCell<ListNode<K, T>>>> = HashMap::with_capacity(cap);
        LruCache {
            cap,
            node_map,
            head: None,
            tail: None,
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        self.node_map.contains_key(key)
    }

    pub fn update(&mut self, key: &K, value: T) -> Result<(), CacheError> {
        self.assert_invariants();
        let node_rc = {
            let Some(rc) = self.node_map.get(&key) else {
                return Err(CacheError::KeyNotExist);
            };
            rc.clone()
        };

        if let Some(head_clone) = self.head.clone() {
            if let Some(head_rc) = head_clone.upgrade() {
                if !Rc::ptr_eq(&head_rc, &node_rc) {
                    self.unlink_node(Rc::clone(&node_rc));
                    let node_weak_ref = Rc::downgrade(&node_rc);
                    self.push_node_to_head(node_weak_ref);
                }
            }
        }
        let mut node_ref: RefMut<ListNode<K, T>> = node_rc.as_ref().borrow_mut();
        node_ref.value = value;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.head.is_none() && self.tail.is_none() && self.node_map.len() == 0
    }

    fn is_full(&self) -> bool {
        self.node_map.len() == self.cap
    }

    pub fn insert(&mut self, key: K, value: T) {
        self.assert_invariants();
        match self.node_map.contains_key(&key) {
            true => {
                let _ = self.update(&key, value);
            }
            false => {
                let new_node = Rc::new(RefCell::new(ListNode {
                    key: key.clone(),
                    value,
                    prev: None,
                    next: None,
                }));

                if self.is_full() {
                    self.pop_tail()
                }

                self.push_node_to_head(Rc::downgrade(&new_node));
                self.node_map.insert(key, new_node);
            }
        }
    }

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

    fn unlink_node(&mut self, node: Rc<RefCell<ListNode<K, T>>>) {
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

    fn push_node_to_head(&mut self, node: Weak<RefCell<ListNode<K, T>>>) {
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
        if self.tail.is_none() {
            return;
        }

        let curr_tail_weak_ref = self.tail.clone().unwrap();
        let curr_tail_rc = curr_tail_weak_ref.upgrade().unwrap();
        let curr_tail_ref: Ref<ListNode<K, T>> = curr_tail_rc.as_ref().borrow();

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

    pub fn get_unchecked(&mut self, key: &K) -> T {
        self.assert_invariants();
        let node_rc = self.node_map[key].clone();
        let node_ref: RefMut<ListNode<K, T>> = node_rc.as_ref().borrow_mut();
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

    pub fn get(&mut self, key: &K) -> Option<T> {
        self.assert_invariants();
        if self.head.is_none() {
            return None;
        }
        let node_rc = self.node_map.get(key)?.clone();
        let node_ref: RefMut<ListNode<K, T>> = node_rc.as_ref().borrow_mut();
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
#[allow(unused)]
struct LockedCache<K, T> {
    handle: RwLock<LruCache<K, T>>,
}

#[allow(unused)]
impl<K, T> LockedCache<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    fn with_capacity(cap: usize) -> LockedCache<K, T> {
        let cache: LruCache<K, T> = LruCache::with_capacity(cap);
        let handle = RwLock::new(cache);
        LockedCache { handle }
    }

    async fn insert(&self, key: K, value: T) {
        let mut guard = self.handle.write().await;
        guard.insert(key, value)
    }

    async fn drain(&self) {
        let mut guard = self.handle.write().await;
        guard.drain();
    }

    async fn get(&self, key: &K) -> Option<T> {
        let mut guard = self.handle.write().await;
        let value = guard.get(key);
        drop(guard);
        value
    }

    async fn update(&self, key: &K, value: T) -> Result<(), CacheError> {
        let mut guard = self.handle.write().await;
        guard.update(key, value)?;
        drop(guard);
        Ok(())
    }

    async fn contains(&self, key: &K) -> bool {
        let guard = self.handle.read().await;
        guard.contains(key)
    }

    async fn get_unchecked(&self, key: &K) -> T {
        let mut guard = self.handle.write().await;
        guard.get_unchecked(key)
    }
}

pub struct DashCache<K, T> {
    inner: Arc<InnerCacheShards<K, T>>,
}

impl<K, T> DashCache<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    pub fn new(cap: u64) -> DashCache<K, T> {
        let inner = Arc::new(InnerCacheShards::new(cap));

        DashCache { inner }
    }

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

    pub async fn get(&self, key: &K) -> Option<T> {
        self.inner.get(key).await
    }

    pub async fn get_unchecked(&self, key: &K) -> T {
        self.inner.get_unchecked(key).await
    }

    pub async fn insert(&self, key: K, value: T) {
        self.inner.insert(key, value).await;
    }

    pub async fn contains(&self, key: &K) -> bool {
        self.inner.contains(key).await
    }

    pub async fn update(&self, key: &K, value: T) -> Result<(), CacheError> {
        self.inner.update(key, value).await?;
        Ok(())
    }

    pub async fn drain(&self) {
        self.inner.drain().await;
    }

    pub async fn num_shards(&self) -> usize {
        usize::from(self.inner.num_shards)
    }
}

struct InnerCacheShards<K, T> {
    cache_shards: Box<[LockedCache<K, T>]>,
    num_shards: NonZeroUsize,
}

impl<K, T> InnerCacheShards<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    fn new(cap: u64) -> InnerCacheShards<K, T> {
        if cap == 0 {
            panic!("Capacity must be greater than zero");
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
        let mut shards_vec: Vec<LockedCache<K, T>> = Vec::with_capacity(shard_capacity);

        for _ in 0..num_shards {
            let shard: LockedCache<K, T> = LockedCache::with_capacity(shard_capacity);
            shards_vec.push(shard);
        }
        let cache_shards = shards_vec.into_boxed_slice();
        let num_shards = unsafe { NonZeroUsize::new_unchecked(num_shards) };

        InnerCacheShards {
            cache_shards,
            num_shards,
        }
    }

    async fn get(&self, key: &K) -> Option<T> {
        let shard_key = self.compute_shard(key);
        let shard_cache = &self.cache_shards[shard_key];
        let value = shard_cache.get(key).await;
        value
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

    async fn get_unchecked(&self, key: &K) -> T {
        let shard_key = self.compute_shard(&key);
        let shard_cache = &self.cache_shards[shard_key];
        shard_cache.get_unchecked(key).await
    }
}

#[cfg(test)]
mod tests {}
