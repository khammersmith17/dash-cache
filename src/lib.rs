use ahash::AHasher;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug)]
struct ListNode<K, T> {
    value: T,
    key: K,
    prev: Option<Weak<RefCell<ListNode<K, T>>>>,
    next: Option<Weak<RefCell<ListNode<K, T>>>>,
}

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

    fn is_empty(&self) -> bool {
        self.node_map.len() == 0
    }

    fn is_full(&self) -> bool {
        self.node_map.len() == self.cap
    }

    pub fn insert(&mut self, key: K, value: T) {
        if self.node_map.contains_key(&key) {
            // swap node up to the head
            let node_rc = self.node_map.get(&key).unwrap().clone();
            let mut node_ref = node_rc.as_ref().borrow_mut();
            node_ref.value = value;
            drop(node_ref);

            self.unlink_node(Rc::clone(&node_rc));
            self.push_node_to_head(Rc::downgrade(&node_rc));
            return;
        }

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

    fn unlink_node(&mut self, node: Rc<RefCell<ListNode<K, T>>>) {
        let node_ref = node.borrow();
        let prev_weak = node_ref.prev.clone();
        let next_weak = node_ref.next.clone();

        if let Some(ref prev) = prev_weak {
            if let Some(prev_rc) = prev.upgrade() {
                let mut prev_ref = prev_rc.borrow_mut();
                prev_ref.next = next_weak.clone();
            }
        }

        if let Some(ref next) = next_weak {
            if let Some(next_rc) = next.upgrade() {
                let mut next_ref = next_rc.borrow_mut();
                next_ref.prev = prev_weak.clone();
            }
        }

        if prev_weak.is_none() {
            self.head = next_weak.clone();
        }

        if next_weak.is_none() {
            self.tail = prev_weak.clone();
        }
    }

    fn push_node_to_head(&mut self, node: Weak<RefCell<ListNode<K, T>>>) {
        if self.is_empty() {
            self.head = Some(node.clone());
            self.tail = Some(node);
            return;
        }

        let old_head = self.head.clone();

        if let Some(curr_head) = old_head {
            if let Some(curr_head_rc) = curr_head.upgrade() {
                let mut curr_head_mut = curr_head_rc.as_ref().borrow_mut();
                curr_head_mut.prev = Some(node.clone());
            }
        }

        if let Some(new_head_rc) = node.upgrade() {
            let mut new_head_mut = new_head_rc.as_ref().borrow_mut();
            new_head_mut.prev = None;
            new_head_mut.next = self.head.clone();
        }

        self.head = Some(node)
    }

    fn pop_tail(&mut self) {
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

    pub fn get(&mut self, key: &K) -> Option<T> {
        if self.head.is_none() {
            return None;
        }
        let node_rc = self.node_map.get(&key)?.clone();
        let node_ref: RefMut<ListNode<K, T>> = node_rc.as_ref().borrow_mut();

        let head_clone = self.head.clone()?;
        let head_rc = head_clone.upgrade()?;
        let value = node_ref.value.clone();
        let res = Some(value);
        drop(node_ref);

        if Rc::ptr_eq(&head_rc, &node_rc) {
            return res;
        };
        drop(head_rc);

        self.unlink_node(Rc::clone(&node_rc));
        let node_weak_ref = Rc::downgrade(&node_rc);
        self.push_node_to_head(node_weak_ref);

        res
    }
}
#[allow(unused)]
struct ThreadSafeLruCache<K, T> {
    handle: Arc<Mutex<LruCache<K, T>>>,
}

#[allow(unused)]
impl<K, T> ThreadSafeLruCache<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    fn with_capacity(cap: usize) -> ThreadSafeLruCache<K, T> {
        let cache: LruCache<K, T> = LruCache::with_capacity(cap);
        let cache_mutex = Mutex::new(cache);
        let handle = Arc::new(cache_mutex);
        ThreadSafeLruCache { handle }
    }

    async fn insert(&mut self, key: K, value: T) {
        let mut guard = self.handle.lock().await;
        guard.insert(key, value)
    }

    async fn get(&mut self, key: &K) -> Option<T> {
        let mut guard = self.handle.lock().await;
        let value = guard.get(key);
        drop(guard);
        value
    }

    async fn contains(&self, key: &K) -> bool {
        let guard = self.handle.lock().await;
        guard.contains(key)
    }
}

pub struct RwLruCache<K, T> {
    inner: InnerCacheShards<K, T>,
}

impl<K, T> RwLruCache<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    pub fn new(cap: u64) -> RwLruCache<K, T> {
        let inner = InnerCacheShards::new(cap);

        RwLruCache { inner }
    }

    pub async fn get(&mut self, key: &K) -> Option<T> {
        self.inner.get(key).await
    }

    pub async fn insert(&mut self, key: K, value: T) {
        self.inner.insert(key, value).await;
    }

    pub async fn contains(&self, key: &K) -> bool {
        self.inner.contains(key).await
    }
}

struct InnerCacheShards<K, T> {
    cache_shards: Box<[ThreadSafeLruCache<K, T>]>,
    num_shards: u64,
}

impl<K, T> InnerCacheShards<K, T>
where
    K: Hash + Eq + Clone,
    T: Clone,
{
    fn new(cap: u64) -> InnerCacheShards<K, T> {
        let num_shards = num_cpus::get();
        let mut shards_vec: Vec<ThreadSafeLruCache<K, T>> = Vec::with_capacity(num_shards);

        let shard_size = (cap as f32 / num_shards as f32).ceil();

        for _ in 0..num_shards {
            let shard: ThreadSafeLruCache<K, T> =
                ThreadSafeLruCache::with_capacity(shard_size as usize);
            shards_vec.push(shard);
        }

        let cache_shards = shards_vec.into_boxed_slice();

        InnerCacheShards {
            cache_shards,
            num_shards: num_shards as u64,
        }
    }

    pub async fn get(&mut self, key: &K) -> Option<T> {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        let shard_key = hash_value % self.num_shards;

        let shard_cache = &mut self.cache_shards[shard_key as usize];
        let value = shard_cache.get(key).await;
        value
    }

    pub async fn insert(&mut self, key: K, value: T) {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        let shard_key = hash_value % self.num_shards;
        let shard_cache = &mut self.cache_shards[shard_key as usize];
        shard_cache.insert(key, value).await;
    }

    pub async fn contains(&self, key: &K) -> bool {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        let shard_key = hash_value % self.num_shards;
        let shard_cache = &self.cache_shards[shard_key as usize];
        shard_cache.contains(key).await
    }
}

#[cfg(test)]
mod tests {}
