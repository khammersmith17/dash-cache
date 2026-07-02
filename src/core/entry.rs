use std::cell::RefCell;
use std::fmt;
use std::hash::Hash;
use std::ptr::NonNull;
use std::rc::Weak;

#[derive(Debug)]
pub(crate) struct CacheEntry<K: Hash + Eq + fmt::Debug, T: Clone + fmt::Debug> {
    pub(crate) value: T,
    pub(crate) key: K,
    pub(crate) prev: Option<Weak<RefCell<CacheEntry<K, T>>>>,
    pub(crate) next: Option<Weak<RefCell<CacheEntry<K, T>>>>,
}

// Internal linked list node for CacheShard. Uses NonNull raw pointers for prev/next to avoid
// the reference-counting overhead of Rc, at the cost of requiring manual safety invariants.
#[derive(Clone, Debug)]
pub(crate) struct ShardCacheEntry<K: Hash + Eq + fmt::Debug, T: Clone + fmt::Debug> {
    pub(crate) key: K,
    pub(crate) value: T,
    pub(crate) next: Option<NonNull<ShardCacheEntry<K, T>>>,
    pub(crate) prev: Option<NonNull<ShardCacheEntry<K, T>>>,
}

#[derive(Debug)]
pub(crate) struct CacheSlabEntry<K: Hash + Eq + fmt::Debug, V: Clone + fmt::Debug> {
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) prev: Option<u32>,
    pub(crate) next: Option<u32>,
}
