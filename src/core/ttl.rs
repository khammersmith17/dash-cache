use std::cell::RefCell;
use std::fmt;
use std::hash::Hash;
use std::ptr::NonNull;
use std::rc::Weak;
use std::time::Instant;

/*
* TTL implementation
*   1. Same api surface
*       with additional TTL sweep method
*       on sweep, walk from the back and evict nodes.
*   2. On get, ttl is evaluate before pushing item to head and returning the value to the user
*       Removing from current position in the map happens regardless
*
*   expiration time is determined when touched.
*   ttl value is set on insert and update only. Get does not update ttl.
*   Users can set a default ttl on construction, and insert/update with different ttl values
*   ttl values are defined in milliseconds
* */

#[derive(Debug)]
pub(crate) struct CacheEntryTtl<K: Hash + Eq + fmt::Debug, T: Clone + fmt::Debug> {
    pub(crate) value: T,
    pub(crate) key: K,
    pub(crate) prev: Option<Weak<RefCell<CacheEntryTtl<K, T>>>>,
    pub(crate) next: Option<Weak<RefCell<CacheEntryTtl<K, T>>>>,
    pub(crate) expiration: Instant,
}

impl<K: Hash + Eq + fmt::Debug, T: Clone + fmt::Debug> CacheEntryTtl<K, T> {
    pub(crate) fn is_live(&self) -> bool {
        Instant::now() >= self.expiration
    }
}

// Internal linked list node for CacheShard. Uses NonNull raw pointers for prev/next to avoid
// the reference-counting overhead of Rc, at the cost of requiring manual safety invariants.
#[derive(Clone, Debug)]
pub(crate) struct ShardCacheEntryTtl<K: Hash + Eq + fmt::Debug, T: Clone + fmt::Debug> {
    pub(crate) key: K,
    pub(crate) value: T,
    pub(crate) next: Option<NonNull<ShardCacheEntryTtl<K, T>>>,
    pub(crate) prev: Option<NonNull<ShardCacheEntryTtl<K, T>>>,
    pub(crate) expiration: Instant,
}

impl<K: Hash + Eq + fmt::Debug, T: Clone + fmt::Debug> ShardCacheEntryTtl<K, T> {
    pub(crate) fn is_live(&self) -> bool {
        Instant::now() >= self.expiration
    }
}

#[derive(Debug)]
pub(crate) struct CacheSlabEntryTtl<K: Hash + Eq + fmt::Debug, V: Clone + fmt::Debug> {
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) prev: Option<u32>,
    pub(crate) next: Option<u32>,
    pub(crate) expiration: Instant,
}

impl<K: Hash + Eq + fmt::Debug, T: Clone + fmt::Debug> CacheSlabEntryTtl<K, T> {
    pub(crate) fn is_live(&self) -> bool {
        Instant::now() >= self.expiration
    }
}
