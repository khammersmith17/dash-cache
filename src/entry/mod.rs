use crate::util;
use std::hash::Hash;

#[derive(Debug)]
pub(crate) struct CacheEntry<K: Hash + Eq, V: Clone> {
    pub(crate) key: K,
    pub(crate) value: V,
    list_neighbors: u64,
    pub(crate) expires: u64,
}

impl<K: Hash + Eq, V: Clone> CacheEntry<K, V> {
    pub(crate) fn new(key: K, value: V, expires: u64) -> CacheEntry<K, V> {
        CacheEntry {
            key,
            value,
            list_neighbors: util::pointer_idx::null_neighbors(),
            expires,
        }
    }
    #[inline]
    pub(crate) fn is_live(&self) -> bool {
        !util::is_expired(self.expires)
    }

    // All list pointer bit math.

    #[inline]
    pub(crate) fn clear_neighbors(&mut self) {
        self.list_neighbors = util::pointer_idx::null_neighbors();
    }

    #[inline]
    pub(crate) fn set_next(&mut self, next: Option<u32>) {
        self.list_neighbors = util::pointer_idx::set_next_pointer(self.list_neighbors, next);
    }

    #[inline]
    pub(crate) fn set_prev(&mut self, prev: Option<u32>) {
        self.list_neighbors = util::pointer_idx::set_prev_pointer(self.list_neighbors, prev);
    }

    #[inline]
    pub(crate) fn prev(&self) -> Option<u32> {
        util::pointer_idx::get_prev_pointer(self.list_neighbors)
    }

    #[inline]
    pub(crate) fn next(&self) -> Option<u32> {
        util::pointer_idx::get_next_pointer(self.list_neighbors)
    }
}
