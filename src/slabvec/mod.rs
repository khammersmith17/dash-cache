use crate::backend::SlabBackend;
use crate::entry::CacheEntry;
use std::hash::Hash;

#[derive(Debug)]
pub struct SlabVec<K: Hash + Eq, V: Clone>(Vec<CacheEntry<K, V>>);

impl<K: Hash + Eq, V: Clone> SlabVec<K, V> {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        SlabVec(Vec::with_capacity(cap))
    }

    fn push(&mut self, entry: CacheEntry<K, V>) {
        debug_assert!(self.0.len() < self.0.capacity());
        self.0.push(entry);
    }

    fn get(&self, idx: usize) -> &CacheEntry<K, V> {
        debug_assert!(idx < self.0.len());
        unsafe { self.0.get_unchecked(idx) }
    }

    fn get_mut(&mut self, idx: usize) -> &mut CacheEntry<K, V> {
        debug_assert!(idx < self.0.len());
        unsafe { self.0.get_unchecked_mut(idx) }
    }

    fn swap_remove(&mut self, idx: usize) -> CacheEntry<K, V> {
        debug_assert!(idx < self.0.len());
        self.0.swap_remove(idx)
    }
}

impl<K: Hash + Eq, V: Clone> SlabBackend<K, V> for SlabVec<K, V> {
    fn push_entry(&mut self, entry: CacheEntry<K, V>) {
        self.push(entry);
    }

    fn get_entry(&self, idx: usize) -> &CacheEntry<K, V> {
        self.get(idx)
    }

    fn get_entry_mut(&mut self, idx: usize) -> &mut CacheEntry<K, V> {
        self.get_mut(idx)
    }

    fn swap_remove_entry(&mut self, idx: usize) -> CacheEntry<K, V> {
        self.swap_remove(idx)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}
