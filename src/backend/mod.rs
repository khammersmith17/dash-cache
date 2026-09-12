use crate::entry::CacheEntry;
use std::hash::Hash;

/// The core methods required to satisfy shard backend storage. This allows for single threaded and
/// concurrent [`crate::core::SlabShard`] contexts to be able to use either plain Vec<Entry> or
/// SharedVecRef<Entry>.
pub(crate) trait SlabBackend<K: Hash + Eq, V: Clone> {
    fn push_entry(&mut self, entry: CacheEntry<K, V>);
    fn get_entry(&self, idx: usize) -> &CacheEntry<K, V>;
    fn get_entry_mut(&mut self, idx: usize) -> &mut CacheEntry<K, V>;
    fn swap_remove_entry(&mut self, idx: usize) -> CacheEntry<K, V>;
    fn len(&self) -> usize;
    fn clear(&mut self);
}
