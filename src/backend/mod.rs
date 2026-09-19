use crate::entry::CacheEntry;
use std::hash::Hash;

/// The core methods required to satisfy shard backend storage. This allows for single threaded and
/// concurrent [`crate::core::SlabShard`] contexts to be able to use either plain Vec<Entry> or
/// SharedVecRef<Entry>.
pub(crate) trait SlabBackend<K: Hash + Eq, V: Clone> {
    // Push an entry into the slab.
    fn push_entry(&mut self, entry: CacheEntry<K, V>);
    // Get a reference to an entry in the slab.
    fn get_entry(&self, idx: usize) -> &CacheEntry<K, V>;
    // Get an exclusive reference to an entry in the slab.
    fn get_entry_mut(&mut self, idx: usize) -> &mut CacheEntry<K, V>;
    // Remove and entry by swapping it to the end of the slab and popping it off.
    fn swap_remove_entry(&mut self, idx: usize) -> CacheEntry<K, V>;
    // Get entry size.
    fn len(&self) -> usize;
    // Clear the entry.
    fn clear(&mut self);
}
