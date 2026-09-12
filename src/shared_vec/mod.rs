use crate::backend::SlabBackend;
use crate::entry::CacheEntry;
use std::cell::UnsafeCell;
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;

pub(crate) struct SharedVec<K: Hash + Eq, V: Clone> {
    source: UnsafeCell<Box<[MaybeUninit<CacheEntry<K, V>>]>>,
    num_shards: usize,
}

fn make_uninit_buffer<K: Hash + Eq, V: Clone>(
    capacity: usize,
) -> Vec<MaybeUninit<CacheEntry<K, V>>> {
    let mut source = Vec::with_capacity(capacity);
    unsafe { source.set_len(capacity) };
    source
}

fn compute_shard_base_ptr<K: Hash + Eq, V: Clone>(
    base_ptr: *const MaybeUninit<CacheEntry<K, V>>,
    shard_idx: usize,
    shard_len: usize,
) -> *const MaybeUninit<CacheEntry<K, V>> {
    let shard_start_ptr = unsafe { base_ptr.add(shard_idx * shard_len) };
    shard_start_ptr as *const MaybeUninit<CacheEntry<K, V>>
}

impl<K: Hash + Eq, V: Clone> SharedVec<K, V> {
    pub(crate) fn new(capacity: NonZeroUsize, num_shards: NonZeroUsize) -> SharedVec<K, V> {
        assert_eq!(
            capacity.get() % num_shards.get(),
            0_usize,
            "num_shards must evenly divide capacity"
        );
        let full_slab = make_uninit_buffer::<K, V>(capacity.get()).into_boxed_slice();
        let source = UnsafeCell::new(full_slab);

        SharedVec {
            source,
            num_shards: num_shards.into(),
        }
    }

    pub(crate) fn make_shards(&self) -> Vec<SharedVecRef<K, V>> {
        self.make_buffer_shards()
    }

    fn make_buffer_shards(&self) -> Vec<SharedVecRef<K, V>> {
        let (base_ptr, len) = unsafe {
            let inner = self.source.get();
            ((*inner).as_ptr(), (&*inner).len())
        };
        let shard_len = len / self.num_shards;

        (0..self.num_shards)
            .map(|shard_idx| {
                let shard_start_ptr = compute_shard_base_ptr(base_ptr, shard_idx, shard_len);
                SharedVecRef::new(shard_start_ptr, shard_len)
            })
            .collect()
    }
}

pub(crate) struct SharedVecRef<K: Hash + Eq, V: Clone> {
    ptr: *const MaybeUninit<CacheEntry<K, V>>,
    len: usize,
    cap: usize,
}

impl<K: Hash + Eq, V: Clone> SharedVecRef<K, V> {
    fn new(ptr: *const MaybeUninit<CacheEntry<K, V>>, cap: usize) -> SharedVecRef<K, V> {
        SharedVecRef {
            ptr,
            len: 0_usize,
            cap,
        }
    }

    unsafe fn swap_remove_inner(&mut self, idx: usize) -> CacheEntry<K, V> {
        unsafe {
            let evicted = (*self.ptr.add(idx)).assume_init_read();
            self.len -= 1;
            if idx != self.len {
                let last = (*self.ptr.add(self.len)).assume_init_read();
                (*self.ptr.add(idx).cast_mut()).write(last);
            }
            evicted
        }
    }

    fn swap_remove(&mut self, idx: usize) -> CacheEntry<K, V> {
        unsafe { self.swap_remove_inner(idx) }
    }

    fn push(&mut self, entry: CacheEntry<K, V>) {
        unsafe { self.push_item(entry) }
    }

    unsafe fn push_item(&mut self, entry: CacheEntry<K, V>) {
        debug_assert!(self.len < self.cap,);
        unsafe {
            self.ptr
                .add(self.len)
                .cast_mut()
                .write(MaybeUninit::new(entry))
        }
        self.len += 1;
    }

    fn get(&self, idx: usize) -> &CacheEntry<K, V> {
        unsafe { self.get_unchecked(idx) }
    }

    fn get_mut(&mut self, idx: usize) -> &mut CacheEntry<K, V> {
        unsafe { self.get_unchecked_mut(idx) }
    }

    unsafe fn get_unchecked(&self, idx: usize) -> &CacheEntry<K, V> {
        debug_assert!(idx < self.len);
        unsafe { (*self.ptr.add(idx)).assume_init_ref() }
    }

    unsafe fn get_unchecked_mut(&mut self, idx: usize) -> &mut CacheEntry<K, V> {
        debug_assert!(idx < self.len);
        unsafe { (*self.ptr.add(idx).cast_mut()).assume_init_mut() }
    }
}

// SAFETY: each SharedVecRef owns a non-overlapping sub-slice of the global allocation.
// Concurrent access to individual shards is controlled by the RwLock wrapping each SlabShard.
unsafe impl<K: Hash + Eq + Send, V: Clone + Send> Send for SharedVecRef<K, V> {}
unsafe impl<K: Hash + Eq + Sync, V: Clone + Sync> Sync for SharedVecRef<K, V> {}

unsafe impl<K: Hash + Eq + Send, V: Clone + Send> Send for SharedVec<K, V> {}
unsafe impl<K: Hash + Eq + Sync, V: Clone + Sync> Sync for SharedVec<K, V> {}

impl<K: Hash + Eq, V: Clone> std::fmt::Debug for SharedVecRef<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedVecRef")
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .field("cap", &self.cap)
            .finish()
    }
}

impl<K: Hash + Eq, V: Clone> std::fmt::Debug for SharedVec<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedVec")
            .field("num_shards", &self.num_shards)
            .finish_non_exhaustive()
    }
}

impl<K: Hash + Eq, V: Clone> SlabBackend<K, V> for SharedVecRef<K, V> {
    fn get_entry(&self, idx: usize) -> &CacheEntry<K, V> {
        self.get(idx)
    }

    fn get_entry_mut(&mut self, idx: usize) -> &mut CacheEntry<K, V> {
        self.get_mut(idx)
    }

    fn push_entry(&mut self, entry: CacheEntry<K, V>) {
        self.push(entry)
    }

    fn swap_remove_entry(&mut self, idx: usize) -> CacheEntry<K, V> {
        self.swap_remove(idx)
    }

    fn len(&self) -> usize {
        self.len
    }

    fn clear(&mut self) {
        for i in 0..self.len {
            unsafe { (*self.ptr.add(i).cast_mut()).assume_init_drop() }
        }
        self.len = 0;
    }
}
