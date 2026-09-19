//! Shared slab allocation for concurrent cache shards.
//!
//! [`SharedVec`] allocates a single contiguous block of memory up-front and partitions it into
//! `num_shards` non-overlapping sub-slices. Each sub-slice is handed to one [`SharedVecRef`],
//! which is the per-shard backing store used by [`SlabShard`](crate::core::SlabShard).
//!
//! # Memory model
//!
//! The allocation is owned by an `Arc<UnsafeCell<[MaybeUninit<CacheEntry<K, V>>]>>` inside
//! [`SharedVec`]. [`SharedVecRef`] holds a raw `NonNull` pointer into its sub-slice rather than
//! an `Arc` clone, so shard access is zero-cost after construction. The `Arc` in [`SharedVec`]
//! keeps the backing memory alive for the lifetime of the cache — it is stored in
//! `InnerCacheShards` alongside the shards themselves.
//!
//! # Safety invariants
//!
//! * Sub-slices are non-overlapping by construction (`make_shards` offsets each pointer by
//!   `shard_len` elements).
//! * Each [`SharedVecRef`] has exactly one owner (`InnerCacheShards` stores them inside
//!   `LockedCache` values, each protected by a `tokio::RwLock`). Exclusive (`&mut`) access to a
//!   [`SharedVecRef`] is therefore only possible while holding the write lock.
//! * Elements are stored as `MaybeUninit<CacheEntry<K, V>>`. Only indices `0..len` are
//!   initialised; reads outside that range are undefined behaviour.

use crate::backend::SlabBackend;
use crate::entry::CacheEntry;
use std::alloc::{Layout, alloc};
use std::cell::UnsafeCell;
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::ptr::{NonNull, slice_from_raw_parts_mut};
use std::sync::Arc;

/// Owner of the single contiguous slab allocation shared across all shards.
///
/// Constructed once during cache initialisation. Must be kept alive for the
/// lifetime of the cache so the backing memory is not freed.
#[derive(Debug)]
pub(crate) struct SharedVec<K: Hash + Eq, V: Clone> {
    source: Arc<UnsafeCell<[MaybeUninit<CacheEntry<K, V>>]>>,
    num_shards: usize,
}

/// Allocate the raw slab.
fn make_slab<T>(cap: usize) -> Arc<UnsafeCell<[MaybeUninit<T>]>> {
    // Slab layout and pointer.
    let layout = Layout::array::<MaybeUninit<T>>(cap).expect("Unable to allocate the base slab");
    let ptr: *mut MaybeUninit<T> = unsafe { alloc(layout) as *mut MaybeUninit<T> };
    assert!(!ptr.is_null(), "Slab allocation failed");

    // Make pointer fat then cast to UnsafeCell.
    // SAFETY: UnsafeCell<T> is #[repr(transparent)], so *mut [MaybeUninit<T>] and
    // *mut UnsafeCell<[MaybeUninit<T>]> have identical layout and validity.
    let slice_ptr: *mut [MaybeUninit<T>] = slice_from_raw_parts_mut(ptr, cap);
    let cell_ptr = slice_ptr as *mut UnsafeCell<[MaybeUninit<T>]>;
    let cell = unsafe { Box::from_raw(cell_ptr) };
    Arc::from(cell)
}

/// Validate the validity of the user provided configs.
fn validate_slab_config(cap: usize, num_shards: usize) {
    assert!(
        cap >= num_shards,
        "capacity must be larger than then number of shards"
    );
    assert_eq!(
        cap % num_shards,
        0,
        "num_shards must evenly divide capacity"
    );
}

/// Get a pointer to the front of the full slab.
fn compute_shard_base_ptr<K: Hash + Eq, V: Clone>(
    base_ptr: *mut MaybeUninit<CacheEntry<K, V>>,
    shard_idx: usize,
    shard_len: usize,
) -> *mut MaybeUninit<CacheEntry<K, V>> {
    let shard_start_ptr = unsafe { base_ptr.add(shard_idx * shard_len) };
    shard_start_ptr
}

impl<K: Hash + Eq, V: Clone> SharedVec<K, V> {
    /// Allocate a new full slab.
    pub(crate) fn new(capacity: NonZeroUsize, num_shards: NonZeroUsize) -> SharedVec<K, V> {
        let num_shards = num_shards.get();
        let cap = capacity.get();
        validate_slab_config(cap, num_shards);

        let source = make_slab(cap);

        SharedVec { source, num_shards }
    }

    /// Define the shards by slicing up the full slab.
    /// Compute the pointer to the beginning of each shard.
    pub(crate) fn make_shards(&self) -> Vec<SharedVecRef<K, V>> {
        let (base_ptr, len) = unsafe {
            let inner = self.source.get();
            ((*inner).as_mut_ptr(), (&*inner).len())
        };
        let shard_len = len / self.num_shards;

        (0..self.num_shards)
            .map(|shard_idx| {
                let shard_start_ptr = compute_shard_base_ptr(base_ptr, shard_idx, shard_len);
                let ptr = NonNull::new(shard_start_ptr).unwrap();
                SharedVecRef::new(ptr, shard_len)
            })
            .collect()
    }
}

/// A handle to one shard's sub-slice of the global slab allocation.
///
/// Each `SharedVecRef` owns a non-overlapping region of the allocation created by [`SharedVec`].
///
/// `SharedVecRef` implements [`SlabBackend`] and is the backing store passed to
/// [`SlabShard`](crate::core::SlabShard) for concurrent caches.
///
/// # Safety
///
/// The pointer is non-null and valid for the lifetime of the parent [`SharedVec`] allocation.
/// Callers must ensure no two `SharedVecRef`s alias the same memory region.
#[derive(Debug)]
pub(crate) struct SharedVecRef<K: Hash + Eq, V: Clone> {
    /// Pointer to the first slot of this shard's sub-slice.
    ptr: NonNull<MaybeUninit<CacheEntry<K, V>>>,
    /// Number of initialised entries currently stored.
    len: usize,
    /// Maximum number of entries this shard can hold.
    cap: usize,
}

impl<K: Hash + Eq, V: Clone> SharedVecRef<K, V> {
    fn new(ptr: NonNull<MaybeUninit<CacheEntry<K, V>>>, cap: usize) -> SharedVecRef<K, V> {
        SharedVecRef {
            ptr,
            len: 0_usize,
            cap,
        }
    }

    /// Remove an entry from the slab, returning the entry to the user.
    unsafe fn swap_remove_inner(&mut self, idx: usize) -> CacheEntry<K, V> {
        unsafe {
            let evicted = (*self.ptr.add(idx).as_ptr()).assume_init_read();
            self.len -= 1;
            if idx != self.len {
                let last = (*self.ptr.add(self.len).as_ptr()).assume_init_read();
                (*self.ptr.add(idx).as_mut()).write(last);
            }
            evicted
        }
    }

    /// Safe wrapper around unsafe swap_remove_inner.
    fn swap_remove(&mut self, idx: usize) -> CacheEntry<K, V> {
        unsafe { self.swap_remove_inner(idx) }
    }

    /// Safe wrapper around unsafe push_item.
    fn push(&mut self, entry: CacheEntry<K, V>) {
        unsafe { self.push_item(entry) }
    }

    /// Write entry and increment len.
    unsafe fn push_item(&mut self, entry: CacheEntry<K, V>) {
        debug_assert!(self.len < self.cap,);
        unsafe {
            let ptr = self.ptr.add(self.len).as_mut();
            std::ptr::write(ptr, MaybeUninit::new(entry));
        }
        self.len += 1;
    }

    /// Returns a shared reference to the entry at `idx`. Caller must ensure `idx < len`.
    fn get(&self, idx: usize) -> &CacheEntry<K, V> {
        unsafe { self.get_unchecked(idx) }
    }

    /// Returns an exclusive reference to the entry at `idx`. Caller must ensure `idx < len`.
    fn get_mut(&mut self, idx: usize) -> &mut CacheEntry<K, V> {
        unsafe { self.get_unchecked_mut(idx) }
    }

    unsafe fn get_unchecked(&self, idx: usize) -> &CacheEntry<K, V> {
        debug_assert!(idx < self.len);
        unsafe { (*self.ptr.add(idx).as_ptr()).assume_init_ref() }
    }

    unsafe fn get_unchecked_mut(&mut self, idx: usize) -> &mut CacheEntry<K, V> {
        debug_assert!(idx < self.len);
        unsafe { (*self.ptr.add(idx).as_mut()).assume_init_mut() }
    }
}

// SAFETY: each SharedVecRef owns a non-overlapping sub-slice of the global allocation.
// Each shard has only a single owner, access is gated through a RwLock.
unsafe impl<K: Hash + Eq + Send, V: Clone + Send> Send for SharedVecRef<K, V> {}
unsafe impl<K: Hash + Eq + Sync, V: Clone + Sync> Sync for SharedVecRef<K, V> {}

unsafe impl<K: Hash + Eq + Send, V: Clone + Send> Send for SharedVec<K, V> {}
unsafe impl<K: Hash + Eq + Sync, V: Clone + Sync> Sync for SharedVec<K, V> {}

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
            unsafe { (*self.ptr.add(i).as_mut()).assume_init_drop() }
        }
        self.len = 0;
    }
}
