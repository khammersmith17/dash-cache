use std::cell::UnsafeCell;
use std::iter::Iterator;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) const DEFAULT_BUFFER_SIZE: usize = 128_usize;
const FULL_SENTINEL: usize = 1_usize << (usize::BITS - 1);

// Concurrent queue to queue up read promotions, implemented as a ring buffer.
#[derive(Debug)]
pub(crate) struct CacheQueue {
    buffer: UnsafeCell<Box<[u32]>>,
    capacity: usize,
    head: usize, // head is not concurrently mutated.
    tail: AtomicUsize,
}

// SAFETY: Explicit impl for Send + Sync given `UnsafeCell`.
unsafe impl Send for CacheQueue {}
unsafe impl Sync for CacheQueue {}

impl Default for CacheQueue {
    fn default() -> CacheQueue {
        CacheQueue::new(DEFAULT_BUFFER_SIZE)
    }
}

impl CacheQueue {
    pub(crate) fn new(queue_size: usize) -> CacheQueue {
        let buffer = UnsafeCell::new(vec![0_u32; queue_size].into_boxed_slice());
        let head = 0_usize;
        let tail = AtomicUsize::new(0_usize);
        CacheQueue {
            buffer,
            head,
            tail,
            capacity: queue_size,
        }
    }
    // Push an entry for promotion on the queue.
    //
    // When the queue is full, the MSB of the tail will be flipped on. This will never result in a
    // collision given the max buffer size.
    //
    // If the MSB is flipped on, then we skip and drop the entry. This will ignore some valid
    // operations, but will ensure that this buffer is safe and statically sized.
    pub(crate) fn push(&self, idx: u32) {
        // First check is the buffer is full.
        let state = self.tail.load(Ordering::Relaxed);
        if state & FULL_SENTINEL != 0 {
            return;
        }

        // Fetch the slot in the queue to write the entry to.
        let entry = self.tail.fetch_add(1_usize, Ordering::Relaxed);

        // Check again to ensure nobody else marked as full in the meantime.
        if entry & FULL_SENTINEL != 0 {
            return;
        }

        // If we have reached max capacity, mark as full.
        if entry >= self.capacity {
            let _ = self.tail.fetch_or(FULL_SENTINEL, Ordering::Release);
            return;
        }

        // SAFETY: entry offset is guaranteed to be within the bounds of the array bounds, given
        // the above statement.
        unsafe {
            (*self.buffer.get()).as_mut_ptr().add(entry).write(idx);
        }
    }

    /// We know one thread will pop off the entire list of entries. The callers of this will have
    /// an exclusive lock on the shards slab. Thus we can just iterate with a guard to ensure that
    /// reader wrote the value after incrementing the atomic.
    ///
    /// The exclusive reference is reasonable here, given the constraint this method is only called
    /// in the context of a held write lock.
    ///
    /// This also clears the queue, and resets the head and tail to the front of the queue.
    pub(crate) fn drain(&mut self) -> QueueIter {
        let mut tail = self.tail.load(Ordering::Relaxed);
        tail = (tail & !FULL_SENTINEL).min(self.capacity);
        let queue = unsafe { &*self.buffer.get() };
        let promotion_iter = QueueIter {
            queue,
            current: 0_usize,
            end: tail,
            capacity: self.capacity,
        };
        self.head = 0;
        self.tail.store(0_usize, Ordering::Release);
        promotion_iter
    }
}

// Provides zero copy view into the CacheQueue to perform the promotions.
// This is guaranteed to only be held while an exlusive write lock is held, thus the lifetime is
// safe and upheld.
pub(crate) struct QueueIter<'a> {
    queue: &'a Box<[u32]>, // Ref to the Queue's buffer.
    current: usize,
    end: usize,
    capacity: usize,
}

impl<'a> Iterator for QueueIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            return None;
        }

        let item = self.queue[self.current % self.capacity];
        self.current += 1;
        Some(item)
    }
}
