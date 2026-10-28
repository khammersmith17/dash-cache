use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

pub(crate) const DEFAULT_BUFFER_SIZE: usize = 256_usize;
const FULL_SENTINEL: usize = 1_usize << (usize::BITS - 1);

// Concurrent queue to queue up read promotions, implemented as a ring buffer.
#[derive(Debug)]
pub(crate) struct CacheQueue {
    // Miri found some UB when this was simply u32 and two gets where called at exactly the same
    // time. This adds some overhead but prevents that.
    buffer: Box<[AtomicU32]>,
    capacity: usize,
    tail: AtomicUsize,
}

impl Default for CacheQueue {
    fn default() -> CacheQueue {
        CacheQueue::new(DEFAULT_BUFFER_SIZE)
    }
}

impl CacheQueue {
    pub(crate) fn new(queue_size: usize) -> CacheQueue {
        let buffer = (0..queue_size).map(|_| AtomicU32::new(0_u32)).collect();
        let tail = AtomicUsize::new(0_usize);
        CacheQueue {
            buffer,
            tail,
            capacity: queue_size,
        }
    }
    // Push an entry for promotion on the queue.
    //
    // When the queue is full, the MSB of the tail will be flipped on. This will never result in a
    // collision given the max buffer size.
    //
    // If the MSB is flipped on, the entry is not written and `Err(())` is returned, so the caller
    // can drain the queue under a write lock. This keeps the buffer safe and statically sized.
    pub(crate) fn try_push(&self, idx: u32) -> Result<(), ()> {
        // First check is the buffer is full.
        let state = self.tail.load(Ordering::Relaxed);
        if state & FULL_SENTINEL != 0 {
            return Err(());
        }

        // Fetch the slot in the queue to write the entry to.
        let entry = self.tail.fetch_add(1_usize, Ordering::Relaxed);

        // Check again to ensure nobody else marked as full in the meantime.
        if entry & FULL_SENTINEL != 0 {
            return Err(());
        }

        // If we have reached max capacity, mark as full.
        if entry >= self.capacity {
            let _ = self.tail.fetch_or(FULL_SENTINEL, Ordering::Release);
            return Err(());
        }

        // SAFETY: entry offset is guaranteed to be within the bounds of the array bounds, given
        // the above statement.
        self.buffer[entry].store(idx, Ordering::Relaxed);
        Ok(())
    }

    /// We know one thread will pop off the entire list of entries. The callers of this will have
    /// an exclusive lock on the shards slab. Thus we can just iterate with a guard to ensure that
    /// reader wrote the value after incrementing the atomic.
    ///
    /// The exclusive reference is reasonable here, given the constraint this method is only called
    /// in the context of a held write lock.
    ///
    /// This also clears the queue and resets the tail to the front of the queue.
    pub(crate) fn drain(&mut self) -> QueueIter<'_> {
        let mut tail = self.tail.load(Ordering::Relaxed);
        tail = (tail & !FULL_SENTINEL).min(self.capacity);

        let promotion_iter = QueueIter {
            queue: self.buffer.as_ref(),
            current: 0_usize,
            end: tail,
        };
        self.tail.store(0_usize, Ordering::Release);
        promotion_iter
    }
}

// Provides zero copy view into the CacheQueue to perform the promotions.
// This is guaranteed to only be held while an exlusive write lock is held, thus the lifetime is
// safe and upheld.
pub(crate) struct QueueIter<'a> {
    queue: &'a [AtomicU32], // Ref to the Queue's buffer.
    current: usize,
    end: usize,
}

impl<'a> Iterator for QueueIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            return None;
        }

        let item = self.queue[self.current].load(Ordering::Relaxed);
        self.current += 1;
        Some(item)
    }
}
