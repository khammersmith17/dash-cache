use std::cell::UnsafeCell;
use std::iter::Iterator;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const BUFFER_SIZE: usize = 64_usize;

// Concurrent queue to queue up read promotions, implemented as a ring buffer.
#[derive(Debug)]
pub(crate) struct PromotionQueue {
    buffer: UnsafeCell<[u32; BUFFER_SIZE]>,
    head: usize,
    tail: AtomicUsize,
    full: AtomicBool,
}

unsafe impl Send for PromotionQueue {}
unsafe impl Sync for PromotionQueue {}

impl Default for PromotionQueue {
    fn default() -> PromotionQueue {
        let buffer = UnsafeCell::new([u32::MAX; BUFFER_SIZE]);
        let head = 0_usize;
        let tail = AtomicUsize::new(0_usize);
        let full = AtomicBool::new(false);
        PromotionQueue {
            buffer,
            head,
            tail,
            full,
        }
    }
}

impl PromotionQueue {
    // Push an entry for promotion on the queue.
    pub(crate) fn push(&self, idx: u32) {
        if self.full.load(Ordering::Acquire) {
            return;
        }
        let count = self.tail.fetch_add(1_usize, Ordering::Relaxed);
        let entry = count % BUFFER_SIZE;
        if count - self.head >= BUFFER_SIZE {
            self.full.store(true, Ordering::Release);
            return;
        }
        unsafe {
            (*self.buffer.get()).as_mut_ptr().add(entry).write(idx);
        }
    }

    /// We know one thread will pop off the entire list of entries. The callers of this will have
    /// an exclusive lock on the shards slab. Thus we can just iterate with a guard to ensure that
    /// reader wrote the value after incrementing the atomic.
    pub(crate) fn drain(&mut self) -> PromotionIter {
        let tail = self.tail.load(Ordering::Relaxed);
        let queue = unsafe { &*self.buffer.get() };
        let promotion_iter = PromotionIter {
            queue,
            current: self.head,
            end: tail,
        };
        self.head = tail;
        self.full.store(false, Ordering::Release);
        promotion_iter
    }
}

// Provides zero copy view into the PromotionQueue to perform the promotions.
pub(crate) struct PromotionIter<'a> {
    queue: &'a [u32; BUFFER_SIZE],
    current: usize,
    end: usize,
}

impl<'a> Iterator for PromotionIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            return None;
        }

        let item = self.queue[self.current % BUFFER_SIZE];
        self.current += 1;
        Some(item)
    }
}
