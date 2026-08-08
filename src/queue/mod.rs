use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const BUFFER_SIZE: usize = 64_usize;

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
    // Push
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
    pub(crate) fn drain(&mut self) -> Vec<u32> {
        let tail = self.tail.load(Ordering::Relaxed);
        let mut entries = Vec::with_capacity((tail - self.head).min(BUFFER_SIZE));
        while self.head != tail {
            let slot = self.head % BUFFER_SIZE;
            let idx: u32 = unsafe { *(*self.buffer.get()).as_ptr().add(slot) };
            entries.push(idx);
            self.head = self.head.wrapping_add(1);
        }
        self.full.store(false, Ordering::Release);
        entries
    }
}
