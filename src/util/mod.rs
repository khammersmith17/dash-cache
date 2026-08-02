use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

pub static CACHE_START: OnceLock<Instant> = OnceLock::new();

pub(crate) mod pointer_idx {

    pub(crate) const VALID_FLAG: u32 = u32::MAX;
    const NEXT_MASK: u64 = 0xFFFFFFFF00000000;
    const PREV_MASK: u64 = 0xFFFFFFFF;

    pub(crate) const fn null_neighbors() -> u64 {
        ((VALID_FLAG as u64) << 32) | (VALID_FLAG as u64)
    }

    // Next and prev pointers are stored as a single u64, with the first 4 bytes storing the next
    // pointer and the last 4 bytes storing the prev pointer. The most signifcant bit of each
    // represents existance, flipped on for Some and flipped off for None.
    //
    // This now limits the number of possible entries to 1 << 31.
    pub(crate) fn get_next_pointer(ptr: u64) -> Option<u32> {
        let next_masked = ptr & NEXT_MASK;
        let next = (next_masked >> 32) as u32;

        if next == VALID_FLAG {
            return None;
        };
        Some(next)
    }

    pub(crate) fn get_prev_pointer(ptr: u64) -> Option<u32> {
        let prev_masked = ptr & PREV_MASK;
        let prev = prev_masked as u32;

        if prev == VALID_FLAG {
            return None;
        };
        Some(prev)
    }

    // Mask the next portion, clear then or with new pointer.
    pub(crate) fn set_next_pointer(ptr: u64, next: Option<u32>) -> u64 {
        let next = next.unwrap_or(VALID_FLAG);
        (ptr & PREV_MASK) | ((next as u64) << 32)
    }

    // Mask the prev portion, clear then or with new pointer.
    pub(crate) fn set_prev_pointer(ptr: u64, prev: Option<u32>) -> u64 {
        let prev = prev.unwrap_or(VALID_FLAG) as u64;
        (ptr & NEXT_MASK) | prev
    }
}

fn now_nanos() -> u64 {
    let start = CACHE_START.get_or_init(Instant::now);
    Instant::now().duration_since(*start).as_nanos() as u64
}

pub(crate) fn expires_from_ttl(ttl: Option<Duration>) -> u64 {
    let Some(ttl) = ttl else { return 0_u64 };
    ttl.as_nanos() as u64 + now_nanos()
}

pub(crate) fn is_expired(t: u64) -> bool {
    if t == 0_u64 { false } else { t <= now_nanos() }
}
