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

#[cfg(test)]
mod pointer_idx_tests {
    use super::pointer_idx::*;

    #[test]
    fn null_neighbors_both_none() {
        let ptr = null_neighbors();
        assert_eq!(get_next_pointer(ptr), None);
        assert_eq!(get_prev_pointer(ptr), None);
    }

    #[test]
    fn null_neighbors_is_u64_max() {
        assert_eq!(null_neighbors(), u64::MAX);
    }

    #[test]
    fn set_and_get_next_some() {
        let ptr = null_neighbors();
        let ptr = set_next_pointer(ptr, Some(42));
        assert_eq!(get_next_pointer(ptr), Some(42));
        // prev must be unaffected
        assert_eq!(get_prev_pointer(ptr), None);
    }

    #[test]
    fn set_and_get_prev_some() {
        let ptr = null_neighbors();
        let ptr = set_prev_pointer(ptr, Some(99));
        assert_eq!(get_prev_pointer(ptr), Some(99));
        // next must be unaffected
        assert_eq!(get_next_pointer(ptr), None);
    }

    #[test]
    fn set_both_independently() {
        let ptr = null_neighbors();
        let ptr = set_next_pointer(ptr, Some(7));
        let ptr = set_prev_pointer(ptr, Some(13));
        assert_eq!(get_next_pointer(ptr), Some(7));
        assert_eq!(get_prev_pointer(ptr), Some(13));
    }

    #[test]
    fn set_next_does_not_clobber_prev() {
        let ptr = null_neighbors();
        let ptr = set_prev_pointer(ptr, Some(5));
        let ptr = set_next_pointer(ptr, Some(10));
        assert_eq!(get_prev_pointer(ptr), Some(5));
        assert_eq!(get_next_pointer(ptr), Some(10));
    }

    #[test]
    fn set_prev_does_not_clobber_next() {
        let ptr = null_neighbors();
        let ptr = set_next_pointer(ptr, Some(20));
        let ptr = set_prev_pointer(ptr, Some(30));
        assert_eq!(get_next_pointer(ptr), Some(20));
        assert_eq!(get_prev_pointer(ptr), Some(30));
    }

    #[test]
    fn clear_next_to_none() {
        let ptr = null_neighbors();
        let ptr = set_next_pointer(ptr, Some(100));
        let ptr = set_next_pointer(ptr, None);
        assert_eq!(get_next_pointer(ptr), None);
    }

    #[test]
    fn clear_prev_to_none() {
        let ptr = null_neighbors();
        let ptr = set_prev_pointer(ptr, Some(200));
        let ptr = set_prev_pointer(ptr, None);
        assert_eq!(get_prev_pointer(ptr), None);
    }

    #[test]
    fn max_valid_index() {
        // u32::MAX is the sentinel; u32::MAX - 1 is the largest valid index
        let max_idx = VALID_FLAG - 1;
        let ptr = null_neighbors();
        let ptr = set_next_pointer(ptr, Some(max_idx));
        let ptr = set_prev_pointer(ptr, Some(max_idx));
        assert_eq!(get_next_pointer(ptr), Some(max_idx));
        assert_eq!(get_prev_pointer(ptr), Some(max_idx));
    }

    #[test]
    fn zero_index_roundtrips() {
        let ptr = null_neighbors();
        let ptr = set_next_pointer(ptr, Some(0));
        let ptr = set_prev_pointer(ptr, Some(0));
        assert_eq!(get_next_pointer(ptr), Some(0));
        assert_eq!(get_prev_pointer(ptr), Some(0));
    }

    #[test]
    fn overwrite_next_preserves_prev() {
        let ptr = null_neighbors();
        let ptr = set_next_pointer(ptr, Some(1));
        let ptr = set_prev_pointer(ptr, Some(2));
        // overwrite next
        let ptr = set_next_pointer(ptr, Some(99));
        assert_eq!(get_next_pointer(ptr), Some(99));
        assert_eq!(get_prev_pointer(ptr), Some(2));
    }

    #[test]
    fn overwrite_prev_preserves_next() {
        let ptr = null_neighbors();
        let ptr = set_next_pointer(ptr, Some(3));
        let ptr = set_prev_pointer(ptr, Some(4));
        // overwrite prev
        let ptr = set_prev_pointer(ptr, Some(88));
        assert_eq!(get_next_pointer(ptr), Some(3));
        assert_eq!(get_prev_pointer(ptr), Some(88));
    }
}
