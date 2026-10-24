//! Regression test for a data race on the read path, meant to be run under Miri:
//!
//! ```sh
//! cargo +nightly miri test --test miri_probe
//! ```
//!
//! `DashCache::get` runs under the shard's *read* lock and queues a promotion through
//! `CacheQueue::push`, so concurrent reads of one shard push at the same time. The queue used to
//! write slots through a `&mut [u32]` borrow of the whole buffer, which Miri reports as a data race
//! as soon as two readers overlap. Thread and address sanitizers do not check Rust's aliasing
//! rules, so only Miri catches this.
//!
//! The test also passes under plain `cargo test`, where it just checks that concurrent reads of
//! one shard return the right value.

use dash_cache::DashCacheBuilder;
use std::num::NonZeroUsize;

#[test]
fn concurrent_reads_queue_promotions() {
    // One shard, so both threads read the same shard. Fixed hasher seeds keep Miri runs
    // deterministic.
    let cache = DashCacheBuilder::<u64, u64>::new(NonZeroUsize::new(4).unwrap())
        .with_hasher(ahash::RandomState::with_seeds(1, 2, 3, 4))
        .with_num_shards(NonZeroUsize::new(1).unwrap())
        .build();
    futures::executor::block_on(cache.insert(1, 1));

    // Plain threads and `block_on` rather than a tokio runtime, so Miri only has to interpret the
    // cache itself.
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let cache = cache.clone();
            std::thread::spawn(move || {
                for _ in 0..3 {
                    assert_eq!(futures::executor::block_on(cache.get(&1)), Some(1));
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}
