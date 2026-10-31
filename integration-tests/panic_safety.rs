//! Regression tests for panic safety: a panic in a user's `Clone` or `Drop` must never leave a
//! shard in an inconsistent state. Also run under Miri:
//!
//! ```sh
//! cargo +nightly miri test --test panic_safety
//! ```
//!
//! Every shard operation calls user code (`Clone`, `Drop`) somewhere in a multi-step update of
//! the map, the recency list and the slab. A panic between those steps used to orphan slab
//! entries: later inserts then wrote past the shard's capacity, which was an out-of-bounds write
//! in release builds, and stale list indices led to out-of-bounds reads.
//!
//! The keys and values here panic at random in `clone` and `drop` while an operation runs. The
//! cache is kept in use after each panic, as a caller catching the panic, or another task sharing
//! a `DashCache` shard, would. Any panic other than an injected one fails the test, which catches
//! the `len < cap` assertions and the debug assertions on the list. After each run, the cache must
//! still fill to capacity and return every value.

use dash_cache::{DashCacheBuilder, SlabShard};
use futures::executor::block_on;
use std::cell::Cell;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Once;

const CAP: usize = 8;
const INJECTED: &str = "injected panic";

thread_local! {
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static RNG: Cell<u64> = const { Cell::new(1) };
}

// xorshift64, so every run is deterministic.
fn rnd() -> u64 {
    RNG.with(|r| {
        let mut x = r.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        r.set(x);
        x
    })
}

fn seed(seed: u64) {
    RNG.with(|r| r.set(seed.wrapping_mul(2_654_435_761) | 1));
}

// Panics about once in 7 calls while an operation is running. Never panics while already
// unwinding, since that would abort the process.
fn maybe_panic() {
    if ARMED.with(|a| a.get()) && !std::thread::panicking() && rnd() % 7 == 0 {
        panic!("{INJECTED}");
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct Key(u64);

impl Clone for Key {
    fn clone(&self) -> Key {
        maybe_panic();
        Key(self.0)
    }
}

impl Drop for Key {
    fn drop(&mut self) {
        maybe_panic();
    }
}

#[derive(Debug)]
struct Value(u64);

impl Clone for Value {
    fn clone(&self) -> Value {
        maybe_panic();
        Value(self.0)
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        maybe_panic();
    }
}

// Silences injected panics, which would otherwise print thousands of messages, and leaves every
// other panic message alone.
fn quiet_injected_panics() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if info.payload_as_str() != Some(INJECTED) {
                default(info);
            }
        }));
    });
}

// Runs `op` with injected panics enabled. Returns whether a panic was injected; fails the test on
// any other panic.
fn run_armed(seed: u64, step: usize, op: impl FnOnce()) -> bool {
    ARMED.with(|a| a.set(true));
    let result = catch_unwind(AssertUnwindSafe(op));
    ARMED.with(|a| a.set(false));
    match result {
        Ok(()) => false,
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("<non-string panic>");
            assert_eq!(
                message, INJECTED,
                "seed {seed}, step {step}: cache panicked on its own after earlier user panics"
            );
            true
        }
    }
}

fn runs() -> (u64, usize) {
    // (seeds, operations per seed). Miri is far slower, and CI runs it over several seeds.
    if cfg!(miri) { (6, 120) } else { (500, 300) }
}

#[test]
fn slab_shard_survives_user_panics() {
    quiet_injected_panics();
    let (seeds, steps) = runs();
    let mut injected = 0;

    for s in 1..=seeds {
        seed(s);
        let mut cache = SlabShard::<Key, Value>::with_capacity(NonZeroUsize::new(CAP).unwrap());

        for step in 0..steps {
            let op = rnd() % 25;
            let k = rnd() % (2 * CAP as u64);
            let cache = &mut cache;
            injected += run_armed(s, step, || match op {
                0..=7 => cache.insert(Key(k), Value(k)),
                8..=15 => drop(cache.get(&Key(k))),
                16..=19 => drop(cache.update(&Key(k), Value(k))),
                20..=23 => drop(cache.evict(&Key(k))),
                _ => cache.drain(),
            }) as usize;
        }

        // The shard must still work: fill it with fresh keys and read every one back.
        for i in 0..CAP as u64 {
            cache.insert(Key(1_000 + i), Value(i));
        }
        assert_eq!(cache.len(), CAP, "seed {s}: shard no longer holds its capacity");
        for i in 0..CAP as u64 {
            let value = cache.get(&Key(1_000 + i)).map(|v| v.0);
            assert_eq!(value, Some(i), "seed {s}: lost key {}", 1_000 + i);
        }
    }

    assert!(injected > 0, "no panics were injected, so nothing was tested");
}

#[test]
fn dash_cache_survives_user_panics() {
    quiet_injected_panics();
    let (seeds, steps) = runs();
    let mut injected = 0;

    for s in 1..=seeds {
        seed(s);
        // One shard and small queues, so every operation hits the same shard and the queues fill
        // up and drain early. Fixed hasher seeds keep runs deterministic.
        let queue = NonZeroUsize::new(4).unwrap();
        let cache = DashCacheBuilder::<Key, Value>::new(NonZeroUsize::new(CAP).unwrap())
            .with_hasher(ahash::RandomState::with_seeds(1, 2, 3, 4))
            .with_num_shards(NonZeroUsize::new(1).unwrap())
            .with_promotion_queue_size(queue)
            .with_eviction_queue_size(queue)
            .build();

        for step in 0..steps {
            let op = rnd() % 25;
            let k = rnd() % (2 * CAP as u64);
            injected += run_armed(s, step, || {
                block_on(async {
                    match op {
                        0..=6 => cache.insert(Key(k), Value(k)).await,
                        7..=13 => drop(cache.get(&Key(k)).await),
                        14..=18 => drop(cache.update(&Key(k), Value(k)).await),
                        19..=23 => drop(cache.evict(&Key(k)).await),
                        _ => cache.drain().await,
                    }
                })
            }) as usize;
        }

        block_on(async {
            for i in 0..CAP as u64 {
                cache.insert(Key(1_000 + i), Value(i)).await;
            }
            assert_eq!(cache.len().await, CAP, "seed {s}: cache no longer holds its capacity");
            for i in 0..CAP as u64 {
                let value = cache.get(&Key(1_000 + i)).await.map(|v| v.0);
                assert_eq!(value, Some(i), "seed {s}: lost key {}", 1_000 + i);
            }
        });
    }

    assert!(injected > 0, "no panics were injected, so nothing was tested");
}
