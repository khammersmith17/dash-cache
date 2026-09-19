/// Concurrent stress test for DashCache.
///
/// Spawns many Tokio tasks performing interleaved inserts, gets, updates, and evictions
/// against a shared cache. Designed to surface data races under ThreadSanitizer and
/// exercise the full concurrent code path at a scale beyond the unit tests.
use dash_cache::DashCache;
use dash_cache::DashCacheBuilder;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::task::JoinSet;

const TASKS: usize = 16;
const OPS_PER_TASK: usize = 10_000;
const CAPACITY: usize = 2_048;

fn build_cache() -> DashCache<u64, u64> {
    DashCacheBuilder::new(NonZeroUsize::new(CAPACITY).unwrap())
        .with_num_shards(NonZeroUsize::new(8).unwrap())
        .build()
}

/// 16 tasks each performing 10 000 mixed ops (insert / get / update / evict).
/// Every key is in [0, CAPACITY), so there is constant eviction pressure and
/// heavy key overlap across tasks — maximising lock contention.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mixed_rw_high_contention() {
    let cache = Arc::new(build_cache());

    // warm the cache to full before the contention phase
    for i in 0..CAPACITY as u64 {
        cache.insert(i, i).await;
    }

    let mut set = JoinSet::new();

    for task_id in 0..TASKS {
        let cache = Arc::clone(&cache);
        set.spawn(async move {
            for i in 0..OPS_PER_TASK {
                let key = ((task_id * OPS_PER_TASK + i) % CAPACITY) as u64;
                match i % 4 {
                    0 => {
                        cache.insert(key, key).await;
                    }
                    1 => {
                        let _ = cache.get(&key).await;
                    }
                    2 => {
                        let _ = cache.update(&key, key + 1).await;
                    }
                    _ => {
                        let _ = cache.evict(&key).await;
                    }
                }
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("task panicked");
    }

    // cache must still be internally consistent: len <= capacity
    assert!(cache.len().await <= CAPACITY);
}

/// Producer / consumer pattern: half the tasks insert disjoint key ranges,
/// half read back from the full key space. Tests that reads never observe
/// torn writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn producer_consumer() {
    const KEYS: u64 = 4_096;
    let cache = Arc::new(build_cache());
    let mut set = JoinSet::new();

    // producers — each owns a disjoint slice
    for p in 0..(TASKS / 2) {
        let cache = Arc::clone(&cache);
        set.spawn(async move {
            let start = (p as u64) * (KEYS / (TASKS as u64 / 2));
            let end = start + (KEYS / (TASKS as u64 / 2));
            for k in start..end {
                cache.insert(k, k * 2).await;
            }
        });
    }

    // consumers — read from the full key space
    for _ in 0..(TASKS / 2) {
        let cache = Arc::clone(&cache);
        set.spawn(async move {
            for k in 0..KEYS {
                if let Some(v) = cache.get(&k).await {
                    // a value, if present, must equal k * 2
                    assert_eq!(v, k * 2, "torn write detected for key {k}");
                }
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("task panicked");
    }
}

/// All tasks hammer a single hot key with inserts and gets to stress the
/// per-shard lock under maximum contention on one shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn hot_key_contention() {
    let cache = Arc::new(build_cache());
    cache.insert(0u64, 0u64).await;

    let mut set = JoinSet::new();

    for _ in 0..TASKS {
        let cache = Arc::clone(&cache);
        set.spawn(async move {
            for i in 0..OPS_PER_TASK as u64 {
                if i % 2 == 0 {
                    cache.insert(0, i).await;
                } else {
                    let _ = cache.get(&0).await;
                }
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("task panicked");
    }
}

/// Concurrent drains interleaved with inserts — exercises the drain path
/// under contention without corrupting internal state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_drain_and_insert() {
    let cache = Arc::new(build_cache());
    let mut set = JoinSet::new();

    for task_id in 0..8usize {
        let cache = Arc::clone(&cache);
        set.spawn(async move {
            for i in 0..1_000u64 {
                let key = (task_id as u64 * 1_000 + i) % CAPACITY as u64;
                if i % 100 == 0 {
                    cache.drain().await;
                } else {
                    cache.insert(key, key).await;
                }
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res.expect("task panicked");
    }

    assert!(cache.len().await <= CAPACITY);
}
