use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use dash_cache::core::LruCache;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::num::NonZeroUsize;

fn bench_insert_get_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_sync");
    for &cap in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(1_000));
        group.bench_function(format!("insert_then_get_cap={}", cap), |b| {
            b.iter_batched(
                || {
                    // setup
                    let cache = LruCache::with_capacity(NonZeroUsize::new(cap).unwrap());
                    let mut range = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..1_000).map(|_| range.r#gen()).collect();
                    (cache, keys)
                },
                |(mut cache, keys)| {
                    // measure
                    for k in &keys {
                        cache.insert(*k, *k);
                    }
                    for k in &keys {
                        let _ = black_box(cache.get(k));
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

use lru::LruCache as LruBenchmarkCache;
fn bench_insert_get_sync_lru_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_crate_comp");
    for &cap in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(1_000));
        group.bench_function(format!("insert_then_get_cap={}", cap), |b| {
            b.iter_batched(
                || {
                    // setup
                    let cache = LruBenchmarkCache::new(std::num::NonZeroUsize::new(cap).unwrap());
                    let mut range = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..1_000).map(|_| range.r#gen()).collect();
                    (cache, keys)
                },
                |(mut cache, keys)| {
                    // measure
                    for k in &keys {
                        cache.put(*k, *k);
                    }
                    for k in &keys {
                        let _ = black_box(cache.get(k));
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

use dash_cache::core::{CacheShard, IndexedCacheShard};
fn bench_insert_get_sync_single_threaded_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded_shard");
    for &cap in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(1_000));
        group.bench_function(format!("insert_then_get_cap={}", cap), |b| {
            b.iter_batched(
                || {
                    // setup
                    let cache = CacheShard::with_capacity(NonZeroUsize::new(cap).unwrap());
                    let mut range = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..1_000).map(|_| range.r#gen()).collect();
                    (cache, keys)
                },
                |(mut cache, keys)| {
                    // measure
                    for k in &keys {
                        cache.insert(*k, *k);
                    }
                    for k in &keys {
                        let _ = black_box(cache.get(k));
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_insert_get_sync_indexed_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexed_shard");
    for &cap in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(1_000));
        group.bench_function(format!("insert_then_get_cap={}", cap), |b| {
            b.iter_batched(
                || {
                    let cache = IndexedCacheShard::with_capacity(NonZeroUsize::new(cap).unwrap());
                    let mut rng = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..1_000).map(|_| rng.r#gen()).collect();
                    (cache, keys)
                },
                |(mut cache, keys)| {
                    for k in &keys {
                        cache.insert(*k, *k);
                    }
                    for k in &keys {
                        let _ = black_box(cache.get(k));
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

// Measures only the get/promote path with a fully warm cache — no insert cost.
fn bench_get_hit_only_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_get_hit_only");
    for &n in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("n={}", n), |b| {
            b.iter_batched(
                || {
                    let mut cache = CacheShard::with_capacity(NonZeroUsize::new(n).unwrap());
                    let mut rng = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..n).map(|_| rng.r#gen()).collect();
                    for &k in &keys {
                        cache.insert(k, k);
                    }
                    (cache, keys)
                },
                |(mut cache, keys)| {
                    for k in &keys {
                        let _ = black_box(cache.get(k));
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_get_hit_only_lru_crate(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_crate_get_hit_only");
    for &n in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("n={}", n), |b| {
            b.iter_batched(
                || {
                    let mut cache = LruBenchmarkCache::new(NonZeroUsize::new(n).unwrap());
                    let mut rng = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..n).map(|_| rng.r#gen()).collect();
                    for &k in &keys {
                        cache.put(k, k);
                    }
                    (cache, keys)
                },
                |(mut cache, keys)| {
                    for k in &keys {
                        let _ = black_box(cache.get(k));
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_get_hit_only_indexed_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexed_shard_get_hit_only");
    for &n in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("n={}", n), |b| {
            b.iter_batched(
                || {
                    let mut cache =
                        IndexedCacheShard::with_capacity(NonZeroUsize::new(n).unwrap());
                    let mut rng = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..n).map(|_| rng.r#gen()).collect();
                    for &k in &keys {
                        cache.insert(k, k);
                    }
                    (cache, keys)
                },
                |(mut cache, keys)| {
                    for k in &keys {
                        let _ = black_box(cache.get(k));
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_insert_existing_non_full_indexed_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexed_shard_insert_existing_non_full");
    for &n in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("n={}", n), |b| {
            b.iter_batched(
                || {
                    let mut cache =
                        IndexedCacheShard::with_capacity(NonZeroUsize::new(n * 2).unwrap());
                    let mut rng = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..n).map(|_| rng.r#gen()).collect();
                    for &k in &keys {
                        cache.insert(k, k);
                    }
                    (cache, keys)
                },
                |(mut cache, keys)| {
                    for k in &keys {
                        cache.insert(*k, *k);
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

// All inserts hit existing keys in a non-full cache — exercises the Entry API
// Occupied branch exclusively. Compared against insert-then-get to show the cost
// of promotion without allocation.
fn bench_insert_existing_non_full_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_insert_existing_non_full");
    for &n in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("n={}", n), |b| {
            b.iter_batched(
                || {
                    // cap = 2*n keeps the cache non-full throughout, so all
                    // re-inserts go through the Entry Occupied branch
                    let mut cache =
                        CacheShard::with_capacity(NonZeroUsize::new(n * 2).unwrap());
                    let mut rng = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..n).map(|_| rng.r#gen()).collect();
                    for &k in &keys {
                        cache.insert(k, k);
                    }
                    (cache, keys)
                },
                |(mut cache, keys)| {
                    for k in &keys {
                        cache.insert(*k, *k);
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(sync, bench_insert_get_sync);
criterion_group!(lru_crate_bench_comp, bench_insert_get_sync_lru_bench);
criterion_group!(
    single_threaded_shard_comp,
    bench_insert_get_sync_single_threaded_shard
);
criterion_group!(shard_get_hit_only, bench_get_hit_only_shard);
criterion_group!(lru_crate_get_hit_only, bench_get_hit_only_lru_crate);
criterion_group!(
    shard_insert_existing_non_full,
    bench_insert_existing_non_full_shard
);

criterion_group!(indexed_shard_comp, bench_insert_get_sync_indexed_shard);
criterion_group!(indexed_shard_get_hit_only, bench_get_hit_only_indexed_shard);
criterion_group!(
    indexed_shard_insert_existing_non_full,
    bench_insert_existing_non_full_indexed_shard
);

criterion_main!(
    sync,
    lru_crate_bench_comp,
    single_threaded_shard_comp,
    shard_get_hit_only,
    lru_crate_get_hit_only,
    shard_insert_existing_non_full,
    indexed_shard_comp,
    indexed_shard_get_hit_only,
    indexed_shard_insert_existing_non_full
);
