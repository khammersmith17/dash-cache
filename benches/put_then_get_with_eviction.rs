use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use dash_cache::core::LruCache;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::num::NonZeroUsize;

fn bench_insert_with_eviction_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_sync");
    for &cap in &[1_00usize, 1_000] {
        group.throughput(Throughput::Elements(10_000));
        group.bench_function(format!("insert_with_eviction={}", cap), |b| {
            b.iter_batched(
                || {
                    // setup
                    let cache = LruCache::with_capacity(NonZeroUsize::new(cap).unwrap());
                    let mut range = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..10_000).map(|_| range.r#gen()).collect();
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

fn bench_insert_with_eviction_crate_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_crate_comp");
    for &cap in &[1_00usize, 1_000] {
        group.throughput(Throughput::Elements(10_000));
        group.bench_function(format!("insert_with_eviction={}", cap), |b| {
            b.iter_batched(
                || {
                    // setup
                    let cache = LruBenchmarkCache::new(std::num::NonZeroUsize::new(cap).unwrap());
                    let mut range = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..10_000).map(|_| range.r#gen()).collect();
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

use dash_cache::core::CacheShard;

fn bench_insert_with_eviction_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded_shard");
    for &cap in &[1_00usize, 1_000] {
        group.throughput(Throughput::Elements(10_000));
        group.bench_function(format!("insert_with_eviction={}", cap), |b| {
            b.iter_batched(
                || {
                    // setup
                    let cache = CacheShard::with_capacity(NonZeroUsize::new(cap).unwrap());
                    let mut range = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..10_000).map(|_| range.r#gen()).collect();
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

// Inserts only — no gets. Isolates the eviction + memory reuse path in CacheShard
// against lru crate's allocation strategy under the same eviction pressure.
fn bench_insert_only_eviction_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_insert_only_eviction");
    for &cap in &[100usize, 1_000] {
        group.throughput(Throughput::Elements(10_000));
        group.bench_function(format!("cap={}", cap), |b| {
            b.iter_batched(
                || {
                    let cache = CacheShard::with_capacity(NonZeroUsize::new(cap).unwrap());
                    let mut rng = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..10_000).map(|_| rng.r#gen()).collect();
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

fn bench_insert_only_eviction_lru_crate(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_crate_insert_only_eviction");
    for &cap in &[100usize, 1_000] {
        group.throughput(Throughput::Elements(10_000));
        group.bench_function(format!("cap={}", cap), |b| {
            b.iter_batched(
                || {
                    let cache = LruBenchmarkCache::new(NonZeroUsize::new(cap).unwrap());
                    let mut rng = StdRng::seed_from_u64(42);
                    let keys: Vec<u64> = (0..10_000).map(|_| rng.r#gen()).collect();
                    (cache, keys)
                },
                |(mut cache, keys)| {
                    for k in &keys {
                        cache.put(*k, *k);
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

// All inserts hit existing keys on a full cache — exercises the get + update_cache_entry
// path without any eviction or allocation.
fn bench_insert_existing_key_full_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("shard_insert_existing_full");
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
                        cache.insert(*k, *k);
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(sync_eviction, bench_insert_with_eviction_sync);
criterion_group!(
    lru_crate_bench_comp_eviction,
    bench_insert_with_eviction_crate_bench
);
criterion_group!(
    single_threaded_shard_comp_eviction,
    bench_insert_with_eviction_shard
);
criterion_group!(shard_insert_only_eviction, bench_insert_only_eviction_shard);
criterion_group!(
    lru_crate_insert_only_eviction,
    bench_insert_only_eviction_lru_crate
);
criterion_group!(
    shard_insert_existing_full,
    bench_insert_existing_key_full_shard
);

criterion_main!(
    sync_eviction,
    lru_crate_bench_comp_eviction,
    single_threaded_shard_comp_eviction,
    shard_insert_only_eviction,
    lru_crate_insert_only_eviction,
    shard_insert_existing_full
);
