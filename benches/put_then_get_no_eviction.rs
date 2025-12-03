use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use dash_cache::core::LruCache;
use rand::{Rng, SeedableRng, rngs::StdRng};

fn bench_insert_get_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_sync");
    for &cap in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(1_000));
        group.bench_function(format!("insert_then_get_cap={}", cap), |b| {
            b.iter_batched(
                || {
                    // setup
                    let cache = LruCache::with_capacity(cap);
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

use dash_cache::core::CacheShard;
fn bench_insert_get_sync_single_threaded_shard(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded_shard");
    for &cap in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(1_000));
        group.bench_function(format!("insert_then_get_cap={}", cap), |b| {
            b.iter_batched(
                || {
                    // setup
                    let cache = CacheShard::with_capacity(cap);
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

criterion_group!(sync, bench_insert_get_sync);
criterion_group!(lru_crate_bench_comp, bench_insert_get_sync_lru_bench);
criterion_group!(
    single_threaded_shard_comp,
    bench_insert_get_sync_single_threaded_shard
);

criterion_main!(sync, lru_crate_bench_comp, single_threaded_shard_comp);
