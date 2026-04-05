use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, black_box, criterion_group, criterion_main,
};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::num::NonZeroUsize;
use tokio::runtime::Runtime;

use dash_cache::dash_cache::DashCache;

fn bench_dashcache_async_sequential(rt: &Runtime, c: &mut Criterion) {
    let mut group = c.benchmark_group("dashcache_async_sequential");
    group.sampling_mode(SamplingMode::Flat);
    for &cap in &[10_000usize, 100_000] {
        group.throughput(Throughput::Elements(cap as u64 * 2));
        group.bench_with_input(BenchmarkId::new("insert_then_get", cap), &cap, |b, &cap| {
            let cache = DashCache::<u64, u64>::new(NonZeroUsize::new(cap).unwrap());
            let range = StdRng::seed_from_u64(7);
            b.to_async(rt).iter(|| {
                let mut rng = range.clone();
                let cache_c = cache.clone();
                let generated_range: Vec<u64> = (0..cap as usize)
                    .map(|_| rng.gen_range(0..(cap as usize)) as u64)
                    .collect();
                async move {
                    cache_c.drain().await;
                    for i in 0..(cap as usize) {
                        cache_c.insert(i as u64, i as u64).await;
                    }
                    for i in 0..(cap as usize) {
                        let k = generated_range[i];
                        let _ = black_box(cache_c.get(&k).await);
                    }
                }
            });
        });
    }
    group.finish();
}

fn bench_dashcache_concurrent_inserts(rt: &Runtime, c: &mut Criterion) {
    let mut group = c.benchmark_group("dashcache_concurrent_inserts");
    group.sampling_mode(SamplingMode::Flat);
    // (total_items, tasks)
    for &(n, tasks) in &[(50_000usize, 4usize), (100_000, 8), (200_000, 16)] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{n}_items_{tasks}_tasks")),
            &(n, tasks),
            |b, &(n, tasks)| {
                b.to_async(rt).iter(|| async move {
                    let cache =
                        DashCache::<u64, u64>::new(NonZeroUsize::new(n.max(10_000)).unwrap());
                    let per_task = (n as usize + tasks - 1) / tasks;
                    let mut handles = Vec::with_capacity(tasks);
                    for t in 0..tasks {
                        let start = t * per_task;
                        let end = ((t + 1) * per_task).min(n as usize);
                        let cache_cl = cache.clone();
                        handles.push(tokio::spawn(async move {
                            for i in start..end {
                                cache_cl.insert(i as u64, i as u64).await;
                            }
                        }));
                    }
                    for h in handles {
                        let _ = h.await;
                    }
                    black_box(());
                });
            },
        );
    }
    group.finish();
}

fn bench_dashcache_mixed_rw(rt: &Runtime, c: &mut Criterion) {
    // 80/20 read/write with random keys
    let mut group = c.benchmark_group("dashcache_mixed_rw");
    group.sampling_mode(SamplingMode::Flat);
    let cap = 100_000usize;
    let ops = 200_000usize;
    group.throughput(Throughput::Elements(ops as u64));
    group.bench_function("80r_20w_random", |b| {
        let cache = DashCache::<u64, u64>::new(NonZeroUsize::new(cap).unwrap());
        // warm up a bit
        let warm_rt = rt;
        warm_rt.block_on(async {
            for i in 0..(cap as usize / 2) {
                cache.insert(i as u64, i as u64).await;
            }
        });
        let range = StdRng::seed_from_u64(1337);
        b.to_async(rt).iter(|| {
            let mut rng = range.clone();
            let cache_c = cache.clone();
            async move {
                for _ in 0..ops {
                    let p: u8 = rng.gen_range(0..100);
                    let k = rng.gen_range(0..(cap as usize)) as u64;
                    if p < 80 {
                        let _ = black_box(cache_c.get(&k).await);
                    } else {
                        cache_c.insert(k, k).await;
                    }
                }
            }
        });
    });
    group.finish();
}

fn bench_dashcache_hot_key_contention(rt: &Runtime, c: &mut Criterion) {
    // Many tasks hammer a single key amid random access (measures lock contention)
    let mut group = c.benchmark_group("dashcache_hot_key_contention");
    group.sampling_mode(SamplingMode::Flat);
    let cap = 50_000usize;
    let tasks = 8usize;
    let ops_per_task = 25_000usize;

    group.throughput(Throughput::Elements((tasks * ops_per_task) as u64));
    group.bench_function(format!("hot_key_tasks_{tasks}_ops_{ops_per_task}"), |b| {
        b.to_async(rt).iter(|| async move {
            let cache = DashCache::<u64, u64>::new(NonZeroUsize::new(cap).unwrap());
            cache.insert(0, 0).await; // hot key
            let mut handles = Vec::with_capacity(tasks);
            for t in 0..tasks {
                let cache_cl = cache.clone();
                handles.push(tokio::spawn(async move {
                    let mut rng = StdRng::seed_from_u64(2024 + t as u64);
                    for i in 0..ops_per_task {
                        if i % 10 == 0 {
                            let _ = black_box(cache_cl.get(&0).await); // hot key
                        } else {
                            let k = rng.gen_range(1..50_000) as u64;
                            if i % 4 == 0 {
                                cache_cl.insert(k, k).await;
                            } else {
                                let _ = black_box(cache_cl.get(&k).await);
                            }
                        }
                    }
                }));
            }
            for h in handles {
                let _ = h.await;
            }
            black_box(());
        });
    });
    group.finish();
}

fn bench_dashcache_eviction_pressure(rt: &Runtime, c: &mut Criterion) {
    // capacity tiny, churn keys to stress eviction
    let mut group = c.benchmark_group("dashcache_eviction_pressure");
    group.sampling_mode(SamplingMode::Flat);
    let cap = 1_000usize;
    let ops = 50_000usize;
    group.throughput(Throughput::Elements(ops as u64));
    group.bench_function("churn_inserts", |b| {
        b.to_async(rt).iter(|| async move {
            let cache = DashCache::<u64, u64>::new(NonZeroUsize::new(cap).unwrap());
            for i in 0..ops {
                cache.insert(i as u64, i as u64).await;
                if i % 3 == 0 {
                    let _ = black_box(cache.get(&(i as u64 / 2)).await);
                }
            }
        });
    });
    group.finish();
}

// Contains (read lock) vs get (write lock) on a warm cache with no contention.
// Surfaces the cost difference between a pure read-lock and a write-lock + promotion.
fn bench_dashcache_contains_vs_get(rt: &Runtime, c: &mut Criterion) {
    let mut group = c.benchmark_group("dashcache_contains_vs_get");
    group.sampling_mode(SamplingMode::Flat);
    let n = 50_000usize;
    group.throughput(Throughput::Elements(n as u64));

    let cache = DashCache::<u64, u64>::new(NonZeroUsize::new(n).unwrap());
    rt.block_on(async {
        for i in 0..n {
            cache.insert(i as u64, i as u64).await;
        }
    });
    let keys: Vec<u64> = (0..n as u64).collect();

    group.bench_function("contains", |b| {
        let cache_c = cache.clone();
        let keys_c = keys.clone();
        b.to_async(rt).iter(|| {
            let cache_c = cache_c.clone();
            let keys_c = keys_c.clone();
            async move {
                for k in &keys_c {
                    let _ = black_box(cache_c.contains(k).await);
                }
            }
        });
    });

    group.bench_function("get", |b| {
        let cache_c = cache.clone();
        let keys_c = keys.clone();
        b.to_async(rt).iter(|| {
            let cache_c = cache_c.clone();
            let keys_c = keys_c.clone();
            async move {
                for k in &keys_c {
                    let _ = black_box(cache_c.get(k).await);
                }
            }
        });
    });

    group.finish();
}

// Update path — key must already exist; measures promote + value overwrite under a write lock.
fn bench_dashcache_update(rt: &Runtime, c: &mut Criterion) {
    let mut group = c.benchmark_group("dashcache_update");
    group.sampling_mode(SamplingMode::Flat);
    let n = 50_000usize;
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("sequential_update", |b| {
        let cache = DashCache::<u64, u64>::new(NonZeroUsize::new(n).unwrap());
        rt.block_on(async {
            for i in 0..n {
                cache.insert(i as u64, i as u64).await;
            }
        });
        b.to_async(rt).iter(|| {
            let cache_c = cache.clone();
            async move {
                for i in 0..n {
                    let _ = cache_c.update(&(i as u64), i as u64).await;
                }
            }
        });
    });
    group.finish();
}

// Concurrent mixed R/W with a fresh cache per iteration for stable measurements.
fn bench_dashcache_concurrent_mixed_rw_isolated(rt: &Runtime, c: &mut Criterion) {
    let mut group = c.benchmark_group("dashcache_concurrent_mixed_rw_isolated");
    group.sampling_mode(SamplingMode::Flat);
    let cap = 100_000usize;
    let tasks = 8usize;
    let ops_per_task = 25_000usize;
    group.throughput(Throughput::Elements((tasks * ops_per_task) as u64));
    group.bench_function(format!("tasks_{tasks}_ops_{ops_per_task}"), |b| {
        b.to_async(rt).iter(|| async move {
            let cache = DashCache::<u64, u64>::new(NonZeroUsize::new(cap).unwrap());
            // warm to 50% so reads have something to hit
            for i in 0..(cap / 2) {
                cache.insert(i as u64, i as u64).await;
            }
            let mut handles = Vec::with_capacity(tasks);
            for t in 0..tasks {
                let cache_cl = cache.clone();
                handles.push(tokio::spawn(async move {
                    let mut rng = StdRng::seed_from_u64(99 + t as u64);
                    for _ in 0..ops_per_task {
                        let k = rng.gen_range(0..cap) as u64;
                        if rng.gen_range(0..5u8) == 0 {
                            cache_cl.insert(k, k).await;
                        } else {
                            let _ = black_box(cache_cl.get(&k).await);
                        }
                    }
                }));
            }
            for h in handles {
                let _ = h.await;
            }
        });
    });
    group.finish();
}

fn criterion_benches(c: &mut Criterion) {
    // A single multi-thread Tokio runtime reused across async benches
    let rt = Runtime::new().expect("tokio rt");
    bench_dashcache_async_sequential(&rt, c);
    bench_dashcache_concurrent_inserts(&rt, c);
    bench_dashcache_mixed_rw(&rt, c);
    bench_dashcache_hot_key_contention(&rt, c);
    bench_dashcache_eviction_pressure(&rt, c);
    bench_dashcache_contains_vs_get(&rt, c);
    bench_dashcache_update(&rt, c);
    bench_dashcache_concurrent_mixed_rw_isolated(&rt, c);
}

criterion_group!(name = benches; config = {
    let c = Criterion::default()
        .sample_size(40)             // adjust for stability vs. time
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(5));
    c
}; targets = criterion_benches);
criterion_main!(benches);
