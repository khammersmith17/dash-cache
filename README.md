![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/dash_cache) 
![Crates.io Version](https://img.shields.io/crates/v/dash_cache)


# dash-cache
This package is an efficient LRU cache implementation. This implementation is different than a typical LRU cache implementation in that all entries are stored in a contiguous buffer, and the list is tracked using offsets into this buffer, rather than pointers. The introduces some CPU cache performance benefit, as observed in the benchmark numbers below.

The thread safe implementation is dubbed DashCache, as an homage to DashMap. As such, the internal structure is a sharded LRU Cache for performant concurrent access. Shard count can be defined by the user, or defaults to the number of cpu cores available on the machine. Given the sharded nature, each key value pair priority is local to the cache shard and not a total least recently used ordering. This is of course not a characteristic of the single threaded version.

Time to live is opt in, and can be assigned per unique entry. There is no garbage collection TTL loop. TTL is lazily evaluated on a `get`, and evicted when the entry is expired. This implies that the internal state is eventually consistent which is true, but all observed state is stronly consistent.

The concurrent implementation only performs promotions and time based evictions on a write operateion(`insert_*` or `update_*`). In this way `get` does not incur as much lock contention.

This implementation is designed to be _fast_, and leans toward making cache hits fast, as this is the desired hot path when using a cache. The performance benefit for a single threaded implementation levels out at large cache sizes with higher eviction rates. Otherwise, this implementation performs very well.


## Benchmarks

All times are mean latency **per operation**. Benchmarks run using [Criterion](https://github.com/bheisler/criterion.rs). The benchmarks themselves can be found in /benches.

### Insert + Get — no eviction (1 000 inserts + 1 000 gets)

| cap    | lru crate   | SlabShard       |
|--------|-------------|-----------------|
| 1 000  | 35.4 ns/op  | **17.5 ns/op**  |
| 10 000 | 64.6 ns/op  | **43.7 ns/op**  |

### Insert + Get — with eviction (10 000 inserts + 10 000 gets)

| cap    | lru crate   | SlabShard       |
|--------|-------------|-----------------|
| 1 000  | 27.7 ns/op  | **25.0 ns/op**  |
| 10 000 | 33.8 ns/op  | **20.0 ns/op**  |

### Get hit only — warm cache (n gets)

| n      | lru crate   | SlabShard       |
|--------|-------------|-----------------|
| 1 000  | 24.8 ns/op  | **13.8 ns/op**  |
| 10 000 | 23.7 ns/op  | **23.3 ns/op**  |

### Insert existing key — non-full cache (n inserts)

| n      | lru crate   | SlabShard       |
|--------|-------------|-----------------|
| 1 000  | 25.2 ns/op  | **23.0 ns/op**  |
| 10 000 | **27.6 ns/op**  | 29.7 ns/op   |

### Insert only — eviction pressure (100 000 inserts)

| cap    | lru crate      | SlabShard       |
|--------|----------------|-----------------|
| 1 000  | **27.2 ns/op** | 31.4 ns/op      |
| 10 000 | 26.4 ns/op     | **24.4 ns/op**  |

### Insert existing key — full cache (n inserts)

| n      | lru crate   | SlabShard       |
|--------|-------------|-----------------|
| 1 000  | 22.2 ns/op  | **11.4 ns/op**  |
| 10 000 | 22.2 ns/op  | **19.6 ns/op**  |

## DashCache — concurrent benchmarks

All times are mean latency **per operation**. Benchmarks run on a Tokio multi-thread runtime.

### Sequential insert + get (single async task)

cap inserts followed by cap gets, no concurrency.

| cap     | ns/op |
|---------|-------|
| 10 000  | 44.7  |
| 100 000 | 48.7  |

### Concurrent inserts (N tasks, disjoint key ranges)

Each task inserts its own slice of keys with no key overlap.

| items   | tasks | ns/op |
|---------|-------|-------|
| 50 000  | 4     | 60.0  |
| 100 000 | 8     | 83.5  |
| 200 000 | 16    | 92.2  |

### Mixed R/W — 80% get / 20% insert, random keys (single task)

200 000 ops, cap=100 000, cache warmed to 50% before measurement.

| ops     | ns/op |
|---------|-------|
| 200 000 | 79.5  |

### Concurrent mixed R/W isolated (8 tasks, fresh cache per iteration)

8 tasks each performing 25 000 ops (80% get / 20% insert) against a shared cache.
Cache warmed to 50% capacity before tasks spawn.

| tasks | ops/task | total ops | ns/op |
|-------|----------|-----------|-------|
| 8     | 25 000   | 200 000   | 109.1 |

### Hot key contention (8 tasks)

8 tasks each performing 25 000 ops. Every 10th op is a get on a single shared hot key;
remaining ops are random gets (75%) and inserts (25%).

| tasks | ops/task | total ops | ns/op |
|-------|----------|-----------|-------|
| 8     | 25 000   | 200 000   | 80.7  |

### Eviction pressure (single task, tiny cap)

50 000 sequential inserts into a cap=1 000 cache (constant eviction).
Every 3rd op is also a get on a recently inserted key.

| ops    | ns/op |
|--------|-------|
| 50 000 | 61.1  |

### Contains vs Get — warm cache, no contention (single task)

50 000 ops on a fully warm cache. `contains` takes a read lock with no promotion;
`get` takes a write lock and promotes.

| operation | ns/op |
|-----------|-------|
| contains  | 32.0  |
| get       | 35.4  |

### Sequential update (single task)

50 000 updates on existing keys in a fully warm cache (write lock + promotion + value overwrite).

| ops    | ns/op |
|--------|-------|
| 50 000 | 54.0  |
