![Crates.io Downloads (recent)](https://img.shields.io/crates/dr/dash_cache) 
![Crates.io Version](https://img.shields.io/crates/v/dash_cache)


# dash-cache
This repo is an attempt at an efficient LRU cache implementation. There are three implementations I am experimenting with, one geared toward single threaded usage, and two geared toward performant thread safe usage.

All implementations use an internal linked list for priority, and a HashMap to track existence in the cache.
The implementations in LruCache, and CacheShard are very similar, diverging in the type of pointer used. LruCache uses "safe" pointers, ie Rc<RefCell<T>>, where are the CacheShard implementation uses NonNullPointers, which introduces unsafety. The invariants that define safety within this implementation are documented in the code, and heavily asserted in debug builds.

The other implementation is the most performant. SlabShard uses a contiguous allocation for the cache nodes, and maintains the least recently used list using index pointers into this slab. The HashMap holds keys and entry indexes. There is some unsafety in this implementations for performance reasons when accessing entries in the allocated slab, but this is because the invariants defined guarantee that the indexes will be valid.

The thread safe implementation is dubbed DashCache, as an homage to DashMap. As such, the internal structure is a sharded LRU Cache for performant concurrent access. Shard count can be defined by the user, or defaults to the number of cpu cores available on the machine. Given the sharded nature, each key value pair priority is local to the cache shard and not a total least recently used ordering. This is of course not a characteristic of the single threaded version.

The internal implementation for the shards in DashCache, is the SlabShard

There are three single-threaded implementations:
- **LruCache** — safe, single-threaded, uses `Rc<RefCell<>>` for linked list nodes
- **CacheShard** — unsafe, uses `NonNull` raw pointers and `Box`-rallocated nodes; the internal shard type for `DashCache`
- **SlabShard** — unsafe, uses a contiguous slab (`Vec`) with `u32` index pointers for improved cache locality

## Benchmarks

All times are mean latency **per operation**. Benchmarks run in release mode via [Criterion](https://github.com/bheisler/criterion.rs). The benchmarks themselves can be found in /benches.

### Insert + Get — no eviction (1 000 inserts + 1 000 gets)

| cap    | lru crate   | SlabShard       |
|--------|-------------|-----------------|
| 1 000  | 16.8 ns/op  | **8.3 ns/op**   |
| 10 000 | 33.8 ns/op  | **22.6 ns/op**  |

### Insert + Get — with eviction (10 000 inserts + 10 000 gets)

| cap   | lru crate   | SlabShard       |
|-------|-------------|-----------------|
| 100   | **19.0 ns/op** | 21.4 ns/op   |
| 1 000 | 12.8 ns/op  | **12.4 ns/op**  |

### Get hit only — warm cache (n gets)

| n      | lru crate   | SlabShard       |
|--------|-------------|-----------------|
| 1 000  | 21.7 ns/op  | **11.2 ns/op**  |
| 10 000 | 22.3 ns/op  | **19.0 ns/op**  |

### Insert existing key — non-full cache (n inserts)

| n      | lru crate   | SlabShard       |
|--------|-------------|-----------------|
| 1 000  | 25.9 ns/op  | **18.3 ns/op**  |
| 10 000 | 27.1 ns/op  | **23.6 ns/op**  |

### Insert only — eviction pressure (10 000 inserts)

| cap   | lru crate   | SlabShard       |
|-------|-------------|-----------------|
| 100   | **30.1 ns/op** | 32.4 ns/op   |
| 1 000 | 23.9 ns/op  | **21.4 ns/op**  |

### Insert existing key — full cache (n inserts)

| n      | lru crate   | SlabShard       |
|--------|-------------|-----------------|
| 1 000  | 21.9 ns/op  | **10.9 ns/op**  |
| 10 000 | 23.1 ns/op  | **20.1 ns/op**  |
