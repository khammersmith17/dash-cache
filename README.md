# rust_lru_cache
This repo is an attempt at an efficient LRU cache implementation. There are three implementations I am experimenting with, one geared toward single threaded usage, and two geared toward performant thread safe usage.

All implementations use an internal linked list for priority, and a HashMap to track existence in the cache.
The implementations in LruCache, and CacheShard are very similar, diverging in the type of pointer used. LruCache uses "safe" pointers, ie Rc<RefCell<T>>, where are the CacheShard implementation uses NonNullPointers, which introduces unsafety. The invariants that define safety within this implementation are documented in the code, and heavily asserted in debug builds.

The other implementation is the most performant. SlabShard uses a contiguous allocation for the cache nodes, and maintains the least recently used list using index pointers into this slab. The HashMap holds keys and entry indexes. There is some unsafety in this implementations for performance reasons when accessing entries in the allocated slab, but this is because the invariants defined guarantee that the indexes will be valid.

The thread safe implementation is dubbed DashCache, as an homage to DashMap. As such, the internal structure is a sharded LRU Cache for performant concurrent access. Shard count can be defined by the user, or defaults to the number of cpu cores available on the machine. Given the sharded nature, each key value pair priority is local to the cache shard and not a total least recently used ordering. This is of course not a characteristic of the single threaded version.

The internal implementation for the shards in DashCache, is the SlabShard

There are three single-threaded implementations:
- **LruCache** — safe, single-threaded, uses `Rc<RefCell<>>` for linked list nodes
- **CacheShard** — unsafe, uses `NonNull` raw pointers and `Box`-heap-allocated nodes; the internal shard type for `DashCache`
- **SlabShard** — unsafe, uses a contiguous slab (`Vec`) with `u32` index pointers for improved cache locality

## Benchmarks

All times are mean latency for the full batch of operations. Benchmarks run in release mode via [Criterion](https://github.com/bheisler/criterion.rs). If you would like to see the raw benchmark reports, please reach out.

### Insert + Get (no eviction, 1 000 ops)

| cap    | LruCache | lru crate | CacheShard | SlabShard |
|--------|----------|-----------|------------|-------------------|
| 1 000  | 97.4 µs  | 44.2 µs   | 55.1 µs    | **21.4 µs**       |
| 10 000 | 112.1 µs | 116.9 µs  | 77.3 µs    | **67.8 µs**       |

### Insert + Get (with eviction, 10 000 ops)

| cap   | LruCache | lru crate | CacheShard | SlabShard |
|-------|----------|-----------|------------|-------------------|
| 100   | 738.1 µs | 436.3 µs  | 477.4 µs   | 499.1 µs          |
| 1 000 | 680.0 µs | 318.2 µs  | 338.1 µs   | **294.6 µs**      |

### Get hit only (warm cache, n ops)

| n      | lru crate | CacheShard | SlabShard |
|--------|-----------|------------|-------------------|
| 1 000  | 27.7 µs   | 32.7 µs    | **9.8 µs**        |
| 10 000 | 335.9 µs  | 332.0 µs   | **207.1 µs**      |

### Insert existing key — non-full cache (n ops)

| n      | CacheShard  | SlabShard |
|--------|-------------|-------------------|
| 1 000  | **38.0 µs** | 41.8 µs           |
| 10 000 | 344.4 µs    | **295.2 µs**      |

### Insert only — eviction pressure (10 000 ops)

| cap   | lru crate | CacheShard | SlabShard |
|-------|-----------|------------|-------------------|
| 100   | 385.2 µs  | 377.0 µs   | **340.3 µs**      |
| 1 000 | 286.3 µs  | 282.5 µs   | **229.8 µs**      |

### Insert existing key — full cache (n ops)

| n      | CacheShard | SlabShard |
|--------|------------|-------------------|
| 1 000  | 33.1 µs    | **15.9 µs**       |
| 10 000 | 311.5 µs   | **272.9 µs**      |
