# Changelog

## [0.2.3]

### Bug fixes

- When a `DashCache` shard's promotion or eviction queue fills up, the `get` that finds it full now takes the shard's write lock and drains both queues early, then applies its own promotion or eviction by key. Previously, promotions and evictions pushed to a full queue were silently dropped.
- `update` and `update_with_ttl` now return `Err(CacheError::KeyNotExist)` for an expired entry instead of updating it in place.
- `SharedVecRef::push_item` now checks `len < cap` in release builds too (`assert!` instead of `debug_assert!`), so an out-of-bounds write panics instead of corrupting memory.

### Internal

- `DashCache` shard routing uses a multiply-shift range reduction on 32 bits from the middle of the key's hash instead of `hash % num_shards`.
- Remove the manual `unsafe impl Send + Sync` for `CacheQueue`; it is now made only of atomics, so both are derived automatically.

## [0.2.2]

### Bug fixes

- A TTL too large to fit in `u64` nanoseconds (for example `Duration::MAX`) now means "never expires". Previously it panicked on overflow in debug builds and was truncated in release builds, which could make the entry expire immediately.
- The default shard count is capped so each shard holds at least 8 entries (and there is at least one shard). Previously a cache smaller than the default shard count rounded every shard up to one entry and held more than the requested capacity. An explicit `with_num_shards` is still used as given.
- `CacheQueue` stores its buffer as `AtomicU32` instead of `u32` behind an `UnsafeCell`, fixing a data race Miri found when two `get`s pushed promotions at the same time.

### Documentation

- `DashCache::checkout` and `CacheEntryGuard` now document the asynchronous write-back and its caveats.

### CI

- Add a Miri job running the unit tests and a targeted `miri_probe` integration test across several seeds.

## [0.2.1]

### Bug fixes

- **`CacheEntryGuard` write-back no longer silent on drop outside a Tokio runtime** — if the guard is dropped on a thread with no active runtime, the write-back now falls back to `futures::executor::block_on` instead of silently discarding the entry.
- Fix silent ignore of user defined hasher.

### Internal

- `DashCache` internal shards now share a single allocated arena. Each shard is given exclusive share of a slice of this slab. The full slab is held in `SharedVec` and each shard is accessed through `SharedVecRef`.
- `dash_cache` module split into focused sub-modules: `slab` (per-shard locking layer), `inner` (shard routing), `cache` (`DashCache` public API), and `builder` (`DashCacheBuilder`).
- `compute_shard` now hashes via the stored `BuildHasher` instance (`self.hasher.build_hasher()`) rather than a default/static call, ensuring the configured hasher seed is always respected.
- `eviction_keys` deduplication changed from `Vec::contains` (O(n²)) to `HashSet` (O(n)).
- Default shard count reduced from `num_cpus * 8` to `num_cpus * 4`.
- Remove some internal dead code.

### CI

- Add Github Action runners.
- Add thread and memory santizer tests to ensure memory safety with new slab backend implementation.

## [0.2.0]

### Breaking changes

- **`DashCache` constructors removed** — all construction now goes through `DashCacheBuilder::new(cap).build()`
- **`DashCacheBuilder` method signatures changed** — builder methods now take `&mut self` and return `&mut Self`; `build()` now takes `&self`
- **`DashCacheBuilder::with_hasher`** — hasher bound tightened to `BuildHasher + Clone + Send + Sync`
- **`CacheShard` and `LruCache` removed** — use `SlabShard` and `DashCache`
- **`CacheStats` has a new `expirations: usize` field**

### New APIs

#### TTL

Opt-in TTL with lazy eviction on `get`. No background GC.

- `DashCacheBuilder::with_default_ttl(Duration)` — cache-wide default TTL
- `DashCache::insert_with_ttl(key, value, Duration)`
- `DashCache::update_with_ttl(&key, value, Duration)`
- `SlabShardBuilder::with_default_ttl(Duration)`
- `SlabShard::insert_with_ttl(key, value, Duration)`
- `SlabShard::update_with_ttl(&key, value, Duration)`

#### `DashCache::checkout` / `CacheEntryGuard`

`checkout` returns a `CacheEntryGuard` (implements `Deref`/`DerefMut`) holding a cloned entry. On drop the value is written back to the cache, preserving the original TTL.

#### `DashCacheBuilder` new options

- `with_promotion_queue_size(n)` — internal promotion queue size per shard
- `with_eviction_queue_size(n)` — internal eviction queue size per shard

#### `SlabShardBuilder`

New builder for `SlabShard`. Supports `.with_default_ttl()` and `.with_hasher()`.

#### New `SlabShard` methods

`contains`, `len`, `update`, `update_with_ttl`, `evict`, `insert_with_ttl`, `statistics`

#### Crate root exports

`DashCacheBuilder`, `CacheEntryGuard`, and `CacheStats` are now re-exported from the crate root.
