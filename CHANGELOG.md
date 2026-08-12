# Changelog

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
