# dash_cache design

The design of the LRU cache implementation in this crate breaks down into two major components, the single threaded shard `SlabShard` and the concurrent implementation `DashCache`. The design of the single threaded shard informs the design of the concurrent implementation.

## Core design

### SlabShard
As the name implies, this is implemented using the concept of a slab. It looks like a rather traditional LRU cache with a `HashMap` for storing keys and a doubly linked list to bookkeep the least recently used keys. This implementation is slightly more unique in the sense that all actual entries are stored in a contiguous slab, and list pointers are implemented using offsets into this contiguous slab.

There are a few benefits to this approach. The first being safety. As long as the offset bounds stay correct, there is less risk of invalid pointers, and less pointer handling. The alternatives are either safe pointers, using `Rc<RefCell<Entry<K, V>>>` or `Weak<RefCell<Entry<K, V>>>`, which carries the runtime overhead of ensuring exclusive access upon interior mutability. An unsafe option is `NonNull<Entry<K, V>>`. With the contiguous slab approach, these pointers are all unsigned integers, and the only unsafe code is to avoid bounds checks, since the offsets are guaranteed valid by the list invariants.


Another benefit is cache locality. Using a pointer based method, each entry is scattered across the heap. Whereas with a contiguous slab, there is much better locality between entries on the heap. This benefit decreases as the size of the cache grows and when the cache is very small, but for a moderate sized cache, the cache locality provides real performance benefit. This can be seen through the benchmarks provided.

### DashCache
The idea behind the design of dash cache is to limit the amount of concurrent contention, while remaining mostly safe. This is achieved by sharding keys across many different slabs, so accessing a single key only locks one shard, rather than the entire cache.

This does come with the trade off that the LRU semantics only apply at the shard level and not globally across the cache, and this implementation benefits from a rather uniform distribution of key hash values. The shard is chosen from 32 bits in the middle of the key's hash using a multiply-shift reduction rather than a modulo.

Promotion and eviction as a result of ttl (discussed below) are also handled in a non conventional way. All read operations (`get`) would typically require a write or exclusive lock in order to update the cache state. The cache state needs to be updated to account for the promotion of a key. Intuitively, a read should not require a write lock, and read is the hot path optimization a cache solves for. Given this, we should look to lower the cost of a read. Acquiring a write lock for a read style operation limits the concurrency of reads, and adds some overhead to consider the bookkeeping. A ttl driven eviction is defined as an eviction that is caused by an entry expiring. Evictions that occur during an insert are not queued, as this occurs during an operation that has a write lock, that being a write.

To account for that, promotions and evictions are queued during read operations, and fire on write operations. They are queued using the `CacheQueue` data structure and are implemented as a statically sized buffer. If a queue fills up before the next write, the `get` that found it full releases its read lock, takes the shard's write lock and drains both queues early, then applies its own promotion or eviction directly. Because other writers may have changed the shard while no lock was held, that last operation looks the key up again rather than reusing the queued entry index, and an expired entry is only evicted if it is still expired. This keeps the buffers statically sized without dropping any bookkeeping. Both buffer sizes are configurable. There would need to be `buffer_size` back to back reads to the same shard, with no write in between, for a queue to fill. Every time a write lock is held, both fire. This makes writes more expensive, but the conditions that surround a write to cache typically implies some increased cost anyways. Also, firing all these operations at once limits CPU cache invalidation between mutations, and fires all of them when "hot". This amortizes the cost of internal bookkeeping.

### Time to Live (TTL)
TTL is lazily evaluated. On an insert, the time in which the entry will expire is computed as a monotonic unsigned nanosecond u64. On a get, the expires time is evaluated for liveness. If the entry is expired, it is queued for eviction using the protocol described above. Lazy evaluation here removes the need for a background garbage collection loop, lowering the overall overhead. This does mean that the cache is eventually consistent internally, but will be observed as strongly consistent. A user will never receive an expired entry from the cache, but expired entries may be present in the cache. Expired entries are also treated as absent by `contains` and `update*`.


## Other design considerations
### CacheEntryGuard
Checking out an entry to mutate would require providing long lived exclusive access to the shard by the task thread that acquires it. This would prevent any other task thread from making progress. A `get_mut` would limit concurrency, which is something this design aims to avoid.

This informs the design of `CacheEntryGuard`, which provides similar semantics to a held mutex, but does not force exclusive access to the cache. Rather, that entry is removed from the cache while the guard is held. `CacheEntryGuard` derefs to the value of the entry for any mutation that might be desired and carries a copy of the cache (`DashCache` itself is a type wrapper around an `Arc` pointer to the core type). The `Drop` implementation writes the entry back into the cache using the copy of the pointer held. This creates the illusion of having a mutable copy while still maintaining concurrent properties.

### Stats
Both the single threaded and concurrent types track the statistics around cache performance, tracking hits, misses, evictions, and expirations. Evictions here represent an involuntary eviction: evicting the least recently used entry in a saturated cache.

The way these differ is the concurrent stats type bookkeeps using atomic unsigned integers, whereas the single threaded slab on its own will use a `Cell` implementation. During benchmarking, using the atomics for both the single threaded implementation and the concurrent implementation resulted in poor performance. The Cell is required because not all stats mutating operations have mutable cache access, for example `get` in a concurrent setting makes no mutations, but does need to mutate the stats.
