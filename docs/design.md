# dash_cache design

The design of the LRU cache implementation in this crate breaks down into two major components, the single threaded shard `SlabShard` and the concurrent implementation `DashCache`. The design of the single threaded shard informs the design of the concurrent implementation.

## Core design

### SlabShard
As the name implies, this is implemented using the concept of a slab. It looks like a rather traditional LRU cache with a `HashMap` for storing keys and a doubly linked list to bookkeep the least recently used keys. This implementation is slightly more unique in the sense that all actual entries are stored in a contiguous slab, and list pointers are implemented using offsets into this contiguous slab.

There are a few benefits to this approach. The first being safety. As long as the offset bounds stay correct, there is less risk of invalid pointers, and less pointer handling. The alternatives are either safe pointers, using `Rc<RefCell<Entry<K, V>>>` or `Rc<WeakRef<Entry<K, V>>>`, which carries the runtime overhead of ensuring exclusive access upon interior mutability. An unsafe option is `NonNullPointer<Entry<K, V>>`. With the contiguous slab approach, these pointers are all unsigned integers, and the only unsafe code is to avoid bounds checks, since the pointers can be ensured to be validated given the invariant properties.


Another benefit is cache locality. Using a pointer based method, each entry is scattered across the heap. Whereas with a contiguous slab, there is much better locality between entries on the heap. This benefit decreases as the size of the cache grows and when the cache is very small, but for a moderate sized cache, the cache locality provides real performance benefit. This can be seen through the benchmarks provided.

### DashCache
The idea behind the design of dash cache is to limit the amount of concurrent contention, while remaining mostly safe. This is acheived by sharding keys across many different slabs, so accessing a single key only locks one shard, rather than the entire cache.

This does come with the trade off that the LRU semantics only apply at the shard level and not globally across the cache, and this implemention benefits with a rather uniform distribution of hash value of the keys.

Promotion and eviction as a result of ttl (discussed below) are also handled in a non conventional way. All read operations (`get`) would typically require a write or exclusive lock in order to update the cache state. The cache stateneeds to be update to consider the promotion of a key. Intuitively, a read should not require a write lock, and read is the hot path optimization a cache solves for. Given this, we should look to lower the cost of a read. Acquiring a write lock for a read style operation limits the concurrency of reads, and adds some overhead to consider the bookkeeping. A ttl driven eviction is defined as an eviction that is caused by an entry expiring. Evictions that occur during an insert are not queued, as this occurs during an operation that has a write lock, that being a write.

To account for that, promotions and evictions are queued during read operations, and fire on write operations. They are queued using the `CacheQueue` data structure and are implemented as a statically sized buffer. There is a trade off with this implementation in that if the number of queued operations grows beyond the size of the buffer, then they are dropped and ignored. But the static size and the ability to amortize the more expensive operations outweighs the risk. Both of these buffer sizes are configurable. There would need to be `buffer_size` back to back reads to the same shard in order to present this condition. Every time a write lock is held, both fire. This makes writes more expensive, but the conditions that surround a write to cache typically implies some increased cost anyways. Also, firing all these operations at once limits CPU cache invalidation between mutatations, and fires all of them when "hot". This amortizes the cost of internal bookkeeping.

### Time to Live (TTL)
TTL is lazily evaluated, on an insert the time in which the entry will expire is computed as a monotonic unsigned nanosecond u64. On a get, the expires time is evaluated for liveness. If the entry is expired, it is queued for eviction using the protocol described above. Lazy evaluation here removes the need for a background garbage collection loop, lowering the overall overhead. This does mean that the cache is eventually consistent internally, but will be observed as strongly consistent. A user will never receive an expired entry from the cache, but expired entries may be present in the cache.


## Other design considerations.
### CacheEntryGuard
Checkouting an entry to mutate would require providing long lived exclusive access to the shard by the task thread that acquires it. This would prevent any other task thread from making progress. A 'get_mut` would limit concurrency, which is something this design aims to avoid.

This informs the design of `CacheEntryGuard`, which provides similar semantics to a held mutex, but does not force exclusive access to the cache. Rather, that entry is evicted from the cache. `CacheEntryGuard` derefs to the value of the entry for any mutation that might be desired and carries a copy of the cache (`DashCache` itself is a type wrapper around an `Arc` pointer to the core type). The `Drop` implementation writes the entry back into the cache using the copy of the pointer held. This creates the allusion of having a mutable copy while still maintaining concurrent properties.

### Stats
Both the single threaded and concurrent types track the statistics around cache performance, tracking hits, misses, evictions, and expirations. Evictions here represent a non voluntary exviction, when the evicting the least recently used entry in a saturated cache.

They way these differ is the concurrent stats type bookkeeps using atomic unsigned integers, whereas the single threaded slab on its own will use a `Cell` implementation. During benchmarking, using the atomics for both the single threaded implementation and the concurrent implementation resulted in poor performance. The Cell is required because not all stats mutating operations have mutatble cache access, for example `get` in a concurrent setting makes no mutations, but does need to mutate the stats.
