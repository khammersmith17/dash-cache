mod builder;
mod cache;
mod inner;
mod slab;

pub use builder::DashCacheBuilder;
pub use cache::DashCache;

#[cfg(test)]
mod builder_tests {
    use super::*;
    use std::num::NonZeroUsize;

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    #[tokio::test]
    async fn default_build_is_functional() {
        let cache = DashCacheBuilder::<u64, u64>::new(nz(100)).build();
        cache.insert(1, 10).await;
        assert_eq!(cache.get(&1).await, Some(10));
    }

    #[tokio::test]
    async fn with_num_shards_sets_shard_count() {
        let cache = DashCacheBuilder::<u64, u64>::new(nz(100))
            .with_num_shards(nz(8))
            .build();
        assert_eq!(cache.num_shards(), 8);
    }

    #[tokio::test]
    async fn shard_capacity_is_ceil_divided() {
        // 10 total / 3 shards = ceil(3.33) = 4 per shard → total capacity = 12
        let cache = DashCacheBuilder::<u64, u64>::new(nz(10))
            .with_num_shards(nz(3))
            .build();
        // Insert 12 entries — should fit (3 shards * 4 cap each).
        // Keys are routed by hash so we just verify no panic and len <= 12.
        for i in 0..12u64 {
            cache.insert(i, i).await;
        }
        assert!(cache.len().await <= 12);
    }

    #[tokio::test]
    async fn with_num_shards_one_works() {
        let cache = DashCacheBuilder::<u64, u64>::new(nz(4))
            .with_num_shards(nz(1))
            .build();
        assert_eq!(cache.num_shards(), 1);
        for i in 0..4u64 {
            cache.insert(i, i).await;
        }
        assert_eq!(cache.len().await, 4);
    }

    #[tokio::test]
    async fn with_hasher_retypes_builder() {
        // Verifies that with_hasher compiles and produces a working cache.
        let hasher = ahash::RandomState::with_seeds(1, 2, 3, 4);
        let cache = DashCacheBuilder::<u64, u64>::new(nz(100))
            .with_hasher(hasher)
            .build();
        cache.insert(42, 99).await;
        assert_eq!(cache.get(&42).await, Some(99));
    }

    #[tokio::test]
    async fn with_hasher_then_num_shards_composes() {
        let hasher = ahash::RandomState::with_seeds(5, 6, 7, 8);
        let cache = DashCacheBuilder::<u64, u64>::new(nz(40))
            .with_hasher(hasher)
            .with_num_shards(nz(4))
            .build();
        assert_eq!(cache.num_shards(), 4);
        cache.insert(1, 1).await;
        assert_eq!(cache.get(&1).await, Some(1));
    }

    #[tokio::test]
    async fn build_without_num_shards_uses_cpu_count() {
        // No num_shards set → defaults to cpu_count shards.
        let cache = DashCacheBuilder::<u64, u64>::new(nz(100)).build();
        assert!(cache.num_shards() >= 1);
    }

    #[tokio::test]
    async fn builder_cap_equals_one_per_shard() {
        // Edge case: total cap == num_shards → shard_cap = 1 each.
        let n = 4usize;
        let cache = DashCacheBuilder::<u64, u64>::new(nz(n))
            .with_num_shards(nz(n))
            .build();
        assert_eq!(cache.num_shards(), n);
        // Each shard holds exactly 1 entry; inserting n+1 distinct-shard keys
        // must trigger at least one eviction.
        for i in 0..(n * 4) as u64 {
            cache.insert(i, i).await;
        }
        assert!(cache.len().await <= n);
    }
}

#[cfg(test)]
mod dash_cache_tests {
    use super::*;
    use std::num::NonZeroUsize;
    use std::time::Duration;

    // Fixed shard count and capacity for deterministic tests — avoids CPU-count variance.
    fn make_cache(shard_cap: usize) -> DashCache<u64, u64> {
        DashCacheBuilder::new(NonZeroUsize::new(4 * shard_cap).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .build()
    }

    #[tokio::test]
    async fn insert_get_roundtrip() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        assert_eq!(cache.get(&1).await, Some(100));
    }

    #[tokio::test]
    async fn get_miss_returns_none() {
        let cache = make_cache(10);
        assert_eq!(cache.get(&99).await, None);
    }

    #[tokio::test]
    async fn insert_updates_existing_key() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        cache.insert(1, 200).await;
        assert_eq!(cache.get(&1).await, Some(200));
    }

    #[tokio::test]
    async fn contains_returns_correct_values() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        assert!(cache.contains(&1).await);
        assert!(!cache.contains(&2).await);
    }

    #[tokio::test]
    async fn contains_does_not_count_as_hit_in_stats() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        cache.contains(&1).await;
        // contains is a read-only check — stats should show no hits
        let stats = cache.statistics().await;
        assert_eq!(stats.hits, 0);
    }

    #[tokio::test]
    async fn evict_removes_key_and_returns_value() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        let val = cache.evict(&1).await;
        assert_eq!(val, Some(100));
        assert!(!cache.contains(&1).await);
    }

    #[tokio::test]
    async fn evict_missing_key_returns_none() {
        let cache = make_cache(10);
        assert_eq!(cache.evict(&99).await, None);
    }

    #[tokio::test]
    async fn update_existing_key_changes_value() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        cache.update(&1, 999).await.unwrap();
        assert_eq!(cache.get(&1).await, Some(999));
    }

    #[tokio::test]
    async fn update_missing_key_returns_err() {
        let cache = make_cache(10);
        assert!(cache.update(&99, 1).await.is_err());
    }

    #[tokio::test]
    async fn drain_empties_all_shards() {
        let cache = make_cache(10);
        for i in 0..20u64 {
            cache.insert(i, i).await;
        }
        cache.drain().await;
        assert_eq!(cache.len().await, 0);
        for i in 0..20u64 {
            assert_eq!(cache.get(&i).await, None);
        }
    }

    #[tokio::test]
    async fn statistics_aggregates_hits_across_shards() {
        let cache = make_cache(10);
        for i in 0..8u64 {
            cache.insert(i, i).await;
        }
        // hit every key once
        for i in 0..8u64 {
            cache.get(&i).await;
        }
        let stats = cache.statistics().await;
        assert_eq!(stats.hits, 8);
    }

    #[tokio::test]
    async fn statistics_counts_misses() {
        let cache = make_cache(10);
        cache.get(&1).await;
        cache.get(&2).await;
        let stats = cache.statistics().await;
        assert_eq!(stats.misses, 2);
    }

    #[tokio::test]
    async fn len_reflects_inserted_keys() {
        let cache = make_cache(10);
        for i in 0..8u64 {
            cache.insert(i, i).await;
        }
        assert_eq!(cache.len().await, 8);
    }

    #[tokio::test]
    async fn num_shards_matches_constructor() {
        let cache = make_cache(10);
        assert_eq!(cache.num_shards(), 4);
    }

    #[tokio::test]
    async fn concurrent_inserts_all_keys_present() {
        let cache = DashCacheBuilder::<u64, u64>::new(NonZeroUsize::new(4000).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .build();
        let n = 200u64;
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let cache_c = cache.clone();
            handles.push(tokio::spawn(async move {
                let start = t * (n / 4);
                let end = start + (n / 4);
                for i in start..end {
                    cache_c.insert(i, i * 10).await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        for i in 0..n {
            assert_eq!(cache.get(&i).await, Some(i * 10), "missing key {i}");
        }
    }

    #[tokio::test]
    async fn get_accepts_borrowed_key() {
        let cache: DashCache<String, u32> = DashCacheBuilder::new(NonZeroUsize::new(40).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .build();
        cache.insert("hello".to_string(), 1).await;
        cache.insert("world".to_string(), 2).await;
        // &str is accepted where K = String via Borrow<str>
        assert_eq!(cache.get("hello").await, Some(1));
        assert_eq!(cache.get("world").await, Some(2));
        assert_eq!(cache.get("missing").await, None);
        let stats = cache.statistics().await;
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test]
    async fn contains_accepts_borrowed_key() {
        let cache: DashCache<String, u32> = DashCacheBuilder::new(NonZeroUsize::new(40).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .build();
        cache.insert("hello".to_string(), 1).await;
        assert!(cache.contains("hello").await);
        assert!(!cache.contains("missing").await);
    }

    #[tokio::test]
    async fn evict_accepts_borrowed_key() {
        let cache: DashCache<String, u32> = DashCacheBuilder::new(NonZeroUsize::new(40).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .build();
        cache.insert("hello".to_string(), 1).await;
        cache.insert("world".to_string(), 2).await;
        assert_eq!(cache.evict("hello").await, Some(1));
        assert_eq!(cache.evict("hello").await, None);
        assert!(cache.contains("world").await);
    }

    #[tokio::test]
    async fn update_accepts_borrowed_key() {
        let cache: DashCache<String, u32> = DashCacheBuilder::new(NonZeroUsize::new(40).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .build();
        cache.insert("hello".to_string(), 1).await;
        cache.update("hello", 99).await.unwrap();
        assert_eq!(cache.get("hello").await, Some(99));
        assert!(cache.update("missing", 0).await.is_err());
    }

    // TTL tests

    #[tokio::test]
    async fn insert_with_ttl_accessible_before_expiry() {
        let cache = make_cache(10);
        cache.insert_with_ttl(1, 100, Duration::from_secs(60)).await;
        assert_eq!(cache.get(&1).await, Some(100));
        assert!(cache.contains(&1).await);
    }

    #[tokio::test]
    async fn insert_with_ttl_expires() {
        let cache = make_cache(10);
        cache
            .insert_with_ttl(1, 100, Duration::from_millis(50))
            .await;
        assert_eq!(cache.get(&1).await, Some(100));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, None);
        assert!(!cache.contains(&1).await);
    }

    #[tokio::test]
    async fn expiration_stat_incremented() {
        let cache = make_cache(10);
        cache
            .insert_with_ttl(1, 100, Duration::from_millis(50))
            .await;
        cache
            .insert_with_ttl(2, 200, Duration::from_millis(50))
            .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, None);
        assert_eq!(cache.get(&2).await, None);
        let stats = cache.statistics().await;
        assert_eq!(stats.expirations, 2);
        assert_eq!(stats.hits, 0);
    }

    #[tokio::test]
    async fn update_with_ttl_resets_expiration() {
        let cache = make_cache(10);
        cache
            .insert_with_ttl(1, 100, Duration::from_millis(50))
            .await;
        cache
            .update_with_ttl(&1, 200, Duration::from_secs(60))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, Some(200));
    }

    #[tokio::test]
    async fn default_ttl_applied_via_builder() {
        let cache = DashCacheBuilder::<u64, u64>::new(NonZeroUsize::new(40).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .with_default_ttl(Duration::from_millis(50))
            .build();
        cache.insert(1, 100).await;
        assert_eq!(cache.get(&1).await, Some(100));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, None);
    }

    #[tokio::test]
    async fn insert_with_ttl_overrides_default_ttl() {
        let cache = DashCacheBuilder::<u64, u64>::new(NonZeroUsize::new(40).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .with_default_ttl(Duration::from_millis(50))
            .build();
        // explicit long TTL overrides the short default
        cache.insert_with_ttl(1, 100, Duration::from_secs(60)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, Some(100));
    }

    #[tokio::test]
    async fn no_default_ttl_entries_do_not_expire() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&1).await, Some(100));
    }

    #[tokio::test]
    async fn eviction_does_not_affect_other_shards() {
        // Fill the cache past capacity to trigger evictions across shards.
        // Hash routing is not uniform so we can't assert an exact len, but total
        // len must never exceed total capacity and evictions must have occurred.
        let shards = 4usize;
        let shard_cap = 4usize;
        let total_cap = shards * shard_cap;
        let cache = DashCacheBuilder::<u64, u64>::new(NonZeroUsize::new(total_cap).unwrap())
            .with_num_shards(NonZeroUsize::new(shards).unwrap())
            .build();
        for i in 0..(total_cap * 4) as u64 {
            cache.insert(i, i).await;
        }
        assert!(cache.len().await <= total_cap);
        let stats = cache.statistics().await;
        assert!(stats.evictions > 0);
    }

    #[tokio::test]
    async fn checkout_removes_entry_from_cache() {
        let cache = make_cache(10);
        cache.insert(1, 100).await;
        let guard = cache.checkout(&1).await;
        assert!(guard.is_some());
        assert!(!cache.contains(&1).await);
    }

    #[tokio::test]
    async fn checkout_miss_returns_none() {
        let cache = make_cache(10);
        assert!(cache.checkout(&99u64).await.is_none());
    }

    #[tokio::test]
    async fn checkout_deref_returns_value() {
        let cache = make_cache(10);
        cache.insert(1, 100u64).await;
        let guard = cache.checkout(&1).await.unwrap();
        assert_eq!(*guard, 100);
        drop(guard);
    }

    #[tokio::test]
    async fn checkout_drop_reinserts_value() {
        let cache = make_cache(10);
        cache.insert(1, 100u64).await;
        let guard = cache.checkout(&1).await.unwrap();
        drop(guard);
        // yield to let the spawned re-insert task run
        tokio::task::yield_now().await;
        assert_eq!(cache.get(&1).await, Some(100));
    }

    #[tokio::test]
    async fn checkout_drop_reinserts_modified_value() {
        let cache = make_cache(10);
        cache.insert(1, 100u64).await;
        let mut guard = cache.checkout(&1).await.unwrap();
        *guard = 999;
        drop(guard);
        tokio::task::yield_now().await;
        assert_eq!(cache.get(&1).await, Some(999));
    }

    #[tokio::test]
    async fn checkout_other_keys_unaffected() {
        let cache = make_cache(10);
        cache.insert(1, 100u64).await;
        cache.insert(2, 200u64).await;
        let _guard = cache.checkout(&1).await.unwrap();
        assert_eq!(cache.get(&2).await, Some(200));
    }

    #[tokio::test]
    async fn checkout_then_insert_new_value_while_checked_out() {
        // If the caller re-inserts a new value while the guard is live, the guard's
        // drop re-inserts the original (checked-out) value, overwriting the interim one.
        let cache = make_cache(10);
        cache.insert(1, 100u64).await;
        let guard = cache.checkout(&1).await.unwrap();
        cache.insert(1, 777).await;
        assert_eq!(cache.get(&1).await, Some(777));
        drop(guard);
        tokio::task::yield_now().await;
        // guard re-inserted the original value, overwriting the interim insert
        assert_eq!(cache.get(&1).await, Some(100));
    }

    #[tokio::test]
    async fn checkout_drop_preserves_ttl() {
        // The entry should be re-inserted with its original expiry, not a fresh TTL.
        let cache: DashCache<u64, u64> = DashCacheBuilder::new(NonZeroUsize::new(40).unwrap())
            .with_num_shards(NonZeroUsize::new(4).unwrap())
            .build();
        let ttl = Duration::from_millis(200);
        cache.insert_with_ttl(1u64, 100u64, ttl).await;

        // Wait for half the TTL, then check out.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let guard = cache.checkout(&1u64).await.unwrap();
        drop(guard);
        tokio::task::yield_now().await;

        // Still within the original TTL window — entry should be present.
        assert_eq!(cache.get(&1u64).await, Some(100));

        // Wait for the remainder of the original TTL to elapse.
        tokio::time::sleep(Duration::from_millis(150)).await;

        // The entry should now be expired (original expiry has passed).
        assert_eq!(cache.get(&1u64).await, None);
    }
}
