use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

mod private {
    pub trait Sealed {}
}

/*
* The two cache stat tracking types here are to avoid the atomic overhead when using only the
* SlabShard in a single threaded context. Some methods in SlabShard that do not mutate the cache
* may need to mutate the stats. They are only used in the concurrent type.
*
* The atomics add non trivial overhead in the benchmarks when used on a single thread. To avoid
* this, Cell is used in the single threaded case, and atomics used in the concurrent case.
* */

/// A snapshot of cache statistics returned to callers.
#[derive(Default, Debug, Clone)]
pub struct CacheStats {
    pub hits: usize,
    pub misses: usize,
    pub evictions: usize,
    pub expirations: usize,
}

impl std::ops::Add for CacheStats {
    type Output = Self;
    fn add(self, other: Self) -> Self::Output {
        CacheStats {
            hits: self.hits + other.hits,
            misses: self.misses + other.misses,
            evictions: self.evictions + other.evictions,
            expirations: self.expirations + other.expirations,
        }
    }
}

impl std::ops::AddAssign for CacheStats {
    fn add_assign(&mut self, other: Self) {
        self.hits += other.hits;
        self.misses += other.misses;
        self.evictions += other.evictions;
        self.expirations += other.expirations;
    }
}

/// Sealed trait for cache statistics backends. Cannot be implemented outside this crate.
pub trait Stats: private::Sealed + Default + std::fmt::Debug {
    fn hit(&self);
    fn miss(&self);
    fn eviction(&self);
    fn expiration(&self);
    fn snapshot(&self) -> CacheStats;
}

/// Single-threaded stats backend using `Cell<usize>`. `!Sync`.
///
/// This is the default stats backend for `SlabShard`. It avoids atomic overhead
/// by using `Cell` for interior mutability, which is sound because `SlabShard`
/// requires exclusive (`&mut self`) access for all mutating operations.
///
#[derive(Default, Debug)]
pub struct LocalStats {
    hits: Cell<usize>,
    misses: Cell<usize>,
    evictions: Cell<usize>,
    expirations: Cell<usize>,
}

impl private::Sealed for LocalStats {}

impl Stats for LocalStats {
    #[inline]
    fn hit(&self) {
        self.hits.set(self.hits.get() + 1);
    }

    #[inline]
    fn miss(&self) {
        self.misses.set(self.misses.get() + 1);
    }

    #[inline]
    fn eviction(&self) {
        self.evictions.set(self.evictions.get() + 1);
    }

    #[inline]
    fn expiration(&self) {
        self.expirations.set(self.expirations.get() + 1);
    }

    fn snapshot(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.get(),
            misses: self.misses.get(),
            evictions: self.evictions.get(),
            expirations: self.expirations.get(),
        }
    }
}

/// Concurrent stats backend using `AtomicUsize`. `Sync`.
///
/// Used by `DashCache` shards, where `get` holds only a read lock and must
/// record hits/misses through a shared reference.
#[derive(Default, Debug)]
pub(crate) struct AtomicStats {
    hits: AtomicUsize,
    misses: AtomicUsize,
    evictions: AtomicUsize,
    expirations: AtomicUsize,
}

impl private::Sealed for AtomicStats {}

impl Stats for AtomicStats {
    #[inline]
    fn hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn expiration(&self) {
        self.expirations.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            expirations: self.expirations.load(Ordering::Relaxed),
        }
    }
}
