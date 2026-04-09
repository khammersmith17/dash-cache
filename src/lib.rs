pub mod core;
pub mod dash_cache;

/// This crate implements both an LruCache geared toward single threaded use and also a thread safe
/// cache intended for use across threads. The thread safe cache is optimized for concurrent
/// access, leveraging internal sharding, inspired by the dashmap crate, and thus is called
/// DashCache as an homage. All mutation is done internally, and thus not get_mut type methods are
/// exposed to optimize concurrent and shared access, thus there is some overhead paid for cloning
/// data on get and set type methods.
pub use core::{CacheShard, IndexedCacheShard, LruCache};
pub use dash_cache::DashCache;
