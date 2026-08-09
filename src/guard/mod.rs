use crate::dash_cache::DashCache;
use std::hash::{BuildHasher, Hash};
use std::ops::{Deref, DerefMut};
use tokio::runtime::Handle;

pub struct CacheEntryGuard<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    key: Option<K>,
    value: Option<V>,
    expires: u64,
    cache: DashCache<K, V, S>,
}

impl<K, V, S> CacheEntryGuard<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(key: K, value: V, expires: u64, cache: DashCache<K, V, S>) -> CacheEntryGuard<K, V, S> {
        CacheEntryGuard {
            key: Some(key),
            value: Some(value),
            expires,
            cache,
        }
    }
}

impl<K, V, S> Deref for CacheEntryGuard<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.value.as_ref().unwrap()
    }
}

impl<K, V, S> DerefMut for CacheEntryGuard<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value.as_mut().unwrap()
    }
}

impl<K, V, S> Drop for CacheEntryGuard<K, V, S>
where
    K: Hash + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let Some(value) = self.value.take() else {
            return;
        };

        let Some(key) = self.key.take() else { return };
        let expires = self.expires;
        let cache = self.cache.clone();

        let Ok(handle) = Handle::try_current() else {
            return;
        };

        handle.spawn(async move { cache.insert_with_expires(key, value, expires).await });
    }
}

