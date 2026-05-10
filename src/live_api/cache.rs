use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::LiveAuth;

pub struct AuthCache {
    entries: HashMap<u64, CachedAuth>,
    ttl: Duration,
}

struct CachedAuth {
    auth: LiveAuth,
    fetched_at: Instant,
}

impl AuthCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            ttl,
        }
    }

    pub fn get(&self, room_id: u64) -> Option<&LiveAuth> {
        let entry = self.entries.get(&room_id)?;
        if entry.fetched_at.elapsed() >= self.ttl {
            return None;
        }
        Some(&entry.auth)
    }

    pub fn insert(&mut self, room_id: u64, auth: LiveAuth) {
        self.entries.insert(
            room_id,
            CachedAuth {
                auth,
                fetched_at: Instant::now(),
            },
        );
    }

    pub fn is_expired(&self, room_id: u64) -> bool {
        match self.entries.get(&room_id) {
            Some(entry) => entry.fetched_at.elapsed() >= self.ttl,
            None => true,
        }
    }

    pub fn remove(&mut self, room_id: u64) {
        self.entries.remove(&room_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_api::LiveEndpoint;

    fn make_auth(room_id: u64) -> LiveAuth {
        LiveAuth {
            token: "test_token".to_string(),
            endpoints: vec![LiveEndpoint {
                host: "ws.example.com".to_string(),
                port: 443,
            }],
            room_id,
            uid: Some(12345),
            buvid3: "test_buvid".to_string(),
        }
    }

    #[test]
    fn insert_and_get() {
        let mut cache = AuthCache::new(Duration::from_secs(60));
        cache.insert(123, make_auth(123));
        let auth = cache.get(123).unwrap();
        assert_eq!(auth.room_id, 123);
    }

    #[test]
    fn miss_returns_none() {
        let cache = AuthCache::new(Duration::from_secs(60));
        assert!(cache.get(999).is_none());
    }

    #[test]
    fn expired_returns_none() {
        let mut cache = AuthCache::new(Duration::from_secs(0));
        cache.insert(123, make_auth(123));
        assert!(cache.get(123).is_none());
    }

    #[test]
    fn is_expired() {
        let mut cache = AuthCache::new(Duration::from_secs(0));
        assert!(cache.is_expired(123));
        cache.insert(123, make_auth(123));
        assert!(cache.is_expired(123));
    }

    #[test]
    fn remove() {
        let mut cache = AuthCache::new(Duration::from_secs(60));
        cache.insert(123, make_auth(123));
        cache.remove(123);
        assert!(cache.get(123).is_none());
    }
}
