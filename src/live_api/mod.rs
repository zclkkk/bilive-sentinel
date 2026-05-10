mod cache;
mod client;
mod wbi;

pub use cache::AuthCache;
pub use client::LiveApiClient;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveAuth {
    pub token: String,
    pub endpoints: Vec<LiveEndpoint>,
    pub room_id: u64,
    pub uid: Option<u64>,
    pub buvid3: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveEndpoint {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum LiveApiError {
    #[error("network error: {0}")]
    Network(String),
    #[error("API error: code={code}, message={message}")]
    Api { code: i64, message: String },
    #[error("parse error: {0}")]
    Parse(String),
    #[error("auth error: {0}")]
    Auth(String),
}

pub trait LiveApi: Send + Sync {
    fn resolve_room_id(
        &self,
        room_id: u64,
    ) -> impl std::future::Future<Output = Result<u64, LiveApiError>> + Send;
    fn fetch_live_auth(
        &self,
        room_id: u64,
    ) -> impl std::future::Future<Output = Result<LiveAuth, LiveApiError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct MockLiveApi {
        room_ids: HashMap<u64, u64>,
        auths: HashMap<u64, LiveAuth>,
        errors: HashMap<u64, LiveApiError>,
    }

    impl MockLiveApi {
        fn new() -> Self {
            Self {
                room_ids: HashMap::new(),
                auths: HashMap::new(),
                errors: HashMap::new(),
            }
        }

        fn with_room(mut self, short: u64, long: u64) -> Self {
            self.room_ids.insert(short, long);
            self
        }

        fn with_auth(mut self, room_id: u64, auth: LiveAuth) -> Self {
            self.auths.insert(room_id, auth);
            self
        }

        fn with_error(mut self, room_id: u64, error: LiveApiError) -> Self {
            self.errors.insert(room_id, error);
            self
        }
    }

    impl LiveApi for MockLiveApi {
        async fn resolve_room_id(&self, room_id: u64) -> Result<u64, LiveApiError> {
            if let Some(err) = self.errors.get(&room_id) {
                return Err(clone_error(err));
            }
            Ok(self.room_ids.get(&room_id).copied().unwrap_or(room_id))
        }

        async fn fetch_live_auth(&self, room_id: u64) -> Result<LiveAuth, LiveApiError> {
            if let Some(err) = self.errors.get(&room_id) {
                return Err(clone_error(err));
            }
            self.auths
                .get(&room_id)
                .cloned()
                .ok_or(LiveApiError::Auth("not found".into()))
        }
    }

    fn clone_error(err: &LiveApiError) -> LiveApiError {
        match err {
            LiveApiError::Network(m) => LiveApiError::Network(m.clone()),
            LiveApiError::Api { code, message } => LiveApiError::Api {
                code: *code,
                message: message.clone(),
            },
            LiveApiError::Parse(m) => LiveApiError::Parse(m.clone()),
            LiveApiError::Auth(m) => LiveApiError::Auth(m.clone()),
        }
    }

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

    #[tokio::test]
    async fn mock_resolve_room_id() {
        let api = MockLiveApi::new().with_room(123, 456);
        assert_eq!(api.resolve_room_id(123).await.unwrap(), 456);
    }

    #[tokio::test]
    async fn mock_resolve_room_id_passthrough() {
        let api = MockLiveApi::new();
        assert_eq!(api.resolve_room_id(999).await.unwrap(), 999);
    }

    #[tokio::test]
    async fn mock_fetch_live_auth() {
        let api = MockLiveApi::new().with_auth(123, make_auth(123));
        let auth = api.fetch_live_auth(123).await.unwrap();
        assert_eq!(auth.room_id, 123);
        assert_eq!(auth.token, "test_token");
    }

    #[tokio::test]
    async fn mock_auth_not_found() {
        let api = MockLiveApi::new();
        let err = api.fetch_live_auth(999).await.unwrap_err();
        match err {
            LiveApiError::Auth(_) => {}
            _ => panic!("expected Auth error"),
        }
    }

    #[tokio::test]
    async fn mock_network_error() {
        let api =
            MockLiveApi::new().with_error(123, LiveApiError::Network("connection refused".into()));
        let err = api.fetch_live_auth(123).await.unwrap_err();
        match err {
            LiveApiError::Network(msg) => assert!(msg.contains("connection refused")),
            _ => panic!("expected Network error"),
        }
    }

    #[tokio::test]
    async fn mock_api_error() {
        let api = MockLiveApi::new().with_error(
            123,
            LiveApiError::Api {
                code: -101,
                message: "not login".to_string(),
            },
        );
        let err = api.fetch_live_auth(123).await.unwrap_err();
        match err {
            LiveApiError::Api { code, .. } => assert_eq!(code, -101),
            _ => panic!("expected Api error"),
        }
    }

    #[tokio::test]
    async fn mock_parse_error() {
        let api = MockLiveApi::new().with_error(123, LiveApiError::Parse("invalid json".into()));
        let err = api.fetch_live_auth(123).await.unwrap_err();
        match err {
            LiveApiError::Parse(_) => {}
            _ => panic!("expected Parse error"),
        }
    }

    #[tokio::test]
    async fn cache_refresh_on_expiry() {
        let api = MockLiveApi::new().with_auth(123, make_auth(123));
        let mut cache = AuthCache::new(std::time::Duration::from_secs(60));

        let auth = api.fetch_live_auth(123).await.unwrap();
        cache.insert(123, auth);
        assert_eq!(cache.get(123).unwrap().room_id, 123);

        cache.remove(123);
        assert!(cache.get(123).is_none());

        let auth = api.fetch_live_auth(123).await.unwrap();
        cache.insert(123, auth);
        assert_eq!(cache.get(123).unwrap().room_id, 123);
    }
}
