use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::JsonRpcNotification;

/// Visibility of one cacheable MCP result.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheScope {
    /// The result is identical for every authorization context in a namespace.
    Public,
    /// The result can vary by authorization context.
    #[default]
    Private,
}

/// Server-provided freshness policy for one cacheable MCP operation.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheHint {
    /// Freshness lifetime in milliseconds. Zero means immediately stale.
    pub ttl_ms: u64,
    /// Whether callers may share the result across authorization contexts.
    pub cache_scope: CacheScope,
}

impl CacheHint {
    /// Creates a cache hint.
    pub const fn new(ttl_ms: u64, cache_scope: CacheScope) -> Self {
        Self {
            ttl_ms,
            cache_scope,
        }
    }

    /// Creates the conservative protocol default: immediately stale and private.
    pub const fn no_store() -> Self {
        Self::new(0, CacheScope::Private)
    }
}

/// Client behavior for one cacheable request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CacheMode {
    /// Reuse a fresh entry, otherwise fetch and cache the response.
    #[default]
    Use,
    /// Skip lookup, fetch a new response, and update the cache.
    Refresh,
    /// Skip both lookup and storage.
    Bypass,
}

/// Cacheable MCP operation configured by a server.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CacheOperation {
    /// `server/discover`
    ServerDiscover,
    /// `tools/list`
    ToolsList,
    /// `prompts/list`
    PromptsList,
    /// `resources/list`
    ResourcesList,
    /// `resources/templates/list`
    ResourceTemplatesList,
    /// `resources/read`
    ResourceRead,
}

impl CacheOperation {
    pub(crate) const fn method(self) -> &'static str {
        match self {
            Self::ServerDiscover => "server/discover",
            Self::ToolsList => "tools/list",
            Self::PromptsList => "prompts/list",
            Self::ResourcesList => "resources/list",
            Self::ResourceTemplatesList => "resources/templates/list",
            Self::ResourceRead => "resources/read",
        }
    }

    pub(crate) fn from_method(method: &str) -> Option<Self> {
        match method {
            "server/discover" => Some(Self::ServerDiscover),
            "tools/list" => Some(Self::ToolsList),
            "prompts/list" => Some(Self::PromptsList),
            "resources/list" => Some(Self::ResourcesList),
            "resources/templates/list" => Some(Self::ResourceTemplatesList),
            "resources/read" => Some(Self::ResourceRead),
            _ => None,
        }
    }
}

/// Raw response retained by a [`ResponseCacheStore`].
#[derive(Clone, Debug)]
pub struct CachedResponse {
    /// Serialized JSON-RPC result value.
    pub value: Value,
    /// Wall-clock time at which the result was received.
    pub received_at_ms: u64,
    /// Effective, client-bounded freshness lifetime.
    pub ttl_ms: u64,
}

impl CachedResponse {
    fn is_fresh(&self, now_ms: u64) -> bool {
        now_ms >= self.received_at_ms && now_ms.saturating_sub(self.received_at_ms) < self.ttl_ms
    }
}

/// Pluggable storage boundary for MCP response caching.
///
/// Implementations must be thread-safe. Keys are opaque and already include
/// the server namespace and, for private results, the authorization partition.
pub trait ResponseCacheStore: Send + Sync + fmt::Debug {
    /// Loads one opaque cache key.
    fn get(&self, key: &str) -> Option<CachedResponse>;

    /// Replaces one opaque cache key.
    fn put(&self, key: String, response: CachedResponse);

    /// Removes one opaque cache key.
    fn remove(&self, key: &str);

    /// Removes every key beginning with the opaque prefix.
    fn remove_prefix(&self, prefix: &str);
}

/// Bounded, process-local MCP response cache.
#[derive(Debug)]
pub struct InMemoryResponseCache {
    capacity: usize,
    state: Mutex<MemoryCacheState>,
}

#[derive(Debug, Default)]
struct MemoryCacheState {
    entries: HashMap<String, CachedResponse>,
    insertion_order: VecDeque<String>,
}

impl InMemoryResponseCache {
    /// Creates a bounded cache. A zero capacity is normalized to one.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(MemoryCacheState::default()),
        }
    }
}

impl ResponseCacheStore for InMemoryResponseCache {
    fn get(&self, key: &str) -> Option<CachedResponse> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(key)
            .cloned()
    }

    fn put(&self, key: String, response: CachedResponse) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.entries.contains_key(&key) {
            state.insertion_order.push_back(key.clone());
        }
        state.entries.insert(key, response);
        while state.entries.len() > self.capacity {
            if let Some(oldest) = state.insertion_order.pop_front() {
                state.entries.remove(&oldest);
            }
        }
    }

    fn remove(&self, key: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.entries.remove(key);
        state.insertion_order.retain(|candidate| candidate != key);
    }

    fn remove_prefix(&self, prefix: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.entries.retain(|key, _| !key.starts_with(prefix));
        state.insertion_order.retain(|key| !key.starts_with(prefix));
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ClientResponseCache {
    store: Arc<dyn ResponseCacheStore>,
    namespace: Arc<str>,
    private_partition: Arc<str>,
    max_ttl: Duration,
}

impl ClientResponseCache {
    pub(crate) fn new(
        store: Arc<dyn ResponseCacheStore>,
        namespace: String,
        private_partition: String,
        max_ttl: Duration,
    ) -> Self {
        Self {
            store,
            namespace: namespace.into(),
            private_partition: private_partition.into(),
            max_ttl,
        }
    }

    pub(crate) fn get(&self, method: &str, params: &Value) -> Option<Value> {
        let operation = CacheOperation::from_method(method)?;
        let suffix = cache_suffix(operation, params);
        let now = unix_time_ms();
        for key in [self.private_key(&suffix), self.public_key(&suffix)] {
            if let Some(entry) = self.store.get(&key) {
                if entry.is_fresh(now) {
                    return Some(entry.value);
                }
                self.store.remove(&key);
            }
        }
        None
    }

    pub(crate) fn put(&self, method: &str, params: &Value, value: Value) {
        let Some(operation) = CacheOperation::from_method(method) else {
            return;
        };
        let Some(object) = value.as_object() else {
            return;
        };
        let ttl_ms = object.get("ttlMs").and_then(Value::as_u64).unwrap_or(0);
        let scope = object
            .get("cacheScope")
            .cloned()
            .and_then(|scope| serde_json::from_value(scope).ok())
            .unwrap_or(CacheScope::Private);
        let effective_ttl = ttl_ms.min(duration_ms(self.max_ttl));
        if effective_ttl == 0 {
            return;
        }
        let suffix = cache_suffix(operation, params);
        let key = match scope {
            CacheScope::Public => self.public_key(&suffix),
            CacheScope::Private => self.private_key(&suffix),
        };
        self.store.put(
            key,
            CachedResponse {
                value,
                received_at_ms: unix_time_ms(),
                ttl_ms: effective_ttl,
            },
        );
    }

    pub(crate) fn invalidate_notification(&self, notification: &JsonRpcNotification) {
        match notification.method.as_str() {
            "notifications/tools/list_changed" => {
                self.invalidate_operation(CacheOperation::ToolsList);
            }
            "notifications/prompts/list_changed" => {
                self.invalidate_operation(CacheOperation::PromptsList);
            }
            "notifications/resources/list_changed" => {
                self.invalidate_operation(CacheOperation::ResourcesList);
                self.invalidate_operation(CacheOperation::ResourceTemplatesList);
            }
            "notifications/resources/updated" => {
                if let Some(uri) = notification
                    .params
                    .as_ref()
                    .and_then(Value::as_object)
                    .and_then(|params| params.get("uri"))
                    .and_then(Value::as_str)
                {
                    self.invalidate_resource(uri);
                }
            }
            _ => {}
        }
    }

    pub(crate) fn invalidate_method(&self, method: &str) {
        if let Some(operation) = CacheOperation::from_method(method) {
            self.invalidate_operation(operation);
        }
    }

    fn invalidate_operation(&self, operation: CacheOperation) {
        let prefix = format!("{}|{}|", self.namespace, operation.method());
        self.store.remove_prefix(&prefix);
    }

    fn invalidate_resource(&self, uri: &str) {
        let params = serde_json::json!({"uri": uri});
        let suffix = cache_suffix(CacheOperation::ResourceRead, &params);
        self.store.remove(&self.public_key(&suffix));
        self.store.remove(&self.private_key(&suffix));
    }

    fn public_key(&self, suffix: &str) -> String {
        format!("{}|{}|public", self.namespace, suffix)
    }

    fn private_key(&self, suffix: &str) -> String {
        format!(
            "{}|{}|private|{}",
            self.namespace, suffix, self.private_partition
        )
    }
}

fn cache_suffix(operation: CacheOperation, params: &Value) -> String {
    format!(
        "{}|{}",
        operation.method(),
        serde_json::to_string(params).unwrap_or_default()
    )
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, duration_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_hints_are_not_cached() {
        let cache = ClientResponseCache::new(
            Arc::new(InMemoryResponseCache::new(8)),
            "server".into(),
            "principal".into(),
            Duration::from_secs(60),
        );
        let params = serde_json::json!({});
        cache.put("tools/list", &params, serde_json::json!({"tools": []}));
        assert_eq!(cache.get("tools/list", &params), None);
    }

    #[test]
    fn private_entries_are_partitioned_and_public_entries_are_shared() {
        let store: Arc<dyn ResponseCacheStore> = Arc::new(InMemoryResponseCache::new(8));
        let first = ClientResponseCache::new(
            Arc::clone(&store),
            "server".into(),
            "alice".into(),
            Duration::from_secs(60),
        );
        let second = ClientResponseCache::new(
            store,
            "server".into(),
            "bob".into(),
            Duration::from_secs(60),
        );
        let params = serde_json::json!({});
        first.put(
            "tools/list",
            &params,
            serde_json::json!({"tools": [], "ttlMs": 1000, "cacheScope": "private"}),
        );
        assert_eq!(second.get("tools/list", &params), None);
        first.put(
            "tools/list",
            &params,
            serde_json::json!({"tools": [], "ttlMs": 1000, "cacheScope": "public"}),
        );
        assert!(second.get("tools/list", &params).is_some());
    }

    #[test]
    fn list_change_invalidates_every_cached_page() {
        let cache = ClientResponseCache::new(
            Arc::new(InMemoryResponseCache::new(8)),
            "server".into(),
            "principal".into(),
            Duration::from_secs(60),
        );
        for cursor in [None, Some("next")] {
            let params = cursor.map_or_else(
                || serde_json::json!({}),
                |cursor| serde_json::json!({"cursor": cursor}),
            );
            cache.put(
                "tools/list",
                &params,
                serde_json::json!({"tools": [], "ttlMs": 1000, "cacheScope": "private"}),
            );
        }
        cache.invalidate_notification(&JsonRpcNotification::new(
            "notifications/tools/list_changed",
            None,
        ));
        assert_eq!(cache.get("tools/list", &serde_json::json!({})), None);
        assert_eq!(
            cache.get("tools/list", &serde_json::json!({"cursor": "next"})),
            None
        );
    }

    #[test]
    fn expired_and_client_bounded_entries_fail_closed() {
        let store: Arc<dyn ResponseCacheStore> = Arc::new(InMemoryResponseCache::new(8));
        store.put(
            "server|tools/list|{}|public".into(),
            CachedResponse {
                value: serde_json::json!({"tools": []}),
                received_at_ms: 0,
                ttl_ms: 1,
            },
        );
        let cache = ClientResponseCache::new(
            Arc::clone(&store),
            "server".into(),
            "principal".into(),
            Duration::ZERO,
        );
        assert_eq!(cache.get("tools/list", &serde_json::json!({})), None);
        cache.put(
            "tools/list",
            &serde_json::json!({}),
            serde_json::json!({
                "tools": [],
                "ttlMs": u64::MAX,
                "cacheScope": "private",
            }),
        );
        assert_eq!(cache.get("tools/list", &serde_json::json!({})), None);
    }

    #[test]
    fn memory_store_is_safe_under_concurrent_mutation() {
        let store = Arc::new(InMemoryResponseCache::new(32));
        std::thread::scope(|scope| {
            for worker in 0..16 {
                let store = Arc::clone(&store);
                scope.spawn(move || {
                    for index in 0..100 {
                        let key = format!("{worker}:{index}");
                        store.put(
                            key.clone(),
                            CachedResponse {
                                value: serde_json::json!(index),
                                received_at_ms: unix_time_ms(),
                                ttl_ms: 1000,
                            },
                        );
                        let _ = store.get(&key);
                        store.remove(&key);
                    }
                });
            }
        });
    }
}
