//! Internal transport unification for typed Realtime sessions.

use std::collections::BTreeMap;

#[cfg(not(target_arch = "wasm32"))]
use secrecy::ExposeSecret;

#[cfg(target_arch = "wasm32")]
use super::realtime_data_channel::RealtimeDataChannelTransport;
use super::{
    OpenAiConfig,
    realtime_transport::{RealtimeTransport, RealtimeTransportError},
};

#[derive(Debug)]
pub(super) enum OpenAiRealtimeTransport {
    WebSocket(RealtimeTransport),
    #[cfg(target_arch = "wasm32")]
    WebRtc(RealtimeDataChannelTransport),
}

impl OpenAiRealtimeTransport {
    pub(super) async fn send_text(&mut self, text: &str) -> Result<(), RealtimeTransportError> {
        match self {
            Self::WebSocket(transport) => transport.send_text(text).await,
            #[cfg(target_arch = "wasm32")]
            Self::WebRtc(transport) => transport.send_text(text),
        }
    }

    pub(super) async fn next_text(&mut self) -> Result<Option<String>, RealtimeTransportError> {
        match self {
            Self::WebSocket(transport) => transport.next_text().await,
            #[cfg(target_arch = "wasm32")]
            Self::WebRtc(transport) => transport.next_text().await,
        }
    }

    pub(super) async fn close(&mut self) -> Result<(), RealtimeTransportError> {
        match self {
            Self::WebSocket(transport) => transport.close().await,
            #[cfg(target_arch = "wasm32")]
            Self::WebRtc(transport) => {
                transport.close();
                Ok(())
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn connect_headers(config: &OpenAiConfig) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    if let Some(api_key) = &config.api_key {
        headers.insert(
            "authorization".into(),
            format!("Bearer {}", api_key.expose_secret()),
        );
    }
    if let Some(organization) = &config.organization {
        headers.insert("openai-organization".into(), organization.clone());
    }
    if let Some(project) = &config.project {
        headers.insert("openai-project".into(), project.clone());
    }
    headers
}

#[cfg(target_arch = "wasm32")]
pub(super) fn connect_headers(_config: &OpenAiConfig) -> BTreeMap<String, String> {
    BTreeMap::new()
}
