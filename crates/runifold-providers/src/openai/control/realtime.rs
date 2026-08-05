//! Server-side OpenAI-compatible Realtime control operations.

use reqwest::{Response, multipart};
use runifold_model::ModelCallContext;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    OpenAiControlError, OpenAiControlFuture, OpenAiControlPlane, provider_error, read_bounded_text,
    request_id,
};
use crate::openai::{
    OpenAiRealtimeCall, OpenAiRealtimeCallRequest, OpenAiRealtimeModality,
    realtime_call::{validate_safety_identifier, validate_sdp},
};

const MAX_REALTIME_INSTRUCTIONS_BYTES: usize = 256 * 1024;

/// Validated server-side request for a short-lived Realtime client secret.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiRealtimeClientSecretRequest {
    model: String,
    instructions: Option<String>,
    output_modality: Option<OpenAiRealtimeModality>,
    expires_after_seconds: u32,
    safety_identifier: Option<String>,
}

impl OpenAiRealtimeClientSecretRequest {
    /// Creates a request with a ten-minute lifetime.
    ///
    /// # Errors
    ///
    /// Rejects invalid model identifiers.
    pub fn new(model: impl Into<String>) -> Result<Self, OpenAiControlError> {
        Ok(Self {
            model: super::validate_id("Realtime model", model.into())?,
            instructions: None,
            output_modality: None,
            expires_after_seconds: 600,
            safety_identifier: None,
        })
    }

    /// Applies bounded session instructions to every use of the secret.
    ///
    /// # Errors
    ///
    /// Rejects instructions larger than 256 KiB.
    pub fn with_instructions(
        mut self,
        instructions: impl Into<String>,
    ) -> Result<Self, OpenAiControlError> {
        self.instructions = Some(validate_realtime_instructions(instructions.into())?);
        Ok(self)
    }

    /// Selects the initial output modality attached to the secret.
    #[must_use]
    pub fn with_modality(mut self, modality: OpenAiRealtimeModality) -> Self {
        self.output_modality = Some(modality);
        self
    }

    /// Changes the short-lived credential lifetime.
    ///
    /// # Errors
    ///
    /// Rejects values outside the GA 10-second to 2-hour range.
    pub fn with_expiration_seconds(mut self, seconds: u32) -> Result<Self, OpenAiControlError> {
        if !(10..=7_200).contains(&seconds) {
            return Err(OpenAiControlError::InvalidRequest(
                "Realtime client secret expiration must be between 10 and 7200 seconds".into(),
            ));
        }
        self.expires_after_seconds = seconds;
        Ok(self)
    }

    /// Binds a stable privacy-preserving end-user identifier to the secret.
    ///
    /// # Errors
    ///
    /// Rejects blank, control-containing, or values over 64 bytes.
    pub fn with_safety_identifier(
        mut self,
        identifier: impl Into<String>,
    ) -> Result<Self, OpenAiControlError> {
        let identifier = identifier.into();
        validate_safety_identifier(&identifier)?;
        self.safety_identifier = Some(identifier);
        Ok(self)
    }
}

/// Short-lived Realtime credential and its effective Provider session.
#[derive(Clone)]
pub struct OpenAiRealtimeClientSecret {
    value: SecretString,
    /// Unix timestamp at which the credential expires.
    pub expires_at: u64,
    /// Effective forward-compatible GA session returned by the Provider.
    pub session: Value,
}

impl OpenAiRealtimeClientSecret {
    /// Returns the redacting secret container.
    pub const fn secret(&self) -> &SecretString {
        &self.value
    }
}

impl std::fmt::Debug for OpenAiRealtimeClientSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiRealtimeClientSecret")
            .field("value", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .field("session", &"[REDACTED CONFIG]")
            .finish()
    }
}

impl OpenAiControlPlane {
    /// Creates a short-lived credential for a browser or mobile Realtime
    /// connection without exposing the configured long-lived API key.
    pub fn create_realtime_client_secret(
        &self,
        request: OpenAiRealtimeClientSecretRequest,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiRealtimeClientSecret, OpenAiControlError>> {
        Box::pin(async move {
            let body = CreateRealtimeClientSecretWire {
                expires_after: RealtimeSecretExpirationWire {
                    anchor: "created_at",
                    seconds: request.expires_after_seconds,
                },
                session: RealtimeSecretSessionWire {
                    session_type: "realtime",
                    model: request.model,
                    instructions: request.instructions,
                    output_modalities: request.output_modality.map(|value| vec![value]),
                },
            };
            let mut builder = self
                .http
                .post(self.endpoint("realtime/client_secrets"))
                .json(&body);
            if let Some(identifier) = request.safety_identifier {
                builder = builder.header("openai-safety-identifier", identifier);
            }
            let response = self.send(builder, &context).await?;
            let wire: RealtimeClientSecretWire = self.decode(response, &context).await?;
            wire.try_into()
        })
    }

    /// Creates a WebRTC call through the server-side unified interface.
    ///
    /// The configured long-lived credential remains on this control plane;
    /// browsers should send their offer to an application-owned endpoint.
    pub fn create_realtime_call(
        &self,
        request: OpenAiRealtimeCallRequest,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiRealtimeCall, OpenAiControlError>> {
        Box::pin(async move {
            let session = RealtimeSecretSessionWire {
                session_type: "realtime",
                model: request.model,
                instructions: request.instructions,
                output_modalities: request.output_modality.map(|value| vec![value]),
            };
            let session = serde_json::to_string(&session).map_err(|error| {
                OpenAiControlError::InvalidRequest(format!(
                    "Realtime session configuration could not be encoded: {error}"
                ))
            })?;
            let sdp = multipart::Part::text(request.offer.0)
                .mime_str("application/sdp")
                .map_err(|error| {
                    OpenAiControlError::InvalidRequest(format!(
                        "Realtime SDP part could not be constructed: {error}"
                    ))
                })?;
            let session = multipart::Part::text(session)
                .mime_str("application/json")
                .map_err(|error| {
                    OpenAiControlError::InvalidRequest(format!(
                        "Realtime session part could not be constructed: {error}"
                    ))
                })?;
            let form = multipart::Form::new()
                .part("sdp", sdp)
                .part("session", session);
            let mut builder = self
                .http
                .post(self.endpoint("realtime/calls"))
                .multipart(form);
            if let Some(identifier) = request.safety_identifier {
                builder = builder.header("openai-safety-identifier", identifier);
            }
            let response = self.send(builder, &context).await?;
            self.decode_realtime_call(response, &context).await
        })
    }

    async fn decode_realtime_call(
        &self,
        response: Response,
        context: &ModelCallContext,
    ) -> Result<OpenAiRealtimeCall, OpenAiControlError> {
        let status = response.status();
        let request_id = request_id(&response);
        let location = response
            .headers()
            .get("location")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let deadline = context.deadline();
        let answer = read_bounded_text(response, context, deadline).await?;
        if !status.is_success() {
            return Err(provider_error(status, request_id, &answer));
        }
        validate_sdp("answer", &answer)
            .map_err(|error| OpenAiControlError::Protocol(error.to_string()))?;
        Ok(OpenAiRealtimeCall { answer, location })
    }
}

#[derive(Serialize)]
struct CreateRealtimeClientSecretWire {
    expires_after: RealtimeSecretExpirationWire,
    session: RealtimeSecretSessionWire,
}

#[derive(Serialize)]
struct RealtimeSecretExpirationWire {
    anchor: &'static str,
    seconds: u32,
}

#[derive(Serialize)]
struct RealtimeSecretSessionWire {
    #[serde(rename = "type")]
    session_type: &'static str,
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_modalities: Option<Vec<OpenAiRealtimeModality>>,
}

#[derive(Deserialize)]
struct RealtimeClientSecretWire {
    value: String,
    expires_at: u64,
    session: Value,
}

impl TryFrom<RealtimeClientSecretWire> for OpenAiRealtimeClientSecret {
    type Error = OpenAiControlError;

    fn try_from(wire: RealtimeClientSecretWire) -> Result<Self, Self::Error> {
        if wire.value.is_empty()
            || wire.value.len() > 4_096
            || wire.value.chars().any(char::is_control)
        {
            return Err(OpenAiControlError::Protocol(
                "Realtime client secret was empty or malformed".into(),
            ));
        }
        if !wire.session.is_object() {
            return Err(OpenAiControlError::Protocol(
                "Realtime client secret response omitted its effective session".into(),
            ));
        }
        Ok(Self {
            value: SecretString::from(wire.value),
            expires_at: wire.expires_at,
            session: wire.session,
        })
    }
}

pub(crate) fn validate_realtime_instructions(value: String) -> Result<String, OpenAiControlError> {
    if value.len() > MAX_REALTIME_INSTRUCTIONS_BYTES {
        return Err(OpenAiControlError::InvalidRequest(format!(
            "Realtime instructions exceed the {MAX_REALTIME_INSTRUCTIONS_BYTES} byte limit"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::OpenAiRealtimeClientSecretRequest;

    #[test]
    fn realtime_client_secret_request_validates_lifetime() {
        let request = OpenAiRealtimeClientSecretRequest::new("gpt-realtime").unwrap();
        assert!(request.clone().with_expiration_seconds(9).is_err());
        assert!(request.with_expiration_seconds(7_201).is_err());
    }
}
