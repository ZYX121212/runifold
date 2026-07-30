//! Validated SDP values for `OpenAI` Realtime WebRTC calls.

use super::{
    OpenAiRealtimeModality,
    control::{OpenAiControlError, validate_id, validate_realtime_instructions},
};

const MAX_REALTIME_SDP_BYTES: usize = 256 * 1024;
const MAX_SAFETY_IDENTIFIER_BYTES: usize = 64;

/// Validated WebRTC SDP offer.
#[derive(Clone, Eq, PartialEq)]
pub struct OpenAiRealtimeSdpOffer(pub(crate) String);

impl OpenAiRealtimeSdpOffer {
    /// Validates one bounded SDP offer.
    ///
    /// # Errors
    ///
    /// Rejects malformed or oversized session descriptions.
    pub fn new(value: impl Into<String>) -> Result<Self, OpenAiControlError> {
        let value = value.into();
        validate_sdp("offer", &value)?;
        Ok(Self(value))
    }

    /// Borrows the SDP wire representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for OpenAiRealtimeSdpOffer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiRealtimeSdpOffer")
            .field("bytes", &self.0.len())
            .finish()
    }
}

/// Server-side unified WebRTC call creation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiRealtimeCallRequest {
    pub(crate) offer: OpenAiRealtimeSdpOffer,
    pub(crate) model: String,
    pub(crate) instructions: Option<String>,
    pub(crate) output_modality: Option<OpenAiRealtimeModality>,
    pub(crate) safety_identifier: Option<String>,
}

impl OpenAiRealtimeCallRequest {
    /// Creates a request containing one offer and GA Realtime model.
    ///
    /// # Errors
    ///
    /// Rejects invalid model identifiers.
    pub fn new(
        offer: OpenAiRealtimeSdpOffer,
        model: impl Into<String>,
    ) -> Result<Self, OpenAiControlError> {
        Ok(Self {
            offer,
            model: validate_id("Realtime model", model.into())?,
            instructions: None,
            output_modality: None,
            safety_identifier: None,
        })
    }

    /// Adds bounded session instructions.
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

    /// Selects the initial output modality.
    #[must_use]
    pub fn with_modality(mut self, modality: OpenAiRealtimeModality) -> Self {
        self.output_modality = Some(modality);
        self
    }

    /// Binds a stable privacy-preserving end-user identifier.
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

pub(crate) fn validate_safety_identifier(value: &str) -> Result<(), OpenAiControlError> {
    if value.trim().is_empty()
        || value.len() > MAX_SAFETY_IDENTIFIER_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(OpenAiControlError::InvalidRequest(
            "Realtime safety identifier must be non-empty, control-free, and at most 64 bytes"
                .into(),
        ));
    }
    Ok(())
}

/// SDP answer and call resource returned by the unified WebRTC endpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct OpenAiRealtimeCall {
    pub(crate) answer: String,
    /// Relative call resource URL returned by `OpenAI`.
    pub location: Option<String>,
}

impl OpenAiRealtimeCall {
    /// Borrows the validated SDP answer.
    pub fn answer_sdp(&self) -> &str {
        &self.answer
    }
}

impl std::fmt::Debug for OpenAiRealtimeCall {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiRealtimeCall")
            .field("answer_bytes", &self.answer.len())
            .field("location", &self.location)
            .finish()
    }
}

pub(crate) fn validate_sdp(kind: &str, value: &str) -> Result<(), OpenAiControlError> {
    if value.len() > MAX_REALTIME_SDP_BYTES
        || !value.starts_with("v=0")
        || !value.lines().any(|line| line.starts_with("m="))
    {
        return Err(OpenAiControlError::InvalidRequest(format!(
            "Realtime SDP {kind} must be a valid description no larger than {MAX_REALTIME_SDP_BYTES} bytes"
        )));
    }
    Ok(())
}
