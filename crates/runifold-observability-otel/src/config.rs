/// Controls recording of potentially sensitive model content.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ContentCapture {
    /// Record only low-cardinality operational metadata.
    #[default]
    Disabled,
    /// Record system instructions and input/output messages.
    Messages,
    /// Record messages and model-visible tool definitions.
    MessagesAndTools,
}

/// OpenTelemetry instrumentation policy.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OtelConfig {
    pub(crate) content_capture: ContentCapture,
    pub(crate) capture_error_messages: bool,
}

impl OtelConfig {
    /// Creates the safe default policy with all model content disabled.
    pub const fn new() -> Self {
        Self {
            content_capture: ContentCapture::Disabled,
            capture_error_messages: false,
        }
    }

    /// Explicitly selects a model-content capture policy.
    #[must_use]
    pub const fn with_content_capture(mut self, capture: ContentCapture) -> Self {
        self.content_capture = capture;
        self
    }

    /// Explicitly permits provider error messages in telemetry events.
    ///
    /// Provider messages may echo request content, so this is independent from
    /// message capture.
    #[must_use]
    pub const fn with_error_messages(mut self, enabled: bool) -> Self {
        self.capture_error_messages = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{ContentCapture, OtelConfig};

    #[test]
    fn sensitive_capture_is_disabled_by_default() {
        let config = OtelConfig::default();

        assert_eq!(config.content_capture, ContentCapture::Disabled);
        assert!(!config.capture_error_messages);
    }
}
