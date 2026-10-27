use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::{CircuitBreakerConfig, FeaturePolicy, ModelRetryPolicy, ResponseMode};

/// A reviewed workload preset layered over a Provider's protocol-safe policy.
pub trait RuntimeProfilePreset: Copy + Send + Sync + 'static {
    /// Applies this workload's choices without discarding Provider-owned
    /// request options, retry exceptions, or circuit policy.
    fn apply(self, provider: ProviderRuntimeProfile) -> ProviderRuntimeProfile;
}

/// Long-lived service defaults prioritizing correctness and Provider policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProductionProfile;

impl RuntimeProfilePreset for ProductionProfile {
    fn apply(self, provider: ProviderRuntimeProfile) -> ProviderRuntimeProfile {
        provider
    }
}

/// User-facing latency defaults that commit canonical stream events promptly.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InteractiveProfile;

impl RuntimeProfilePreset for InteractiveProfile {
    fn apply(self, provider: ProviderRuntimeProfile) -> ProviderRuntimeProfile {
        provider.response_mode(ResponseMode::Streaming)
    }
}

/// Offline execution defaults that validate a complete response before commit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BatchProfile;

impl RuntimeProfilePreset for BatchProfile {
    fn apply(self, provider: ProviderRuntimeProfile) -> ProviderRuntimeProfile {
        provider.response_mode(ResponseMode::Complete)
    }
}

/// Provider-recommended defaults for one fully composed model runtime.
///
/// The profile contains execution policy rather than transport credentials or
/// model identity. Concrete providers may override [`Default`] when their wire
/// protocol requires a different delivery mode, capability policy, retry
/// authority, circuit policy, or namespaced request option.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRuntimeProfile {
    response_mode: ResponseMode,
    feature_policy: FeaturePolicy,
    retry_policy: ModelRetryPolicy,
    circuit_breaker: CircuitBreakerConfig,
    provider_options: BTreeMap<String, Value>,
}

/// Payload-free effective policy for one runtime profile, without retry authority.
///
/// This is an operational snapshot, not a recovery contract or build manifest.
/// Provider option keys and values are excluded because they may be private.
/// Capacity pool configuration and other runtimes' state are not inferred.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RuntimePolicySnapshot {
    /// Version of this diagnostic representation.
    pub schema_version: u32,
    /// Effective canonical response delivery mode.
    pub response_mode: ResponseMode,
    /// Effective capability handling.
    pub feature_policy: FeaturePolicy,
    /// Attempt bound, backoff and explicit unknown-safety exceptions.
    pub retry_policy: ModelRetryPolicy,
    /// Enabled state, threshold, cooldown and counted failure kinds.
    pub circuit_breaker: CircuitBreakerConfig,
    /// Whether additional private adapter-owned request options are configured.
    pub has_provider_options: bool,
}

impl Default for ProviderRuntimeProfile {
    fn default() -> Self {
        Self {
            response_mode: ResponseMode::Streaming,
            feature_policy: FeaturePolicy::Strict,
            retry_policy: ModelRetryPolicy::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            provider_options: BTreeMap::new(),
        }
    }
}

impl ProviderRuntimeProfile {
    /// Snapshots this runtime's effective policy without provider option contents.
    pub fn diagnostic_snapshot(&self) -> RuntimePolicySnapshot {
        RuntimePolicySnapshot {
            schema_version: 1,
            response_mode: self.response_mode,
            feature_policy: self.feature_policy,
            retry_policy: self.retry_policy.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
            has_provider_options: !self.provider_options.is_empty(),
        }
    }

    /// Creates the provider-neutral fail-closed baseline.
    pub fn conservative() -> Self {
        Self::default()
    }

    /// Replaces response delivery behavior.
    #[must_use]
    pub const fn response_mode(mut self, response_mode: ResponseMode) -> Self {
        self.response_mode = response_mode;
        self
    }

    /// Replaces handling for unknown or emulated model capabilities.
    #[must_use]
    pub const fn feature_policy(mut self, feature_policy: FeaturePolicy) -> Self {
        self.feature_policy = feature_policy;
        self
    }

    /// Replaces bounded same-route retry policy.
    #[must_use]
    pub fn retry_policy(mut self, retry_policy: ModelRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Replaces circuit-breaker policy.
    #[must_use]
    pub fn circuit_breaker(mut self, circuit_breaker: CircuitBreakerConfig) -> Self {
        self.circuit_breaker = circuit_breaker;
        self
    }

    /// Disables shared route-health suppression while retaining retry policy.
    ///
    /// Capacity sharing is configured separately through a shared scheduler.
    /// Clones of the resulting runtime still share that scheduler when bound.
    #[must_use]
    pub fn without_circuit_breaker(mut self) -> Self {
        self.circuit_breaker = CircuitBreakerConfig::disabled();
        self
    }

    /// Adds or replaces one adapter-owned namespaced request option.
    #[must_use]
    pub fn provider_option(mut self, provider: impl Into<String>, options: Value) -> Self {
        self.provider_options.insert(provider.into(), options);
        self
    }

    /// Returns response delivery behavior.
    pub const fn selected_response_mode(&self) -> ResponseMode {
        self.response_mode
    }

    /// Returns handling for unknown or emulated model capabilities.
    pub const fn selected_feature_policy(&self) -> FeaturePolicy {
        self.feature_policy
    }

    /// Returns bounded same-route retry policy.
    pub const fn selected_retry_policy(&self) -> &ModelRetryPolicy {
        &self.retry_policy
    }

    /// Returns circuit-breaker policy.
    pub const fn selected_circuit_breaker(&self) -> &CircuitBreakerConfig {
        &self.circuit_breaker
    }

    /// Returns adapter-owned namespaced request options.
    pub const fn provider_options(&self) -> &BTreeMap<String, Value> {
        &self.provider_options
    }

    /// Splits the profile into owned runtime components.
    pub fn into_parts(
        self,
    ) -> (
        ResponseMode,
        FeaturePolicy,
        ModelRetryPolicy,
        CircuitBreakerConfig,
        BTreeMap<String, Value>,
    ) {
        (
            self.response_mode,
            self.feature_policy,
            self.retry_policy,
            self.circuit_breaker,
            self.provider_options,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        BatchProfile, FeaturePolicy, InteractiveProfile, ProductionProfile, ProviderRuntimeProfile,
        ResponseMode, RuntimeProfilePreset,
    };

    #[test]
    fn conservative_profile_is_fail_closed_and_provider_neutral() {
        let profile = ProviderRuntimeProfile::conservative();

        assert_eq!(profile.selected_response_mode(), ResponseMode::Streaming);
        assert_eq!(profile.selected_feature_policy(), FeaturePolicy::Strict);
        assert_eq!(profile.provider_options().len(), 0);
        assert_eq!(profile.selected_retry_policy().max_attempts(), 3);
    }

    #[test]
    fn workload_presets_preserve_provider_owned_options() {
        let provider = ProviderRuntimeProfile::conservative()
            .provider_option("provider", serde_json::json!({"safe": true}));

        let production = ProductionProfile.apply(provider.clone());
        let interactive = InteractiveProfile.apply(provider.clone());
        let batch = BatchProfile.apply(provider);

        assert_eq!(production.selected_response_mode(), ResponseMode::Streaming);
        assert_eq!(
            interactive.selected_response_mode(),
            ResponseMode::Streaming
        );
        assert_eq!(batch.selected_response_mode(), ResponseMode::Complete);
        assert_eq!(batch.provider_options()["provider"]["safe"], true);
    }
}
