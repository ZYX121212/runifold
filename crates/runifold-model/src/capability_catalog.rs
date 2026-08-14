//! Exact per-model capability declarations with conservative fallback semantics.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ModelCapabilities, ModelRef};

/// Application-owned capability declarations keyed by exact provider and model.
///
/// Wildcards are deliberately excluded: model families evolve independently,
/// so unknown models should use an adapter's conservative fallback instead of
/// silently inheriting stale assumptions.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ModelCapabilityCatalog {
    models: BTreeMap<ModelRef, ModelCapabilities>,
}

impl ModelCapabilityCatalog {
    /// Creates an empty catalog.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            models: BTreeMap::new(),
        }
    }

    /// Inserts or replaces one exact model declaration.
    pub fn insert(
        &mut self,
        model: ModelRef,
        capabilities: ModelCapabilities,
    ) -> Option<ModelCapabilities> {
        self.models.insert(model, capabilities)
    }

    /// Returns the exact declaration for a provider/model pair.
    #[must_use]
    pub fn get(&self, model: &ModelRef) -> Option<&ModelCapabilities> {
        self.models.get(model)
    }

    /// Returns the number of exact declarations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.models.len()
    }

    /// Returns whether the catalog has no declarations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::{FeatureSupport, SupportLevel};

    use super::*;

    #[test]
    fn catalog_requires_exact_provider_and_model_identity() {
        let model = ModelRef::new("openai", "model-a");
        let capabilities = ModelCapabilities {
            tools: FeatureSupport::new(SupportLevel::Native),
            ..ModelCapabilities::default()
        };
        let mut catalog = ModelCapabilityCatalog::new();
        catalog.insert(model.clone(), capabilities.clone());

        assert_eq!(catalog.get(&model), Some(&capabilities));
        assert!(catalog.get(&ModelRef::new("openai", "model-b")).is_none());
        assert!(catalog.get(&ModelRef::new("other", "model-a")).is_none());
    }
}
