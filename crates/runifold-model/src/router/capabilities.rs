//! Conservative capability aggregation across physical model routes.

use std::collections::BTreeSet;

use super::ModelCapabilities;

pub(super) fn intersect_capabilities(mut all: Vec<ModelCapabilities>) -> ModelCapabilities {
    let Some(mut intersection) = all.pop() else {
        return ModelCapabilities::default();
    };
    for capabilities in all {
        intersect_feature(&mut intersection.streaming, &capabilities.streaming);
        intersect_feature(&mut intersection.tools, &capabilities.tools);
        intersect_feature(
            &mut intersection.parallel_tools,
            &capabilities.parallel_tools,
        );
        intersect_feature(
            &mut intersection.structured_output,
            &capabilities.structured_output,
        );
        intersect_feature(&mut intersection.reasoning, &capabilities.reasoning);
        intersect_feature(&mut intersection.image_input, &capabilities.image_input);
        intersect_feature(&mut intersection.audio_input, &capabilities.audio_input);
        intersect_feature(
            &mut intersection.document_input,
            &capabilities.document_input,
        );
        intersection.max_context_tokens = match (
            intersection.max_context_tokens,
            capabilities.max_context_tokens,
        ) {
            (Some(left), Some(right)) => Some(left.min(right)),
            _ => None,
        };
        let common = intersection
            .extensions
            .keys()
            .filter(|key| capabilities.extensions.contains_key(*key))
            .cloned()
            .collect::<BTreeSet<_>>();
        intersection.extensions.retain(|key, support| {
            if !common.contains(key) {
                return false;
            }
            if let Some(other) = capabilities.extensions.get(key) {
                intersect_feature(support, other);
            }
            true
        });
    }
    intersection
}

fn intersect_feature(left: &mut crate::FeatureSupport, right: &crate::FeatureSupport) {
    use crate::SupportLevel::{Emulated, Native, Unknown, Unsupported};

    let constraints_match = left.constraints == right.constraints;
    left.level = match (left.level, right.level) {
        (Unsupported, _) | (_, Unsupported) => Unsupported,
        (Unknown, _) | (_, Unknown) => Unknown,
        (Emulated, _) | (_, Emulated) => Emulated,
        (Native, Native) => Native,
    };
    if !constraints_match {
        if left.level != Unsupported {
            left.level = Unknown;
        }
        left.constraints.clear();
    }
}
