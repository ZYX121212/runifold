use std::{collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc};

use futures_util::future::{Either, select};
use runifold_core::{
    CapabilityDescriptor, CapabilityId, CapabilityKind, CapabilitySet, EffectClass, RiskLevel,
    RunContext,
};
use serde_json::{Value, json};
use thiserror::Error;

use crate::{CompleteParams, CompleteResult, CompletionReference};

const MAX_COMPLETION_VALUES: usize = 100;

/// Boxed future returned by a completion provider.
pub type CompletionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CompleteResult, CompletionError>> + Send + 'a>>;

/// Stable completion failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompletionErrorKind {
    /// No completion provider is registered for the reference.
    NotFound,
    /// The run lacks authority to inspect completion values.
    CapabilityDenied,
    /// The request or provider output violates the completion contract.
    InvalidInput,
    /// Provider output violates the completion result contract.
    InvalidOutput,
    /// Completion was cancelled.
    Cancelled,
    /// The effective deadline elapsed.
    DeadlineExceeded,
    /// The provider failed.
    Execution,
}

/// Safe completion failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct CompletionError {
    /// Stable failure category.
    pub kind: CompletionErrorKind,
    /// Safe operator-facing explanation.
    pub message: String,
}

impl CompletionError {
    /// Creates a completion error.
    pub fn new(kind: CompletionErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

/// Completion-provider registration failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum CompletionRegistrationError {
    /// Reference name or URI template is blank.
    #[error("completion reference must not be blank")]
    EmptyReference,
    /// A provider already owns this reference.
    #[error("completion provider `{0}` is already registered")]
    DuplicateReference(String),
}

/// Host-only completion contract.
#[derive(Clone, Debug)]
pub struct CompletionDescriptor {
    /// Stable capability identity shared with the referenced prompt/resource.
    pub id: CapabilityId,
    /// Semantic contract version.
    pub version: String,
    /// Prompt name or resource URI template.
    pub reference: CompletionReference,
    /// Host-selected risk of revealing suggestions.
    pub risk: RiskLevel,
    /// Host-only namespaced metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl CompletionDescriptor {
    /// Creates a prompt-argument completion contract.
    ///
    /// # Errors
    ///
    /// Returns [`CompletionRegistrationError`] when the prompt name is blank.
    pub fn prompt(
        id: CapabilityId,
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, CompletionRegistrationError> {
        Self::new(
            id,
            CompletionReference::Prompt { name: name.into() },
            version,
        )
    }

    /// Creates a resource-template-argument completion contract.
    ///
    /// # Errors
    ///
    /// Returns [`CompletionRegistrationError`] when the URI template is blank.
    pub fn resource(
        id: CapabilityId,
        uri: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, CompletionRegistrationError> {
        Self::new(
            id,
            CompletionReference::Resource { uri: uri.into() },
            version,
        )
    }

    fn new(
        id: CapabilityId,
        reference: CompletionReference,
        version: impl Into<String>,
    ) -> Result<Self, CompletionRegistrationError> {
        if reference_value(&reference).trim().is_empty() {
            return Err(CompletionRegistrationError::EmptyReference);
        }
        Ok(Self {
            id,
            version: version.into(),
            reference,
            risk: RiskLevel::Low,
            metadata: BTreeMap::new(),
        })
    }

    /// Converts the completion contract into attenuated runtime authority.
    pub fn capability(&self) -> CapabilityDescriptor {
        CapabilityDescriptor {
            id: self.id,
            name: format!("completion:{}", reference_value(&self.reference)),
            version: self.version.clone(),
            kind: match self.reference {
                CompletionReference::Prompt { .. } => CapabilityKind::Prompt,
                CompletionReference::Resource { .. } => CapabilityKind::Resource,
            },
            input_schema: json!({"type": "object"}),
            output_schema: json!({"type": "object", "required": ["completion"]}),
            effect: EffectClass::ReadOnly,
            risk: self.risk,
            metadata: self.metadata.clone(),
        }
    }
}

/// Object-safe completion boundary.
pub trait CompletionProvider: Send + Sync {
    /// Returns the immutable completion contract.
    fn descriptor(&self) -> &CompletionDescriptor;

    /// Suggests values inside an authority-attenuated child run.
    fn complete(&self, params: CompleteParams, context: RunContext) -> CompletionFuture<'_>;
}

/// Completion provider backed by a synchronous Rust closure.
pub struct FunctionCompletion<F> {
    descriptor: CompletionDescriptor,
    complete: F,
}

impl<F> FunctionCompletion<F> {
    /// Creates a closure-backed completion provider.
    pub fn new(descriptor: CompletionDescriptor, complete: F) -> Self {
        Self {
            descriptor,
            complete,
        }
    }
}

impl<F> CompletionProvider for FunctionCompletion<F>
where
    F: Fn(&CompleteParams, &RunContext) -> Result<CompleteResult, CompletionError> + Send + Sync,
{
    fn descriptor(&self) -> &CompletionDescriptor {
        &self.descriptor
    }

    fn complete(&self, params: CompleteParams, context: RunContext) -> CompletionFuture<'_> {
        Box::pin(async move { (self.complete)(&params, &context) })
    }
}

impl<F> fmt::Debug for FunctionCompletion<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FunctionCompletion")
            .field("descriptor", &self.descriptor)
            .field("complete", &"<function>")
            .finish()
    }
}

/// Deterministic, capability-gated completion registry.
#[derive(Clone, Default)]
pub struct CompletionRegistry {
    providers: BTreeMap<String, Arc<dyn CompletionProvider>>,
}

impl CompletionRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a provider without replacing an existing reference.
    ///
    /// # Errors
    ///
    /// Returns [`CompletionRegistrationError`] when the reference is already registered.
    pub fn register(
        &mut self,
        provider: Arc<dyn CompletionProvider>,
    ) -> Result<(), CompletionRegistrationError> {
        let key = reference_key(&provider.descriptor().reference);
        if self.providers.contains_key(&key) {
            return Err(CompletionRegistrationError::DuplicateReference(key));
        }
        self.providers.insert(key, provider);
        Ok(())
    }

    /// Returns a registered descriptor.
    pub fn descriptor(&self, reference: &CompletionReference) -> Option<&CompletionDescriptor> {
        self.providers
            .get(&reference_key(reference))
            .map(|provider| provider.descriptor())
    }

    /// Returns whether any completion provider is registered.
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// Completes one authorized prompt or resource-template argument.
    pub fn complete<'a>(
        &'a self,
        params: CompleteParams,
        authority: &'a RunContext,
    ) -> CompletionFuture<'a> {
        Box::pin(async move {
            let provider = self
                .providers
                .get(&reference_key(&params.reference))
                .ok_or_else(|| {
                    CompletionError::new(
                        CompletionErrorKind::NotFound,
                        "completion reference is not registered",
                    )
                })?;
            let descriptor = provider.descriptor();
            if !authority.capabilities().contains(descriptor.id) {
                return Err(CompletionError::new(
                    CompletionErrorKind::CapabilityDenied,
                    "completion capability is not granted",
                ));
            }
            if params.argument.name.trim().is_empty() {
                return Err(CompletionError::new(
                    CompletionErrorKind::InvalidInput,
                    "completion argument name must not be blank",
                ));
            }
            let mut capabilities = CapabilitySet::new();
            capabilities.grant(descriptor.capability());
            let child = authority.child(capabilities).map_err(|error| {
                CompletionError::new(CompletionErrorKind::CapabilityDenied, error.to_string())
            })?;
            if child
                .deadline()
                .is_some_and(|deadline| deadline <= std::time::Instant::now())
            {
                return Err(CompletionError::new(
                    CompletionErrorKind::DeadlineExceeded,
                    "completion deadline already elapsed",
                ));
            }
            let cancellation = child.cancellation().clone();
            let result = match select(
                Box::pin(cancellation.cancelled()),
                Box::pin(provider.complete(params, child)),
            )
            .await
            {
                Either::Left(_) => {
                    return Err(CompletionError::new(
                        CompletionErrorKind::Cancelled,
                        "completion was cancelled",
                    ));
                }
                Either::Right((result, _)) => result?,
            };
            validate_result(&result)?;
            Ok(result)
        })
    }
}

impl fmt::Debug for CompletionRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompletionRegistry")
            .field("references", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn reference_key(reference: &CompletionReference) -> String {
    match reference {
        CompletionReference::Prompt { name } => format!("prompt:{name}"),
        CompletionReference::Resource { uri } => format!("resource:{uri}"),
    }
}

fn reference_value(reference: &CompletionReference) -> &str {
    match reference {
        CompletionReference::Prompt { name } => name,
        CompletionReference::Resource { uri } => uri,
    }
}

fn validate_result(result: &CompleteResult) -> Result<(), CompletionError> {
    if result.completion.values.len() > MAX_COMPLETION_VALUES {
        return Err(CompletionError::new(
            CompletionErrorKind::InvalidOutput,
            "completion returned more than 100 values",
        ));
    }
    if result.completion.values.iter().any(String::is_empty) {
        return Err(CompletionError::new(
            CompletionErrorKind::InvalidOutput,
            "completion values must not be empty",
        ));
    }
    if result
        .completion
        .total
        .is_some_and(|total| total < result.completion.values.len() as u64)
    {
        return Err(CompletionError::new(
            CompletionErrorKind::InvalidOutput,
            "completion total is smaller than the returned value count",
        ));
    }
    Ok(())
}
