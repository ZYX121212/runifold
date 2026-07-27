use std::{collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc};

use futures_util::future::{Either, select};
use runifold_core::{
    CapabilityDescriptor, CapabilityId, CapabilityKind, CapabilitySet, EffectClass, RiskLevel,
    RunContext,
};
use serde_json::{Value, json};
use thiserror::Error;

use crate::{GetPromptResult, McpPrompt};

const DEFAULT_MAX_MESSAGES: usize = 64;
const DEFAULT_MAX_SERIALIZED_BYTES: usize = 1024 * 1024;

/// Boxed future returned by a prompt handler.
pub type PromptFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GetPromptResult, PromptError>> + Send + 'a>>;

/// Stable prompt failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PromptErrorKind {
    /// The prompt name is not registered.
    NotFound,
    /// The run lacks authority to render the prompt.
    CapabilityDenied,
    /// Caller arguments violate the prompt contract.
    InvalidArguments,
    /// Rendered messages violate output limits.
    InvalidOutput,
    /// Prompt rendering was cancelled.
    Cancelled,
    /// The effective deadline elapsed.
    DeadlineExceeded,
    /// The handler failed.
    Execution,
}

/// Safe prompt-render failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct PromptError {
    /// Stable failure category.
    pub kind: PromptErrorKind,
    /// Safe operator-facing explanation.
    pub message: String,
}

impl PromptError {
    /// Creates a prompt error.
    pub fn new(kind: PromptErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

/// Prompt registration failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum PromptRegistrationError {
    /// Prompt name is blank.
    #[error("prompt name must not be blank")]
    EmptyName,
    /// One argument name is blank.
    #[error("prompt argument name must not be blank")]
    EmptyArgumentName,
    /// An argument name appears more than once.
    #[error("prompt argument `{0}` is declared more than once")]
    DuplicateArgument(String),
    /// A prompt with the same name is already registered.
    #[error("prompt `{0}` is already registered")]
    DuplicateName(String),
}

/// Host-only prompt contract and grantable capability.
#[derive(Clone, Debug)]
pub struct PromptDescriptor {
    /// Stable capability identity.
    pub id: CapabilityId,
    /// Semantic contract version.
    pub version: String,
    /// Model-facing MCP prompt descriptor.
    pub prompt: McpPrompt,
    /// Host-selected prompt-rendering risk.
    pub risk: RiskLevel,
    /// Host-only namespaced metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl PromptDescriptor {
    /// Creates a prompt contract.
    ///
    /// # Errors
    ///
    /// Returns [`PromptRegistrationError`] when the name is blank.
    pub fn new(
        id: CapabilityId,
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, PromptRegistrationError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(PromptRegistrationError::EmptyName);
        }
        Ok(Self {
            id,
            version: version.into(),
            prompt: McpPrompt {
                name,
                title: None,
                description: None,
                arguments: Vec::new(),
                icons: Vec::new(),
                meta: BTreeMap::new(),
            },
            risk: RiskLevel::Low,
            metadata: BTreeMap::new(),
        })
    }

    /// Converts the prompt into a grantable runtime capability.
    pub fn capability(&self) -> CapabilityDescriptor {
        let properties = self
            .prompt
            .arguments
            .iter()
            .map(|argument| {
                (
                    argument.name.clone(),
                    json!({
                        "type": "string",
                        "description": argument.description
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let required = self
            .prompt
            .arguments
            .iter()
            .filter(|argument| argument.required)
            .map(|argument| Value::String(argument.name.clone()))
            .collect::<Vec<_>>();
        CapabilityDescriptor {
            id: self.id,
            name: self.prompt.name.clone(),
            version: self.version.clone(),
            kind: CapabilityKind::Prompt,
            input_schema: json!({
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "required": ["messages"]
            }),
            effect: EffectClass::Pure,
            risk: self.risk,
            metadata: self.metadata.clone(),
        }
    }

    fn validate(&self) -> Result<(), PromptRegistrationError> {
        if self.prompt.name.trim().is_empty() {
            return Err(PromptRegistrationError::EmptyName);
        }
        let mut names = std::collections::HashSet::new();
        for argument in &self.prompt.arguments {
            if argument.name.trim().is_empty() {
                return Err(PromptRegistrationError::EmptyArgumentName);
            }
            if !names.insert(argument.name.as_str()) {
                return Err(PromptRegistrationError::DuplicateArgument(
                    argument.name.clone(),
                ));
            }
        }
        Ok(())
    }
}

/// Object-safe prompt rendering boundary.
pub trait PromptHandler: Send + Sync {
    /// Returns the immutable prompt contract.
    fn descriptor(&self) -> &PromptDescriptor;

    /// Renders prompt messages inside an authority-attenuated child run.
    fn render(&self, arguments: BTreeMap<String, String>, context: RunContext) -> PromptFuture<'_>;
}

/// Prompt backed by a synchronous Rust closure.
///
/// The registry validates declared arguments before invoking the closure.
/// Closures must remain non-blocking; asynchronous work belongs in a custom
/// [`PromptHandler`] implementation.
pub struct FunctionPrompt<F> {
    descriptor: PromptDescriptor,
    render: F,
}

impl<F> FunctionPrompt<F> {
    /// Creates a closure-backed prompt.
    pub fn new(descriptor: PromptDescriptor, render: F) -> Self {
        Self { descriptor, render }
    }
}

impl<F> PromptHandler for FunctionPrompt<F>
where
    F: Fn(&BTreeMap<String, String>, &RunContext) -> Result<GetPromptResult, PromptError>
        + Send
        + Sync,
{
    fn descriptor(&self) -> &PromptDescriptor {
        &self.descriptor
    }

    fn render(&self, arguments: BTreeMap<String, String>, context: RunContext) -> PromptFuture<'_> {
        Box::pin(async move { (self.render)(&arguments, &context) })
    }
}

impl<F> fmt::Debug for FunctionPrompt<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FunctionPrompt")
            .field("descriptor", &self.descriptor)
            .field("render", &"<function>")
            .finish()
    }
}

/// Deterministic, capability-gated prompt registry.
#[derive(Clone)]
pub struct PromptRegistry {
    prompts: BTreeMap<String, Arc<dyn PromptHandler>>,
    max_messages: usize,
    max_serialized_bytes: usize,
}

impl PromptRegistry {
    /// Creates an empty registry with conservative output limits.
    pub fn new() -> Self {
        Self {
            prompts: BTreeMap::new(),
            max_messages: DEFAULT_MAX_MESSAGES,
            max_serialized_bytes: DEFAULT_MAX_SERIALIZED_BYTES,
        }
    }

    /// Replaces the maximum rendered message count.
    #[must_use]
    pub const fn with_max_messages(mut self, messages: usize) -> Self {
        self.max_messages = messages;
        self
    }

    /// Replaces the maximum serialized result size.
    #[must_use]
    pub const fn with_max_serialized_bytes(mut self, bytes: usize) -> Self {
        self.max_serialized_bytes = bytes;
        self
    }

    /// Registers a prompt without replacing an existing name.
    ///
    /// # Errors
    ///
    /// Returns [`PromptRegistrationError`] for invalid or duplicate contracts.
    pub fn register(
        &mut self,
        prompt: Arc<dyn PromptHandler>,
    ) -> Result<(), PromptRegistrationError> {
        prompt.descriptor().validate()?;
        let name = prompt.descriptor().prompt.name.clone();
        if self.prompts.contains_key(&name) {
            return Err(PromptRegistrationError::DuplicateName(name));
        }
        self.prompts.insert(name, prompt);
        Ok(())
    }

    /// Lists only prompts granted to `authority`.
    pub fn list_authorized(&self, authority: &RunContext) -> Vec<McpPrompt> {
        self.prompts
            .values()
            .filter(|prompt| authority.capabilities().contains(prompt.descriptor().id))
            .map(|prompt| prompt.descriptor().prompt.clone())
            .collect()
    }

    /// Returns a registered descriptor.
    pub fn descriptor(&self, name: &str) -> Option<&PromptDescriptor> {
        self.prompts.get(name).map(|prompt| prompt.descriptor())
    }

    /// Validates arguments and renders one authorized prompt.
    pub fn render<'a>(
        &'a self,
        name: &'a str,
        arguments: BTreeMap<String, String>,
        authority: &'a RunContext,
    ) -> PromptFuture<'a> {
        Box::pin(async move {
            let prompt = self.prompts.get(name).ok_or_else(|| {
                PromptError::new(PromptErrorKind::NotFound, "prompt is not registered")
            })?;
            let descriptor = prompt.descriptor();
            if !authority.capabilities().contains(descriptor.id) {
                return Err(PromptError::new(
                    PromptErrorKind::CapabilityDenied,
                    "prompt capability is not granted",
                ));
            }
            validate_arguments(&descriptor.prompt, &arguments)?;
            let mut capabilities = CapabilitySet::new();
            capabilities.grant(descriptor.capability());
            let child = authority.child(capabilities);
            if child
                .deadline()
                .is_some_and(|deadline| deadline <= std::time::Instant::now())
            {
                return Err(PromptError::new(
                    PromptErrorKind::DeadlineExceeded,
                    "prompt deadline already elapsed",
                ));
            }
            let cancellation = child.cancellation().clone();
            let output = match select(
                Box::pin(cancellation.cancelled()),
                Box::pin(prompt.render(arguments, child)),
            )
            .await
            {
                Either::Left(_) => {
                    return Err(PromptError::new(
                        PromptErrorKind::Cancelled,
                        "prompt rendering was cancelled",
                    ));
                }
                Either::Right((result, _)) => result?,
            };
            validate_output(&output, self.max_messages, self.max_serialized_bytes)?;
            Ok(output)
        })
    }

    /// Returns the number of registered prompts.
    pub fn len(&self) -> usize {
        self.prompts.len()
    }

    /// Returns whether no prompts are registered.
    pub fn is_empty(&self) -> bool {
        self.prompts.is_empty()
    }
}

impl Default for PromptRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for PromptRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PromptRegistry")
            .field("names", &self.prompts.keys().collect::<Vec<_>>())
            .field("max_messages", &self.max_messages)
            .field("max_serialized_bytes", &self.max_serialized_bytes)
            .finish()
    }
}

fn validate_arguments(
    prompt: &McpPrompt,
    arguments: &BTreeMap<String, String>,
) -> Result<(), PromptError> {
    if let Some(missing) = prompt
        .arguments
        .iter()
        .find(|argument| argument.required && !arguments.contains_key(&argument.name))
    {
        return Err(PromptError::new(
            PromptErrorKind::InvalidArguments,
            format!("required prompt argument `{}` is missing", missing.name),
        ));
    }
    if let Some(unknown) = arguments.keys().find(|name| {
        !prompt
            .arguments
            .iter()
            .any(|argument| &argument.name == *name)
    }) {
        return Err(PromptError::new(
            PromptErrorKind::InvalidArguments,
            format!("unknown prompt argument `{unknown}`"),
        ));
    }
    Ok(())
}

fn validate_output(
    output: &GetPromptResult,
    max_messages: usize,
    max_serialized_bytes: usize,
) -> Result<(), PromptError> {
    if output.messages.is_empty() {
        return Err(PromptError::new(
            PromptErrorKind::InvalidOutput,
            "prompt returned no messages",
        ));
    }
    if output.messages.len() > max_messages {
        return Err(PromptError::new(
            PromptErrorKind::InvalidOutput,
            "prompt returned too many messages",
        ));
    }
    let bytes = serde_json::to_vec(output)
        .map_err(|_| PromptError::new(PromptErrorKind::InvalidOutput, "prompt cannot be encoded"))?
        .len();
    if bytes > max_serialized_bytes {
        return Err(PromptError::new(
            PromptErrorKind::InvalidOutput,
            "prompt output exceeds the configured serialized-size limit",
        ));
    }
    Ok(())
}
