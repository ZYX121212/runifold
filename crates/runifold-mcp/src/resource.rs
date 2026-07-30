use std::{collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc};

use base64::Engine;
use futures_util::future::{Either, select};
use runifold_core::{
    CapabilityDescriptor, CapabilityId, CapabilityKind, CapabilitySet, EffectClass, RiskLevel,
    RunContext,
};
use serde_json::{Value, json};
use thiserror::Error;
use url::Url;

use crate::{McpResource, McpResourceTemplate, ReadResourceResult, ResourceContents};

const DEFAULT_MAX_CONTENT_BYTES: usize = 4 * 1024 * 1024;

/// Boxed future returned by a resource handler.
pub type ResourceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReadResourceResult, ResourceError>> + Send + 'a>>;

/// Stable resource failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResourceErrorKind {
    /// The URI is not registered.
    NotFound,
    /// The run lacks authority to read the resource.
    CapabilityDenied,
    /// Resource output violated its descriptor or size contract.
    InvalidOutput,
    /// Resource reading was cancelled.
    Cancelled,
    /// The effective deadline elapsed.
    DeadlineExceeded,
    /// The handler failed.
    Execution,
}

/// Safe resource-read failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct ResourceError {
    /// Stable failure category.
    pub kind: ResourceErrorKind,
    /// Safe operator-facing explanation.
    pub message: String,
}

impl ResourceError {
    /// Creates a resource error.
    pub fn new(kind: ResourceErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

/// Resource registration failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResourceRegistrationError {
    /// Resource name is blank.
    #[error("resource name must not be blank")]
    EmptyName,
    /// Resource URI is not absolute and valid.
    #[error("invalid resource URI `{uri}`: {message}")]
    InvalidUri {
        /// Rejected URI.
        uri: String,
        /// Safe parser explanation.
        message: String,
    },
    /// Annotation priority is outside 0.0 through 1.0.
    #[error("resource annotation priority must be finite and between 0.0 and 1.0")]
    InvalidPriority,
    /// A resource with the same URI is already registered.
    #[error("resource URI `{0}` is already registered")]
    DuplicateUri(String),
    /// URI template is blank or structurally invalid.
    #[error("invalid resource URI template `{0}`")]
    InvalidUriTemplate(String),
    /// A resource template with the same URI template is already registered.
    #[error("resource URI template `{0}` is already registered")]
    DuplicateUriTemplate(String),
}

/// Host-only resource-template contract and grantable capability.
#[derive(Clone, Debug)]
pub struct ResourceTemplateDescriptor {
    /// Stable capability identity.
    pub id: CapabilityId,
    /// Semantic contract version.
    pub version: String,
    /// Model-facing MCP resource-template descriptor.
    pub template: McpResourceTemplate,
    /// Host-selected risk of reading a matched resource.
    pub risk: RiskLevel,
    /// Host-only namespaced metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl ResourceTemplateDescriptor {
    /// Creates a resource-template contract.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceRegistrationError`] for a blank name or invalid URI template.
    pub fn new(
        id: CapabilityId,
        uri_template: impl Into<String>,
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, ResourceRegistrationError> {
        let uri_template = uri_template.into();
        let name = name.into();
        validate_uri_template(&uri_template)?;
        if name.trim().is_empty() {
            return Err(ResourceRegistrationError::EmptyName);
        }
        Ok(Self {
            id,
            version: version.into(),
            template: McpResourceTemplate {
                uri_template,
                name,
                title: None,
                description: None,
                mime_type: None,
                icons: Vec::new(),
                annotations: None,
                meta: BTreeMap::new(),
            },
            risk: RiskLevel::Low,
            metadata: BTreeMap::new(),
        })
    }

    /// Converts the template into a grantable runtime capability.
    pub fn capability(&self) -> CapabilityDescriptor {
        CapabilityDescriptor {
            id: self.id,
            name: self.template.name.clone(),
            version: self.version.clone(),
            kind: CapabilityKind::Resource,
            input_schema: json!({
                "type": "object",
                "properties": {"uri": {"type": "string"}},
                "required": ["uri"],
                "additionalProperties": false
            }),
            output_schema: json!({"type": "object", "required": ["contents"]}),
            effect: EffectClass::ReadOnly,
            risk: self.risk,
            metadata: self.metadata.clone(),
        }
    }

    fn validate(&self) -> Result<(), ResourceRegistrationError> {
        validate_uri_template(&self.template.uri_template)?;
        if self.template.name.trim().is_empty() {
            return Err(ResourceRegistrationError::EmptyName);
        }
        validate_priority(self.template.annotations.as_ref())
    }
}

/// Host-only resource contract and grantable capability.
#[derive(Clone, Debug)]
pub struct ResourceDescriptor {
    /// Stable capability identity.
    pub id: CapabilityId,
    /// Semantic contract version.
    pub version: String,
    /// Model-facing MCP resource descriptor.
    pub resource: McpResource,
    /// Host-selected risk of reading this resource.
    pub risk: RiskLevel,
    /// Host-only namespaced metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl ResourceDescriptor {
    /// Validates and creates a readable resource contract.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceRegistrationError`] for a blank name, invalid URI, or
    /// invalid annotation priority.
    pub fn new(
        id: CapabilityId,
        uri: impl Into<String>,
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, ResourceRegistrationError> {
        let uri = uri.into();
        let name = name.into();
        validate_uri(&uri)?;
        if name.trim().is_empty() {
            return Err(ResourceRegistrationError::EmptyName);
        }
        Ok(Self {
            id,
            version: version.into(),
            resource: McpResource {
                uri,
                name,
                title: None,
                description: None,
                mime_type: None,
                icons: Vec::new(),
                annotations: None,
                size: None,
                meta: BTreeMap::new(),
            },
            risk: RiskLevel::Low,
            metadata: BTreeMap::new(),
        })
    }

    /// Converts the resource into a grantable runtime capability.
    pub fn capability(&self) -> CapabilityDescriptor {
        CapabilityDescriptor {
            id: self.id,
            name: self.resource.name.clone(),
            version: self.version.clone(),
            kind: CapabilityKind::Resource,
            input_schema: json!({
                "type": "object",
                "properties": {"uri": {"const": self.resource.uri}},
                "required": ["uri"],
                "additionalProperties": false
            }),
            output_schema: json!({
                "type": "object",
                "required": ["contents"]
            }),
            effect: EffectClass::ReadOnly,
            risk: self.risk,
            metadata: self.metadata.clone(),
        }
    }

    fn validate(&self) -> Result<(), ResourceRegistrationError> {
        validate_uri(&self.resource.uri)?;
        if self.resource.name.trim().is_empty() {
            return Err(ResourceRegistrationError::EmptyName);
        }
        if self
            .resource
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.priority)
            .is_some_and(|priority| !priority.is_finite() || !(0.0..=1.0).contains(&priority))
        {
            return Err(ResourceRegistrationError::InvalidPriority);
        }
        Ok(())
    }
}

/// Object-safe resource read boundary.
pub trait ResourceHandler: Send + Sync {
    /// Returns the immutable resource contract.
    fn descriptor(&self) -> &ResourceDescriptor;

    /// Reads the resource inside an authority-attenuated child run.
    fn read(&self, context: RunContext) -> ResourceFuture<'_>;
}

/// Object-safe parameterized resource boundary.
///
/// `matches_uri` must implement the advertised RFC 6570 contract and must not
/// perform I/O. The registry still validates the concrete URI and authority.
pub trait ResourceTemplateHandler: Send + Sync {
    /// Returns the immutable template contract.
    fn descriptor(&self) -> &ResourceTemplateDescriptor;

    /// Returns whether this template owns one concrete URI.
    fn matches_uri(&self, uri: &str) -> bool;

    /// Reads a matching URI inside an authority-attenuated child run.
    fn read(&self, uri: String, context: RunContext) -> ResourceFuture<'_>;
}

/// Deterministic, capability-gated registry of readable resources.
#[derive(Clone)]
pub struct ResourceRegistry {
    resources: BTreeMap<String, Arc<dyn ResourceHandler>>,
    templates: BTreeMap<String, Arc<dyn ResourceTemplateHandler>>,
    max_content_bytes: usize,
}

impl ResourceRegistry {
    /// Creates an empty registry with a 4 MiB per-read decoded-content limit.
    pub fn new() -> Self {
        Self {
            resources: BTreeMap::new(),
            templates: BTreeMap::new(),
            max_content_bytes: DEFAULT_MAX_CONTENT_BYTES,
        }
    }

    /// Registers a parameterized resource without replacing a template.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceRegistrationError`] for an invalid or duplicate template.
    pub fn register_template(
        &mut self,
        template: Arc<dyn ResourceTemplateHandler>,
    ) -> Result<(), ResourceRegistrationError> {
        template.descriptor().validate()?;
        let uri_template = template.descriptor().template.uri_template.clone();
        if self.templates.contains_key(&uri_template) {
            return Err(ResourceRegistrationError::DuplicateUriTemplate(
                uri_template,
            ));
        }
        self.templates.insert(uri_template, template);
        Ok(())
    }

    /// Replaces the decoded-content limit.
    #[must_use]
    pub const fn with_max_content_bytes(mut self, bytes: usize) -> Self {
        self.max_content_bytes = bytes;
        self
    }

    /// Registers a resource without replacing an existing URI.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceRegistrationError`] for invalid or duplicate
    /// descriptors.
    pub fn register(
        &mut self,
        resource: Arc<dyn ResourceHandler>,
    ) -> Result<(), ResourceRegistrationError> {
        resource.descriptor().validate()?;
        let uri = resource.descriptor().resource.uri.clone();
        if self.resources.contains_key(&uri) {
            return Err(ResourceRegistrationError::DuplicateUri(uri));
        }
        self.resources.insert(uri, resource);
        Ok(())
    }

    /// Lists only resources granted to `authority`.
    pub fn list_authorized(&self, authority: &RunContext) -> Vec<McpResource> {
        self.resources
            .values()
            .filter(|resource| authority.capabilities().contains(resource.descriptor().id))
            .map(|resource| resource.descriptor().resource.clone())
            .collect()
    }

    /// Returns a registered descriptor.
    pub fn descriptor(&self, uri: &str) -> Option<&ResourceDescriptor> {
        self.resources
            .get(uri)
            .map(|resource| resource.descriptor())
    }

    /// Lists only resource templates granted to `authority`.
    pub fn list_templates_authorized(&self, authority: &RunContext) -> Vec<McpResourceTemplate> {
        self.templates
            .values()
            .filter(|template| authority.capabilities().contains(template.descriptor().id))
            .map(|template| template.descriptor().template.clone())
            .collect()
    }

    /// Returns the descriptor of the first deterministic matching template.
    pub fn template_descriptor_for_uri(&self, uri: &str) -> Option<&ResourceTemplateDescriptor> {
        self.templates
            .values()
            .find(|template| template.matches_uri(uri))
            .map(|template| template.descriptor())
    }

    /// Returns a template descriptor by its advertised URI template.
    pub fn template_descriptor(&self, uri_template: &str) -> Option<&ResourceTemplateDescriptor> {
        self.templates
            .get(uri_template)
            .map(|template| template.descriptor())
    }

    /// Returns whether an advertised template declares `variable`.
    pub fn template_has_variable(&self, uri_template: &str, variable: &str) -> bool {
        self.templates.contains_key(uri_template)
            && template_variables(uri_template)
                .is_ok_and(|variables| variables.iter().any(|name| name == variable))
    }

    /// Returns whether an authorized exact resource or template owns `uri`.
    pub fn contains_authorized_uri(&self, uri: &str, authority: &RunContext) -> bool {
        self.resources
            .get(uri)
            .is_some_and(|resource| authority.capabilities().contains(resource.descriptor().id))
            || self.templates.values().any(|template| {
                template.matches_uri(uri)
                    && authority.capabilities().contains(template.descriptor().id)
            })
    }

    /// Reads one resource after explicit capability, deadline, cancellation,
    /// URI, base64, and output-size checks.
    pub fn read<'a>(&'a self, uri: &'a str, authority: &'a RunContext) -> ResourceFuture<'a> {
        Box::pin(async move {
            if let Some(resource) = self.resources.get(uri) {
                return self.read_exact(resource, uri, authority).await;
            }
            let template = self
                .templates
                .values()
                .find(|template| template.matches_uri(uri))
                .ok_or_else(|| {
                    ResourceError::new(ResourceErrorKind::NotFound, "resource is not registered")
                })?;
            let descriptor = template.descriptor();
            if !authority.capabilities().contains(descriptor.id) {
                return Err(ResourceError::new(
                    ResourceErrorKind::CapabilityDenied,
                    "resource capability is not granted",
                ));
            }
            let mut capabilities = CapabilitySet::new();
            capabilities.grant(descriptor.capability());
            let child = authority.child(capabilities).map_err(|error| {
                ResourceError::new(ResourceErrorKind::CapabilityDenied, error.to_string())
            })?;
            if child
                .deadline()
                .is_some_and(|deadline| deadline <= std::time::Instant::now())
            {
                return Err(ResourceError::new(
                    ResourceErrorKind::DeadlineExceeded,
                    "resource deadline already elapsed",
                ));
            }
            let cancellation = child.cancellation().clone();
            let output = match select(
                Box::pin(cancellation.cancelled()),
                Box::pin(template.read(uri.to_owned(), child)),
            )
            .await
            {
                Either::Left(_) => {
                    return Err(ResourceError::new(
                        ResourceErrorKind::Cancelled,
                        "resource read was cancelled",
                    ));
                }
                Either::Right((result, _)) => result?,
            };
            validate_output(uri, &output, self.max_content_bytes)?;
            Ok(output)
        })
    }

    async fn read_exact(
        &self,
        resource: &Arc<dyn ResourceHandler>,
        uri: &str,
        authority: &RunContext,
    ) -> Result<ReadResourceResult, ResourceError> {
        let descriptor = resource.descriptor();
        if !authority.capabilities().contains(descriptor.id) {
            return Err(ResourceError::new(
                ResourceErrorKind::CapabilityDenied,
                "resource capability is not granted",
            ));
        }
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(descriptor.capability());
        let child = authority.child(capabilities).map_err(|error| {
            ResourceError::new(ResourceErrorKind::CapabilityDenied, error.to_string())
        })?;
        if child
            .deadline()
            .is_some_and(|deadline| deadline <= std::time::Instant::now())
        {
            return Err(ResourceError::new(
                ResourceErrorKind::DeadlineExceeded,
                "resource deadline already elapsed",
            ));
        }
        let cancellation = child.cancellation().clone();
        let output = match select(
            Box::pin(cancellation.cancelled()),
            Box::pin(resource.read(child)),
        )
        .await
        {
            Either::Left(_) => {
                return Err(ResourceError::new(
                    ResourceErrorKind::Cancelled,
                    "resource read was cancelled",
                ));
            }
            Either::Right((result, _)) => result?,
        };
        validate_output(uri, &output, self.max_content_bytes)?;
        Ok(output)
    }

    /// Returns the number of registered resources.
    pub fn len(&self) -> usize {
        self.resources.len() + self.templates.len()
    }

    /// Returns whether no resources are registered.
    pub fn is_empty(&self) -> bool {
        self.resources.is_empty() && self.templates.is_empty()
    }
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ResourceRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResourceRegistry")
            .field("uris", &self.resources.keys().collect::<Vec<_>>())
            .field("uri_templates", &self.templates.keys().collect::<Vec<_>>())
            .field("max_content_bytes", &self.max_content_bytes)
            .finish()
    }
}

/// Immutable text resource for common configuration and documentation cases.
#[derive(Clone, Debug)]
pub struct StaticTextResource {
    descriptor: ResourceDescriptor,
    text: String,
}

impl StaticTextResource {
    /// Creates an immutable text resource.
    pub fn new(descriptor: ResourceDescriptor, text: impl Into<String>) -> Self {
        Self {
            descriptor,
            text: text.into(),
        }
    }
}

impl ResourceHandler for StaticTextResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn read(&self, _context: RunContext) -> ResourceFuture<'_> {
        Box::pin(async move {
            Ok(ReadResourceResult {
                contents: vec![ResourceContents::text(
                    self.descriptor.resource.uri.clone(),
                    self.text.clone(),
                )],
                ttl_ms: None,
                cache_scope: None,
            })
        })
    }
}

fn validate_uri(uri: &str) -> Result<(), ResourceRegistrationError> {
    Url::parse(uri)
        .map(|_| ())
        .map_err(|error| ResourceRegistrationError::InvalidUri {
            uri: uri.to_owned(),
            message: error.to_string(),
        })
}

fn validate_uri_template(template: &str) -> Result<(), ResourceRegistrationError> {
    let Some(first_brace) = template.find('{') else {
        return Err(ResourceRegistrationError::InvalidUriTemplate(
            template.to_owned(),
        ));
    };
    if first_brace == 0 || !template[..first_brace].contains(':') {
        return Err(ResourceRegistrationError::InvalidUriTemplate(
            template.to_owned(),
        ));
    }
    if template_variables(template).is_err() {
        return Err(ResourceRegistrationError::InvalidUriTemplate(
            template.to_owned(),
        ));
    }
    Ok(())
}

fn template_variables(template: &str) -> Result<Vec<String>, ()> {
    let mut variables = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let after_open = &rest[open + 1..];
        let close = after_open.find('}').ok_or(())?;
        let expression = &after_open[..close];
        if expression.is_empty() || expression.contains('{') {
            return Err(());
        }
        let expression = expression
            .strip_prefix(['+', '#', '.', '/', ';', '?', '&'])
            .unwrap_or(expression);
        if expression.is_empty() {
            return Err(());
        }
        for variable in expression.split(',') {
            let variable = variable.strip_suffix('*').unwrap_or(variable);
            let name = variable.split_once(':').map_or(variable, |(name, prefix)| {
                if prefix.is_empty()
                    || prefix.len() > 4
                    || !prefix.chars().all(|character| character.is_ascii_digit())
                {
                    ""
                } else {
                    name
                }
            });
            if name.is_empty()
                || !name.split('.').all(|part| {
                    !part.is_empty()
                        && part.chars().all(|character| {
                            character.is_ascii_alphanumeric()
                                || character == '_'
                                || character == '%'
                        })
                })
            {
                return Err(());
            }
            variables.push(name.to_owned());
        }
        rest = &after_open[close + 1..];
        if rest.contains('}') && !rest.contains('{') {
            return Err(());
        }
    }
    if rest.contains('}') || variables.is_empty() {
        return Err(());
    }
    Ok(variables)
}

fn validate_priority(
    annotations: Option<&crate::Annotations>,
) -> Result<(), ResourceRegistrationError> {
    if annotations
        .and_then(|annotations| annotations.priority)
        .is_some_and(|priority| !priority.is_finite() || !(0.0..=1.0).contains(&priority))
    {
        return Err(ResourceRegistrationError::InvalidPriority);
    }
    Ok(())
}

fn validate_output(
    requested_uri: &str,
    output: &ReadResourceResult,
    max_content_bytes: usize,
) -> Result<(), ResourceError> {
    if output.contents.is_empty() {
        return Err(ResourceError::new(
            ResourceErrorKind::InvalidOutput,
            "resource returned no content",
        ));
    }
    let mut total = 0_usize;
    for content in &output.contents {
        if content.uri() != requested_uri {
            return Err(ResourceError::new(
                ResourceErrorKind::InvalidOutput,
                "resource output URI does not match the request",
            ));
        }
        let bytes = match content {
            ResourceContents::Text { text, .. } => text.len(),
            ResourceContents::Blob { blob, .. } => base64::engine::general_purpose::STANDARD
                .decode(blob)
                .map_err(|_| {
                    ResourceError::new(
                        ResourceErrorKind::InvalidOutput,
                        "resource blob is not valid base64",
                    )
                })?
                .len(),
        };
        total = total.saturating_add(bytes);
        if total > max_content_bytes {
            return Err(ResourceError::new(
                ResourceErrorKind::InvalidOutput,
                "resource output exceeds the configured decoded-content limit",
            ));
        }
    }
    Ok(())
}
