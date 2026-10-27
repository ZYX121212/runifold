//! MCP request scopes preserve grants and reject conflicting references.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use runifold_core::{
    Budget, BudgetTracker, CapabilityId, CapabilityKind, CapabilitySet, RiskLevel, RunContext,
};
use runifold_mcp::{
    CallToolParams, CompleteParams, CompleteResult, Completion, CompletionArgument,
    CompletionDescriptor, CompletionErrorKind, CompletionReference, CompletionRegistry,
    FunctionCompletion, FunctionPrompt, GetPromptResult, Implementation, McpClient,
    McpClientConfig, McpError, McpServer, PromptArgument, PromptDescriptor, PromptRegistry,
};
use runifold_tool::{FunctionTool, Tool, ToolContext, ToolError, ToolRegistry};
use serde_json::json;

fn prompt_descriptor() -> PromptDescriptor {
    let mut descriptor = PromptDescriptor::new(CapabilityId::new(), "review", "1").expect("prompt");
    descriptor.risk = RiskLevel::High;
    descriptor
        .metadata
        .insert("policy.scope".into(), json!("private"));
    descriptor.prompt.arguments.push(PromptArgument {
        name: "language".into(),
        title: None,
        description: None,
        required: true,
    });
    descriptor
}

fn params() -> CompleteParams {
    CompleteParams {
        reference: CompletionReference::Prompt {
            name: "review".into(),
        },
        argument: CompletionArgument {
            name: "language".into(),
            value: "ru".into(),
        },
        context: None,
    }
}

fn result() -> CompleteResult {
    CompleteResult {
        completion: Completion {
            values: vec!["rust".into()],
            total: Some(1),
            has_more: Some(false),
        },
    }
}

fn authority(grants: CapabilitySet) -> RunContext {
    RunContext::root(BudgetTracker::new(Budget::default()), grants)
}

#[tokio::test]
async fn completion_preserves_the_exact_referenced_grant_without_ambient_authority() {
    let descriptor = prompt_descriptor();
    let expected = descriptor.capability();
    let mut grants = CapabilitySet::new();
    grants.grant(expected.clone());
    grants.grant(
        PromptDescriptor::new(CapabilityId::new(), "other", "1")
            .expect("other prompt")
            .capability(),
    );
    let parent = authority(grants);
    let parent_id = parent.run_id();
    let completion =
        CompletionDescriptor::prompt(descriptor.id, "review", "1").expect("completion");
    let mut registry = CompletionRegistry::new();
    registry
        .register(Arc::new(FunctionCompletion::new(
            completion,
            move |_: &CompleteParams, child: &RunContext| {
                assert_eq!(child.parent_run_id(), Some(parent_id));
                assert_eq!(child.capabilities().len(), 1);
                assert_eq!(child.capabilities().get(expected.id), Some(&expected));
                Ok(result())
            },
        )))
        .expect("register completion");
    assert_eq!(
        registry
            .complete(params(), &parent)
            .await
            .expect("completion")
            .completion
            .values,
        ["rust"]
    );
}

#[tokio::test]
async fn completion_rejects_wrong_grant_kind_or_version_before_handler() {
    for change_kind in [true, false] {
        let descriptor = prompt_descriptor();
        let mut grant = descriptor.capability();
        if change_kind {
            grant.kind = CapabilityKind::Resource;
        } else {
            grant.version = "2".into();
        }
        let mut grants = CapabilitySet::new();
        grants.grant(grant);
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        let mut registry = CompletionRegistry::new();
        registry
            .register(Arc::new(FunctionCompletion::new(
                CompletionDescriptor::prompt(descriptor.id, "review", "1").expect("completion"),
                move |_: &CompleteParams, _: &RunContext| {
                    handler_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(result())
                },
            )))
            .expect("register completion");
        let error = registry
            .complete(params(), &authority(grants))
            .await
            .expect_err("conflicting grant");
        assert_eq!(error.kind, CompletionErrorKind::CapabilityDenied);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn server_completion_binds_reference_identity_and_version_before_handler() {
    for change_id in [true, false] {
        let descriptor = prompt_descriptor();
        let mut grants = CapabilitySet::new();
        grants.grant(descriptor.capability());
        let completion = CompletionDescriptor::prompt(
            if change_id {
                CapabilityId::new()
            } else {
                descriptor.id
            },
            "review",
            if change_id { "1" } else { "2" },
        )
        .expect("completion");
        // Granting both IDs still cannot bind a completion to another reference.
        if change_id {
            grants.grant(completion.capability());
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        let mut completions = CompletionRegistry::new();
        completions
            .register(Arc::new(FunctionCompletion::new(
                completion,
                move |_: &CompleteParams, _: &RunContext| {
                    handler_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(result())
                },
            )))
            .expect("register completion");
        let mut prompts = PromptRegistry::new();
        prompts
            .register(Arc::new(FunctionPrompt::new(
                descriptor,
                |_: &BTreeMap<String, String>, _: &RunContext| {
                    Ok(GetPromptResult {
                        description: None,
                        messages: Vec::new(),
                    })
                },
            )))
            .expect("register prompt");
        let server = McpServer::new(
            Arc::new(ToolRegistry::new()),
            authority(grants),
            Implementation::new("server", "1"),
        )
        .with_prompt_registry(Arc::new(prompts))
        .with_completion_registry(Arc::new(completions));
        let client = McpClient::new(
            Arc::new(server.session()),
            McpClientConfig::new(Implementation::new("client", "1")),
        );
        client.initialize().await.expect("initialize");
        assert!(matches!(
            client.complete(params()).await,
            Err(McpError::Remote {
                code: -32601 | -32602,
                ..
            })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn tool_descriptor_mismatch_is_denied_without_panicking_or_dispatching() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let tool = Arc::new(FunctionTool::<String, String, _>::new(
        "echo",
        "Echo",
        move |input: String, _: ToolContext| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, ToolError>(input) }
        },
    ));
    let mut grant = tool.descriptor().capability();
    grant.version = "old".into();
    let mut grants = CapabilitySet::new();
    grants.grant(grant);
    let mut tools = ToolRegistry::new();
    tools.register(tool).expect("register tool");
    let server = McpServer::new(
        Arc::new(tools),
        authority(grants),
        Implementation::new("server", "1"),
    );
    let client = McpClient::new(
        Arc::new(server.session()),
        McpClientConfig::new(Implementation::new("client", "1")),
    );
    client.initialize().await.expect("initialize");
    assert!(matches!(
        client
            .call_tool(CallToolParams {
                name: "echo".into(),
                arguments: None
            })
            .await,
        Err(McpError::Remote {
            code: -32601 | -32602,
            ..
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
