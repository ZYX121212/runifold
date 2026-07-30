use std::{
    collections::BTreeSet,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::{
    JsonRpcNotification, McpError, RequestId, ServerNotificationStream, StatelessRequestMetadata,
};

pub(crate) const SUBSCRIPTION_ID_META_KEY: &str = "io.modelcontextprotocol/subscriptionId";

/// Notification classes requested on one `subscriptions/listen` stream.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionFilter {
    /// Receive `notifications/tools/list_changed`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub tools_list_changed: bool,
    /// Receive `notifications/prompts/list_changed`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub prompts_list_changed: bool,
    /// Receive `notifications/resources/list_changed`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub resources_list_changed: bool,
    /// Receive `notifications/resources/updated` for these exact URIs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_subscriptions: Vec<String>,
    /// Receive `notifications/tasks` for these exact durable Task IDs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_ids: Vec<String>,
}

impl SubscriptionFilter {
    pub(crate) fn normalized(mut self) -> Self {
        let resources = self
            .resource_subscriptions
            .into_iter()
            .collect::<BTreeSet<_>>();
        self.resource_subscriptions = resources.into_iter().collect();
        let task_ids = self
            .task_ids
            .into_iter()
            .filter(|task_id| !task_id.is_empty())
            .collect::<BTreeSet<_>>();
        self.task_ids = task_ids.into_iter().collect();
        self
    }

    pub(crate) fn accepts(&self, notification: &JsonRpcNotification) -> bool {
        match notification.method.as_str() {
            "notifications/tools/list_changed" => self.tools_list_changed,
            "notifications/prompts/list_changed" => self.prompts_list_changed,
            "notifications/resources/list_changed" => self.resources_list_changed,
            "notifications/resources/updated" => notification
                .params
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|params| params.get("uri"))
                .and_then(Value::as_str)
                .is_some_and(|uri| {
                    self.resource_subscriptions
                        .binary_search_by(|candidate| candidate.as_str().cmp(uri))
                        .is_ok()
                }),
            "notifications/tasks" => notification
                .params
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|params| params.get("taskId"))
                .and_then(Value::as_str)
                .is_some_and(|task_id| {
                    self.task_ids
                        .binary_search_by(|candidate| candidate.as_str().cmp(task_id))
                        .is_ok()
                }),
            _ => false,
        }
    }

    pub(crate) fn is_subset_of(&self, requested: &Self) -> bool {
        (!self.tools_list_changed || requested.tools_list_changed)
            && (!self.prompts_list_changed || requested.prompts_list_changed)
            && (!self.resources_list_changed || requested.resources_list_changed)
            && self.resource_subscriptions.iter().all(|uri| {
                requested
                    .resource_subscriptions
                    .iter()
                    .any(|requested| requested == uri)
            })
            && self.task_ids.iter().all(|task_id| {
                requested
                    .task_ids
                    .iter()
                    .any(|requested| requested == task_id)
            })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct SubscriptionsListenParams {
    pub(crate) notifications: SubscriptionFilter,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<StatelessRequestMetadata>,
}

impl SubscriptionsListenParams {
    pub(crate) fn new(notifications: SubscriptionFilter) -> Self {
        Self {
            notifications,
            metadata: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SubscriptionAcknowledgedParams {
    pub(crate) notifications: SubscriptionFilter,
    #[serde(rename = "_meta")]
    metadata: Map<String, Value>,
}

impl SubscriptionAcknowledgedParams {
    pub(crate) fn subscription_id(&self) -> Result<RequestId, McpError> {
        self.metadata
            .get(SUBSCRIPTION_ID_META_KEY)
            .cloned()
            .ok_or_else(|| McpError::protocol("subscription acknowledgment omitted its id"))
            .and_then(|value| serde_json::from_value(value).map_err(Into::into))
    }
}

pub(crate) fn acknowledgement(
    id: &RequestId,
    notifications: &SubscriptionFilter,
) -> JsonRpcNotification {
    JsonRpcNotification::new(
        "notifications/subscriptions/acknowledged",
        Some(json!({
            "notifications": notifications,
            "_meta": {(SUBSCRIPTION_ID_META_KEY): id},
        })),
    )
}

pub(crate) fn attach_subscription_id(
    mut notification: JsonRpcNotification,
    id: &RequestId,
) -> JsonRpcNotification {
    let params = notification
        .params
        .get_or_insert_with(|| Value::Object(Map::new()));
    let Some(params) = params.as_object_mut() else {
        return notification;
    };
    let metadata = params
        .entry("_meta")
        .or_insert_with(|| Value::Object(Map::new()));
    if let Some(metadata) = metadata.as_object_mut() {
        metadata.insert(
            SUBSCRIPTION_ID_META_KEY.to_owned(),
            serde_json::to_value(id).unwrap_or(Value::Null),
        );
    }
    notification
}

pub(crate) fn notification_subscription_id(
    notification: &JsonRpcNotification,
) -> Option<RequestId> {
    notification
        .params
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get(SUBSCRIPTION_ID_META_KEY))
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

/// One acknowledged long-lived MCP notification subscription.
pub struct McpSubscription {
    id: RequestId,
    accepted: SubscriptionFilter,
    stream: ServerNotificationStream,
}

impl McpSubscription {
    pub(crate) fn new(
        id: RequestId,
        accepted: SubscriptionFilter,
        stream: ServerNotificationStream,
    ) -> Self {
        Self {
            id,
            accepted,
            stream,
        }
    }

    /// Returns the JSON-RPC request ID that owns this subscription.
    pub const fn id(&self) -> &RequestId {
        &self.id
    }

    /// Returns the notification filter accepted by the server.
    pub const fn accepted(&self) -> &SubscriptionFilter {
        &self.accepted
    }
}

impl Stream for McpSubscription {
    type Item = Result<JsonRpcNotification, McpError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().stream.as_mut().poll_next(context)
    }
}

impl std::fmt::Debug for McpSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpSubscription")
            .field("id", &self.id)
            .field("accepted", &self.accepted)
            .finish_non_exhaustive()
    }
}
