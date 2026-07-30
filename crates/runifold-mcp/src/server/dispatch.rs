use super::{
    INVALID_REQUEST, JsonRpcRequest, JsonRpcResponse, METHOD_NOT_FOUND, McpSession, RequestEra,
    json,
};

impl McpSession {
    /// Dispatches one JSON-RPC request through this MCP session.
    pub async fn handle_request(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        let id = request.id.clone();
        if request.jsonrpc != "2.0" {
            return JsonRpcResponse::error(id, INVALID_REQUEST, "jsonrpc must be `2.0`", None);
        }
        if request.method == "server/discover" {
            return self.discover(id, request.params);
        }
        let era = match Self::request_era(&id, request.params.as_ref()) {
            Ok(era) => era,
            Err(response) => return response,
        };
        let response = match request.method.as_str() {
            "initialize" if era == RequestEra::Legacy => self.initialize(id, request.params),
            "ping" => JsonRpcResponse::success(id, json!({})),
            "tools/list" => self.list_tools(id, request.params, era),
            "tools/call" => self.call_tool(id, request.params, era).await,
            "tasks/get" => self.get_task(id, request.params, era).await,
            "tasks/update" => self.update_task(id, request.params, era).await,
            "tasks/cancel" => self.cancel_task(id, request.params, era).await,
            "resources/list" => self.list_resources(id, request.params, era),
            "resources/templates/list" => self.list_resource_templates(id, request.params, era),
            "resources/read" => self.read_resource(id, request.params, era).await,
            "resources/subscribe" if era == RequestEra::Legacy => {
                self.subscribe_resource(id, request.params, era)
            }
            "resources/unsubscribe" if era == RequestEra::Legacy => {
                self.unsubscribe_resource(id, request.params, era)
            }
            "prompts/list" => self.list_prompts(id, request.params, era),
            "prompts/get" => self.get_prompt(id, request.params, era).await,
            "completion/complete" => self.complete(id, request.params, era).await,
            _ => JsonRpcResponse::error(id, METHOD_NOT_FOUND, "method not found", None),
        };
        if era == RequestEra::Stateless {
            self.stateless_response(response)
        } else {
            response
        }
    }
}
