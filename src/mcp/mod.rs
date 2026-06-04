use crate::{JsonRpcError, JsonRpcHandler};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod handler;
pub mod mcptooldefs;
pub use handler::{create_default_session_mcp, create_session_mcp, create_session_mcp_with_manager, DummySessionHost, SessionMcpHandler};

pub const EXECUTOR_SESSION_PARAM: &str = "ExecutorSessionID";

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct McpCallContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_session_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct McpToolDef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct McpContentText {
    #[serde(rename = "type")]
    pub kind: String,
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct McpCallResult {
    pub content: Vec<McpContentText>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "structuredContent")]
    pub structured_content: Option<Value>,
}

#[async_trait]
pub trait McpToolHandler: Send + Sync + 'static {
    async fn list_tools(&self) -> Result<Vec<McpToolDef>, JsonRpcError>;
    async fn call_tool(&self, name: &str, arguments: Value, context: McpCallContext) -> Result<McpCallResult, JsonRpcError>;
}

pub struct EmbeddedMcp<H: McpToolHandler> {
    handler: H,
}

impl<H: McpToolHandler> EmbeddedMcp<H> {
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl<H: McpToolHandler> JsonRpcHandler for EmbeddedMcp<H> {
    async fn call(&self, method: &str, params: Value) -> Result<Value, JsonRpcError> {
        match method {
            "tools/list" => Ok(json!({ "tools": self.handler.list_tools().await? })),
            "tools/call" => {
                let name = params.get("name").and_then(Value::as_str).ok_or_else(|| JsonRpcError::internal("tools/call params.name is required"))?;
                let mut arguments = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
                let context = extract_context(&mut arguments);
                Ok(serde_json::to_value(self.handler.call_tool(name, arguments, context).await?).unwrap())
            }
            _ => Err(JsonRpcError::method_not_found(method)),
        }
    }
}

fn extract_context(arguments: &mut Value) -> McpCallContext {
    let Some(object) = arguments.as_object_mut() else {
        return McpCallContext::default();
    };
    let executor_session_id = object
        .remove(EXECUTOR_SESSION_PARAM)
        .and_then(|value| value.as_str().map(str::to_string));
    McpCallContext { executor_session_id }
}

pub fn text_result(text: impl Into<String>, structured_content: Option<Value>) -> McpCallResult {
    McpCallResult {
        content: vec![McpContentText { kind: "text".to_string(), text: text.into() }],
        structured_content,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JsonRpcEndpoint;
    use serde_json::json;

    struct Demo;

    #[async_trait]
    impl McpToolHandler for Demo {
        async fn list_tools(&self) -> Result<Vec<McpToolDef>, JsonRpcError> {
            Ok(vec![McpToolDef {
                name: "demo".into(),
                description: None,
                input_schema: json!({ "type": "object", "properties": { EXECUTOR_SESSION_PARAM: { "type": "string" } } }),
            }])
        }

        async fn call_tool(&self, _name: &str, arguments: Value, context: McpCallContext) -> Result<McpCallResult, JsonRpcError> {
            assert!(arguments.get(EXECUTOR_SESSION_PARAM).is_none());
            Ok(text_result(context.executor_session_id.unwrap_or_default(), None))
        }
    }

    #[tokio::test]
    async fn extracts_executor_session_id_from_call_arguments() {
        let endpoint = JsonRpcEndpoint::new(EmbeddedMcp::new(Demo));
        let listed = endpoint.handle_value(json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" })).await;
        assert_eq!(listed["result"]["tools"][0]["inputSchema"]["properties"][EXECUTOR_SESSION_PARAM]["type"], "string");
        let called = endpoint.handle_value(json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": { "name": "demo", "arguments": { EXECUTOR_SESSION_PARAM: "ses_1" } } })).await;
        assert_eq!(called["result"]["content"][0]["text"], "ses_1");
    }
}
