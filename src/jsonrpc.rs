use async_trait::async_trait;
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    #[serde(default = "version")]
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcErrorObject {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcErrorObject>,
}

#[derive(Clone, Debug)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl JsonRpcError {
    pub fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("method not found: {method}"),
            data: None,
        }
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: -32603,
            message: message.into(),
            data: None,
        }
    }
}

#[async_trait]
pub trait JsonRpcHandler: Send + Sync + 'static {
    async fn call(&self, method: &str, params: Value) -> Result<Value, JsonRpcError>;
}

#[derive(Clone)]
pub struct JsonRpcEndpoint<H: JsonRpcHandler> {
    handler: Arc<H>,
}

impl<H: JsonRpcHandler> JsonRpcEndpoint<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }

    pub async fn handle_value(&self, input: Value) -> Value {
        if let Some(batch) = input.as_array() {
            let futures = batch.iter().cloned().map(|item| self.handle_one(item));
            return Value::Array(join_all(futures).await);
        }
        self.handle_one(input).await
    }
    pub async fn handle_bytes(&self, input: &[u8]) -> Vec<u8> {
        let output = match serde_json::from_slice::<Value>(input) {
            Ok(value) => self.handle_value(value).await,
            Err(error) => error_response(Value::Null, -32700, error.to_string(), None),
        };
        serde_json::to_vec(&output).unwrap_or_else(|_| b"null".to_vec())
    }

    async fn handle_one(&self, input: Value) -> Value {
        let request = match serde_json::from_value::<JsonRpcRequest>(input) {
            Ok(request) => request,
            Err(error) => return error_response(Value::Null, -32600, error.to_string(), None),
        };
        if request.jsonrpc != "2.0" {
            return error_response(request.id, -32600, "jsonrpc must be 2.0", None);
        }
        match self.handler.call(&request.method, request.params).await {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": request.id, "result": result }),
            Err(error) => error_response(request.id, error.code, error.message, error.data),
        }
    }
}

fn error_response(id: Value, code: i64, message: impl Into<String>, data: Option<Value>) -> Value {
    let error = JsonRpcErrorObject {
        code,
        message: message.into(),
        data,
    };
    serde_json::to_value(JsonRpcResponse {
        jsonrpc: version(),
        id,
        result: None,
        error: Some(error),
    })
    .unwrap()
}

fn version() -> String {
    "2.0".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Echo;

    #[async_trait]
    impl JsonRpcHandler for Echo {
        async fn call(&self, method: &str, params: Value) -> Result<Value, JsonRpcError> {
            if method == "echo" {
                Ok(params)
            } else {
                Err(JsonRpcError::method_not_found(method))
            }
        }
    }

    #[tokio::test]
    async fn handles_batch() {
        let endpoint = JsonRpcEndpoint::new(Echo);
        let result = endpoint
            .handle_value(json!([
                { "jsonrpc": "2.0", "id": 1, "method": "echo", "params": { "a": 1 } },
                { "jsonrpc": "2.0", "id": 2, "method": "missing", "params": {} }
            ]))
            .await;
        assert_eq!(result[0]["result"]["a"], 1);
        assert_eq!(result[1]["error"]["code"], -32601);
    }
}
