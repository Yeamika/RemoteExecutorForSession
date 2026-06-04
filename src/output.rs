use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputParts {
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub info: String,
}

pub fn merge_output(output: &Value) -> String {
    if let Some(text) = output.as_str() {
        return text.to_string();
    }
    let parts = serde_json::from_value::<OutputParts>(output.clone()).unwrap_or_default();
    [parts.message, parts.text, parts.info]
        .into_iter()
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merges_three_output_fields_with_newlines() {
        let output = json!({ "message": "m", "text": "t", "info": "i" });
        assert_eq!(merge_output(&output), "m\nt\ni");
    }

    #[test]
    fn preserves_legacy_string_output() {
        assert_eq!(merge_output(&json!("hello")), "hello");
    }
}
