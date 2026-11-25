//! Runtime-private per-incarnation restricted agent policy. No credential enters tool specs.
use super::{ToolCall, ToolResult, ToolSpec};
use crate::{runtime_db::RuntimeDb, types::AgentIdentityRecord};
use anyhow::{bail, ensure, Result};
use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};

fn is_false(value: &bool) -> bool {
    !value
}

const MAX_IO: usize = 64 * 1024;
#[derive(Clone, Serialize, PartialEq)]
pub struct ToolAllowlist {
    tool_name_prefix: String,
    tools: Vec<RegisteredTool>,
    bridge: Bridge,
    structured_output_only: bool,
    #[serde(skip_serializing_if = "is_false")]
    compact_system_prompt: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    completion_tool: Option<String>,
}
impl std::fmt::Debug for ToolAllowlist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ToolAllowlist([private])")
    }
}
#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct RegisteredTool {
    name: String,
    description: String,
    #[serde(deserialize_with = "deserialize_schema")]
    parameters: Value,
}
#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Bridge {
    url: String,
    bearer: String,
}
impl<'de> Deserialize<'de> for ToolAllowlist {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tool_name_prefix: String,
            tools: Vec<RegisteredTool>,
            bridge: Bridge,
            #[serde(default)]
            structured_output_only: bool,
            #[serde(default)]
            compact_system_prompt: bool,
            completion_tool: Option<String>,
        }
        let wire =
            Wire::deserialize(d).map_err(|_| serde::de::Error::custom("invalid tool_allowlist"))?;
        let parse = || -> Result<Self> {
            let policy = Self {
                tool_name_prefix: wire.tool_name_prefix,
                tools: wire.tools,
                bridge: wire.bridge,
                structured_output_only: wire.structured_output_only,
                compact_system_prompt: wire.compact_system_prompt,
                completion_tool: wire.completion_tool,
            };
            policy.validate()?;
            Ok(policy)
        };
        parse().map_err(|_| serde::de::Error::custom("invalid tool_allowlist"))
    }
}
impl ToolAllowlist {
    pub(crate) fn compact_system_prompt(&self) -> bool {
        self.compact_system_prompt
    }

    pub(crate) fn structured_output_only(&self) -> bool {
        self.structured_output_only
    }

    pub(crate) fn completion_tool(&self) -> Option<&str> {
        self.completion_tool.as_deref()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let valid = || -> Result<()> {
            ensure!(serde_json::to_vec(self)?.len() <= MAX_IO, "size");
            ensure!(self.tools.len() <= 16, "tools");
            ensure!(
                !self.compact_system_prompt
                    || (self.structured_output_only && self.tools.len() == 1),
                "compact requires structured output and one tool"
            );
            if let Some(tool) = self.completion_tool() {
                ensure!(self.structured_output_only, "structured output required");
                self.check(tool)?;
            }
            let prefix = self.tool_name_prefix.as_bytes();
            ensure!(
                (2..=63).contains(&prefix.len())
                    && prefix[0].is_ascii_lowercase()
                    && prefix.ends_with(b"_")
                    && prefix.iter().copied().all(is_tool_name_byte),
                "prefix"
            );
            ensure!(
                (16..=256).contains(&self.bridge.bearer.len())
                    && self
                        .bridge
                        .bearer
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b)),
                "bearer"
            );
            let url = url::Url::parse(&self.bridge.url)?;
            ensure!(
                url.scheme() == "http"
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.path() == "/call"
                    && url.port().is_some(),
                "url"
            );
            // Compare serialized host as well: reject nonliteral or normalized IPv4 spellings.
            let host = match url.host() {
                Some(url::Host::Ipv4(ip)) if ip == std::net::Ipv4Addr::LOCALHOST => "127.0.0.1",
                Some(url::Host::Ipv6(ip)) if ip == std::net::Ipv6Addr::LOCALHOST => "[::1]",
                _ => bail!("host"),
            };
            ensure!(
                self.bridge.url == format!("http://{host}:{}/call", url.port().unwrap()),
                "literal url"
            );
            let mut names = std::collections::HashSet::new();
            for tool in &self.tools {
                ensure!(
                    tool.name
                        .strip_prefix(&self.tool_name_prefix)
                        .is_some_and(|suffix| !suffix.is_empty())
                        && (8..=64).contains(&tool.name.len())
                        && tool.name.bytes().all(is_tool_name_byte)
                        && names.insert(&tool.name),
                    "name"
                );
                ensure!(
                    !super::ToolRegistry::new(std::path::PathBuf::new())
                        .tool_specs()?
                        .iter()
                        .any(|s| s.name == tool.name),
                    "collision"
                );
                ensure!(
                    !tool.description.is_empty() && tool.description.len() <= 4096,
                    "description"
                );
                ensure!(
                    tool.parameters.is_object()
                        && tool.parameters.get("type").and_then(Value::as_str) == Some("object"),
                    "parameters"
                );
                validate_schema(&tool.parameters, 0)?;
                ensure!(
                    !serde_json::to_string(tool)?.contains(&self.bridge.bearer),
                    "credential in spec"
                );
            }
            Ok(())
        };
        valid().map_err(|_| anyhow::anyhow!("invalid tool_allowlist"))
    }
    pub(crate) fn specs(&self) -> Vec<ToolSpec> {
        self.tools
            .iter()
            .map(|t| ToolSpec {
                name: t.name.clone(),
                description: t.description.clone(),
                input_schema: t.parameters.clone(),
                freeform_grammar: None,
            })
            .collect()
    }
    pub(crate) fn check(&self, name: &str) -> Result<()> {
        ensure!(
            self.tools.iter().any(|t| t.name == name),
            "tool not allowed by restricted agent policy"
        );
        Ok(())
    }
    pub(crate) async fn call(&self, runtime: &crate::runtime::RuntimeHandle, call: &ToolCall) -> Result<ToolResult> {
        self.call_inner(Some(runtime), call).await
    }
    async fn call_inner(&self, runtime: Option<&crate::runtime::RuntimeHandle>, call: &ToolCall) -> Result<ToolResult> {
        self.check(&call.name)?;
        let send = async {
            ensure!(call.input.is_object(), "args");
            let body = serde_json::to_vec(&json!({"name":call.name,"args":call.input}))?;
            ensure!(
                body.len() <= MAX_IO
                    && !String::from_utf8_lossy(&body).contains(&self.bridge.bearer),
                "args"
            );
            let client = reqwest::Client::builder()
                .no_gzip()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(std::time::Duration::from_secs(2))
                .timeout(std::time::Duration::from_secs(10))
                .build()?;
            let request = client
                .post(&self.bridge.url)
                .bearer_auth(&self.bridge.bearer)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body)
                .build()?;
            // Validate/encode locally before acquiring uncertain output ownership.
            let fence = if let Some(runtime) = runtime {
                runtime.fence_claimed_abort_for_tool(&call.name).await?
            } else { None };
            let mut response = client.execute(request).await?;
            ensure!(response.status().is_success(), "status");
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                ensure!(bytes.len().saturating_add(chunk.len()) <= MAX_IO, "size");
                bytes.extend_from_slice(&chunk);
            }
            ensure!(
                !String::from_utf8_lossy(&bytes).contains(&self.bridge.bearer),
                "credential in reply"
            );
            let reply: Value = serde_json::from_slice(&bytes)?;
            ensure!(
                !serde_json::to_string(&reply)?.contains(&self.bridge.bearer),
                "credential in decoded reply"
            );
            ensure!(
                reply.get("ok") == Some(&Value::Bool(true)) && reply.get("result").is_some(),
                "reply"
            );
            // Only this authenticated send's explicit rejection releases
            // its own fence. Acceptance, malformed/unknown results and older sends stay fenced.
            let rejected = reply["result"].get("accepted") == Some(&Value::Bool(false))
                && matches!(reply["result"].get("retryable"), Some(Value::Bool(_)))
                && matches!(reply.get("complete_turn"), None | Some(Value::Bool(false)));
            if rejected {
                if let (Some(runtime), Some(fence)) = (runtime, fence.as_ref()) {
                    runtime.release_claimed_abort_after_rejection(fence).await?;
                }
            }
            let mut result = ToolResult::success(&call.name, reply["result"].clone(), None);
            // The authenticated bridge's outer receipt is separate from tool input and output.
            if self.completion_tool() == Some(call.name.as_str())
                && reply.get("complete_turn") == Some(&Value::Bool(true))
            {
                result.loop_directive = Some(super::spec::ToolLoopDirective::CompleteStructuredOutput);
            }
            Ok::<_, anyhow::Error>(result)
        };
        // Never return provider bodies, transport errors, URL, or credentials.
        Ok(match send.await {
            Ok(result) => result,
            Err(error) if error.is::<crate::runtime::CurrentRunAborted>() => return Err(error),
            Err(_) => ToolResult::error(
                &call.name,
                super::ToolError::new("service_bridge_failed", "Service bridge call failed"),
            ),
        })
    }
}

fn is_tool_name_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
}

// serde_json::Value alone silently folds duplicate schema/property keys.
fn deserialize_schema<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Value, D::Error> {
    struct StrictValue(Value);
    impl<'de> Deserialize<'de> for StrictValue {
        fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = StrictValue;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("unique JSON schema keys")
                }
                fn visit_bool<E: serde::de::Error>(
                    self,
                    v: bool,
                ) -> std::result::Result<StrictValue, E> {
                    Ok(StrictValue(v.into()))
                }
                fn visit_i64<E: serde::de::Error>(
                    self,
                    v: i64,
                ) -> std::result::Result<StrictValue, E> {
                    Ok(StrictValue(v.into()))
                }
                fn visit_u64<E: serde::de::Error>(
                    self,
                    v: u64,
                ) -> std::result::Result<StrictValue, E> {
                    Ok(StrictValue(v.into()))
                }
                fn visit_f64<E: serde::de::Error>(
                    self,
                    v: f64,
                ) -> std::result::Result<StrictValue, E> {
                    Ok(StrictValue(
                        serde_json::Number::from_f64(v)
                            .ok_or_else(|| E::custom("invalid number"))?
                            .into(),
                    ))
                }
                fn visit_str<E: serde::de::Error>(
                    self,
                    v: &str,
                ) -> std::result::Result<StrictValue, E> {
                    Ok(StrictValue(v.into()))
                }
                fn visit_string<E: serde::de::Error>(
                    self,
                    v: String,
                ) -> std::result::Result<StrictValue, E> {
                    Ok(StrictValue(v.into()))
                }
                fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<StrictValue, E> {
                    Ok(StrictValue(Value::Null))
                }
                fn visit_none<E: serde::de::Error>(self) -> std::result::Result<StrictValue, E> {
                    Ok(StrictValue(Value::Null))
                }
                fn visit_seq<A: serde::de::SeqAccess<'de>>(
                    self,
                    mut seq: A,
                ) -> std::result::Result<StrictValue, A::Error> {
                    let mut values = Vec::new();
                    while let Some(StrictValue(value)) = seq.next_element()? {
                        values.push(value);
                    }
                    Ok(StrictValue(Value::Array(values)))
                }
                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> std::result::Result<StrictValue, A::Error> {
                    let mut values = serde_json::Map::new();
                    while let Some((key, StrictValue(value))) =
                        map.next_entry::<String, StrictValue>()?
                    {
                        if values.insert(key, value).is_some() {
                            return Err(serde::de::Error::custom("duplicate schema key"));
                        }
                    }
                    Ok(StrictValue(Value::Object(values)))
                }
            }
            d.deserialize_any(Visitor)
        }
    }
    StrictValue::deserialize(d).map(|v| v.0)
}

// The restricted intentionally accepts a small explicit JSON Schema subset.
fn validate_schema(schema: &Value, depth: usize) -> Result<()> {
    ensure!(depth <= 12, "schema depth");
    let object = schema
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("schema object"))?;
    for (key, value) in object {
        match key.as_str() {
            "type" => ensure!(
                matches!(
                    value.as_str(),
                    Some("object" | "array" | "string" | "number" | "integer" | "boolean" | "null")
                ),
                "schema type"
            ),
            "description" | "title" => ensure!(value.is_string(), "schema text"),
            "properties" => {
                let properties = value
                    .as_object()
                    .ok_or_else(|| anyhow::anyhow!("properties"))?;
                for child in properties.values() {
                    validate_schema(child, depth + 1)?;
                }
            }
            "required" => {
                let required = value
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("required"))?;
                let mut names = std::collections::HashSet::new();
                for name in required {
                    let name = name
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("required name"))?;
                    ensure!(
                        names.insert(name)
                            && object.get("properties").and_then(|p| p.get(name)).is_some(),
                        "required property"
                    );
                }
            }
            "additionalProperties" => ensure!(value.is_boolean(), "additional properties"),
            "items" => validate_schema(value, depth + 1)?,
            "enum" => ensure!(value.as_array().is_some_and(|a| !a.is_empty()), "enum"),
            "default" | "const" => {}
            "minimum" | "maximum" | "exclusiveMinimum" | "exclusiveMaximum" => {
                ensure!(value.is_number(), "numeric bound")
            }
            "minLength" | "maxLength" | "minItems" | "maxItems" => {
                ensure!(value.as_u64().is_some(), "length bound")
            }
            _ => bail!("unsupported schema keyword"),
        }
    }
    Ok(())
}

pub(crate) const SCHEMA: &str = "CREATE TABLE restricted_agent_tool_settings (agent_id TEXT NOT NULL, incarnation INTEGER NOT NULL, payload_json TEXT NOT NULL, PRIMARY KEY(agent_id, incarnation));";

pub(crate) fn protect_store(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for suffix in ["", "-wal", "-shm"] {
            let mut file = path.as_os_str().to_os_string();
            file.push(suffix);
            match std::fs::set_permissions(
                std::path::Path::new(&file),
                std::fs::Permissions::from_mode(0o600),
            ) {
                Ok(()) => {}
                Err(error)
                    if !suffix.is_empty() && error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => bail!("private restricted storage unavailable"),
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        bail!("private restricted storage unavailable")
    }
}

pub(crate) fn persist(
    tx: &Transaction<'_>,
    identity: &AgentIdentityRecord,
    policy: Option<&ToolAllowlist>,
) -> Result<()> {
    ensure!(
        identity.tool_allowlist_required == policy.is_some(),
        "restricted identity policy mismatch"
    );
    if let Some(policy) = policy {
        policy.validate()?;
        // INSERT only: no update route for an existing incarnation.
        tx.execute(
            "INSERT INTO restricted_agent_tool_settings VALUES (?1,?2,?3)",
            params![
                identity.agent_id,
                identity.incarnation,
                serde_json::to_string(policy)?
            ],
        )?;
    }
    Ok(())
}
pub(crate) fn load(
    db: &RuntimeDb,
    agent_id: &str,
    incarnation: u64,
) -> Result<Option<ToolAllowlist>> {
    db.with_read(|conn| {
        let identity: Option<String> = conn.query_row("SELECT payload_json FROM agent_identities WHERE agent_id=?1", [agent_id], |r| r.get(0)).optional()?;
        let required = identity.map(|body| serde_json::from_str::<AgentIdentityRecord>(&body).map_err(|_| anyhow::anyhow!("invalid restricted identity"))).transpose()?
            .is_some_and(|identity| identity.tool_allowlist_required);
        let body: Option<String> = conn.query_row("SELECT payload_json FROM restricted_agent_tool_settings WHERE agent_id=?1 AND incarnation=?2", params![agent_id,incarnation], |r| r.get(0)).optional()?;
        ensure!(!required || body.is_some(), "required restricted agent policy missing");
        body.map(|body| serde_json::from_str(&body).map_err(|_| anyhow::anyhow!("invalid persisted restricted agent policy"))).transpose()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn structured_output_only_requires_explicit_policy_opt_in() {
        let mut value = json!({"tool_name_prefix":"fixture_","tools":[],"bridge":{"url":"http://127.0.0.1:34567/call","bearer":"synthetic-structured-output"}});
        let ordinary: ToolAllowlist = serde_json::from_value(value.clone()).unwrap();
        assert!(!ordinary.structured_output_only());
        value["structured_output_only"] = json!(true);
        let opted_in: ToolAllowlist = serde_json::from_value(value.clone()).unwrap();
        assert!(opted_in.structured_output_only());
        assert_eq!(serde_json::to_value(opted_in).unwrap(), value);
        value["structured_output_only"] = json!("true");
        assert!(serde_json::from_value::<ToolAllowlist>(value).is_err());
    }

    #[test]
    fn compact_prompt_requires_structured_single_tool_policy() {
        let mut value = json!({"tool_name_prefix":"fixture_","tools":[{"name":"fixture_submit","description":"Synthetic result","parameters":{"type":"object"}}],"bridge":{"url":"http://127.0.0.1:34567/call","bearer":"synthetic-compact-policy"}});
        assert!(!serde_json::from_value::<ToolAllowlist>(value.clone())
            .unwrap()
            .compact_system_prompt());
        value["compact_system_prompt"] = json!(true);
        assert!(serde_json::from_value::<ToolAllowlist>(value.clone()).is_err());
        value["structured_output_only"] = json!(true);
        let policy: ToolAllowlist = serde_json::from_value(value.clone()).unwrap();
        assert!(policy.compact_system_prompt());
        assert!(
            serde_json::from_value::<ToolAllowlist>(serde_json::to_value(&policy).unwrap())
                .unwrap()
                .compact_system_prompt()
        );
        assert!(policy.check("ExecCommand").is_err());
        for invalid in [json!(null), json!("true"), json!(1)] {
            let mut bad = value.clone();
            bad["compact_system_prompt"] = invalid;
            assert!(serde_json::from_value::<ToolAllowlist>(bad).is_err());
        }
        for tools in [
            json!([]),
            json!([value["tools"][0].clone(), {"name":"fixture_second","description":"second","parameters":{"type":"object"}}]),
        ] {
            let mut bad = value.clone();
            bad["tools"] = tools;
            assert!(serde_json::from_value::<ToolAllowlist>(bad).is_err());
        }
    }

    #[test]
    fn restricted_wire_redacts_and_rejects_malformed() {
        let valid = json!({"tool_name_prefix":"fixture_","tools":[{"name":"fixture_test","description":"test","parameters":{"type":"object"}}],"bridge":{"url":"http://127.0.0.1:34567/call","bearer":"synthetic-ephemeral-0007"}});
        let policy: ToolAllowlist = serde_json::from_value(valid.clone()).unwrap();
        assert!(!format!("{policy:?}").contains("synthetic"));
        assert!(!serde_json::to_string(&policy.specs())
            .unwrap()
            .contains("synthetic"));
        assert!(policy.check("ExecCommandBatch").is_err());
        for value in [
            Value::Null,
            json!({}),
            json!({"tools":false,"bridge":valid["bridge"]}),
        ] {
            assert!(serde_json::from_value::<ToolAllowlist>(value).is_err());
        }
    }
    #[cfg(unix)]
    #[test]
    fn restricted_store_permissions_cover_sidecars_and_missing_main() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("restricted-例.sqlite");
        let paths = [
            file.clone(),
            dir.path().join("restricted-例.sqlite-wal"),
            dir.path().join("restricted-例.sqlite-shm"),
        ];
        for path in &paths {
            std::fs::write(path, b"synthetic")?;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644))?;
        }
        protect_store(&file)?;
        for path in &paths {
            assert_eq!(std::fs::metadata(path)?.permissions().mode() & 0o777, 0o600);
        }
        assert!(protect_store(&dir.path().join("missing.sqlite")).is_err());
        Ok(())
    }
    #[test]
    fn restricted_schema_rejects_nested_duplicate_keys() {
        for parameters in [
            r#"{"type":"string","type":"object"}"#,
            r#"{"type":"object","properties":{"id":{"type":"string"},"id":{"type":"number"}}}"#,
        ] {
            let raw = format!(
                r#"{{"tool_name_prefix":"fixture_","tools":[{{"name":"fixture_test","description":"test","parameters":{parameters}}}],"bridge":{{"url":"http://127.0.0.1:34567/call","bearer":"synthetic-ephemeral-0007"}}}}"#
            );
            let error = serde_json::from_str::<ToolAllowlist>(&raw).unwrap_err();
            assert!(!error.to_string().contains("synthetic-ephemeral"));
        }
    }
    #[tokio::test]
    async fn restricted_callback_bounds_and_redacts_failures() -> Result<()> {
        use axum::{routing::post, Router};
        for mode in ["denied", "oversize", "redirect", "echo", "escaped"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let port = listener.local_addr()?.port();
            let router = Router::new().route(
                "/call",
                post(move || async move {
                    use axum::response::IntoResponse;
                    match mode {
                        "denied" => {
                            axum::Json(json!({"ok":false,"error":"synthetic-ephemeral-0007"}))
                                .into_response()
                        }
                        "oversize" => "x".repeat(MAX_IO + 1).into_response(),
                        "escaped" => (
                            [("content-type", "application/json")],
                            r#"{"ok":true,"result":"\u0073ynthetic-ephemeral-0007"}"#,
                        )
                            .into_response(),
                        "redirect" => (
                            axum::http::StatusCode::FOUND,
                            [("location", "http://127.0.0.1:9/call")],
                        )
                            .into_response(),
                        _ => axum::Json(json!({"ok":true,"result":"synthetic-ephemeral-0007"}))
                            .into_response(),
                    }
                }),
            );
            let server = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            let policy: ToolAllowlist = serde_json::from_value(
                json!({"tool_name_prefix":"fixture_","tools":[{"name":"fixture_test","description":"test","parameters":{"type":"object"}}],"bridge":{"url":format!("http://127.0.0.1:{port}/call"),"bearer":"synthetic-ephemeral-0007"}}),
            )?;
            let result = policy
                .call_inner(None, &ToolCall {
                    id: "test".into(),
                    name: "fixture_test".into(),
                    input: json!({}),
                })
                .await?;
            assert!(result.is_error());
            let rendered = serde_json::to_string(&result)?;
            assert!(!rendered.contains("synthetic-ephemeral"));
            assert!(!rendered.contains(&port.to_string()));
            server.abort();
        }
        Ok(())
    }
}
