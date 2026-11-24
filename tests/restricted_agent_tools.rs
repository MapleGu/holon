//! Synthetic local HTTP bridge and provider only; no shell effects or external services.
mod support;
use anyhow::{ensure, Result};
use async_trait::async_trait;
use holon::{
    host::RuntimeHost,
    provider::{
        AgentProvider, ModelBlock, ModelToolCallKind, ProviderTurnRequest, ProviderTurnResponse,
        StubProvider,
    },
    tool::{ToolCall, ToolRegistry},
    types::AuthorityClass,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use support::{spawn_server_for_host, TestConfigBuilder};
use tokio::time::{sleep, Duration, Instant};
const TOKEN: &str = "synthetic-ephemeral-bearer-0007";
fn policy(port: u16) -> Value {
    json!({"tool_name_prefix":"fixture_","tools":[{"name":"fixture_record_lookup","description":"Synthetic record lookup","parameters":{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}}],"bridge":{"url":format!("http://127.0.0.1:{port}/call"),"bearer":TOKEN}})
}
struct RestrictedProvider {
    calls: AtomicUsize,
    catalogs: Mutex<Vec<Vec<String>>>,
    forbidden: Option<String>,
}
#[async_trait]
impl AgentProvider for RestrictedProvider {
    async fn complete_turn(&self, request: ProviderTurnRequest) -> Result<ProviderTurnResponse> {
        ensure!(request.native_web_search.is_none());
        ensure!(!format!("{request:?}").contains(TOKEN));
        self.catalogs
            .lock()
            .unwrap()
            .push(request.tools.iter().map(|t| t.name.clone()).collect());
        let i = self.calls.fetch_add(1, Ordering::SeqCst);
        let mut response = StubProvider::new("synthetic reply")
            .complete_turn(request)
            .await?;
        if i == 0 {
            response.blocks = vec![ModelBlock::ToolUse {
                id: "restricted-call".into(),
                name: self
                    .forbidden
                    .clone()
                    .unwrap_or("fixture_record_lookup".into()),
                input: json!({"id":"synthetic"}),
                kind: ModelToolCallKind::Function,
                provider_data: None,
            }];
        }
        if i == 0 && self.forbidden.is_some() {
            response.blocks.insert(
                0,
                ModelBlock::ToolUse {
                    id: "allowed-before-denied".into(),
                    name: "fixture_record_lookup".into(),
                    input: json!({"id":"synthetic"}),
                    kind: ModelToolCallKind::Function,
                    provider_data: None,
                },
            );
        }
        Ok(response)
    }
}
async fn ready(client: &reqwest::Client, base: &str, agent: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let v: Value = client
            .get(format!("{base}/api/control/agents/{agent}/detail"))
            .bearer_auth("synthetic-control")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let v = v.get("detail").unwrap_or(&v);
        if v["bootstrap"]["status"] == "ready" {
            return Ok(());
        }
        ensure!(Instant::now() < deadline, "bootstrap not ready");
        sleep(Duration::from_millis(20)).await;
    }
}
#[tokio::test]
async fn restricted_agent_wire_auth_validation_and_atomic_failure() -> Result<()> {
    let fixture = TestConfigBuilder::new()
        .with_control_token("synthetic-control")
        .build();
    let host = RuntimeHost::new_with_provider(
        fixture.config().clone(),
        Arc::new(StubProvider::new("done")),
    )?;
    let (base, server) = spawn_server_for_host(host.clone()).await?;
    let c = reqwest::Client::new();
    let endpoint = format!("{base}/api/control/runtime/restricted-agent-capabilities");
    assert_eq!(c.get(&endpoint).send().await?.status(), 401);
    let caps: Value = c
        .get(&endpoint)
        .bearer_auth("synthetic-control")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        caps,
        json!({"toolAllowlist":true,"promptCancellation":true,"structuredOutputOnly":true,"compactSystemPrompt":true,"structuredToolCompletion":true})
    );
    let create = format!("{base}/api/control/agents/custom-prefix/restricted/create");
    let mut custom = policy(34567);
    custom["tool_name_prefix"] = json!("widget_");
    custom["tools"][0]["name"] = json!("widget_read");
    let request = json!({"default_initial_message": false, "tool_allowlist": custom});
    assert_eq!(c.post(&create).json(&request).send().await?.status(), 401);
    let created: Value = c
        .post(&create)
        .bearer_auth("synthetic-control")
        .json(&request)
        .send().await?.error_for_status()?.json().await?;
    assert_eq!(created["tool_allowlist_applied"], true);
    for (path, body) in [
        ("generic-route/create", request),
        ("missing-policy/restricted/create", json!({})),
    ] {
        let response = c.post(format!("{base}/api/control/agents/{path}"))
            .bearer_auth("synthetic-control").json(&body).send().await?;
        assert_eq!(response.status(), 422);
        assert!(host.get_public_agent(path.split('/').next().unwrap()).await.is_err());
    }
    let mut invalid = vec![Value::Null, json!({}), json!({"tool_name_prefix":"fixture_","tools":[],"bridge":null})];
    for name in [
        "ExecCommand",
        "Work",
        "Memory",
        "InvokeAgent",
        "fixture_",
        "fixture_Upper",
    ] {
        let mut p = policy(34567);
        p["tools"][0]["name"] = json!(name);
        invalid.push(p);
    }
    for prefix in [Value::Null, json!(""), json!("_"), json!("fixture"), json!("Fixture_"), json!("9fixture_"), json!("fixture-"), json!("other_")] {
        let mut p = policy(34567);
        p["tool_name_prefix"] = prefix;
        invalid.push(p);
    }
    let mut missing_prefix = policy(34567);
    missing_prefix.as_object_mut().unwrap().remove("tool_name_prefix");
    invalid.push(missing_prefix);
    for url in [
        "http://localhost:34567/call",
        "http://127.1:34567/call",
        "http://2130706433:34567/call",
        "http://127.0.0.2:34567/call",
        "http://example.com:34567/call",
        "http://127.0.0.1:34567/call?x=y",
        "https://127.0.0.1:34567/call",
        "http://user@127.0.0.1:34567/call",
    ] {
        let mut p = policy(34567);
        p["bridge"]["url"] = json!(url);
        invalid.push(p);
    }
    for path in ["extra", "bearer"] {
        let mut p = policy(34567);
        p[path] = json!(TOKEN);
        invalid.push(p);
    }
    for (structured, tool) in [(false, "fixture_record_lookup"), (true, "fixture_other")] {
        let mut p = policy(34567);
        p["structured_output_only"] = json!(structured);
        p["completion_tool"] = json!(tool);
        invalid.push(p);
    }
    let mut p = policy(34567);
    p["tools"][0]["extra"] = json!(TOKEN);
    invalid.push(p);
    let mut p = policy(34567);
    p["bridge"]["extra"] = json!(TOKEN);
    invalid.push(p);
    for schema in [
        json!({"type":"object","unknown":true}),
        json!({"type":"object","properties":[]}),
        json!({"type":"object","required":["missing"]}),
        json!({"type":"object","properties":{"id":{"type":"invalid"}}}),
    ] {
        let mut p = policy(34567);
        p["tools"][0]["parameters"] = schema;
        invalid.push(p);
    }
    let mut p = policy(34567);
    p["tools"][0]["parameters"] = json!([]);
    invalid.push(p);
    let mut p = policy(34567);
    p["tools"] = json!([p["tools"][0].clone(), p["tools"][0].clone()]);
    invalid.push(p);
    for (i, p) in invalid.into_iter().enumerate() {
        let r = c
            .post(format!("{base}/api/control/agents/bad-{i}/restricted/create"))
            .bearer_auth("synthetic-control")
            .json(&json!({"tool_allowlist":p}))
            .send()
            .await?;
        assert!(r.status().is_client_error());
        assert!(!r.text().await?.contains(TOKEN));
        assert!(host.get_public_agent(&format!("bad-{i}")).await.is_err());
    }
    let raw = format!(
        r#"{{"tool_allowlist":{{"tool_name_prefix":"fixture_","tools":[],"tools":[],"bridge":{{"url":"http://127.0.0.1:34567/call","bearer":"{TOKEN}"}}}}}}"#
    );
    assert!(c
        .post(format!("{base}/api/control/agents/duplicate/restricted/create"))
        .bearer_auth("synthetic-control")
        .header("content-type", "application/json")
        .body(raw)
        .send()
        .await?
        .status()
        .is_client_error());
    // SQL failure after identity INSERT must roll the whole creation back.
    let db = host.runtime_db();
    db.transaction(|tx| {tx.execute_batch("CREATE TRIGGER reject_restricted BEFORE INSERT ON restricted_agent_tool_settings BEGIN SELECT RAISE(ABORT,'synthetic failure'); END;")?;Ok(())})?;
    let r = c
        .post(format!("{base}/api/control/agents/atomic-fail/restricted/create"))
        .bearer_auth("synthetic-control")
        .json(&json!({"tool_allowlist":policy(34567)}))
        .send()
        .await?;
    assert!(!r.status().is_success());
    assert!(host.get_public_agent("atomic-fail").await.is_err());
    server.abort();
    host.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn restricted_agent_visible_invocable_rejected_native_and_restart() -> Result<()> {
    let hits = Arc::new(AtomicUsize::new(0));
    let captured = hits.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let app = axum::Router::new().route(
        "/call",
        axum::routing::post(
            move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<Value>| {
                let hits = captured.clone();
                async move {
                    assert_eq!(headers["authorization"], format!("Bearer {TOKEN}"));
                    assert_eq!(
                        body,
                        json!({"name":"fixture_record_lookup","args":{"id":"synthetic"}})
                    );
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::Json(json!({"ok":true,"result":{"record":"synthetic-only"}}))
                }
            },
        ),
    );
    let bridge = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let fixture = TestConfigBuilder::new()
        .with_control_token("synthetic-control")
        .build();
    let mut config = fixture.config().clone();
    for phase in 0..2 {
        let provider = Arc::new(RestrictedProvider {
            calls: AtomicUsize::new(0),
            catalogs: Mutex::new(vec![]),
            forbidden: None,
        });
        let host = RuntimeHost::new_with_provider(config.clone(), provider.clone())?;
        let (base, server) = spawn_server_for_host(host.clone()).await?;
        let c = reqwest::Client::new();
        if phase == 0 {
            // Keep deletion subject separate from the runtime's default identity.
            let owner: Value = c
                .post(format!("{base}/api/control/agents/synthetic-owner/create"))
                .bearer_auth("synthetic-control")
                .json(&json!({"default_initial_message":false}))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            assert!(owner.get("tool_allowlist_applied").is_none());
            ready(&c, &base, "synthetic-owner").await?;
            let r: Value = c
                .post(format!("{base}/api/control/agents/restricted/restricted/create"))
                .bearer_auth("synthetic-control")
                .json(&json!({"default_initial_message":true,"tool_allowlist":policy(port)}))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            assert_eq!(r["tool_allowlist_applied"], true);
            assert_eq!(r["identity"]["incarnation"], 1);
            assert!(!r.to_string().contains(TOKEN));
        }
        ready(&c, &base, "restricted").await?;
        let runtime = host.get_public_agent("restricted").await?;
        if phase == 1 {
            c.post(format!("{base}/api/control/agents/restricted/prompt")).bearer_auth("synthetic-control").json(&json!({"text":"Synthetic restart check","client_request_id":"restart-check","expected_incarnation":1})).send().await?.error_for_status()?;
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        while provider.calls.load(Ordering::SeqCst) < 2 {
            ensure!(Instant::now() < deadline, "provider did not finish");
            sleep(Duration::from_millis(20)).await;
        }
        let completed_deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let terminal_count = runtime
                .storage()
                .read_recent_turns(10)?
                .iter()
                .filter(|turn| turn.terminal.is_some())
                .count();
            if terminal_count >= phase + 1 && runtime.agent_state().await?.current_run_id.is_none()
            {
                break;
            }
            ensure!(
                Instant::now() < completed_deadline,
                "restricted terminal state did not settle"
            );
            sleep(Duration::from_millis(20)).await;
        }
        assert!(provider
            .catalogs
            .lock()
            .unwrap()
            .iter()
            .all(|names| names == &vec!["fixture_record_lookup".to_string()]));
        assert_eq!(hits.load(Ordering::SeqCst), phase + 1);
        let registry = ToolRegistry::new(fixture.data_dir().to_path_buf());
        let native = registry.tool_specs()?;
        for tool in native {
            assert!(registry
                .execute(
                    &runtime,
                    "restricted",
                    &AuthorityClass::OperatorInstruction,
                    &ToolCall {
                        id: "all-native-denied".into(),
                        name: tool.name,
                        input: json!({})
                    }
                )
                .await
                .is_err());
        }
        for name in [
            "ExecCommand",
            "WriteStdin",
            "ApplyPatch",
            "ReadFile",
            "Work",
            "Memory",
            "InvokeAgent",
            "CreateAgent",
            "Sleep",
            "WaitFor",
            "CompleteWorkItem",
            "WebSearch",
            "fixture_unregistered",
        ] {
            let call = ToolCall {
                id: "denied".into(),
                name: name.into(),
                input: json!({}),
            };
            let error = registry
                .execute(
                    &runtime,
                    "restricted",
                    &AuthorityClass::OperatorInstruction,
                    &call,
                )
                .await
                .unwrap_err();
            assert!(!error.to_string().contains(TOKEN));
        }
        let r = c
            .post(format!("{base}/api/control/agents/restricted/restricted/create"))
            .bearer_auth("synthetic-control")
            .json(&json!({"tool_allowlist":policy(port+1)}))
            .send()
            .await?;
        assert!(!r.status().is_success());
        if phase == 1 {
            let db = host.runtime_db();
            for broken in [Some("malformed"), None] {
                db.transaction(|tx| { match broken {
                    Some(body) => { tx.execute("UPDATE restricted_agent_tool_settings SET payload_json=?1 WHERE agent_id='restricted'", [body])?; },
                    None => { tx.execute("DELETE FROM restricted_agent_tool_settings WHERE agent_id='restricted'", [])?; },
                } Ok(()) })?;
                assert!(registry
                    .execute(
                        &runtime,
                        "restricted",
                        &AuthorityClass::OperatorInstruction,
                        &ToolCall {
                            id: "corrupt".into(),
                            name: "ExecCommandBatch".into(),
                            input: json!({})
                        }
                    )
                    .await
                    .is_err());
            }
            db.transaction(|tx| {
                tx.execute(
                    "INSERT INTO restricted_agent_tool_settings VALUES ('restricted',1,?1)",
                    [policy(port).to_string()],
                )?;
                Ok(())
            })?;
            host.spawn_daemon_deletion_coordinator();
            let delete_response = c
                .delete(format!("{base}/api/control/agents/restricted"))
                .bearer_auth("synthetic-control")
                .json(&json!({"archive":true}))
                .send()
                .await?;
            let delete_status = delete_response.status();
            let deleted: Value = delete_response.json().await?;
            ensure!(
                delete_status.is_success(),
                "synthetic restricted deletion: {delete_status} {deleted}"
            );
            let archive = deleted["job"]["archive_path"].as_str().unwrap().to_string();
            let until = Instant::now() + Duration::from_secs(20);
            loop {
                let status: Value = c
                    .get(format!("{base}/api/control/agents/restricted/delete-status"))
                    .bearer_auth("synthetic-control")
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if status["job"]["status"] == "completed" {
                    break;
                }
                ensure!(
                    Instant::now() < until,
                    "restricted deletion incomplete: {status}"
                );
                sleep(Duration::from_millis(20)).await;
            }
            let bytes = std::fs::read(archive)?;
            let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(bytes.as_slice()));
            for entry in tar.entries()? {
                let mut entry = entry?;
                let mut contents = Vec::new();
                std::io::Read::read_to_end(&mut entry, &mut contents)?;
                assert!(!String::from_utf8_lossy(&contents).contains(TOKEN));
            }
            let remaining: i64 = db.connection()?.query_row(
                "SELECT COUNT(*) FROM restricted_agent_tool_settings WHERE agent_id='restricted'",
                [],
                |r| r.get(0),
            )?;
            assert_eq!(remaining, 0);
            let recreated: Value = c
                .post(format!("{base}/api/control/agents/restricted/restricted/create"))
                .bearer_auth("synthetic-control")
                .json(&json!({"default_initial_message":false,"tool_allowlist":policy(port)}))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            assert_eq!(recreated["tool_allowlist_applied"], true);
            assert_eq!(recreated["identity"]["incarnation"], 2);
            ready(&c, &base, "restricted").await?;
        }
        config = host.config().as_ref().clone();
        server.abort();
        host.shutdown().await?;
        drop(runtime);
        drop(host);
    }
    bridge.abort();
    Ok(())
}

#[tokio::test]
async fn restricted_agent_rejects_whole_provider_batch_before_special_effects() -> Result<()> {
    for forbidden in [
        "ExecCommandBatch",
        "Sleep",
        "WaitFor",
        "CompleteWorkItem",
        "InvokeAgent",
    ] {
        let fixture = TestConfigBuilder::new()
            .with_control_token("synthetic-control")
            .build();
        let provider = Arc::new(RestrictedProvider {
            calls: AtomicUsize::new(0),
            catalogs: Mutex::new(vec![]),
            forbidden: Some(forbidden.into()),
        });
        let host = RuntimeHost::new_with_provider(fixture.config().clone(), provider.clone())?;
        let (base, server) = spawn_server_for_host(host.clone()).await?;
        let c = reqwest::Client::new();
        // No bridge listener: a callback must never be attempted for this batch.
        c.post(format!("{base}/api/control/agents/batch-restricted/restricted/create"))
            .bearer_auth("synthetic-control")
            .json(&json!({"tool_allowlist":policy(9)}))
            .send()
            .await?
            .error_for_status()?;
        ready(&c, &base, "batch-restricted").await?;
        let runtime = host.get_public_agent("batch-restricted").await?;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if runtime
                .storage()
                .read_recent_turns(5)?
                .iter()
                .any(|turn| turn.terminal.is_some())
            {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "forged batch did not terminate: {forbidden}"
            );
            sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert!(
            runtime
                .storage()
                .read_recent_tool_executions(10)?
                .is_empty(),
            "batch must fail before even the allowed sibling dispatch"
        );
        server.abort();
        host.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn restricted_agent_callback_errors_are_bounded_and_redacted() -> Result<()> {
    use axum::response::IntoResponse;
    let mode = Arc::new(AtomicUsize::new(0));
    let selected = mode.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let router = axum::Router::new().route(
        "/call",
        axum::routing::post(move || {
            let selected = selected.clone();
            async move {
                match selected.load(Ordering::SeqCst) {
                    0 => axum::Json(json!({"ok":false,"error":TOKEN})).into_response(),
                    1 => "x".repeat(65537).into_response(),
                    2 => (
                        axum::http::StatusCode::FOUND,
                        [("location", "http://127.0.0.1:9/call")],
                    )
                        .into_response(),
                    _ => (
                        [("content-type", "application/json")],
                        r#"{"ok":true,"result":"\u0073ynthetic-ephemeral-bearer-0007"}"#,
                    )
                        .into_response(),
                }
            }
        }),
    );
    let bridge = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let fixture = TestConfigBuilder::new()
        .with_control_token("synthetic-control")
        .build();
    let host = RuntimeHost::new_with_provider(
        fixture.config().clone(),
        Arc::new(StubProvider::new("done")),
    )?;
    let (base, server) = spawn_server_for_host(host.clone()).await?;
    let c = reqwest::Client::new();
    c.post(format!("{base}/api/control/agents/error-restricted/restricted/create"))
        .bearer_auth("synthetic-control")
        .json(&json!({"default_initial_message":false,"tool_allowlist":policy(port)}))
        .send()
        .await?
        .error_for_status()?;
    ready(&c, &base, "error-restricted").await?;
    let runtime = host.get_public_agent("error-restricted").await?;
    let registry = ToolRegistry::new(fixture.data_dir().to_path_buf());
    for selected in 0..4 {
        mode.store(selected, Ordering::SeqCst);
        let (result, record) = registry
            .execute(
                &runtime,
                "error-restricted",
                &AuthorityClass::OperatorInstruction,
                &ToolCall {
                    id: "error-test".into(),
                    name: "fixture_record_lookup".into(),
                    input: json!({"id":"synthetic"}),
                },
            )
            .await?;
        assert!(result.is_error(), "mode {selected}");
        let wire = serde_json::to_string(&(result, record))?;
        assert!(!wire.contains(TOKEN));
        assert!(!wire.contains(&port.to_string()));
        assert!(wire.len() < 8192);
    }
    server.abort();
    bridge.abort();
    host.shutdown().await?;
    Ok(())
}

struct CompletionProvider {
    calls: AtomicUsize,
    first_request: tokio::sync::Notify,
    release_first: tokio::sync::Notify,
    second_request: tokio::sync::Notify,
}

#[async_trait]
impl AgentProvider for CompletionProvider {
    async fn complete_turn(&self, request: ProviderTurnRequest) -> Result<ProviderTurnResponse> {
        assert_eq!(request.tools.len(), 1);
        assert_eq!(request.tools[0].name, "fixture_record_lookup");
        let additional_source = format!("{:?}", request.conversation).contains("fixture-second-origin");
        if additional_source {
            return StubProvider::new("processed second source independently").complete_turn(request).await;
        }
        if self.calls.fetch_add(1, Ordering::SeqCst) != 0 {
            self.second_request.notify_one();
            return std::future::pending().await;
        }
        self.first_request.notify_one();
        self.release_first.notified().await;
        let mut response = StubProvider::new("prose beside the submission")
            .complete_turn(request)
            .await?;
        response.blocks.push(ModelBlock::ToolUse {
            id: "fixture-submission".into(),
            name: "fixture_record_lookup".into(),
            input: json!({"id":"synthetic"}),
            kind: ModelToolCallKind::Function,
            provider_data: None,
        });
        Ok(response)
    }
}

#[tokio::test]
async fn accepted_restricted_output_settles_and_can_be_archived() -> Result<()> {
    for (bridge_receipt, compact) in [(true, false), (false, false), (true, true), (false, true)] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let app = axum::Router::new().route(
            "/call",
            axum::routing::post(move |headers: axum::http::HeaderMap| async move {
                assert_eq!(headers["authorization"], format!("Bearer {TOKEN}"));
                // The false branch simulates a durable acceptance whose control bit
                // did not reach the execution loop, so a second request is in flight.
                axum::Json(json!({"ok":true,"complete_turn":bridge_receipt,
                    "result":{"accepted":true,"complete_turn":true}}))
            }),
        );
        let bridge = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let fixture = TestConfigBuilder::new().with_control_token("synthetic-control").build();
        let provider = Arc::new(CompletionProvider {
            calls: AtomicUsize::new(0),
            first_request: Default::default(),
            release_first: Default::default(),
            second_request: Default::default(),
        });
        let host = RuntimeHost::new_with_provider(fixture.config().clone(), provider.clone())?;
        let (base, server) = spawn_server_for_host(host.clone()).await?;
        host.spawn_daemon_deletion_coordinator();
        let client = reqwest::Client::new();
        client.post(format!("{base}/api/control/agents/fixture-anchor/create"))
            .bearer_auth("synthetic-control")
            .json(&json!({"default_initial_message":false}))
            .send().await?.error_for_status()?;
        ready(&client, &base, "fixture-anchor").await?;
        let control = format!("{base}/api/control/agents/completion");
        let mut tools = policy(port);
        tools["structured_output_only"] = json!(true);
        tools["completion_tool"] = json!("fixture_record_lookup");
        if compact {
            tools["compact_system_prompt"] = json!(true);
        }
        client.post(format!("{control}/restricted/create"))
            .bearer_auth("synthetic-control")
            .json(&json!({"default_initial_message":false,"tool_allowlist":tools}))
            .send().await?.error_for_status()?;
        ready(&client, &base, "completion").await?;
        let runtime = host.get_public_agent("completion").await?;
        let first: Value = client.post(format!("{control}/prompt")).bearer_auth("synthetic-control")
            .json(&json!({"text":"Submit synthetic output","client_request_id":"fixture-request","expected_incarnation":1}))
            .send().await?.error_for_status()?.json().await?;
        tokio::time::timeout(Duration::from_secs(10), provider.first_request.notified()).await?;
        let second: Value = client.post(format!("{control}/prompt")).bearer_auth("synthetic-control")
            .json(&json!({"text":"fixture-second-origin","client_request_id":"fixture-second-request","expected_incarnation":1}))
            .send().await?.error_for_status()?.json().await?;
        let first_id = first["message_id"].as_str().unwrap();
        let second_id = second["message_id"].as_str().unwrap();
        provider.release_first.notify_one();
        if !bridge_receipt {
            tokio::time::timeout(Duration::from_secs(10), provider.second_request.notified()).await?;
            let cancel = |key: &str, incarnation: u64, complete: bool| {
                client.post(format!("{control}/prompt-cancel")).bearer_auth("synthetic-control")
                    .header("x-holon-expected-incarnation", incarnation.to_string())
                    .json(&json!({"client_request_id":key,"complete_structured_output":complete}))
            };
            assert!(cancel("fixture-other", 1, true).send().await?.status().is_client_error());
            assert_eq!(cancel("fixture-request", 2, true).send().await?.status(), 409);
            let ordinary: Value = cancel("fixture-request", 1, false).send().await?.error_for_status()?.json().await?;
            assert_eq!(ordinary["status"], "already_claimed");
            assert!(runtime.agent_state().await?.current_run_id.is_some());
            let accepted: Value = cancel("fixture-request", 1, true).send().await?.error_for_status()?.json().await?;
            assert_eq!(accepted["status"], "already_claimed");
        }
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            let state = runtime.agent_state().await?;
            let turns = runtime.storage().read_recent_turns(5)?;
            let first_turn = turns.iter().find(|turn| turn.input_message_ids.iter().any(|id| id == first_id));
            let second_turn = turns.iter().find(|turn| turn.input_message_ids.iter().any(|id| id == second_id));
            if let (None, Some(first_turn), Some(second_turn)) = (state.current_run_id, first_turn, second_turn) {
                assert_eq!(first_turn.input_message_ids, [first_id]);
                assert_eq!(second_turn.input_message_ids, [second_id]);
                assert_ne!(first_turn.turn_id, second_turn.turn_id);
                assert!(state.current_execution_binding.is_none());
                assert!(state.current_turn_id.is_none());
                let terminal = first_turn.terminal.as_ref().unwrap();
                assert_eq!(terminal.kind, holon::types::TurnTerminalKind::Completed);
                assert_eq!(terminal.no_brief_reason, Some(holon::types::TurnNoBriefReason::StructuredOutputOnly));
                let settled: bool = host.runtime_db().transaction(|tx| Ok(tx.query_row(
                    "SELECT result_settled FROM conversation_turn_revisions WHERE agent_id='completion' AND turn_id=?1",
                    [&first_turn.turn_id], |row| row.get(0))?))?;
                assert!(settled);
                break;
            }
            ensure!(Instant::now() < deadline, "submitted turn did not settle");
            sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), if bridge_receipt { 1 } else { 2 });
        client.post(format!("{control}/stop-if-incarnation")).bearer_auth("synthetic-control")
            .json(&json!({"expected_incarnation":1})).send().await?.error_for_status()?;
        client.delete(&control).bearer_auth("synthetic-control")
            .json(&json!({"archive":true,"expected_incarnation":1})).send().await?.error_for_status()?;
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            let deletion: Value = client.get(format!("{control}/delete-status"))
                .bearer_auth("synthetic-control").send().await?.error_for_status()?.json().await?;
            if deletion["job"]["status"] == "completed" {
                assert_eq!(deletion["identity"]["status"], "deleted");
                assert_eq!(deletion["identity"]["incarnation"], 1);
                break;
            }
            ensure!(Instant::now() < deadline, "reclamation did not finish");
            sleep(Duration::from_millis(20)).await;
        }
        bridge.abort();
        server.abort();
        host.shutdown().await?;
    }
    Ok(())
}
