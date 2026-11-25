//! Synthetic provider and local authenticated HTTP only. No model calls or external effects.
mod support;
use anyhow::{ensure, Result};
use async_trait::async_trait;
use holon::{
    host::RuntimeHost,
    provider::{
        AgentProvider, ModelBlock, ModelToolCallKind, ProviderTurnRequest, ProviderTurnResponse,
        StubProvider,
    },
    types::{QueueEntryStatus, TurnNoBriefReason, TurnTerminalKind},
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use support::{spawn_server_for_host, TestConfigBuilder};
use tokio::time::{sleep, timeout, Duration, Instant};

#[derive(Default)]
struct BlockingProvider {
    calls: AtomicUsize,
    started: tokio::sync::Notify,
    emit_tool: bool,
    invalid_tool: bool,
    tool_rounds: AtomicUsize,
}
#[async_trait]
impl AgentProvider for BlockingProvider {
    async fn complete_turn(&self, request: ProviderTurnRequest) -> Result<ProviderTurnResponse> {
        let round = self.calls.fetch_add(1, Ordering::SeqCst);
        let first = round == 0 || round < self.tool_rounds.load(Ordering::SeqCst);
        self.started.notify_one();
        if self.emit_tool && first {
            let mut response = StubProvider::new("synthetic submit")
                .complete_turn(request)
                .await?;
            response.blocks = vec![ModelBlock::ToolUse {
                id: format!("synthetic-submit-{round}"),
                name: "fixture_submit".into(),
                input: if self.invalid_tool {
                    json!([])
                } else {
                    json!({})
                },
                kind: ModelToolCallKind::Function,
                provider_data: None,
            }];
            return Ok(response);
        }
        std::future::pending().await
    }
}
async fn ready(client: &reqwest::Client, base: &str, agent: &str) -> Result<()> {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        ensure!(Instant::now() < end, "synthetic bootstrap not ready");
        let response = client
            .get(format!("{base}/api/agents/{agent}/status"))
            .bearer_auth("synthetic-control")
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            sleep(Duration::from_millis(10)).await;
            continue;
        }
        let value: Value = response.error_for_status()?.json().await?;
        if value["identity"]["status"] == "active" && value["model"]["effective_model"].is_string()
        {
            return Ok(());
        }
        ensure!(Instant::now() < end, "synthetic bootstrap not ready");
        sleep(Duration::from_millis(10)).await;
    }
}
async fn fixture(
    structured: bool,
) -> Result<(
    support::TestConfig,
    RuntimeHost,
    String,
    support::TestServerHandle,
    reqwest::Client,
    Arc<BlockingProvider>,
)> {
    fixture_with_bridge(structured, None, false).await
}
async fn fixture_with_bridge(
    structured: bool,
    bridge: Option<(u16, bool)>,
    invalid_tool: bool,
) -> Result<(
    support::TestConfig,
    RuntimeHost,
    String,
    support::TestServerHandle,
    reqwest::Client,
    Arc<BlockingProvider>,
)> {
    let fixture = TestConfigBuilder::new()
        .with_control_token("synthetic-control")
        .build();
    let provider = Arc::new(BlockingProvider {
        emit_tool: bridge.is_some(),
        invalid_tool,
        ..Default::default()
    });
    let host = RuntimeHost::new_with_provider(fixture.config().clone(), provider.clone())?;
    let (base, server) = spawn_server_for_host(host.clone()).await?;
    let c = reqwest::Client::new();
    c.post(format!("{base}/api/control/agents/fixture-anchor/create"))
        .bearer_auth("synthetic-control")
        .json(&json!({"default_initial_message":false}))
        .send()
        .await?
        .error_for_status()?;
    ready(&c, &base, "fixture-anchor").await?;
    let mut policy = json!({"tool_name_prefix":"fixture_", "structured_output_only":structured,
        "tools":[{"name":"fixture_submit","description":"Synthetic only","parameters":{"type":"object"}}],
        "bridge":{"url":format!("http://127.0.0.1:{}/call", bridge.map(|b| b.0).unwrap_or(34567)),"bearer":"synthetic-ephemeral-bearer-0012"}});
    if bridge.is_some_and(|b| b.1) {
        policy["completion_tool"] = json!("fixture_submit");
    }
    c.post(format!(
        "{base}/api/control/agents/subject/restricted/create"
    ))
    .bearer_auth("synthetic-control")
    .json(&json!({"default_initial_message":false,"tool_allowlist":policy}))
    .send()
    .await?
    .error_for_status()?;
    ready(&c, &base, "subject").await?;
    Ok((fixture, host, base, server, c, provider))
}
fn cancel(
    c: &reqwest::Client,
    base: &str,
    key: &str,
    message: &str,
    run: &str,
) -> reqwest::RequestBuilder {
    cancel_scoped(c, base, key, message, run, 1, "synthetic-control")
}
fn cancel_scoped(
    c: &reqwest::Client,
    base: &str,
    key: &str,
    message: &str,
    run: &str,
    incarnation: u64,
    token: &str,
) -> reqwest::RequestBuilder {
    c.post(format!("{base}/api/control/agents/subject/prompt-cancel"))
        .bearer_auth(token)
        .header("x-holon-expected-incarnation", incarnation.to_string())
        .json(&json!({"client_request_id":key,"abort_claimed":{"message_id":message,"run_id":run}}))
}
async fn terminal(
    runtime: &holon::runtime::RuntimeHandle,
    host: &RuntimeHost,
    message: &str,
) -> Result<()> {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let state = runtime.agent_state().await?;
        let turns = runtime.storage().read_recent_turns(10)?;
        if let Some(turn) = turns.iter().find(|t| t.input_message_ids == [message]) {
            if let Some(t) = turn.terminal.as_ref() {
                assert_eq!(t.kind, TurnTerminalKind::Aborted);
                assert_eq!(t.no_brief_reason, Some(TurnNoBriefReason::Aborted));
                assert_eq!(t.reason.as_deref(), Some("structured_prompt_cancelled"));
                assert!(turn.produced_brief_ids.is_empty());
                if state.current_run_id != turn.run_id
                    && !state
                        .current_execution_binding
                        .as_ref()
                        .is_some_and(|b| b.source_message_id == message)
                {
                    let settled: bool = host.runtime_db().transaction(|tx| Ok(tx.query_row(
                        "SELECT result_settled FROM conversation_turn_revisions WHERE agent_id='subject' AND turn_id=?1",
                        [&turn.turn_id], |r| r.get(0))?))?;
                    assert!(settled);
                    assert!(runtime
                        .storage()
                        .latest_queue_entries()?
                        .iter()
                        .any(|q| q.message_id == message && q.status == QueueEntryStatus::Aborted));
                    return Ok(());
                }
            }
        }
        ensure!(Instant::now() < end, "real abort did not settle exact turn");
        sleep(Duration::from_millis(10)).await;
    }
}
#[tokio::test]
async fn claimed_prompt_abort_exact_http_scope_and_native_terminal() -> Result<()> {
    let (_fixture, host, base, server, c, provider) = fixture(true).await?;
    let runtime = host.get_public_agent("subject").await?;
    let caps: Value = c
        .get(format!(
            "{base}/api/control/runtime/restricted-agent-capabilities"
        ))
        .bearer_auth("synthetic-control")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(caps["claimedPromptAbort"], true);
    let mut previous: Option<(String, String, String)> = None;
    let mut queued: Option<Value> = None;
    for n in 0..2 {
        let key = format!("synthetic-{n}");
        let prompt: Value = if let Some(queued) = queued.take() {
            queued
        } else {
            c.post(format!("{base}/api/control/agents/subject/prompt")).bearer_auth("synthetic-control")
                .json(&json!({"text":"Synthetic blocked request","expected_incarnation":1,"client_request_id":key})).send().await?.error_for_status()?.json().await?
        };
        timeout(Duration::from_secs(10), provider.started.notified()).await?;
        let message = prompt["message_id"].as_str().unwrap();
        let run = runtime.agent_state().await?.current_run_id.unwrap();
        if let Some((old_key, old_message, old_run)) = &previous {
            assert_eq!(
                cancel(&c, &base, old_key, old_message, old_run)
                    .send()
                    .await?
                    .status(),
                409
            );
            assert_eq!(
                runtime.agent_state().await?.current_run_id.as_deref(),
                Some(run.as_str())
            );
        }
        for (key, source, expected_run) in [
            ("wrong-key", message, run.as_str()),
            (key.as_str(), "wrong-source", run.as_str()),
            (key.as_str(), message, "stale-run"),
        ] {
            assert_eq!(
                cancel(&c, &base, key, source, expected_run)
                    .send()
                    .await?
                    .status(),
                409
            );
            assert_eq!(
                runtime.agent_state().await?.current_run_id.as_deref(),
                Some(run.as_str())
            );
        }
        assert_eq!(
            cancel_scoped(&c, &base, &key, message, &run, 2, "synthetic-control")
                .send()
                .await?
                .status(),
            409
        );
        assert_eq!(
            cancel_scoped(&c, &base, &key, message, &run, 1, "wrong")
                .send()
                .await?
                .status(),
            401
        );
        let ordinary: Value = c
            .post(format!("{base}/api/control/agents/subject/prompt-cancel"))
            .bearer_auth("synthetic-control")
            .header("x-holon-expected-incarnation", "1")
            .json(&json!({"client_request_id":key}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(ordinary["status"], "already_claimed");
        assert_eq!(
            runtime.agent_state().await?.current_run_id.as_deref(),
            Some(run.as_str())
        );
        if n == 0 {
            queued = Some(c.post(format!("{base}/api/control/agents/subject/prompt")).bearer_auth("synthetic-control")
                .json(&json!({"text":"Independent next input","expected_incarnation":1,"client_request_id":"synthetic-1"})).send().await?.error_for_status()?.json().await?);
        }
        let ack: Value = cancel(&c, &base, &key, message, &run)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(
            ack,
            json!({"agentId":"subject","incarnation":1,"clientRequestId":key,"status":"abort_requested","messageId":message,"runId":run})
        );
        terminal(&runtime, &host, message).await?;
        previous = Some((key.clone(), message.to_owned(), run.clone()));
        assert_eq!(
            cancel(&c, &base, &key, message, &run)
                .send()
                .await?
                .status(),
            409
        );
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    host.spawn_daemon_deletion_coordinator();
    c.post(format!(
        "{base}/api/control/agents/subject/stop-if-incarnation"
    ))
    .bearer_auth("synthetic-control")
    .json(&json!({"expected_incarnation":1}))
    .send()
    .await?
    .error_for_status()?;
    c.delete(format!("{base}/api/control/agents/subject"))
        .bearer_auth("synthetic-control")
        .json(&json!({"expected_incarnation":1,"archive":true}))
        .send()
        .await?
        .error_for_status()?;
    host.shutdown().await?;
    server.abort();
    Ok(())
}
#[tokio::test]
async fn claimed_prompt_abort_rejects_ordinary_policy_and_conflicting_wire_modes() -> Result<()> {
    let (_fixture, host, base, server, c, provider) = fixture(false).await?;
    let runtime = host.get_public_agent("subject").await?;
    let prompt: Value = c.post(format!("{base}/api/control/agents/subject/prompt")).bearer_auth("synthetic-control")
        .json(&json!({"text":"Synthetic ordinary policy","client_request_id":"ordinary","expected_incarnation":1})).send().await?.error_for_status()?.json().await?;
    timeout(Duration::from_secs(10), provider.started.notified()).await?;
    let message = prompt["message_id"].as_str().unwrap();
    let run = runtime.agent_state().await?.current_run_id.unwrap();
    assert_eq!(
        cancel(&c, &base, "ordinary", message, &run)
            .send()
            .await?
            .status(),
        409
    );
    for body in [
        json!({"client_request_id":"ordinary","complete_structured_output":true,"abort_claimed":{"message_id":message,"run_id":run}}),
        json!({"client_request_id":"ordinary","abort_claimed":null}),
        json!({"client_request_id":"ordinary","abort_claimed":{"message_id":message}}),
        json!({"client_request_id":"ordinary","abort_claimed":{"message_id":message,"run_id":run,"all":true}}),
    ] {
        assert!(c
            .post(format!("{base}/api/control/agents/subject/prompt-cancel"))
            .bearer_auth("synthetic-control")
            .header("x-holon-expected-incarnation", "1")
            .json(&body)
            .send()
            .await?
            .status()
            .is_client_error());
    }
    assert_eq!(
        runtime.agent_state().await?.current_run_id.as_deref(),
        Some(run.as_str())
    );
    host.shutdown().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn claimed_prompt_abort_never_overrides_an_inflight_submit_acceptance() -> Result<()> {
    for completion_declared in [false, true] {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let app = axum::Router::new().route(
            "/call",
            axum::routing::post({
                let entered = entered.clone();
                let release = release.clone();
                move || {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.notify_one(); // Synthetic durable acceptance, reply still in flight.
                        release.notified().await;
                        axum::Json(
                            json!({"ok":true,"complete_turn":true,"result":{"accepted":true}}),
                        )
                    }
                }
            }),
        );
        let bridge = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (_fixture, host, base, server, c, _provider) =
            fixture_with_bridge(true, Some((port, completion_declared)), false).await?;
        let runtime = host.get_public_agent("subject").await?;
        let prompt: Value = c.post(format!("{base}/api/control/agents/subject/prompt")).bearer_auth("synthetic-control")
            .json(&json!({"text":"Synthetic submit","client_request_id":"submit","expected_incarnation":1})).send().await?.error_for_status()?.json().await?;
        timeout(Duration::from_secs(10), entered.notified()).await?;
        let run = runtime.agent_state().await?.current_run_id.unwrap();
        assert_eq!(
            cancel(
                &c,
                &base,
                "submit",
                prompt["message_id"].as_str().unwrap(),
                &run
            )
            .send()
            .await?
            .status(),
            409
        );
        assert_eq!(
            runtime.agent_state().await?.current_run_id.as_deref(),
            Some(run.as_str())
        );
        release.notify_one();
        if completion_declared {
            timeout(Duration::from_secs(10), async {
                loop {
                    let turns = runtime.storage().read_recent_turns(5).unwrap();
                    if turns.iter().any(|t| {
                        t.input_message_ids == [prompt["message_id"].as_str().unwrap()]
                            && t.terminal
                                .as_ref()
                                .is_some_and(|end| end.kind == TurnTerminalKind::Completed)
                    }) {
                        break;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
        }
        host.shutdown().await?;
        server.abort();
        bridge.abort();
    }
    Ok(())
}

#[tokio::test]
async fn claimed_prompt_abort_after_local_submit_validation_failure_is_not_fenced() -> Result<()> {
    let (_fixture, host, base, server, c, provider) =
        fixture_with_bridge(true, Some((34567, true)), true).await?;
    let runtime = host.get_public_agent("subject").await?;
    let prompt: Value = c.post(format!("{base}/api/control/agents/subject/prompt")).bearer_auth("synthetic-control")
        .json(&json!({"text":"Synthetic invalid submit then stalled inference","client_request_id":"invalid-submit","expected_incarnation":1})).send().await?.error_for_status()?.json().await?;
    timeout(Duration::from_secs(10), async {
        while provider.calls.load(Ordering::SeqCst) < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let run = runtime.agent_state().await?.current_run_id.unwrap();
    assert_eq!(
        cancel(
            &c,
            &base,
            "invalid-submit",
            prompt["message_id"].as_str().unwrap(),
            &run
        )
        .send()
        .await?
        .status(),
        200
    );
    terminal(&runtime, &host, prompt["message_id"].as_str().unwrap()).await?;
    host.shutdown().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn claimed_prompt_abort_only_releases_its_own_explicit_rejection() -> Result<()> {
    // Last case: a later explicit rejection cannot clear an earlier unknown send.
    for scenario in [
        "rejected",
        "accepted",
        "unknown",
        "retryable-missing",
        "unknown-then-rejected",
        "unknown-then-final-rejected",
        "accepted-then-final-rejected",
        "retryable-malformed",
        "rejected-control-false",
        "control-null",
        "control-string",
        "control-object",
        "control-true",
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let calls = Arc::new(AtomicUsize::new(0));
        let bridge_calls = calls.clone();
        let app = axum::Router::new().route(
            "/call",
            axum::routing::post(move || {
                let calls = bridge_calls.clone();
                async move {
                    let index = calls.fetch_add(1, Ordering::SeqCst);
                    if scenario == "unknown"
                        || (scenario.starts_with("unknown-then-") && index == 0)
                    {
                        return (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            axum::Json(json!({"ok":false})),
                        );
                    }
                    let result = if scenario == "accepted"
                        || (scenario == "accepted-then-final-rejected" && index == 0)
                    {
                        json!({"accepted":true})
                    } else if scenario == "retryable-missing" {
                        json!({"accepted":false})
                    } else if scenario.ends_with("final-rejected") {
                        json!({"accepted":false,"retryable":false})
                    } else if scenario == "retryable-malformed" {
                        json!({"accepted":false,"retryable":"false"})
                    } else {
                        json!({"accepted":false,"retryable":true})
                    };
                    let mut reply = json!({"ok":true,"result":result});
                    match scenario {
                        "rejected-control-false" => reply["complete_turn"] = json!(false),
                        "control-null" => reply["complete_turn"] = Value::Null,
                        "control-string" => reply["complete_turn"] = json!("true"),
                        "control-object" => reply["complete_turn"] = json!({"state":false}),
                        "control-true" => reply["complete_turn"] = json!(true),
                        _ => {}
                    }
                    (axum::http::StatusCode::OK, axum::Json(reply))
                }
            }),
        );
        let bridge = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (_fixture, host, base, server, c, provider) =
            fixture_with_bridge(true, Some((port, true)), false).await?;
        let tool_rounds = if scenario.starts_with("unknown-then-")
            || scenario == "accepted-then-final-rejected"
        {
            2
        } else {
            1
        };
        provider.tool_rounds.store(tool_rounds, Ordering::SeqCst);
        let runtime = host.get_public_agent("subject").await?;
        let prompt: Value = c.post(format!("{base}/api/control/agents/subject/prompt")).bearer_auth("synthetic-control")
            .json(&json!({"text":"Synthetic submit response then stalled inference","client_request_id":"retryable-submit","expected_incarnation":1})).send().await?.error_for_status()?.json().await?;
        if scenario == "control-true" {
            let turn = timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(turn) = runtime
                        .storage()
                        .read_recent_turns(5)
                        .unwrap()
                        .into_iter()
                        .find(|t| {
                            t.input_message_ids == [prompt["message_id"].as_str().unwrap()]
                                && t.terminal
                                    .as_ref()
                                    .is_some_and(|t| t.kind == TurnTerminalKind::Completed)
                        })
                    {
                        break turn;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            assert_eq!(
                cancel(
                    &c,
                    &base,
                    "retryable-submit",
                    prompt["message_id"].as_str().unwrap(),
                    turn.run_id.as_deref().unwrap()
                )
                .send()
                .await?
                .status(),
                409
            );
            host.shutdown().await?;
            server.abort();
            bridge.abort();
            continue;
        }
        timeout(Duration::from_secs(10), async {
            while provider.calls.load(Ordering::SeqCst) <= tool_rounds {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert_eq!(calls.load(Ordering::SeqCst), tool_rounds);
        let run = runtime.agent_state().await?.current_run_id.unwrap();
        let response = cancel(
            &c,
            &base,
            "retryable-submit",
            prompt["message_id"].as_str().unwrap(),
            &run,
        )
        .send()
        .await?;
        if scenario == "rejected" || scenario == "rejected-control-false" {
            assert_eq!(response.status(), 200);
            terminal(&runtime, &host, prompt["message_id"].as_str().unwrap()).await?;
        } else {
            assert_eq!(response.status(), 409, "scenario={scenario}");
            assert_eq!(
                runtime.agent_state().await?.current_run_id.as_deref(),
                Some(run.as_str())
            );
        }
        host.shutdown().await?;
        server.abort();
        bridge.abort();
    }
    Ok(())
}

#[tokio::test]
async fn claimed_prompt_abort_after_three_rejected_submissions_keeps_native_reason() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let calls = Arc::new(AtomicUsize::new(0));
    let bridge_calls = calls.clone();
    let app = axum::Router::new().route("/call", axum::routing::post(move || {
        let calls = bridge_calls.clone(); async move {
            let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
            assert!(attempt <= 3);
            let result = if attempt < 3 {
                json!({"accepted":false,"retryable":true,"code":"MAIL_STEP_RESULT_INVALID"})
            } else {
                json!({"accepted":false,"retryable":false,"code":"HOLON_STEP_SUBMISSION_RETRIES_EXHAUSTED"})
            };
            axum::Json(json!({"ok":true,"result":result}))
        }
    }));
    let bridge = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (_fixture, host, base, server, c, provider) =
        fixture_with_bridge(true, Some((port, true)), false).await?;
    provider.tool_rounds.store(3, Ordering::SeqCst);
    let runtime = host.get_public_agent("subject").await?;
    let prompt: Value = c.post(format!("{base}/api/control/agents/subject/prompt")).bearer_auth("synthetic-control")
        .json(&json!({"text":"Synthetic validation exhausted then stalled inference","client_request_id":"exhausted-submit","expected_incarnation":1})).send().await?.error_for_status()?.json().await?;
    timeout(Duration::from_secs(10), async {
        while provider.calls.load(Ordering::SeqCst) < 4 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let run = runtime.agent_state().await?.current_run_id.unwrap();
    let ack: Value = cancel(
        &c,
        &base,
        "exhausted-submit",
        prompt["message_id"].as_str().unwrap(),
        &run,
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    assert_eq!(ack["status"], "abort_requested");
    terminal(&runtime, &host, prompt["message_id"].as_str().unwrap()).await?;
    host.shutdown().await?;
    server.abort();
    bridge.abort();
    Ok(())
}
