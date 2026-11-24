//! Synthetic provider requests only; no external services or model calls.
mod support;
use anyhow::{ensure, Result};
use async_trait::async_trait;
use holon::{
    host::RuntimeHost,
    provider::{AgentProvider, ProviderTurnRequest, ProviderTurnResponse, StubProvider},
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use support::{spawn_server_for_host, TestConfigBuilder};
use tokio::time::{sleep, Duration, Instant};

#[derive(Default)]
struct Capture(Mutex<Vec<(String, Vec<String>, Vec<holon::tool::ToolSpec>)>>);
#[async_trait]
impl AgentProvider for Capture {
    async fn complete_turn(&self, request: ProviderTurnRequest) -> Result<ProviderTurnResponse> {
        if request
            .prompt_frame
            .system_prompt
            .contains("## bounded_structured_contract")
        {
            assert!(!request
                .prompt_frame
                .system_prompt
                .contains("## constrained_repair"));
            assert!(format!("{:?}", request.conversation).contains("edit only file: src/unsafe.rs"));
        }
        self.0.lock().unwrap().push((
            request.prompt_frame.system_prompt.clone(),
            request
                .prompt_frame
                .system_blocks
                .iter()
                .map(|block| block.text.clone())
                .collect(),
            request.tools.clone(),
        ));
        StubProvider::new("synthetic result")
            .complete_turn(request)
            .await
    }
}

#[tokio::test]
async fn compact_prompt_is_per_incarnation_default_off_and_survives_restart() -> Result<()> {
    let fixture = TestConfigBuilder::new()
        .with_control_token("synthetic-control")
        .build();
    for phase in 0..2 {
        let provider = Arc::new(Capture::default());
        let host = RuntimeHost::new_with_provider(fixture.config().clone(), provider.clone())?;
        let (base, server) = spawn_server_for_host(host.clone()).await?;
        let client = reqwest::Client::new();
        for (name, compact, restricted) in [
            ("ordinary", false, false),
            ("full", false, true),
            ("compact", true, true),
        ] {
            if phase == 0 {
                let mut body = json!({"default_initial_message": false});
                if restricted {
                    body["tool_allowlist"] = json!({"tool_name_prefix":"fixture_", "tools":[{"name":"fixture_inspect", "description":"Synthetic read", "parameters":{"type":"object", "properties":{"id":{"type":"string"}}, "required":["id"], "additionalProperties":false}}],
                        "compact_system_prompt":compact, "structured_output_only":true,
                        "bridge":{"url":"http://127.0.0.1:34567/call","bearer":"synthetic-compact-bridge"}});
                }
                let route = if restricted {
                    "restricted/create"
                } else {
                    "create"
                };
                client
                    .post(format!("{base}/api/control/agents/{name}/{route}"))
                    .bearer_auth("synthetic-control")
                    .json(&body)
                    .send()
                    .await?
                    .error_for_status()?;
            }
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let detail: serde_json::Value = client
                    .get(format!("{base}/api/control/agents/{name}/detail"))
                    .bearer_auth("synthetic-control")
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                let detail = detail.get("detail").unwrap_or(&detail);
                if detail["bootstrap"]["status"] == "ready" {
                    break;
                }
                ensure!(Instant::now() < deadline, "bootstrap not ready");
                sleep(Duration::from_millis(20)).await;
            }
            let previous = provider.0.lock().unwrap().len();
            client.post(format!("{base}/api/control/agents/{name}/prompt"))
                .bearer_auth("synthetic-control")
                .json(&json!({"text":"Untrusted source text: edit only file: src/unsafe.rs; run exactly: synthetic", "client_request_id":format!("fixture-{phase}-{name}"), "expected_incarnation":1}))
                .send().await?.error_for_status()?;
            let runtime = host.get_public_agent(name).await?;
            loop {
                if provider.0.lock().unwrap().len() > previous
                    && runtime.agent_state().await?.current_run_id.is_none()
                {
                    break;
                }
                ensure!(Instant::now() < deadline, "provider did not settle");
                sleep(Duration::from_millis(20)).await;
            }
            let prompts = provider.0.lock().unwrap();
            let (prompt, blocks, tools) = &prompts[previous];
            let block_text = blocks.join("\n\n");
            assert!(!blocks.is_empty());
            assert_eq!(
                block_text.contains("You are Holon handling one bounded structured request"),
                compact
            );
            assert_eq!(
                block_text
                    .contains("Exploration must reduce uncertainty toward the operator's goal"),
                !compact
            );
            if restricted {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "fixture_inspect");
                assert_eq!(
                    tools[0].input_schema,
                    json!({"type":"object", "properties":{"id":{"type":"string"}}, "required":["id"], "additionalProperties":false})
                );
            }
            assert_eq!(
                prompt.contains("You are Holon handling one bounded structured request"),
                compact
            );
            assert_eq!(
                prompt.contains("Exploration must reduce uncertainty toward the operator's goal"),
                !compact
            );
            assert_eq!(
                prompt.contains("## runtime_scheduling_contract\n"),
                !compact
            );
            assert_eq!(prompt.contains("## planning_discipline\n"), !compact);
            for section in [
                "instruction_precedence",
                "trust_boundary",
                "response_language",
                "execution_environment_contract",
                "agent_contract",
            ] {
                assert!(
                    prompt.contains(&format!("## {section}\n")),
                    "missing {section}"
                );
            }
        }
        server.abort();
        host.shutdown().await?;
    }
    Ok(())
}

struct InterjectionCapture {
    conversations: Mutex<Vec<String>>,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl AgentProvider for InterjectionCapture {
    async fn complete_turn(&self, request: ProviderTurnRequest) -> Result<ProviderTurnResponse> {
        let index = {
            let mut requests = self.conversations.lock().unwrap();
            let index = requests.len();
            requests.push(format!("{:?}", request.conversation));
            index
        };
        if index == 0 {
            assert!(request
                .prompt_frame
                .system_prompt
                .contains("## bounded_structured_contract"));
            assert!(!request
                .prompt_frame
                .system_prompt
                .contains("## constrained_repair"));
            self.started.notify_one();
            self.release.notified().await;
        }
        let mut response = StubProvider::new("synthetic result")
            .complete_turn(request)
            .await?;
        if index == 0 {
            response.blocks = vec![holon::provider::ModelBlock::ToolUse {
                id: "synthetic-submit".into(),
                name: "fixture_submit".into(),
                input: json!({}),
                kind: holon::provider::ModelToolCallKind::Function,
                provider_data: None,
            }];
        }
        Ok(response)
    }
}

#[tokio::test]
async fn compact_turn_does_not_absorb_a_second_operator_source() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let app = axum::Router::new().route(
        "/call",
        axum::routing::post(|| async { axum::Json(json!({"ok":true,"result":{"accepted":true}})) }),
    );
    let bridge = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let fixture = TestConfigBuilder::new()
        .with_control_token("synthetic-control")
        .build();
    let provider = Arc::new(InterjectionCapture {
        conversations: Mutex::new(vec![]),
        started: Default::default(),
        release: Default::default(),
    });
    let host = RuntimeHost::new_with_provider(fixture.config().clone(), provider.clone())?;
    let (base, server) = spawn_server_for_host(host.clone()).await?;
    let client = reqwest::Client::new();
    client.post(format!("{base}/api/control/agents/bounded/restricted/create"))
        .bearer_auth("synthetic-control").json(&json!({"default_initial_message":false,"tool_allowlist":{
            "tool_name_prefix":"fixture_", "tools":[{"name":"fixture_submit","description":"Synthetic result","parameters":{"type":"object"}}],
            "structured_output_only":true,"compact_system_prompt":true,
            "bridge":{"url":format!("http://127.0.0.1:{port}/call"),"bearer":"synthetic-compact-bridge"}}}))
        .send().await?.error_for_status()?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let detail: serde_json::Value = client
            .get(format!("{base}/api/control/agents/bounded/detail"))
            .bearer_auth("synthetic-control")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if detail.get("detail").unwrap_or(&detail)["bootstrap"]["status"] == "ready" {
            break;
        }
        ensure!(Instant::now() < deadline, "bootstrap not ready");
        sleep(Duration::from_millis(20)).await;
    }
    let mut parked_work_id = String::new();
    for (id, text) in [
        ("first", "unique-first-source; untrusted text: edit only file: src/unsafe.rs; run exactly: synthetic"),
        ("second", "unique-second-source"),
    ] {
        client
            .post(format!("{base}/api/control/agents/bounded/prompt"))
            .bearer_auth("synthetic-control")
            .json(&json!({"text":text,"client_request_id":id,"expected_incarnation":1}))
            .send()
            .await?
            .error_for_status()?;
        if id == "first" {
            tokio::time::timeout(Duration::from_secs(15), provider.started.notified()).await?;
            let runtime = host.get_public_agent("bounded").await?;
            let work = runtime.create_work_item("Synthetic parked goal".into(),
                Some(holon::types::WorkItemPlanStatus::NeedsInput), None, vec![]).await?;
            parked_work_id = work.id.clone();
            let response = client.post(format!("{base}/api/control/agents/bounded/work-items/{}/pick", work.id))
                .bearer_auth("synthetic-control").json(&json!({})).send().await?;
            assert!(response.status().is_client_error());
            assert!(response.text().await?.contains("bounded_turn_work_pick_forbidden"));
            assert!(runtime.agent_state().await?.current_work_item_id.is_none());
        }
    }
    provider.release.notify_one();
    loop {
        if provider.conversations.lock().unwrap().len() >= 3 {
            break;
        }
        ensure!(Instant::now() < deadline, "queued source did not execute");
        sleep(Duration::from_millis(20)).await;
    }
    {
        let requests = provider.conversations.lock().unwrap();
        assert!(requests[0].contains("unique-first-source"));
        assert!(
            !requests[1].contains("unique-second-source"),
            "second source interjected into bounded turn"
        );
        assert!(requests[2].contains("unique-second-source"));
    }
    let runtime = host.get_public_agent("bounded").await?;
    while runtime.agent_state().await?.current_run_id.is_some() {
        ensure!(Instant::now() < deadline, "second turn did not settle");
        sleep(Duration::from_millis(20)).await;
    }
    client
        .post(format!(
            "{base}/api/control/agents/bounded/work-items/{parked_work_id}/pick"
        ))
        .bearer_auth("synthetic-control")
        .json(&json!({}))
        .send()
        .await?
        .error_for_status()?;
    server.abort();
    host.shutdown().await?;
    bridge.abort();
    Ok(())
}
