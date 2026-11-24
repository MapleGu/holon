//! Isolated real native HTTP + stub-provider regressions. No native service binary or real config.
mod support;
use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use holon::{host::RuntimeHost, provider::{AgentProvider, StubProvider, ProviderTurnRequest, ProviderTurnResponse, ModelBlock, ModelToolCallKind}};
use reqwest::Client;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration, Instant};
use support::{TestConfigBuilder, spawn_server_for_host, ready_observer_get};

struct RecordingProvider {
    calls: AtomicUsize,
    frames: Mutex<Vec<String>>,
    inputs: Vec<Value>,
}
impl RecordingProvider { fn new(inputs: Vec<Value>) -> Self { Self { calls: AtomicUsize::new(0), frames: Mutex::new(vec![]), inputs } } }
#[async_trait]
impl AgentProvider for RecordingProvider {
    async fn complete_turn(&self, request: ProviderTurnRequest) -> Result<ProviderTurnResponse> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.frames.lock().unwrap().push(format!("{:?}", request.prompt_frame));
        let mut response = StubProvider::new("synthetic final reply").complete_turn(request.clone()).await?;
        if call == 0 && !self.inputs.is_empty() {
            let spec = request.tools.iter().find(|s| s.name == "ExecCommand").expect("native ExecCommand schema");
            assert!(spec.input_schema["properties"]["description"].is_object());
            assert!(!spec.input_schema["required"].as_array().unwrap().iter().any(|v| v == "description"));
            response.blocks = self.inputs.iter().enumerate().map(|(i,input)| ModelBlock::ToolUse {
                id: format!("caption-call-{i}"), name: "ExecCommand".into(), input: input.clone(),
                kind: ModelToolCallKind::Function, provider_data: None,
            }).collect();
        }
        Ok(response)
    }
}
async fn detail_ready(client: &Client, base: &str, agent: &str) -> Result<Value> {
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        let response = client.get(format!("{base}/api/control/agents/{agent}/detail")).send().await?.error_for_status()?;
        let detail: Value = response.json().await?;
        // The detail endpoint wraps its canonical record.
        let detail = detail.get("detail").cloned().unwrap_or(detail);
        if detail["bootstrap"]["status"] == "ready" || ["template", "model", "runtime", "workspace", "initial_message"].iter().any(|step| detail["bootstrap"][step]["status"] == "failed") { return Ok(detail); }
        ensure!(Instant::now() < until, "bootstrap timeout: {detail}");
        sleep(Duration::from_millis(20)).await;
    }
}
async fn wait_calls(provider: &RecordingProvider, count: usize) -> Result<()> {
    let until = Instant::now() + Duration::from_secs(15);
    while provider.calls.load(Ordering::SeqCst) < count {
        ensure!(Instant::now() < until, "provider count did not reach {count}");
        sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}
async fn create_default_case(flag: Option<bool>) -> Result<()> {
    let fixture = TestConfigBuilder::new().build();
    let provider = Arc::new(RecordingProvider::new(vec![]));
    let host = RuntimeHost::new_with_provider(fixture.config().clone(), provider.clone())?;
    let (base, server) = spawn_server_for_host(host.clone()).await?;
    let client = Client::new();
    let mut body = json!({"name":"Synthetic default"});
    if let Some(flag) = flag { body["default_initial_message"] = json!(flag); }
    let created: Value = client.post(format!("{base}/api/control/agents/default-case/create")).json(&body).send().await?.error_for_status()?.json().await?;
    assert_eq!(created["identity"]["incarnation"], 1);
    let detail = detail_ready(&client,&base,"default-case").await?;
    assert_eq!(detail["bootstrap"]["status"], "ready", "{detail}");
    wait_calls(&provider,1).await?;
    let runtime = host.get_public_agent("default-case").await?;
    let messages = runtime.storage().read_recent_messages(10)?;
    assert_eq!(messages.iter().filter(|m| m.metadata.as_ref().is_some_and(|m| m["bootstrap_default"] == true)).count(),1);
    assert_eq!(provider.calls.load(Ordering::SeqCst),1);
    server.abort();host.shutdown().await?;Ok(())
}
#[tokio::test] async fn chat_creation_omitted_flag_preserves_default_greeting() -> Result<()> { create_default_case(None).await }
#[tokio::test] async fn chat_creation_enabled_flag_preserves_default_greeting() -> Result<()> { create_default_case(Some(true)).await }

#[tokio::test]
async fn chat_creation_optout_loads_identity_then_first_prompt_executes_caption_and_legacy_cmd() -> Result<()> {
    let fixture = TestConfigBuilder::new().build();
    let marker = fixture.data_dir().join("caption-must-not-execute");
    let caption = format!("检查合成输出 $(touch '{}')",marker.display());
    let inputs = vec![json!({"cmd":"printf CAPTION_OK","description":caption,"login":false}),json!({"cmd":"printf LEGACY_OK","login":false})];
    let provider = Arc::new(RecordingProvider::new(inputs));
    let host = RuntimeHost::new_with_provider(fixture.config().clone(),provider.clone())?;
    let template = fixture.data_dir().join(".agents/agent_templates/chat-fixture");
    std::fs::create_dir_all(&template)?;
    std::fs::write(template.join("AGENTS.md"),"# Synthetic identity\nCHAT_BOOTSTRAP_IDENTITY_MARKER\n")?;
    let (base, server) = spawn_server_for_host(host.clone()).await?;let client = Client::new();
    let created: Value = client.post(format!("{base}/api/control/agents/chat-case/create"))
        .json(&json!({"template":"chat-fixture","name":"Chat fixture","default_initial_message":false})).send().await?.error_for_status()?.json().await?;
    assert_eq!(created["identity"]["incarnation"],1);
    let detail = detail_ready(&client,&base,"chat-case").await?;
    assert_eq!(detail["bootstrap"]["status"],"ready","{detail}");
    for step in ["template","model","runtime","workspace","initial_message"] { assert_eq!(detail["bootstrap"][step]["status"],"succeeded","{detail}"); }
    let runtime = host.get_public_agent("chat-case").await?;
    assert!(runtime.storage().read_recent_messages(10)?.is_empty());
    sleep(Duration::from_millis(150)).await;
    assert_eq!(provider.calls.load(Ordering::SeqCst),0,"optout must not consume a provider turn");
    let status: Value = ready_observer_get(client.get(format!("{base}/api/agents/chat-case/status?detail=full"))).await?.error_for_status()?.json().await?;
    assert_eq!(status["identity"]["incarnation"],1);
    assert_eq!(status["loaded_agents_md"]["agent_status"],"loaded","{status}");
    assert_eq!(status["model"]["effective_model"],"anthropic@default/claude-sonnet-4-6");
    client.post(format!("{base}/api/control/agents/chat-case/prompt"))
        .json(&json!({"text":"Run the two synthetic printf checks, then reply.","client_request_id":"first-user","expected_incarnation":1})).send().await?.error_for_status()?;
    wait_calls(&provider,2).await?;
    let until=Instant::now()+Duration::from_secs(15);
    loop {
        if !runtime.storage().read_recent_briefs(10)?.is_empty() {break;}
        ensure!(Instant::now()<until,"first user prompt did not produce a final brief");sleep(Duration::from_millis(20)).await;
    }
    assert!(provider.frames.lock().unwrap().iter().any(|s|s.contains("CHAT_BOOTSTRAP_IDENTITY_MARKER")));
    let executions=runtime.storage().read_recent_tool_executions(10)?;
    assert_eq!(executions.len(),2);
    for execution in executions {
        let wire:Value=client.get(format!("{base}/api/agents/chat-case/tool-executions/{}",execution.id))
            .header("x-holon-expected-incarnation","1").send().await?.error_for_status()?.json().await?;
        assert_eq!(wire["status"],"success","{wire}");
        if wire["input"]["cmd"] == "printf CAPTION_OK" {
            assert_eq!(wire["input"]["description"],caption);
            assert!(wire["output"].to_string().contains("CAPTION_OK"));
        } else {
            assert_eq!(wire["input"]["cmd"],"printf LEGACY_OK");assert!(wire["input"].get("description").is_none());
            assert!(wire["output"].to_string().contains("LEGACY_OK"));
        }
    }
    assert!(!marker.exists(),"caption content must never be executed");
    server.abort();host.shutdown().await?;Ok(())
}

#[tokio::test]
async fn chat_creation_optout_does_not_hide_template_failure() -> Result<()> {
    let fixture=TestConfigBuilder::new().build();let provider=Arc::new(RecordingProvider::new(vec![]));
    let host=RuntimeHost::new_with_provider(fixture.config().clone(),provider.clone())?;
    let (base,server)=spawn_server_for_host(host.clone()).await?;let client=Client::new();
    client.post(format!("{base}/api/control/agents/bad-template/create")).json(&json!({"template":"not-present","default_initial_message":false})).send().await?.error_for_status()?;
    let detail=detail_ready(&client,&base,"bad-template").await?;
    assert_eq!(detail["bootstrap"]["status"],"degraded");assert_eq!(detail["bootstrap"]["template"]["status"],"failed");
    assert_eq!(provider.calls.load(Ordering::SeqCst),0);server.abort();host.shutdown().await?;Ok(())
}

#[tokio::test]
async fn chat_creation_rejects_nonboolean_greeting_flag() -> Result<()> {
    let fixture=TestConfigBuilder::new().build();let host=RuntimeHost::new_with_provider(fixture.config().clone(),Arc::new(StubProvider::new("unused")))?;
    let (base,server)=spawn_server_for_host(host.clone()).await?;
    let response=Client::new().post(format!("{base}/api/control/agents/invalid-flag/create")).json(&json!({"default_initial_message":"false"})).send().await?;
    assert!(response.status().is_client_error());assert!(host.agent_identity_record("invalid-flag")?.is_none());
    server.abort();host.shutdown().await?;Ok(())
}
