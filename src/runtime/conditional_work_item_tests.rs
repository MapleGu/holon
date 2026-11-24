use super::*;
use crate::runtime::tests::support::{StubProvider, context_config, tempdir};
use crate::types::{AgentIdentityRecord, AgentOwnership, AgentProfilePreset, AgentVisibility};

fn fixture() -> (tempfile::TempDir, RuntimeHandle) {
    let home = tempdir().unwrap();
    let runtime = RuntimeHandle::new(
        "synthetic-conditional",
        home.path().into(),
        InitialWorkspaceBinding::Detached,
        String::new(),
        Arc::new(StubProvider::new("unused")),
        "default".into(),
        context_config(),
    )
    .unwrap();
    (home, runtime)
}

#[tokio::test]
async fn issue134_conditional_create_writes_native_record_and_rejects_wrong_incarnation() {
    let (_home, runtime) = fixture();
    let incarnation = runtime.instance_incarnation();
    assert!(
        runtime
            .create_work_item_if_incarnation(incarnation + 1, "synthetic wrong owner".into())
            .await
            .is_err()
    );
    assert!(
        runtime
            .runtime_db()
            .work_items()
            .latest_for_agent("synthetic-conditional", 10)
            .unwrap()
            .is_empty()
    );
    let record = runtime
        .create_work_item_if_incarnation(incarnation, "synthetic create objective".into())
        .await
        .unwrap();
    assert_eq!(record.agent_id, "synthetic-conditional");
    assert_eq!(record.objective, "synthetic create objective");
    assert_eq!(
        runtime
            .runtime_db()
            .work_items()
            .latest(&record.id)
            .unwrap()
            .unwrap()
            .id,
        record.id
    );
    runtime.join_owned_mutations().await.unwrap();
}

#[tokio::test]
async fn issue134_conditional_create_revalidates_durable_owner_after_waiting_for_mutation() {
    let (_home, runtime) = fixture();
    let incarnation = runtime.instance_incarnation();
    let lock = runtime.begin_mutation().await.unwrap();
    let request =
        runtime.create_work_item_if_incarnation(incarnation, "synthetic stale writer".into());
    tokio::pin!(request);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut request)
            .await
            .is_err()
    );
    // Deliberate test-only corruption simulates retirement after admission, before writer reservation.
    let mut successor = AgentIdentityRecord::new(
        "synthetic-conditional",
        AgentKind::Default,
        AgentVisibility::Public,
        AgentOwnership::SelfOwned,
        AgentProfilePreset::PublicNamed,
        None,
        None,
    );
    successor.incarnation = incarnation + 1;
    runtime
        .runtime_db()
        .agent_identities()
        .upsert(&successor)
        .unwrap();
    drop(lock);
    assert!(request.await.is_err());
    assert!(
        runtime
            .runtime_db()
            .work_items()
            .latest_for_agent("synthetic-conditional", 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        runtime
            .runtime_db()
            .agent_identities()
            .latest("synthetic-conditional")
            .unwrap()
            .unwrap()
            .incarnation,
        incarnation + 1
    );
    runtime.join_owned_mutations().await.unwrap();
}

#[tokio::test]
async fn issue134_conditional_create_finishes_owned_write_after_caller_disconnects() {
    let (_home, runtime) = fixture();
    let lock = runtime.begin_mutation().await.unwrap();
    let clone = runtime.clone();
    let caller = tokio::spawn(async move {
        clone
            .create_work_item_if_incarnation(
                clone.instance_incarnation(),
                "synthetic disconnected request".into(),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(lock);
    runtime.join_owned_mutations().await.unwrap();
    let rows = runtime
        .runtime_db()
        .work_items()
        .latest_for_agent("synthetic-conditional", 10)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].objective, "synthetic disconnected request");
}

#[tokio::test]
async fn issue134_event_wait_is_native_without_timer_and_rejects_active_run() {
    let (_home, runtime) = fixture();
    let item = runtime
        .create_work_item_if_incarnation(
            runtime.instance_incarnation(),
            "synthetic event waiting".into(),
        )
        .await
        .unwrap();
    {
        let mut mutation = runtime.begin_mutation().await.unwrap();
        mutation.state.status = AgentStatus::AwakeRunning;
        mutation.state.current_run_id = Some("synthetic-active-run".into());
        mutation
            .persist_state(&runtime.inner.storage)
            .await
            .unwrap();
    }
    assert!(
        runtime
            .wait_work_item_for_event(item.id.clone(), "synthetic wait".into())
            .await
            .is_err()
    );
    assert!(
        runtime
            .runtime_db()
            .wait_conditions()
            .latest_for_agent("synthetic-conditional")
            .unwrap()
            .is_empty()
    );
    {
        let mut mutation = runtime.begin_mutation().await.unwrap();
        mutation.state.current_run_id = None;
        mutation.state.status = AgentStatus::AwakeIdle;
        mutation
            .persist_state(&runtime.inner.storage)
            .await
            .unwrap();
    }
    let waiting = runtime
        .wait_work_item_for_event(item.id.clone(), "synthetic event".into())
        .await
        .unwrap();
    assert!(waiting.recheck_at.is_none());
    let waits = runtime
        .runtime_db()
        .wait_conditions()
        .active_for_agent("synthetic-conditional")
        .unwrap();
    assert_eq!(waits.len(), 1);
    assert_eq!(waits[0].work_item_id.as_deref(), Some(item.id.as_str()));
    assert_eq!(
        waiting.blocked_by_wait_id.as_deref(),
        Some(waits[0].id.as_str())
    );
    assert_eq!(waits[0].kind, crate::types::WaitConditionKind::Operator);
    assert!(waits[0].recheck_at().is_none());
    assert!(
        runtime
            .runtime_db()
            .timers()
            .latest_all()
            .unwrap()
            .is_empty()
    );
}

struct PureTextFollowUpProvider(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl crate::provider::AgentProvider for PureTextFollowUpProvider {
    async fn complete_turn(
        &self,
        request: crate::provider::ProviderTurnRequest,
    ) -> Result<crate::provider::ProviderTurnResponse> {
        assert!(
            request.tools.is_empty(),
            "restricted follow-up exposes no executable tools or bridge"
        );
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        crate::provider::AgentProvider::complete_turn(
            &StubProvider::new("synthetic canonical follow-up result"),
            request,
        )
        .await
    }
    fn configured_model_refs(&self) -> Vec<String> {
        vec!["stub".into()]
    }
}

#[tokio::test]
async fn issue134_restart_event_prompt_produces_exact_canonical_text_brief_without_sleep_calls() {
    use crate::types::{
        AuthorityClass, BriefKind, MessageBody, MessageEnvelope, MessageKind, MessageOrigin,
        Priority, WaitConditionStatus,
    };
    use std::sync::atomic::Ordering;
    let home = tempdir().unwrap();
    let provider = Arc::new(PureTextFollowUpProvider(
        std::sync::atomic::AtomicUsize::new(0),
    ));
    let new_runtime = || {
        RuntimeHandle::new(
            "synthetic-restart",
            home.path().into(),
            InitialWorkspaceBinding::Detached,
            String::new(),
            provider.clone(),
            "default".into(),
            context_config(),
        )
        .unwrap()
    };
    let runtime = new_runtime();
    let incarnation = runtime.instance_incarnation();
    // Synthetic 0007 restriction persists across restart; no callback server is started.
    let mut identity = AgentIdentityRecord::new(
        "synthetic-restart",
        AgentKind::Default,
        AgentVisibility::Public,
        AgentOwnership::SelfOwned,
        AgentProfilePreset::PublicNamed,
        None,
        None,
    );
    identity.incarnation = incarnation;
    identity.tool_allowlist_required = true;
    runtime
        .runtime_db()
        .agent_identities()
        .upsert(&identity)
        .unwrap();
    let policy: crate::tool::restricted::ToolAllowlist = serde_json::from_value(serde_json::json!({
        "tool_name_prefix": "fixture_",
        "tools": [], "bridge": {"url":"http://127.0.0.1:9/call", "bearer":"synthetic-closed-bridge-134"}
    })).unwrap();
    runtime
        .runtime_db()
        .transaction(|tx| crate::tool::restricted::persist(tx, &identity, Some(&policy)))
        .unwrap();
    let item = runtime
        .create_work_item_if_incarnation(incarnation, "synthetic wait for later evidence".into())
        .await
        .unwrap();
    let waiting = runtime
        .wait_work_item_for_event(item.id.clone(), "synthetic external evidence".into())
        .await
        .unwrap();
    let wait_id = waiting.blocked_by_wait_id.unwrap();
    let original_loop = tokio::spawn(runtime.clone().run());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        provider.0.load(Ordering::SeqCst),
        0,
        "sleep must not call provider"
    );
    assert_eq!(
        runtime.agent_state().await.unwrap().status,
        AgentStatus::Asleep
    );
    runtime.request_service_shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), original_loop)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(runtime);

    let restarted = new_runtime();
    assert_eq!(restarted.instance_incarnation(), incarnation);
    let reloaded = restarted.latest_work_item(&item.id).await.unwrap().unwrap();
    assert_eq!(
        reloaded.blocked_by_wait_id.as_deref(),
        Some(wait_id.as_str())
    );
    assert!(reloaded.recheck_at.is_none());
    let resumed_loop = tokio::spawn(restarted.clone().run());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        provider.0.load(Ordering::SeqCst),
        0,
        "restart must preserve sleeping Work without model calls"
    );
    assert_eq!(
        restarted.agent_state().await.unwrap().status,
        AgentStatus::Asleep
    );
    let mut prompt = MessageEnvelope::new(
        "synthetic-restart",
        MessageKind::OperatorPrompt,
        MessageOrigin::Operator {
            actor_id: None,
            actor_display_name: None,
        },
        AuthorityClass::OperatorInstruction,
        Priority::Normal,
        MessageBody::Text {
            text: "synthetic follow-up evidence; return plain text only".into(),
        },
    );
    prompt.work_item_id = Some(item.id.clone());
    prompt.turn_id = Some("synthetic-exact-follow-up-turn".into());
    let accepted = restarted.enqueue(prompt).await.unwrap();
    let brief = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(brief) = restarted
                .recent_briefs(20)
                .await
                .unwrap()
                .into_iter()
                .find(|b| {
                    b.text == "synthetic canonical follow-up result"
                        && b.turn_id.as_deref() == Some("synthetic-exact-follow-up-turn")
                })
            {
                break brief;
            }
            assert!(
                !resumed_loop.is_finished(),
                "native run loop exited before result"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("native direct prompt must publish text result");
    assert_eq!(brief.agent_id, "synthetic-restart");
    assert_eq!(brief.kind, BriefKind::Result);
    assert_eq!(brief.work_item_id.as_deref(), Some(item.id.as_str()));
    assert_eq!(brief.turn_id.as_deref(), accepted.turn_id.as_deref());
    assert_eq!(
        brief.turn_id.as_deref(),
        Some("synthetic-exact-follow-up-turn")
    );
    assert!(
        brief.created_event_seq.is_some(),
        "canonical persisted brief must link publication event"
    );
    let stored = restarted.brief_by_id(&brief.id).await.unwrap().unwrap();
    assert_eq!(stored.work_item_id, brief.work_item_id);
    assert_eq!(stored.turn_id, brief.turn_id);
    let latest = restarted.latest_work_item(&item.id).await.unwrap().unwrap();
    assert!(
        latest.blocked_by.is_none(),
        "matching operator input must clear native derived blocker"
    );
    let wait = restarted
        .runtime_db()
        .wait_conditions()
        .latest_for_agent("synthetic-restart")
        .unwrap()
        .into_iter()
        .find(|w| w.id == wait_id)
        .unwrap();
    assert_eq!(wait.status, WaitConditionStatus::Resolved);
    assert!(
        restarted
            .storage()
            .read_recent_tool_executions(10)
            .unwrap()
            .is_empty(),
        "pure text follow-up must not reopen 0007 bridge or execute tools"
    );
    assert!(
        restarted
            .runtime_db()
            .timers()
            .latest_all()
            .unwrap()
            .is_empty()
    );
    assert!(provider.0.load(Ordering::SeqCst) >= 1);
    restarted.request_service_shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), resumed_loop)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
