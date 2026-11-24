use super::*;
use crate::runtime::tests::support::{context_config, tempdir, StubProvider};

async fn fixture() -> (tempfile::TempDir, RuntimeHandle) {
    let home = tempdir().unwrap();
    let runtime = RuntimeHandle::new(
        "default",
        home.path().into(),
        InitialWorkspaceBinding::Detached,
        String::new(),
        Arc::new(StubProvider::new("unused")),
        "default".into(),
        context_config(),
    )
    .unwrap();
    let mut mutation = runtime.begin_mutation().await.unwrap();
    mutation.state.status = AgentStatus::AwakeIdle;
    mutation
        .persist_state(&runtime.inner.storage)
        .await
        .unwrap();
    drop(mutation);
    (home, runtime)
}

#[tokio::test]
async fn issue10_same_incarnation_new_run_after_preflight_is_not_stopped() {
    let (_home, runtime) = fixture().await;
    let preflight = runtime.agent_state().await.unwrap();
    assert_eq!(preflight.status, AgentStatus::AwakeIdle);
    let incarnation = runtime.instance_incarnation();
    let mut turn = runtime.begin_mutation().await.unwrap();
    let request = runtime.stop_if_idle_for_incarnation(incarnation);
    tokio::pin!(request);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut request)
            .await
            .is_err()
    );
    // A competing producer commits a run under the same mutation owner,
    // after preflight and before the guarded Stop can acquire that owner.
    turn.state.status = AgentStatus::AwakeRunning;
    turn.state.current_run_id = Some("new-run-after-preflight".into());
    turn.persist_state(&runtime.inner.storage).await.unwrap();
    drop(turn);
    let error = request.await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<crate::tool::ToolError>().unwrap().kind,
        "agent_stop_requires_idle"
    );
    let current = runtime.agent_state().await.unwrap();
    assert_eq!(current.status, AgentStatus::AwakeRunning);
    assert_eq!(
        current.current_run_id.as_deref(),
        Some("new-run-after-preflight")
    );
    assert_eq!(runtime.instance_incarnation(), incarnation);
    // Ordinary Stop remains intentionally allowed to cancel that run.
    runtime.control(ControlAction::Stop).await.unwrap();
    assert_eq!(
        runtime.agent_state().await.unwrap().status,
        AgentStatus::Stopped
    );
    runtime.join_owned_mutations().await.unwrap();
}

#[tokio::test]
async fn issue10_same_incarnation_input_after_preflight_is_not_consumed() {
    let (_home, runtime) = fixture().await;
    assert_eq!(runtime.agent_state().await.unwrap().pending, 0);
    let message = runtime
        .enqueue(MessageEnvelope::new(
            "default",
            MessageKind::OperatorPrompt,
            MessageOrigin::Operator {
                actor_id: None,
                actor_display_name: None,
            },
            AuthorityClass::OperatorInstruction,
            Priority::Normal,
            MessageBody::Text {
                text: "new work after preflight".into(),
            },
        ))
        .await
        .unwrap();
    let before = runtime.agent_state().await.unwrap();
    let error = runtime
        .stop_if_idle_for_incarnation(runtime.instance_incarnation())
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<crate::tool::ToolError>().unwrap().kind,
        "agent_stop_requires_idle"
    );
    assert_eq!(runtime.agent_state().await.unwrap(), before);
    assert_eq!(
        runtime
            .runtime_db()
            .queue_entries()
            .latest(&message.id)
            .unwrap()
            .unwrap()
            .status,
        QueueEntryStatus::Queued
    );
    runtime.join_owned_mutations().await.unwrap();
}

#[tokio::test]
async fn issue10_idle_stop_snapshot_allows_command_residue_and_rejects_unknown_posture() {
    let (_home, runtime) = fixture().await;
    let (key, _) =
        runtime.freeze_direct_control(ControlAction::Stop, AuthorityClass::OperatorInstruction);
    let mut snapshot = runtime.runtime_db().load_control_snapshot(&key).unwrap();
    for status in [
        AgentStatus::AwakeIdle,
        AgentStatus::Asleep,
        AgentStatus::Stopped,
    ] {
        snapshot.state.status = status;
        assert!(ensure_idle_stop_snapshot(&snapshot).is_ok());
    }
    for status in [
        AgentStatus::Booting,
        AgentStatus::AwakeRunning,
        AgentStatus::AwaitingTask,
    ] {
        snapshot.state.status = status;
        assert!(ensure_idle_stop_snapshot(&snapshot).is_err());
    }
    snapshot.tasks.push(TaskRecord {
        id: "residual-command".into(),
        agent_id: "default".into(),
        kind: TaskKind::CommandTask,
        status: TaskStatus::Running,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        parent_message_id: None,
        work_item_id: None,
        summary: None,
        detail: None,
        recovery: None,
    });
    snapshot.state.status = AgentStatus::AwaitingTask;
    assert!(ensure_idle_stop_snapshot(&snapshot).is_ok());
    snapshot.tasks[0].kind = TaskKind::ChildAgentTask;
    assert!(ensure_idle_stop_snapshot(&snapshot).is_err());
    snapshot.tasks.clear();
    snapshot.state.status = AgentStatus::Asleep;
    snapshot.state.sleeping_until = Some(Utc::now() + chrono::Duration::minutes(1));
    assert!(ensure_idle_stop_snapshot(&snapshot).is_err());
    snapshot.state.sleeping_until = None;
    snapshot.state.status = AgentStatus::AwakeIdle;
    snapshot.state.current_turn_id = Some("new-turn".into());
    assert!(ensure_idle_stop_snapshot(&snapshot).is_err());
    runtime.join_owned_mutations().await.unwrap();
}

#[tokio::test]
async fn issue10_guarded_stop_commits_native_cleanup_for_command_residue() {
    let (_home, runtime) = fixture().await;
    let task = TaskRecord {
        id: "residual-command-native".into(),
        agent_id: "default".into(),
        kind: TaskKind::CommandTask,
        status: TaskStatus::Running,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        parent_message_id: None,
        work_item_id: None,
        summary: None,
        detail: None,
        recovery: None,
    };
    runtime.runtime_db().tasks().upsert(&task).unwrap();
    let mut mutation = runtime.begin_mutation().await.unwrap();
    mutation.state.status = AgentStatus::AwaitingTask;
    mutation
        .persist_state(&runtime.inner.storage)
        .await
        .unwrap();
    drop(mutation);
    runtime
        .stop_if_idle_for_incarnation(runtime.instance_incarnation())
        .await
        .unwrap();
    assert_eq!(
        runtime.agent_state().await.unwrap().status,
        AgentStatus::Stopped
    );
    assert_eq!(
        runtime
            .runtime_db()
            .tasks()
            .latest(&task.id)
            .unwrap()
            .unwrap()
            .status,
        TaskStatus::Interrupted
    );
    // Missing process handles remain native unproven physical obligations;
    // an admission ACK is deliberately not a process-exit receipt.
    runtime.join_owned_mutations().await.unwrap();
}

#[tokio::test]
async fn issue10_guarded_stop_preserves_active_and_triggered_waits() {
    for status in [WaitConditionStatus::Active, WaitConditionStatus::Triggered] {
        let (_home, runtime) = fixture().await;
        let wait = WaitConditionRecord {
            id: "existing-business-wait".into(),
            agent_id: "default".into(),
            work_item_id: None,
            status,
            kind: crate::types::WaitConditionKind::External,
            source: None,
            subject_ref: None,
            waiting_for: "external evidence".into(),
            wake_sources: vec![],
            continuation: None,
            created_at: chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
            updated_at: chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
            expires_at: None,
            resolved_at: None,
            cancelled_at: None,
            turn_id: None,
            trigger_message_id: None,
            triggered_at: None,
        };
        runtime
            .runtime_db()
            .transaction(|tx| crate::runtime_db::repositories::upsert_wait_condition_tx(tx, &wait))
            .unwrap();
        let (key, _) =
            runtime.freeze_direct_control(ControlAction::Stop, AuthorityClass::OperatorInstruction);
        let before = runtime.runtime_db().load_control_snapshot(&key).unwrap();
        assert_eq!(before.waits, vec![wait.clone()]);
        let error = runtime
            .stop_if_idle_for_incarnation(runtime.instance_incarnation())
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<crate::tool::ToolError>().unwrap().kind,
            "agent_stop_requires_idle"
        );
        assert_eq!(
            runtime.runtime_db().load_control_snapshot(&key).unwrap(),
            before
        );
        runtime.join_owned_mutations().await.unwrap();
    }
}
