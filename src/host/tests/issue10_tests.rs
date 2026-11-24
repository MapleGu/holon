use super::*;

fn fixture(host: &RuntimeHost, id: &str, status: AgentStatus) -> AgentIdentityRecord {
    let identity = AgentIdentityRecord::new(
        id,
        AgentKind::Default,
        AgentVisibility::Public,
        AgentOwnership::SelfOwned,
        AgentProfilePreset::PublicNamed,
        None,
        None,
    );
    host.append_agent_identity(&identity).unwrap();
    host.runtime_db()
        .agent_identities()
        .upsert(&identity)
        .unwrap();
    let mut state = AgentState::new(id);
    state.status = status;
    host.runtime_db()
        .transaction(|tx| crate::runtime_db::evidence::upsert_agent_state_tx(tx, &state))
        .unwrap();
    identity
}

fn error_code(error: &anyhow::Error) -> String {
    error
        .downcast_ref::<crate::tool::ToolError>()
        .unwrap()
        .kind
        .clone()
}

#[tokio::test]
async fn issue10_wrong_guard_does_not_activate_or_delete() {
    let (_home, host) = test_host();
    let identity = fixture(&host, "guard-wrong", AgentStatus::Stopped);
    let before = host
        .runtime_db()
        .agent_states()
        .latest(&identity.agent_id)
        .unwrap();
    let error = host
        .stop_public_agent_if_incarnation(&identity.agent_id, identity.incarnation + 1)
        .await
        .unwrap_err();
    assert!(format!("{error}").contains("target incarnation changed"));
    assert!(host
        .try_get_loaded_runtime(&identity.agent_id)
        .await
        .is_none());
    let error = host
        .begin_public_agent_deletion_if_incarnation(
            &identity.agent_id,
            false,
            "test",
            true,
            Some(identity.incarnation + 1),
        )
        .await
        .unwrap_err();
    assert!(format!("{error}").contains("target incarnation changed"));
    assert_eq!(
        host.agent_identity_record(&identity.agent_id).unwrap(),
        Some(identity.clone())
    );
    assert_eq!(
        host.runtime_db()
            .agent_states()
            .latest(&identity.agent_id)
            .unwrap(),
        before
    );
    assert!(host
        .runtime_db()
        .agent_deletions()
        .latest_for_agent(&identity.agent_id)
        .unwrap()
        .is_none());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_delete_transaction_checks_incarnation_idle_and_pending() {
    let (_home, host) = test_host();
    let identity = fixture(&host, "guard-delete", AgentStatus::AwakeRunning);
    let begin = |expected| {
        host.runtime_db()
            .agent_deletions()
            .begin_with_archive_if_incarnation(
                &identity.agent_id,
                identity.revision,
                "test",
                false,
                None,
                Some(expected),
            )
    };
    assert_eq!(
        error_code(&begin(identity.incarnation + 1).unwrap_err()),
        "agent_incarnation_mismatch"
    );
    assert_eq!(
        error_code(&begin(identity.incarnation).unwrap_err()),
        "agent_delete_requires_idle"
    );
    let mut state = host
        .runtime_db()
        .agent_states()
        .latest(&identity.agent_id)
        .unwrap()
        .unwrap();
    state.status = AgentStatus::Stopped;
    state.current_run_id = Some("pending-run".into());
    host.runtime_db()
        .transaction(|tx| crate::runtime_db::evidence::upsert_agent_state_tx(tx, &state))
        .unwrap();
    assert_eq!(
        error_code(&begin(identity.incarnation).unwrap_err()),
        "agent_delete_blocked"
    );
    assert!(host
        .runtime_db()
        .agent_deletions()
        .latest_for_agent(&identity.agent_id)
        .unwrap()
        .is_none());
    state.current_run_id = None;
    host.runtime_db()
        .transaction(|tx| crate::runtime_db::evidence::upsert_agent_state_tx(tx, &state))
        .unwrap();
    let (deleted, job, created) = host
        .begin_public_agent_deletion_if_incarnation(
            &identity.agent_id,
            false,
            "test",
            true,
            Some(identity.incarnation),
        )
        .await
        .unwrap();
    assert!(created);
    assert_eq!(deleted.status, AgentRegistryStatus::Deleting);
    assert!(job.archive_path.is_some());
    let (_, repeated, created) = host
        .begin_public_agent_deletion_if_incarnation(
            &identity.agent_id,
            false,
            "test",
            true,
            Some(identity.incarnation),
        )
        .await
        .unwrap();
    assert!(!created);
    assert_eq!(repeated.deletion_id, job.deletion_id);
    // Drive real native cleanup: Deleted has no agent_states row.
    host.execute_deletion_job(job.clone()).await.unwrap();
    assert!(host
        .runtime_db()
        .agent_states()
        .latest(&identity.agent_id)
        .unwrap()
        .is_none());
    let (_, repeated, created) = host
        .begin_public_agent_deletion_if_incarnation(
            &identity.agent_id,
            false,
            "test",
            true,
            Some(identity.incarnation),
        )
        .await
        .unwrap();
    assert!(!created);
    assert_eq!(repeated.deletion_id, job.deletion_id);
    assert!(host
        .begin_public_agent_deletion_if_incarnation(
            &identity.agent_id,
            false,
            "test",
            true,
            Some(identity.incarnation + 1)
        )
        .await
        .is_err());
    let (_, repair, created) = host
        .begin_public_agent_deletion_with_archive(&identity.agent_id, false, "test", false)
        .await
        .unwrap();
    assert!(created);
    assert_eq!(repair.mode, AgentDeletionMode::CleanupRepair);
    assert_ne!(repair.deletion_id, job.deletion_id);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_unconditional_delete_keeps_awake_upstream_behavior() {
    let (_home, host) = test_host();
    let identity = fixture(&host, "guard-unconditional", AgentStatus::AwakeIdle);
    let (_, job, created) = host
        .begin_public_agent_deletion_with_archive(&identity.agent_id, false, "test", true)
        .await
        .unwrap();
    assert!(created);
    assert!(job.archive_path.is_some());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_conditional_delete_admits_idle_postures_and_respects_runtime_leases() {
    let (_home, host) = test_host();
    for (id, posture, load_runtime) in [
        ("guard-asleep", AgentStatus::Asleep, false),
        ("guard-awake-idle", AgentStatus::AwakeIdle, false),
        ("guard-loaded", AgentStatus::AwakeIdle, true),
    ] {
        let identity = fixture(&host, id, posture.clone());
        let runtime = if load_runtime {
            let runtime = host
                .activate_agent(id, RuntimeActivationReason::OperatorControl)
                .await
                .unwrap();
            runtime.wait_for_bootstrap().await.unwrap();
            // With no inputs, the live scheduler settles the fixture to Asleep.
            tokio::time::timeout(Duration::from_secs(5), async {
                while runtime.agent_state().await.unwrap().status != AgentStatus::Asleep {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(runtime.accepts_execution_admission());
            Some(runtime)
        } else {
            assert!(host.try_get_loaded_runtime(id).await.is_none());
            None
        };
        let state = host
            .runtime_db()
            .agent_states()
            .latest(id)
            .unwrap()
            .unwrap();
        assert_eq!(
            state.status,
            if load_runtime {
                AgentStatus::Asleep
            } else {
                posture
            }
        );
        if let Some(runtime) = &runtime {
            let lease = runtime.execution_admission_lease().unwrap();
            let error = host
                .begin_public_agent_deletion_if_incarnation(
                    id,
                    false,
                    "test",
                    true,
                    Some(identity.incarnation),
                )
                .await
                .unwrap_err();
            assert!(format!("{error}").contains("in_flight_admission"));
            assert!(runtime.accepts_execution_admission());
            assert!(host
                .runtime_db()
                .agent_deletions()
                .latest_for_agent(id)
                .unwrap()
                .is_none());
            drop(lease);
        }
        let (retired, job, created) = host
            .begin_public_agent_deletion_if_incarnation(
                id,
                false,
                "test",
                true,
                Some(identity.incarnation),
            )
            .await
            .unwrap();
        assert!(created);
        assert_eq!(retired.incarnation, identity.incarnation);
        assert_eq!(retired.status, AgentRegistryStatus::Deleting);
        assert!(host.try_get_loaded_runtime(id).await.is_none());
        if let Some(runtime) = runtime {
            assert!(!runtime.accepts_execution_admission());
        }
        host.execute_deletion_job(job.clone()).await.unwrap();
        let (retired, completed) = host.public_agent_deletion_status(id).unwrap();
        assert_eq!(retired.status, AgentRegistryStatus::Deleted);
        assert_eq!(retired.incarnation, identity.incarnation);
        let completed = completed.unwrap();
        assert_eq!(completed.deletion_id, job.deletion_id);
        assert_eq!(
            completed.status,
            crate::types::AgentDeletionStatus::Completed
        );
        assert!(completed.archive_path.unwrap().is_file());
    }
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_guarded_stop_and_delete_preserve_timers_and_unsettled_deliveries() {
    let (_home, host) = test_host();
    let id = "guard-timers";
    let identity = fixture(&host, id, AgentStatus::Asleep);
    let runtime = host
        .activate_agent(id, RuntimeActivationReason::OperatorControl)
        .await
        .unwrap();
    runtime.wait_for_bootstrap().await.unwrap();
    let stop_blocker = async || {
        let error = host
            .stop_public_agent_if_incarnation(id, identity.incarnation)
            .await
            .unwrap_err();
        let PublicAgentError::Runtime(error) = error else {
            panic!("expected the native guarded Stop rejection, found {error}");
        };
        let error = error
            .downcast_ref::<crate::tool::ToolError>()
            .unwrap_or_else(|| panic!("expected the guarded Stop tool error, found {error:#}"));
        assert_eq!(error.kind, "agent_stop_requires_idle");
        error.details.clone()
    };
    let now = chrono::Utc::now();
    let mut timer = crate::types::TimerRecord {
        id: "guard-one-shot".into(),
        agent_id: id.into(),
        created_at: now,
        duration_ms: 3_600_000,
        interval_ms: None,
        repeat: false,
        status: TimerStatus::Active,
        summary: None,
        next_fire_at: Some(now + chrono::Duration::hours(1)),
        last_fired_at: None,
        fire_count: 0,
        calendar: None,
    };
    let repo = host.runtime_db().agent_deletions();
    let begin = || {
        repo.begin_with_archive_if_incarnation(
            id,
            identity.revision,
            "test",
            false,
            None,
            Some(identity.incarnation),
        )
    };
    let assert_blocked = |reason| {
        let error = begin().unwrap_err();
        let error = error.downcast_ref::<crate::tool::ToolError>().unwrap();
        assert_eq!(error.kind, "agent_delete_blocked");
        assert_eq!(error.details, Some(serde_json::json!({"blocker": reason})));
        assert!(repo.latest_for_agent(id).unwrap().is_none());
        assert_eq!(
            host.agent_identity_record(id).unwrap(),
            Some(identity.clone())
        );
    };
    host.runtime_db()
        .transaction(|tx| crate::runtime_db::repositories::upsert_timer_tx(tx, &timer))
        .unwrap();
    assert_eq!(stop_blocker().await, None);
    assert_eq!(
        host.runtime_db()
            .timers()
            .latest(&timer.id)
            .unwrap()
            .unwrap()
            .status,
        TimerStatus::Active
    );
    assert_blocked("timer");

    timer.status = TimerStatus::Completed;
    timer.next_fire_at = None;
    timer.last_fired_at = Some(now);
    timer.fire_count = 1;
    host.runtime_db().transaction(|tx| {
        crate::runtime_db::repositories::upsert_timer_tx(tx, &timer)?;
        tx.execute(
            "INSERT INTO timer_wakes (timer_id, message_id, fire_count, status, created_at, updated_at)
             VALUES (?1, 'fixture-wake', 1, 'pending', ?2, ?2)",
            rusqlite::params![timer.id, now.to_rfc3339()],
        )?;
        Ok(())
    }).unwrap();
    assert_blocked("timer_wake");

    host.runtime_db().transaction(|tx| {
        tx.execute("UPDATE timer_wakes SET status='incorporated' WHERE timer_id=?1", [&timer.id])?;
        tx.execute(
            "INSERT INTO managed_timers (timer_id, agent_id, incarnation, schedule_id, generation, schedule_json)
             VALUES (?1, ?2, ?3, 'fixture-schedule', 1, ?4)",
            rusqlite::params![timer.id, id, identity.incarnation,
                r#"{"kind":"once","at":1700000000000,"timeZone":"UTC"}"#],
        )?;
        tx.execute(
            "INSERT INTO managed_timer_deliveries (delivery_id, timer_id, scheduled_at, fired_at, last_fired_at, fire_count, status)
             VALUES ('fixture-delivery', ?1, ?2, ?2, ?2, 1, 'pending')",
            rusqlite::params![timer.id, now.timestamp_millis()],
        )?;
        Ok(())
    }).unwrap();
    assert_eq!(
        stop_blocker().await,
        Some(serde_json::json!({"blocker": "managed_timer_delivery"}))
    );
    assert_blocked("managed_timer_delivery");

    host.runtime_db()
        .transaction(|tx| {
            let changed = tx.execute(
                "UPDATE managed_timer_deliveries SET status='acknowledged' WHERE timer_id=?1 AND status='pending'",
                [&timer.id],
            )?;
            assert_eq!(changed, 1);
            Ok(())
        })
        .unwrap();
    assert!(begin().unwrap().2);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_conditional_delete_preserves_open_work_after_stop_and_restart() {
    let (_home, host) = test_host();
    let id = "guard-stopped-open-work";
    let identity = fixture(&host, id, AgentStatus::AwakeIdle);
    let work = WorkItemRecord::new(id, "Keep this accepted work", WorkItemState::Open);
    assert!(
        host.runtime_db()
            .work_items()
            .insert_new_qualified_for_fixture(&work, identity.incarnation)
            .unwrap()
    );
    host.control_public_agent(id, ControlAction::Stop)
        .await
        .unwrap();

    async fn assert_blocked(
        host: &RuntimeHost,
        identity: &AgentIdentityRecord,
        work: &WorkItemRecord,
    ) {
        let id = &identity.agent_id;
        let state = host
            .runtime_db()
            .agent_states()
            .latest(id)
            .unwrap()
            .unwrap();
        assert_eq!(state.status, AgentStatus::Stopped);
        assert_eq!(state.sleeping_until, None);
        let error = host
            .begin_public_agent_deletion_if_incarnation(
                id,
                false,
                "test",
                false,
                Some(identity.incarnation),
            )
            .await
            .unwrap_err();
        let PublicAgentError::Runtime(error) = error else {
            panic!("expected a conditional deletion conflict: {error}");
        };
        let error = error.downcast_ref::<crate::tool::ToolError>().unwrap();
        assert_eq!(error.kind, "agent_delete_blocked");
        assert_eq!(
            error.details,
            Some(serde_json::json!({"blocker": "open_work_item"}))
        );
        assert_eq!(
            host.runtime_db()
                .work_items()
                .latest(&work.id)
                .unwrap()
                .as_ref(),
            Some(work)
        );
        assert_eq!(
            host.agent_identity_record(id).unwrap().as_ref(),
            Some(identity)
        );
        assert!(
            host.runtime_db()
                .agent_deletions()
                .latest_for_agent(id)
                .unwrap()
                .is_none()
        );
    }

    assert_blocked(&host, &identity, &work).await;
    let config = host.config().as_ref().clone();
    host.shutdown().await.unwrap();
    drop(host);
    let host = RuntimeHost::new_with_provider(config, Arc::new(StubProvider::new("done"))).unwrap();
    assert_blocked(&host, &identity, &work).await;
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_conditional_delete_ignores_only_expired_sleep_after_settlement() {
    use crate::runtime_db::transitions::{WorkItemMutation, WorkItemTransitionCommand};
    use crate::types::{
        CompletionReportRequirement, CompletionReportState, WorkItemCompletionIntent,
    };

    let (_home, host) = test_host();
    let id = "guard-expired-sleep";
    let identity = fixture(&host, id, AgentStatus::Asleep);
    let db = host.runtime_db();
    let mut work = WorkItemRecord::new(id, "Synthetic work", WorkItemState::Open);
    assert!(db.work_items().insert_new(&work).unwrap());
    let now = chrono::Utc::now();
    let mut state = db.agent_states().latest(id).unwrap().unwrap();
    state.sleeping_until = Some(now - chrono::Duration::hours(1));
    db.transaction(|tx| crate::runtime_db::evidence::upsert_agent_state_tx(tx, &state))
        .unwrap();
    let mut timer = crate::types::TimerRecord {
        id: "guard-expired-sleep-timer".into(),
        agent_id: id.into(),
        created_at: now,
        duration_ms: 3_600_000,
        interval_ms: None,
        repeat: false,
        status: TimerStatus::Active,
        summary: None,
        next_fire_at: Some(now + chrono::Duration::hours(1)),
        last_fired_at: None,
        fire_count: 0,
        calendar: None,
    };
    db.timers().upsert(&timer).unwrap();
    let begin = || {
        db.agent_deletions().begin_with_archive_if_incarnation(
            id,
            identity.revision,
            "test",
            false,
            None,
            Some(identity.incarnation),
        )
    };
    assert_eq!(error_code(&begin().unwrap_err()), "agent_delete_blocked");

    let mut brief = BriefRecord::new(id, BriefKind::Result, "Synthetic work closed", None, None);
    brief.work_item_id = Some(work.id.clone());
    let expected_revision = work.revision;
    work.state = WorkItemState::Completed;
    work.revision += 1;
    work.result_brief_id = Some(brief.id.clone());
    work.result_summary = Some(brief.text.clone());
    work.completion_intent = Some(WorkItemCompletionIntent {
        work_item_id: work.id.clone(),
        source_activation_id: None,
        source_message_id: None,
        source_turn_id: None,
        expected_work_revision: expected_revision,
        report_requirement: CompletionReportRequirement::Required,
        report_state: CompletionReportState::Bound,
        result_brief_id: Some(brief.id.clone()),
        created_at: now,
        updated_at: now,
    });
    assert!(
        db.transitions()
            .commit_work_item(&WorkItemTransitionCommand {
                agent_id: id.into(),
                mutation: WorkItemMutation::Update {
                    record: work.clone(),
                    expected_revision,
                },
                agent_state: None,
                brief_evidence: vec![brief],
                audit_events: Vec::new(),
                index_changes: Vec::new(),
                notify_scheduler: false,
                fault: None,
            })
            .unwrap()
            .applied
    );
    let err = begin().unwrap_err();
    assert_eq!(
        err.downcast_ref::<crate::tool::ToolError>()
            .unwrap()
            .details,
        Some(serde_json::json!({"blocker": "timer"}))
    );
    timer.status = TimerStatus::Cancelled;
    timer.next_fire_at = None;
    db.timers().upsert(&timer).unwrap();

    state.sleeping_until = Some(now + chrono::Duration::hours(1));
    db.transaction(|tx| crate::runtime_db::evidence::upsert_agent_state_tx(tx, &state))
        .unwrap();
    assert_eq!(error_code(&begin().unwrap_err()), "agent_delete_blocked");
    state.sleeping_until = Some(now - chrono::Duration::hours(1));
    db.transaction(|tx| crate::runtime_db::evidence::upsert_agent_state_tx(tx, &state))
        .unwrap();

    let config = host.config().as_ref().clone();
    host.shutdown().await.unwrap();
    drop(host);
    let host = RuntimeHost::new_with_provider(config, Arc::new(StubProvider::new("done"))).unwrap();
    assert_eq!(
        host.runtime_db()
            .agent_states()
            .latest(id)
            .unwrap()
            .unwrap()
            .sleeping_until,
        state.sleeping_until
    );
    assert_eq!(
        host.runtime_db().work_items().latest(&work.id).unwrap(),
        Some(work)
    );
    assert_eq!(
        host.runtime_db().timers().latest(&timer.id).unwrap(),
        Some(timer)
    );
    let (_, job, created) = host
        .begin_public_agent_deletion_if_incarnation(
            id,
            false,
            "test",
            false,
            Some(identity.incarnation),
        )
        .await
        .unwrap();
    assert!(created);
    host.execute_deletion_job(job).await.unwrap();
    let (retired, job) = host.public_agent_deletion_status(id).unwrap();
    assert_eq!(retired.status, AgentRegistryStatus::Deleted);
    assert_eq!(job.unwrap().status, AgentDeletionStatus::Completed);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_guarded_stop_uses_native_control() {
    let (_home, host) = test_host();
    let id = "guard-stop";
    let identity = fixture(&host, id, AgentStatus::AwakeIdle);
    host.stop_public_agent_if_incarnation(id, identity.incarnation)
        .await
        .unwrap();
    assert_eq!(
        host.runtime_db()
            .agent_states()
            .latest(id)
            .unwrap()
            .unwrap()
            .status,
        AgentStatus::Stopped
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_guarded_stop_rechecks_after_waiting_for_lifecycle_lock() {
    let (_home, host) = test_host();
    let mut identity = fixture(&host, "guard-reborn", AgentStatus::Stopped);
    let lock = host.agent_bootstrap_lock(&identity.agent_id);
    let guard = lock.lock().await;
    let request = host.stop_public_agent_if_incarnation(&identity.agent_id, identity.incarnation);
    tokio::pin!(request);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut request)
            .await
            .is_err()
    );
    identity.incarnation += 1;
    host.runtime_db()
        .agent_identities()
        .upsert(&identity)
        .unwrap();
    // Native reincarnation publishes the acknowledged identity cache while
    // retaining this lifecycle lock; model that publication too.
    host.cache_agent_identity(&identity).unwrap();
    drop(guard);
    assert!(tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .unwrap()
        .is_err());
    assert!(host
        .try_get_loaded_runtime(&identity.agent_id)
        .await
        .is_none());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_delete_rechecks_posture_after_native_owner_wait() {
    let (_home, host) = test_host();
    let identity = fixture(&host, "guard-posture-race", AgentStatus::Stopped);
    let reservations = host
        .reserve_agent_mutations(&[identity.agent_id.clone()])
        .await
        .unwrap();
    let request = host.begin_public_agent_deletion_if_incarnation(
        &identity.agent_id,
        false,
        "test",
        false,
        Some(identity.incarnation),
    );
    tokio::pin!(request);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut request)
            .await
            .is_err()
    );
    // An intervening operation holds the owner while publishing its busy posture.
    let mut state = host
        .runtime_db()
        .agent_states()
        .latest(&identity.agent_id)
        .unwrap()
        .unwrap();
    state.status = AgentStatus::AwakeRunning;
    host.runtime_db()
        .transaction(|tx| crate::runtime_db::evidence::upsert_agent_state_tx(tx, &state))
        .unwrap();
    drop(reservations);
    let error = request.await.unwrap_err();
    assert!(format!("{error}").contains("requires an idle or stopped state"));
    assert_eq!(
        host.agent_identity_record(&identity.agent_id).unwrap(),
        Some(identity.clone())
    );
    assert!(host
        .runtime_db()
        .agent_deletions()
        .latest_for_agent(&identity.agent_id)
        .unwrap()
        .is_none());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_retired_conditional_delete_cannot_enable_archive() {
    let (_home, host) = test_host();
    let identity = fixture(&host, "guard-archive", AgentStatus::Stopped);
    let (_, job, _) = host
        .begin_public_agent_deletion_if_incarnation(
            &identity.agent_id,
            false,
            "test",
            false,
            Some(identity.incarnation),
        )
        .await
        .unwrap();
    let error = host
        .begin_public_agent_deletion_if_incarnation(
            &identity.agent_id,
            false,
            "test",
            true,
            Some(identity.incarnation),
        )
        .await
        .unwrap_err();
    assert!(format!("{error}").contains("cannot enable archival"));
    assert_eq!(
        host.runtime_db()
            .agent_deletions()
            .latest_for_agent(&identity.agent_id)
            .unwrap(),
        Some(job)
    );
    host.shutdown().await.unwrap();
}


#[tokio::test]
async fn issue134_cancelled_operator_prompt_archives_and_purges_only_exact_agent() {
    let (_home, host) = test_host();
    let id = "cancel-delete-subject";
    let foreign = "cancel-delete-foreign";
    let identity = fixture(&host, id, AgentStatus::Asleep);
    fixture(&host, foreign, AgentStatus::Asleep);
    // The real cancellation path persists a receipt even before prompt admission.
    // This is the release/cancel case after a model fixture rejects the prompt.
    for agent in [id, foreign] {
        let runtime = host
            .activate_agent(agent, RuntimeActivationReason::OperatorControl)
            .await
            .unwrap();
        runtime.wait_for_bootstrap().await.unwrap();
        let cancelled = runtime
            .cancel_operator_prompt(
                runtime.instance_incarnation(),
                "control".into(),
                "synthetic-cancel-before-prompt".into(),
            )
            .await
            .unwrap();
        assert_eq!(
            cancelled,
            crate::runtime_db::operator_prompt_cancel::CancelStatus::Cancelled { message_id: None }
        );
        host.stop_public_agent_if_incarnation(agent, runtime.instance_incarnation())
            .await
            .unwrap();
    }
    let receipts = |agent: &str| {
        host.runtime_db()
            .with_read(|c| {
                Ok(c.query_row(
                    "SELECT COUNT(*) FROM operator_prompt_cancellations WHERE agent_id=?1",
                    [agent],
                    |r| r.get::<_, i64>(0),
                )?)
            })
            .unwrap()
    };
    assert_eq!(receipts(id), 1);
    assert_eq!(receipts(foreign), 1);
    let stale = host
        .begin_public_agent_deletion_if_incarnation(
            id,
            false,
            "synthetic",
            true,
            Some(identity.incarnation + 1),
        )
        .await
        .unwrap_err();
    assert!(format!("{stale}").contains("target incarnation changed"));
    assert_eq!(receipts(id), 1);
    assert!(host
        .runtime_db()
        .agent_deletions()
        .latest_for_agent(id)
        .unwrap()
        .is_none());
    let (_, job, created) = host
        .begin_public_agent_deletion_if_incarnation(
            id,
            false,
            "synthetic",
            true,
            Some(identity.incarnation),
        )
        .await
        .unwrap();
    assert!(created);
    host.execute_deletion_job(job.clone()).await.unwrap();
    let (deleted, completed) = host.public_agent_deletion_status(id).unwrap();
    assert_eq!(deleted.status, AgentRegistryStatus::Deleted);
    assert_eq!(deleted.incarnation, identity.incarnation);
    let completed = completed.unwrap();
    assert_eq!(completed.status, AgentDeletionStatus::Completed);
    assert_eq!(completed.deletion_id, job.deletion_id);
    let file = std::fs::File::open(completed.archive_path.unwrap()).unwrap();
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
    let mut history = None;
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        if entry.path().unwrap() == std::path::Path::new("runtime-history.json") {
            let mut bytes = String::new();
            std::io::Read::read_to_string(&mut entry, &mut bytes).unwrap();
            history = Some(serde_json::from_str::<serde_json::Value>(&bytes).unwrap());
        }
    }
    let history = history.expect("sealed archive includes private history");
    let rows = history["tables"]["operator_prompt_cancellations"]
        .as_array()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["agent_id"], id);
    assert_eq!(rows[0]["incarnation"], identity.incarnation);
    assert_eq!(rows[0]["principal"], "control");
    assert!(rows[0]["message_id"].is_null());
    assert_eq!(receipts(id), 0);
    assert_eq!(receipts(foreign), 1);
    assert_eq!(
        host.agent_identity_record(foreign).unwrap().unwrap().status,
        AgentRegistryStatus::Active
    );
    host.shutdown().await.unwrap();
}
