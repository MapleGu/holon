use super::*;
use crate::runtime_db::operator_prompt_cancel::CancelStatus;
use crate::runtime_db::operator_prompt_receipts::{
    lookup, OperatorPromptRecovery, OperatorPromptRecoveryStatus,
};
use crate::{
    runtime::tests::support::{context_config, tempdir, StubProvider},
    runtime::{AgentMessageDeliveryService, RuntimeHandle},
    types::*,
};
use std::sync::Arc;

async fn runtime(home: &std::path::Path) -> Result<RuntimeHandle> {
    let r = RuntimeHandle::new(
        "default",
        home.into(),
        None::<WorkspaceEntry>,
        "http://synthetic.invalid".into(),
        Arc::new(StubProvider::new("must not execute")),
        "default".into(),
        context_config(),
    )?;
    Ok(r)
}
async fn send(r: &RuntimeHandle, key: &str, fenced: bool) -> Result<String> {
    let mut p = AgentMessageDeliveryService::prepare(
        AgentMessageSendRequest {
            target_agent_id: "default".into(),
            content: MessageBody::Text {
                text: "synthetic environment-bearing prepared text; receipt never reconstructs it"
                    .into(),
            },
            client_idempotency_key: key.into(),
            correlation_id: None,
            causation_id: None,
            requested_priority: Some(Priority::Interject),
            forward: false,
            reply_context_id: None,
        },
        AgentMessageCallerContext {
            caller_principal: "control".into(),
            caller_agent_id: None,
            principal_kind: AgentMessagePrincipalKind::Operator,
            route: "operator_control".into(),
            origin: MessageOrigin::Operator {
                actor_id: Some("control".into()),
                actor_display_name: None,
            },
            authority_class: AuthorityClass::OperatorInstruction,
            delivery_surface: MessageDeliverySurface::HttpControlPrompt,
            admission_context: AdmissionContext::RuntimeOwned,
            current_turn_id: None,
            current_task_id: None,
            current_work_item_id: None,
            inherited_routing_context: None,
            routing_activation_id: None,
        },
    )?;
    p.message.kind = MessageKind::OperatorPrompt;
    if fenced {
        p.record.expected_incarnation = Some(r.instance_incarnation());
    }
    Ok(r.agent_message_delivery_service()
        .deliver(&p)
        .await?
        .message_id
        .expect("accepted synthetic message"))
}
fn read(r: &RuntimeHandle, inc: u64, principal: &str, key: &str) -> Result<OperatorPromptRecovery> {
    r.runtime_db().with_read(|c| {
        let tx = c.transaction()?;
        lookup(&tx, "default", inc, principal, key)
    })
}
fn queued(r: &RuntimeHandle) -> Result<u64> {
    r.runtime_db()
        .with_read(|c| Ok(c.query_row("SELECT COUNT(*) FROM queue_entries", [], |row| row.get(0))?))
}

async fn cancel(r: &RuntimeHandle, key: &str) -> Result<CancelStatus> {
    r.cancel_operator_prompt(r.instance_incarnation(), "control".into(), key.into())
        .await
}
fn claim_command(
    r: &RuntimeHandle,
    expected: QueueEntryRecord,
    interject: bool,
) -> QueueTransitionCommand {
    let mut record = expected;
    record.status = if interject {
        QueueEntryStatus::Interjected
    } else {
        QueueEntryStatus::Dequeued
    };
    record.updated_at = r.now();
    QueueTransitionCommand {
        agent_id: "default".into(),
        operation: if interject {
            QueueOperation::Interject
        } else {
            QueueOperation::Claim
        },
        mutation: QueueMutation::Consume(record),
        scheduler_claim_work_item: None,
        agent_state: None,
        message_evidence: Vec::new(),
        transcript_entries: Vec::new(),
        turn_record: None,
        audit_events: Vec::new(),
        notify_scheduler: false,
        fault: None,
        brief_evidence: Vec::new(),
    }
}
async fn claim(r: &RuntimeHandle, id: &str, interject: bool) -> Result<bool> {
    let mut owner = r.begin_mutation().await?;
    let expected = r.runtime_db().queue_entries().latest(id)?.unwrap();
    let command = claim_command(r, expected, interject);
    let commit = owner
        .commit_prepared_queue(
            crate::ids::audit_event_id(),
            PreparedQueueTransition::new(command),
        )
        .await?;
    let applied = commit.applied;
    r.apply_transition_commit(commit).await;
    Ok(applied)
}

#[tokio::test]
async fn prompt_cancel_pre_admission_reopen_late_enqueue_and_exact_key() -> Result<()> {
    let home = tempdir()?;
    let r = runtime(home.path()).await?;
    let inc = r.instance_incarnation();
    assert_eq!(
        cancel(&r, "late-key").await?,
        CancelStatus::Cancelled { message_id: None }
    );
    assert_eq!(queued(&r)?, 0);
    drop(r);
    let r = runtime(home.path()).await?;
    assert_eq!(
        read(&r, inc, "control", "late-key")?.outcome,
        OperatorPromptRecoveryStatus::Cancelled { message_id: None }
    );
    assert!(send(&r, "late-key", true).await.is_err());
    assert!(send(&r, "late-key", false).await.is_err());
    assert_eq!(queued(&r)?, 0);
    assert_eq!(
        cancel(&r, "late-key").await?,
        CancelStatus::Cancelled { message_id: None }
    );
    // Principal and exact bytes remain separate scopes.
    assert_eq!(
        read(&r, inc, "someone-else", "late-key")?.outcome,
        OperatorPromptRecoveryStatus::NotFound
    );
    let id = send(&r, "different-key", true).await?;
    assert!(!id.is_empty());
    assert!(r
        .cancel_operator_prompt(inc + 1, "control".into(), "other".into())
        .await
        .is_err());
    assert_eq!(
        read(&r, inc, "control", "other")?.outcome,
        OperatorPromptRecoveryStatus::NotFound
    );
    Ok(())
}
#[tokio::test]
async fn prompt_cancel_wins_real_claim_and_interject_preserves_source_and_projections() -> Result<()>
{
    for interject in [false, true] {
        let home = tempdir()?;
        let r = runtime(home.path()).await?;
        let id = send(&r, "target", true).await?;
        let other = send(&r, "unrelated", true).await?;
        let expected = CancelStatus::Cancelled {
            message_id: Some(id.clone()),
        };
        assert_eq!(cancel(&r, "target").await?, expected);
        assert!(!claim(&r, &id, interject).await?);
        assert_eq!(cancel(&r, "target").await?, expected);
        assert_eq!(
            r.runtime_db().queue_entries().latest(&id)?.unwrap().status,
            QueueEntryStatus::Dropped
        );
        assert_eq!(
            r.runtime_db()
                .queue_entries()
                .latest(&other)?
                .unwrap()
                .status,
            QueueEntryStatus::Queued
        );
        assert_eq!(r.inner.agent.lock().await.queue.len(), 1);
        let root = r.inner.mutation_owner.capture()?.unwrap();
        assert_eq!(root.queue_posture.as_ref().unwrap().pending, 1);
        assert!(root.current.is_some());
        assert!(r
            .runtime_db()
            .messages()
            .by_id(Some("default"), &id)?
            .is_some());
        let inc = r.instance_incarnation();
        assert_eq!(
            read(&r, inc, "control", "target")?.outcome,
            OperatorPromptRecoveryStatus::Cancelled {
                message_id: Some(id.clone())
            }
        );
        assert!(send(&r, "target", true).await.is_err());
        drop(root);
        drop(r);
        let r = runtime(home.path()).await?;
        assert_eq!(cancel(&r, "target").await?, expected);
        assert!(!claim(&r, &id, interject).await?);
    }
    Ok(())
}
#[tokio::test]
async fn prompt_cancel_loses_real_claim_and_interject_and_reopen() -> Result<()> {
    for interject in [false, true] {
        let home = tempdir()?;
        let r = runtime(home.path()).await?;
        let id = send(&r, "claimed", true).await?;
        assert!(claim(&r, &id, interject).await?);
        let expected = CancelStatus::AlreadyClaimed {
            message_id: id.clone(),
        };
        assert_eq!(cancel(&r, "claimed").await?, expected);
        assert_eq!(
            read(&r, r.instance_incarnation(), "control", "claimed")?.outcome,
            OperatorPromptRecoveryStatus::Accepted {
                message_id: id.clone()
            }
        );
        drop(r);
        let r = runtime(home.path()).await?;
        assert_eq!(cancel(&r, "claimed").await?, expected);
    }
    Ok(())
}
#[tokio::test]
async fn prompt_cancel_races_actual_claim_without_double_winner() -> Result<()> {
    for interject in [false, true] {
        for _ in 0..4 {
            let home = tempdir()?;
            let r = runtime(home.path()).await?;
            let id = send(&r, "race", true).await?;
            let (cancelled, claimed) = tokio::join!(cancel(&r, "race"), claim(&r, &id, interject));
            match cancelled? {
                CancelStatus::Cancelled { .. } => assert!(!claimed?),
                CancelStatus::AlreadyClaimed { message_id } => {
                    assert!(claimed?);
                    assert_eq!(message_id, id);
                }
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn prompt_cancel_races_admission_and_cannot_revive_after_reopen() -> Result<()> {
    for _ in 0..4 {
        let home = tempdir()?;
        let r = runtime(home.path()).await?;
        let (sent, cancelled) =
            tokio::join!(send(&r, "enqueue-race", true), cancel(&r, "enqueue-race"));
        assert!(matches!(cancelled?, CancelStatus::Cancelled { .. }));
        if let Ok(id) = sent {
            assert_eq!(
                r.runtime_db().queue_entries().latest(&id)?.unwrap().status,
                QueueEntryStatus::Dropped
            );
        }
        assert_eq!(r.inner.agent.lock().await.queue.len(), 0);
        drop(r);
        let r = runtime(home.path()).await?;
        assert!(send(&r, "enqueue-race", true).await.is_err());
        assert_eq!(r.inner.agent.lock().await.queue.len(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn prompt_cancel_unavailable_legacy_and_different_principal_isolation() -> Result<()> {
    let home = tempdir()?;
    let r = runtime(home.path()).await?;
    let legacy = send(&r, "legacy", false).await?;
    assert!(cancel(&r, "legacy").await.is_err());
    assert_eq!(
        r.runtime_db()
            .queue_entries()
            .latest(&legacy)?
            .unwrap()
            .status,
        QueueEntryStatus::Queued
    );
    let id = send(&r, "same-key", true).await?;
    let other = r
        .cancel_operator_prompt(
            r.instance_incarnation(),
            "different-principal".into(),
            "same-key".into(),
        )
        .await?;
    assert_eq!(other, CancelStatus::Cancelled { message_id: None });
    assert_eq!(
        read(&r, r.instance_incarnation(), "control", "same-key")?.outcome,
        OperatorPromptRecoveryStatus::Accepted {
            message_id: id.clone()
        }
    );
    assert!(claim(&r, &id, false).await?);
    assert_eq!(
        cancel(&r, "same-key").await?,
        CancelStatus::AlreadyClaimed { message_id: id }
    );
    Ok(())
}

#[tokio::test]
async fn prompt_cancel_stale_prepared_plan_loses_to_real_claim_transaction() -> Result<()> {
    for interject in [false, true] {
        let home = tempdir()?;
        let r = runtime(home.path()).await?;
        let id = send(&r, "stale-plan", true).await?;
        let scope = CancelScope::new(
            "default".into(),
            r.instance_incarnation(),
            "control".into(),
            "stale-plan",
        )?;
        let plan = r.runtime_db().transaction(|tx| scope.plan_tx(tx))?;
        let CancelPlan::Queued(expected) = plan else {
            panic!("must have a queued source");
        };
        let frozen = prepare_cancel(&scope, expected.clone(), r.now());
        let mut owner = r.begin_mutation().await?;
        // Bypass the Runtime owner deliberately: a separate real SQLite Queue writer
        // wins after cancel observed Queued, before its final prepared transaction.
        let claim = r
            .runtime_db()
            .transitions()
            .commit_queue(&claim_command(&r, expected, interject))?;
        assert!(claim.applied);
        assert_eq!(
            r.runtime_db().queue_entries().latest(&id)?.unwrap().status,
            if interject {
                QueueEntryStatus::Interjected
            } else {
                QueueEntryStatus::Dequeued
            }
        );
        assert!(owner
            .commit_prepared_queue(crate::ids::audit_event_id(), frozen)
            .await
            .is_err());
        owner.reconcile_known_commit().await?;
        drop(owner);
        assert!(r.runtime_db().with_read(|c| scope.receipt(c))?.is_none());
        assert_eq!(
            cancel(&r, "stale-plan").await?,
            CancelStatus::AlreadyClaimed { message_id: id }
        );
    }
    Ok(())
}
#[tokio::test]
async fn prompt_cancel_rollback_keeps_queue_and_fence_atomic() -> Result<()> {
    let home = tempdir()?;
    let r = runtime(home.path()).await?;
    let id = send(&r, "rollback", true).await?;
    let scope = CancelScope::new(
        "default".into(),
        r.instance_incarnation(),
        "control".into(),
        "rollback",
    )?;
    let CancelPlan::Queued(expected) = r.runtime_db().transaction(|tx| scope.plan_tx(tx))? else {
        panic!("queued");
    };
    let mut frozen = prepare_cancel(&scope, expected, r.now());
    frozen.command.fault =
        Some(crate::runtime_db::transitions::TransitionFaultPoint::AfterAuditWrites);
    let mut owner = r.begin_mutation().await?;
    assert!(owner
        .commit_prepared_queue(crate::ids::audit_event_id(), frozen)
        .await
        .is_err());
    owner.reconcile_known_commit().await?;
    drop(owner);
    assert!(r.runtime_db().with_read(|c| scope.receipt(c))?.is_none());
    assert_eq!(
        r.runtime_db().queue_entries().latest(&id)?.unwrap().status,
        QueueEntryStatus::Queued
    );
    assert_eq!(
        cancel(&r, "rollback").await?,
        CancelStatus::Cancelled {
            message_id: Some(id)
        }
    );
    Ok(())
}

#[tokio::test]
async fn prompt_cancel_post_commit_ack_loss_recovers_exact_receipt_resident_and_reopen(
) -> Result<()> {
    let home = tempdir()?;
    let r = runtime(home.path()).await?;
    let inc = r.instance_incarnation();
    let id = send(&r, "lost-cancel-ack", true).await?;
    let other = send(&r, "unrelated-after-ack", true).await?;
    r.inject_next_transition_fault(
        crate::runtime_db::transitions::TransitionFaultPoint::BeforeCacheUpdate,
    );
    let expected = CancelStatus::Cancelled {
        message_id: Some(id.clone()),
    };
    assert_eq!(cancel(&r, "lost-cancel-ack").await?, expected);
    assert_eq!(
        read(&r, inc, "control", "lost-cancel-ack")?.outcome,
        OperatorPromptRecoveryStatus::Cancelled {
            message_id: Some(id.clone())
        }
    );
    let scope = CancelScope::new("default".into(), inc, "control".into(), "lost-cancel-ack")?;
    assert_eq!(
        r.runtime_db().with_read(|c| scope.receipt(c))?,
        Some(expected.clone())
    );
    assert_eq!(
        read(&r, inc, "other-principal", "lost-cancel-ack")?.outcome,
        OperatorPromptRecoveryStatus::NotFound
    );
    assert_eq!(
        r.runtime_db().queue_entries().latest(&id)?.unwrap().status,
        QueueEntryStatus::Dropped
    );
    assert_eq!(
        r.runtime_db()
            .queue_entries()
            .latest(&other)?
            .unwrap()
            .status,
        QueueEntryStatus::Queued
    );
    assert!(r
        .runtime_db()
        .messages()
        .by_id(Some("default"), &id)?
        .is_some());
    assert_eq!(r.inner.agent.lock().await.queue.len(), 1);
    let root = r.inner.mutation_owner.capture()?.unwrap();
    assert_eq!(root.queue_posture.as_ref().unwrap().pending, 1);
    assert!(root.current.is_some());
    drop(root);
    assert!(!claim(&r, &id, false).await?);
    assert!(!claim(&r, &id, true).await?);
    assert_eq!(cancel(&r, "lost-cancel-ack").await?, expected);
    let audit_count = |r: &RuntimeHandle| {
        r.runtime_db().with_read(|c| Ok(c.query_row(
        "SELECT COUNT(*) FROM audit_events WHERE kind='operator_prompt_cancelled' AND json_extract(data_json,'$.data.message_id')=?1",
        [&id], |row| row.get::<_,u64>(0))?))
    };
    assert_eq!(audit_count(&r)?, 1);
    drop(r);
    let r = runtime(home.path()).await?;
    assert_eq!(
        read(&r, inc, "control", "lost-cancel-ack")?.outcome,
        OperatorPromptRecoveryStatus::Cancelled {
            message_id: Some(id.clone())
        }
    );
    assert_eq!(cancel(&r, "lost-cancel-ack").await?, expected);
    assert!(send(&r, "lost-cancel-ack", true).await.is_err());
    assert_eq!(r.inner.agent.lock().await.queue.len(), 1);
    assert_eq!(audit_count(&r)?, 1);
    Ok(())
}
