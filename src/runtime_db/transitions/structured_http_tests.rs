use super::*;
use crate::types::*;

#[test]
fn issue160_native_brief_settlement_requires_explicit_marker_and_exact_identity() -> Result<()> {
    for work in [false, true] {
        for kind in [
            TurnTerminalKind::Completed,
            TurnTerminalKind::ProviderFailedNeedsRecovery,
        ] {
            for variant in [
                "exact",
                "unmarked",
                "missing",
                "wrong-work",
                "wrong-turn",
                "wrong-attempt",
                "rollback",
                "cancelled",
            ] {
                let dir = tempfile::tempdir()?;
                let db = RuntimeDb::open_and_migrate(
                    dir.path().join("runtime.sqlite"),
                    dir.path().join("runtime.lock"),
                )?;
                let now = Utc::now();
                let mut message = MessageEnvelope::new(
                    "synthetic",
                    MessageKind::OperatorPrompt,
                    MessageOrigin::Operator {
                        actor_id: None,
                        actor_display_name: None,
                    },
                    AuthorityClass::OperatorInstruction,
                    Priority::Normal,
                    MessageBody::Text {
                        text: "synthetic".into(),
                    },
                );
                message.delivery_surface = Some(MessageDeliverySurface::HttpControlPrompt);
                message.work_item_id = work.then(|| "work".into());
                db.evidence().append_message(&message)?;
                let owner = if work {
                    TurnOwner::WorkItem {
                        work_item_id: "work".into(),
                    }
                } else {
                    TurnOwner::Conversation {
                        interaction_id: "conversation".into(),
                    }
                };
                let mut original = AgentState::new("synthetic");
                original.turn_index = 3;
                original.current_turn_id = Some("turn".into());
                original.current_run_id = Some("run".into());
                original.current_turn_work_item_id = message.work_item_id.clone();
                original.current_execution_binding = Some(WorkItemExecutionBinding {
                    turn_id: "turn".into(),
                    source_message_id: message.id.clone(),
                    owner: Some(owner.clone()),
                    activation_id: Some("attempt".into()),
                    admission_provenance: None,
                    work_item_id: message.work_item_id.clone(),
                    claimed_work_revision: work.then_some(1),
                });
                let mut turn = TurnRecord::new("synthetic", "turn", 3);
                turn.owner = Some(owner);
                turn.run_id = Some("run".into());
                turn.current_work_item_id = message.work_item_id.clone();
                turn.input_message_ids = vec![message.id.clone()];
                turn.trigger = Some(TurnTriggerSummary::from_message(&message));
                turn.terminal = Some(TurnTerminalSummary {
                    kind: if variant == "cancelled" {
                        TurnTerminalKind::Aborted
                    } else {
                        kind
                    },
                    reason: None,
                    no_brief_reason: None,
                    completed_at: now,
                    duration_ms: 1,
                });
                let mut brief = BriefRecord::new(
                    "synthetic",
                    if kind == TurnTerminalKind::Completed {
                        BriefKind::Result
                    } else {
                        BriefKind::Failure
                    },
                    "synthetic result",
                    Some(message.id.clone()),
                    None,
                );
                brief.turn_id = Some(
                    if variant == "wrong-turn" {
                        "another-turn"
                    } else {
                        "turn"
                    }
                    .into(),
                );
                brief.work_item_id = if variant == "wrong-work" {
                    Some("another-work".into())
                } else {
                    message.work_item_id.clone()
                };
                turn.produced_brief_ids = vec![brief.id.clone()];
                let mut released = original.clone();
                assert!(
                    crate::runtime::structured_turn_settlement::release_exact_turn(
                        &original,
                        &mut released,
                        &turn,
                        &message.id
                    )
                );
                let queue = QueueEntryRecord {
                    message_id: message.id.clone(),
                    agent_id: "synthetic".into(),
                    priority: Priority::Normal,
                    status: QueueEntryStatus::Processed,
                    created_at: now,
                    updated_at: now,
                };
                let command = QueueTransitionCommand {
                    agent_id: "synthetic".into(),
                    operation: QueueOperation::Settle,
                    mutation: if variant == "unmarked" {
                        QueueMutation::Upsert(queue)
                    } else {
                        QueueMutation::SettleStructuredHttp {
                            record: queue,
                            attempt_id: if variant == "wrong-attempt" {
                                "another-attempt"
                            } else {
                                "attempt"
                            }
                            .into(),
                            turn_id: "turn".into(),
                        }
                    },
                    scheduler_claim_work_item: None,
                    agent_state: Some(AgentStateMutation {
                        expected: Some(Box::new(original)),
                        record: Box::new(released),
                    }),
                    message_evidence: vec![],
                    transcript_entries: vec![],
                    turn_record: Some(turn.clone()),
                    audit_events: vec![],
                    notify_scheduler: false,
                    fault: None,
                    brief_evidence: vec![],
                };
                db.turn_records().upsert(&turn)?;
                let result = db.transaction(|tx| {
                    if variant != "missing" {
                        insert_brief_evidence_tx(tx, &brief)?;
                    }
                    settle_released_http_turn_tx(tx, &command, &turn)?;
                    if variant == "rollback" {
                        anyhow::bail!("synthetic terminal transaction fault");
                    }
                    Ok(())
                });
                assert_eq!(
                    result.is_ok(),
                    matches!(variant, "exact" | "unmarked"),
                    "work={work} kind={kind:?} variant={variant}: {result:?}"
                );
                let settled: bool = db.connection()?.query_row("SELECT result_settled FROM conversation_turn_revisions WHERE agent_id='synthetic' AND turn_id='turn'", [], |row| row.get(0))?;
                assert_eq!(
                    settled,
                    variant == "exact",
                    "work={work} kind={kind:?} variant={variant}"
                );
            }
        }
    }
    Ok(())
}
