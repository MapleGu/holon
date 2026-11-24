//! Admission checks shared by parent cleanup and idle runtime retirement.
//!
//! These queries run in the deletion admission transaction. Producers check
//! the durable fence in the same transaction as their new work/reference.

use anyhow::{Result, bail};
use chrono::Utc;
use rusqlite::{OptionalExtension, Transaction};

use crate::domain::execution_protocol::{
    ExecutionAttempt, ExecutionAttemptState, ExecutionBinding, ExecutionOutcomeRecord,
    validate_outcome_binding,
};
use crate::types::{AgentState, AgentStatus, ExecutionAdmissionProvenance, TurnOwner};

/// Existing operator/one-shot deletion retains its cancellation semantics.
/// Parent cleanup closes new admission without cancelling accepted work.
pub(crate) fn ensure_work_admission_tx(tx: &Transaction<'_>, agent_id: &str) -> Result<()> {
    let fenced: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_deletion_jobs j
         JOIN agent_identities i ON i.agent_id = j.agent_id
         WHERE j.agent_id = ?1 AND i.status != 'active'
           AND json_extract(j.payload_json, '$.mode') = 'parent_cleanup')",
        [agent_id],
        |row| row.get(0),
    )?;
    if fenced {
        bail!("agent_cleanup_fenced: agent {agent_id} is being deleted");
    }
    Ok(())
}

/// A root cannot gain new users or change ownership while its owner is fenced.
pub(crate) fn ensure_root_admission_tx(tx: &Transaction<'_>, root_id: &str) -> Result<()> {
    let payload: Option<String> = tx
        .query_row(
            "SELECT payload_json FROM execution_root_entries WHERE execution_root_id = ?1",
            [root_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(payload) = payload {
        let root: crate::types::ExecutionRootEntry = serde_json::from_str(&payload)?;
        if let Some(worktree) = root.worktree {
            for agent_id in worktree
                .authorized_agent_ids
                .iter()
                .chain(worktree.registered_by_agent_id.iter())
            {
                ensure_work_admission_tx(tx, agent_id)?;
            }
        }
    }
    Ok(())
}

/// Durable state that proves live execution or scheduling. These block both
/// deletion admission and reminder delivery because the agent is not idle.
const EXECUTION_BLOCKER_CHECKS: &[(&str, &str)] = &[
    (
        "active_task",
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE owner_agent_id = ?1 AND status IN ('queued','running','cancelling'))",
    ),
    (
        "inbound_invocation",
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE child_agent_id = ?1 AND status IN ('queued','running','cancelling'))",
    ),
    (
        "queued_or_claimed_input",
        "SELECT EXISTS(SELECT 1 FROM queue_entries WHERE agent_id = ?1 AND status IN ('queued','dequeued','interrupted'))",
    ),
    (
        "accepted_delivery",
        "SELECT EXISTS(SELECT 1 FROM agent_message_deliveries WHERE target_agent_id = ?1 AND state IN ('queued','dispatched'))",
    ),
    (
        "wait",
        "SELECT EXISTS(SELECT 1 FROM wait_conditions WHERE agent_id = ?1 AND status IN ('active','triggered'))",
    ),
    (
        "pending_result_settlement",
        "SELECT EXISTS(SELECT 1 FROM task_result_settlements WHERE agent_id = ?1 AND state != 'settled')",
    ),
    (
        "open_execution",
        "SELECT EXISTS(SELECT 1 FROM execution_protocol_attempts WHERE agent_id = ?1 AND lifecycle_state = 'open')",
    ),
    (
        "workspace_occupancy",
        "SELECT EXISTS(SELECT 1 FROM workspace_occupancies WHERE holder_agent_id = ?1 AND released_at IS NULL)",
    ),
];

const UNFINISHED_WORK_CHECK: (&str, &str) = (
    "open_work_item",
    "SELECT EXISTS(SELECT 1 FROM work_items WHERE agent_id = ?1 AND state != 'completed')",
);

/// Durable retention state that outlives the current turn. For parent cleanup
/// and reminder delivery it is a diagnostic hint owned by the parent.
const RETENTION_BLOCKER_CHECKS: &[(&str, &str)] = &[
    UNFINISHED_WORK_CHECK,
    (
        "timer",
        "SELECT EXISTS(SELECT 1 FROM timers WHERE agent_id = ?1 AND status = 'active')",
    ),
    (
        "timer_wake",
        "SELECT EXISTS(SELECT 1 FROM timers t JOIN timer_wakes w USING(timer_id) WHERE t.agent_id = ?1 AND w.status = 'pending')",
    ),
];

/// Fail closed on unknown state. Historical rows alone are not live work.
/// `last_runtime_failure` is a diagnostic and is intentionally not an execution
/// obligation: a settled idle agent remains deletable and idle even if the last
/// turn failed and no later turn succeeded.
fn runtime_state_blocker_tx(tx: &Transaction<'_>, agent_id: &str) -> Result<Option<String>> {
    let state: Option<String> = tx
        .query_row(
            "SELECT payload_json FROM agent_states WHERE agent_id = ?1",
            [agent_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(state) = state else {
        return Ok(Some("unknown_runtime_state".into()));
    };
    let state: AgentState = serde_json::from_str(&state)?;
    Ok(runtime_state_blocker(
        &state,
        state.sleeping_until.is_some(),
    ))
}

fn runtime_state_blocker(state: &AgentState, has_scheduled_sleep: bool) -> Option<String> {
    (!matches!(
        state.status,
        AgentStatus::AwakeIdle | AgentStatus::Asleep | AgentStatus::Stopped
    ) || state.current_run_id.is_some()
        || has_scheduled_sleep
        || state.pending_wake_hint.is_some())
    .then(|| "runtime_execution_or_wake".into())
}

// Each query uses an owner/target index. Keep these predicates aligned with
// producer fencing; a caller must never make work idle by cancelling it.
fn first_blocker_tx(
    tx: &Transaction<'_>,
    agent_id: &str,
    checks: &[(&str, &str)],
) -> Result<Option<String>> {
    for (reason, sql) in checks {
        if tx.query_row(sql, [agent_id], |row| row.get::<_, bool>(0))? {
            return Ok(Some((*reason).into()));
        }
    }
    Ok(None)
}

/// Full idle safety facts. Used by idle runtime retirement, which must keep
/// considering retention state before exiting a runtime.
pub(crate) fn idle_blocker_tx(tx: &Transaction<'_>, agent_id: &str) -> Result<Option<String>> {
    if let Some(blocker) = runtime_state_blocker_tx(tx, agent_id)? {
        return Ok(Some(blocker));
    }
    if let Some(blocker) = first_blocker_tx(tx, agent_id, EXECUTION_BLOCKER_CHECKS)? {
        return Ok(Some(blocker));
    }
    first_blocker_tx(tx, agent_id, RETENTION_BLOCKER_CHECKS)
}

/// Execution-only blockers: live runtime state plus active, queued, or waiting
/// work. Retention state (open work items, timers, timer wakes) is excluded so
/// callers can treat it as a hint instead of a hard stop.
pub(crate) fn execution_blocker_tx(tx: &Transaction<'_>, agent_id: &str) -> Result<Option<String>> {
    if let Some(blocker) = runtime_state_blocker_tx(tx, agent_id)? {
        return Ok(Some(blocker));
    }
    first_blocker_tx(tx, agent_id, EXECUTION_BLOCKER_CHECKS)
}

/// Check live work before fencing the operator's conditional delete. An elapsed
/// sleep deadline can remain in the projection after its Work and timers settle.
pub(crate) fn conditional_delete_blocker_tx(
    tx: &Transaction<'_>,
    state: &AgentState,
) -> Result<Option<String>> {
    let now = Utc::now();
    let has_scheduled_sleep = state.sleeping_until.is_some_and(|deadline| deadline > now);
    if let Some(blocker) = runtime_state_blocker(state, has_scheduled_sleep) {
        return Ok(Some(blocker));
    }
    if let Some(blocker) = first_blocker_tx(tx, &state.id, EXECUTION_BLOCKER_CHECKS)? {
        return Ok(Some(blocker));
    }
    if !terminal_execution_references_tx(tx, state)? {
        return Ok(Some("unverified_execution_references".into()));
    }
    if let Some(blocker) = first_blocker_tx(tx, &state.id, &[UNFINISHED_WORK_CHECK])? {
        return Ok(Some(blocker));
    }
    reclamation_timer_blocker_tx(tx, &state.id)
}

// Older terminal turns can leave execution references behind. A conditional
// delete may archive those only when the same write transaction proves their
// ownership and terminality. Conversation result delivery is a separate fact.
fn terminal_execution_references_tx(tx: &Transaction<'_>, state: &AgentState) -> Result<bool> {
    let Some(turn_id) = state.current_turn_id.as_deref() else {
        return Ok(
            state.current_execution_binding.is_none() && state.current_turn_work_item_id.is_none()
        );
    };
    let turn_payload: Option<String> = tx
        .query_row(
            "SELECT payload_json FROM turn_records WHERE agent_id=?1 AND turn_id=?2",
            rusqlite::params![state.id, turn_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(turn_payload) = turn_payload else {
        return Ok(false);
    };
    let turn = super::repositories::decode_turn_record_payload(&turn_payload)?;
    if turn.agent_id != state.id
        || turn.turn_id != turn_id
        || turn.turn_index != state.turn_index
        || turn.current_work_item_id != state.current_turn_work_item_id
        || !turn.terminal.as_ref().is_some_and(|terminal| {
            state.last_turn_terminal.as_ref().is_some_and(|last| {
                last.turn_id == turn_id
                    && last.turn_index == turn.turn_index
                    && last.kind == terminal.kind
            })
        })
    {
        return Ok(false);
    }
    let Some(binding) = &state.current_execution_binding else {
        return Ok(true);
    };
    let Some(activation_id) = &binding.activation_id else {
        return Ok(false);
    };
    if matches!(binding.admission_provenance.as_ref(),
        Some(ExecutionAdmissionProvenance::Canonical { activation_id: admitted, .. })
            if admitted != activation_id)
        || binding.turn_id != turn_id
        || binding.owner != turn.owner
        || binding.work_item_id != turn.current_work_item_id
        || !turn.input_message_ids.contains(&binding.source_message_id)
        || turn
            .trigger
            .as_ref()
            .and_then(|trigger| trigger.message_id.as_ref())
            != Some(&binding.source_message_id)
    {
        return Ok(false);
    }
    let evidence: Option<(String, String)> = tx
        .query_row(
            "SELECT attempt.payload_json, outcome.payload_json FROM execution_protocol_attempts attempt
         JOIN execution_protocol_outcomes outcome
           ON outcome.agent_id=attempt.agent_id AND outcome.attempt_id=attempt.attempt_id
           AND outcome.outcome_id=attempt.terminal_outcome_id
         WHERE attempt.agent_id=?1 AND attempt.attempt_id=?2
           AND attempt.lifecycle_state IN ('settled','interrupted')",
            rusqlite::params![state.id, activation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((attempt_payload, outcome_payload)) = evidence else {
        return Ok(false);
    };
    let attempt: ExecutionAttempt = serde_json::from_str(&attempt_payload)?;
    let outcome: ExecutionOutcomeRecord = serde_json::from_str(&outcome_payload)?;
    let owner_matches = match (&binding.owner, &attempt.binding) {
        (
            Some(TurnOwner::Conversation { interaction_id }),
            ExecutionBinding::Conversation {
                interaction_id: admitted,
            },
        ) => interaction_id == admitted,
        (
            Some(TurnOwner::WorkItem { work_item_id }),
            ExecutionBinding::WorkItem {
                work_item_id: admitted,
            },
        ) => work_item_id == admitted,
        (
            Some(TurnOwner::AgentLifecycle { agent_id }),
            ExecutionBinding::AgentLifecycle { agent_id: admitted },
        ) => agent_id == admitted,
        (Some(TurnOwner::Command), ExecutionBinding::Command) => true,
        _ => false,
    };
    Ok(owner_matches
        && attempt.agent_id == state.id
        && attempt.attempt_id == *activation_id
        && attempt
            .turn_id
            .as_deref()
            .is_none_or(|admitted_turn| admitted_turn == turn_id)
        && attempt
            .run_id
            .as_ref()
            .is_none_or(|admitted_run| Some(admitted_run) == turn.run_id.as_ref())
        && attempt.source_message_id.as_ref() == Some(&binding.source_message_id)
        && matches!(
            attempt.state,
            ExecutionAttemptState::Settled | ExecutionAttemptState::Interrupted
        )
        && outcome.attempt_id == attempt.attempt_id
        && attempt.terminal_outcome_id.as_ref() == Some(&outcome.outcome_id)
        && validate_outcome_binding(&attempt.binding, &outcome.outcome).is_ok()
        && attempt.terminal_at.is_some())
}

/// Operator reclamation must preserve scheduled work and deliveries,
/// even after a one-shot timer is completed. Run inside the writer's transaction.
pub(crate) fn reclamation_timer_blocker_tx(
    tx: &Transaction<'_>,
    agent_id: &str,
) -> Result<Option<String>> {
    first_blocker_tx(
        tx,
        agent_id,
        &[
            (
                "timer",
                "SELECT EXISTS(SELECT 1 FROM timers WHERE agent_id = ?1 AND status NOT IN ('completed','cancelled'))",
            ),
            (
                "timer_wake",
                "SELECT EXISTS(SELECT 1 FROM timers t JOIN timer_wakes w USING(timer_id) WHERE t.agent_id = ?1 AND w.status NOT IN ('incorporated','cancelled'))",
            ),
            (
                "managed_timer_delivery",
                "SELECT EXISTS(SELECT 1 FROM managed_timers m JOIN managed_timer_deliveries d USING(timer_id) WHERE m.agent_id = ?1 AND d.status NOT IN ('acknowledged','cancelled'))",
            ),
            (
                "managed_timer_missing",
                "SELECT EXISTS(SELECT 1 FROM managed_timers m LEFT JOIN timers t USING(timer_id) WHERE m.agent_id = ?1 AND (t.timer_id IS NULL OR t.agent_id != m.agent_id))",
            ),
        ],
    )
}

/// Deletion admission for a supervised child. Execution blockers, descendant
/// responsibility, and protected artifacts block cleanup. Retention state
/// (open work items, timers) and retained command-task output do not: the parent
/// decides whether they are still needed, and deletion later terminalizes or
/// cancels them through the normal phases.
pub(crate) fn parent_cleanup_blocker_tx(
    tx: &Transaction<'_>,
    agent_id: &str,
) -> Result<Option<String>> {
    if let Some(blocker) = execution_blocker_tx(tx, agent_id)? {
        return Ok(Some(blocker));
    }
    for (reason, sql) in [
        (
            "descendant_responsibility",
            "SELECT EXISTS(SELECT 1 FROM agent_supervisions WHERE supervisor_agent_id = ?1 AND state != 'closed')",
        ),
        (
            "protected_artifact",
            "SELECT EXISTS(SELECT 1 FROM artifact_metadata WHERE agent_id = ?1)",
        ),
    ] {
        if tx.query_row(sql, [agent_id], |row| row.get::<_, bool>(0))? {
            return Ok(Some(reason.into()));
        }
    }
    Ok(None)
}

pub(crate) fn retirable_child_tx(tx: &Transaction<'_>, agent_id: &str) -> Result<bool> {
    use crate::types::*;
    let payload: Option<String> = tx
        .query_row(
            "SELECT payload_json FROM agent_identities WHERE agent_id = ?1 AND status = 'active'",
            [agent_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(payload) = payload else {
        return Ok(false);
    };
    let identity: AgentIdentityRecord = serde_json::from_str(&payload)?;
    if identity.kind != AgentKind::Child
        || identity.visibility != AgentVisibility::Private
        || identity.ownership() != AgentOwnership::ParentSupervised
    {
        return Ok(false);
    }
    let relations = super::agent_relations::canonical_relations_from_connection(tx, agent_id)?;
    Ok(relations.is_some_and(|r| {
        r.resolution == AgentCanonicalResolution::Resolved
            && r.durability
                .is_some_and(|d| d.durability == AgentCanonicalDurability::Ephemeral)
            && r.lifecycle_attachment
                .is_some_and(|a| a.attachment == AgentLifecycleAttachment::SupervisionAttached)
            && r.supervision.is_some_and(|s| {
                matches!(
                    s.state,
                    AgentSupervisionState::Active | AgentSupervisionState::CleanupRequired
                )
            })
    }))
}

pub(crate) fn idle_evidence_tx(tx: &Transaction<'_>, agent_id: &str) -> Result<String> {
    let (incarnation, state): (u64, String) = tx.query_row("SELECT COALESCE(json_extract(i.payload_json, '$.incarnation'), 1), COALESCE(s.payload_json, 'null') FROM agent_identities i LEFT JOIN agent_states s USING(agent_id) WHERE i.agent_id = ?1", [agent_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let state: Option<crate::types::AgentState> = serde_json::from_str(&state)?;
    let Some(state) = state else {
        return Ok(serde_json::to_string(&(
            incarnation,
            "unknown_runtime_state",
        ))?);
    };
    let message_sequence: Option<u64> = tx
        .query_row(
            "SELECT last_value FROM runtime_sequences WHERE domain=?1 AND scope_key=?2",
            rusqlite::params![
                super::evidence::MESSAGE_SEQUENCE_DOMAIN,
                super::evidence::agent_sequence_scope(agent_id),
            ],
            |row| row.get(0),
        )
        .optional()?;
    let latest_task: Option<String> = tx.query_row(
        "SELECT MAX(updated_at) FROM tasks WHERE owner_agent_id = ?1 OR child_agent_id = ?1",
        [agent_id],
        |row| row.get(0),
    )?;
    Ok(serde_json::to_string(&(
        incarnation,
        state.turn_index,
        message_sequence,
        state.last_turn_terminal,
        latest_task,
    ))?)
}

#[cfg(holon_lib_test_shard = "b")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_db::RuntimeDb;
    use crate::types::{
        AgentState, AgentStatus, PendingWakeHint, QueueEntryRecord, QueueEntryStatus,
        RuntimeFailurePhase, RuntimeFailureSummary, TaskKind, TaskRecord, TaskStatus,
        TurnTerminalKind, TurnTerminalRecord, WorkItemRecord, WorkItemState,
    };
    use chrono::Utc;

    fn open_db() -> (tempfile::TempDir, RuntimeDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = RuntimeDb::open_and_migrate(
            dir.path().join("runtime.sqlite"),
            dir.path().join("runtime.lock"),
        )
        .unwrap();
        (dir, db)
    }

    fn failure(summary: &str) -> RuntimeFailureSummary {
        RuntimeFailureSummary {
            occurred_at: Utc::now(),
            summary: summary.to_string(),
            phase: RuntimeFailurePhase::RuntimeTurn,
            detail_hint: Some("historical diagnostic".into()),
            failure_artifact: None,
        }
    }

    fn terminal_record(kind: TurnTerminalKind) -> TurnTerminalRecord {
        TurnTerminalRecord {
            turn_id: "turn-1".into(),
            turn_index: 1,
            kind,
            reason: Some("fixture".into()),
            last_assistant_message: None,
            no_brief_reason: None,
            checkpoint: None,
            completed_at: Utc::now(),
            duration_ms: 1,
        }
    }

    fn settled(agent_id: &str, status: AgentStatus, kind: TurnTerminalKind) -> AgentState {
        let mut state = AgentState::new(agent_id);
        state.status = status;
        state.last_turn_terminal = Some(terminal_record(kind));
        state.last_runtime_failure = Some(failure("provider_dispatch_rejected"));
        state
    }

    fn blockers(
        db: &RuntimeDb,
        agent_id: &str,
    ) -> (Option<String>, Option<String>, Option<String>) {
        db.transaction(|tx| {
            Ok((
                execution_blocker_tx(tx, agent_id)?,
                idle_blocker_tx(tx, agent_id)?,
                parent_cleanup_blocker_tx(tx, agent_id)?,
            ))
        })
        .unwrap()
    }

    #[test]
    fn idle_evidence_tracks_new_messages_without_a_state_update() -> Result<()> {
        use crate::types::{
            AgentIdentityRecord, AgentKind, AgentOwnership, AgentProfilePreset, AgentVisibility,
            AuthorityClass, MessageBody, MessageEnvelope, MessageKind, MessageOrigin, Priority,
        };

        let (_dir, db) = open_db();
        let agent_id = "message-activity";
        let state = settled(
            agent_id,
            AgentStatus::AwakeIdle,
            TurnTerminalKind::Completed,
        );
        db.transaction(|tx| {
            super::super::evidence::upsert_agent_identity_tx(
                tx,
                &AgentIdentityRecord::new(
                    agent_id,
                    AgentKind::Named,
                    AgentVisibility::Public,
                    AgentOwnership::SelfOwned,
                    AgentProfilePreset::PublicNamed,
                    None,
                    None,
                ),
            )?;
            super::super::evidence::upsert_agent_state_tx(tx, &state)
        })?;
        let evidence = || db.transaction(|tx| idle_evidence_tx(tx, agent_id));
        let before = evidence()?;
        let message = MessageEnvelope::new(
            agent_id,
            MessageKind::InternalFollowup,
            MessageOrigin::System {
                subsystem: "fixture".into(),
            },
            AuthorityClass::RuntimeInstruction,
            Priority::Normal,
            MessageBody::Text {
                text: "new activity".into(),
            },
        );
        let stored = db.messages().append_with_index_changes(&message, &[])?;
        assert_eq!(stored.message_seq, Some(1));
        let after = evidence()?;
        assert_ne!(before, after);
        assert_eq!(db.agent_states().latest(agent_id)?, Some(state));

        let replay = db.messages().append_with_index_changes(&message, &[])?;
        assert_eq!(replay.message_seq, stored.message_seq);
        assert_eq!(evidence()?, after);
        Ok(())
    }

    #[test]
    fn conditional_delete_requires_exact_terminal_execution_references() -> Result<()> {
        use crate::types::{
            MessageEnvelope, TurnRecord, TurnTriggerSummary, WorkItemExecutionBinding,
        };
        let (_dir, db) = open_db();
        let agent = "synthetic-historical";
        let mut state = settled(agent, AgentStatus::Asleep, TurnTerminalKind::Completed);
        state.turn_index = 1;
        state.current_turn_id = Some("turn-1".into());
        let owner = TurnOwner::Conversation {
            interaction_id: "synthetic-conversation".into(),
        };
        let source = MessageEnvelope::new(
            agent,
            crate::types::MessageKind::InternalFollowup,
            crate::types::MessageOrigin::System {
                subsystem: "synthetic".into(),
            },
            crate::types::AuthorityClass::RuntimeInstruction,
            crate::types::Priority::Next,
            crate::types::MessageBody::Text {
                text: "synthetic".into(),
            },
        );
        state.current_execution_binding = Some(WorkItemExecutionBinding {
            activation_id: Some("attempt".into()),
            admission_provenance: None,
            source_message_id: source.id.clone(),
            turn_id: "turn-1".into(),
            owner: Some(owner.clone()),
            work_item_id: None,
            claimed_work_revision: None,
        });
        let check =
            |state: &AgentState| db.transaction(|tx| conditional_delete_blocker_tx(tx, state));
        let blocked = Some("unverified_execution_references".into());
        assert_eq!(check(&state)?, blocked);
        let mut turn = TurnRecord::new(agent, "turn-1", 1);
        turn.owner = Some(owner);
        turn.trigger = Some(TurnTriggerSummary::from_message(&source));
        turn.input_message_ids.push(source.id.clone());
        db.messages().append_with_index_changes(&source, &[])?;
        db.turn_records().upsert(&turn)?;
        assert_eq!(check(&state)?, blocked);
        turn.terminal = Some(crate::types::TurnTerminalSummary::from_terminal(
            &terminal_record(TurnTerminalKind::Completed),
        ));
        db.turn_records().upsert(&turn)?;
        assert_eq!(check(&state)?, blocked);
        let payload = serde_json::json!({
            "agent_id": agent, "attempt_id":"attempt", "source_message_id":source.id,
            "source":{"identity":{"kind":"internal_followup","message_id":source.id},"generation":1},
            "binding":turn.owner,"provenance":{"origin":"runtime_recovery","trust":"runtime_instruction","priority":"next"},
            "admitted_fences":{"source_revision":1,"agent_control_revision":1,"host_registry_revision":1},
            "state":"settled","recovery_of_attempt_id":"prior","terminal_outcome_id":"outcome",
            "admitted_at":"2040-01-01T00:00:00Z","terminal_at":"2040-01-01T00:00:01Z"
        });
        let outcome = serde_json::json!({
            "outcome_id":"outcome","attempt_id":"attempt",
            "outcome":{"owner":"conversation","outcome":{"kind":"replied"}},
            "created_at":"2040-01-01T00:00:01Z"
        });
        db.transaction(|tx| {
            tx.execute("INSERT INTO execution_protocol_attempts VALUES(?1,'attempt','settled','{}',1,'prior','outcome',?2)",
                rusqlite::params![agent, payload.to_string()])?;
            tx.execute("INSERT INTO execution_protocol_outcomes VALUES(?1,'outcome','attempt',?2)",
                rusqlite::params![agent, outcome.to_string()])?;
            Ok(())
        })?;
        assert_eq!(check(&state)?, None);
        let mut conflicting = state.clone();
        let binding = conflicting.current_execution_binding.as_mut().unwrap();
        binding.source_message_id = "other".into();
        assert_eq!(check(&conflicting)?, blocked);
        let binding = conflicting.current_execution_binding.as_mut().unwrap();
        binding.source_message_id = source.id.clone();
        binding.admission_provenance = Some(ExecutionAdmissionProvenance::Canonical {
            scenario_class: crate::domain::scheduler::SchedulerScenarioClass::OperatorInterjection,
            activation_id: "another-attempt".into(),
        });
        assert_eq!(check(&conflicting)?, blocked);

        use crate::types::{
            AgentIdentityRecord, AgentKind, AgentOwnership, AgentProfilePreset,
            AgentRegistryStatus, AgentVisibility,
        };
        let identity = AgentIdentityRecord::new(
            agent,
            AgentKind::Named,
            AgentVisibility::Public,
            AgentOwnership::SelfOwned,
            AgentProfilePreset::PublicNamed,
            None,
            None,
        );
        db.agent_identities().upsert(&identity)?;
        let store = |state: &AgentState| {
            db.transaction(|tx| super::super::evidence::upsert_agent_state_tx(tx, state))
        };
        let begin = |incarnation| {
            db.agent_deletions().begin_with_archive_if_incarnation(
                agent,
                identity.revision,
                "synthetic",
                false,
                Some(_dir.path()),
                Some(incarnation),
            )
        };
        store(&conflicting)?;
        assert!(begin(identity.incarnation).is_err());
        store(&state)?;
        assert!(begin(identity.incarnation + 1).is_err());
        let (retired, job, created) = begin(identity.incarnation)?;
        assert!(created);
        assert_eq!(retired.incarnation, identity.incarnation);
        assert_eq!(retired.status, AgentRegistryStatus::Deleting);
        assert!(job.archive_path.is_some());
        Ok(())
    }

    #[test]
    fn historical_failure_is_not_an_execution_obligation() {
        let (_dir, db) = open_db();
        assert_eq!(
            blockers(&db, "missing").0.as_deref(),
            Some("unknown_runtime_state")
        );

        for (agent_id, status, kind) in [
            (
                "failed-then-ok",
                AgentStatus::AwakeIdle,
                TurnTerminalKind::Completed,
            ),
            (
                "last-failed-idle",
                AgentStatus::AwakeIdle,
                TurnTerminalKind::Aborted,
            ),
            (
                "last-failed-asleep",
                AgentStatus::Asleep,
                TurnTerminalKind::ProviderFailedNeedsRecovery,
            ),
            (
                "last-failed-stopped",
                AgentStatus::Stopped,
                TurnTerminalKind::Interrupted,
            ),
        ] {
            let state = settled(agent_id, status, kind);
            let diagnostic = state.last_runtime_failure.clone();
            db.agent_states().upsert_for_fixture(&state).unwrap();
            assert_eq!(blockers(&db, agent_id), (None, None, None), "{agent_id}");
            assert_eq!(
                db.agent_states()
                    .latest(agent_id)
                    .unwrap()
                    .unwrap()
                    .last_runtime_failure,
                diagnostic,
                "diagnostic must survive admission"
            );
        }

        let mut running = settled(
            "running",
            AgentStatus::AwakeRunning,
            TurnTerminalKind::Aborted,
        );
        db.agent_states().upsert_for_fixture(&running).unwrap();
        assert_eq!(
            blockers(&db, "running").0.as_deref(),
            Some("runtime_execution_or_wake")
        );

        running.id = "run-id".into();
        running.status = AgentStatus::AwakeIdle;
        running.current_run_id = Some("run-1".into());
        db.agent_states().upsert_for_fixture(&running).unwrap();
        assert_eq!(
            blockers(&db, "run-id").0.as_deref(),
            Some("runtime_execution_or_wake")
        );

        let mut sleeping = settled("sleeping", AgentStatus::Stopped, TurnTerminalKind::Aborted);
        sleeping.sleeping_until = Some(Utc::now());
        db.agent_states().upsert_for_fixture(&sleeping).unwrap();
        assert_eq!(
            blockers(&db, "sleeping").0.as_deref(),
            Some("runtime_execution_or_wake")
        );

        let mut hinted = settled("hinted", AgentStatus::AwakeIdle, TurnTerminalKind::Aborted);
        hinted.pending_wake_hint = Some(PendingWakeHint {
            reason: "external".into(),
            description: None,
            source: None,
            scope: None,
            external_trigger_id: None,
            resource: None,
            body: None,
            content_type: None,
            correlation_id: None,
            causation_id: None,
            created_at: Utc::now(),
        });
        db.agent_states().upsert_for_fixture(&hinted).unwrap();
        assert_eq!(
            blockers(&db, "hinted").0.as_deref(),
            Some("runtime_execution_or_wake")
        );

        let retained = settled(
            "retained",
            AgentStatus::AwakeIdle,
            TurnTerminalKind::Aborted,
        );
        db.agent_states().upsert_for_fixture(&retained).unwrap();
        db.work_items()
            .insert_new(&WorkItemRecord::new(
                "retained",
                "still open",
                WorkItemState::Open,
            ))
            .unwrap();
        let (execution, idle, cleanup) = blockers(&db, "retained");
        assert_eq!(execution, None);
        assert_eq!(idle.as_deref(), Some("open_work_item"));
        assert_eq!(cleanup, None);

        let live = settled(
            "live-task",
            AgentStatus::AwakeIdle,
            TurnTerminalKind::Aborted,
        );
        db.agent_states().upsert_for_fixture(&live).unwrap();
        let now = Utc::now();
        let task = TaskRecord {
            id: "live-task-1".into(),
            agent_id: "live-task".into(),
            kind: TaskKind::ChildAgentTask,
            status: TaskStatus::Running,
            created_at: now,
            updated_at: now,
            parent_message_id: None,
            work_item_id: None,
            summary: None,
            detail: None,
            recovery: None,
        };
        db.tasks().upsert(&task).unwrap();
        assert_eq!(blockers(&db, "live-task").0.as_deref(), Some("active_task"));
        assert_eq!(
            db.tasks().latest(&task.id).unwrap().unwrap().status,
            TaskStatus::Running
        );

        for (agent_id, status) in [
            ("queued", QueueEntryStatus::Queued),
            ("claimed", QueueEntryStatus::Dequeued),
            ("interrupted", QueueEntryStatus::Interrupted),
        ] {
            let agent = settled(agent_id, AgentStatus::Asleep, TurnTerminalKind::Aborted);
            db.agent_states().upsert_for_fixture(&agent).unwrap();
            let entry = QueueEntryRecord {
                message_id: format!("{agent_id}-1"),
                agent_id: agent_id.into(),
                priority: crate::types::Priority::Normal,
                status,
                created_at: now,
                updated_at: now,
            };
            db.queue_entries().upsert(&entry).unwrap();
            let (execution, idle, cleanup) = blockers(&db, agent_id);
            for blocker in [execution, idle, cleanup] {
                assert_eq!(blocker.as_deref(), Some("queued_or_claimed_input"));
            }
            assert_eq!(
                db.queue_entries().latest(&entry.message_id).unwrap(),
                Some(entry)
            );
        }
    }
}
