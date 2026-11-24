use super::*;
use crate::runtime_db::{
    operator_prompt_cancel::{CancelPlan, CancelScope, CancelStatus},
    transitions::{PreparedQueueTransition, QueueMutation, QueueOperation, QueueTransitionCommand},
};

impl RuntimeHandle {
    pub(crate) async fn cancel_operator_prompt(
        &self,
        incarnation: u64,
        principal: String,
        request_id: String,
    ) -> Result<CancelStatus> {
        anyhow::ensure!(
            incarnation == self.instance_incarnation(),
            crate::runtime_db::conversation::ConversationReadError::IdentityChanged
        );
        let scope = CancelScope::new(
            self.instance_agent_id().to_owned(),
            incarnation,
            principal,
            &request_id,
        )?;
        let admission = self.execution_admission_lease()?;
        let runtime = self.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.spawn_owned_mutation(async move {
            let _admission = admission;
            let result = runtime.cancel_operator_prompt_owned(scope).await;
            let _ = tx.send(result);
        })?;
        rx.await.context("prompt cancellation worker stopped")?
    }
    async fn cancel_operator_prompt_owned(&self, scope: CancelScope) -> Result<CancelStatus> {
        let mut mutation = self.begin_mutation().await?;
        let copy = scope.clone();
        let db = self.runtime_db().clone();
        let plan =
            tokio::task::spawn_blocking(move || db.transaction(|tx| copy.plan_tx(tx))).await??;
        let expected = match plan {
            CancelPlan::Complete(status) => return Ok(status),
            CancelPlan::Queued(queue) => queue,
        };
        let id = expected.message_id.clone();
        let mut prepared = prepare_cancel(&scope, expected, self.now());
        prepared.command.fault = self.take_transition_fault();
        match mutation
            .commit_prepared_queue(crate::ids::audit_event_id(), prepared)
            .await
        {
            Ok(commit) => {
                self.apply_transition_commit(commit).await;
                Ok(CancelStatus::Cancelled {
                    message_id: Some(id),
                })
            }
            Err(error) => {
                mutation.reconcile_known_commit().await?;
                // Read the exact durable outcome after a lost ACK or a competing claim.
                // Never enqueue, and never turn a different terminal state into success.
                let db = self.runtime_db().clone();
                let plan =
                    tokio::task::spawn_blocking(move || db.transaction(|tx| scope.plan_tx(tx)))
                        .await??;
                match plan {
                    CancelPlan::Complete(status) => Ok(status),
                    CancelPlan::Queued(_) => Err(error),
                }
            }
        }
    }
}

fn prepare_cancel(
    scope: &CancelScope,
    expected: QueueEntryRecord,
    now: chrono::DateTime<chrono::Utc>,
) -> PreparedQueueTransition {
    let mut record = expected.clone();
    record.status = QueueEntryStatus::Dropped;
    record.updated_at = now;
    let id = record.message_id.clone();
    let mut prepared = PreparedQueueTransition::new(QueueTransitionCommand {
        agent_id: scope.agent.clone(),
        operation: QueueOperation::RepairDrop,
        mutation: QueueMutation::CompareAndSet { expected, record },
        scheduler_claim_work_item: None,
        agent_state: None,
        message_evidence: Vec::new(),
        transcript_entries: Vec::new(),
        turn_record: None,
        audit_events: vec![AuditEvent::legacy(
            "operator_prompt_cancelled",
            serde_json::json!({
                "message_id": id, "delivery_id": scope.delivery_id,
                "principal": scope.principal, "incarnation": scope.incarnation,
            }),
        )],
        notify_scheduler: true,
        fault: None,
        brief_evidence: Vec::new(),
    });
    prepared.prompt_cancel = Some(scope.clone());
    prepared
}

#[cfg(all(test, holon_lib_test_shard = "a"))]
#[path = "operator_prompt_cancel_tests.rs"]
mod tests;
