//! Exact operator-request cancellation. The fence and Queue CAS share one writer transaction.
use super::{
    agent_message_delivery::delivery_by_id,
    conversation::{active_agent_identity, ConversationReadError},
    transitions::{QueueMutation, QueueOperation, QueueTransitionCommand},
};
use crate::{runtime::AgentMessageDeliveryService, types::*};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;

pub(crate) const SCHEMA: &str = r#"
CREATE TABLE operator_prompt_cancellations (
 agent_id TEXT NOT NULL, incarnation INTEGER NOT NULL CHECK(incarnation>0),
 principal TEXT NOT NULL, delivery_id TEXT NOT NULL, message_id TEXT,
 cancelled_at TEXT NOT NULL,
 PRIMARY KEY(agent_id, incarnation, principal, delivery_id)
);
"#;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum CancelStatus {
    Cancelled {
        #[serde(rename = "messageId", skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
    AlreadyClaimed {
        #[serde(rename = "messageId")]
        message_id: String,
    },
}
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CancelScope {
    pub agent: String,
    pub incarnation: u64,
    pub principal: String,
    pub delivery_id: String,
    idempotency_scope: String,
    idempotency_key_digest: String,
}
pub(crate) enum CancelPlan {
    Complete(CancelStatus),
    Queued(QueueEntryRecord),
}
pub(crate) fn unavailable() -> anyhow::Error {
    crate::runtime_error::RuntimeError::new(
        crate::runtime_error::RuntimeErrorDomain::Conflict,
        "prompt_cancel_unavailable",
        "request cancellation unavailable; recover using the original request ID",
    )
    .into()
}
impl CancelScope {
    pub(crate) fn new(
        agent: String,
        incarnation: u64,
        principal: String,
        key: &str,
    ) -> Result<Self> {
        // Match the control prompt admission key normalization.
        let key = key.trim();
        ensure!(
            incarnation > 0 && !key.trim().is_empty() && key.len() <= 200,
            ConversationReadError::InvalidRequest(
                "client request ID or incarnation invalid".into()
            )
        );
        let (idempotency_scope, idempotency_key_digest, delivery_id) =
            AgentMessageDeliveryService::idempotency_locator(&principal, &agent, key)?;
        Ok(Self {
            agent,
            incarnation,
            principal,
            delivery_id,
            idempotency_scope,
            idempotency_key_digest,
        })
    }
    fn validate(&self, c: &Connection) -> Result<()> {
        let identity =
            active_agent_identity(c, &self.agent)?.ok_or(ConversationReadError::IdentityChanged)?;
        ensure!(
            identity.agent_incarnation == self.incarnation,
            ConversationReadError::IdentityChanged
        );
        Ok(())
    }
    pub(crate) fn receipt(&self, c: &Connection) -> Result<Option<CancelStatus>> {
        let row: Option<Option<String>> = c.query_row(
            "SELECT message_id FROM operator_prompt_cancellations WHERE agent_id=?1 AND incarnation=?2 AND principal=?3 AND delivery_id=?4",
            params![self.agent, self.incarnation, self.principal, self.delivery_id], |r| r.get(0)).optional()?;
        Ok(row.map(|message_id| CancelStatus::Cancelled { message_id }))
    }
    fn exact(&self, c: &Connection) -> Result<Option<AgentMessageDeliveryRecord>> {
        let record = delivery_by_id(c, &self.delivery_id)?;
        if let Some(r) = &record {
            ensure!(
                r.delivery_id == self.delivery_id
                    && r.idempotency_scope == self.idempotency_scope
                    && r.idempotency_key_digest == self.idempotency_key_digest
                    && r.target_agent_id == self.agent
                    && r.expected_incarnation == Some(self.incarnation)
                    && r.caller.caller_principal == self.principal
                    && r.caller.caller_agent_id.is_none()
                    && r.caller.principal_kind == AgentMessagePrincipalKind::Operator
                    && r.caller.route == "operator_control"
                    && r.caller.delivery_surface == MessageDeliverySurface::HttpControlPrompt,
                unavailable()
            );
        }
        Ok(record)
    }
    pub(crate) fn claimed_message(&self, c: &Connection) -> Result<String> {
        self.validate(c)?;
        let record = self.exact(c)?.ok_or_else(unavailable)?;
        ensure!(
            record.outcome == AgentMessageDeliveryOutcome::Accepted
                && record.accepted_at.is_some()
                && matches!(record.state, AgentMessageDeliveryState::Dispatched | AgentMessageDeliveryState::Consumed),
            unavailable()
        );
        record.message_id.ok_or_else(unavailable)
    }

    pub(crate) fn plan_tx(&self, tx: &Transaction<'_>) -> Result<CancelPlan> {
        self.validate(tx)?;
        if let Some(status) = self.receipt(tx)? {
            return Ok(CancelPlan::Complete(status));
        }
        let Some(record) = self.exact(tx)? else {
            self.persist_tx(tx, None)?;
            return Ok(CancelPlan::Complete(CancelStatus::Cancelled {
                message_id: None,
            }));
        };
        ensure!(
            record.outcome == AgentMessageDeliveryOutcome::Accepted && record.accepted_at.is_some(),
            unavailable()
        );
        let id = record.message_id.context(unavailable())?;
        let payload: String = tx
            .query_row(
                "SELECT payload_json FROM queue_entries WHERE agent_id=?1 AND message_id=?2",
                params![self.agent, id],
                |r| r.get(0),
            )
            .optional()?
            .context(unavailable())?;
        let queue: QueueEntryRecord = serde_json::from_str(&payload)?;
        ensure!(
            queue.agent_id == self.agent && queue.message_id == id,
            unavailable()
        );
        if record.state != AgentMessageDeliveryState::Queued
            || queue.status != QueueEntryStatus::Queued
        {
            // Only actual consumption is evidence of a claim. Other terminal states fail closed.
            ensure!(
                matches!(
                    record.state,
                    AgentMessageDeliveryState::Dispatched | AgentMessageDeliveryState::Consumed
                ) || matches!(
                    queue.status,
                    QueueEntryStatus::Dequeued
                        | QueueEntryStatus::Interjected
                        | QueueEntryStatus::Processed
                ),
                unavailable()
            );
            return Ok(CancelPlan::Complete(CancelStatus::AlreadyClaimed {
                message_id: id,
            }));
        }
        Ok(CancelPlan::Queued(queue))
    }
    pub(crate) fn require_queued_tx(
        &self,
        tx: &Transaction<'_>,
        command: &QueueTransitionCommand,
    ) -> Result<()> {
        let CancelPlan::Queued(current) = self.plan_tx(tx)? else {
            return Err(unavailable());
        };
        let QueueMutation::CompareAndSet { expected, record } = &command.mutation else {
            return Err(unavailable());
        };
        ensure!(
            command.agent_id == self.agent
                && command.operation == QueueOperation::RepairDrop
                && current == *expected
                && record.message_id == current.message_id
                && record.agent_id == current.agent_id
                && record.status == QueueEntryStatus::Dropped,
            unavailable()
        );
        Ok(())
    }
    pub(crate) fn persist_tx(
        &self,
        tx: &Transaction<'_>,
        mutation: Option<&QueueMutation>,
    ) -> Result<()> {
        let id = match mutation {
            Some(QueueMutation::CompareAndSet { record, .. }) => Some(record.message_id.as_str()),
            None => None,
            _ => return Err(unavailable()),
        };
        tx.execute("INSERT INTO operator_prompt_cancellations(agent_id,incarnation,principal,delivery_id,message_id,cancelled_at) VALUES(?1,?2,?3,?4,?5,?6)",
            params![self.agent,self.incarnation,self.principal,self.delivery_id,id,chrono::Utc::now().to_rfc3339()])?;
        Ok(())
    }
}

pub(crate) fn guard_admission(tx: &Transaction<'_>, r: &AgentMessageDeliveryRecord) -> Result<()> {
    if r.caller.delivery_surface != MessageDeliverySurface::HttpControlPrompt {
        return Ok(());
    }
    // Fence the exact locator even for an unfenced late caller: it must not bypass cancellation.
    let cancelled: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM operator_prompt_cancellations WHERE agent_id=?1 AND delivery_id=?2 AND incarnation=(SELECT json_extract(payload_json,'$.incarnation') FROM agent_identities WHERE agent_id=?1))",
        params![r.target_agent_id,r.delivery_id], |row|row.get(0))?;
    ensure!(
        !cancelled,
        crate::runtime_error::RuntimeError::new(
            crate::runtime_error::RuntimeErrorDomain::Conflict,
            "prompt_request_cancelled",
            "operator request was cancelled"
        )
    );
    Ok(())
}
