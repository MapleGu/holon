//! Exact read-only operator prompt admission recovery using the original ledger.
use super::conversation::{active_agent_identity, ConversationReadError};
use crate::{
    runtime::AgentMessageDeliveryService,
    types::{
        AgentMessageDeliveryOutcome, AgentMessageDeliveryRecord, AgentMessagePrincipalKind,
        MessageDeliverySurface,
    },
};
use anyhow::{ensure, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OperatorPromptRecoveryStatus {
    Accepted {
        #[serde(rename = "messageId")]
        message_id: String,
    },
    NotFound,
    Unknown,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OperatorPromptRecovery {
    pub agent_id: String,
    pub incarnation: u64,
    pub client_request_id: String,
    #[serde(flatten)]
    pub outcome: OperatorPromptRecoveryStatus,
}
pub(crate) fn lookup(
    c: &Connection,
    agent: &str,
    inc: u64,
    principal: &str,
    request_id: &str,
) -> Result<OperatorPromptRecovery> {
    ensure!(
        inc > 0,
        ConversationReadError::InvalidRequest("expected incarnation required".into())
    );
    let key = request_id.trim();
    ensure!(
        !key.is_empty() && key.len() <= 200,
        ConversationReadError::InvalidRequest("client request ID invalid".into())
    );
    let identity =
        active_agent_identity(c, agent)?.ok_or(ConversationReadError::IdentityChanged)?;
    ensure!(
        identity.agent_incarnation == inc,
        ConversationReadError::IdentityChanged
    );
    let (scope, digest, id) =
        AgentMessageDeliveryService::idempotency_locator(principal, agent, key)?;
    let payload:Option<String>=c.query_row("SELECT payload_json FROM agent_message_deliveries WHERE delivery_id=?1 AND target_agent_id=?2 AND idempotency_scope=?3 AND idempotency_key_digest=?4",params![id,agent,scope,digest],|r|r.get(0)).optional()?;
    let outcome = match payload {
        None => OperatorPromptRecoveryStatus::NotFound,
        Some(payload) => {
            let record: AgentMessageDeliveryRecord = serde_json::from_str(&payload)?;
            let exact = record.delivery_id == id
                && record.target_agent_id == agent
                && record.idempotency_scope == scope
                && record.idempotency_key_digest == digest
                && record.expected_incarnation == Some(inc)
                && record.caller.caller_principal == principal
                && record.caller.caller_agent_id.is_none()
                && record.caller.principal_kind == AgentMessagePrincipalKind::Operator
                && record.caller.route == "operator_control"
                && record.caller.delivery_surface == MessageDeliverySurface::HttpControlPrompt;
            if exact
                && record.outcome == AgentMessageDeliveryOutcome::Accepted
                && record.accepted_at.is_some()
            {
                match record.message_id {
                    Some(message_id) if !message_id.is_empty() => {
                        OperatorPromptRecoveryStatus::Accepted { message_id }
                    }
                    _ => OperatorPromptRecoveryStatus::Unknown,
                }
            } else {
                OperatorPromptRecoveryStatus::Unknown
            }
        }
    };
    Ok(OperatorPromptRecovery {
        agent_id: agent.into(),
        incarnation: inc,
        client_request_id: request_id.into(),
        outcome,
    })
}

#[cfg(all(test, holon_lib_test_shard = "a"))]
#[path = "operator_prompt_receipts_tests.rs"]
mod tests;
