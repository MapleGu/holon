//! Abort one active standalone structured prompt, never claim accepted output.
use super::*;
use crate::runtime_db::operator_prompt_cancel::{unavailable, CancelScope, CancelStatus};
use anyhow::ensure;

pub(crate) const CLAIMED_PROMPT_ABORT: &str = "structured_prompt_cancelled";
const OUTPUT_ACCEPTANCE_PENDING: &str = "structured_output_acceptance_pending:";
pub(crate) struct ClaimedOutputFence {
    run_id: String,
    marker: String,
}

impl RuntimeHandle {
    /// Reserve output ownership before an authenticated submit bridge can commit.
    /// An uncertain/lost bridge reply must never be converted into caller timeout abort.
    pub(crate) async fn fence_claimed_abort_for_tool(
        &self,
        name: &str,
    ) -> Result<Option<ClaimedOutputFence>> {
        let Some(policy) = self.restricted_tool_allowlist().await? else {
            return Ok(None);
        };
        if !policy.structured_output_only()
            || policy.completion_tool().is_some_and(|tool| tool != name)
        {
            return Ok(None);
        }
        let mutation = self.begin_mutation().await?;
        let Some(handle) = mutation.current_run_abort.as_ref() else {
            return Ok(None);
        };
        let mut reason = handle.reason.lock().map_err(|_| unavailable())?;
        if handle.token.is_cancelled() {
            return Err(CurrentRunAborted {
                run_id: handle.run_id.clone(),
                reason: reason.clone(),
            }
            .into());
        }
        // A later retry must not acquire authority to clear an earlier unknown send.
        if reason.starts_with(OUTPUT_ACCEPTANCE_PENDING) {
            return Ok(None);
        }
        let marker = format!(
            "{OUTPUT_ACCEPTANCE_PENDING}{}",
            crate::ids::tool_execution_id()
        );
        *reason = marker.clone();
        Ok(Some(ClaimedOutputFence {
            run_id: handle.run_id.clone(),
            marker,
        }))
    }
    pub(crate) async fn release_claimed_abort_after_rejection(
        &self,
        fence: &ClaimedOutputFence,
    ) -> Result<()> {
        let mutation = self.begin_mutation().await?;
        let Some(handle) = mutation.current_run_abort.as_ref() else {
            return Ok(());
        };
        let mut reason = handle.reason.lock().map_err(|_| unavailable())?;
        if handle.run_id == fence.run_id && !handle.token.is_cancelled() && *reason == fence.marker
        {
            *reason = "operator_aborted".to_owned();
        }
        Ok(())
    }
    pub(crate) async fn abort_claimed_structured_prompt(
        &self,
        incarnation: u64,
        principal: String,
        request_id: &str,
        message_id: &str,
        run_id: &str,
    ) -> Result<CancelStatus> {
        ensure!(
            incarnation == self.instance_incarnation(),
            crate::runtime_db::conversation::ConversationReadError::IdentityChanged
        );
        ensure!(
            !message_id.trim().is_empty()
                && message_id.len() <= 240
                && !run_id.trim().is_empty()
                && run_id.len() <= 240,
            crate::runtime_db::conversation::ConversationReadError::InvalidRequest(
                "source and run IDs required".into()
            )
        );
        let scope = CancelScope::new(
            self.instance_agent_id().to_owned(),
            incarnation,
            principal,
            request_id,
        )?;
        let _admission = self.execution_admission_lease()?;
        ensure!(
            self.restricted_tool_allowlist()
                .await?
                .is_some_and(|policy| policy.structured_output_only()),
            unavailable()
        );
        // Keep the incarnation-bound mutation owner through the exact run signal.
        // A late request cannot cancel a later run, even in the same incarnation.
        let mutation = self.begin_mutation().await?;
        let claimed = self
            .runtime_db()
            .read_for_preparation(|tx| scope.claimed_message(tx))?;
        ensure!(claimed == message_id, unavailable());
        let turn_id = mutation
            .state
            .current_turn_id
            .as_deref()
            .ok_or_else(unavailable)?;
        let handle = mutation
            .current_run_abort
            .as_ref()
            .ok_or_else(unavailable)?;
        ensure!(
            handle.run_id == run_id
                && mutation.state.current_run_id.as_deref() == Some(run_id)
                && self.has_standalone_structured_binding(&mutation.state, message_id, turn_id)?,
            unavailable()
        );
        let mut reason = handle.reason.lock().map_err(|_| unavailable())?;
        ensure!(
            !reason.starts_with(OUTPUT_ACCEPTANCE_PENDING),
            unavailable()
        );
        // Never overwrite accepted-output completion or an unrelated abort owner.
        ensure!(
            !handle.token.is_cancelled() || *reason == CLAIMED_PROMPT_ABORT,
            unavailable()
        );
        *reason = CLAIMED_PROMPT_ABORT.to_owned();
        handle.token.cancel();
        Ok(CancelStatus::AbortRequested {
            message_id: claimed,
            run_id: run_id.to_owned(),
        })
    }
}
