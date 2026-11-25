//! Completion of an exact standalone turn whose authenticated caller owns its output.
use super::*;
use crate::runtime_db::operator_prompt_cancel::{unavailable, CancelScope, CancelStatus};
use crate::types::TurnOwner;
use anyhow::ensure;

pub(super) const ACCEPTED_OUTPUT_ABORT: &str = "structured_output_accepted";

impl RuntimeHandle {
    pub(super) fn has_standalone_structured_binding(
        &self,
        state: &AgentState,
        message_id: &str,
        turn_id: &str,
    ) -> Result<bool> {
        if state.current_run_id.is_none()
            || state.current_turn_id.as_deref() != Some(turn_id)
            || state.current_work_item_id.is_some()
            || state.current_turn_work_item_id.is_some()
            || !state
                .current_execution_binding
                .as_ref()
                .is_some_and(|binding| {
                    binding.turn_id == turn_id
                        && binding.source_message_id == message_id
                        && binding.activation_id.is_some()
                        && binding.work_item_id.is_none()
                        && matches!(binding.owner, Some(TurnOwner::Conversation { .. }))
                })
        {
            return Ok(false);
        }
        let Some(turn) = self.inner.storage.read_turn_by_id(turn_id)? else {
            return Ok(false);
        };
        if turn.run_id != state.current_run_id
            || turn.input_message_ids.as_slice() != [message_id]
            || turn.idle_input_batch.is_some()
            || !turn.live_input_batches.is_empty()
        {
            return Ok(false);
        }
        Ok(self
            .inner
            .storage
            .read_message_by_id(message_id)?
            .is_some_and(|message| {
                message.agent_id == state.id
                    && message.kind == MessageKind::OperatorPrompt
                    && message.delivery_surface == Some(MessageDeliverySurface::HttpControlPrompt)
                    && matches!(
                        message.admission_context,
                        Some(
                            AdmissionContext::ControlAuthenticated | AdmissionContext::LocalProcess
                        )
                    )
                    && matches!(message.origin, MessageOrigin::Operator { .. })
                    && message.work_item_id.is_none()
                    && message.task_id.is_none()
                    && Self::operator_transport_from_message(&message) == (None, None)
            }))
    }

    pub(super) fn structured_completion_isolates_input(&self, state: &AgentState) -> Result<bool> {
        let Some(binding) = state.current_execution_binding.as_ref() else {
            return Ok(false);
        };
        if !crate::tool::restricted::load(
            self.runtime_db(),
            &state.id,
            self.inner.identity_incarnation,
        )?
        .is_some_and(|policy| policy.structured_output_only())
        {
            return Ok(false);
        }
        self.has_standalone_structured_binding(state, &binding.source_message_id, &binding.turn_id)
    }

    pub(crate) async fn is_standalone_structured_turn(
        &self,
        message_id: Option<&str>,
        turn_id: Option<&str>,
    ) -> Result<bool> {
        let (Some(message_id), Some(turn_id)) = (message_id, turn_id) else {
            return Ok(false);
        };
        let guard = self.inner.agent.lock().await;
        self.has_standalone_structured_binding(&guard.state, message_id, turn_id)
    }

    pub(crate) async fn complete_claimed_structured_prompt(
        &self,
        incarnation: u64,
        principal: String,
        request_id: &str,
    ) -> Result<CancelStatus> {
        ensure!(
            incarnation == self.instance_incarnation(),
            crate::runtime_db::conversation::ConversationReadError::IdentityChanged
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
                .is_some_and(|policy| policy.completion_tool().is_some()),
            unavailable()
        );
        // This branch is an authenticated assertion of an already accepted result.
        // It cannot fence a future prompt or cancel a Work/ordinary conversation.
        let mutation = self.begin_mutation().await?;
        let id = self
            .runtime_db()
            .read_for_preparation(|tx| scope.claimed_message(tx))?;
        if let (Some(turn_id), Some(handle)) = (
            mutation.state.current_turn_id.as_deref(),
            mutation.current_run_abort.as_ref(),
        ) {
            if mutation.state.current_run_id.as_deref() == Some(handle.run_id.as_str())
                && self.has_standalone_structured_binding(&mutation.state, &id, turn_id)?
            {
                let mut reason = handle.reason.lock().map_err(|_| unavailable())?;
                if !handle.token.is_cancelled() {
                    *reason = ACCEPTED_OUTPUT_ABORT.to_owned();
                    handle.token.cancel();
                }
            }
        }
        Ok(CancelStatus::AlreadyClaimed { message_id: id })
    }
}
