//! Use the same current-turn eligibility for prompt composition and input admission.
use super::*;
impl RuntimeHandle {
    pub(super) fn bounded_compact_turn(&self, state: &AgentState) -> Result<bool> {
        if state.current_run_id.is_none() {
            return Ok(false);
        }
        if let Some((turn_id, selected)) = self
            .inner
            .bounded_prompt_selection
            .lock()
            .map_err(|_| anyhow!("bounded prompt selection lock poisoned"))?
            .as_ref()
        {
            if state.current_turn_id.as_deref() == Some(turn_id.as_str()) {
                return Ok(*selected);
            }
        }
        if !crate::tool::restricted::load(
            self.runtime_db(),
            &state.id,
            self.inner.identity_incarnation,
        )?
        .is_some_and(|policy| policy.compact_system_prompt())
        {
            return Ok(false);
        }
        let Some(binding) = state.current_execution_binding.as_ref() else {
            return Ok(false);
        };
        let Some(source) = self
            .inner
            .storage
            .read_message_by_id(&binding.source_message_id)?
        else {
            return Ok(false);
        };
        let Some(identity) = self.runtime_db().agent_identities().latest(&state.id)? else {
            return Ok(false);
        };
        if identity.incarnation != self.inner.identity_incarnation {
            return Ok(false);
        }
        let identity = AgentIdentityView::from_record(&identity, &self.inner.default_agent_id);
        Ok(crate::prompt::compact::eligible(state, &source, &identity))
    }
}
