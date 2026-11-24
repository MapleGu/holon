//! Release an execution, never its durable WorkItem, using the original native fences.
use crate::types::{AgentState, AgentStatus, TurnOwner, TurnRecord};

pub(crate) fn release_exact_turn(
    original: &AgentState,
    state: &mut AgentState,
    turn: &TurnRecord,
    source_message_id: &str,
) -> bool {
    let Some(binding) = original.current_execution_binding.as_ref() else {
        return false;
    };
    let work = turn.current_work_item_id.as_deref();
    let owner_matches = match (&binding.owner, &turn.owner) {
        (
            Some(TurnOwner::Conversation { interaction_id: a }),
            Some(TurnOwner::Conversation { interaction_id: b }),
        ) => a == b && work.is_none(),
        (
            Some(TurnOwner::WorkItem { work_item_id: a }),
            Some(TurnOwner::WorkItem { work_item_id: b }),
        ) => a == b && work == Some(a.as_str()),
        _ => false,
    };
    if original.id != turn.agent_id
        || original.status == AgentStatus::Stopped
        || original.current_turn_id.as_deref() != Some(turn.turn_id.as_str())
        || original.turn_index != turn.turn_index
        || original.current_run_id != turn.run_id
        || original.current_turn_work_item_id.as_deref() != work
        || binding.turn_id != turn.turn_id
        || binding.source_message_id != source_message_id
        || binding.work_item_id.as_deref() != work
        || turn
            .trigger
            .as_ref()
            .and_then(|trigger| trigger.message_id.as_deref())
            != Some(source_message_id)
        || !turn
            .input_message_ids
            .iter()
            .any(|id| id == source_message_id)
        || !owner_matches
    {
        return false;
    }
    state.current_run_id = None;
    state.current_turn_id = None;
    state.current_turn_work_item_id = None;
    state.current_execution_binding = None;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    fn fixture(work: bool) -> (AgentState, TurnRecord) {
        let mut state = AgentState::new("synthetic-agent");
        state.turn_index = 7;
        state.current_turn_id = Some("turn".into());
        state.current_run_id = Some("run".into());
        state.current_turn_work_item_id = work.then(|| "work".into());
        state.current_work_item_id = state.current_turn_work_item_id.clone();
        let owner = if work {
            TurnOwner::WorkItem {
                work_item_id: "work".into(),
            }
        } else {
            TurnOwner::Conversation {
                interaction_id: "conversation".into(),
            }
        };
        state.current_execution_binding = Some(WorkItemExecutionBinding {
            turn_id: "turn".into(),
            source_message_id: "source".into(),
            owner: Some(owner.clone()),
            activation_id: Some("activation".into()),
            admission_provenance: None,
            work_item_id: state.current_turn_work_item_id.clone(),
            claimed_work_revision: work.then_some(3),
        });
        let message = MessageEnvelope::new(
            "synthetic-agent",
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
        let mut turn = TurnRecord::new("synthetic-agent", "turn", 7);
        turn.run_id = Some("run".into());
        turn.current_work_item_id = state.current_turn_work_item_id.clone();
        turn.owner = Some(owner);
        let mut trigger = TurnTriggerSummary::from_message(&message);
        trigger.message_id = Some("source".into());
        turn.trigger = Some(trigger);
        turn.input_message_ids = vec!["source".into()];
        (state, turn)
    }

    #[test]
    fn issue160_exact_completion_preserves_durable_work_focus() {
        for work in [false, true] {
            let (original, turn) = fixture(work);
            let mut released = original.clone();
            assert!(release_exact_turn(
                &original,
                &mut released,
                &turn,
                "source"
            ));
            assert_eq!(released.current_work_item_id, original.current_work_item_id);
            assert!(released.current_run_id.is_none());
            assert!(released.current_turn_id.is_none());
            assert!(released.current_turn_work_item_id.is_none());
            assert!(released.current_execution_binding.is_none());
        }
    }

    #[test]
    fn issue160_stale_completion_never_clears_new_execution_or_stop() {
        for work in [false, true] {
            for change in 0..10 {
                let (mut original, mut turn) = fixture(work);
                match change {
                    0 => original.current_turn_id = Some("new-turn".into()),
                    1 => original.current_run_id = Some("new-run".into()),
                    2 => original.turn_index += 1,
                    3 => {
                        original
                            .current_execution_binding
                            .as_mut()
                            .unwrap()
                            .source_message_id = "other".into()
                    }
                    4 => {
                        original.current_execution_binding.as_mut().unwrap().owner =
                            Some(TurnOwner::Command)
                    }
                    5 => original.current_turn_work_item_id = Some("other-work".into()),
                    6 => {
                        original
                            .current_execution_binding
                            .as_mut()
                            .unwrap()
                            .work_item_id = Some("other-work".into())
                    }
                    7 => original.status = AgentStatus::Stopped,
                    8 => turn.input_message_ids.clear(),
                    9 => turn.trigger.as_mut().unwrap().message_id = Some("other".into()),
                    _ => unreachable!(),
                }
                let mut released = original.clone();
                assert!(
                    !release_exact_turn(&original, &mut released, &turn, "source"),
                    "work={work} change={change}"
                );
                assert_eq!(released, original);
            }
        }
    }
}
