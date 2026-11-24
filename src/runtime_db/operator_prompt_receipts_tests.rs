use super::*;
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

#[tokio::test]
async fn operator_prompt_receipt_recovers_accepted_response_loss_after_restart_without_second_enqueue(
) -> Result<()> {
    let home = tempdir()?;
    let r = runtime(home.path()).await?;
    let inc = r.instance_incarnation();
    let original = send(&r, "original-unknown-key", true).await?;
    assert_eq!(queued(&r)?, 1);
    drop(r);
    let r = runtime(home.path()).await?;
    let before = queued(&r)?;
    let found = read(&r, inc, "control", "original-unknown-key")?;
    assert_eq!(
        found.outcome,
        OperatorPromptRecoveryStatus::Accepted {
            message_id: original.clone()
        }
    );
    assert_eq!(read(&r, inc, "control", "original-unknown-key")?, found);
    assert_eq!(
        read(&r, inc, "different-control-actor", "original-unknown-key")?.outcome,
        OperatorPromptRecoveryStatus::NotFound
    );
    assert_eq!(
        read(&r, inc, "control", "different-key")?.outcome,
        OperatorPromptRecoveryStatus::NotFound
    );
    assert!(read(&r, inc + 1, "control", "original-unknown-key").is_err());
    assert_eq!(queued(&r)?, before);
    Ok(())
}
#[tokio::test]
async fn operator_prompt_receipt_unfenced_legacy_is_unknown_and_missing_ledger_is_not_found(
) -> Result<()> {
    let home = tempdir()?;
    let r = runtime(home.path()).await?;
    let inc = r.instance_incarnation();
    assert_eq!(
        read(&r, inc, "control", "missing")?.outcome,
        OperatorPromptRecoveryStatus::NotFound
    );
    send(&r, "legacy-no-incarnation", false).await?;
    assert_eq!(
        read(&r, inc, "control", "legacy-no-incarnation")?.outcome,
        OperatorPromptRecoveryStatus::Unknown
    );
    assert!(read(&r, 0, "control", "missing").is_err());
    assert!(read(&r, inc, "control", " ").is_err());
    Ok(())
}
