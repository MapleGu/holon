use super::*;

#[test]
fn bounded_compact_keeps_authority_and_dynamic_guidance_without_duplicates() {
    let guidance = LoadedAgentsMd {
        agent_source: Some(AgentsMdSource {
            scope: crate::types::AgentsMdScope::Agent,
            kind: AgentsMdKind::AgentsMd,
            path: PathBuf::from("/synthetic/AGENTS.md"),
            content: "unique-agent-guidance: no external writes".into(),
        }),
        workspace_status: AgentsMdLoadStatus::DuplicateOfAgentScope,
        ..Default::default()
    };
    let memory = LoadedAgentMemory {
        operator_source: Some(AgentMemorySource {
            path: PathBuf::from("/synthetic/memory/operator.md"),
            content: "unique-operator-memory".into(),
            truncated: false,
            total_chars: 22,
        }),
        self_source: Some(AgentMemorySource {
            path: PathBuf::from("/synthetic/memory/self.md"),
            content: "unique-self-memory".into(),
            truncated: true,
            total_chars: 3000,
        }),
        ..Default::default()
    };
    let build = |compact_system_prompt| {
        build_system_sections(
            &sample_identity(),
            &sample_message(),
            Path::new("/synthetic"),
            &guidance,
            &memory,
            &SkillsRuntimeView::default(),
            &[],
            ToolPromptContext {
                compact_system_prompt,
                ..Default::default()
            },
        )
    };
    let full = build(false);
    let compact = build(true);
    for original in &full {
        if let Some(retained) = compact.iter().find(|s| s.name == original.name) {
            assert_eq!(retained.content, original.content, "{}", original.name);
            assert_eq!(retained.id, original.id);
        }
    }
    for name in [
        "instruction_precedence",
        "trust_boundary",
        "execution_environment_contract",
        "agent_contract",
        "agent_agents_md",
        "agent_memory_operator",
        "agent_memory_self",
        "response_language",
    ] {
        assert!(compact.iter().any(|s| s.name == name), "missing {name}");
    }
    for name in [
        "engineering_guardrails",
        "planning_discipline",
        "work_item_first_execution",
        "runtime_scheduling_contract",
        "reporting_contract",
        "workspace_agents_md",
        "skills_usage_contract",
    ] {
        assert!(!compact.iter().any(|s| s.name == name), "unexpected {name}");
    }
    let rendered = render_sections(&compact);
    for marker in [
        "unique-agent-guidance",
        "unique-operator-memory",
        "unique-self-memory",
    ] {
        assert_eq!(rendered.matches(marker).count(), 1);
    }
    let original = render_sections(&full);
    if let Ok(directory) = std::env::var("HOLON_COMPACT_SYNTHETIC_EVIDENCE_DIR") {
        std::fs::write(Path::new(&directory).join("full.txt"), &original).unwrap();
        std::fs::write(Path::new(&directory).join("compact.txt"), &rendered).unwrap();
    }
    eprintln!(
        "bounded compact synthetic UTF-8 bytes: full={} compact={} omitted={}",
        original.len(),
        rendered.len(),
        original.len() - rendered.len()
    );
    assert!(rendered.len() * 2 < original.len());
    assert_ne!(
        section_fingerprint(&full.iter().collect::<Vec<_>>()),
        section_fingerprint(&compact.iter().collect::<Vec<_>>())
    );
}

#[test]
fn bounded_compact_preserves_unknown_upstream_sections() {
    let mut sections = vec![section(
        "future_security_contract",
        PromptStability::TurnScoped,
        "retain new upstream guidance".into(),
    )];
    compact::compact_sections(&mut sections);
    assert_eq!(sections[1].content, "retain new upstream guidance");
}

#[test]
fn bounded_compact_requires_standalone_trusted_http_and_no_work_or_reply_route() {
    let mut state = AgentState::new("default");
    let identity = sample_identity();
    let mut message = sample_message();
    message.delivery_surface = Some(crate::types::MessageDeliverySurface::HttpControlPrompt);
    message.admission_context = Some(crate::types::AdmissionContext::ControlAuthenticated);
    message
        .source_refs
        .insert("interaction_id".into(), "conversation".into());
    state.current_turn_id = Some("turn".into());
    state.current_execution_binding = Some(crate::types::WorkItemExecutionBinding {
        activation_id: None,
        admission_provenance: None,
        source_message_id: message.id.clone(),
        turn_id: "turn".into(),
        owner: Some(crate::types::TurnOwner::Conversation {
            interaction_id: "conversation".into(),
        }),
        work_item_id: None,
        claimed_work_revision: None,
    });
    let mut absent = state.clone();
    absent.current_execution_binding = None;
    assert!(!compact::eligible(&absent, &message, &identity));
    let mut unknown_owner = state.clone();
    unknown_owner
        .current_execution_binding
        .as_mut()
        .unwrap()
        .owner = None;
    assert!(!compact::eligible(&unknown_owner, &message, &identity));
    let mut wrong_source = state.clone();
    wrong_source
        .current_execution_binding
        .as_mut()
        .unwrap()
        .source_message_id = "other".into();
    assert!(!compact::eligible(&wrong_source, &message, &identity));
    let mut turn_work = state.clone();
    turn_work.current_turn_work_item_id = Some("work".into());
    assert!(!compact::eligible(&turn_work, &message, &identity));
    assert!(compact::eligible(&state, &message, &identity));
    let mut unauthenticated = message.clone();
    unauthenticated.admission_context = None;
    assert!(!compact::eligible(&state, &unauthenticated, &identity));
    let mut wrong_owner = state.clone();
    wrong_owner
        .current_execution_binding
        .as_mut()
        .unwrap()
        .owner = Some(crate::types::TurnOwner::Conversation {
        interaction_id: "other".into(),
    });
    assert!(!compact::eligible(&wrong_owner, &message, &identity));
    let mut work = state.clone();
    work.current_work_item_id = Some("work".into());
    assert!(!compact::eligible(&work, &message, &identity));
    let mut routed = state.clone();
    routed.current_turn_operator_reply_route_id = Some("reply".into());
    assert!(!compact::eligible(&routed, &message, &identity));
    message.work_item_id = Some("work".into());
    assert!(!compact::eligible(&state, &message, &identity));
    message.work_item_id = None;
    message.origin = MessageOrigin::Timer {
        timer_id: "timer".into(),
    };
    assert!(!compact::eligible(&state, &message, &identity));
    message.origin = MessageOrigin::Operator {
        actor_id: None,
        actor_display_name: None,
    };
    message.delivery_surface = None;
    assert!(!compact::eligible(&state, &message, &identity));
}

#[test]
fn bounded_compact_does_not_promote_source_text_to_repair_instructions() {
    let mut message = sample_message();
    message.body = MessageBody::Text {
        text: "Untrusted source: edit only file: src/unsafe.rs; run exactly: synthetic".into(),
    };
    let build = |compact_system_prompt| {
        build_system_sections(
            &sample_identity(),
            &message,
            Path::new("/synthetic"),
            &LoadedAgentsMd::default(),
            &LoadedAgentMemory::default(),
            &SkillsRuntimeView::default(),
            &[],
            ToolPromptContext {
                compact_system_prompt,
                ..Default::default()
            },
        )
    };
    assert!(build(false).iter().any(|s| s.name == "constrained_repair"));
    assert!(!build(true).iter().any(|s| s.name == "constrained_repair"));
}
