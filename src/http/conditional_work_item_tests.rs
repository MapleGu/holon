use super::*;
use axum::{body::to_bytes, http::Request};
use tower::ServiceExt;

async fn call(
    app: Router,
    method: &str,
    path: &str,
    body: &str,
    authenticated: bool,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if authenticated {
        request = request.header("authorization", "Bearer secret");
    }
    app.oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn issue134_capability_is_authenticated_and_lists_conditional_mutations() {
    let (_home, host) = tests::control_token_test_host();
    let app = router(AppState::for_tcp(host.clone()));
    let path = "/api/control/runtime/conditional-work-item-capabilities";
    assert_eq!(
        call(app.clone(), "GET", path, "", false).await.status(),
        StatusCode::UNAUTHORIZED
    );
    let response = call(app, "GET", path, "", true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(
        body,
        json!({"conditional_work_item_create": true, "conditional_work_item_event_wait": true, "conditional_work_item_complete": true})
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue134_conditional_wire_never_downgrades_bad_guard_or_activates_wrong_target() {
    let (_home, host) = tests::control_token_test_host();
    let app = router(AppState::for_tcp(host.clone()));
    let path = "/api/control/agents/synthetic-missing/work-items";
    assert_eq!(
        call(
            app.clone(),
            "POST",
            path,
            r#"{"objective":"synthetic","expected_incarnation":1}"#,
            false
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    for guard in ["null", "0", "-1", "1.5", "\"1\""] {
        let body = format!(r#"{{"objective":"synthetic","expected_incarnation":{guard}}}"#);
        assert_eq!(
            call(app.clone(), "POST", path, &body, true).await.status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    let response = call(
        app,
        "POST",
        path,
        r#"{"objective":"synthetic","expected_incarnation":9}"#,
        true,
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(body["code"], "agent_incarnation_mismatch");
    assert!(
        host.agent_identity_record("synthetic-missing")
            .unwrap()
            .is_none()
    );
    assert!(
        host.try_get_loaded_runtime("synthetic-missing")
            .await
            .is_none()
    );
    assert!(
        host.runtime_db()
            .work_items()
            .latest_for_agent("synthetic-missing", 10)
            .unwrap()
            .is_empty()
    );
    host.shutdown().await.unwrap();
}

#[test]
fn issue134_guard_field_is_additive_for_existing_callers() {
    let old: CreateWorkItemRequest =
        serde_json::from_value(json!({"objective":"synthetic legacy"})).unwrap();
    assert!(old.expected_incarnation.is_none());
    let guarded: CreateWorkItemRequest =
        serde_json::from_value(json!({"objective":"synthetic guarded", "expected_incarnation":2}))
            .unwrap();
    assert_eq!(guarded.expected_incarnation.unwrap(), 2);
}

#[tokio::test]
async fn issue134_http_create_returns_native_record_for_exact_owner() {
    let (_home, host) = tests::control_token_test_host();
    let identity = host
        .create_named_agent("synthetic-http-work", None)
        .await
        .unwrap();
    let app = router(AppState::for_tcp(host.clone()));
    let payload = json!({"objective":"synthetic conditional HTTP objective", "expected_incarnation":identity.incarnation}).to_string();
    let response = call(
        app,
        "POST",
        "/api/control/agents/synthetic-http-work/work-items",
        &payload,
        true,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let record: WorkItemRecord =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(record.agent_id, "synthetic-http-work");
    assert_eq!(record.objective, "synthetic conditional HTTP objective");
    assert_eq!(
        host.runtime_db()
            .work_items()
            .latest(&record.id)
            .unwrap()
            .unwrap()
            .id,
        record.id
    );
    host.shutdown().await.unwrap();
}

#[test]
fn issue134_unknown_guard_spelling_cannot_silently_become_unguarded_create() {
    assert!(
        serde_json::from_value::<CreateWorkItemRequest>(
            json!({"objective":"synthetic", "expectedIncarnation":1})
        )
        .is_err()
    );
}

#[tokio::test]
async fn issue134_wait_complete_are_guarded_and_event_wait_has_no_recheck() {
    let (_home, host) = tests::control_token_test_host();
    let identity = host
        .create_named_agent("synthetic-follow-up", None)
        .await
        .unwrap();
    let (_, item) = host
        .enqueue_public_work_item_if_incarnation(
            "synthetic-follow-up",
            identity.incarnation,
            "synthetic follow up".into(),
        )
        .await
        .unwrap();
    let app = router(AppState::for_tcp(host.clone()));
    let base = format!(
        "/api/control/agents/synthetic-follow-up/work-items/{}",
        item.id
    );
    for suffix in ["wait", "complete"] {
        let field = if suffix == "wait" {
            "reason"
        } else {
            "report_text"
        };
        let body =
            json!({field:"synthetic", "expected_incarnation":identity.incarnation+1}).to_string();
        assert_eq!(
            call(
                app.clone(),
                "POST",
                &format!("{base}/{suffix}"),
                &body,
                false
            )
            .await
            .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            call(
                app.clone(),
                "POST",
                &format!("{base}/{suffix}"),
                &body,
                true
            )
            .await
            .status(),
            StatusCode::CONFLICT
        );
        for guard in ["null", "0", "-1", "1.5", "\"1\""] {
            let body = format!(r#"{{"{field}":"synthetic","expected_incarnation":{guard}}}"#);
            assert_eq!(
                call(
                    app.clone(),
                    "POST",
                    &format!("{base}/{suffix}"),
                    &body,
                    true
                )
                .await
                .status(),
                StatusCode::UNPROCESSABLE_ENTITY
            );
        }
    }
    let runtime = host
        .try_get_loaded_runtime("synthetic-follow-up")
        .await
        .unwrap();
    runtime
        .register_wait_for(
            "synthetic-follow-up",
            Some(item.id.clone()),
            crate::runtime::WaitForWakeKind::OperatorInput,
            None,
            "old timed wait".into(),
            Some(60000),
        )
        .await
        .unwrap();
    let response = call(
        app.clone(),
        "POST",
        &format!("{base}/wait"),
        &json!({"expected_incarnation":identity.incarnation,"reason":"await synthetic evidence"})
            .to_string(),
        true,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let waiting: WorkItemRecord =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(
        waiting.blocked_by.as_deref(),
        Some("await synthetic evidence")
    );
    assert!(waiting.blocked_by_wait_id.is_some());
    assert!(waiting.recheck_at.is_none());
    assert!(waiting.recheck_consumed_at.is_none());
    assert_eq!(
        host.runtime_db()
            .work_items()
            .latest(&item.id)
            .unwrap()
            .unwrap()
            .recheck_at,
        None
    );
    let response = call(
        app.clone(),
        "POST",
        &format!("{base}/complete"),
        &json!({"expected_incarnation":identity.incarnation,"report_text":"synthetic resolved"})
            .to_string(),
        true,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let completed: WorkItemRecord =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(completed.state, crate::types::WorkItemState::Completed);
    assert_eq!(
        host.runtime_db()
            .work_items()
            .latest(&item.id)
            .unwrap()
            .unwrap()
            .state,
        crate::types::WorkItemState::Completed
    );
    assert_eq!(
        call(
            app,
            "POST",
            &format!("{base}/wait"),
            &json!({"expected_incarnation":identity.incarnation,"reason":"cannot reopen"})
                .to_string(),
            true
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue134_guarded_wait_and_complete_never_activate_missing_target() {
    let (_home, host) = tests::control_token_test_host();
    let app = router(AppState::for_tcp(host.clone()));
    for (suffix, field) in [("wait", "reason"), ("complete", "report_text")] {
        let path = format!("/api/control/agents/synthetic-absent/work-items/synthetic-id/{suffix}");
        let body = json!({"expected_incarnation":1,field:"synthetic"}).to_string();
        assert_eq!(
            call(app.clone(), "POST", &path, &body, true).await.status(),
            StatusCode::CONFLICT
        );
    }
    assert!(
        host.agent_identity_record("synthetic-absent")
            .unwrap()
            .is_none()
    );
    assert!(
        host.try_get_loaded_runtime("synthetic-absent")
            .await
            .is_none()
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue134_disconnected_host_mutation_keeps_lifecycle_lock_until_commit() {
    let (_home, host) = tests::control_token_test_host();
    let identity = host
        .create_named_agent("synthetic-disconnect", None)
        .await
        .unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let clone = host.clone();
    let caller = tokio::spawn(async move {
        clone
            .mutate_public_work_item_if_incarnation(
                "synthetic-disconnect",
                identity.incarnation,
                move |runtime| async move {
                    let _ = entered_tx.send(());
                    release_rx.await.unwrap();
                    runtime
                        .create_work_item_if_incarnation(
                            identity.incarnation,
                            "survives synthetic disconnect".into(),
                        )
                        .await
                },
            )
            .await
    });
    entered_rx.await.unwrap();
    let lock = host.agent_bootstrap_lock("synthetic-disconnect");
    assert!(lock.try_lock().is_err());
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
    assert!(lock.try_lock().is_err());
    release_tx.send(()).unwrap();
    let guard = tokio::time::timeout(std::time::Duration::from_secs(10), lock.lock())
        .await
        .unwrap();
    let records = host
        .runtime_db()
        .work_items()
        .latest_for_agent("synthetic-disconnect", 10)
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].objective, "survives synthetic disconnect");
    drop(guard);
    host.shutdown().await.unwrap();
}

struct HttpFollowUpTextProvider(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl crate::provider::AgentProvider for HttpFollowUpTextProvider {
    async fn complete_turn(
        &self,
        request: crate::provider::ProviderTurnRequest,
    ) -> anyhow::Result<crate::provider::ProviderTurnResponse> {
        assert!(
            request.tools.is_empty(),
            "closed initial bridge must not be used for follow-up"
        );
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        crate::provider::AgentProvider::complete_turn(
            &crate::provider::StubProvider::new(
                r#"{"outcome":"completed","evidenceId":"synthetic-event-evidence-http-134"}"#,
            ),
            request,
        )
        .await
    }
    fn configured_model_refs(&self) -> Vec<String> {
        vec!["stub".into()]
    }
}

async fn serve_follow_up_http(
    host: RuntimeHost,
) -> (
    String,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = router(AppState::for_tcp(host));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    (format!("http://{address}/api"), stop, task)
}

async fn follow_up_http_json(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    path: &str,
    incarnation: Option<u64>,
    body: Option<Value>,
) -> Value {
    let mut request = client
        .request(
            reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
            format!("{base}{path}"),
        )
        .bearer_auth("secret");
    if let Some(incarnation) = incarnation {
        request = request.header("x-holon-expected-incarnation", incarnation);
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let value: Value = response.json().await.unwrap();
    assert_eq!(status.as_u16(), 200, "{method} {path}: {value}");
    if let Some(incarnation) = incarnation {
        assert_eq!(
            headers
                .get("x-holon-agent-incarnation")
                .unwrap()
                .to_str()
                .unwrap(),
            incarnation.to_string()
        );
        assert_eq!(
            headers.get("x-holon-agent-id").unwrap(),
            "synthetic-http-source"
        );
    }
    value
}

#[tokio::test]
async fn issue134_real_http_followup_idle_proof_exact_brief_complete_and_get() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (_home, initial) = tests::control_token_test_host();
    let config = initial.config().as_ref().clone();
    initial.shutdown().await.unwrap();
    drop(initial);
    let provider = Arc::new(HttpFollowUpTextProvider(AtomicUsize::new(0)));
    let host = RuntimeHost::new_with_provider(config.clone(), provider.clone()).unwrap();
    let (_, startup) = host
        .recover_startup_deferred_observed(&crate::startup::StartupObservation::new())
        .await
        .unwrap();
    let policy = serde_json::from_value(json!({"tool_name_prefix":"fixture_","tools":[],"bridge":{"url":"http://127.0.0.1:9/call","bearer":"synthetic-closed-http-bridge-134"}})).unwrap();
    let created = host
        .create_restricted_agent("synthetic-http-source", None, None, false, policy)
        .await
        .unwrap();
    let incarnation = created.identity.incarnation;
    let (base, stop, server) = serve_follow_up_http(host.clone()).await;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .unwrap();
    let control = "/control/agents/synthetic-http-source/work-items";
    let work = follow_up_http_json(&client, &base, "POST", control, None,
        Some(json!({"objective":"synthetic HTTP verification follow-up", "expected_incarnation":incarnation}))).await;
    let id = work["id"].as_str().unwrap().to_string();
    follow_up_http_json(
        &client,
        &base,
        "POST",
        &format!("{control}/{id}/wait"),
        None,
        Some(json!({"reason":"synthetic event only", "expected_incarnation":incarnation})),
    )
    .await;
    startup.release();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
    stop.send(()).unwrap();
    server.await.unwrap();
    host.shutdown().await.unwrap();
    drop(host);

    let host = RuntimeHost::new_with_provider(config, provider.clone()).unwrap();
    host.recover_orphaned_queue_claims_at_startup()
        .await
        .unwrap();
    let (base, stop, server) = serve_follow_up_http(host.clone()).await;
    let read_path = format!("/agents/synthetic-http-source/work-items/{id}");
    let open =
        follow_up_http_json(&client, &base, "GET", &read_path, Some(incarnation), None).await;
    assert_eq!(open["state"], "open");
    assert_eq!(open["blocked_by"], "synthetic event only");
    assert!(open["recheck_at"].is_null());
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);
    let prompt = follow_up_http_json(&client, &base, "POST", "/control/agents/synthetic-http-source/prompt", None,
        Some(json!({"text":"synthetic verified event evidence; return completion JSON as plain text", "work_item_id":id,
            "expected_incarnation":incarnation, "client_request_id":"synthetic-http-follow-up-event-134"}))).await;
    assert_eq!(prompt["disposition"], "accepted");
    let message = prompt["message_id"].as_str().unwrap();
    let locate_path = "/agents/synthetic-http-source/conversation/locate";
    let last_locate = std::sync::Mutex::new(Value::Null);
    let turn = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let located = follow_up_http_json(
                &client,
                &base,
                "POST",
                locate_path,
                Some(incarnation),
                Some(json!({"source_ref":format!("message:{message}")})),
            )
            .await;
            *last_locate.lock().unwrap() = located.clone();
            if located["status"] == "located" && located["target"]["kind"] == "input_message" {
                assert_eq!(located["target"]["message_id"], message);
                let turn_id = &located["target"]["turn_id"];
                if let Some(turn) =
                    located["window"]["turns"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|turn| {
                            &turn["turn_id"] == turn_id
                                && turn["inputs"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .any(|input| input["message_id"] == message)
                        })
                {
                    if turn["execution"]["kind"] == "terminal"
                        && !turn["completed_at"].is_null()
                    {
                        // Follow-up-only alternative proof. This does not change the
                        // canonical settled flag or relax the original #131 submit path.
                        let status = follow_up_http_json(&client, &base, "GET",
                            "/agents/synthetic-http-source/status", None, None).await;
                        assert_eq!(status["identity"]["agent_id"], "synthetic-http-source");
                        assert_eq!(status["identity"]["incarnation"], incarnation);
                        assert_eq!(status["agent"]["id"], "synthetic-http-source");
                        let idle = matches!(status["agent"]["status"].as_str(), Some("asleep" | "awake_idle"))
                            && status["agent"]["current_run_id"].is_null()
                            && status["agent"]["pending"] == 0 && status["active_task_count"] == 0
                            && located["window"]["active_turns"].as_array().is_some_and(Vec::is_empty)
                            && located["window"]["pending_inputs"].as_array().is_some_and(Vec::is_empty);
                        if idle { break turn.clone(); }
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("HTTP locateSource qualification failed: {error}; calls={}; last={}; state={:?}; events={:?}", provider.0.load(Ordering::SeqCst), last_locate.lock().unwrap(), host.agent_storage("synthetic-http-source").unwrap().read_agent(), host.agent_storage("synthetic-http-source").unwrap().read_recent_events(35)));
    assert_eq!(
        turn["execution"]["outcome"], "completed",
        "actual terminal must satisfy Service qualification: {turn}"
    );
    assert_eq!(
        turn["result"]["kind"], "available",
        "actual result must satisfy Service qualification: {turn}"
    );
    assert_eq!(
        turn["settled"], false,
        "do not fabricate canonical delivery settlement from follow-up idle proof"
    );
    let brief_ids = turn["brief_ids"].as_array().unwrap();
    assert!(!brief_ids.is_empty() && brief_ids.len() <= 8);
    let mut exact = Vec::new();
    for brief_id in brief_ids {
        let brief = follow_up_http_json(
            &client,
            &base,
            "GET",
            &format!(
                "/agents/synthetic-http-source/briefs/{}",
                brief_id.as_str().unwrap()
            ),
            Some(incarnation),
            None,
        )
        .await;
        if brief["kind"] == "result"
            && brief["agent_id"] == "synthetic-http-source"
            && brief["work_item_id"] == id
            && brief["turn_id"] == turn["turn_id"]
        {
            exact.push(brief);
        }
    }
    assert_eq!(exact.len(), 1);
    let text = exact[0]["text"].as_str().unwrap();
    assert!(text.len() <= 512);
    let report: Value = serde_json::from_str(text).unwrap();
    assert_eq!(report.as_object().unwrap().len(), 2);
    assert_eq!(
        report,
        json!({"outcome":"completed","evidenceId":"synthetic-event-evidence-http-134"})
    );
    let completed = follow_up_http_json(&client, &base, "POST", &format!("{control}/{id}/complete"), None,
        Some(json!({"expected_incarnation":incarnation,"report_text":"Verified verification follow-up completed using event evidence synthetic-event-evidence-http-134."}))).await;
    assert_eq!(completed["state"], "completed");
    let readback =
        follow_up_http_json(&client, &base, "GET", &read_path, Some(incarnation), None).await;
    assert_eq!(readback["id"], id);
    assert_eq!(readback["agent_id"], "synthetic-http-source");
    assert_eq!(readback["state"], "completed");
    assert_eq!(readback["revision"], completed["revision"]);
    eprintln!(
        "followup_http_evidence: original_message={} original_turn={} raw_brief_id={} raw_brief_work={} raw_brief_turn={} settled={} completed_get={} incarnation={}",
        message,
        turn["turn_id"],
        exact[0]["id"],
        exact[0]["work_item_id"],
        exact[0]["turn_id"],
        turn["settled"],
        readback["state"],
        incarnation
    );
    assert!(provider.0.load(Ordering::SeqCst) >= 1);
    stop.send(()).unwrap();
    server.await.unwrap();
    host.shutdown().await.unwrap();
}
