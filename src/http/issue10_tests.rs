use super::*;
use axum::{body::to_bytes, http::Request};
use tower::ServiceExt;

#[tokio::test]
async fn issue10_routes_reject_wrong_guard_without_identity_or_runtime_write() {
    let (_home, host) = tests::control_token_test_host();
    let app = router(AppState::for_tcp(host.clone()));
    for (method, path, body, expected) in [
        (
            "POST",
            "/api/control/agents/missing/stop-if-incarnation",
            r#"{"expected_incarnation":9}"#,
            StatusCode::CONFLICT,
        ),
        (
            "DELETE",
            "/api/control/agents/missing",
            r#"{"expected_incarnation":null}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "POST",
            "/api/control/agents/missing/stop-if-incarnation",
            r#"{"expected_incarnation":9,"action":"start"}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "POST",
            "/api/control/agents/missing/stop-if-incarnation",
            r#"{}"#,
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("authorization", "Bearer secret")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        if expected == StatusCode::CONFLICT {
            let body: Value =
                serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                    .unwrap();
            assert_eq!(body["code"], "agent_incarnation_mismatch");
        }
    }
    assert!(host.agent_identity_record("missing").unwrap().is_none());
    assert!(host.try_get_loaded_runtime("missing").await.is_none());
    assert!(host
        .runtime_db()
        .agent_deletions()
        .latest_for_agent("missing")
        .unwrap()
        .is_none());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn issue10_deletion_capability_requires_control_auth() {
    let (_home, host) = tests::control_token_test_host();
    let before = host.agent_identity_record("default").unwrap();
    let app = router(AppState::for_tcp(host.clone()));
    let uri = "/api/control/agent-deletion-capabilities";
    let request = |authorized| {
        let builder = Request::builder().uri(uri);
        let builder = if authorized {
            builder.header("authorization", "Bearer secret")
        } else {
            builder
        };
        builder.body(Body::empty()).unwrap()
    };
    assert_eq!(
        app.clone().oneshot(request(false)).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let response = app.oneshot(request(true)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body, json!({"conditional_idle_delete": true}));
    assert_eq!(host.agent_identity_record("default").unwrap(), before);
    assert!(host.try_get_loaded_runtime("default").await.is_none());
    host.shutdown().await.unwrap();
}
