//! Authenticated request cancellation and opt-in exact structured run abort.
use super::*;
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelRequest {
    client_request_id: String,
    #[serde(default)]
    complete_structured_output: bool,
    #[serde(default, deserialize_with = "claimed_abort_request")]
    abort_claimed: Option<ClaimedPromptAbort>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimedPromptAbort {
    message_id: String,
    run_id: String,
}
fn claimed_abort_request<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<ClaimedPromptAbort>, D::Error> {
    ClaimedPromptAbort::deserialize(deserializer).map(Some)
}
pub async fn cancel(
    Path(agent): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(input): ApiJson<CancelRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    authorize_control(&headers, &state).map_err(authentication_error_response)?;
    if input.complete_structured_output && input.abort_claimed.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"completion and abort are mutually exclusive"})),
        ));
    }
    let principal = control_actor(&headers, &state)
        .map_err(authentication_error_response)?
        .principal_id();
    let incarnation = conversation::request_incarnation(&headers)?
        .filter(|n| *n > 0)
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"expected incarnation required"})),
            )
        })?;
    let runtime = state
        .host
        .get_operator_agent(&agent)
        .await
        .map_err(agent_access_error)?;
    let outcome = if let Some(abort) = input.abort_claimed {
        runtime
            .abort_claimed_structured_prompt(
                incarnation,
                principal,
                &input.client_request_id,
                &abort.message_id,
                &abort.run_id,
            )
            .await
    } else if input.complete_structured_output {
        runtime
            .complete_claimed_structured_prompt(incarnation, principal, &input.client_request_id)
            .await
    } else {
        runtime
            .cancel_operator_prompt(incarnation, principal, input.client_request_id.clone())
            .await
    }
    .map_err(conversation::conversation_error)?;
    Ok(Json(CancelResponse {
        agent_id: agent,
        incarnation,
        client_request_id: input.client_request_id,
        outcome,
    }))
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelResponse {
    agent_id: String,
    incarnation: u64,
    client_request_id: String,
    #[serde(flatten)]
    outcome: crate::runtime_db::operator_prompt_cancel::CancelStatus,
}
#[cfg(all(test, holon_lib_test_shard = "a"))]
mod tests {
    use super::*;
    use axum::{body::to_bytes, http::Request};
    use tower::ServiceExt;
    #[test]
    fn prompt_cancel_claimed_response_contains_exact_scope_and_message() {
        let response = CancelResponse {
            agent_id: "a".into(),
            incarnation: 7,
            client_request_id: "original".into(),
            outcome: crate::runtime_db::operator_prompt_cancel::CancelStatus::AlreadyClaimed {
                message_id: "m".into(),
            },
        };
        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "agentId":"a", "incarnation":7, "clientRequestId":"original",
                "status":"already_claimed", "messageId":"m",
            })
        );
    }
    #[tokio::test]
    async fn prompt_cancel_http_auth_guard_contract_and_receipt() {
        let (_home, host) = super::super::tests::control_token_test_host();
        let runtime = host.default_runtime().await.unwrap();
        let inc = runtime.instance_incarnation();
        let agent = runtime.instance_agent_id();
        let app = router(AppState::for_tcp(host.clone()));
        for (auth, incarnation, body, expected) in [
            (
                false,
                Some(inc),
                r#"{"client_request_id":"synthetic"}"#,
                StatusCode::UNAUTHORIZED,
            ),
            (
                true,
                None,
                r#"{"client_request_id":"synthetic"}"#,
                StatusCode::BAD_REQUEST,
            ),
            (
                true,
                Some(inc + 1),
                r#"{"client_request_id":"synthetic"}"#,
                StatusCode::CONFLICT,
            ),
            (
                true,
                Some(inc),
                r#"{"client_request_id":""}"#,
                StatusCode::BAD_REQUEST,
            ),
            (
                true,
                Some(inc),
                r#"{"client_request_id":"synthetic","all":true}"#,
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                true,
                Some(inc),
                r#"{"client_request_id":"synthetic"}"#,
                StatusCode::OK,
            ),
        ] {
            let mut request = Request::builder()
                .method("POST")
                .uri(format!("/api/control/agents/{agent}/prompt-cancel"))
                .header("content-type", "application/json");
            if auth {
                request = request.header("authorization", "Bearer secret");
            }
            if let Some(n) = incarnation {
                request = request.header("x-holon-expected-incarnation", n.to_string());
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::from(body)).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            if expected == StatusCode::OK {
                let value: Value = serde_json::from_slice(
                    &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
                )
                .unwrap();
                assert_eq!(
                    value,
                    json!({"agentId":agent, "incarnation":inc, "clientRequestId":"synthetic", "status":"cancelled"})
                );
            }
        }
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/control/agents/{agent}/prompt-receipt?client_request_id=synthetic"
                    ))
                    .header("authorization", "Bearer secret")
                    .header("x-holon-expected-incarnation", inc.to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(value["status"], "cancelled");
        host.shutdown().await.unwrap();
    }
}
