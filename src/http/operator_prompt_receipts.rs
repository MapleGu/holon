//! Control-only exact request receipt lookup; never resends a prompt.
use super::*;
use anyhow::Context;
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptQuery {
    client_request_id: String,
}
pub async fn get(
    Path(agent): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ReceiptQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    authorize_control(&headers, &state).map_err(authentication_error_response)?;
    let principal = control_actor(&headers, &state)
        .map_err(authentication_error_response)?
        .principal_id();
    let inc = conversation::request_incarnation(&headers)?
        .filter(|n| *n > 0)
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"expected incarnation required"})),
            )
        })?;
    let storage = state
        .host
        .operator_agent_read_storage(&agent)
        .map_err(agent_access_error)?;
    let receipt = storage
        .read_block("operator_prompt.exact_receipt", move |s| {
            s.runtime_db()?
                .context("runtime DB missing")?
                .with_read(|c| {
                    let tx = c.transaction()?;
                    crate::runtime_db::operator_prompt_receipts::lookup(
                        &tx,
                        &agent,
                        inc,
                        &principal,
                        &q.client_request_id,
                    )
                })
        })
        .await
        .map_err(conversation::conversation_error)?;
    Ok(Json(receipt))
}
