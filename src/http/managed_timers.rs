//! Service-only managed timer controls; native remains the sole due producer.
use super::*;
use crate::runtime_db::managed_timers::{
    self as managed, ManagedAck, ManagedCancel, ManagedCreate, ManagedSchedule, PendingQuery,
};
use anyhow::Context;
fn incarnation(headers: &HeaderMap) -> Result<u64, (StatusCode, Json<Value>)> {
    conversation::request_incarnation(headers)?
        .filter(|n| *n > 0)
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"managed owner incarnation required"})),
            )
        })
}
fn failure(e: anyhow::Error) -> (StatusCode, Json<Value>) {
    error_response(e)
}

pub async fn create(
    Path(agent): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(input): ApiJson<ManagedCreate>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    authorize_control(&headers, &state).map_err(authentication_error_response)?;
    let inc = incarnation(&headers)?;
    let runtime = state
        .host
        .get_operator_agent(&agent)
        .await
        .map_err(agent_access_error)?;
    Ok(Json(
        runtime
            .create_managed_timer(inc, input)
            .await
            .map_err(failure)?,
    ))
}
pub async fn cancel(
    Path((agent, id)): Path<(String, String)>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(input): ApiJson<ManagedCancel>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    authorize_control(&headers, &state).map_err(authentication_error_response)?;
    let inc = incarnation(&headers)?;
    let runtime = state
        .host
        .get_operator_agent(&agent)
        .await
        .map_err(agent_access_error)?;
    Ok(Json(
        runtime
            .cancel_managed_timer(inc, id, input)
            .await
            .map_err(failure)?,
    ))
}
pub async fn get(
    Path((agent, id)): Path<(String, String)>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    authorize_control(&headers, &state).map_err(authentication_error_response)?;
    let inc = incarnation(&headers)?;
    let storage = state
        .host
        .operator_agent_read_storage(&agent)
        .map_err(agent_access_error)?;
    Ok(Json(
        storage
            .read_block("managed_timer.get", move |s| {
                s.runtime_db()?
                    .context("runtime DB missing")?
                    .with_read(|c| {
                        let tx = c.transaction()?;
                        managed::get(&tx, &agent, inc, &id)
                    })
            })
            .await
            .map_err(failure)?,
    ))
}
pub async fn list(
    Path(agent): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<LimitQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    authorize_control(&headers, &state).map_err(authentication_error_response)?;
    let inc = incarnation(&headers)?;
    let storage = state
        .host
        .operator_agent_read_storage(&agent)
        .map_err(agent_access_error)?;
    Ok(Json(
        storage
            .read_block("managed_timer.list", move |s| {
                s.runtime_db()?
                    .context("runtime DB missing")?
                    .with_read(|c| {
                        let tx = c.transaction()?;
                        managed::list(&tx, &agent, inc, q.limit.unwrap_or(100))
                    })
            })
            .await
            .map_err(failure)?,
    ))
}
pub async fn pending(
    Path(agent): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<PendingQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    authorize_control(&headers, &state).map_err(authentication_error_response)?;
    let inc = incarnation(&headers)?;
    let storage = state
        .host
        .operator_agent_read_storage(&agent)
        .map_err(agent_access_error)?;
    Ok(Json(
        storage
            .read_block("managed_timer.pending", move |s| {
                s.runtime_db()?
                    .context("runtime DB missing")?
                    .with_read(|c| {
                        let tx = c.transaction()?;
                        managed::pending(&tx, &agent, inc, &q)
                    })
            })
            .await
            .map_err(failure)?,
    ))
}
pub async fn ack(
    Path((agent, id)): Path<(String, String)>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(a): ApiJson<ManagedAck>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    authorize_control(&headers, &state).map_err(authentication_error_response)?;
    let inc = incarnation(&headers)?;
    let storage = state
        .host
        .operator_agent_read_storage(&agent)
        .map_err(agent_access_error)?;
    let db = storage
        .runtime_db()
        .map_err(failure)?
        .context("runtime DB missing")
        .map_err(failure)?;
    let result = tokio::task::spawn_blocking(move || {
        db.transaction(|tx| managed::ack_tx(tx, &agent, inc, &id, &a))
    })
    .await
    .map_err(|e| failure(e.into()))?
    .map_err(failure)?;
    Ok(Json(result))
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Preview {
    schedule: ManagedSchedule,
    after: Option<i64>,
    count: Option<usize>,
}
pub async fn preview(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(p): ApiJson<Preview>,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    authorize_control(&headers, &state).map_err(authentication_error_response)?;
    Ok(Json(
        p.schedule
            .preview(
                p.after
                    .unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
                p.count.unwrap_or(3),
            )
            .map_err(failure)?,
    ))
}
