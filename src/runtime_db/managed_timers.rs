//! Service-managed timer metadata and durable due deliveries. No inference path.
use crate::{
    calendar::CalendarSchedule,
    types::{TimerRecord, TimerStatus},
};
use anyhow::{ensure, Context, Result};
use chrono::DateTime;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

pub(crate) const SCHEMA: &str = r#"
CREATE TABLE managed_timers (
 timer_id TEXT PRIMARY KEY REFERENCES timers(timer_id), agent_id TEXT NOT NULL,
 incarnation INTEGER NOT NULL CHECK(incarnation>0), schedule_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0), schedule_json TEXT NOT NULL,
 UNIQUE(agent_id,incarnation,schedule_id,generation)
);
CREATE TABLE managed_timer_requests (
 agent_id TEXT NOT NULL, incarnation INTEGER NOT NULL, request_id TEXT NOT NULL,
 payload_json TEXT NOT NULL, timer_id TEXT NOT NULL REFERENCES managed_timers(timer_id),
 PRIMARY KEY(agent_id,incarnation,request_id)
);
CREATE TABLE managed_timer_deliveries (
 delivery_id TEXT PRIMARY KEY, timer_id TEXT NOT NULL REFERENCES managed_timers(timer_id),
 scheduled_at INTEGER NOT NULL, fired_at INTEGER NOT NULL, last_fired_at INTEGER NOT NULL,
 fire_count INTEGER NOT NULL, coalesced_count INTEGER NOT NULL DEFAULT 0,
 status TEXT NOT NULL CHECK(status IN ('pending','acknowledged','cancelled')), run_id TEXT
);
CREATE UNIQUE INDEX idx_managed_timer_pending ON managed_timer_deliveries(timer_id) WHERE status='pending';
CREATE INDEX idx_managed_timer_delivery_status ON managed_timer_deliveries(status,timer_id,scheduled_at);
"#;

pub(crate) fn rejected(message: &'static str) -> anyhow::Error {
    crate::runtime_error::RuntimeError::new(
        crate::runtime_error::RuntimeErrorDomain::Conflict,
        "managed_timer_conflict",
        message,
    )
    .into()
}

pub(crate) fn is_managed(id: &str) -> bool {
    id.starts_with("managed_timer_")
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManagedSchedule {
    Once {
        at: i64,
        #[serde(rename = "timeZone")]
        time_zone: String,
    },
    Calendar {
        expression: String,
        #[serde(rename = "timeZone")]
        time_zone: String,
    },
}
impl ManagedSchedule {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Once { at, time_zone } => {
                ensure!(
                    *at > 0 && *at <= 8_640_000_000_000_000,
                    rejected("invalid absolute time")
                );
                DateTime::from_timestamp_millis(*at).context("invalid absolute time")?;
                CalendarSchedule::new("0 0 * * *", time_zone)
                    .map_err(|_| rejected("invalid managed timezone"))?;
            }
            Self::Calendar {
                expression,
                time_zone,
            } => {
                CalendarSchedule::new(expression, time_zone)
                    .map_err(|_| rejected("invalid managed calendar"))?;
            }
        }
        Ok(())
    }
    pub fn preview(&self, after: i64, count: usize) -> Result<Vec<i64>> {
        self.validate()?;
        ensure!(
            (1..=10).contains(&count),
            rejected("preview count must be 1..10")
        );
        match self {
            Self::Once { at, .. } => Ok(vec![*at]),
            Self::Calendar {
                expression,
                time_zone,
            } => {
                let calendar = CalendarSchedule::new(expression, time_zone)
                    .map_err(|_| rejected("invalid managed calendar"))?;
                let mut cursor =
                    DateTime::from_timestamp_millis(after).context("invalid preview time")?;
                let mut times = Vec::with_capacity(count);
                for _ in 0..count {
                    cursor = calendar.next_after(cursor)?.utc;
                    times.push(cursor.timestamp_millis());
                }
                Ok(times)
            }
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedCreate {
    pub request_id: String,
    pub schedule_id: String,
    pub generation: u64,
    pub schedule: ManagedSchedule,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedCancel {
    pub request_id: String,
    pub schedule_id: String,
    pub generation: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedAck {
    pub timer_id: String,
    pub schedule_id: String,
    pub generation: u64,
    pub run_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedTimer {
    pub timer_id: String,
    pub agent_id: String,
    pub incarnation: u64,
    pub schedule_id: String,
    pub generation: u64,
    pub schedule: ManagedSchedule,
    pub state: TimerStatus,
    pub next_fire_at: Option<i64>,
    pub last_fired_at: Option<i64>,
    pub fire_count: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ManagedDelivery {
    pub delivery_id: String,
    pub timer_id: String,
    pub agent_id: String,
    pub incarnation: u64,
    pub schedule_id: String,
    pub generation: u64,
    pub scheduled_at: i64,
    pub fired_at: i64,
    pub last_fired_at: i64,
    pub fire_count: u64,
    pub coalesced_count: u64,
    pub state: String,
    pub run_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PendingQuery {
    pub timer_id: Option<String>,
    pub schedule_id: Option<String>,
    pub generation: Option<u64>,
    pub limit: Option<usize>,
}

pub(crate) fn token(v: &str) -> Result<()> {
    ensure!(
        !v.is_empty() && v.len() <= 200 && v.trim() == v && !v.chars().any(char::is_control),
        rejected("invalid timer identity")
    );
    Ok(())
}
pub(crate) fn fence(c: &Connection, agent: &str, inc: u64) -> Result<()> {
    ensure!(inc > 0, rejected("owner incarnation required"));
    let payload: Option<String> = c
        .query_row(
            "SELECT payload_json FROM agent_identities WHERE agent_id=?1",
            [agent],
            |r| r.get(0),
        )
        .optional()?;
    let id = super::repositories::decode_agent_identity_payload(
        &payload.ok_or_else(|| rejected("managed owner missing"))?,
    )?;
    ensure!(
        id.agent_id == agent
            && id.incarnation == inc
            && id.status == crate::types::AgentRegistryStatus::Active,
        rejected("managed owner incarnation changed")
    );
    Ok(())
}
pub(crate) fn payload<T: Serialize>(op: &str, body: &T) -> Result<String> {
    Ok(serde_json::to_string(&(op, body))?)
}
pub(crate) fn replay(
    c: &Connection,
    agent: &str,
    inc: u64,
    key: &str,
    payload: &str,
) -> Result<Option<String>> {
    fence(c, agent, inc)?;
    token(key)?;
    let row:Option<(String,String)>=c.query_row("SELECT payload_json,timer_id FROM managed_timer_requests WHERE agent_id=?1 AND incarnation=?2 AND request_id=?3",params![agent,inc,key],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    row.map(|(p, id)| {
        ensure!(
            p == payload,
            rejected("managed request key payload conflict")
        );
        Ok(id)
    })
    .transpose()
}
fn request_tx(
    tx: &Transaction<'_>,
    agent: &str,
    inc: u64,
    key: &str,
    p: &str,
    id: &str,
) -> Result<()> {
    token(key)?;
    tx.execute("INSERT INTO managed_timer_requests(agent_id,incarnation,request_id,payload_json,timer_id) VALUES(?1,?2,?3,?4,?5)",params![agent,inc,key,p,id])?;
    Ok(())
}
pub(crate) fn get(c: &Connection, agent: &str, inc: u64, id: &str) -> Result<ManagedTimer> {
    fence(c, agent, inc)?;
    let row:Option<(String,u64,String,String)>=c.query_row("SELECT m.schedule_id,m.generation,m.schedule_json,t.payload_json FROM managed_timers m JOIN timers t ON t.timer_id=m.timer_id AND t.agent_id=m.agent_id WHERE m.timer_id=?1 AND m.agent_id=?2 AND m.incarnation=?3",params![id,agent,inc],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let (schedule_id, generation, schedule, timer) =
        row.ok_or_else(|| rejected("managed timer not found"))?;
    let t: TimerRecord = serde_json::from_str(&timer)?;
    Ok(ManagedTimer {
        timer_id: t.id,
        agent_id: t.agent_id,
        incarnation: inc,
        schedule_id,
        generation,
        schedule: serde_json::from_str(&schedule)?,
        state: t.status,
        next_fire_at: t.next_fire_at.map(|t| t.timestamp_millis()),
        last_fired_at: t.last_fired_at.map(|t| t.timestamp_millis()),
        fire_count: t.fire_count,
    })
}
pub(crate) fn list(
    c: &Connection,
    agent: &str,
    inc: u64,
    limit: usize,
) -> Result<Vec<ManagedTimer>> {
    fence(c, agent, inc)?;
    ensure!((1..=100).contains(&limit), rejected("limit must be 1..100"));
    let mut s=c.prepare("SELECT timer_id FROM managed_timers WHERE agent_id=?1 AND incarnation=?2 ORDER BY timer_id LIMIT ?3")?;
    let ids = s
        .query_map(params![agent, inc, limit], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ids.iter().map(|id| get(c, agent, inc, id)).collect()
}
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub(crate) enum ManagedMutation {
    Create {
        record: TimerRecord,
        incarnation: u64,
        input: ManagedCreate,
    },
    Fire {
        expected: TimerRecord,
        record: TimerRecord,
        incarnation: u64,
        scheduled_at: i64,
        fired_at: i64,
    },
    Cancel {
        expected: TimerRecord,
        record: TimerRecord,
        incarnation: u64,
        input: ManagedCancel,
    },
}
impl ManagedMutation {
    pub(crate) fn record(&self) -> &TimerRecord {
        match self {
            Self::Create { record, .. }
            | Self::Fire { record, .. }
            | Self::Cancel { record, .. } => record,
        }
    }
    pub(crate) fn validate(&self, agent: &str, inc: u64) -> Result<()> {
        let r = self.record();
        ensure!(
            r.agent_id == agent && is_managed(&r.id),
            rejected("managed timer namespace differs")
        );
        match self {
            Self::Create {
                incarnation, input, ..
            } => {
                ensure!(
                    *incarnation == inc && r.status == TimerStatus::Active && r.fire_count == 0,
                    rejected("invalid managed create")
                );
                token(&input.request_id)?;
                token(&input.schedule_id)?;
                ensure!(
                    input.generation > 0 && input.generation <= i64::MAX as u64,
                    rejected("invalid generation")
                );
                input.schedule.validate()?;
                if let Some(s) = &input.summary {
                    ensure!(s.len() <= 2048, rejected("summary too long"));
                }
            }
            Self::Fire {
                expected,
                incarnation,
                ..
            }
            | Self::Cancel {
                expected,
                incarnation,
                ..
            } => {
                ensure!(
                    *incarnation == inc && expected.id == r.id && expected.agent_id == agent,
                    rejected("managed mutation owner differs")
                );
            }
        }
        Ok(())
    }
    pub(crate) fn recover_tx(&self, tx: &Transaction<'_>) -> Result<Option<TimerRecord>> {
        let r = self.record();
        let (inc, key, p) = match self {
            Self::Create {
                incarnation, input, ..
            } => (
                *incarnation,
                Some(input.request_id.as_str()),
                Some(payload("create", input)?),
            ),
            Self::Cancel {
                incarnation, input, ..
            } => (
                *incarnation,
                Some(input.request_id.as_str()),
                Some(payload("cancel", &(r.id.as_str(), input))?),
            ),
            Self::Fire { incarnation, .. } => (*incarnation, None, None),
        };
        fence(tx, &r.agent_id, inc)?;
        if let (Some(key), Some(p)) = (key, p) {
            let Some(id) = replay(tx, &r.agent_id, inc, key, &p)? else {
                return Ok(None);
            };
            ensure!(id == r.id, rejected("managed recovery target mismatch"));
            return super::repositories::timer_tx(tx, &id);
        }
        let actual = super::repositories::timer_tx(tx, &r.id)?;
        Ok(actual.filter(|a| a.fire_count == r.fire_count && a.last_fired_at == r.last_fired_at))
    }
    pub(crate) fn execute_tx(&self, tx: &Transaction<'_>) -> Result<TimerRecord> {
        let r = self.record();
        let inc = match self {
            Self::Create { incarnation, .. }
            | Self::Fire { incarnation, .. }
            | Self::Cancel { incarnation, .. } => *incarnation,
        };
        fence(tx, &r.agent_id, inc)?;
        match self {
            Self::Create { input, .. } => {
                let p = payload("create", input)?;
                ensure!(
                    replay(tx, &r.agent_id, inc, &input.request_id, &p)?.is_none(),
                    rejected("managed create already exists")
                );
                let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM managed_timers WHERE agent_id=?1 AND incarnation=?2 AND schedule_id=?3 AND generation=?4)",params![r.agent_id,inc,input.schedule_id,input.generation],|row|row.get(0))?;
                ensure!(
                    !exists,
                    rejected("managed schedule generation already exists")
                );
                super::repositories::upsert_timer_tx(tx, r)?;
                tx.execute("INSERT INTO managed_timers(timer_id,agent_id,incarnation,schedule_id,generation,schedule_json) VALUES(?1,?2,?3,?4,?5,?6)",params![r.id,r.agent_id,inc,input.schedule_id,input.generation,serde_json::to_string(&input.schedule)?])?;
                request_tx(tx, &r.agent_id, inc, &input.request_id, &p, &r.id)?;
            }
            Self::Fire {
                expected,
                scheduled_at,
                fired_at,
                ..
            } => {
                let actual =
                    super::repositories::timer_tx(tx, &r.id)?.context("managed timer missing")?;
                if actual != *expected || actual.status != TimerStatus::Active {
                    return Ok(actual);
                }
                let _binding = get(tx, &r.agent_id, inc, &r.id)?;
                ensure!(
                    r.fire_count == expected.fire_count + 1,
                    rejected("invalid managed fire count")
                );
                super::repositories::upsert_timer_tx(tx, r)?;
                let changed=tx.execute("UPDATE managed_timer_deliveries SET last_fired_at=?2,fire_count=?3,coalesced_count=coalesced_count+1 WHERE timer_id=?1 AND status='pending'",params![r.id,fired_at,r.fire_count])?;
                if changed == 0 {
                    let id = format!("{}:delivery:{}", r.id, r.fire_count);
                    tx.execute("INSERT INTO managed_timer_deliveries(delivery_id,timer_id,scheduled_at,fired_at,last_fired_at,fire_count,status) VALUES(?1,?2,?3,?4,?4,?5,'pending')",params![id,r.id,scheduled_at,fired_at,r.fire_count])?;
                }
            }
            Self::Cancel { input, .. } => {
                let binding = get(tx, &r.agent_id, inc, &r.id)?;
                ensure!(
                    binding.schedule_id == input.schedule_id
                        && binding.generation == input.generation,
                    rejected("managed cancel generation mismatch")
                );
                let p = payload("cancel", &(r.id.as_str(), input))?;
                if let Some(id) = replay(tx, &r.agent_id, inc, &input.request_id, &p)? {
                    ensure!(id == r.id, rejected("cancel receipt target mismatch"));
                    return super::repositories::timer_tx(tx, &r.id)?
                        .context("managed timer missing");
                }
                let mut cancelled =
                    super::repositories::timer_tx(tx, &r.id)?.context("managed timer missing")?;
                // Firing and cancellation share this transaction. Cancel the
                // latest occurrence, preserving its fire count, even if the
                // caller's qualified producer copy preceded the fire.
                cancelled.status = TimerStatus::Cancelled;
                cancelled.next_fire_at = None;
                ensure!(
                    r.status == TimerStatus::Cancelled && r.next_fire_at.is_none(),
                    rejected("invalid managed cancellation")
                );
                if binding.state == TimerStatus::Completed {
                    // Unmanaged upsert ranks Completed above Cancelled. The
                    // managed API additionally revokes an unconsumed once tick,
                    // so this narrow terminal update stays inside its prepared
                    // scheduling proof without changing unmanaged precedence.
                    crate::diagnostics::resource_io::admit_serialized_value(&cancelled)?;
                    let payload = serde_json::to_string(&cancelled)?;
                    tx.execute("UPDATE timers SET status='cancelled',next_fire_at=NULL,payload_json=?2 WHERE timer_id=?1 AND agent_id=?3 AND status='completed'",params![r.id,payload,r.agent_id])?;
                } else {
                    super::repositories::upsert_timer_tx(tx, &cancelled)?;
                }
                ensure!(
                    super::repositories::timer_tx(tx, &r.id)?.as_ref() == Some(&cancelled),
                    rejected("managed cancel canonical ACK mismatch")
                );
                tx.execute("UPDATE managed_timer_deliveries SET status='cancelled' WHERE timer_id=?1 AND status='pending'",[&r.id])?;
                request_tx(tx, &r.agent_id, inc, &input.request_id, &p, &r.id)?;
                return Ok(cancelled);
            }
        }
        Ok(r.clone())
    }
}
fn delivery(c: &Connection, agent: &str, inc: u64, id: &str) -> Result<ManagedDelivery> {
    let row:Option<(String,String,u64,i64,i64,i64,u64,u64,String,Option<String>)>=c.query_row("SELECT m.timer_id,m.schedule_id,m.generation,d.scheduled_at,d.fired_at,d.last_fired_at,d.fire_count,d.coalesced_count,d.status,d.run_id FROM managed_timer_deliveries d JOIN managed_timers m ON m.timer_id=d.timer_id WHERE d.delivery_id=?1 AND m.agent_id=?2 AND m.incarnation=?3",params![id,agent,inc],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?))).optional()?;
    let (
        timer_id,
        schedule_id,
        generation,
        scheduled_at,
        fired_at,
        last_fired_at,
        fire_count,
        coalesced_count,
        state,
        run_id,
    ) = row.ok_or_else(|| rejected("managed delivery not found"))?;
    Ok(ManagedDelivery {
        delivery_id: id.into(),
        timer_id,
        agent_id: agent.into(),
        incarnation: inc,
        schedule_id,
        generation,
        scheduled_at,
        fired_at,
        last_fired_at,
        fire_count,
        coalesced_count,
        state,
        run_id,
    })
}
pub(crate) fn pending(
    c: &Connection,
    agent: &str,
    inc: u64,
    q: &PendingQuery,
) -> Result<Vec<ManagedDelivery>> {
    fence(c, agent, inc)?;
    let limit = q.limit.unwrap_or(100);
    ensure!((1..=100).contains(&limit), rejected("limit must be 1..100"));
    ensure!(
        q.generation.is_none() || q.schedule_id.is_some(),
        rejected("generation requires scheduleId")
    );
    let mut s=c.prepare("SELECT d.delivery_id FROM managed_timer_deliveries d JOIN managed_timers m ON m.timer_id=d.timer_id JOIN timers t ON t.timer_id=m.timer_id WHERE m.agent_id=?1 AND m.incarnation=?2 AND d.status='pending' AND t.status<>'cancelled' AND (?3 IS NULL OR m.timer_id=?3) AND (?4 IS NULL OR m.schedule_id=?4) AND (?5 IS NULL OR m.generation=?5) ORDER BY d.scheduled_at,d.delivery_id LIMIT ?6")?;
    let ids = s
        .query_map(
            params![agent, inc, q.timer_id, q.schedule_id, q.generation, limit],
            |r| r.get::<_, String>(0),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ids.iter().map(|id| delivery(c, agent, inc, id)).collect()
}
pub(crate) fn ack_tx(
    tx: &Transaction<'_>,
    agent: &str,
    inc: u64,
    id: &str,
    a: &ManagedAck,
) -> Result<ManagedDelivery> {
    fence(tx, agent, inc)?;
    token(&a.run_id)?;
    let d = delivery(tx, agent, inc, id)?;
    ensure!(
        d.timer_id == a.timer_id && d.schedule_id == a.schedule_id && d.generation == a.generation,
        rejected("managed delivery generation mismatch")
    );
    ensure!(
        d.state != "cancelled",
        rejected("managed delivery cancelled")
    );
    if d.state == "acknowledged" {
        ensure!(
            d.run_id.as_deref() == Some(a.run_id.as_str()),
            rejected("managed delivery run conflict")
        );
        return Ok(d);
    }
    ensure!(
        get(tx, agent, inc, &a.timer_id)?.state != TimerStatus::Cancelled,
        rejected("managed timer cancelled")
    );
    tx.execute("UPDATE managed_timer_deliveries SET status='acknowledged',run_id=?2 WHERE delivery_id=?1 AND status='pending'",params![id,a.run_id])?;
    delivery(tx, agent, inc, id)
}
