use super::*;
use crate::runtime::clock::TestClock;
use crate::runtime::tests::support::{
    context_config, controlled_clock, tempdir, StubProvider, TempDir,
};
use crate::runtime_db::managed_timers::{
    self as m, ManagedAck, ManagedCancel, ManagedCreate, ManagedSchedule, PendingQuery,
};
use std::time::Duration;

async fn open(
    home: &std::path::Path,
    workspace: &std::path::Path,
    clock: Arc<TestClock>,
) -> Result<RuntimeHandle> {
    let r = RuntimeHandle::new_with_clock(
        "default",
        home.into(),
        workspace.to_path_buf(),
        "http://synthetic.invalid".into(),
        Arc::new(StubProvider::new("must never execute")),
        "default".into(),
        context_config(),
        clock,
    )?;
    r.join_resource_worker().await?;
    Ok(r)
}
async fn fixture() -> Result<(TempDir, TempDir, Arc<TestClock>, RuntimeHandle)> {
    let h = tempdir()?;
    let w = tempdir()?;
    let c = controlled_clock();
    let r = open(h.path(), w.path(), c.clone()).await?;
    Ok((h, w, c, r))
}
fn input(clock: &TestClock, calendar: bool) -> ManagedCreate {
    ManagedCreate {
        request_id: "original-create".into(),
        schedule_id: "product-schedule".into(),
        generation: 1,
        summary: Some("synthetic task reference".into()),
        schedule: if calendar {
            ManagedSchedule::Calendar {
                expression: "* * * * *".into(),
                time_zone: "UTC".into(),
            }
        } else {
            ManagedSchedule::Once {
                at: clock.now().timestamp_millis() + 60_000,
                time_zone: "Asia/Shanghai".into(),
            }
        },
    }
}
async fn stop_loop(r: &RuntimeHandle, id: &str) -> Result<()> {
    for l in r.inner.control_cancellations.timer_for(id) {
        l.cancel();
        l.join().await?;
    }
    Ok(())
}
async fn record(r: &RuntimeHandle, id: &str) -> Result<TimerRecord> {
    Ok(r.acquire_timer_record_read(id)
        .await?
        .record()?
        .context("timer missing")?
        .clone())
}
fn pending(r: &RuntimeHandle) -> Result<Vec<m::ManagedDelivery>> {
    r.runtime_db().with_read(|c| {
        let tx = c.transaction()?;
        m::pending(
            &tx,
            "default",
            r.instance_incarnation(),
            &PendingQuery::default(),
        )
    })
}
fn ack(d: &m::ManagedDelivery, run: &str) -> ManagedAck {
    ManagedAck {
        timer_id: d.timer_id.clone(),
        schedule_id: d.schedule_id.clone(),
        generation: d.generation,
        run_id: run.into(),
    }
}
fn cancel() -> ManagedCancel {
    ManagedCancel {
        request_id: "original-cancel".into(),
        schedule_id: "product-schedule".into(),
        generation: 1,
    }
}
fn queue_count(r: &RuntimeHandle) -> Result<u64> {
    r.runtime_db()
        .with_read(|c| Ok(c.query_row("SELECT COUNT(*) FROM queue_entries", [], |row| row.get(0))?))
}

#[tokio::test(start_paused = true)]
async fn managed_create_replay_conflict_owner_and_stable_generation() -> Result<()> {
    let (_h, _w, c, r) = fixture().await?;
    let i = input(&c, false);
    let inc = r.instance_incarnation();
    let a = r.create_managed_timer(inc, i.clone()).await?;
    let b = r.create_managed_timer(inc, i.clone()).await?;
    assert_eq!(a, b);
    let mut other = i.clone();
    other.summary = Some("different".into());
    assert!(r.create_managed_timer(inc, other).await.is_err());
    let mut duplicate = i.clone();
    duplicate.request_id = "another-key".into();
    assert!(r.create_managed_timer(inc, duplicate).await.is_err());
    assert!(r.create_managed_timer(inc + 1, i.clone()).await.is_err());
    assert!(r.read_managed_timer(inc + 1, &a.timer_id).await.is_err());
    assert_eq!(
        r.runtime_db()
            .with_read(|c| {
                let tx = c.transaction()?;
                m::list(&tx, "default", inc, 100)
            })?
            .len(),
        1
    );
    assert_eq!(queue_count(&r)?, 0);
    let mut wrong = cancel();
    wrong.generation = 2;
    assert!(r
        .cancel_managed_timer(inc, a.timer_id.clone(), wrong)
        .await
        .is_err());
    let mut wrong = cancel();
    wrong.request_id = i.request_id;
    assert!(r
        .cancel_managed_timer(inc, a.timer_id.clone(), wrong)
        .await
        .is_err());
    assert_eq!(
        r.read_managed_timer(inc, &a.timer_id).await?.state,
        TimerStatus::Active
    );
    r.cancel_managed_timer(inc, a.timer_id, cancel()).await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn managed_once_absolute_delivery_survives_restart_and_ack_exact_run() -> Result<()> {
    let (h, w, c, r) = fixture().await?;
    let inc = r.instance_incarnation();
    let i = input(&c, false);
    let at = match i.schedule {
        ManagedSchedule::Once { at, .. } => at,
        _ => unreachable!(),
    };
    let t = r.create_managed_timer(inc, i).await?;
    assert_eq!(t.next_fire_at, Some(at));
    stop_loop(&r, &t.timer_id).await?;
    c.advance(Duration::from_secs(3600));
    let mut original = record(&r, &t.timer_id).await?;
    r.fire_timer_record(&mut original).await?;
    assert_eq!(original.status, TimerStatus::Completed);
    assert_eq!(original.fire_count, 1);
    let d = pending(&r)?.remove(0);
    assert_eq!(d.scheduled_at, at);
    assert_eq!(queue_count(&r)?, 0);
    assert!(r.runtime_db().timers().pending_wake(&t.timer_id)?.is_none());
    drop(r);
    let r = open(h.path(), w.path(), c).await?;
    assert_eq!(pending(&r)?, vec![d.clone()]);
    let mut wrong = ack(&d, "run-1");
    wrong.generation = 2;
    assert!(r
        .runtime_db()
        .transaction(|tx| m::ack_tx(tx, "default", inc, &d.delivery_id, &wrong))
        .is_err());
    assert!(r
        .runtime_db()
        .transaction(|tx| m::ack_tx(tx, "default", inc + 1, &d.delivery_id, &ack(&d, "run-1")))
        .is_err());
    let a = r
        .runtime_db()
        .transaction(|tx| m::ack_tx(tx, "default", inc, &d.delivery_id, &ack(&d, "run-1")))?;
    assert_eq!(a.state, "acknowledged");
    assert_eq!(
        r.runtime_db().transaction(|tx| m::ack_tx(
            tx,
            "default",
            inc,
            &d.delivery_id,
            &ack(&d, "run-1")
        ))?,
        a
    );
    assert!(r
        .runtime_db()
        .transaction(|tx| m::ack_tx(tx, "default", inc, &d.delivery_id, &ack(&d, "another-run")))
        .is_err());
    assert!(pending(&r)?.is_empty());
    assert_eq!(queue_count(&r)?, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn managed_missed_calendar_coalesces_one_pending_and_stale_loop_cannot_duplicate(
) -> Result<()> {
    let (_h, _w, c, r) = fixture().await?;
    let inc = r.instance_incarnation();
    let i = input(&c, true);
    let preview = i.schedule.preview(c.now().timestamp_millis(), 3)?;
    let t = r.create_managed_timer(inc, i.clone()).await?;
    assert_eq!(t.next_fire_at, Some(preview[0]));
    stop_loop(&r, &t.timer_id).await?;
    let mut current = record(&r, &t.timer_id).await?;
    let mut stale = current.clone();
    c.advance(Duration::from_secs(3 * 86400 + 15));
    r.fire_timer_record(&mut current).await?;
    assert_eq!(current.fire_count, 1);
    assert_eq!(
        current.next_fire_at.unwrap().timestamp_millis(),
        i.schedule.preview(c.now().timestamp_millis(), 1)?[0]
    );
    let first = pending(&r)?.remove(0);
    r.fire_timer_record(&mut stale).await?;
    assert_eq!(pending(&r)?, vec![first.clone()]);
    c.advance(Duration::from_secs(120));
    r.fire_timer_record(&mut current).await?;
    let d = pending(&r)?.remove(0);
    assert_eq!(d.delivery_id, first.delivery_id);
    assert_eq!(d.coalesced_count, 1);
    assert_eq!(d.fire_count, 2);
    assert_eq!(d.scheduled_at, first.scheduled_at);
    assert!(d.last_fired_at > d.fired_at);
    assert_eq!(queue_count(&r)?, 0);
    r.cancel_managed_timer(inc, t.timer_id, cancel()).await?;
    assert!(pending(&r)?.is_empty());
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn managed_overdue_restart_uses_original_native_timer_recovery() -> Result<()> {
    let (h, w, c, r) = fixture().await?;
    let inc = r.instance_incarnation();
    let t = r.create_managed_timer(inc, input(&c, true)).await?;
    stop_loop(&r, &t.timer_id).await?;
    drop(r);
    c.advance(Duration::from_secs(7 * 86400));
    let r = open(h.path(), w.path(), c).await?;
    let selected = r
        .inner
        .recovered_timers
        .lock()
        .await
        .take()
        .context("recovery omitted timers")?;
    r.recover_active_timers(selected).await?;
    let d = pending(&r)?;
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].fire_count, 1);
    assert_eq!(r.read_managed_timer(inc, &t.timer_id).await?.fire_count, 1);
    assert_eq!(queue_count(&r)?, 0);
    r.cancel_managed_timer(inc, t.timer_id, cancel()).await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn managed_cancel_racing_due_revokes_pending_and_late_ack_then_replays_same_key() -> Result<()>
{
    let (_h, _w, c, r) = fixture().await?;
    let inc = r.instance_incarnation();
    let i = input(&c, false);
    let t = r.create_managed_timer(inc, i.clone()).await?;
    stop_loop(&r, &t.timer_id).await?;
    let mut timer = record(&r, &t.timer_id).await?;
    c.advance(Duration::from_secs(120));
    let fire = r.fire_timer_record(&mut timer);
    let cancellation = r.cancel_managed_timer(inc, t.timer_id.clone(), cancel());
    let (f, c) = tokio::join!(fire, cancellation);
    f?;
    let cancelled = c?;
    assert_eq!(cancelled.state, TimerStatus::Cancelled);
    assert!(pending(&r)?.is_empty());
    assert_eq!(
        r.cancel_managed_timer(inc, t.timer_id.clone(), cancel())
            .await?,
        cancelled
    );
    assert_eq!(r.create_managed_timer(inc, i).await?, cancelled);
    let a = ManagedAck {
        timer_id: t.timer_id.clone(),
        schedule_id: t.schedule_id,
        generation: 1,
        run_id: "late-run".into(),
    };
    let delivery_id = format!("{}:delivery:1", t.timer_id);
    assert!(r
        .runtime_db()
        .transaction(|tx| m::ack_tx(tx, "default", inc, &delivery_id, &a))
        .is_err());
    r.fire_timer_record(&mut timer).await?;
    assert_eq!(timer.status, TimerStatus::Cancelled);
    assert_eq!(queue_count(&r)?, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn managed_cancel_completed_once_and_rollback_unknown_same_key_recovery() -> Result<()> {
    let (_h, _w, c, r) = fixture().await?;
    let inc = r.instance_incarnation();
    let i = input(&c, false);
    r.inject_next_transition_fault(
        crate::runtime_db::transitions::TransitionFaultPoint::AfterCanonicalWrites,
    );
    assert!(r.create_managed_timer(inc, i.clone()).await.is_err());
    assert!(r
        .runtime_db()
        .with_read(|c| {
            let tx = c.transaction()?;
            m::list(&tx, "default", inc, 100)
        })?
        .is_empty());
    let t = r.create_managed_timer(inc, i.clone()).await?;
    stop_loop(&r, &t.timer_id).await?;
    c.advance(Duration::from_secs(120));
    let mut timer = record(&r, &t.timer_id).await?;
    r.fire_timer_record(&mut timer).await?;
    let d = pending(&r)?.remove(0);
    r.inject_next_transition_fault(
        crate::runtime_db::transitions::TransitionFaultPoint::AfterCanonicalWrites,
    );
    assert!(r
        .cancel_managed_timer(inc, t.timer_id.clone(), cancel())
        .await
        .is_err());
    assert_eq!(pending(&r)?.len(), 1);
    r.cancel_managed_timer(inc, t.timer_id.clone(), cancel())
        .await?;
    assert!(pending(&r)?.is_empty());
    assert!(r
        .runtime_db()
        .transaction(|tx| m::ack_tx(tx, "default", inc, &d.delivery_id, &ack(&d, "late")))
        .is_err());
    assert_eq!(r.create_managed_timer(inc, i).await?.timer_id, t.timer_id);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn managed_after_commit_cache_ack_loss_keeps_one_timer_and_live_producer() -> Result<()> {
    let (_h, _w, c, r) = fixture().await?;
    let inc = r.instance_incarnation();
    let i = input(&c, false);
    r.inject_next_transition_fault(
        crate::runtime_db::transitions::TransitionFaultPoint::BeforeCacheUpdate,
    );
    // Native original commit reconciliation may return success or UNKNOWN. Only same-key recovery is allowed.
    let _unknown = r.create_managed_timer(inc, i.clone()).await;
    let t = r.create_managed_timer(inc, i).await?;
    assert_eq!(
        r.runtime_db()
            .with_read(|c| {
                let tx = c.transaction()?;
                m::list(&tx, "default", inc, 100)
            })?
            .len(),
        1
    );
    assert_eq!(
        r.inner.control_cancellations.timer_for(&t.timer_id).len(),
        1
    );
    r.cancel_managed_timer(inc, t.timer_id, cancel()).await?;
    Ok(())
}

#[test]
fn managed_native_preview_uses_same_parser_across_dst_and_invalid_zones() -> Result<()> {
    for (expression, after) in [
        ("30 2 * * *", "2026-03-08T05:00:00Z"),
        ("30 1 * * *", "2026-11-01T04:00:00Z"),
    ] {
        let s = ManagedSchedule::Calendar {
            expression: expression.into(),
            time_zone: "America/New_York".into(),
        };
        let after = chrono::DateTime::parse_from_rfc3339(after)?.with_timezone(&chrono::Utc);
        let times = s.preview(after.timestamp_millis(), 3)?;
        let c = crate::calendar::CalendarSchedule::new(expression, "America/New_York")?;
        let mut cursor = after;
        for time in times {
            let next = c.next_after(cursor)?;
            assert_eq!(time, next.utc.timestamp_millis());
            assert!(time > cursor.timestamp_millis());
            cursor = next.utc;
        }
    }
    assert!(ManagedSchedule::Calendar {
        expression: "* * * * *".into(),
        time_zone: "Missing/Zone".into()
    }
    .preview(1_700_000_000_000, 3)
    .is_err());
    Ok(())
}
