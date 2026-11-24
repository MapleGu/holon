//! Managed timers use the original mutation owner, timer loop and recovery.
use super::*;
use crate::runtime_db::managed_timers::{
    self as managed, ManagedCancel, ManagedCreate, ManagedMutation, ManagedSchedule, ManagedTimer,
};
use anyhow::ensure;
use sha2::{Digest, Sha256};

impl RuntimeHandle {
    pub async fn create_managed_timer(
        &self,
        incarnation: u64,
        input: ManagedCreate,
    ) -> Result<ManagedTimer> {
        ensure!(
            self.instance_incarnation() == incarnation,
            managed::rejected("managed owner incarnation changed")
        );
        input.schedule.validate()?;
        managed::token(&input.request_id)?;
        managed::token(&input.schedule_id)?;
        ensure!(
            input.generation > 0 && input.generation <= i64::MAX as u64,
            managed::rejected("invalid generation")
        );
        let admission = self.execution_admission_lease()?;
        let runtime = self.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.spawn_owned_mutation(async move {
            let _admission = admission;
            let result = runtime.create_managed_timer_owned(incarnation, input).await;
            let _ = tx.send(result);
        })?;
        rx.await
            .context("managed timer scheduling worker stopped")?
    }
    async fn create_managed_timer_owned(
        &self,
        incarnation: u64,
        input: ManagedCreate,
    ) -> Result<ManagedTimer> {
        let mut mutation = self.begin_mutation().await?;
        let agent = mutation.state.id.clone();
        let db = self.runtime_db();
        let a = agent.clone();
        let i = input.clone();
        let replay = db
            .with_read_async(move |c| {
                let tx = c.transaction()?;
                managed::replay(
                    &tx,
                    &a,
                    incarnation,
                    &i.request_id,
                    &managed::payload("create", &i)?,
                )
            })
            .await?;
        if let Some(id) = replay {
            let read = mutation
                .prepare_timer_mutation_read(&id, None, false)
                .await?
                .context("managed receipt timer missing")?;
            drop(mutation);
            self.spawn_timer_loop(read.timer).await?;
            return self.read_managed_timer(incarnation, &id).await;
        }
        let now = self.now();
        let (next, calendar) = match &input.schedule {
            ManagedSchedule::Once { at, .. } => (
                chrono::DateTime::from_timestamp_millis(*at)
                    .context("invalid absolute timer time")?,
                None,
            ),
            ManagedSchedule::Calendar {
                expression,
                time_zone,
            } => {
                let c = crate::calendar::CalendarSchedule::new(expression, time_zone)
                    .map_err(|_| managed::rejected("invalid managed calendar"))?;
                (
                    c.next_after(now)
                        .map_err(|_| {
                            managed::rejected("managed calendar has no future occurrence")
                        })?
                        .utc,
                    Some(c),
                )
            }
        };
        let id = format!(
            "managed_timer_{:x}",
            Sha256::digest(serde_json::to_vec(&(
                &agent,
                incarnation,
                &input.request_id
            ))?)
        );
        let record = TimerRecord {
            id: id.clone(),
            agent_id: agent,
            created_at: now,
            duration_ms: (next - now).num_milliseconds().max(0) as u64,
            interval_ms: None,
            repeat: calendar.is_some(),
            status: TimerStatus::Active,
            summary: input.summary.clone(),
            next_fire_at: Some(next),
            last_fired_at: None,
            fire_count: 0,
            calendar,
        };
        let command = ManagedMutation::Create {
            record,
            incarnation,
            input,
        };
        let timer = self.commit_managed_timer(&mut mutation, command).await?;
        drop(mutation);
        self.spawn_timer_loop(timer).await?;
        self.read_managed_timer(incarnation, &id).await
    }
    pub async fn read_managed_timer(&self, incarnation: u64, id: &str) -> Result<ManagedTimer> {
        let agent = self.agent_id().await?;
        let id = id.to_owned();
        self.runtime_db()
            .with_read_async(move |c| {
                let tx = c.transaction()?;
                managed::get(&tx, &agent, incarnation, &id)
            })
            .await
    }
    pub async fn cancel_managed_timer(
        &self,
        incarnation: u64,
        id: String,
        input: ManagedCancel,
    ) -> Result<ManagedTimer> {
        ensure!(
            self.instance_incarnation() == incarnation,
            managed::rejected("managed owner incarnation changed")
        );
        managed::token(&input.request_id)?;
        managed::token(&input.schedule_id)?;
        let admission = self.execution_admission_lease()?;
        let runtime = self.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.spawn_owned_mutation(async move {
            let _admission = admission;
            let result = runtime
                .cancel_managed_timer_owned(incarnation, id, input)
                .await;
            let _ = tx.send(result);
        })?;
        rx.await.context("managed cancellation worker stopped")?
    }
    async fn cancel_managed_timer_owned(
        &self,
        incarnation: u64,
        id: String,
        input: ManagedCancel,
    ) -> Result<ManagedTimer> {
        let mut mutation = self.begin_mutation().await?;
        let read = mutation
            .prepare_timer_mutation_read(&id, None, false)
            .await?
            .context("managed timer not found")?;
        let _material = read.material;
        let mut record = read.timer.clone();
        record.status = TimerStatus::Cancelled;
        record.next_fire_at = None;
        let command = ManagedMutation::Cancel {
            expected: read.timer,
            record,
            incarnation,
            input,
        };
        self.commit_managed_timer(&mut mutation, command).await?;
        drop(mutation);
        // Interrupt the original due sleep; cancellation never starts another scheduler.
        for lease in self.inner.control_cancellations.timer_for(&id) {
            lease.cancel();
        }
        self.read_managed_timer(incarnation, &id).await
    }
    async fn commit_managed_timer(
        &self,
        mutation: &mut super::state_mutation::RuntimeMutation,
        command: ManagedMutation,
    ) -> Result<TimerRecord> {
        let original = command.clone();
        let prepared = crate::runtime_db::transitions::PreparedInputTransition::timer(
            mutation.input_evidence_scope(),
            crate::ids::audit_event_id(),
            self.now(),
            crate::runtime_db::transitions::PreparedTimerTransition::managed(
                command,
                self.take_transition_fault(),
            ),
        )?;
        match mutation.commit_prepared_timer_transition(prepared).await {
            Ok((
                commit,
                crate::runtime_db::transitions::TimerMutationOutcome::Managed { record },
            )) => {
                self.apply_transition_commit(commit).await;
                Ok(record)
            }
            Ok(_) => anyhow::bail!("managed timer returned another outcome"),
            Err(error) => {
                // Reconciliation may also see a concurrent timer fire. Only
                // this command's own exact request receipt proves its commit.
                mutation.reconcile_known_commit().await?;
                let recovered = self
                    .runtime_db()
                    .with_read_async(move |c| {
                        let tx = c.transaction()?;
                        original.recover_tx(&tx)
                    })
                    .await?;
                recovered.ok_or(error)
            }
        }
    }
    pub(super) async fn fire_managed_timer_record(&self, timer: &mut TimerRecord) -> Result<()> {
        let mut mutation = self.begin_mutation().await?;
        let read = mutation
            .prepare_timer_mutation_read(&timer.id, Some(timer), false)
            .await?
            .context("managed timer missing")?;
        let _material = read.material;
        let expected = read.timer;
        if expected.status != TimerStatus::Active {
            // A completed once timer can subsequently be cancelled to revoke its
            // pending delivery. Reconcile that terminal copy through the same
            // canonical transaction rather than treating local completion as truth.
            let now = self.now().timestamp_millis();
            let command = ManagedMutation::Fire {
                record: expected.clone(),
                expected,
                incarnation: self.instance_incarnation(),
                scheduled_at: now,
                fired_at: now,
            };
            *timer = self.commit_managed_timer(&mut mutation, command).await?;
            return Ok(());
        }
        // Multiple stale timer loop copies must not advance the same occurrence twice.
        if timer.fire_count != expected.fire_count || timer.next_fire_at != expected.next_fire_at {
            *timer = expected;
            return Ok(());
        }
        let now = self.now();
        let scheduled = expected
            .next_fire_at
            .context("active managed timer has no due time")?;
        if scheduled > now {
            *timer = expected;
            return Ok(());
        }
        let mut record = expected.clone();
        record.fire_count = record
            .fire_count
            .checked_add(1)
            .context("managed fire count overflow")?;
        record.last_fired_at = Some(now);
        if let Some(c) = &record.calendar {
            record.next_fire_at = Some(
                c.next_after(now)
                    .map_err(|_| managed::rejected("managed calendar has no future occurrence"))?
                    .utc,
            );
        } else {
            record.status = TimerStatus::Completed;
            record.next_fire_at = None;
        }
        let command = ManagedMutation::Fire {
            expected,
            record,
            incarnation: self.instance_incarnation(),
            scheduled_at: scheduled.timestamp_millis(),
            fired_at: now.timestamp_millis(),
        };
        *timer = self.commit_managed_timer(&mut mutation, command).await?;
        // Deliberately no Message/Queue/notify_scheduler/provider execution.
        Ok(())
    }
}

#[cfg(all(test, holon_lib_test_shard = "a"))]
#[path = "managed_timers_tests.rs"]
mod tests;
