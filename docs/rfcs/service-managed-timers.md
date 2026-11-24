# Service-managed timer delivery

Managed timers use the existing timer producer, CalendarSchedule parser, restart recovery, mutation owner and qualified scheduling snapshot publication. They add no due scheduler and no inference route.

Migration 107 adds only managed_timers, managed_timer_requests and managed_timer_deliveries plus their indexes. TimerRecord and unmanaged TimerTick/WaitFor behavior retain their existing shape. Managed timer IDs have a reserved managed_timer_ prefix, so recovery and firing select managed semantics without a hot-path metadata lookup or changes to the existing TimerRecord wire/fixture contract. A missing or wrong-owner managed binding fails closed inside the firing transaction.

A managed prepared transition commits the existing timer row and its delivery or request receipt in one SQLite transaction, then publishes the original scheduling snapshot delta. It never creates a Message, timer_wake or Queue entry. At most one unacknowledged delivery is pending per native timer; additional fires update its count and last-fired time. Calendar misfires use the existing next-after-actual-fire policy, rather than replaying every missed instant. Once schedules retain an absolute instant and may fire once after late recovery.

The control-only create/get/list/cancel/preview/pending/ack routes require the current active owner incarnation. Create and cancel retain original-key payload receipts, and each product schedule/generation has a unique native mapping. Cancellation also revokes completed-once pending deliveries. Ack binds one exact owner/timer/schedule/generation/delivery to an immutable Service run ID; wrong-run replay and cancelled pending delivery are rejected.

The Service persists delivery-to-run association before ack and executes via the existing executor with a fresh bridge. Product authorization, generation checks, run serialization, per-tool checks and result-delivery fences remain Service responsibilities. Native ack is consumption evidence, not business execution permission or success. An already-running model may require cleanup after cancellation.

Synthetic tests cover stable request replay, rollback and committed-cache-loss recovery, restart/misfire/coalescing, cancellation races, exact ack ownership, parser preview/DST parity and absence of model queue entries. Native control readbacks are the scheduling truth; SSE/global hints are not required for durable delivery recovery.

Unmanaged timer upsert preserves its terminal precedence (Completed above Cancelled). Managed cancellation has one narrow extra case: inside the same prepared timer transaction it may mark a completed once timer Cancelled to revoke the still-pending delivery. It preserves all occurrence fields and independently reads back the canonical cancelled record before storing the request receipt. This does not change unmanaged cancellation or generic timer merge rules.

Exact prompt recovery uses the existing operator delivery ledger. A control-only GET shares POST's principal/target/key locator and reads one fenced snapshot; it returns only accepted(message ID), not_found or unknown. No semantic prompt content is reconstructed or sent, and historical receipts without a matching expected incarnation cannot become accepted evidence for a new owner.
