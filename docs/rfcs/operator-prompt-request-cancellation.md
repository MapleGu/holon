# Downstream 0007: exact operator prompt cancellation

`POST /api/control/agents/{agent_id}/prompt-cancel` requires control authentication,
`x-holon-expected-incarnation: <positive u64>`, and JSON
`{"client_request_id":"original request ID"}`. Unknown body fields are rejected.
The principal comes from the same authenticated actor as prompt admission and
0005 receipt recovery. Request IDs use control prompt's trimmed-key semantics.
No prompt content is reconstructed or resubmitted.

Every success includes `agentId`, `incarnation`, and `clientRequestId` (the submitted ID),
in addition to the following disposition fields.

Success is `{"status":"cancelled"}` before admission, or
`{"status":"cancelled","messageId":"..."}` for a queued request. An actual
Claim or Interject that won first returns
`{"status":"already_claimed","messageId":"..."}`. Missing/wrong incarnation,
invalid IDs, inconsistent/missing delivery or queue evidence, and other terminal
states fail closed through guarded errors. A different principal cannot cancel
another principal's request: it fences only its own separate request scope.

Migration 109 adds a durable cancellation ledger scoped by agent, incarnation,
principal and original delivery locator, with timestamp and optional message ID.
Pre-admission cancellation commits a fence in an immediate writer transaction.
Admission checks that fence before idempotency replay in its own queue transaction.
Queued cancellation freezes a CompareAndSet from Queued to Dropped; the final
prepared Input transaction rechecks exact delivery scope and Queued state, writes
the original Queue transition/audit, and inserts the fence atomically. Real Claim
and Interject use the same SQLite writer boundary. No agent-wide cancellation,
run abort, timer cancellation, message deletion or provider call is involved.
The accepted delivery remains historical admission evidence; its queue processing
state becomes Failed. The cancellation ledger is the authoritative cancellation
receipt. The 0005 receipt endpoint returns `cancelled` (optional `messageId`) before
consulting historical accepted delivery evidence. Late admission fails with
`prompt_request_cancelled` (409), including an unfenced late control caller.

Queued cancellation uses native prepared Queue ACKs, scheduling deltas, current
Queue/Event read qualification and resident queue removal. HTTP disconnect does
not interrupt an owned cancellation. Reopen preserves the fence and source; a
lost ACK recovers only from the exact cancellation ledger or real claim evidence.
Without the explicit 0012 option below, claimed work is never cancelled by this endpoint. Fences have no TTL; no automatic
retention cleanup may reopen a cancelled scope during its incarnation.

Verification is synthetic: real prepared Claim and Interject before/after cancel,
concurrent claim/cancel and enqueue/cancel, reopen, late admission, exact recovery,
unrelated queue preservation, resident/current read projections and authenticated
HTTP guards. Runtime tests never run a provider turn. Main integration still
owns independent review and composition with the policy worker.

The composed runtime owns `GET /api/control/runtime/restricted-agent-capabilities`
and advertises `promptCancellation:true` there. This cancellation slice registers
no independent capability route; the main integration owns that marker.


## Downstream 0012: exact claimed structured prompt abort

The existing authenticated capability endpoint additionally advertises
`claimedPromptAbort:true`. The same prompt-cancel handler accepts:

```json
{"client_request_id":"original-request","abort_claimed":{"message_id":"original-source","run_id":"observed-active-run"}}
```

The original positive `x-holon-expected-incarnation` remains required. The
option is mutually exclusive with `complete_structured_output`. Unknown fields,
null and malformed abort objects fail closed. Authentication derives the same
principal as original admission. The runtime requires a restricted
`structured_output_only` policy and an exact standalone HTTP input, current run,
source binding and open execution; Work-bound, batched/interjected, stale run,
foreign request/principal and reincarnated targets are rejected. The mutation
owner remains held while signalling the exact run's real cancellation token.
No Stop, accepted-output assertion or completion result is manufactured.

An accepted request returns `status:"abort_requested"`, `messageId`, `runId`,
`agentId`, `incarnation` and `clientRequestId`. This is an in-memory signal ACK,
not durable cancellation or settlement proof. The original native execution
observes the abort, then commits an `Aborted` Turn with reason
`structured_prompt_cancelled`, an `Aborted` queue entry, exact execution release
and settled conversation result in the existing terminal transaction. It emits
no successful output. Callers independently locate the original message under
its incarnation fence and require terminal settlement before treating it as ended.
A process exit before that commit leaves the request unconfirmed; an ACK cannot
be replayed as proof of cross-restart cancellation. Never resend the prompt.

Standalone structured turns isolate other queued inputs. A possible submit
bridge acquires an output fence under the same mutation owner before sending.
If abort wins, the tool is not sent; if submit wins, claimed abort returns
`prompt_cancel_unavailable` (409). A valid authenticated result with
`accepted:false` and a boolean `retryable` (true or false) and an absent or boolean-false complete-turn control bit releases only
that send's own fence. Acceptance, a malformed result, transport
failure or a lost reply keeps it fenced; a later rejection cannot clear an
older unknown send. Locally rejected requests acquire no output fence.
Without a declared completion tool, every restricted tool is conservatively a
possible submit. The existing 0011 accepted-result path remains separate.

Synthetic regression: `cargo test --no-default-features --test claimed_prompt_abort
--test restricted_agent_tools -- --test-threads=1`. This exercises real native
provider cancellation and terminal readback, exact identity and stale request
rejection, independent queued sources, in-flight bridge acceptance, explicit
retryable and final rejection, and retained unknown fences. No real models or external
systems are used.
