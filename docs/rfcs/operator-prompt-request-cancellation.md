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
Claimed work is never cancelled by this endpoint. Fences have no TTL; no automatic
retention cleanup may reopen a cancelled scope during its incarnation.

Verification is synthetic: real prepared Claim and Interject before/after cancel,
concurrent claim/cancel and enqueue/cancel, reopen, late admission, exact recovery,
unrelated queue preservation, resident/current read projections and authenticated
HTTP guards. Runtime tests never run a provider turn. Main integration still
owns independent review and composition with the policy worker.

The composed runtime owns `GET /api/control/runtime/restricted-agent-capabilities`
and advertises `promptCancellation:true` there. This cancellation slice registers
no independent capability route; the main integration owns that marker.
