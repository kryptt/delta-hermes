# Delta CLI plugin contract (ACP v2) → Hermes bridge

From Delta's "build-a-plugin" docs + the `zed-industries/delta-claude` reference (AGPL).
Drive EVERYTHING through the `delta` CLI; never read/write Delta's DB directly.

## Four plugin responsibilities
1. Translate the agent's activity into ACP records and send them to Delta.
2. Save records until Delta confirms them (outbox + retry).
3. Give the agent the activity it receives from teammates.
4. Stop the agent's turn when someone else takes over the thread.

## Record stream — `delta --json thread append <thread> --source <plugin> --external-id <session>`
JSONL on stdin; each row `{schemaVersion:1, sequence:N, turnSequence?, update:{sessionUpdate,...}}`.
`--producer-name <Name>` is the display identity of the agent's output (defaults to `--source`).

Supported `sessionUpdate`: `user_message`, `agent_message`, `agent_thought`,
`tool_call_update`, `state_update` (`running`/`idle`), `session_info_update` (sets title).
Streaming chunks are REJECTED — re-send the same `messageId` with full content so far.

## Delivery-mode findings (verified 2026-11-11 against live CLI 0.24.2)
- **Use `--acknowledge local` for append — the server-ack path is unreliable here.**
  With the default (`--acknowledge server`), `delta thread append` fails with
  `{"error":{"code":"failed","reason":"remote","message":"disconnected","retryable":true}}`
  (and `thread status` can too, intermittently, even while `thread list`/`share`/HTTP work).
  `--acknowledge local` records locally and uploads in the background; receipts come back the
  same way. This is also the contract-documented offline path — always use it.
- **A thread created via CLI is `local_only` with `upload: queued` until shared** —
  `thread share <id> --visibility organization` (or invite) is what uploads it.
  Appending to an unshared/local-only thread fails with
  `not_visible: the history is unavailable: NotFound`. In the normal flow (humans create
  threads in the Delta app, Hermes joins) threads are already shared, so this only bites
  throwaway/CLI-created test threads.
- **Append receipts (stdout, one JSON per record, in order):**
  `{"operation":"appended","thread_id","sequence":N,"next_sequence":N+1,"duplicate":bool,
  "disposition":"recorded"|"duplicate"|"fenced","turn_sequence":T,"version"}`.
  Re-sending an already-recorded sequence returns `disposition:"duplicate"` — treat as
  acknowledged (outbox can safely re-send; sequences dedupe server-side). `next_sequence` in
  receipts advances the local producer counter.
- **Daemon flakiness is transient and retryable; `delta` commands spawn `delta-app` per call.**
  Stale `thread watch` subprocesses accumulate across plugin reloads — kill by PID
  (`pgrep -f 'thread watc[h]'`) before re-enabling, or watches pile up on old threads.
- **delta-claude is AGPL** — port concepts/protocol, do not copy code.

## Other key commands
- `thread create --source … --external-id <session>` — idempotent: same two IDs ⇒ same thread.
- `worktree create $PWD` → `reference`; `worktree attach <ref> --thread <id>` (no file upload);
  `worktree mount <ref> $PWD --existing-checkout` keeps running and syncs both ways — `kill -TERM` to stop.
- `thread share <id> --visibility organization` | `thread invite <id> <email>…`
  (invite uploads privately if not yet uploaded; one failed address doesn't fail the command).
- `thread status --source --external-id` — access, attached worktrees, NEXT sequence number.
- `thread watch <thread> --source --external-id --participate <Name>` — replaces/extends items
  (`item`, `item.append`, `remove`, ordered by `after_id`; `turn`/`presence`/`worktrees` replace;
  `producer.turn` reports your turn incl. supersession; `comment`; `children` arrives after `live`).
  Omit `--source/--external-id` to see everything; `--participate` shows presence while it runs.
- `thread mount <thread> --owner <session>` — join a shared thread: creates checkouts, prints
  ready line with the directory; keep running.
- `thread send DEST --from SRC "msg"` — agent→agent; `--from` defaults to `DELTA_CURRENT_THREAD_ID`.
- `thread stop <thread>` — end the current turn without starting another.

## Turn/scoping semantics
- A live turn: `state_update running` with `turnSequence == sequence`, then content + final
  `idle` all with that same `turnSequence`. A `state_update` without `turnSequence` = history.
- Sequences are monotonic per thread; recover the next from `thread status`; never resend.
- Keep a disk outbox (JSONL) + delivery retry/backoff; `--acknowledge local` for offline
  (create with `--organization <id> --visibility private`; uploads in background; server
  rejection after offline changes ⇒ `reload_required` → `thread fork` then `thread discard --force`).
- If the agent process exits mid-turn, Delta ends the turn when the watch ends — a crash
  doesn't leave the thread wedged.

## Hermes hook → ACP mapping — VERIFIED against the live build (2026-11-11)
Verified with the `delta-hook-probe` plugin (`/opt/data/plugins/delta-hook-probe`, sanitized
log at `/opt/data/cache/scratch/delta-hook-probe.jsonl`); payload field names below were
observed in the running build, not just the docs. Enable reasoning deltas with
`plugins.stream_reasoning_deltas: true` (already set here).

| ACP record | Hermes plugin hook (observed fields) |
|---|---|
| `user_message` + turn `running` | **`post_gateway_admission`** (`session_key`, `platform`, `source`, `message_id`, `text`) — fires for gateway non-internal messages (Telegram/Discord/Slack/email/webhook/A2A), NEVER for CLI/TUI/Desktop/API turns. NOTE: `on_human_input_resolved` is NOT the user-message event — it only fires when the agent blocks on a person (`kind` `sudo`/`clarify`/`approval`, `request_id`, `prompt` redacted, `outcome`). |
| `agent_message` (streaming) | `on_interim_message` (`text`, `already_streamed`, `turn_id`, `iteration`, `session_id`, `model`, `provider`, `surface`) / `on_stream_delta` (`delta`, `kind`, …) / `post_llm_call` (`assistant_response`, `user_message`, `conversation_history`, `turn_id`, `model`, `platform`) |
| `tool_call_update` (name,args,result) | `pre_tool_call` (`tool_name`, `args`, `tool_call_id`, `conversation_id`, `turn_id`, `api_request_id`) + `post_tool_call` (adds `result`, `status`, `duration_ms`, `error_type`, `error_message`, `middleware_trace`) |
| `agent_thought` | `on_stream_delta` with `kind="reasoning"` (opt-in) — observed 699 reasoning vs 39 text deltas on one turn; also `post_api_request` (`assistant_message`, `usage`) |
| turn `idle` / session end | `on_session_end` (`completed`/`failed`/`interrupted`, `turn_exit_reason`, `turn_id`, `task_id`, `model`, `platform`) / `agent_loop_stopped` (`session_key`, `platform`, `reason` — interrupts only) / `on_session_finalize` (`session_id`, `platform`, `reason`) |
| session identity | `on_session_start` (`session_id`, `model`, `platform`) / `on_session_open` (`session_id`, `platform`, `reason`=`create`\|`resume`) — use `session_id` as the ACP `external-id` |
| title / search summary | `ctx.llm.complete_structured` (mirrors delta-claude's PostTurn summarize) |
| RECEIVE: remote items → context, no turn | `ctx.inject_message(content, role=…)` |

Not yet observed (documented-only, capture opportunistically): `post_gateway_admission`
(via an inbound email/A2A message), `on_human_input_request`/`resolved` (a `clarify` or
approval prompt), `agent_loop_stopped` (`/stop`), `on_session_reset` (`/new`).

delta-claude records by reading Claude's settled transcript; Hermes is event-driven —
reconstruct the SAME records live from hook payloads. Re-run the probe after any Hermes
upgrade before trusting the field names again.

## Known gaps (accepted)
- Hermes has NO in-flight abort hook ⇒ supersession is COOPERATIVE only: gate new tool
  calls/turns on `producer.turn superseded` / current-turn `fenced` from the watch; you
  cannot stop a tool that already started.
- Both contracts are beta — the `thread contract` guard is mandatory.
- delta-claude's host-specific pieces (/clear, transcript API, trust prompt, folder select)
  must be replaced by Hermes session/hook/CLI APIs; the ACP/CLI surface ports cleanly.
