# delta-bridge

A [Hermes](https://hermes-agent.nousresearch.com) plugin that live-bridges a
Hermes agent session into a [Delta](https://delta.dev) (delta.dev / Zed)
thread as an ACP producer — the **"bring your own harness"** seam: Hermes
does **not** run inside Delta as an LLM; it participates as a peer agent
through the Delta CLI's recording protocol.

- **Sender:** Hermes turn activity (user messages, assistant text, reasoning,
  tool calls) becomes ACP schema-1 records appended to a Delta thread via
  `delta thread append`.
- **Receiver:** a supervised `delta thread watch` feeds teammate activity
  back; the agent stays in **standby** and only gets a turn when it is
  mentioned/assigned (@Hermes).
- **Reliability:** a disk outbox with monotonic per-thread sequences,
  receipt parsing, retry, and duplicate-safe re-sends (`disposition:
  "duplicate"` receipts are treated as acknowledged).

## Requirements

- Hermes agent (plugin hooks + slash commands; tested on v0.23.x)
- [Delta CLI](https://delta.dev) ≥ 0.24.x on PATH, `~/.local/bin/delta`,
  or at `$DELTA_BIN`. The plugin checks the CLI contract at runtime
  (`delta --json thread contract` → min/max) — see `docs/plugin-contract.md`.
- Signed in to Delta (`delta auth login`, device flow) as the account that
  should own the recorded threads.

## Install

```bash
# from the published repo:
hermes plugins install kryptt/delta-hermes
hermes plugins enable delta-bridge
```

The plugin does nothing until a session is explicitly bridged:

```
/delta-join <thread-id-or-delta-link>   # bind this session to a thread
/delta-status                           # binding, watch, outbox state
/delta-leave                            # unbind (pending outbox is kept)
```

## How it works

```
Hermes session ──hooks──▶ ACP records ──▶ disk outbox ──▶ delta thread append
     ▲                                                       │ receipts
     └──────────── delta thread watch ◀──────────────────────┘
                   (standby until @mention → inject_message)
```

- Hooks are ignored for sessions that are not bridged (zero overhead).
- A live turn is recorded as `state_update running` (turnSequence = its
  sequence), then content (`user_message`, `agent_message`, `agent_thought`,
  `tool_call_update`) and a final `state_update idle` — all under the same
  `turnSequence`, per the Delta recording contract.
- Streaming output is accumulated and re-sent under a stable `messageId`
  (the contract's "full content so far" rule); reasoning deltas require
  `plugins.stream_reasoning_deltas: true`.
- Appends use `--acknowledge local` (the CLI's offline mode): records are
  recorded locally and uploaded in the background — this is both the
  contract-recommended path and, in practice, the only one that is reliable
  for a server-backed CLI daemon. The outbox is drained on turn end and on
  demand; undelivered records survive restarts.
- **Supersession is cooperative** (Hermes has no in-flight abort hook): a
  `fenced` receipt or `producer.turn superseded` from the watch gates future
  auto-turns until a fresh mention arrives.

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `DELTA_BIN` | `delta` on PATH (verified via `thread contract`), platform install paths, or `~/.local/bin/delta` | Delta CLI binary |
| `DELTA_BRIDGE_STATE_DIR` | `$HERMES_HOME/delta-bridge` (or `~/.hermes/delta-bridge`) | bindings + outbox |
| `DELTA_PRODUCER_NAME` | `config.json` `producer_name`, profile name, or `Hermes` | Display identity & `@mention` handle |

## Layout

```
plugin.yaml          manifest (hooks + commands)
bridge.py            state, records, outbox/delivery, watch receiver
__init__.py          hook wiring + /delta-* slash commands
docs/plugin-contract.md   Delta CLI recording contract, hook↔ACP mapping,
                          delivery-mode findings (verified against CLI 0.24.2)
tools/delta-hook-probe    scratch hook-payload probe (verifies field names
                          against the live Hermes build)
```

## Known gaps

- No in-flight turn abort: supersession is cooperative (gated at turn
  boundaries), not forceful.
- Threads created via the CLI are `local_only` until `thread share` —
  normal human-created threads are already shared.
- The bot-identity question is operational, not technical: the CLI signs in
  as whatever account completes the device flow; run Hermes' Delta identity
  as a dedicated bot account and enroll it in the org before sharing threads.

## License

MIT. The Delta recording protocol and ACP record shapes are the public Delta
CLI contract; the vendor reference plugin
[zed-industries/delta-claude](https://github.com/zed-industries/delta-claude)
(AGPL-3.0) was used as a **behavioral reference only** — no code was copied;
this is an original Python implementation of the documented protocol.
