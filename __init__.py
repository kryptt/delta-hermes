"""Delta ACP bridge — Hermes <-> Delta thread live bridge (slice 1).

Use: /delta-join <thread-id-or-link> in a session to bridge it to a Delta
thread. The session's turns are recorded as ACP records via the delta CLI;
teammate activity is watched and injected only when it mentions Hermes
(standby model). Cooperative supersession: a fenced/superseded turn gates
further auto-turns until a fresh mention arrives.

State lives in $DELTA_BRIDGE_STATE_DIR (default $HERMES_HOME/delta-bridge,
falling back to ~/.hermes/delta-bridge), so bridges survive gateway restarts
and undelivered records are retried.
"""

from __future__ import annotations

import json
import logging

from .bridge import (
    Bridge,
    PRODUCER_NAME,
    SOURCE,
    STATE_DIR,
    Watch,
    check_contract,
    delta,
    parse_status_response,
    tool_update,
)

log = logging.getLogger("delta-bridge")

_bridges: dict[str, Bridge] = {}  # session_id -> Bridge
_ctx = None
_pending_user: dict[str, dict] = {}  # session_id -> {"message_id": str, "text": str}
_session_keys: dict[str, str] = {}  # session_id -> session_key (learned from hooks)

LOG_FILE = STATE_DIR / "plugin.log"


def _log(msg: str) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(f"{msg}\n")
    except Exception:  # noqa: BLE001
        pass


def _bridge_for(session_id: str | None) -> Bridge | None:
    if not session_id or session_id == "config":
        return None
    bridge = _bridges.get(session_id)
    if bridge is None:
        bridge = Bridge.load(session_id)
        if bridge is not None:
            _bridges[session_id] = bridge
            _start_watch(bridge)
    return bridge


def _start_watch(bridge: Bridge) -> None:
    if bridge.watch_proc is not None and bridge.watch_proc.proc and bridge.watch_proc.proc.poll() is None:
        return
    if bridge.watch_proc is not None:
        bridge.watch_proc.stop()
    watch = Watch(bridge, lambda text: _inject(bridge, text))
    bridge.watch_proc = watch
    watch.start()
    _log(f"watch started: session={bridge.session_id} thread={bridge.thread}")


def _inject(bridge: Bridge, text: str) -> None:
    if bridge.gated or _ctx is None:
        return
    key = _session_keys.get(bridge.session_id)
    try:
        if key:
            ok = _ctx.inject_message(text, role="user", session_key=key)
        else:
            ok = _ctx.inject_message(text, role="user")
        _log(f"inject session={bridge.session_id} key={key} ok={ok} text={text[:80]!r}")
    except Exception as exc:  # noqa: BLE001
        _log(f"inject failed: {exc!r}")


def _ensure_turn(bridge: Bridge, turn_id: str | None) -> None:
    if not turn_id or bridge.is_turn_superseded(turn_id):
        return
    bridge.start_turn(turn_id)
    # A buffered gateway user message is emitted once the turn is live.
    pending = _pending_user.pop(bridge.session_id, None)
    if pending and pending.get("text"):
        bridge.record_user_message(turn_id, pending["text"])


def _safe(name: str, fn):
    def wrapped(**kwargs):
        try:
            fn(**kwargs)
        except Exception as exc:  # noqa: BLE001
            _log(f"hook {name} error: {exc!r}")
    return wrapped


# ------------------------------------------------------------------ hooks

def _on_post_gateway_admission(**kw) -> None:
    # Gateway non-internal user message ingress.
    session_id = kw.get("session_id")
    if kw.get("session_key") and session_id:
        _session_keys[session_id] = kw["session_key"]
    bridge = _bridge_for(session_id)
    if bridge is None:
        return
    text = kw.get("text") or ""
    if not text.strip():
        return
    # A direct user message explicitly un-gates the session.
    bridge.gated = False
    bridge.superseded_turn_id = None
    _pending_user[bridge.session_id] = {"message_id": kw.get("message_id"), "text": text}


def _on_pre_llm_call(**kw) -> None:
    session_id = kw.get("session_id")
    if kw.get("session_key") and session_id:
        _session_keys[session_id] = kw["session_key"]
    bridge = _bridge_for(session_id)
    if bridge is None:
        return
    turn_id = kw.get("turn_id")
    if bridge.is_turn_superseded(turn_id):
        return
    _ensure_turn(bridge, turn_id)
    user_msg = kw.get("user_message")
    if isinstance(user_msg, str) and user_msg.strip():
        bridge.record_user_message(turn_id, user_msg)


def _on_pre_tool_call(**kw) -> None:
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    turn_id = kw.get("turn_id")
    if bridge.is_turn_superseded(turn_id):
        return
    _ensure_turn(bridge, turn_id)
    if bridge.turn_sequence is None:
        return
    bridge.append(
        tool_update(
            kw.get("tool_call_id") or "t",
            kw.get("tool_name") or "tool",
            "in_progress",
            raw_input=kw.get("args"),
        ),
        turn_sequence=bridge.turn_sequence,
    )


def _on_post_tool_call(**kw) -> None:
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    turn_id = kw.get("turn_id")
    if bridge.is_turn_superseded(turn_id):
        return
    _ensure_turn(bridge, turn_id)
    if bridge.turn_sequence is None:
        return
    status = "completed" if kw.get("status") in (None, "ok", "success", "completed") else "failed"
    if status == "failed" and kw.get("error_message"):
        bridge.append(
            tool_update(
                kw.get("tool_call_id") or "t",
                kw.get("tool_name") or "tool",
                "failed",
                error=str(kw["error_message"]),
            ),
            turn_sequence=bridge.turn_sequence,
        )
    else:
        bridge.append(
            tool_update(
                kw.get("tool_call_id") or "t",
                kw.get("tool_name") or "tool",
                "completed",
                raw_output=kw.get("result"),
            ),
            turn_sequence=bridge.turn_sequence,
        )


def _on_stream_delta(**kw) -> None:
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    turn_id = kw.get("turn_id")
    if bridge.is_turn_superseded(turn_id):
        return
    _ensure_turn(bridge, turn_id)
    bridge.stream_delta(turn_id, kw.get("kind", "text"), kw.get("delta") or "")


def _on_stream_end(**kw) -> None:
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    bridge.stream_end(kw.get("turn_id"))


def _on_post_llm_call(**kw) -> None:
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    turn_id = kw.get("turn_id")
    if bridge.is_turn_superseded(turn_id):
        return
    _ensure_turn(bridge, turn_id)
    user_msg = kw.get("user_message")
    if isinstance(user_msg, str) and user_msg.strip():
        bridge.record_user_message(turn_id, user_msg)
    bridge.final_answer(turn_id, kw.get("assistant_response") or "")


def _on_session_end(**kw) -> None:
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    interrupted = bool(kw.get("interrupted")) or kw.get("turn_exit_reason") in (
        "interrupted",
        "cancelled",
        "stopped",
        "aborted",
    )
    bridge.end_turn(interrupted=interrupted)


def _on_agent_loop_stopped(**kw) -> None:
    session_id = kw.get("session_id")
    if not session_id and kw.get("session_key"):
        session_id = kw["session_key"].rsplit(":", 1)[-1]
    bridge = _bridge_for(session_id)
    if bridge is None:
        return
    bridge.end_turn(interrupted=True)
    if kw.get("session_key"):
        _session_keys[bridge.session_id] = kw["session_key"]


def _on_human_input_request(**kw) -> None:
    if kw.get("session_key") and kw.get("session_id"):
        _session_keys[kw["session_id"]] = kw["session_key"]


# ----------------------------------------------------------------- commands

def _cmd_delta_join(args, session_id=None, session_key=None, **kw):
    locator = (args or "").strip()
    if not locator:
        return (
            "Usage: /delta-join <thread-id-or-link>. Bind this session to a "
            "Delta thread; recording and watch start immediately."
        )
    if not session_id:
        return "Cannot join a Delta thread without an active session_id."

    ok, contract_info = check_contract()
    if not ok:
        return str(contract_info)

    rc, out, err = delta([
        "thread", "status", locator,
        "--source", SOURCE,
        "--external-id", session_id,
    ])
    if rc != 0:
        try:
            err_env = json.loads(out).get("error")
            if isinstance(err_env, dict) and err_env.get("message"):
                return f"delta thread status failed ({err_env.get('code', rc)}): {err_env['message']}"
        except Exception:  # noqa: BLE001
            pass
        return f"delta thread status failed (rc={rc}): {err.strip()[:200] or out.strip()[:200]}"

    thread_id, next_seq, actor_id = parse_status_response(out, locator)

    existing = _bridges.get(session_id) or Bridge.load(session_id)
    seen_ids = None
    if existing is not None:
        if existing.watch_proc is not None:
            existing.watch_proc.stop()
            existing.watch_proc = None
        if existing.thread == thread_id or existing.locator == locator:
            # Only carry over sequence bookkeeping when the existing binding was
            # recorded under the same producer source we just queried.
            if existing.source == SOURCE:
                next_seq = max(next_seq, existing.next_sequence)
            seen_ids = existing.seen_ids

    bridge = Bridge(
        session_id,
        thread_id,
        next_sequence=next_seq,
        locator=locator,
        actor_id=actor_id,
        seen_ids=seen_ids,
    )
    bridge._contract_verified = True
    _bridges[session_id] = bridge
    bridge.save()
    if session_key:
        _session_keys[session_id] = session_key
    _start_watch(bridge)
    # Drain any previously queued outbox records for this session.
    bridge.flush_async()
    return (
        f"Bridged session {session_id} -> thread {thread_id} "
        f"(next sequence {next_seq}). Recording turns and watching for "
        f"@{PRODUCER_NAME} mentions. Say /delta-leave to unbind."
    )


def _cmd_delta_leave(args, session_id=None, **kw):
    bridge = _bridges.get(session_id) or Bridge.load(session_id)
    if bridge is None:
        return "This session is not bridged to any Delta thread."
    if bridge.watch_proc is not None:
        bridge.watch_proc.stop()
        bridge.watch_proc = None
    _bridges.pop(session_id, None)
    try:
        bridge.binding_path.unlink(missing_ok=True)
        (STATE_DIR / f"{session_id}.json").unlink(missing_ok=True)
    except Exception:  # noqa: BLE001
        pass
    return (
        f"Unbridged session {session_id} from thread {bridge.thread}. "
        f"Pending outbox kept at {bridge.outbox_path}."
    )


def _cmd_delta_status(args, session_id=None, **kw):
    bridge = _bridges.get(session_id) or Bridge.load(session_id)
    if bridge is None:
        return "Not bridged. Use /delta-join <thread-id-or-link>."
    watch = bridge.watch_proc
    outbox = bridge.outbox_path
    pending = (
        len([ln for ln in outbox.read_text(encoding="utf-8").splitlines() if ln.strip()])
        if outbox.exists()
        else 0
    )
    watch_state = (
        "running"
        if watch and watch.proc and watch.proc.poll() is None
        else "stopped"
    )
    return (
        f"thread={bridge.thread} producer={PRODUCER_NAME} "
        f"next_sequence={bridge.next_sequence} gated={bridge.gated} "
        f"watch={watch_state} outbox_pending={pending}"
    )


# ------------------------------------------------------------------ plugin

def register(ctx) -> None:
    global _ctx
    _ctx = ctx
    for name, fn in [
        ("post_gateway_admission", _on_post_gateway_admission),
        ("pre_llm_call", _on_pre_llm_call),
        ("pre_tool_call", _on_pre_tool_call),
        ("post_tool_call", _on_post_tool_call),
        ("on_stream_delta", _on_stream_delta),
        ("on_stream_end", _on_stream_end),
        ("post_llm_call", _on_post_llm_call),
        ("on_session_end", _on_session_end),
        ("agent_loop_stopped", _on_agent_loop_stopped),
        ("on_human_input_request", _on_human_input_request),
    ]:
        ctx.register_hook(name, _safe(name, fn))

    ctx.register_command(
        "delta-join",
        _cmd_delta_join,
        "Bind this session to a Delta thread: /delta-join <thread-id>",
    )
    ctx.register_command(
        "delta-leave",
        _cmd_delta_leave,
        "Unbind this session from its Delta thread",
    )
    ctx.register_command(
        "delta-status",
        _cmd_delta_status,
        "Show this session's Delta bridge state",
    )

    # Re-attach watches for persisted bindings (gateway restart).
    if STATE_DIR.is_dir():
        for path in STATE_DIR.glob("*.json"):
            if path.name == "config.json":
                continue
            sid = path.stem
            if sid in _bridges:
                continue
            try:
                bridge = Bridge.load(sid)
                if bridge is not None:
                    _bridges[bridge.session_id] = bridge
                    _start_watch(bridge)
                    bridge.flush_async()
            except Exception as exc:  # noqa: BLE001
                _log(f"failed to reload binding {path.name}: {exc!r}")
    _log(f"delta-bridge registered: {len(_bridges)} persisted binding(s) re-attached")

