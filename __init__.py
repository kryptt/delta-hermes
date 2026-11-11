"""Delta ACP bridge — Hermes <-> Delta thread live bridge (slice 1).

Use: /delta-join <thread-id-or-link> in a session to bridge it to a Delta
thread. The session's turns are recorded as ACP records via the delta CLI;
teammate activity is watched and injected only when it mentions Hermes
(standby model). Cooperative supersession: a fenced/superseded turn gates
further auto-turns until a fresh mention arrives.

State lives in /opt/data/cache/scratch/delta-bridge/ (bindings + outbox),
so bridges survive gateway restarts and undelivered records are retried.
"""

from __future__ import annotations

import json
import logging
import pathlib

from .bridge import (
    Bridge,
    STATE_DIR,
    PRODUCER_NAME,
    Watch,
    delta,
    text_update,
    tool_update,
)

log = logging.getLogger("delta-bridge")

_bridges = {}  # session_id -> Bridge
_ctx = None
_pending_user = {}  # session_id -> {"message_id": str, "text": str}
_session_keys = {}  # session_id -> session_key (learned from hooks)

LOG_FILE = STATE_DIR / "plugin.log"


def _log(msg):
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a") as f:
            f.write(f"{msg}\n")
    except Exception:  # noqa: BLE001
        pass


def _bridge_for(session_id):
    if not session_id:
        return None
    bridge = _bridges.get(session_id)
    if bridge is None:
        bridge = Bridge.load(session_id)
        if bridge is not None:
            _bridges[session_id] = bridge
            _start_watch(bridge)
    return bridge


def _start_watch(bridge):
    if bridge.watch_proc is not None:
        return
    watch = Watch(bridge, lambda text: _inject(bridge, text))
    bridge.watch_proc = watch
    watch.start()
    _log(f"watch started: session={bridge.session_id} thread={bridge.thread}")


def _inject(bridge, text):
    key = _session_keys.get(bridge.session_id)
    try:
        if key:
            ok = _ctx.inject_message(text, role="user", session_key=key)
        else:
            ok = _ctx.inject_message(text, role="user")
        _log(f"inject session={bridge.session_id} key={key} ok={ok} text={text[:80]!r}")
    except Exception as exc:  # noqa: BLE001
        _log(f"inject failed: {exc!r}")


def _ensure_turn(bridge, turn_id):
    if not turn_id:
        return
    bridge.start_turn(turn_id)
    # a buffered gateway user message is emitted once the turn is live
    pending = _pending_user.pop(bridge.session_id, None)
    if pending and bridge.turn_sequence is not None:
        bridge.append(
            text_update("user_message", pending["text"], f"{turn_id}:user"),
            turn_sequence=bridge.turn_sequence,
        )


def _safe(name, fn):
    def wrapped(**kwargs):
        try:
            fn(**kwargs)
        except Exception as exc:  # noqa: BLE001
            _log(f"hook {name} error: {exc!r}")
    return wrapped


# ------------------------------------------------------------------ hooks

def _on_post_gateway_admission(**kw):
    # gateway non-internal user message ingress
    session_id = kw.get("session_id")
    bridge = _bridge_for(session_id)
    if bridge is None:
        return
    text = kw.get("text") or ""
    if not text.strip():
        return
    _pending_user[bridge.session_id] = {"message_id": kw.get("message_id"), "text": text}
    if kw.get("session_key"):
        _session_keys[bridge.session_id] = kw["session_key"]


def _on_pre_llm_call(**kw):
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    _ensure_turn(bridge, kw.get("turn_id"))
    pending = _pending_user.get(bridge.session_id)
    if pending and bridge.turn_sequence is not None:
        bridge.append(
            text_update("user_message", pending["text"], f"{bridge.turn_id}:user"),
            turn_sequence=bridge.turn_sequence,
        )
        _pending_user.pop(bridge.session_id, None)


def _on_pre_tool_call(**kw):
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    _ensure_turn(bridge, kw.get("turn_id"))
    if bridge.turn_sequence is None:
        return
    bridge.append(
        tool_update(kw.get("tool_call_id") or "t", kw.get("tool_name") or "tool",
                    "in_progress", raw_input=kw.get("args")),
        turn_sequence=bridge.turn_sequence,
    )


def _on_post_tool_call(**kw):
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    _ensure_turn(bridge, kw.get("turn_id"))
    if bridge.turn_sequence is None:
        return
    status = "completed" if kw.get("status") in (None, "ok", "success", "completed") else "failed"
    if status == "failed" and kw.get("error_message"):
        bridge.append(
            tool_update(kw.get("tool_call_id") or "t", kw.get("tool_name") or "tool",
                        "failed", error=str(kw["error_message"])),
            turn_sequence=bridge.turn_sequence,
        )
    else:
        bridge.append(
            tool_update(kw.get("tool_call_id") or "t", kw.get("tool_name") or "tool",
                        "completed", raw_output=kw.get("result")),
            turn_sequence=bridge.turn_sequence,
        )


def _on_stream_delta(**kw):
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    bridge.start_turn(kw.get("turn_id"))
    bridge.stream_delta(kw.get("turn_id"), kw.get("kind", "text"), kw.get("delta") or "")


def _on_stream_end(**kw):
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    bridge.stream_end(kw.get("turn_id"))


def _on_post_llm_call(**kw):
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    _ensure_turn(bridge, kw.get("turn_id"))
    bridge.final_answer(kw.get("turn_id"), kw.get("assistant_response") or "")


def _on_session_end(**kw):
    bridge = _bridge_for(kw.get("session_id"))
    if bridge is None:
        return
    bridge.end_turn(interrupted=bool(kw.get("interrupted")))


def _on_agent_loop_stopped(**kw):
    session_id = kw.get("session_id")
    if not session_id and kw.get("session_key"):
        session_id = kw["session_key"].rsplit(":", 1)[-1]
    bridge = _bridge_for(session_id)
    if bridge is None:
        return
    bridge.end_turn(interrupted=True)
    if kw.get("session_key"):
        _session_keys[bridge.session_id] = kw["session_key"]


def _on_human_input_request(**kw):
    if kw.get("session_key") and kw.get("session_id"):
        _session_keys[kw["session_id"]] = kw["session_key"]


# ----------------------------------------------------------------- commands

def _cmd_delta_join(args, session_id=None, session_key=None, **kw):
    thread = (args or "").strip()
    if not thread:
        return ("Usage: /delta-join <thread-id-or-link>. Bind this session to a "
                "Delta thread; recording and watch start immediately.")
    rc, out, err = delta(["thread", "status", thread, "--source", "hermes",
                          "--external-id", session_id])
    if rc != 0:
        return f"delta thread status failed (rc={rc}): {err.strip()[:200] or out.strip()[:200]}"
    next_seq = 1
    try:
        info = json.loads(out)
        for key in ("nextSequence", "next_sequence", "next"):
            if isinstance(info.get(key), int):
                next_seq = info[key]
                break
    except Exception:  # noqa: BLE001
        pass
    bridge = Bridge(session_id, thread, next_sequence=next_seq)
    _bridges[session_id] = bridge
    bridge.save()
    if session_key:
        _session_keys[session_id] = session_key
    _start_watch(bridge)
    return (f"Bridged session {session_id} -> thread {thread} "
            f"(next sequence {next_seq}). Recording turns and watching for "
            f"@Hermes mentions. Say /delta-leave to unbind.")


def _cmd_delta_leave(args, session_id=None, **kw):
    bridge = _bridges.get(session_id) or Bridge.load(session_id)
    if bridge is None:
        return "This session is not bridged to any Delta thread."
    if bridge.watch_proc is not None:
        bridge.watch_proc.stop()
        bridge.watch_proc = None
    _bridges.pop(session_id, None)
    try:
        (STATE_DIR / f"{session_id}.json").unlink(missing_ok=True)
    except Exception:  # noqa: BLE001
        pass
    return f"Unbridged session {session_id} from thread {bridge.thread}. Pending outbox kept at {bridge.outbox_path}."


def _cmd_delta_status(args, session_id=None, **kw):
    bridge = _bridges.get(session_id) or Bridge.load(session_id)
    if bridge is None:
        return "Not bridged. Use /delta-join <thread-id-or-link>."
    watch = bridge.watch_proc
    outbox = bridge.outbox_path
    pending = len(outbox.read_text().splitlines()) if outbox.exists() else 0
    return (f"thread={bridge.thread} next_sequence={bridge.next_sequence} "
            f"gated={bridge.gated} watch={'running' if watch and watch.proc and watch.proc.poll() is None else 'stopped'} "
            f"outbox_pending={pending}")


# ------------------------------------------------------------------ plugin

def register(ctx):
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

    ctx.register_command("delta-join", _cmd_delta_join,
                         "Bind this session to a Delta thread: /delta-join <thread-id>")
    ctx.register_command("delta-leave", _cmd_delta_leave,
                         "Unbind this session from its Delta thread")
    ctx.register_command("delta-status", _cmd_delta_status,
                         "Show this session's Delta bridge state")

    # Re-attach watches for persisted bindings (gateway restart).
    try:
        for path in STATE_DIR.glob("*.json"):
            sid = path.stem
            if sid not in _bridges:
                bridge = Bridge.load(sid)
                if bridge is not None:
                    _bridges[sid] = bridge
                    _start_watch(bridge)
    except Exception:  # noqa: BLE001
        pass
    _log(f"delta-bridge registered: {len(_bridges)} persisted binding(s) re-attached")
