"""Scratch hook-payload probe for the Delta ACP bridge (slice 1, step 1).

Purpose: verify the EXACT keyword payloads the running Hermes build delivers
per hook, so the bridge is built against real field names, not docs alone.

Writes one JSON object per line to $DELTA_HOOK_PROBE_LOG (default:
$DELTA_BRIDGE_STATE_DIR/delta-hook-probe.jsonl or ~/.hermes/delta-bridge/delta-hook-probe.jsonl).
Payloads are SANITIZED: values become {type, len/sample} summaries — field
names and shapes are what matters here, not content.

Remove or disable this plugin once the bridge is built.
"""

import json
import os
import pathlib
import time


def _default_log_path() -> pathlib.Path:
    if os.environ.get("DELTA_HOOK_PROBE_LOG"):
        return pathlib.Path(os.environ["DELTA_HOOK_PROBE_LOG"]).expanduser()
    if os.environ.get("DELTA_BRIDGE_STATE_DIR"):
        return pathlib.Path(os.environ["DELTA_BRIDGE_STATE_DIR"]).expanduser() / "delta-hook-probe.jsonl"
    hermes_home = pathlib.Path(os.environ.get("HERMES_HOME") or (pathlib.Path.home() / ".hermes")).expanduser()
    return hermes_home / "delta-bridge" / "delta-hook-probe.jsonl"


LOG_PATH = _default_log_path()

HOOKS = [
    "pre_llm_call",
    "post_llm_call",
    "post_api_request",
    "pre_tool_call",
    "post_tool_call",
    "on_stream_start",
    "on_stream_delta",
    "on_stream_end",
    "on_interim_message",
    "on_human_input_request",
    "on_human_input_resolved",
    "post_gateway_admission",
    "on_session_start",
    "on_session_open",
    "on_session_end",
    "on_session_finalize",
    "on_session_reset",
    "agent_loop_stopped",
]


def _summarize(value):
    if isinstance(value, str):
        return {"type": "str", "len": len(value), "sample": value[:120]}
    if isinstance(value, dict):
        return {"type": "dict", "keys": sorted(str(k) for k in value)[:40]}
    if isinstance(value, (list, tuple)):
        return {"type": type(value).__name__, "len": len(value)}
    if value is None or isinstance(value, (int, float, bool)):
        return {"type": type(value).__name__, "value": value}
    return {"type": type(value).__name__}


def _make_logger(name):
    def logger(**kwargs):
        try:
            payload = {k: _summarize(v) for k, v in kwargs.items()}
            rec = {"hook": name, "ts": time.time(), "fields": sorted(payload), "payload": payload}
            LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            with LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, sort_keys=True, ensure_ascii=False) + "\n")
        except Exception as exc:  # never break the agent loop
            print(f"[delta-hook-probe] {name} log failed: {exc!r}", flush=True)

    return logger


def register(ctx):
    for hook in HOOKS:
        ctx.register_hook(hook, _make_logger(hook))
    # report which hooks actually registered (some may be rejected if invalid)
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"hook": "__register__", "ts": time.time(),
                                "registered": HOOKS}) + "\n")
    except Exception:
        pass

