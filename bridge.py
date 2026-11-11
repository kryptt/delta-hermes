"""Delta ACP bridge core.

Per-session state (binding + outbox), record building, delivery via
`delta thread append`, and the `thread watch` receiver with standby/@mention
injection and cooperative supersession gating.

Protocol facts (from the delta CLI contract, see docs/plugin-contract.md):
- Records are schema-1 JSONL envelopes {schemaVersion:1, sequence, turnSequence?,
  update:{sessionUpdate,...}} piped on stdin to `delta --json thread append`.
- Sequences are monotonic per thread per producer; recover `producer.next_sequence`
  from `delta --json thread status`. Never resend an acknowledged record.
- A live turn is `state_update running` with turnSequence == its sequence,
  then all content and the final `state_update idle` carry that same turnSequence.
- Streaming: re-send the same messageId with the full content so far.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time


CONTRACT_MIN = 8
CONTRACT_MAX = 11
SOURCE = "hermes"
MAX_BATCH_BYTES = 2_000_000
MAX_SEEN_IDS = 500


def _hermes_home() -> pathlib.Path:
    env = os.environ.get("HERMES_HOME")
    if env:
        return pathlib.Path(env).expanduser()
    return pathlib.Path.home() / ".hermes"


def _default_state_dir() -> pathlib.Path:
    env = os.environ.get("DELTA_BRIDGE_STATE_DIR")
    if env:
        return pathlib.Path(env).expanduser()
    return _hermes_home() / "delta-bridge"


def _load_config(state_dir: pathlib.Path | None = None) -> dict:
    base = state_dir or _default_state_dir()
    cfg = base / "config.json"
    try:
        if cfg.is_file():
            data = json.loads(cfg.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:  # noqa: BLE001
        pass
    return {}


def _probes_as_delta(executable: str) -> bool:
    """Return True if `executable` answers `delta --json thread contract` as Delta CLI.

    Distinguishes Zed's Delta CLI from other binaries named `delta` on PATH
    (such as the `git-delta` syntax-highlighting pager).
    """
    try:
        proc = subprocess.run(
            [executable, "--json", "thread", "contract"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            env={**os.environ, "DELTA_CLI_INTEGRATION": SOURCE},
        )
        data = json.loads(proc.stdout)
        return (
            isinstance(data, dict)
            and data.get("operation") == "contract"
            and isinstance(data.get("min"), int)
            and isinstance(data.get("max"), int)
        )
    except Exception:  # noqa: BLE001
        return False


def _platform_install_candidates() -> list[pathlib.Path]:
    home = pathlib.Path.home()
    candidates: list[pathlib.Path] = []
    if sys.platform == "darwin":
        candidates.extend([
            home / "Applications" / "Delta.app" / "Contents" / "MacOS" / "delta",
            pathlib.Path("/Applications/Delta.app/Contents/MacOS/delta"),
        ])
    elif os.name == "nt":
        local_app_data = os.environ.get("LOCALAPPDATA")
        if local_app_data:
            candidates.append(pathlib.Path(local_app_data) / "Programs" / "Delta" / "bin" / "delta.exe")
        try:
            import winreg  # type: ignore[import-not-found]
            for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
                try:
                    with winreg.OpenKey(
                        hive,
                        r"Software\Zed Industries\Delta",
                        0,
                        winreg.KEY_READ | getattr(winreg, "KEY_WOW64_64KEY", 0),
                    ) as key:
                        folder, _ = winreg.QueryValueEx(key, "InstallFolder")
                        if isinstance(folder, str) and folder.strip():
                            candidates.append(pathlib.Path(folder.strip()) / "bin" / "delta.exe")
                except OSError:
                    continue
        except Exception:  # noqa: BLE001
            pass
    else:
        candidates.extend([
            home / ".local" / "delta.app" / "bin" / "delta",
            home / ".local" / "bin" / "delta",
        ])
    return candidates


def _default_delta_bin() -> str:
    env = os.environ.get("DELTA_BIN")
    if env:
        return env
    exe_names = ("delta.exe", "delta") if os.name == "nt" else ("delta",)
    seen: set[str] = set()
    for entry in os.environ.get("PATH", "").split(os.pathsep):
        if not entry:
            continue
        p = pathlib.Path(entry)
        if not p.is_absolute():
            continue
        for name in exe_names:
            cand = p / name
            key = str(cand)
            if key in seen:
                continue
            seen.add(key)
            if cand.is_file() and os.access(cand, os.X_OK) and _probes_as_delta(key):
                return key
    for cand in _platform_install_candidates():
        if cand.is_file() and os.access(cand, os.X_OK):
            return str(cand)
    found = shutil.which("delta")
    if found:
        return found
    return str(pathlib.Path.home() / ".local" / "bin" / "delta")


def _default_producer_name(state_dir: pathlib.Path | None = None) -> str:
    """Per-profile identity for Delta records: who this agent shows as.

    Same plugin code serves every profile (Dave, Simon, ...), so the name is
    never hardcoded. Resolution order:
      1. DELTA_PRODUCER_NAME env var
      2. `producer_name` in $DELTA_BRIDGE_STATE_DIR/config.json (or $HERMES_HOME/delta-bridge/config.json)
      3. capitalized basename of HERMES_HOME (profile dir), when set and non-generic
      4. "Hermes" (generic fallback)
    """
    env = os.environ.get("DELTA_PRODUCER_NAME")
    if env:
        return env
    cfg = _load_config(state_dir)
    name = cfg.get("producer_name")
    if name:
        return str(name)
    if os.environ.get("HERMES_HOME"):
        base = pathlib.Path(os.environ["HERMES_HOME"]).name
        if base and base.lower() not in ("home", ".hermes", "hermes"):
            return base[:1].upper() + base[1:]
    return "Hermes"


STATE_DIR = _default_state_dir()
DELTA_BIN = _default_delta_bin()
PRODUCER_NAME = _default_producer_name(STATE_DIR)


def _safe_session_filename(session_id: str) -> str:
    """Sanitize session_id for safe single-file use inside STATE_DIR."""
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", str(session_id))
    if not cleaned or cleaned in (".", "..", "config"):
        cleaned = f"session_{cleaned or 'unknown'}"
    return cleaned


def _atomic_write_text(path: pathlib.Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    tmp_path = pathlib.Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def delta(args: list[str], input_text: str | None = None, timeout: int = 90) -> tuple[int, str, str]:
    """Run the delta CLI. Returns (rc, stdout, stderr). Never raises."""
    try:
        proc = subprocess.run(
            [DELTA_BIN, "--json", *args],
            input=input_text,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            env={**os.environ, "DELTA_CLI_INTEGRATION": SOURCE},
        )
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as exc:  # noqa: BLE001
        return -1, "", f"delta invocation failed: {exc!r}"


def check_contract() -> tuple[bool, int | str]:
    """Verify the Delta CLI recording contract range.

    Returns (True, negotiated_version) when compatible, or (False, error_message).
    """
    rc, out, err = delta(["thread", "contract"], timeout=15)
    if rc != 0 and not out.strip():
        return False, (
            f"Delta CLI contract check failed (rc={rc}): "
            f"{err.strip()[:200] or 'selected delta binary does not support `thread contract` (is it git-delta?)'}"
        )
    try:
        data = json.loads(out)
    except Exception:  # noqa: BLE001
        return False, f"Unexpected `delta --json thread contract` output: {out.strip()[:200]}"
    if isinstance(data, dict) and isinstance(data.get("error"), dict):
        msg = data["error"].get("message") or str(data["error"])
        return False, f"Delta CLI contract error: {msg}"
    if (
        not isinstance(data, dict)
        or data.get("operation") != "contract"
        or not isinstance(data.get("min"), int)
        or not isinstance(data.get("max"), int)
        or data["min"] < 0
        or data["max"] < data["min"]
    ):
        return False, "Unexpected thread contract response; use matching Delta CLI and plugin builds."
    cli_min, cli_max = data["min"], data["max"]
    if cli_max < CONTRACT_MIN:
        return False, f"Delta CLI contract (max={cli_max}) is too old for this plugin (requires >={CONTRACT_MIN}). Update Delta."
    if cli_min > CONTRACT_MAX:
        return False, f"Delta CLI contract (min={cli_min}) is newer than this plugin (supports <={CONTRACT_MAX}). Update delta-bridge."
    return True, min(cli_max, CONTRACT_MAX)


def parse_status_response(out: str, fallback_thread: str) -> tuple[str, int, str | None]:
    """Extract (thread_id, next_sequence, actor_id) from `delta --json thread status` output."""
    thread_id = fallback_thread
    next_seq = 1
    actor_id = None
    try:
        info = json.loads(out)
        if not isinstance(info, dict):
            return thread_id, next_seq, actor_id
        if isinstance(info.get("thread_id"), str) and info["thread_id"].strip():
            thread_id = info["thread_id"].strip()
        producer = info.get("producer")
        if isinstance(producer, dict):
            p_seq = producer.get("next_sequence")
            if isinstance(p_seq, int) and p_seq >= 1:
                next_seq = p_seq
            if isinstance(producer.get("actor_id"), str) and producer["actor_id"]:
                actor_id = producer["actor_id"]
        else:
            for key in ("nextSequence", "next_sequence", "next"):
                val = info.get(key)
                if isinstance(val, int) and val >= 1:
                    next_seq = val
                    break
    except Exception:  # noqa: BLE001
        pass
    return thread_id, next_seq, actor_id


# ---------------------------------------------------------------- records

def _json_safe(value):
    """Ensure arbitrary hook values (tool args/results) can serialize to JSON."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    try:
        json.dumps(value)
        return value
    except (TypeError, OverflowError):
        return str(value)


def envelope(sequence: int, update: dict, turn_sequence: int | None = None) -> dict:
    rec: dict = {"schemaVersion": 1, "sequence": sequence, "update": update}
    if turn_sequence is not None:
        rec["turnSequence"] = turn_sequence
    return rec


def text_update(kind: str, text: str, message_id: str) -> dict:
    return {
        "sessionUpdate": kind,
        "messageId": message_id,
        "content": [{"type": "text", "text": text}],
    }


def _infer_tool_info(name: str, raw_input) -> tuple[str, str]:
    """Return an ACP (title, kind) pair for a tool call."""
    lower = (name or "tool").lower()
    args = raw_input if isinstance(raw_input, dict) else {}
    if lower in ("terminal", "bash", "execute_command", "shell", "run_command"):
        cmd = args.get("command") or args.get("cmd")
        title = str(cmd).strip() if isinstance(cmd, str) and cmd.strip() else name
        return re.sub(r"\s+", " ", title), "execute"
    if lower in ("read_file", "read", "view_file"):
        path = args.get("path") or args.get("file_path")
        return (f"Read {path}" if isinstance(path, str) and path else name), "read"
    if lower in ("edit_file", "write_file", "edit", "write", "patch"):
        path = args.get("path") or args.get("file_path")
        return (f"{name} {path}" if isinstance(path, str) and path else name), "edit"
    if lower in ("grep", "search", "find", "glob", "search_files"):
        pattern = args.get("pattern") or args.get("query")
        return (f"{name} {pattern}" if isinstance(pattern, str) and pattern else name), "search"
    if lower in ("fetch", "web_search", "web_fetch", "http_request"):
        target = args.get("url") or args.get("query")
        return (f"{name} {target}" if isinstance(target, str) and target else name), "fetch"
    return (name or "tool"), "other"


def tool_update(
    tool_call_id: str,
    name: str,
    status: str,
    raw_input=None,
    raw_output=None,
    error: str | None = None,
) -> dict:
    upd: dict = {
        "sessionUpdate": "tool_call_update",
        "toolCallId": str(tool_call_id or "t"),
        "name": str(name or "tool"),
        "status": status,
    }
    if status == "in_progress":
        title, kind = _infer_tool_info(upd["name"], raw_input)
        upd["title"] = title
        upd["kind"] = kind
    if raw_input is not None:
        upd["rawInput"] = _json_safe(raw_input)
    if error is not None:
        upd["rawOutput"] = str(error)
        upd["content"] = [{"type": "content", "content": {"type": "text", "text": str(error)}}]
    elif raw_output is not None:
        safe_out = _json_safe(raw_output)
        upd["rawOutput"] = safe_out
        text_out = safe_out if isinstance(safe_out, str) else json.dumps(safe_out, ensure_ascii=False)
        if text_out:
            upd["content"] = [{"type": "content", "content": {"type": "text", "text": text_out}}]
    return upd


def state_update(state: str, stop_reason: str | None = None) -> dict:
    upd: dict = {"sessionUpdate": "state_update", "state": state}
    if state == "idle" and stop_reason:
        upd["stopReason"] = stop_reason
    return upd


# ---------------------------------------------------------------- bridge

class Bridge:
    """One Hermes session <-> one Delta thread."""

    def __init__(
        self,
        session_id: str,
        thread: str,
        next_sequence: int = 1,
        last_turn_sequence: int | None = None,
        gated: bool = False,
        locator: str | None = None,
        actor_id: str | None = None,
        user_id: str | None = None,
        seen_ids: list[str] | None = None,
        active_turn_sequence: int | None = None,
    ):
        self.session_id = session_id
        self.thread = thread
        self.locator = locator or thread
        self.next_sequence = max(1, int(next_sequence))
        self.last_turn_sequence = last_turn_sequence
        self.gated = bool(gated)
        self.actor_id = actor_id
        self.user_id = user_id
        self.seen_ids: list[str] = list(seen_ids or [])
        self._seen_set: set[str] = set(self.seen_ids)
        self.turn_id: str | None = None
        self.turn_sequence: int | None = None
        self.superseded_turn_id: str | None = None
        self.user_recorded_for_turn: bool = False
        self.injected_turn_pending: bool = False
        self._stream = self._fresh_stream(None)
        self.watch_proc: Watch | None = None
        self._lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._retry_scheduled = False
        self._contract_verified = False
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        # If a previous process exited mid-turn, close that orphaned turn cleanly.
        if isinstance(active_turn_sequence, int) and active_turn_sequence >= 1:
            self._close_orphaned_turn(active_turn_sequence)

    @staticmethod
    def _fresh_stream(turn_id: str | None) -> dict:
        return {
            "turn_id": turn_id,
            "step": 0,
            "text": "",
            "reasoning": "",
            "streamed_in_step": False,
        }

    def _close_orphaned_turn(self, orphan_turn_seq: int) -> None:
        self.append(
            state_update("idle", stop_reason="cancelled"),
            turn_sequence=orphan_turn_seq,
        )
        self.last_turn_sequence = orphan_turn_seq
        self.save()

    # ---- persistence ----
    @property
    def binding_path(self) -> pathlib.Path:
        return STATE_DIR / f"{_safe_session_filename(self.session_id)}.json"

    @property
    def outbox_path(self) -> pathlib.Path:
        return STATE_DIR / f"{_safe_session_filename(self.session_id)}.outbox.jsonl"

    def remember_seen(self, item_id: str) -> bool:
        """Record an item/comment ID as seen. Returns True if it was newly added."""
        if not item_id:
            return False
        with self._lock:
            if item_id in self._seen_set:
                return False
            self._seen_set.add(item_id)
            self.seen_ids.append(item_id)
            if len(self.seen_ids) > MAX_SEEN_IDS:
                dropped = self.seen_ids[:-MAX_SEEN_IDS]
                self.seen_ids = self.seen_ids[-MAX_SEEN_IDS:]
                for old in dropped:
                    self._seen_set.discard(old)
            return True

    def is_seen(self, item_id: str) -> bool:
        with self._lock:
            return item_id in self._seen_set

    def save(self) -> None:
        data = {
            "session_id": self.session_id,
            "thread": self.thread,
            "locator": self.locator,
            "next_sequence": self.next_sequence,
            "last_turn_sequence": self.last_turn_sequence,
            "active_turn_sequence": self.turn_sequence,
            "gated": self.gated,
            "actor_id": self.actor_id,
            "user_id": self.user_id,
            "seen_ids": self.seen_ids[-MAX_SEEN_IDS:],
        }
        _atomic_write_text(self.binding_path, json.dumps(data, indent=2, ensure_ascii=False) + "\n")

    @classmethod
    def load(cls, session_id: str) -> Bridge | None:
        if not session_id or session_id == "config":
            return None
        path = STATE_DIR / f"{_safe_session_filename(session_id)}.json"
        if not path.is_file():
            # Fallback to raw session_id.json if created by an older version
            legacy = STATE_DIR / f"{session_id}.json"
            if not legacy.is_file() or legacy.name == "config.json":
                return None
            path = legacy
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(data, dict) or not isinstance(data.get("thread"), str):
            return None
        sid = data.get("session_id") or session_id
        return cls(
            sid,
            data["thread"],
            next_sequence=data.get("next_sequence", 1),
            last_turn_sequence=data.get("last_turn_sequence"),
            gated=data.get("gated", False),
            locator=data.get("locator"),
            actor_id=data.get("actor_id"),
            user_id=data.get("user_id"),
            seen_ids=data.get("seen_ids"),
            active_turn_sequence=data.get("active_turn_sequence"),
        )

    # ---- appending + delivery ----
    def _stage_locked(self, update: dict, turn_sequence: int | None = None) -> dict:
        seq = self.next_sequence
        self.next_sequence += 1
        rec = envelope(seq, update, turn_sequence)
        self.outbox_path.parent.mkdir(parents=True, exist_ok=True)
        with self.outbox_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.save()
        return rec

    def append(self, update: dict, turn_sequence: int | None = None) -> dict:
        with self._lock:
            return self._stage_locked(update, turn_sequence)

    def flush_async(self, delay: float = 0.0) -> None:
        """Trigger a non-blocking outbox flush (or retry after `delay` seconds)."""
        with self._lock:
            if self._retry_scheduled:
                return
            self._retry_scheduled = True

        def _worker():
            try:
                if delay > 0:
                    time.sleep(delay)
            finally:
                with self._lock:
                    self._retry_scheduled = False
            self.flush()

        threading.Thread(target=_worker, daemon=True).start()

    def flush(self) -> bool:
        """Deliver pending records; drop acknowledged prefix; keep the rest.
        Returns True when the whole outbox was acknowledged."""
        path = self.outbox_path
        if not self._flush_lock.acquire(blocking=False):
            # Another flush is already in flight; wait for it to finish and check.
            with self._flush_lock:
                return not path.exists() or not path.read_text(encoding="utf-8").strip()
        try:
            while True:
                with self._lock:
                    if not path.exists():
                        return True
                    raw_lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
                    if not raw_lines:
                        return True

                if not self._contract_verified:
                    ok, _ = check_contract()
                    if not ok:
                        return False
                    self._contract_verified = True

                # Build a batch bounded by MAX_BATCH_BYTES (always taking at least 1 line).
                batch_lines: list[str] = []
                pending: list[dict] = []
                batch_bytes = 0
                for ln in raw_lines:
                    line_bytes = len((ln + "\n").encode("utf-8"))
                    if batch_lines and batch_bytes + line_bytes > MAX_BATCH_BYTES:
                        break
                    try:
                        parsed = json.loads(ln)
                    except Exception:  # noqa: BLE001
                        return False
                    batch_lines.append(ln)
                    pending.append(parsed)
                    batch_bytes += line_bytes

                payload = "\n".join(batch_lines) + "\n"
                rc, out, _err = delta(
                    [
                        "thread", "append", self.locator or self.thread,
                        "--source", SOURCE,
                        "--external-id", self.session_id,
                        "--producer-name", PRODUCER_NAME,
                        "--acknowledge", "local",
                    ],
                    input_text=payload,
                )

                acked = 0
                last_acked_seq: int | None = None
                fenced_turns: list[int] = []
                next_seq: int | None = None
                learned_thread_id: str | None = None

                for answer in out.strip().splitlines():
                    if acked >= len(pending):
                        break
                    try:
                        receipt = json.loads(answer)
                    except Exception:  # noqa: BLE001
                        break
                    if not isinstance(receipt, dict) or receipt.get("operation") != "appended":
                        break
                    expected = pending[acked]
                    if receipt.get("sequence") != expected.get("sequence"):
                        break
                    expected_turn = expected.get("turnSequence")
                    if receipt.get("turn_sequence") != expected_turn:
                        break
                    if receipt.get("disposition") not in ("recorded", "duplicate", "fenced"):
                        break
                    if (
                        receipt.get("disposition") == "fenced"
                        and isinstance(receipt.get("turn_sequence"), int)
                    ):
                        fenced_turns.append(receipt["turn_sequence"])
                    if isinstance(receipt.get("next_sequence"), int):
                        next_seq = receipt["next_sequence"]
                    if isinstance(receipt.get("thread_id"), str) and receipt["thread_id"]:
                        learned_thread_id = receipt["thread_id"]
                    last_acked_seq = expected["sequence"]
                    acked += 1

                if acked == 0:
                    return False

                with self._lock:
                    if learned_thread_id and self.thread != learned_thread_id:
                        self.thread = learned_thread_id
                    # Re-read the outbox under lock so records appended during the
                    # subprocess run are preserved while acknowledged records are dropped.
                    current_lines = (
                        [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
                        if path.exists()
                        else []
                    )
                    remaining: list[str] = []
                    for ln in current_lines:
                        try:
                            rec = json.loads(ln)
                            if (
                                isinstance(rec.get("sequence"), int)
                                and last_acked_seq is not None
                                and rec["sequence"] <= last_acked_seq
                            ):
                                continue
                        except Exception:  # noqa: BLE001
                            pass
                        remaining.append(ln)
                    _atomic_write_text(
                        path,
                        ("\n".join(remaining) + "\n") if remaining else "",
                    )
                    if next_seq is not None and next_seq > self.next_sequence:
                        self.next_sequence = next_seq
                    if fenced_turns:
                        if self.turn_sequence in fenced_turns:
                            self.superseded_turn_id = self.turn_id
                            self.turn_sequence = None
                            self.turn_id = None
                        self.gated = True
                    self.save()
                    done = len(remaining) == 0

                if rc != 0 or acked < len(batch_lines):
                    return False
                if done:
                    return True
        finally:
            self._flush_lock.release()

    # ---- supersession ----
    def is_turn_superseded(self, turn_id: str | None) -> bool:
        with self._lock:
            return bool(turn_id and self.superseded_turn_id == turn_id)

    def on_superseded(self, turn_sequence: int | None = None) -> None:
        should_flush = False
        with self._lock:
            if turn_sequence is not None and self.turn_sequence is not None and turn_sequence != self.turn_sequence:
                return
            self.gated = True
            if self.turn_sequence is not None:
                self._stage_locked(
                    state_update("idle", stop_reason="cancelled"),
                    turn_sequence=self.turn_sequence,
                )
                self.last_turn_sequence = self.turn_sequence
                self.superseded_turn_id = self.turn_id
                self.turn_sequence = None
                self.turn_id = None
                self._stream = self._fresh_stream(None)
                should_flush = True
            else:
                self.save()
        if should_flush:
            self.flush_async()

    # ---- turn lifecycle ----
    def start_turn(self, turn_id: str | None) -> int | None:
        if not turn_id:
            return None
        with self._lock:
            if self.superseded_turn_id == turn_id:
                return None
            if self.turn_id == turn_id and self.turn_sequence is not None:
                return self.turn_sequence
            # If a previous turn never closed, close it before opening a new one.
            if self.turn_sequence is not None:
                self._stage_locked(
                    state_update("idle", stop_reason="end_turn"),
                    turn_sequence=self.turn_sequence,
                )
                self.last_turn_sequence = self.turn_sequence
            self.turn_id = turn_id
            self.turn_sequence = self.next_sequence
            self.user_recorded_for_turn = self.injected_turn_pending
            self.injected_turn_pending = False
            self._stream = self._fresh_stream(turn_id)
            self._stage_locked(state_update("running"), turn_sequence=self.turn_sequence)
            return self.turn_sequence

    def record_user_message(self, turn_id: str | None, text: str) -> None:
        if not turn_id or not text or not text.strip():
            return
        self.start_turn(turn_id)
        with self._lock:
            if self.turn_sequence is None or self.user_recorded_for_turn:
                return
            self.user_recorded_for_turn = True
            self._stage_locked(
                text_update("user_message", text, f"{turn_id}:user"),
                turn_sequence=self.turn_sequence,
            )

    def end_turn(self, interrupted: bool = False) -> bool:
        with self._lock:
            if self.turn_sequence is None:
                self.superseded_turn_id = None
                return True
            # Flush any remaining buffered stream fragments before closing the turn.
            self._flush_stream_locked(self.turn_id)
            stop_reason = "cancelled" if interrupted else "end_turn"
            self._stage_locked(
                state_update("idle", stop_reason=stop_reason),
                turn_sequence=self.turn_sequence,
            )
            self.last_turn_sequence = self.turn_sequence
            self.turn_sequence = None
            self.turn_id = None
            self.superseded_turn_id = None
            self.user_recorded_for_turn = False
            self._stream = self._fresh_stream(None)
        ok = self.flush()
        if not ok:
            self.flush_async(delay=5.0)
        return ok

    # ---- streaming accumulation (same messageId per LLM step, full content so far) ----
    def stream_delta(self, turn_id: str | None, kind: str, delta_text: str) -> None:
        if not turn_id or not delta_text or self.is_turn_superseded(turn_id):
            return
        self.start_turn(turn_id)
        with self._lock:
            if self.turn_sequence is None:
                return
            if kind == "reasoning":
                self._stream["reasoning"] += delta_text
            else:
                self._stream["text"] += delta_text

    def _flush_stream_locked(self, turn_id: str | None) -> bool:
        if self.turn_sequence is None:
            return False
        tid = turn_id or self.turn_id or "turn"
        step = self._stream["step"]
        emitted = False
        if self._stream["reasoning"]:
            self._stage_locked(
                text_update("agent_thought", self._stream["reasoning"], f"{tid}:thought:{step}"),
                turn_sequence=self.turn_sequence,
            )
            self._stream["reasoning"] = ""
            emitted = True
        if self._stream["text"]:
            self._stage_locked(
                text_update("agent_message", self._stream["text"], f"{tid}:message:{step}"),
                turn_sequence=self.turn_sequence,
            )
            self._stream["text"] = ""
            self._stream["streamed_in_step"] = True
            emitted = True
        return emitted

    def stream_end(self, turn_id: str | None) -> None:
        if self.is_turn_superseded(turn_id):
            return
        with self._lock:
            self._flush_stream_locked(turn_id)

    def final_answer(self, turn_id: str | None, text: str) -> None:
        """post_llm_call handler: fallback when streaming did not emit text for this step,
        then advance step index so subsequent tool-loop LLM iterations get distinct messageIds."""
        if self.is_turn_superseded(turn_id):
            return
        with self._lock:
            if self.turn_sequence is None:
                return
            # If stream_delta fired without stream_end, flush buffered stream text first.
            self._flush_stream_locked(turn_id)
            if not self._stream["streamed_in_step"] and text and text.strip():
                tid = turn_id or self.turn_id or "turn"
                step = self._stream["step"]
                self._stage_locked(
                    text_update("agent_message", text, f"{tid}:message:{step}"),
                    turn_sequence=self.turn_sequence,
                )
            self._stream["step"] += 1
            self._stream["streamed_in_step"] = False


# ---------------------------------------------------------------- receiver

class WatchState:
    """Per-connection state for a single `delta thread watch` stream."""

    def __init__(self, initial_live: bool = False):
        self.ready: bool = False
        self.live: bool = initial_live
        self.items: dict[str, dict] = {}
        self.comments: dict[str, dict] = {}


class Watch:
    """Supervised `delta thread watch` subprocess with restart backoff."""

    def __init__(self, bridge: Bridge, inject):
        self.bridge = bridge
        self.inject = inject
        self._stop = threading.Event()
        self.proc: subprocess.Popen | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._terminate_proc()

    def _terminate_proc(self) -> None:
        proc = self.proc
        if proc is None:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
        except Exception:  # noqa: BLE001
            pass
        for stream in (proc.stdout, proc.stderr):
            if stream is not None:
                try:
                    stream.close()
                except Exception:  # noqa: BLE001
                    pass

    def _run(self) -> None:
        attempts = 0
        while not self._stop.is_set():
            args = [
                "thread", "watch", self.bridge.locator or self.bridge.thread,
                "--source", SOURCE,
                "--external-id", self.bridge.session_id,
                "--participate", PRODUCER_NAME,
            ]
            state = WatchState(initial_live=False)
            try:
                self.proc = subprocess.Popen(
                    [DELTA_BIN, "--json", *args],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env={**os.environ, "DELTA_CLI_INTEGRATION": SOURCE},
                )
            except Exception:  # noqa: BLE001
                delay = min(60, 2 * (2 ** min(attempts, 5)))
                attempts += 1
                self._stop.wait(delay)
                continue

            try:
                self._consume(self.proc, state)
            finally:
                self._terminate_proc()

            if self._stop.is_set():
                break
            if state.live:
                attempts = 0
            delay = min(60, 2 * (2 ** min(attempts, 5)))
            attempts += 1
            self._stop.wait(delay)

    def _consume(self, proc: subprocess.Popen, state: WatchState) -> None:
        if proc.stdout is None:
            return
        for raw in proc.stdout:
            if self._stop.is_set():
                break
            raw = raw.strip()
            if not raw:
                continue
            try:
                line = json.loads(raw)
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(line, dict):
                continue
            try:
                _handle_watch_line(line, self.bridge, self.inject, state)
            except Exception:  # noqa: BLE001
                continue


def _extract_author_info(item: dict, line: dict) -> tuple[str, str | None, str | None]:
    """Return (lowercased_name, actor_id, user_id) from a watch item or comment."""
    for candidate in (item.get("author"), item.get("producer"), line.get("author"), line.get("producer")):
        if isinstance(candidate, dict):
            name = str(candidate.get("name") or candidate.get("source") or "").strip().lower()
            actor_id = candidate.get("actor_id") if isinstance(candidate.get("actor_id"), str) else None
            user_id = candidate.get("user_id") if isinstance(candidate.get("user_id"), str) else None
            return name, actor_id, user_id
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip().lower(), None, None
    return "", None, None


def _is_own_activity(item: dict, line: dict, bridge: Bridge) -> bool:
    name, actor_id, user_id = _extract_author_info(item, line)
    if actor_id and bridge.actor_id and actor_id == bridge.actor_id:
        return True
    # Only treat matching display name as self-echo when it isn't a different human user_id
    if name in (SOURCE.lower(), PRODUCER_NAME.lower()):
        if user_id is None or (bridge.user_id and user_id == bridge.user_id):
            return True
    return False


def _is_turn_superseded_event(obj: dict) -> bool:
    status = obj.get("status")
    if isinstance(status, str) and status.lower() == "superseded":
        return True
    return bool(obj.get("superseded") or obj.get("fenced") or obj.get("lost"))


def _handle_watch_line(
    line: dict,
    bridge: Bridge,
    inject,
    state: WatchState | None = None,
) -> None:
    if state is None:
        # Direct unit-test / legacy caller without explicit WatchState treats lines as live.
        state = WatchState(initial_live=True)

    if isinstance(line.get("error"), dict):
        return

    kind = line.get("type") or line.get("kind") or ""

    if kind == "ready":
        state.ready = True
        state.live = False
        state.items.clear()
        state.comments.clear()
        if isinstance(line.get("thread_id"), str) and line["thread_id"]:
            bridge.thread = line["thread_id"]
        producer = line.get("producer")
        if isinstance(producer, dict):
            if isinstance(producer.get("actor_id"), str) and producer["actor_id"]:
                bridge.actor_id = producer["actor_id"]
            p_seq = producer.get("next_sequence")
            if isinstance(p_seq, int) and p_seq > bridge.next_sequence:
                bridge.next_sequence = p_seq
            turn = producer.get("turn")
            if isinstance(turn, dict) and _is_turn_superseded_event(turn):
                bridge.on_superseded(turn.get("sequence"))
            else:
                bridge.save()
        return

    if kind == "live":
        state.live = True
        # Record all snapshot items and comments as already seen so reconnects
        # or initial joins do not replay historical @mentions.
        changed = False
        for item_id in state.items:
            if bridge.remember_seen(item_id):
                changed = True
        for comment_id in state.comments:
            if bridge.remember_seen(f"comment:{comment_id}"):
                changed = True
        if changed:
            bridge.save()
        return

    # Supersession: our producer's turn was taken over.
    if kind == "producer.turn" or "producer" in kind:
        if _is_turn_superseded_event(line):
            seq = line.get("sequence") if isinstance(line.get("sequence"), int) else None
            bridge.on_superseded(seq)
        return

    if kind == "remove":
        item_id = line.get("id")
        if isinstance(item_id, str):
            state.items.pop(item_id, None)
        return

    if kind == "comment.remove":
        comment_id = line.get("id")
        if isinstance(comment_id, str):
            state.comments.pop(comment_id, None)
        return

    if kind in ("item.append", "append"):
        item_id = line.get("id")
        delta_text = line.get("text")
        if isinstance(item_id, str) and isinstance(delta_text, str) and item_id in state.items:
            existing = state.items[item_id]
            content = existing.get("content")
            if isinstance(content, dict):
                existing_text = content.get("text") if isinstance(content.get("text"), str) else ""
                existing["content"] = {**content, "text": existing_text + delta_text}
            elif isinstance(content, str):
                existing["content"] = content + delta_text
            # If the stream is not tracking open/close (legacy direct test call), fall through only when not open
            if existing.get("open", False) or not state.live:
                return
            item = existing
        elif state.live and isinstance(delta_text, str) and not isinstance(item_id, str):
            item = line
        else:
            return
    elif kind == "comment":
        comment = line.get("comment") if isinstance(line.get("comment"), dict) else line
        comment_id = comment.get("id") if isinstance(comment.get("id"), str) else None
        if comment_id:
            state.comments[comment_id] = comment
        if not state.live:
            if comment_id:
                bridge.remember_seen(f"comment:{comment_id}")
            return
        if comment.get("routed_to"):
            # Routed to a subthread, not this agent.
            return
        if _is_own_activity(comment, line, bridge) or comment.get("agent") is True and not _mentions_hermes(_item_text(comment), bridge):
            if _is_own_activity(comment, line, bridge):
                return
        if comment_id and not bridge.remember_seen(f"comment:{comment_id}"):
            return
        text = _item_text(comment)
        if not text:
            return
        if _mentions_hermes(text, bridge):
            with bridge._lock:
                bridge.gated = False
                bridge.superseded_turn_id = None
                bridge.injected_turn_pending = True
                bridge.save()
            inject(text)
        return
    elif kind == "item":
        item = line.get("item") or line.get("value") or line
        if not isinstance(item, dict):
            return
        item_id = item.get("id") if isinstance(item.get("id"), str) else None
        if item_id:
            state.items[item_id] = item
        if not state.live:
            if item_id and not item.get("open", False):
                bridge.remember_seen(item_id)
            return
        # Wait until streaming items finish (`open == False`) before acting on them.
        if item.get("open") is True:
            return
    else:
        return

    if not isinstance(item, dict):
        return
    item_kind = item.get("kind")
    if isinstance(item_kind, str) and item_kind in ("agent_thought", "tool_call", "unknown"):
        return
    if _is_own_activity(item, line, bridge):
        return
    item_id = item.get("id") if isinstance(item.get("id"), str) else None
    if item_id and not bridge.remember_seen(item_id):
        return  # already processed this completed item
    text = _item_text(item)
    if not text:
        return
    if _mentions_hermes(text, bridge):
        with bridge._lock:
            bridge.gated = False
            bridge.superseded_turn_id = None
            bridge.injected_turn_pending = True
            bridge.save()
        inject(text)


def _item_text(item) -> str:
    if isinstance(item, str):
        return item.strip()
    if not isinstance(item, dict):
        return ""
    for key in ("text", "content", "message", "body"):
        val = item.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        if isinstance(val, dict):
            nested = val.get("text")
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
        if isinstance(val, list):
            parts = []
            for block in val:
                if isinstance(block, dict) and isinstance(block.get("text"), str):
                    parts.append(block["text"])
            if parts:
                return "\n".join(parts).strip()
    return ""


def _mentions_hermes(text: str, bridge: Bridge | None = None) -> bool:
    lowered = text.lower()
    if bridge and bridge.user_id:
        if f"delta://user/{bridge.user_id.lower()}" in lowered:
            return True
    cfg = _load_config()
    extra = cfg.get("mentions")
    extra_terms = [str(t).strip().lower() for t in extra if str(t).strip()] if isinstance(extra, list) else []
    prod = PRODUCER_NAME.lower()
    # Explicit @-mentions or word-boundary matches for the configured producer name / hermes
    at_terms = {f"@{prod}", "@hermes", "@delta-bridge"}
    for term in extra_terms:
        at_terms.add(term if term.startswith("@") else f"@{term}")
    for at_term in at_terms:
        if re.search(rf"(?<!\w){re.escape(at_term)}\b", lowered):
            return True
    word_terms = {prod, "hermes", "delta-bridge", *(t.lstrip("@") for t in extra_terms)}
    for word in word_terms:
        if word and re.search(rf"\b{re.escape(word)}\b", lowered):
            return True
    return False

