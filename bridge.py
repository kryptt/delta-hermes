"""Delta ACP bridge core.

Per-session state (binding + outbox), record building, delivery via
`delta thread append`, and the `thread watch` receiver with standby/@mention
injection and cooperative supersession gating.

Protocol facts (from the delta CLI contract, see
delta-workspace skill references/plugin-contract.md):
- Records are schema-1 JSONL envelopes {schemaVersion:1, sequence, turnSequence?,
  update:{sessionUpdate,...}} piped to `delta --json thread append`.
- Sequences are monotonic per thread per producer; recover `next` from
  `delta --json thread status`. Never resend an acknowledged record.
- A live turn is `state_update running` with turnSequence == its sequence,
  then all content and the final `state_update idle` carry that same turnSequence.
- Streaming: re-send the same messageId with the full content so far.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import threading
import time


def _default_state_dir():
    env = os.environ.get("DELTA_BRIDGE_STATE_DIR")
    if env:
        return pathlib.Path(env)
    home = pathlib.Path(os.environ.get("HERMES_HOME", pathlib.Path.home()))
    return home / "delta-bridge"


def _default_delta_bin():
    env = os.environ.get("DELTA_BIN")
    if env:
        return env
    found = shutil.which("delta")
    if found:
        return found
    return str(pathlib.Path.home() / ".local" / "bin" / "delta")


def _default_producer_name():
    """Per-profile identity for Delta records: who this agent shows as.

    Same plugin code serves every profile (Dave, Simon, ...), so the name is
    NEVER hardcoded. Resolution order:
      1. DELTA_PRODUCER_NAME env var
      2. `producer_name` in $HERMES_HOME/delta-bridge/config.json
      3. capitalized basename of HERMES_HOME (profile dir), when set
      4. "Hermes" (generic fallback)
    """
    env = os.environ.get("DELTA_PRODUCER_NAME")
    if env:
        return env
    home = pathlib.Path(os.environ.get("HERMES_HOME", pathlib.Path.home()))
    cfg = home / "delta-bridge" / "config.json"
    try:
        name = json.loads(cfg.read_text()).get("producer_name")
        if name:
            return str(name)
    except Exception:
        pass
    if os.environ.get("HERMES_HOME"):
        base = home.name
        if base and base != "home":
            return base[:1].upper() + base[1:]
    return "Hermes"


STATE_DIR = _default_state_dir()
DELTA_BIN = _default_delta_bin()
SOURCE = "hermes"
PRODUCER_NAME = _default_producer_name()
MAX_BATCH_BYTES = 2_000_000


def delta(args, timeout=90):
    """Run the delta CLI. Returns (rc, stdout, stderr). Never raises."""
    try:
        proc = subprocess.run(
            [DELTA_BIN, "--json", *args],
            capture_output=True, text=True, timeout=timeout,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except Exception as exc:  # noqa: BLE001
        return -1, "", f"delta invocation failed: {exc!r}"


# ---------------------------------------------------------------- records

def envelope(sequence, update, turn_sequence=None):
    rec = {"schemaVersion": 1, "sequence": sequence, "update": update}
    if turn_sequence is not None:
        rec["turnSequence"] = turn_sequence
    return rec


def text_update(kind, text, message_id):
    return {
        "sessionUpdate": kind,
        "messageId": message_id,
        "content": [{"type": "text", "text": text}],
    }


def tool_update(tool_call_id, name, status, raw_input=None, raw_output=None, error=None):
    upd = {
        "sessionUpdate": "tool_call_update",
        "toolCallId": tool_call_id,
        "name": name,
        "status": status,
    }
    if raw_input is not None:
        upd["rawInput"] = raw_input
    if raw_output is not None:
        upd["rawOutput"] = raw_output
    if error is not None:
        upd["rawOutput"] = error
    return upd


def state_update(state):
    return {"sessionUpdate": "state_update", "state": state}


# ---------------------------------------------------------------- bridge

class Bridge:
    """One Hermes session <-> one Delta thread."""

    def __init__(self, session_id, thread, next_sequence=1,
                 last_turn_sequence=None, gated=False):
        self.session_id = session_id
        self.thread = thread
        self.next_sequence = next_sequence
        self.last_turn_sequence = last_turn_sequence
        self.gated = gated
        self.turn_id = None
        self.turn_sequence = None
        self._stream = {"turn_id": None, "text": "", "reasoning": ""}
        self.watch_proc = None
        self._watch_thread = None
        self._lock = threading.Lock()
        STATE_DIR.mkdir(parents=True, exist_ok=True)

    # ---- persistence ----
    @property
    def binding_path(self):
        return STATE_DIR / f"{self.session_id}.json"

    @property
    def outbox_path(self):
        return STATE_DIR / f"{self.session_id}.outbox.jsonl"

    def save(self):
        data = {
            "thread": self.thread,
            "next_sequence": self.next_sequence,
            "last_turn_sequence": self.last_turn_sequence,
            "gated": self.gated,
        }
        with self.binding_path.open("w") as f:
            json.dump(data, f, indent=2)

    @classmethod
    def load(cls, session_id):
        path = STATE_DIR / f"{session_id}.json"
        if not path.exists():
            return None
        with path.open() as f:
            data = json.load(f)
        return cls(session_id, data["thread"], data.get("next_sequence", 1),
                   data.get("last_turn_sequence"), data.get("gated", False))

    # ---- appending + delivery ----
    def append(self, update, turn_sequence=None):
        with self._lock:
            seq = self.next_sequence
            self.next_sequence += 1
            rec = envelope(seq, update, turn_sequence)
            with self.outbox_path.open("a") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.save()
            return rec

    def flush(self):
        """Deliver pending records; drop acknowledged prefix; keep the rest.
        Returns True when the whole outbox was acknowledged."""
        path = self.outbox_path
        if not path.exists():
            return True
        with self._lock:
            lines = path.read_text().splitlines()
            if not lines:
                return True
            batch = lines
            if len(batch) == 0:
                return True
            rc, out, err = delta([
                "thread", "append", self.thread,
                "--source", SOURCE, "--external-id", self.session_id,
                "--producer-name", PRODUCER_NAME,
                "--acknowledge", "local",
            ])
            if rc != 0:
                return False
            acked = 0
            fenced_turns = []
            next_seq = None
            pending = [json.loads(ln) for ln in batch]
            for answer in out.strip().splitlines():
                if acked >= len(pending):
                    break
                try:
                    receipt = json.loads(answer)
                except Exception:  # noqa: BLE001
                    break
                if receipt.get("sequence") != pending[acked]["sequence"]:
                    break
                if (receipt.get("disposition") == "fenced"
                        and receipt.get("turn_sequence") is not None):
                    fenced_turns.append(receipt["turn_sequence"])
                if isinstance(receipt.get("next_sequence"), int):
                    next_seq = receipt["next_sequence"]
                acked += 1
            remaining = batch[acked:]
            path.write_text("\n".join(remaining) + ("\n" if remaining else ""))
            if next_seq is not None and next_seq > self.next_sequence:
                self.next_sequence = next_seq
                self.save()
            if fenced_turns:
                self.gated = True
                self.save()
            return acked == len(batch)

    # ---- turn lifecycle ----
    def start_turn(self, turn_id):
        if self.turn_id == turn_id and self.turn_sequence is not None:
            return self.turn_sequence
        self.turn_id = turn_id
        self.turn_sequence = self.next_sequence
        self._stream = {"turn_id": turn_id, "text": "", "reasoning": ""}
        self.append(state_update("running"), turn_sequence=self.turn_sequence)
        return self.turn_sequence

    def end_turn(self, interrupted=False):
        if self.turn_sequence is None:
            return
        self.append(state_update("idle"), turn_sequence=self.turn_sequence)
        self.last_turn_sequence = self.turn_sequence
        self.turn_sequence = None
        self.turn_id = None
        self._stream = {"turn_id": None, "text": "", "reasoning": ""}
        self.flush()

    # ---- streaming accumulation (same messageId, full content so far) ----
    def stream_delta(self, turn_id, kind, delta_text):
        self.start_turn(turn_id)
        if kind == "reasoning":
            self._stream["reasoning"] += delta_text
        else:
            self._stream["text"] += delta_text

    def stream_end(self, turn_id):
        if self._stream["reasoning"]:
            self.append(text_update("agent_thought", self._stream["reasoning"],
                                    f"{turn_id}:thought:0"),
                        turn_sequence=self.turn_sequence)
            self._stream["reasoning"] = ""
        if self._stream["text"]:
            self.append(text_update("agent_message", self._stream["text"],
                                    f"{turn_id}:message:0"),
                        turn_sequence=self.turn_sequence)
            self._stream["text"] = ""

    def final_answer(self, turn_id, text):
        # post_llm_call fallback when streaming never fired
        if not self._stream["text"]:
            self.append(text_update("agent_message", text, f"{turn_id}:message:0"),
                        turn_sequence=self.turn_sequence)


# ---------------------------------------------------------------- receiver

class Watch:
    """Supervised `delta thread watch` subprocess with restart backoff."""

    def __init__(self, bridge, inject):
        self.bridge = bridge
        self.inject = inject
        self._stop = threading.Event()
        self.proc = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self._stop.set()
        if self.proc is not None:
            try:
                self.proc.terminate()
            except Exception:  # noqa: BLE001
                pass

    def _run(self):
        while not self._stop.is_set():
            args = [
                "thread", "watch", self.bridge.thread,
                "--source", SOURCE, "--external-id", self.bridge.session_id,
                "--participate", PRODUCER_NAME,
            ]
            try:
                self.proc = subprocess.Popen(
                    [DELTA_BIN, "--json", *args],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                )
            except Exception as exc:  # noqa: BLE001
                time.sleep(5)
                continue
            self._consume(self.proc)
            if self._stop.is_set():
                break
            time.sleep(5)  # restart backoff after an unexpected exit

    def _consume(self, proc):
        try:
            for raw in proc.stdout:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    line = json.loads(raw)
                except Exception:  # noqa: BLE001
                    continue
                _handle_watch_line(line, self.bridge, self.inject)
        except Exception:  # noqa: BLE001
            pass
        finally:
            try:
                proc.stdout.close()
            except Exception:  # noqa: BLE001
                pass


def _handle_watch_line(line, bridge, inject):
    kind = line.get("kind") or line.get("type") or ""
    # Supersession: our producer's turn was taken over.
    if kind == "producer.turn" or "producer" in kind:
        if line.get("superseded") or line.get("fenced") or line.get("lost"):
            bridge.gated = True
            bridge.save()
        return
    # Human activity: an item (user_message) from a human, or an append.
    if kind not in ("item", "item.append", "append", "comment"):
        return
    item = line.get("item") or line.get("value") or line
    producer = (item.get("producer") or item.get("author") or
                line.get("producer") or "").lower()
    if producer == SOURCE or producer == PRODUCER_NAME.lower():
        return  # our own output echoes back
    text = _item_text(item)
    if not text:
        return
    if _mentions_hermes(text):
        bridge.gated = False
        bridge.save()
        inject(text)


def _item_text(item):
    if isinstance(item, str):
        return item
    if not isinstance(item, dict):
        return ""
    for key in ("text", "content", "message", "body"):
        val = item.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        if isinstance(val, list):
            parts = []
            for block in val:
                if isinstance(block, dict) and isinstance(block.get("text"), str):
                    parts.append(block["text"])
            if parts:
                return "\n".join(parts).strip()
    return ""


def _mentions_hermes(text):
    lowered = text.lower()
    # Per-profile agent name (@Dave, @Simon, ...) plus the shared/generic
    # terms; PRODUCER_NAME is per-profile config, never hardcoded.
    terms = {"@hermes", "hermes", "@queque", "queque", "delta-bridge",
             f"@{PRODUCER_NAME.lower()}", PRODUCER_NAME.lower()}
    return any(term in lowered for term in terms)
