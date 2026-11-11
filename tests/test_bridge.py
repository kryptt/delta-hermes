import importlib
import json
import os
import pathlib
import stat
import sys
import tempfile
import unittest
from unittest import mock


# Load the repository root as a package named `delta_hermes` so relative imports in `__init__.py` work.
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _load_package(state_dir: pathlib.Path, delta_bin: str = "delta", producer_name: str = "Hermes"):
    env_patch = {
        "DELTA_BRIDGE_STATE_DIR": str(state_dir),
        "DELTA_BIN": delta_bin,
        "DELTA_PRODUCER_NAME": producer_name,
    }
    with mock.patch.dict(os.environ, env_patch, clear=False):
        for mod_name in ("delta_hermes.bridge", "delta_hermes"):
            sys.modules.pop(mod_name, None)
        spec = importlib.util.spec_from_file_location(
            "delta_hermes",
            REPO_ROOT / "__init__.py",
            submodule_search_locations=[str(REPO_ROOT)],
        )
        pkg = importlib.util.module_from_spec(spec)
        sys.modules["delta_hermes"] = pkg
        spec.loader.exec_module(pkg)
        return pkg, pkg.bridge


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state_dir = pathlib.Path(self.tmp.name) / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.pkg, self.bridge_mod = _load_package(self.state_dir)

    def tearDown(self):
        for br in list(self.pkg._bridges.values()):
            if br.watch_proc is not None:
                br.watch_proc.stop()
        self.tmp.cleanup()

    def test_contract_check_and_git_delta_filtering(self):
        bin_dir_1 = pathlib.Path(self.tmp.name) / "bin1"
        bin_dir_2 = pathlib.Path(self.tmp.name) / "bin2"
        bin_dir_1.mkdir()
        bin_dir_2.mkdir()

        # Fake git-delta in bin1 (fails on --json thread contract)
        fake_git_delta = bin_dir_1 / "delta"
        fake_git_delta.write_text("#!/bin/sh\necho 'error: unexpected argument --json' >&2\nexit 2\n", encoding="utf-8")
        fake_git_delta.chmod(fake_git_delta.stat().st_mode | stat.S_IEXEC)

        # Fake Zed Delta CLI in bin2 (answers --json thread contract)
        fake_zed_delta = bin_dir_2 / "delta"
        fake_zed_delta.write_text(
            '#!/bin/sh\nif [ "$1" = "--json" ] && [ "$2" = "thread" ] && [ "$3" = "contract" ]; then\n'
            '  echo \'{"operation":"contract","min":8,"max":9}\'\n  exit 0\nfi\nexit 1\n',
            encoding="utf-8",
        )
        fake_zed_delta.chmod(fake_zed_delta.stat().st_mode | stat.S_IEXEC)

        env = {"PATH": f"{bin_dir_1}{os.pathsep}{bin_dir_2}"}
        with mock.patch.dict(os.environ, env, clear=False):
            os.environ.pop("DELTA_BIN", None)
            chosen = self.bridge_mod._default_delta_bin()
            self.assertEqual(chosen, str(fake_zed_delta))

    def test_parse_status_response_nested_producer(self):
        raw = json.dumps({
            "operation": "status",
            "thread_id": "thread_real_123",
            "producer": {
                "source": "hermes",
                "external_id": "sess_1",
                "actor_id": "actor_abc",
                "next_sequence": 42,
                "turn": None,
            },
        })
        tid, next_seq, actor_id = self.bridge_mod.parse_status_response(raw, "https://delta.dev/join/share_xyz")
        self.assertEqual(tid, "thread_real_123")
        self.assertEqual(next_seq, 42)
        self.assertEqual(actor_id, "actor_abc")

    def test_flush_pipes_stdin_and_parses_receipts(self):
        br = self.bridge_mod.Bridge("sess_flush", "thread_1", next_sequence=1)
        br.start_turn("t1")
        br.record_user_message("t1", "Hello world")
        br.stream_delta("t1", "reasoning", "thinking...")
        br.stream_delta("t1", "text", "Hi there!")
        br.stream_end("t1")
        br.final_answer("t1", "Hi there!")

        lines = [json.loads(ln) for ln in br.outbox_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        # Expect exactly 4 records: running (1), user_message (2), thought (3), agent_message (4)
        # (final_answer must NOT emit a duplicate record after stream_end!)
        self.assertEqual(len(lines), 4)
        self.assertEqual([r["sequence"] for r in lines], [1, 2, 3, 4])
        self.assertTrue(all(r["turnSequence"] == 1 for r in lines))
        self.assertEqual(lines[0]["update"]["sessionUpdate"], "state_update")
        self.assertEqual(lines[1]["update"]["sessionUpdate"], "user_message")
        self.assertEqual(lines[2]["update"]["sessionUpdate"], "agent_thought")
        self.assertEqual(lines[3]["update"]["sessionUpdate"], "agent_message")

        captured_stdin = []

        def fake_delta(args, input_text=None, timeout=90):
            if args[:2] == ["thread", "contract"]:
                return 0, '{"operation":"contract","min":8,"max":9}\n', ""
            if args[:2] == ["thread", "append"]:
                captured_stdin.append(input_text)
                sent = [json.loads(ln) for ln in (input_text or "").splitlines() if ln.strip()]
                receipts = []
                for rec in sent:
                    receipts.append(json.dumps({
                        "operation": "appended",
                        "thread_id": "thread_1",
                        "sequence": rec["sequence"],
                        "next_sequence": rec["sequence"] + 1,
                        "duplicate": False,
                        "disposition": "recorded",
                        "turn_sequence": rec.get("turnSequence"),
                        "version": "actor.1",
                    }))
                return 0, "\n".join(receipts) + "\n", ""
            return 1, "", "unexpected"

        with mock.patch.object(self.bridge_mod, "delta", side_effect=fake_delta):
            ok = br.flush()
            self.assertTrue(ok)
            self.assertEqual(len(captured_stdin), 1)
            self.assertIsNotNone(captured_stdin[0])
            self.assertEqual(br.outbox_path.read_text(encoding="utf-8"), "")
            self.assertEqual(br.next_sequence, 5)

    def test_multi_iteration_tool_loop_distinct_message_ids(self):
        br = self.bridge_mod.Bridge("sess_loop", "thread_1", next_sequence=1)
        with mock.patch.object(br, "flush_async"):
            br.start_turn("t_loop")
            # Step 0: LLM streams prelude + calls tool
            br.stream_delta("t_loop", "text", "Let me check that file.")
            br.stream_end("t_loop")
            br.final_answer("t_loop", "Let me check that file.")
            br.append(
                self.bridge_mod.tool_update("call_1", "read_file", "in_progress", raw_input={"path": "a.py"}),
                turn_sequence=br.turn_sequence,
            )
            br.append(
                self.bridge_mod.tool_update("call_1", "read_file", "completed", raw_output="print(1)"),
                turn_sequence=br.turn_sequence,
            )
            # Step 1: LLM streams final response
            br.stream_delta("t_loop", "text", "The file prints 1.")
            br.stream_end("t_loop")
            br.final_answer("t_loop", "The file prints 1.")

        records = [json.loads(ln) for ln in br.outbox_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        msg_records = [r["update"] for r in records if r["update"]["sessionUpdate"] == "agent_message"]
        self.assertEqual(len(msg_records), 2)
        self.assertEqual(msg_records[0]["messageId"], "t_loop:message:0")
        self.assertEqual(msg_records[0]["content"][0]["text"], "Let me check that file.")
        self.assertEqual(msg_records[1]["messageId"], "t_loop:message:1")
        self.assertEqual(msg_records[1]["content"][0]["text"], "The file prints 1.")

    def test_watch_snapshot_vs_live_and_dict_author_and_streaming_items(self):
        br = self.bridge_mod.Bridge("sess_watch", "thread_1", next_sequence=1, actor_id="actor_self")
        injected = []
        state = self.bridge_mod.WatchState(initial_live=False)

        # 1. Ready event
        self.bridge_mod._handle_watch_line(
            {
                "type": "ready",
                "thread_id": "thread_1",
                "producer": {
                    "source": "hermes",
                    "external_id": "sess_watch",
                    "actor_id": "actor_self",
                    "next_sequence": 5,
                    "turn": None,
                },
            },
            br,
            injected.append,
            state,
        )
        self.assertEqual(br.next_sequence, 5)

        # 2. Historical snapshot item mentioning @Hermes (must NOT be injected!)
        self.bridge_mod._handle_watch_line(
            {
                "type": "item",
                "item": {
                    "id": "item_old_1",
                    "kind": "user_message",
                    "after_id": None,
                    "turn_id": "turn_0",
                    "author": {"user_id": "user_ada", "name": "Ada", "actor_id": "actor_ada"},
                    "created_at": 1000,
                    "open": False,
                    "content": {"text": "Hey @Hermes old snapshot request"},
                },
            },
            br,
            injected.append,
            state,
        )
        self.assertEqual(injected, [])

        # 3. Live boundary
        self.bridge_mod._handle_watch_line({"type": "live"}, br, injected.append, state)
        self.assertTrue(state.live)
        self.assertTrue(br.is_seen("item_old_1"))

        # 4. Self-echo item after live (must be ignored by actor_id)
        self.bridge_mod._handle_watch_line(
            {
                "type": "item",
                "item": {
                    "id": "item_self_1",
                    "kind": "agent_message",
                    "after_id": "item_old_1",
                    "turn_id": "turn_1",
                    "author": {"user_id": None, "name": "Hermes", "actor_id": "actor_self"},
                    "created_at": 1001,
                    "open": False,
                    "content": {"text": "I am @Hermes"},
                },
            },
            br,
            injected.append,
            state,
        )
        self.assertEqual(injected, [])

        # 5. Streaming live item from human: open=True -> item.append -> open=False
        self.bridge_mod._handle_watch_line(
            {
                "type": "item",
                "item": {
                    "id": "item_live_1",
                    "kind": "user_message",
                    "after_id": "item_self_1",
                    "turn_id": "turn_2",
                    "author": {"user_id": "user_ada", "name": "Ada", "actor_id": "actor_ada"},
                    "created_at": 1002,
                    "open": True,
                    "content": {"text": "Hey @Hermes"},
                },
            },
            br,
            injected.append,
            state,
        )
        # Still open -> not injected yet
        self.assertEqual(injected, [])

        self.bridge_mod._handle_watch_line(
            {"type": "item.append", "id": "item_live_1", "text": " please review bridge.py"},
            br,
            injected.append,
            state,
        )
        self.assertEqual(injected, [])

        # Item closes with full text
        self.bridge_mod._handle_watch_line(
            {
                "type": "item",
                "item": {
                    "id": "item_live_1",
                    "kind": "user_message",
                    "after_id": "item_self_1",
                    "turn_id": "turn_2",
                    "author": {"user_id": "user_ada", "name": "Ada", "actor_id": "actor_ada"},
                    "created_at": 1002,
                    "open": False,
                    "content": {"text": "Hey @Hermes please review bridge.py"},
                },
            },
            br,
            injected.append,
            state,
        )
        self.assertEqual(injected, ["Hey @Hermes please review bridge.py"])

        # Re-delivery of the same closed item_live_1 must be deduplicated
        self.bridge_mod._handle_watch_line(
            {
                "type": "item",
                "item": {
                    "id": "item_live_1",
                    "kind": "user_message",
                    "after_id": "item_self_1",
                    "turn_id": "turn_2",
                    "author": {"user_id": "user_ada", "name": "Ada", "actor_id": "actor_ada"},
                    "created_at": 1002,
                    "open": False,
                    "content": {"text": "Hey @Hermes please review bridge.py"},
                },
            },
            br,
            injected.append,
            state,
        )
        self.assertEqual(len(injected), 1)

    def test_producer_turn_supersession_gates_and_closes_turn(self):
        br = self.bridge_mod.Bridge("sess_sup", "thread_1", next_sequence=10)
        with mock.patch.object(br, "flush_async"):
            turn_seq = br.start_turn("turn_active")
            self.assertEqual(turn_seq, 10)
            self.bridge_mod._handle_watch_line(
                {"type": "producer.turn", "sequence": 10, "status": "superseded"},
                br,
                lambda _: None,
            )
            self.assertTrue(br.gated)
            self.assertIsNone(br.turn_sequence)
            self.assertTrue(br.is_turn_superseded("turn_active"))

            # Further streaming or tool calls on the superseded turn are ignored
            br.stream_delta("turn_active", "text", "ignored late output")
            br.stream_end("turn_active")

        records = [json.loads(ln) for ln in br.outbox_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["update"], {"sessionUpdate": "state_update", "state": "running"})
        self.assertEqual(
            records[1]["update"],
            {"sessionUpdate": "state_update", "state": "idle", "stopReason": "cancelled"},
        )

    def test_config_json_does_not_break_startup_reattachment(self):
        # Write a config.json and a valid session binding in STATE_DIR
        (self.state_dir / "config.json").write_text(
            json.dumps({"producer_name": "Dave"}), encoding="utf-8"
        )
        br = self.bridge_mod.Bridge("sess_valid", "thread_99", next_sequence=7)
        br.save()

        fake_ctx = mock.MagicMock()
        with mock.patch.object(self.pkg, "_start_watch") as mock_watch, \
             mock.patch.object(self.bridge_mod.Bridge, "flush_async"):
            self.pkg._bridges.clear()
            self.pkg.register(fake_ctx)
            self.assertIn("sess_valid", self.pkg._bridges)
            self.assertNotIn("config", self.pkg._bridges)
            mock_watch.assert_called_once()

    def test_orphan_turn_recovery_and_fenced_receipt(self):
        br = self.bridge_mod.Bridge("sess_orphan", "thread_1", next_sequence=1)
        br.start_turn("t_crash")
        # Simulate process crash mid-turn by reloading from disk without calling end_turn
        reloaded = self.bridge_mod.Bridge.load("sess_orphan")
        self.assertIsNotNone(reloaded)
        self.assertIsNone(reloaded.turn_sequence)
        self.assertEqual(reloaded.last_turn_sequence, 1)
        records = [json.loads(ln) for ln in reloaded.outbox_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["update"], {"sessionUpdate": "state_update", "state": "running"})
        self.assertEqual(
            records[1]["update"],
            {"sessionUpdate": "state_update", "state": "idle", "stopReason": "cancelled"},
        )

        # Now start a new turn and receive a fenced receipt during flush
        reloaded.start_turn("t_fenced")
        reloaded._contract_verified = True

        def fake_fenced_delta(args, input_text=None, timeout=90):
            sent = [json.loads(ln) for ln in (input_text or "").splitlines() if ln.strip()]
            receipts = []
            for rec in sent:
                disp = "fenced" if rec["sequence"] == 3 else "recorded"
                receipts.append(json.dumps({
                    "operation": "appended",
                    "thread_id": "thread_1",
                    "sequence": rec["sequence"],
                    "next_sequence": rec["sequence"] + 1,
                    "duplicate": False,
                    "disposition": disp,
                    "turn_sequence": rec.get("turnSequence"),
                    "version": "actor.2",
                }))
            return 0, "\n".join(receipts) + "\n", ""

        with mock.patch.object(self.bridge_mod, "delta", side_effect=fake_fenced_delta):
            self.assertTrue(reloaded.flush())
            self.assertTrue(reloaded.gated)
            self.assertIsNone(reloaded.turn_sequence)
            self.assertTrue(reloaded.is_turn_superseded("t_fenced"))

    def test_commands_join_status_leave_and_native_user_mention(self):
        def fake_delta(args, input_text=None, timeout=90):
            if args[:2] == ["thread", "contract"]:
                return 0, '{"operation":"contract","min":8,"max":11}\n', ""
            if args[:2] == ["thread", "status"]:
                return 0, json.dumps({
                    "operation": "status",
                    "thread_id": "thread_joined_1",
                    "producer": {
                        "source": "hermes",
                        "external_id": "sess_cmd",
                        "actor_id": "actor_hermes",
                        "next_sequence": 12,
                        "turn": None,
                    },
                }), ""
            return 0, "", ""

        with mock.patch.object(self.bridge_mod, "delta", side_effect=fake_delta), \
             mock.patch.object(self.pkg, "delta", side_effect=fake_delta), \
             mock.patch.object(self.pkg, "_start_watch"):
            join_msg = self.pkg._cmd_delta_join(
                "https://delta.dev/join/share_abc",
                session_id="sess_cmd",
                session_key="tg:123:sess_cmd",
            )
            self.assertIn("thread_joined_1", join_msg)
            self.assertIn("next sequence 12", join_msg)

            status_msg = self.pkg._cmd_delta_status("", session_id="sess_cmd")
            self.assertIn("thread=thread_joined_1", status_msg)
            self.assertIn("next_sequence=12", status_msg)

            br = self.pkg._bridges["sess_cmd"]
            br.user_id = "user_bot_77"
            injected = []
            # Native Delta user mention [@](delta://user/user_bot_77) in a comment
            self.bridge_mod._handle_watch_line(
                {
                    "type": "comment",
                    "comment": {
                        "id": "c_1",
                        "chain": "c_1",
                        "after_id": None,
                        "delivered_with": "item_1",
                        "routed_to": None,
                        "author": {"user_id": "user_human", "name": "Alice", "actor_id": "actor_alice"},
                        "co_authors": [],
                        "agent": False,
                        "created_at": 2000,
                        "text": "Please check this [@](delta://user/user_bot_77)",
                        "target": None,
                    },
                },
                br,
                injected.append,
            )
            self.assertEqual(injected, ["Please check this [@](delta://user/user_bot_77)"])
            self.assertTrue(br.injected_turn_pending)

            # When the injected turn runs, it must not re-record the remote comment/message as a duplicate user_message
            self.pkg._on_pre_llm_call(
                session_id="sess_cmd",
                turn_id="turn_from_watch",
                user_message="Please check this [@](delta://user/user_bot_77)",
            )
            records = [json.loads(ln) for ln in br.outbox_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["update"]["sessionUpdate"], "state_update")

            leave_msg = self.pkg._cmd_delta_leave("", session_id="sess_cmd")
            self.assertIn("Unbridged session sess_cmd", leave_msg)
            self.assertFalse(br.binding_path.exists())


if __name__ == "__main__":
    unittest.main()
