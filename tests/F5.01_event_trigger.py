"""FB1/FB2/FB3/FB4: actual clocks/timezones, local metadata and bounded listener state."""

from datetime import datetime, timezone
from pathlib import Path
import json
import os
import sys
import tempfile
from types import MappingProxyType, SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from blocs.event_trigger import logic
from blocs.event_trigger.block import EventTriggerBlock
from bloxsmith_app.block_api import BlockRuntimeContext


def utc(value):
    """Explicit deterministic UTC clocks, independent of the test machine's timezone."""
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def rejected(call):
    """Bad policy/commands are explicit failures."""
    try:
        call()
    except (logic.TriggerError, OSError):
        return
    raise AssertionError("Invalid value accepted")


def main():
    """Test source engines without waiting for actual calendar days."""
    config = logic.configuration({"interval_sec": 2})
    assert logic.configuration(MappingProxyType({"weekdays": (0, 1), "payload": MappingProxyType({"a": (1,)})}))["payload"] == {"a": [1]}
    start = utc("2026-10-04T08:00:00")
    source = logic.Source(config, "fixture", "node", start, 100)
    assert source.tick(start, 101.9) is None
    one = source.tick(start, 102)
    assert one["type"] == "timer" and one["sequence"] == 1
    assert source.tick(start, 102) is None
    later = source.tick(start, 112)
    assert later["data"]["skipped_ticks"] == 4 and later["id"] != one["id"]
    source.control("pause", start, 113)
    assert source.tick(start, 10000) is None
    source.control("resume", start, 10001)
    assert source.tick(start, 10002.9) is None
    assert source.tick(start, 10003)["sequence"] == 3
    assert source.tick(start, 10006, lambda: True) is None
    source.close()
    daily = logic.configuration({"mode": "daily", "daily_time": "02:30", "timezone": "Europe/Paris"})
    # Spring gap has no 02:30. The next valid occurrence is the following day.
    assert logic.next_daily(daily, utc("2026-03-28T23:59:00")) == utc("2026-03-30T00:30:00")
    # Fall fold chooses the first occurrence; after that instant, never the second.
    assert logic.next_daily(daily, utc("2026-10-24T23:59:00")) == utc("2026-10-25T00:30:00")
    assert logic.next_daily(daily, utc("2026-10-25T00:45:00")) == utc("2026-10-26T01:30:00")
    weekdays = {**daily, "weekdays": [0], "daily_time": "09:00"}
    assert logic.next_daily(weekdays, utc("2026-10-04T08:00:00")) == utc("2026-10-05T07:00:00")
    day = logic.Source(daily, "r", "n", utc("2026-10-24T23:59:00"), 0)
    assert day.tick(utc("2026-10-25T00:30:00"), 1)["type"] == "calendar"
    assert day.tick(utc("2026-10-25T01:30:00"), 2) is None
    assert day.tick(utc("2026-10-24T23:59:00"), 3) is None
    day.close()
    with tempfile.TemporaryDirectory() as folder:
        directory = Path(folder)
        before = directory / "before.txt"
        before.write_text("baseline")
        subdir = directory / "nested"; subdir.mkdir()
        (subdir / "ignored.txt").write_text("ignore")
        (directory / "linked.txt").symlink_to(before)
        watcher = logic.Source(logic.configuration({"mode": "folder", "watch_directory": str(directory),
            "pattern": "*.txt", "stable_sec": 2}), "r", "n", start, 0)
        assert watcher.tick(start, 0) is None
        assert watcher.tick(start, 3) is None
        fresh = directory / "fresh.txt"
        fresh.write_text("first")
        assert watcher.tick(start, 4) is None
        fresh.write_text("second and larger")
        assert watcher.tick(start, 5) is None
        assert watcher.tick(start, 6) is None
        ready = watcher.tick(start, 7)
        assert ready["data"]["path"] == str(fresh) and ready["data"]["size"] == fresh.stat().st_size
        assert not ready["data"]["content_hashed"] and ready["type"] == "file_ready"
        assert watcher.tick(start, 9) is None
        fresh.write_text("third version is observed")
        assert watcher.tick(start, 10) is None
        assert watcher.tick(start, 12)["sequence"] == 2
        watcher.control("pause", start, 13)
        (directory / "paused.txt").write_text("ignored after resume")
        watcher.control("resume", start, 20)
        assert watcher.tick(start, 20) is None and watcher.tick(start, 25) is None
        fd = watcher.folder.fd
        watcher.close()
        try:
            os.fstat(fd)
        except OSError:
            pass
        else:
            raise AssertionError("Watcher leaked its directory descriptor")
        # Overflow is explicit and pauses; no partial batch reaches the graph.
        overloaded = logic.Source(logic.configuration({"mode": "folder", "watch_directory": str(directory),
            "include_existing": True, "max_pending": 1, "stable_sec": 1}), "r", "n", start, 0)
        assert overloaded.tick(start, 0) is None
        assert overloaded.tick(start, 2) is None
        assert overloaded.paused and overloaded.reason == "event_queue_overflow" and overloaded.discarded == 3
        overloaded.close()
        limited = logic.Source(logic.configuration({"mode": "folder", "watch_directory": str(directory),
            "max_entries": 1}), "r", "n", start, 0)
        assert limited.tick(start, 0) is None and limited.paused
        assert limited.reason == "folder_unavailable_or_limit_exceeded"
        limited.close()
        # An explicit resume may reopen a repaired/replaced directory, never silently.
        original = directory / "original"; original.mkdir()
        repair = logic.Source(logic.configuration({"mode": "folder", "watch_directory": str(original)}), "r", "n", start, 0)
        assert repair.tick(start, 0) is None
        original.rename(directory / "moved")
        original.mkdir()
        assert repair.tick(start, 2) is None and repair.paused
        repair.control("resume", start, 3)
        assert repair.tick(start, 3) is None and not repair.paused
        repair.close()
        link = directory / "alias"; link.symlink_to(directory, target_is_directory=True)
        rejected(lambda: logic.Folder({**config, "watch_directory": str(link)}))
    for raw in ({"mode": "webhook"}, {"interval_sec": True}, {"daily_time": "25:00"}, {"weekdays": [0, 0]},
                {"mode": "folder", "watch_directory": "../x"}, {"pattern": "../*"}, {"timezone": "../etc"},
                {"payload": {"x": float("nan")}}, {"unknown": 1}):
        rejected(lambda: logic.configuration(raw))
    for cmd in ("pause", '{"action":"pause","action":"resume"}', {"action": "exec"}, {"action": "pause", "path": "x"}):
        rejected(lambda: logic.command(cmd))
    assert logic.command('{"action":"resume"}') == "resume"
    block = EventTriggerBlock()
    out = (SimpleNamespace(id=9, name="event"), SimpleNamespace(id=8, name="status"))
    result = block.execute_runtime(BlockRuntimeContext(config={}, output_ports=out))
    assert result.status == "success" and [x.port_name for x in result.outputs] == ["status"]
    assert json.loads(result.outputs[0].value)["live"] is False
    prepared = block.prepare_runtime(SimpleNamespace(config=MappingProxyType(config), runtime_mode="zeromq_active"))
    assert prepared.listen_on_run
    commands = []
    class Mailbox:
        def send(self, value):
            commands.append(value)
    ctx = BlockRuntimeContext(config=config, runtime_mode="zeromq_active", output_ports=out,
        input_ports=(SimpleNamespace(id=7, name="command"),),
        input_events=(SimpleNamespace(input_port_id=7, value='{"action":"pause"}'),),
        services={"runtime_listener": Mailbox()})
    assert block.execute_runtime(ctx).status == "success" and commands == [{"action": "pause"}]
    print("[ok] Event Trigger calendar/DST, stable files, overload, commands and preview")


if __name__ == "__main__":
    main()
