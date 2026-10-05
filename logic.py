"""Run-local bounded event sources. Never execute file contents or start private HTTP routes."""

from collections import deque
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import fnmatch
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULTS = {"mode": "interval", "interval_sec": 60, "daily_time": "09:00", "timezone": "UTC",
            "weekdays": [0, 1, 2, 3, 4, 5, 6], "watch_directory": "", "pattern": "*", "stable_sec": 2,
            "poll_sec": 1, "include_existing": False, "start_paused": False,
            "max_entries": 1000, "max_pending": 64, "emit_per_sec": 2, "payload": {}}
UTC = timezone.utc


class TriggerError(ValueError):
    """An explicit source/configuration diagnostic that does not expose file contents."""


def thaw(value):
    """Preparation/listener configuration uses recursively immutable public snapshots."""
    if isinstance(value, Mapping):
        return {k: thaw(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [thaw(v) for v in value]
    return value


def json_object(value, limit):
    """Bound static event data and commands; reject unknown representations and duplicate keys."""
    if isinstance(value, str):
        if len(value.encode("utf-8")) > limit:
            raise TriggerError("JSON exceeds its byte limit.")
        def pairs(items):
            result = {}
            for key, val in items:
                if key in result:
                    raise TriggerError("Duplicate JSON property.")
                result[key] = val
            return result
        try:
            value = json.loads(value, object_pairs_hook=pairs)
        except (ValueError, RecursionError) as exc:
            raise TriggerError("A strict JSON object is required.") from exc
    if not isinstance(value, dict):
        raise TriggerError("A JSON object is required.")
    stack, count = [(value, 0)], 0
    while stack:
        item, depth = stack.pop()
        count += 1
        if depth > 12 or count > 1000:
            raise TriggerError("JSON structure is too deep or large.")
        if isinstance(item, dict):
            if any(not isinstance(k, str) for k in item):
                raise TriggerError("JSON keys must be strings.")
            stack.extend((x, depth + 1) for x in item.values())
        elif isinstance(item, list):
            stack.extend((x, depth + 1) for x in item)
        elif item is not None and type(item) not in (str, bool, int, float):
            raise TriggerError("A JSON value is required.")
    try:
        serialized = json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True)
    except (TypeError, ValueError, RecursionError) as exc:
        raise TriggerError("Finite JSON is required.") from exc
    if len(serialized.encode("ascii")) > limit:
        raise TriggerError("JSON exceeds its byte limit.")
    return json.loads(serialized)


def configuration(raw):
    """Pure validation; preparation never opens directories or timezone data files."""
    if not isinstance(raw, Mapping) or set(raw) - set(DEFAULTS) - {"execution", "runtime_path", "runtime_path_label"}:
        raise TriggerError("Unknown setting.")
    config = {**DEFAULTS, **{key: thaw(value) for key, value in raw.items() if key in DEFAULTS}}
    if not isinstance(config["mode"], str) or config["mode"] not in {"interval", "daily", "folder"}:
        raise TriggerError("Choose interval, daily or folder. Webhooks need a public framework ingress contract.")
    for key, low, high in (("interval_sec", 1, 86400), ("stable_sec", 1, 300), ("poll_sec", 1, 60),
                          ("max_entries", 1, 10000), ("max_pending", 1, 128), ("emit_per_sec", 1, 10)):
        if type(config[key]) is not int or not low <= config[key] <= high:
            raise TriggerError(f"{key} must be an integer between {low} and {high}.")
    for key in ("include_existing", "start_paused"):
        if type(config[key]) is not bool:
            raise TriggerError(f"{key} must be boolean.")
    if not isinstance(config["daily_time"], str) or not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", config["daily_time"]):
        raise TriggerError("daily_time must use 24-hour HH:MM.")
    zone = config["timezone"]
    if not isinstance(zone, str) or not 1 <= len(zone) <= 128 or not re.fullmatch(r"[A-Za-z0-9_+/-]+", zone) or ".." in zone or zone.startswith("/"):
        raise TriggerError("An IANA timezone name is required.")
    days = config["weekdays"]
    if not isinstance(days, list) or not 1 <= len(days) <= 7 or any(type(x) is not int or not 0 <= x <= 6 for x in days) or len(set(days)) != len(days):
        raise TriggerError("weekdays must contain unique integers from 0 (Monday) to 6 (Sunday).")
    folder = config["watch_directory"]
    if not isinstance(folder, str) or len(folder) > 4096 or "\x00" in folder:
        raise TriggerError("Invalid watch_directory.")
    if config["mode"] == "folder" and (not folder or not Path(folder).is_absolute() or ".." in Path(folder).parts):
        raise TriggerError("Folder mode requires an explicit absolute local directory.")
    pattern = config["pattern"]
    if not isinstance(pattern, str) or not 1 <= len(pattern) <= 128 or any(x in pattern for x in ("/", "\\", "\x00")):
        raise TriggerError("pattern must be a simple filename pattern.")
    config["payload"] = json_object(config["payload"], 4096)
    return config


def command(value):
    """Data controls only: no reconfiguration, shell expressions or hidden overrides."""
    result = json_object(value, 1024)
    if set(result) != {"action"} or not isinstance(result["action"], str) or result["action"] not in {"pause", "resume", "status"}:
        raise TriggerError("Commands are {action: pause}, {action: resume} or {action: status}.")
    return result["action"]


def next_daily(config, after, last_date=""):
    """Skip nonexistent local times; pick fold=0 once, with no catch-up history."""
    try:
        zone = ZoneInfo(config["timezone"])
    except ZoneInfoNotFoundError as exc:
        raise TriggerError("Timezone data is unavailable for the configured zone.") from exc
    hour, minute = map(int, config["daily_time"].split(":"))
    local = after.astimezone(zone)
    for offset in range(370):
        day = local.date() + timedelta(days=offset)
        if day.weekday() not in config["weekdays"] or day.isoformat() <= last_date:
            continue
        wall = datetime(day.year, day.month, day.day, hour, minute, tzinfo=zone, fold=0)
        candidate = wall.astimezone(UTC)
        if candidate.astimezone(zone).replace(tzinfo=None) != wall.replace(tzinfo=None):
            continue
        if candidate > after:
            return candidate
    raise TriggerError("No calendar occurrence found within one year.")


class Folder:
    """Bounded nonrecursive metadata snapshots through a held canonical directory descriptor."""

    def __init__(self, config):
        self.config = config
        self.path = Path(config["watch_directory"])
        if self.path.resolve() != self.path or not self.path.is_dir():
            raise TriggerError("The watch directory must exist without symlink components.")
        self.fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        info = os.fstat(self.fd)
        self.identity = (info.st_dev, info.st_ino)

    def close(self):
        """Release the only external handle owned by the watcher."""
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def snapshot(self, stop=lambda: False):
        """Observe all entries before accepting a snapshot; never return a partial scan."""
        info = self.path.stat(follow_symlinks=False)
        if (info.st_dev, info.st_ino) != self.identity or not stat.S_ISDIR(info.st_mode):
            raise TriggerError("The watched directory identity changed; reload is required.")
        result, examined = {}, 0
        with os.scandir(self.fd) as entries:
            for entry in entries:
                if stop():
                    raise TriggerError("Directory scan cancelled.")
                examined += 1
                if examined > self.config["max_entries"]:
                    raise TriggerError("Directory entry limit exceeded; detection paused.")
                if not fnmatch.fnmatchcase(entry.name, self.config["pattern"]):
                    continue
                try:
                    state = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(state.st_mode) or state.st_nlink != 1:
                    continue
                # Filename decoding must be representable in the event contract.
                entry.name.encode("utf-8")
                result[entry.name] = (state.st_dev, state.st_ino, state.st_size, state.st_mtime_ns, state.st_ctime_ns)
        return result


class StableFiles:
    """In-memory per-Run signatures: a stable file version emits at most once while present."""

    def __init__(self, include_existing, stable_sec):
        self.include_existing, self.stable_sec = include_existing, stable_sec
        self.initialized = False
        self.observed = {}

    def reset(self):
        """Resume takes a fresh ignored baseline; events from a pause are not replayed."""
        self.initialized = False
        self.include_existing = False
        self.observed = {}

    def observe(self, snapshot, monotonic):
        """At least two matching observations and a minimum dwell time are required."""
        first = not self.initialized
        self.initialized = True
        rows, ready = {}, []
        for name in sorted(snapshot):
            signature = snapshot[name]
            old = self.observed.get(name)
            if old is None or old["signature"] != signature:
                row = {"signature": signature, "since": monotonic, "emitted": first and not self.include_existing}
            else:
                row = dict(old)
                if not row["emitted"] and monotonic - row["since"] >= self.stable_sec:
                    ready.append((name, signature))
                    row["emitted"] = True
            rows[name] = row
        self.observed = rows
        return ready


class Source:
    """One listener owns schedule, dedup state and a bounded event queue."""

    def __init__(self, config, run_id, node_id, now, monotonic):
        self.config, self.run_id, self.node_id = config, str(run_id), str(node_id)
        self.paused = config["start_paused"]
        self.pending, self.sequence = deque(), 0
        self.discarded = 0
        self.reason = ""
        self.last_date = ""
        self.next_tick = monotonic + config["interval_sec"]
        self.next_calendar = next_daily(config, now) if config["mode"] == "daily" else None
        self.next_poll, self.next_emit = monotonic, monotonic
        self.files = StableFiles(config["include_existing"] and not self.paused, config["stable_sec"])
        self.folder = None

    def close(self):
        """Close resources even when output transport is revoked during Stop."""
        if self.folder:
            self.folder.close()

    def state(self):
        """Expose counters and schedule, never file names or payload contents in metadata."""
        return {"mode": self.config["mode"], "paused": self.paused, "queued": len(self.pending),
                "emitted": self.sequence, "discarded": self.discarded, "reason": self.reason,
                "next_scheduled_at": self.next_calendar.isoformat() if self.next_calendar else None}

    def control(self, action, now, monotonic):
        """Pause drops queued work explicitly; resume has a fresh clock and ignored file baseline."""
        if action == "pause":
            self.paused = True
            self.discarded += len(self.pending)
            self.pending.clear()
            self.reason = "paused_by_command"
        elif action == "resume" and self.paused:
            self.discarded += len(self.pending)
            self.pending.clear()
            self.paused, self.reason = False, ""
            if self.folder:
                self.folder.close()
                self.folder = None
            self.files.reset()
            self.next_tick = monotonic + self.config["interval_sec"]
            self.next_poll = self.next_emit = monotonic
            if self.config["mode"] == "daily":
                self.next_calendar = next_daily(self.config, now, self.last_date)
        elif action not in {"status", "resume"}:
            raise TriggerError("Unknown command.")

    def event(self, kind, data, now):
        """Event IDs are unique within a listener generation and stable in the returned snapshot."""
        return {"source": self.config["mode"], "type": kind, "observed_at": now.isoformat(),
                "run_id": self.run_id, "node_id": self.node_id, "data": data, "payload": self.config["payload"]}

    def tick(self, now, monotonic, stop=lambda: False):
        """One bounded observation; no sleeps, background threads, sockets or implicit replay."""
        if self.paused or stop():
            return None
        incoming = []
        mode = self.config["mode"]
        if mode == "interval" and monotonic >= self.next_tick:
            skipped = max(0, math.floor((monotonic - self.next_tick) / self.config["interval_sec"]))
            self.next_tick += (skipped + 1) * self.config["interval_sec"]
            incoming.append(self.event("timer", {"skipped_ticks": skipped}, now))
        elif mode == "daily" and now >= self.next_calendar:
            scheduled = self.next_calendar
            self.last_date = scheduled.astimezone(ZoneInfo(self.config["timezone"])).date().isoformat()
            incoming.append(self.event("calendar", {"scheduled_at": scheduled.isoformat(),
                "timezone": self.config["timezone"], "late_sec": max(0, (now - scheduled).total_seconds())}, now))
            self.next_calendar = next_daily(self.config, now, self.last_date)
        elif mode == "folder" and monotonic >= self.next_poll:
            self.next_poll = monotonic + self.config["poll_sec"]
            try:
                if not self.folder:
                    self.folder = Folder(self.config)
                snapshot = self.folder.snapshot(stop)
                for name, signature in self.files.observe(snapshot, monotonic):
                    incoming.append(self.event("file_ready", {"path": str(self.folder.path / name), "name": name,
                        "device": signature[0], "inode": signature[1], "size": signature[2],
                        "mtime_ns": signature[3], "ctime_ns": signature[4], "content_hashed": False}, now))
            except (OSError, ValueError, UnicodeError):
                if stop():
                    return None
                self.paused, self.reason = True, "folder_unavailable_or_limit_exceeded"
                return None
        if len(self.pending) + len(incoming) > self.config["max_pending"]:
            self.discarded += len(incoming)
            self.paused, self.reason = True, "event_queue_overflow"
            return None
        self.pending.extend(incoming)
        if stop() or not self.pending or monotonic < self.next_emit:
            return None
        self.next_emit = monotonic + 1 / self.config["emit_per_sec"]
        event = self.pending.popleft()
        self.sequence += 1
        digest = hashlib.sha256((self.run_id + ":" + self.node_id + ":" + str(self.sequence) +
                                 ":" + event["observed_at"]).encode()).hexdigest()
        return {"id": digest, "sequence": self.sequence, **event}
