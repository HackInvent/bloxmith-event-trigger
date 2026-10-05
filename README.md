# Event Trigger

![Event Trigger](media/cover.png)

Start a workflow from an interval, a local calendar time, or a stable file in an
explicitly selected directory. One node selects one source. It emits metadata,
never executes the event's contents.

<!-- block-metadata:start -->
[![Block version: 0.1.0](https://img.shields.io/badge/block-0.1.0-blue)](model.json)
[![BloxSmith compatibility: 1.0.9](https://img.shields.io/badge/BloxSmith-1.0.9-brightgreen)](compatibility.json)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Verified BloxSmith versions: **1.0.9** (bundled-block tests; see [test evidence](compatibility.json)).
<!-- block-metadata:end -->

## Modes

- **Interval:** first event after `interval_sec` (default 60). Uses a monotonic
  clock. Missed ticks coalesce into one event with `data.skipped_ticks`; no burst replay.
- **Calendar (`daily`):** one local `daily_time` (HH:MM) on selected `weekdays`
  (0 Monday … 6 Sunday), in an explicit IANA `timezone`. No cron expression.
  With [Python zoneinfo](https://docs.python.org/3/library/zoneinfo.html), a local
  time missing at a DST transition is skipped; an ambiguous time uses its first
  occurrence only. A late listener emits at most one overdue event, then schedules
  the next future occurrence. It does not replay every missed day.
- **Folder:** nonrecursive polling of regular, singly linked files matching
  `pattern` in `watch_directory`. Symlinks, hardlinks and subdirectories are ignored.
  A file version is ready after unchanged size, inode and modification/change
  timestamps for `stable_sec`, seen at least twice. Contents are not read or hashed.

Existing files form an ignored baseline by default. Enable `include_existing`
to emit them after stability **at Run only**. Changed and new files can emit
later; disappearance/reappearance starts a new observation. No deletion events.
A replaced watched directory pauses detection rather than switching to another tree.

**Webhooks are not implemented in this release.** The pinned framework has no
public external-ingress-to-listener contract. No private HTTP server, framework
route modification or UI command workaround is introduced.

## Lifecycle and data commands

Active Runtime starts listening at **Run**, without Play or an input message.
Stop/Cancel cooperatively closes the directory handle and listener. No detached
threads, sockets or external scheduler are created. A loaded settings change
requires Stop and Run/reload. Closing the modal does not stop the source.

One Shot emits a `status` preview only, with `live:false`. It neither waits for an
expiry nor pretends to observe files or send a real live event.

| Port | Direction | Value |
| --- | --- | --- |
| `command` | Input | JSON object or strict JSON text: `{"action":"pause"}`, `resume` or `status` |
| `event` | Output | One normalized JSON event; only from the active listener |
| `status` | Output | Source state, counters, pause/error reason and next calendar occurrence |

`pause` immediately discards the local pending queue and increments `discarded`.
Already published events cannot be retracted. `resume` starts a fresh interval /
future calendar occurrence / ignored file baseline. No catch-up of paused work.
Repeated resume while already active does not reset the clock. `start_paused`
waits for a real data command. Invalid commands are diagnostics, not executable text.

State/deduplication is **per listener generation**, not a durable exactly-once
delivery guarantee. Restart/inhibition starts a new source. Downstream business
actions should enforce their own idempotency. This block has no API credentials.

## Event contract

All events contain `id`, `sequence`, `source`, `type`, `observed_at` (UTC ISO),
`run_id`, `node_id`, `data` and the configured static JSON `payload`.
Sequence numbers start at one for each listener generation.

- `timer`: `data.skipped_ticks`.
- `calendar`: `data.scheduled_at`, `timezone` and `late_sec`.
- `file_ready`: absolute runtime-host `path`, `name`, `device`, `inode`,
  `size`, `mtime_ns`, `ctime_ns`, `content_hashed:false`.

File readiness is an observation, not a lock or completion guarantee from the
producer. A file may change after emission. Consumers must revalidate metadata
or use producer-side atomic rename for stronger handoff. Network/virtual
filesystems with blocking metadata calls are outside the initial supported scope.

## Limits and overload

Default polling/stability: 1 / 2 seconds. Metadata scans examine at most 1,000
entries (configurable 1–10,000), including unmatched entries; a partial scan never
emits events. Files are not opened. Failure/over-limit pauses detection with
`reason:"folder_unavailable_or_limit_exceeded"`; fix the source and send resume.

The local queue holds at most 64 events (1–128 configurable). Publications are
paced at 2/s (1–10), without a catch-up burst. If a discovered batch will not fit,
the whole new batch is rejected, counted in `discarded`, and detection pauses
with `reason:"event_queue_overflow"`. Previously queued items remain until a
pause/resume explicitly discards them. Inspect status; overload is not silent.
Resume intentionally establishes a fresh baseline, so rejected work is not retried.

The framework's separate bounded mailbox may still reject a result when downstream
processing cannot keep up; that remains an explicit runtime transport error.
No claim of end-to-end backpressure or guaranteed delivery is made.

Commands ≤1 KiB, static payload ≤4 KiB, strict finite JSON depth ≤12, ≤1,000 values.
At most 16 commands per activation. Status metadata/logs do not expose file names
or payload contents; the event output necessarily contains the selected path.
No watched file is modified, deleted or moved.

## Setup and UI

POSIX/Linux; existing local filesystem directory for folder mode; system IANA
timezone data for calendar mode. No package installation at Run.
The modal and inspector share draft/Apply behavior, translated labels, keyboard
navigation and source-specific fields. Advanced queue/rate limits remain separate.

Owned tests cover clock jumps/DST, stability and symlink exclusion, pause/resume,
overflow, cancellation, actual Run/Stop graph propagation, package origins,
One Shot preview and responsive browser UI. No real user directory is monitored.

Licensed under Apache-2.0.
