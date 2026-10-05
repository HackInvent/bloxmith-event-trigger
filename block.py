"""Run-scoped timer/calendar/file listeners, using only the public mailbox contract."""

from datetime import datetime, timezone
import json
import time
from bloxsmith_app.block_api import APPLICATION_JSON, BlockDefinition, BlockRuntimeOutput, BlockRuntimePreparation, BlockRuntimeResult
from . import logic, ui


# FB1 - Start bounded interval or local-calendar events at Run without requiring a data input.
# FB2 - Emit stable nonrecursive file metadata once per observed version, with an explicit startup baseline.
# FB3 - Pause/resume through data commands; bound queues and rates, report overload, and stop cooperatively.
# FB4 - Preserve source behavior across package origins; One Shot previews and never fabricates live events.
# FB5 - Own translated responsive mode-specific settings, draft lifecycle and static release assets.
class EventTriggerBlock(BlockDefinition):
    """One node selects one source; no external HTTP ingress is invented."""

    kind = "event_trigger"

    def ui_assets(self, surface="modal"):
        """Resolve only the static manifest, including managed and linked hosts."""
        return list(self.model.get("ui_assets", {}).get(surface, []))

    def prepare_runtime(self, context):
        """Purely validate settings and declare the generic Run listener."""
        logic.configuration(context.config)
        return BlockRuntimePreparation(listen_on_run=context.runtime_mode == "zeromq_active")

    def result(self, context, state, event=None):
        """Publish one source event and its status through declared message outputs."""
        values = {"status": state}
        if event is not None:
            values["event"] = event
        outputs = [BlockRuntimeOutput(port_id=int(port.id), port_name=port.name,
                    value=json.dumps(values[port.name], ensure_ascii=True, allow_nan=False), content_type=APPLICATION_JSON)
                   for port in context.output_ports if port.name in values]
        return BlockRuntimeResult(status="success", outputs=outputs, metadata={self.kind: state})

    def execute_runtime(self, context):
        """Validate every command before admission; latched unrelated inputs are never replayed."""
        try:
            config = logic.configuration(context.config)
            if context.runtime_mode != "zeromq_active":
                return self.result(context, {"mode": config["mode"], "live": False, "status": "preview",
                    "detail": "No timers or directory events in One Shot. Use Active Runtime."})
            ports = {int(port.id): str(port.name) for port in context.input_ports}
            if context.input_events:
                values = []
                for event in context.input_events:
                    if ports.get(event.input_port_id) != "command":
                        raise logic.TriggerError("Unknown input port.")
                    values.append(event.value)
            else:
                value = context.input_value("command", "1")
                values = [] if value in (None, "") else [value]
            if not values:
                return BlockRuntimeResult(status="skipped", outputs=[])
            if len(values) > 16:
                raise logic.TriggerError("At most 16 commands per activation.")
            actions = [logic.command(value) for value in values]
            client = context.services.get("runtime_listener")
            if client is None:
                raise RuntimeError("Listener unavailable.")
            for action in actions:
                client.send({"action": action})
            return BlockRuntimeResult(status="success", outputs=[])
        except logic.TriggerError as exc:
            return self.result(context, {"status": "command_error", "detail": str(exc)})
        except (RuntimeError, KeyError, TypeError):
            return BlockRuntimeResult(status="failed", outputs=[], exit_code=1,
                error="The event listener command bridge is unavailable; no command was applied.")

    def listen_runtime(self, context):
        """Keep all source state local to this listener generation; no detached worker threads."""
        config = logic.configuration(context.config)
        source = logic.Source(config, context.run_id, context.node_id, datetime.now(timezone.utc), time.monotonic())
        try:
            previous = None
            while not context.stop_requested():
                received = context.receive_command(timeout_sec=0.1)
                now, monotonic = datetime.now(timezone.utc), time.monotonic()
                requested_status = False
                if received is not None:
                    try:
                        action = logic.command(received.payload)
                        source.control(action, now, monotonic)
                        requested_status = True
                    except logic.TriggerError:
                        if not context.stop_requested():
                            context.emit_result(self.result(context, {"status": "command_error", "detail": "Invalid listener command."}))
                event = source.tick(now, monotonic, context.stop_requested)
                state = {**source.state(), "live": True, "status": "paused" if source.paused else "listening"}
                if not context.stop_requested() and (event is not None or state != previous or requested_status):
                    context.emit_result(self.result(context, state, event))
                    previous = state
        finally:
            source.close()

    def handle_ui_action(self, *, node, action, values, payload=None):
        """Save configuration drafts only; settings Apply is not a hidden Run command."""
        if action in {"save_settings", "modal_update_fields", "inspector_update_fields"}:
            patch = (values or {}).get("node_patch") or {}
            if "config" in patch:
                try:
                    logic.configuration({**self.default_config(), **(node.get("config") or {}), **patch["config"]})
                except (ValueError, TypeError) as exc:
                    return {"error": self.translate("block.event_trigger.error", {"detail": str(exc)}, fallback="Trigger failed: {detail}")}
        result = super().handle_ui_action(node=node, action="modal_update_fields" if action == "save_settings" else action,
                                          values=values, payload=payload)
        return ui.replace_config(result, node)

    def render_modal(self, *, node, payload=None):
        """Show only fields relevant to the selected source."""
        return ui.modal(self, node, payload)

    def render_inspector_panel(self, *, node, payload=None):
        """Offer the same source settings in a narrow responsive panel."""
        return ui.inspector(self, node, payload)

    def render_node_card(self, *, node, payload=None):
        """Keep source mode and purpose readable without exposing watched file paths."""
        mode = (node.get("config") or {}).get("mode", "interval")
        return ui.card(self, node, self.translate("block.event_trigger.mode_" + str(mode), fallback=str(mode)))
