"""FB1/FB2/FB3/FB4/FB5: actual Run listeners, data commands, Stop and all package origins."""

from pathlib import Path
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from blocs.event_trigger.block import EventTriggerBlock
from block_test_packages import install_test_package, surface_payload
from ui_smoke_common import (create_project_api, create_run_api, data_edge, display_node, graph_payload,
    http_json, isolated_server, stop_run_api, text_node, wait_for_run_predicate, wait_for_run_terminal)


def main():
    """Fixture sources and watched files stay entirely in disposable test storage."""
    block = EventTriggerBlock()
    for mode in ("centralized", "zeromq_active"):
        for origin in (None, "managed", "linked"):
            with isolated_server() as server:
                node = block.build_node_payload(node_id="trigger", position={"x": 360, "y": 140})
                node["config"] = {**block.default_config(), "interval_sec": 1, "start_paused": True}
                if origin:
                    model = install_test_package(server, "event_trigger", origin=origin)
                    node["block_version"] = model["version"]
                    for surface in ("modal", "inspector_panel", "node_card"):
                        html = surface_payload(server, model, node, surface=surface)["html"]
                        assert "{{" not in html
                        if surface != "node_card":
                            assert 'data-block-config-field="mode"' in html
                graph = graph_payload("Trigger fixture", [
                    text_node("command", "Command", '{"action":"status"}', 60, 140), node,
                    display_node("events", "Events", 700, 140), display_node("state", "State", 700, 340)], [
                    data_edge("control", "command", 1, "trigger", 1), data_edge("events", "trigger", 1, "events", 1),
                    data_edge("state", "trigger", 2, "state", 1)])
                project = create_project_api(server, document=graph)["project"]
                run = create_run_api(server, graph, project_id=project["graph_id"], runtime_mode=mode)
                rid = run["run_id"]
                if mode == "centralized":
                    result = wait_for_run_terminal(server, rid, timeout_sec=30)
                    assert result["status"] == "success", result.get("logs")
                    assert not result["output_values"].get("trigger:1", {}).get("value")
                    assert json.loads(result["output_values"]["trigger:2"]["value"])["live"] is False
                    continue
                def state(data):
                    value = data.get("output_values", {}).get("trigger:2", {}).get("value")
                    return json.loads(value) if value else {}
                def wait(predicate):
                    result = wait_for_run_predicate(server, rid,
                        lambda data: data.get("status") == "failed" or predicate(data), "Trigger condition missing", timeout_sec=20)
                    assert result.get("status") != "failed", result.get("logs")
                    return result
                def send(action):
                    http_json(server.base_url, f"/api/runs/{rid}/active/control", method="POST", payload={
                        "action": "publish_output", "node_id": "command", "port_id": 1,
                        "value": json.dumps({"action": action}), "content_type": "application/json"})
                try:
                    waiting = wait(lambda data: state(data).get("paused") is True)
                    assert not waiting["output_values"].get("trigger:1", {}).get("value")
                    send("resume")
                    active = wait(lambda data: state(data).get("emitted", 0) >= 1)
                    event = json.loads(active["output_values"]["trigger:1"]["value"])
                    assert event["type"] == "timer" and event["run_id"] == rid and event["node_id"] == "trigger"
                    send("pause")
                    paused = wait(lambda data: state(data).get("reason") == "paused_by_command")
                    count = state(paused)["emitted"]
                    time.sleep(1.2)
                    latest = http_json(server.base_url, f"/api/runs/{rid}")
                    assert state(latest)["emitted"] == count
                    send("resume")
                    wait(lambda data: state(data).get("emitted", 0) > count)
                finally:
                    stopped = stop_run_api(server, rid)
                    terminal = wait_for_run_terminal(server, rid, timeout_sec=10)
                    assert "runtime_listeners.shutdown_timeout" not in str(terminal.get("logs"))
            print("[ok] Event Trigger runtime " + mode + " " + str(origin), flush=True)
    # Actual filesystem source, no trigger input or Play, in a managed listener.
    with isolated_server() as server:
        folder = server.root_dir / "inbox"; folder.mkdir()
        (folder / "old.txt").write_text("ignored baseline")
        model = install_test_package(server, "event_trigger", origin="managed")
        node = block.build_node_payload(node_id="watcher", position={"x": 60, "y": 100})
        node["block_version"] = model["version"]
        node["config"] = {**block.default_config(), "mode": "folder", "watch_directory": str(folder),
                          "pattern": "*.txt", "stable_sec": 1}
        graph = graph_payload("Stable file fixture", [node, display_node("sink", "File", 420, 100)],
                              [data_edge("file", "watcher", 1, "sink", 1)])
        project = create_project_api(server, document=graph)["project"]
        rid = create_run_api(server, graph, project_id=project["graph_id"], runtime_mode="zeromq_active")["run_id"]
        try:
            wait_for_run_predicate(server, rid, lambda data: bool(data.get("output_values", {}).get("watcher:2")),
                                   "Watcher not listening", timeout_sec=20)
            (folder / "new.txt").write_text("completed fixture")
            result = wait_for_run_predicate(server, rid, lambda data: bool(data.get("output_values", {}).get("watcher:1")),
                                            "Stable file not emitted", timeout_sec=20)
            event = json.loads(result["output_values"]["watcher:1"]["value"])
            assert event["data"]["name"] == "new.txt" and event["type"] == "file_ready", event
        finally:
            stop_run_api(server, rid)
            wait_for_run_terminal(server, rid, timeout_sec=10)
    print("[ok] Event Trigger real file source and lifecycle")


if __name__ == "__main__":
    main()
