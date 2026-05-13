# Copyright 2023 RobustMQ Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
cluster.py — RobustMQ test cluster lifecycle tool.

Spawns 3 broker processes directly on the local host (no Docker).
Each broker gets its own port and a tempdir for data.

Ports:
  broker-1: 1883
  broker-2: 2883
  broker-3: 3883

State is kept in a module-level dict so start/stop/status share it
within the same Hermes session. For cross-session persistence the
caller (Skill) should use chaos_state.

Required env var:
  ROBUSTMQ_HOME  — directory that contains the RobustMQ binary.
                   Fail-fast if unset; no default.
"""

import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

from tools.registry import registry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state (lives for the lifetime of the Hermes process)
# ---------------------------------------------------------------------------

_BROKERS: dict = {}
# Shape:
# {
#   "broker-1": {
#     "process": subprocess.Popen,
#     "port": 1883,
#     "data_dir": "/tmp/rmq-abc123",
#     "node_name": "broker-1",
#   },
#   ...
# }

_SINGLE_NODE = "broker-1"
_MQTT_PORT = 1883
_HTTP_PORT = 8080
_GRPC_PORT = 1228
_HEALTH_TIMEOUT = 30  # seconds total for health check polling
_HEALTH_INTERVAL = 2  # seconds between polls


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _robustmq_binary() -> Optional[str]:
    home = os.environ.get("ROBUSTMQ_HOME", "").strip()
    if not home:
        return None
    candidates = [
        Path(home) / "bin" / "broker-server",
        Path(home) / "broker-server",
    ]
    for c in candidates:
        if c.is_file():
            return str(c)
    return str(candidates[0])


def _project_root() -> Optional[str]:
    """Walk up from ROBUSTMQ_HOME until config/server.toml is found."""
    home = os.environ.get("ROBUSTMQ_HOME", "").strip()
    if not home:
        return None
    p = Path(home).resolve()
    for candidate in [p, p.parent, p.parent.parent, p.parent.parent.parent]:
        if (candidate / "config" / "server.toml").is_file():
            return str(candidate)
    return None


def _generate_logger_toml(data_dir: str) -> str:
    return f"""[stdout]
kind = "console"
targets = [
    {{ path = "", level = "info" }},
]

[server]
kind = "rolling_file"
targets = [
    {{ path = "", level = "info" }},
]
rotation = "daily"
directory = "{data_dir}/logs"
prefix = "server"
suffix = "log"
max_log_files = 10
"""


def _generate_toml(data_dir: str, http_port: int, grpc_port: int) -> str:
    project_root = _project_root() or "."
    cert_path = os.path.join(project_root, "config", "certs", "cert.pem")
    key_path = os.path.join(project_root, "config", "certs", "key.pem")
    return f"""cluster_name = "broker-server"
broker_id = 1
broker_ip = "127.0.0.1"
roles = ["meta", "broker", "engine"]
grpc_port = {grpc_port}
http_port = {http_port}
meta_addrs = {{ 1 = "127.0.0.1:{grpc_port}" }}

[rocksdb]
data_path = "{data_dir}/data"
max_open_files = 10000

[pprof]
enable = false
port = 6777
frequency = 1000

[log]
log_config = "{data_dir}/logger.toml"
log_path = "{data_dir}/logs"

[network]
accept_thread_num = 1
handler_thread_num = 64
queue_size = 5000
keep_alive_enable = false

[mqtt_keep_alive]
enable = true
default_time = 180
max_time = 3600
default_timeout = 2

[prometheus]
enable = false
port = 9091
monitor_interval_ms = 10000

[mqtt_system_topic]
interval_ms = 60000

[mqtt_offline_message]
enable = true
expire_ms = 3600000
max_messages_num = 100000

[storage_offset]
enable_cache = true

[storage_runtime]
tcp_port = 1779
max_segment_size = 1073741824
data_path = ["{data_dir}/engine"]
io_thread_num = 4

[runtime]
tls_cert = "{cert_path}"
tls_key = "{key_path}"

[grpc_client]
channels_per_address = 4
"""


def _health_check(mqtt_port: int) -> bool:
    """Check readiness by attempting a TCP connection to the MQTT port.

    The /api/health/ready endpoint checks the QUIC port via TCP which always
    fails (QUIC is UDP). Checking the MQTT TCP port directly is more reliable.
    """
    import socket

    try:
        with socket.create_connection(("127.0.0.1", mqtt_port), timeout=2):
            return True
    except OSError:
        return False


def _kill_all() -> None:
    """Best-effort kill of all tracked broker processes."""
    for name, info in list(_BROKERS.items()):
        proc = info.get("process")
        if proc and proc.poll() is None:
            try:
                proc.kill()
                proc.wait(timeout=5)
            except Exception as exc:
                logger.warning("cluster: failed to kill %s: %s", name, exc)
    _BROKERS.clear()


def _cleanup_data_dirs(data_dirs: list) -> None:
    for d in data_dirs:
        try:
            shutil.rmtree(d, ignore_errors=True)
        except Exception as exc:
            logger.warning("cluster: failed to remove data dir %s: %s", d, exc)


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------


def _action_start() -> dict:
    binary = _robustmq_binary()
    if binary is None:
        return {
            "error": (
                "ROBUSTMQ_HOME is not set. "
                "Export it to the directory containing the RobustMQ installation, "
                "e.g. export ROBUSTMQ_HOME=/opt/robustmq"
            )
        }
    if not Path(binary).is_file():
        return {
            "error": (
                f"broker-server binary not found at {binary}. "
                "Check that ROBUSTMQ_HOME points to a valid installation."
            )
        }

    if _BROKERS:
        return {
            "error": "Cluster is already running. Call stop first if you want to restart."
        }

    data_dir = tempfile.mkdtemp(prefix=f"rmq-{_SINGLE_NODE}-")
    Path(data_dir, "logs").mkdir(parents=True, exist_ok=True)
    Path(data_dir, "data").mkdir(parents=True, exist_ok=True)
    Path(data_dir, "engine").mkdir(parents=True, exist_ok=True)

    logger_toml_path = Path(data_dir) / "logger.toml"
    logger_toml_path.write_text(_generate_logger_toml(data_dir))

    toml_content = _generate_toml(data_dir, http_port=_HTTP_PORT, grpc_port=_GRPC_PORT)
    toml_path = Path(data_dir) / "server.toml"
    toml_path.write_text(toml_content)

    log_file = Path(data_dir) / "logs" / "broker.log"
    try:
        with open(log_file, "w") as lf:
            proc = subprocess.Popen(
                [binary, "--conf", str(toml_path)],
                stdout=lf,
                stderr=subprocess.STDOUT,
                close_fds=True,
            )
    except OSError as exc:
        _cleanup_data_dirs([data_dir])
        return {"error": f"Failed to start {_SINGLE_NODE}: {exc}"}

    _BROKERS[_SINGLE_NODE] = {
        "process": proc,
        "mqtt_port": _MQTT_PORT,
        "http_port": _HTTP_PORT,
        "data_dir": data_dir,
        "node_name": _SINGLE_NODE,
    }

    deadline = time.monotonic() + _HEALTH_TIMEOUT
    while time.monotonic() < deadline:
        if _health_check(_MQTT_PORT):
            break
        time.sleep(_HEALTH_INTERVAL)
    else:
        _kill_all()
        _cleanup_data_dirs([data_dir])
        return {
            "status": "failed",
            "error": (
                f"Health check failed after {_HEALTH_TIMEOUT}s. "
                f"Check logs: {log_file}"
            ),
        }

    return {
        "status": "running",
        "endpoint": f"127.0.0.1:{_MQTT_PORT}",
        "nodes": [_SINGLE_NODE],
        "data_dirs": [data_dir],
    }


def _action_stop() -> dict:
    if not _BROKERS:
        return {"status": "stopped", "note": "No running cluster found."}

    data_dirs = [info["data_dir"] for info in _BROKERS.values()]
    _kill_all()
    _cleanup_data_dirs(data_dirs)
    return {"status": "stopped", "cleaned_dirs": data_dirs}


def _action_status() -> dict:
    if not _BROKERS:
        return {"status": "stopped", "running_processes": 0, "endpoint": None}

    alive = []
    dead = []
    for name, info in _BROKERS.items():
        proc = info.get("process")
        if proc and proc.poll() is None:
            alive.append(name)
        else:
            dead.append(name)

    if not alive:
        status = "stopped"
    elif dead:
        status = "degraded"
    else:
        status = "running"

    return {
        "status": status,
        "running_processes": len(alive),
        "total_processes": len(_BROKERS),
        "alive_nodes": alive,
        "dead_nodes": dead,
        "endpoint": "127.0.0.1:1883" if alive else None,
    }


# ---------------------------------------------------------------------------
# Tool handler
# ---------------------------------------------------------------------------


def _cluster_handler(args: dict, **_) -> str:
    action = args.get("action", "")
    try:
        if action == "start":
            result = _action_start()
        elif action == "stop":
            result = _action_stop()
        elif action == "status":
            result = _action_status()
        else:
            result = {
                "error": f"unknown action: '{action}'. Valid: start, stop, status"
            }
    except Exception as exc:
        logger.exception("cluster: unhandled error in action '%s'", action)
        result = {"error": f"internal error: {exc}"}
    return json.dumps(result, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Schema + registration
# ---------------------------------------------------------------------------

_SCHEMA: dict = {
    "name": "cluster_manage",
    "description": (
        "Start, stop, or query the RobustMQ test cluster. "
        "Spawns 3 broker processes locally (no Docker). "
        "Requires ROBUSTMQ_HOME env var — fails immediately if unset. "
        "start: launches broker-1/2/3 on ports 1883/2883/3883, "
        "waits 5 s, health-checks broker-1, returns endpoint '127.0.0.1:1883'. "
        "stop: kills all brokers and removes their temp data dirs. "
        "status: returns per-node liveness."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["start", "stop", "status"],
                "description": (
                    "start: spawn brokers and wait for health. "
                    "stop: kill all brokers and clean up data dirs. "
                    "status: check which broker processes are alive."
                ),
            },
        },
        "required": ["action"],
    },
}

registry.register(
    name="cluster_manage",
    toolset="chaos",
    schema=_SCHEMA,
    handler=_cluster_handler,
    emoji="🖥️",
)
