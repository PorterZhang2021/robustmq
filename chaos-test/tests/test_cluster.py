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

import os
import tempfile
import pytest
from unittest.mock import patch


def test_start_returns_error_when_robustmq_home_not_set():
    env = {k: v for k, v in os.environ.items() if k != "ROBUSTMQ_HOME"}
    with patch.dict(os.environ, env, clear=True):
        from tools.cluster import _action_start, _BROKERS

        _BROKERS.clear()
        result = _action_start()
    assert "error" in result
    assert "ROBUSTMQ_HOME" in result["error"]


def test_start_returns_error_when_binary_not_found():
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch.dict(os.environ, {"ROBUSTMQ_HOME": tmpdir}):
            from tools.cluster import _action_start, _BROKERS

            _BROKERS.clear()
            result = _action_start()
    assert "error" in result
    assert "broker-server" in result["error"]


def test_generate_toml_contains_required_fields():
    from tools.cluster import _generate_toml

    with tempfile.TemporaryDirectory() as tmpdir:
        result = _generate_toml(data_dir=tmpdir, http_port=8080, grpc_port=1228)
    assert "8080" in result
    assert tmpdir in result
    assert "[network]" in result
    assert "[grpc_client]" in result


def test_generate_toml_matches_example_structure():
    from tools.cluster import _generate_toml

    with tempfile.TemporaryDirectory() as tmpdir:
        result = _generate_toml(data_dir=tmpdir, http_port=8080, grpc_port=1228)
    assert '"engine"' in result
    assert "[grpc_client]" in result
    assert "[runtime]" not in result


def test_stop_returns_stopped_when_no_cluster_running():
    from tools.cluster import _action_stop, _BROKERS

    _BROKERS.clear()
    result = _action_stop()
    assert result["status"] == "stopped"
