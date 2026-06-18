#!/usr/bin/env bash
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

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="${PROJECT_ROOT}/chaos-test/robustmq-3env.sh"

fail() {
    echo "[test_3env] FAIL: $*" >&2
    exit 1
}

assert_contains() {
    local haystack="$1"
    local needle="$2"

    if ! grep -Fq -- "$needle" <<< "$haystack"; then
        fail "expected output to contain: $needle"
    fi
}

test_script_has_valid_bash_syntax() {
    bash -n "$SCRIPT"
}

test_help_command_lists_supported_actions() {
    local output
    output="$(bash "$SCRIPT" help)"

    assert_contains "$output" "build"
    assert_contains "$output" "prepare"
    assert_contains "$output" "start"
    assert_contains "$output" "status"
    assert_contains "$output" "stop"
    assert_contains "$output" "restart"
    assert_contains "$output" "clean"
    assert_contains "$output" "all"
}

test_config_sources_live_outside_the_script() {
    local node
    for node in node1 node2 node3; do
        [ -f "${PROJECT_ROOT}/chaos-test/config/robustmq-3env/${node}/server.toml" ] || fail "missing server template for $node"
        [ -f "${PROJECT_ROOT}/chaos-test/config/robustmq-3env/${node}/logger.toml" ] || fail "missing logger template for $node"
    done

    assert_contains "$(grep -F 'CONFIG_SOURCE_DIR' "$SCRIPT")" "ROBUSTMQ_3ENV_CONFIG_DIR"
    assert_contains "$(grep -F 'copy_node_config' "$SCRIPT")" "copy_node_config"
}

test_status_without_state_reports_missing_state() {
    local state_dir="${PROJECT_ROOT}/chaos-test/.robustmq-3env"
    local backup_dir=""
    local output

    if [ -d "$state_dir" ]; then
        backup_dir="$(mktemp -d)"
        mv "$state_dir" "$backup_dir/.robustmq-3env"
    fi

    set +e
    output="$(bash "$SCRIPT" status 2>&1)"
    local code=$?
    set -e

    if [ -n "$backup_dir" ]; then
        mv "$backup_dir/.robustmq-3env" "$state_dir"
        rmdir "$backup_dir"
    fi

    [ "$code" -ne 0 ] || fail "status without state should return non-zero"
    assert_contains "$output" "No state file found"
}

main() {
    test_script_has_valid_bash_syntax
    test_help_command_lists_supported_actions
    test_config_sources_live_outside_the_script
    test_status_without_state_reports_missing_state
    echo "[test_3env] PASS"
}

main "$@"
