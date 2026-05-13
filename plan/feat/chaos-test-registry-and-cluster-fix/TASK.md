# 任务清单

## 状态说明

- `[ ]` 待开始
- `[~]` 进行中
- `[x]` 已完成

---

## Task 1：tools/registry.py ✅

- [x] **[测试]** `chaos-test/tests/test_registry.py`
  - **用例：注册后可通过名称取回 tool**
    - Given：一个空 registry
    - When：`registry.register(name="foo", toolset="chaos", schema={}, handler=lambda args: "{}", emoji="🔧")`
    - Then：`registry.get_all()` 返回列表长度为 1，第一项 `name == "foo"`
  - **用例：重复注册同名 tool 抛出异常**
    - Given：已注册名为 "foo" 的 tool
    - When：再次注册同名 "foo"
    - Then：抛出 `ValueError`
  - **用例：handler 被正确绑定并可调用**
    - Given：注册时传入 `handler=lambda args: json.dumps({"ok": True})`
    - When：从 `get_all()` 取出该 tool 并调用其 handler
    - Then：返回值为 `'{"ok": true}'`

- [x] **[实现]** `chaos-test/tools/registry.py`（测试全部红色后开始）
  - `register(name, toolset, schema, handler, emoji)` — 注册 tool，重复 name 抛 ValueError
  - `get_all() -> list[dict]` — 返回所有注册项，每项含 name/toolset/schema/handler/emoji

---

## Task 2：cluster.py 单节点适配 ✅

- [x] **[测试]** `chaos-test/tests/test_cluster.py`
  - **用例：ROBUSTMQ_HOME 未设置时 start 返回 error**
    - Given：环境变量 `ROBUSTMQ_HOME` 未设置
    - When：调用 `_action_start()`
    - Then：返回 dict 含 `"error"` 字段，内容包含 "ROBUSTMQ_HOME"
  - **用例：二进制不存在时 start 返回 error**
    - Given：`ROBUSTMQ_HOME` 指向一个不含 `broker-server` 的临时目录
    - When：调用 `_action_start()`
    - Then：返回 dict 含 `"error"` 字段，内容包含 "broker-server"
  - **用例：生成的 toml 包含正确字段**
    - Given：任意有效的 run_id 和 data_dir
    - When：调用内部 `_generate_toml(data_dir, http_port, grpc_port)`
    - Then：返回的 toml 字符串包含 `http_port`、`data_path`、`log_path`
  - **用例：stop 时无运行中集群返回 stopped 状态**
    - Given：`_BROKERS` 为空
    - When：调用 `_action_stop()`
    - Then：返回 `{"status": "stopped"}`

- [x] **[实现]** `chaos-test/tools/cluster.py` 修改
  - `_robustmq_binary()` — 查找 `broker-server`（而非 `robustmq-server`）
  - `_generate_toml(data_dir, http_port, grpc_port)` — 生成单节点 server.toml 内容
  - `_health_check(http_port)` — 改为轮询 `http://127.0.0.1:{http_port}/health/ready`
  - `_action_start()` — 生成临时 toml，用 `--conf` 启动 `broker-server`，默认 http_port=8080

---

## Task 2.5：cluster.py 集成测试（需要真实 binary）

- [ ] **[集成测试]** `chaos-test/tests/test_cluster_integration.py`
  - **用例：单节点启停完整流程**
    - Given：`ROBUSTMQ_HOME` 设置为 `target/debug/`，该目录下存在 `broker-server`
    - When：调用 `_action_start()`，等待健康检查通过，再调用 `_action_stop()`
    - Then：start 返回 `{"status": "running"}`，stop 返回 `{"status": "stopped"}`，无残留进程

  > 此测试依赖真实 binary，CI 中需设置 `ROBUSTMQ_HOME`，本地跳过时用 `pytest -m "not integration"` 排除。

---

## Task 3：sdk_clients/python/basic-pubsub.sh ✅

无业务逻辑测试（shell 脚本，以能产出合法 JSON 输出为验收标准）。

- [x] 创建 `chaos-test/sdk_clients/python/basic-pubsub.sh`
  - 连接 `$CLUSTER_ENDPOINT`（默认 `127.0.0.1:1883`）
  - 发送 100 条 QoS 1 消息，等待全部接收（超时 30s）
  - 最后一行输出标准 JSON：`{"sent": N, "received": N, "lost": N, "p99_ms": N, "errors": []}`
  - exit 0 = 成功，exit 1 = 失败或超时

---

## Task 4：安装 skill 到 Hermes ✅

无业务逻辑，无需测试。

- [x] `ln -s <repo>/chaos-test ~/.hermes/skills/robustmq-chaos-test`
- [ ] 验证 Hermes 能识别到该 skill（`hermes` 列出 skill 列表）

---

## Task 5：端到端验证

- [ ] 设置 `ROBUSTMQ_HOME` 指向已编译的 RobustMQ
- [ ] 手动触发：`hermes message "跑一遍单节点 basic-pubsub 场景"`
- [ ] 确认闭环：cluster start → client run → cluster stop → 本地报告文件生成
