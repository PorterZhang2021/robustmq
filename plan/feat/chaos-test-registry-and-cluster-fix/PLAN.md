# feat: chaos-test skill — registry + cluster fix

## 1. 功能目标

让 `chaos-test` skill 能被 Hermes 加载并在单节点模式下跑通基础闭环：
**集群启停 → SDK 客户端验证 → 报告生成**。

当前所有工具依赖的 `tools/registry.py` 缺失导致 import 失败，
`cluster.py` 的二进制名和启动参数与实际 RobustMQ 不符，
`sdk_clients/` 下没有任何测试脚本，三处阻断叠加导致 skill 完全无法运行。

## 2. 技术选型

| 组件 | 选型 | 说明 |
|---|---|---|
| Tool 注册 | 自研轻量 registry | 不引入外部框架，与 Hermes skill 接口对齐 |
| 集群启动 | `broker-server --conf` | RobustMQ 实际启动方式，生成临时 toml |
| 健康检查 | HTTP GET `/health/ready` | admin-server 提供，端口 8080 |
| SDK 测试脚本 | Python + paho-mqtt | 最小覆盖，先跑通 Python，其他语言后续补 |
| 单节点模式 | `roles = ["meta", "broker", "engine"]` | 无需 raft 集群，单进程即完整节点 |

## 3. 目录结构

```
chaos-test/
├── tools/
│   └── registry.py              # 新增：Tool 注册中心
├── sdk_clients/
│   └── python/
│       └── basic-pubsub.sh      # 新增：Python paho-mqtt 基础场景脚本
└── tools/cluster.py             # 修改：适配实际二进制和启动方式
```

## 4. API 设计

### registry.py 对外接口

```python
registry.register(
    name: str,          # tool 名称，如 "cluster_manage"
    toolset: str,       # 所属工具集，如 "chaos"
    schema: dict,       # JSON Schema，供 Hermes 做参数校验
    handler: Callable,  # 实际处理函数 (args: dict) -> str
    emoji: str,         # 展示用图标
)

registry.get_all() -> list[dict]   # 返回所有已注册 tool 的 schema + handler
```

### cluster.py 改动点

启动参数从 CLI flags 改为生成临时配置文件：

```
broker-server --conf /tmp/rmq-broker-1-<run_id>/server.toml
```

临时 toml 关键字段：

```toml
broker_id = 1
broker_ip = "127.0.0.1"
grpc_port = 1228
http_port = 8080

[rocksdb]
data_path = "/tmp/rmq-broker-1-<run_id>/data"

[log]
log_path = "/tmp/rmq-broker-1-<run_id>/logs"

[mqtt_runtime]
# mqtt tcp_port 默认 1883，不需要显式写
```

健康检查：`GET http://127.0.0.1:8080/health/ready` → HTTP 200

## 5. 数据流

```
Hermes 触发
    ↓
registry.get_all() 加载所有 tool
    ↓
cluster_manage(action=start)
    → 生成临时 server.toml
    → subprocess broker-server --conf <toml>
    → 轮询 /health/ready 通过
    → 返回 {endpoint, data_dirs}
    ↓
client(action=run, scenario=basic-pubsub, sdk=python)
    → 执行 sdk_clients/python/basic-pubsub.sh
    → 解析最后一行 JSON 输出
    → 返回 {sent, received, lost, p99_ms}
    ↓
cluster_manage(action=stop)
    → kill 进程，清理临时目录
    ↓
report(action=generate_and_push)
    → 生成本地 JSON + Markdown 报告
```

## 6. 核心参数

| 参数 | 值 | 来源 |
|---|---|---|
| 二进制名 | `broker-server` | `src/cmd/Cargo.toml` |
| HTTP admin 端口 | `8080` | `config/server.toml` → `http_port` |
| MQTT TCP 端口 | `1883` | 默认值 `default_mqtt_tcp_port()` |
| 健康检查路径 | `/health/ready` | `src/admin-server/src/path.rs` |
| 健康检查超时 | `30s`，每 2s 轮询一次 | 单节点启动比集群快 |
| SDK 脚本目录 | `~/.hermes/skills/robustmq-chaos-test/sdk_clients/` | `client.py` 硬编码路径 |

## 7. 功能边界

**做：**
- `registry.py` 最小实现，只满足现有 5 个工具的注册需求
- `cluster.py` 单节点适配，不引入多节点 raft 逻辑
- `basic-pubsub.sh` Python 最小场景，能产出合法 JSON 输出即可

**不做：**
- 多节点集群启动（raft 协调，后续专项处理）
- Go / Rust / Java SDK 测试脚本（P1 场景，后续补充）
- Chaosd 故障注入验证（chaos.py 逻辑已有，单独验证）
- report push_to_github（需要 Deploy Key，本地验证用本地路径）

## 8. 实现顺序

1. **`registry.py`** — 无依赖，其他所有文件都依赖它，最先实现
2. **`cluster.py` 修改** — 依赖 registry，修完后可独立测试集群启停
3. **`basic-pubsub.sh`** — 依赖集群能跑起来，最后写
