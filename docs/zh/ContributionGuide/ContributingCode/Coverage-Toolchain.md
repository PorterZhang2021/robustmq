# Rust 覆盖率工具链

本页面记录仓库覆盖率工作的工具链验证结果，作为后续覆盖率任务的技术
基线。它不是覆盖率门禁，也不会替代现有的单元测试或集成测试流程。

## 已验证的工具链

仓库通过 `rust-toolchain.toml` 固定 Rust `1.97.0`。选用的覆盖率工具是
`cargo-llvm-cov 0.6.21`，它使用 Rust 的 LLVM 源代码覆盖率插桩以及匹配的
`llvm-tools-preview` 组件。

本次验证在 macOS 和 Ubuntu 兼容的 `rust:1.97-bookworm` 容器中执行，并使用仓库
固定的 Rust 版本。容器使用了仓库 Linux 构建依赖，并额外安装了 bindgen 依赖的
`libclang-dev`。后续 GitHub Actions 实现仍然必须在实际的 Linux runner 上再次
验证相同的命令。

## 代表性测试目标

本次采用 `protocol` crate 的库测试目标：

```text
package: protocol
target:  --lib
tests:   95 passed, 0 failed
```

`protocol` crate 覆盖 MQTT、Kafka、NATS 和存储编解码逻辑，不需要启动 broker
或依赖外部服务。现有的 `src/protocol/tests/codec.rs` 集成测试中包含占位测试，
虽然可以通过但可能生成空报告，因此不作为代表性覆盖率目标。

## 本地复现

使用临时 target 目录，避免插桩构建产物与普通构建混合。验证时关闭增量编译
和 `sccache`：

```bash
proof_dir="$(mktemp -d /tmp/robustmq-coverage-proof.XXXXXX)"
export CARGO_TARGET_DIR="$proof_dir/target"
export CARGO_INCREMENTAL=0
export RUSTC_WRAPPER=

cargo llvm-cov --package protocol --lib --no-report

cargo llvm-cov report --text --output-path "$proof_dir/coverage.txt"
cargo llvm-cov report --lcov --output-path "$proof_dir/lcov.info"
cargo llvm-cov report --json --output-path "$proof_dir/coverage.json"
cargo llvm-cov report --html --output-dir "$proof_dir/html"
```

在新环境中安装覆盖率组件和工具：

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --version 0.6.21 --locked
```

运行验证前检查工具版本：

```bash
rustc --version
cargo --version
cargo llvm-cov --version
rustup component list --installed | grep llvm-tools
```

本次验证使用的版本是：

```text
rustc 1.97.0 (2d8144b78 2026-07-07)
cargo 1.97.0 (c980f4866 2026-06-30)
cargo-llvm-cov 0.6.21
```

Ubuntu 兼容环境使用 `rustc 1.97.0-aarch64-unknown-linux-gnu`，95 个测试全部
通过并生成了 837 个不同的 profile。macOS 验证生成了 791 个 profile；差异来自
平台相关的构建和测试进程，而不是 profile 命名冲突。

## Profile 和报告

通过工具输出的环境变量查看实际路径，不要在脚本中硬编码 profile 目录：

```bash
cargo llvm-cov show-env
```

设置 `CARGO_TARGET_DIR` 后，工具会在该目录下创建覆盖率 target，并使用类似
下面的冲突规避命名：

```text
<target>/llvm-cov-target/robustmq-%p-%10m.profraw
```

其中 `%p` 表示进程，`%10m` 表示插桩模块。两次验证都为 95 个测试生成了不同的
`.profraw` 文件，Linux 验证生成了 837 个 profile。报告均成功生成且不为空：

| 格式 | 验证结果 |
| --- | ---: |
| Text | 792,900 字节（Linux） |
| LCOV | 221,939 字节（Linux） |
| JSON | 100,788,201 字节（Linux），解析成功 |
| HTML | 已生成 `html/index.html` |

profile 文件和报告都是临时验证产物，不应提交到仓库；仓库已经忽略
`*.profraw` 和 `*.profdata`。后续覆盖率任务可以再确定更小的报告范围和产物保留
策略。

## 后续任务约定

后续本地覆盖率 runner 应当：

1. 保持 `CARGO_INCREMENTAL=0`。
2. 避免插桩和普通 `sccache` 产物混用；本地验证可以关闭 `RUSTC_WRAPPER`。
3. 运行真实测试目标，例如 `protocol --lib`，不要使用占位的 codec 集成测试。
4. 将缺失或空的 profile 数据视为无效覆盖率运行。
5. 在测试完成后使用 `cargo llvm-cov report` 生成报告。
6. 记录确切的 package/target 范围，因为本次验证不是全 workspace 覆盖率，也不包括
   MQTT broker 进程或集成测试覆盖率。

本次验证没有新增 Make target、GitHub Actions workflow、MQTT 行为测试、Codecov
集成或覆盖率阈值。
