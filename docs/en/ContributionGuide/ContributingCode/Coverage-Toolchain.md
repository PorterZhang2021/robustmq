# Rust Coverage Toolchain

This page records the toolchain proof for repository coverage work. It is a
technical baseline for follow-up coverage tasks, not a coverage gate and not a
replacement for the existing unit or integration-test workflows.

## Proven toolchain

The repository pins Rust `1.97.0` in `rust-toolchain.toml`. The selected
coverage tool is `cargo-llvm-cov 0.6.21`, which uses Rust's LLVM source-based
coverage instrumentation and the matching `llvm-tools-preview` component.

The proof was run on macOS and in an Ubuntu-compatible `rust:1.97-bookworm`
container with the same pinned Rust version used by the repository's
`ubuntu-latest` workflows. The container used the repository's Linux build
dependencies plus `libclang-dev`, which is required by bindgen-based crates.
The first GitHub Actions implementation must still verify the same command on
its actual Linux runner.

## Representative target

The accepted proof target is the `protocol` crate library test target:

```text
package: protocol
target:  --lib
tests:   95 passed, 0 failed
```

The `protocol` crate exercises MQTT, Kafka, NATS, and storage codec code without
starting a broker or requiring an external service. The existing
`src/protocol/tests/codec.rs` integration target contains a placeholder test and
therefore can pass while producing an empty report; it is not the representative
coverage target.

## Reproduce locally

Use a disposable target directory so instrumented artifacts do not mix with
normal builds. Disable incremental compilation and `sccache` for the proof:

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

Install the selected coverage component and tool when setting up a fresh
environment:

```bash
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --version 0.6.21 --locked
```

Check the tool versions before running the proof:

```bash
rustc --version
cargo --version
cargo llvm-cov --version
rustup component list --installed | grep llvm-tools
```

The proof installed and used:

```text
rustc 1.97.0 (2d8144b78 2026-07-07)
cargo 1.97.0 (c980f4866 2026-06-30)
cargo-llvm-cov 0.6.21
```

The Ubuntu-compatible proof used Rust `1.97.0-aarch64-unknown-linux-gnu`, ran
all 95 tests successfully, and produced 837 distinct profiles. The macOS proof
produced 791 profiles; the difference is from platform-specific build and test
processes, not profile-name collisions.

## Profiles and reports

Inspect the environment selected by the tool instead of hard-coding a profile
directory:

```bash
cargo llvm-cov show-env
```

With `CARGO_TARGET_DIR` set, the tool uses a coverage target below that directory
and writes profiles using a collision-resistant pattern equivalent to:

```text
<target>/llvm-cov-target/robustmq-%p-%10m.profraw
```

`%p` identifies the process and `%10m` identifies the instrumented module. Both
proof runs produced distinct `.profraw` files from the 95-test target. The Linux
run generated 837 profiles. Reports were generated successfully and were
non-empty:

| Format | Evidence |
| --- | ---: |
| Text | 792,900 bytes (Linux) |
| LCOV | 221,939 bytes (Linux) |
| JSON | 100,788,201 bytes (Linux); parsed successfully |
| HTML | `html/index.html` generated |

The profile files and reports are disposable proof output. Do not commit them;
the repository already ignores `*.profraw` and `*.profdata`. Later coverage
tasks may choose smaller report scopes and artifact retention rules.

## Follow-up contract

The next local coverage runner should:

1. Keep `CARGO_INCREMENTAL=0`.
2. Avoid mixing instrumented and normal `sccache` artifacts; disabling
   `RUSTC_WRAPPER` is the proven local fallback.
3. Run a real test target such as `protocol --lib`, not the placeholder codec
   integration target.
4. Treat missing or empty profile data as an invalid coverage run.
5. Use `cargo llvm-cov report` to generate reports after tests complete.
6. Record the exact package/target scope because this proof is not full-workspace
   coverage and does not include MQTT broker-process or integration coverage.

This proof does not add a Make target, GitHub Actions workflow, MQTT behavior
tests, Codecov integration, or a coverage threshold.
