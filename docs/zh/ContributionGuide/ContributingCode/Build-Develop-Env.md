# 基础开发环境搭建

## Mac 环境搭建
### 主要步骤

::: tip
 ⚠️注意：当前项目中已经默认携带了`rust-toolchain.toml`, 因此默认情况下会使用该文件配置的rustc环境，如果通过brew安装了rust 可能会出现版本被覆盖的问题， 需要卸载后重新按照该内容进行搭建。
:::

代码运行需要先搭建Rust开发环境，初始化rust环境后。 项目主要依赖 cmake、rocksdb、 protoc， 需要根据不同的操作系统环境去安装这些依赖。

- 安装 Rust 基础环境
参考文档：[Rust Course](https://course.rs/first-try/installation.html)

- Rust 版本
目前依赖的rust 版本是：stable
```shell
rustup install stable
rustup default stable
rustc --version
```
- 安装 Cmake.

mac 安装命令如下：
```shell
brew install cmake
```

- 安装 RocksDB.

参考文档：[Github/rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb) 安装 rocksdb。 mac 安装命令如下：
```shell
brew install rocksdb
```

- 安装 protoc

参考文档：[教程](https://www.jianshu.com/p/341293ee1286) 安装protoc。mac 安装命令如下：
```shell
brew install protobuf
```

### 配置pre-commit插件

[RobustMQ 主仓库](https://github.com/robustmq/robustmq) 和 [RobustMQ PB 协议仓库 Robust-Proto](https://github.com/robustmq/robustmq-proto) 均默认使用`pre-commit`进行了代码预提交，因此需要安装指定版本的`pre-commit`工具。关于`pre-commit`可以参考 [官方文档](https://pre-commit.com/)。

::: tip
对于 [Robust-Proto](https://github.com/robustmq/robustmq-proto)，还需提前另外安装 [buf](https://github.com/bufbuild/buf) 工具:
```shell
brew install bufbuild/buf/buf
````
:::

首先你需要通过`Python`(3.8版本以上)构建一个虚拟环境，安装命令如下:
```shell
python3 -m venv precommit_venv
```

安装完成后，通过如下命令进入虚拟环境:
```shell
source ./precommit_venv/bin/activate
```

然后安装对应版本的`pre-commit`工具, 这里使用项目环境内指定版本的`pre-commit`:
```shell
pip3 install -r ./.requirements-precommit.txt
```

完成安装后，需要初始化一下项目内容的`pre-commit`钩子，使用如下命令:
```shell
pre-commit install
```

::: tip
⚠️注意:
1. 后续进行任何的`git commit`操作，`pre-commit`都会执行对应的检查， 这里如果不想每次提交都进行检查可以使用`git commit -n来跳过检查。
2. `pre-commit`检查已经设置了`stage`，对于测试以及严格的检查会在`pre-push`阶段进行，需要使用`pre-commit install --hook-type pre-push`来安装`pre-push`的`hooks`。
:::

`pre-commit`携带的检查功能使用了next-test,hawkeye,clippy, typos,cargo-deny等工具, 这些工具如果缺少可以参考如下命令进行安装
```shell
cargo install hawkeye@5.8.1
cargo install typos-cli
cargo install cargo-deny@0.16.2 --locked
cargo install cargo-nextest@0.9.84
```
