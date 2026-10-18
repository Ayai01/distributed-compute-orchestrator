# Distributed Compute Orchestrator（DCO）

面向简单局域网的远程计算、统一调度和工程操作工具，用于 ngspice、Verilator、原生模拟器批次与 Python 任务。服务、调度与终端只依赖标准库，源码兼容 Python 3.7–3.15；日常优先本机系统安装的标准版 CPython 3.14。仿真工具和科学依赖按任务安装在设备本地。

工程简称统一使用 **DCO**，仓库为 `distributed-compute-orchestrator`。现有部署目录、`remote_compute` Python 包、`REMOTE_COMPUTE_*` 环境变量和历史回执保持兼容；这些是同一工程的稳定接口，不另设第二套配置字段。

## 快速启动

在工程目录运行，两个启动脚本都调用同一个 `main.py`：

```powershell
.\start.cmd
```

```sh
./start.sh
```

直接使用 Python 时：

```text
python main.py --mode hybrid
python main.py --mode scheduler
python main.py --mode worker --host 0.0.0.0 --port 26133
```

| 模式 | 功能 |
| --- | --- |
| hybrid | 调度中心 + 本机 worker，无本地配置时的默认模式 |
| scheduler | 仅运行调度中心 |
| worker | 独立提供算力和远程操作，不依赖其他工程 |

中心和独立 worker 的对外端口统一 **26133**；hybrid 的本机 worker 直接在调度器进程内调用，**不监听额外端口**。`--url` 连接已有服务，服务尚未启动时先运行启动脚本。

所有系统通过统一入口先检查可见终端，再启动服务和校准。Windows核对实际控制台或终端宿主窗口；Linux/macOS要求前台TTY，tmux/screen须有附着客户端。隐藏窗口、重定向输入输出、nohup及无终端systemd/计划任务会退出并提示正确入口。`--headless`仅关闭交互看板，仍须保留可见终端。一次性查询、文件传输、Python API与测试继续支持管道。

启动后的 `调度>` 输入行可直接执行：

```text
help                       按用途查看指令和例子
status                     查看设备和任务资源
cpus 16                    本机使用16线程，自动保存
memory 8G                  本机任务RAM预算8GiB，自动保存
logs ab12                  查看唯一4位任务ID的日志
fetch ab12 "D:/结果"        保存结果并清理该任务
restart --wait             空闲后重新加载中心
```

资源命令也可指定设备，例如 `cpus Aya1-TX 30`。输入指令或设备名称的一部分后按 Tab 补全，`help cpus` 查看详细用法。方括号表示可省略参数，不需要照着输入；上述任务ID `ab12` 需换成看板上的实际ID。

## 当前设备与控制

| 设备 | 角色 | 默认分配线程 | 设备上限 |
| --- | --- | --- | --- |
| Aya1-PC | 中心 + 本机算力 | 16 | 24 |
| Aya1-TX | 独立 worker | 30 | 31 |
| ku26s | 独立 worker | 96 | 96 |

当前合计 **142 线程**。调度中心终端可直接操作：

```text
nodes
cpus Aya1-TX 30
jobs
restart Aya1-TX
restart --wait
```

各工程 `resource_share` 默认 1.0，按相对权重分配，整数余量与空闲份额继续使用。`queue_ratio` 默认 1.0，提交窗口随当前份额刷新；同步计算可在批次边界向中心借还线程。CPU/内存准入由中心统一控制。看板按设备资源、工程份额、当前任务和通知分区，输入区固定在底部；聚焦时每秒刷新，失焦时每 5 秒刷新。中心与 worker 均支持在原终端等待空闲后重新加载，更新源码后执行 `restart --wait`，指定设备时使用 `restart Aya1-TX --wait`。

默认调度优先选择预计 CPU 负载较低的设备，结合当前采样与尚未反映的线程预留；负载接近时再参考任务历史速度和节点权重。线程、内存及工程份额先参与准入。

## 文档

- [启动、算力控制与维护](docs/operations.md)：启动参数、端口冲突、在线预算、终端命令、远程文件/指令、Git 更新、工具安装、共享 RAM 盘与排错。
- [调度、任务接口与工程接入](docs/integration.md)：份额与提前排队、节点策略、AOP/RGCN/ARH/Bench 接入、Python API、任务变量、结果回收与验证。
- [worker 配置示例](worker.example.json)、[节点配置示例](nodes.example.json)、[任务示例](examples/)。

源码统一使用 [Ayai01/distributed-compute-orchestrator](https://github.com/Ayai01/distributed-compute-orchestrator) 的 main。设备配置、`.tools/`、`.projects/`、`.runtime/` 和结果留在本机。每台 Windows 设备复用同一块 8 GiB 共享 RAM 盘，工程各用自己的子目录。
