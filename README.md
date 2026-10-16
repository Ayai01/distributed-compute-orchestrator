# Remote-Compute

简单局域网里的远程与分布式计算。用于 ngspice、Verilator、原生模拟器批次，以及现有工程的 Python 计算任务。服务、调度和终端只用 Python 3.11+ 标准库；节点复用各自已有的仿真器和工程环境。

## 启动

在工程目录执行，唯一入口是 `main.py`：

```text
python main.py
python main.py --mode hybrid --cpus 8 --memory-mb 8192
python main.py --mode scheduler --host 0.0.0.0 --headless
python main.py --mode worker --host 0.0.0.0 --cpus 8 --memory-mb 8192
```

| 模式 | 用途 |
| --- | --- |
| `hybrid`，默认 | 总调度器和本机算力同时启动；本机也参与任务执行 |
| `scheduler` | 调度其他设备，本机仅负责分配和回收 |
| `worker` | 提供算力，启动本节点的操作终端 |

调度中心默认端口 `8766`，算力提供者默认端口 `8765`，默认监听 `127.0.0.1`。`--center-port` 设置中心端口，worker 的 `--port` 设置提供者端口；纯 scheduler 的 `--port` 也可设置中心端口。其他设备使用 `--host 0.0.0.0` 启动，再把设备 IP 加入总调度器。`--headless` 启动持续运行的后台服务。

调用工程连接中心的 `8766`，多个工程共享中心队列、资源预算和分配策略。中心将任务转交给配置的 worker，回收结果后同时清理两侧的任务文件；模型和源码资产按内容同步并复用。

默认 CPU 预算为 `min(C,16)+floor(max(C-16,0)/2)`，`--cpus N` 使用显式值。每个任务声明自己的总 CPU 占用，节点将任务并发与数值内核线程共同计账。调低预算后，在途任务正常结束，后续任务按新预算启动。

`--memory-mb N` 设置提供者的任务内存总预算，单位 MiB；0/未设置使用物理内存减去预留量，`memory_reserve_mb` 默认512。任务的 `memory_mb` 默认256。中心同时参考任务预留、实际进程树 RSS 和系统可用内存，CPU与内存都够才派发；同一设备的多个 worker 共享同一份物理内存统计。`nodes` 展示主机 CPU 使用率、内存容量、占用和可用量，结果记录峰值 RSS 与 CPU 秒数。

本机 `worker.local.json` 已配置现有 ngspice、ARH 的 MSYS2/Verilator，以及现有工程的共享路径。它属于本机配置，Git 忽略。迁移时按 `worker.example.json` 填写目标设备的工具和路径。

## 终端操作

总调度器的操作不会阻塞后续输入，任务在后台执行：

```text
nodes
add device2 http://192.168.1.20:8765
set device2 weight=2 limit=8 memory_limit_mb=4096 enabled=true
policy adaptive
spice examples/spice/divider.spice --output results/divider
rtl examples/rtl --top tb --sources tb.sv --output results/rtl
batch examples/batch.json results/batch
jobs
logs device2 任务ID
cancel device2 任务ID
pause device2
resume device2
cpus device2 4
memory device2 4096
save
wait
quit
```

`192.168.1.20` 替换为目标设备 IP。`save` 保存节点、权重、限额和策略到 `nodes.local.json`，下次自动加载。

`set 节点 limit=N memory_limit_mb=M` 控制**本调度器**向该节点分配的 CPU 和内存，0 跟随节点预算；`cpus 节点 N` 和 `memory 节点 MiB` 调整**提供者总预算**。`enabled=false` 暂停本调度器向该节点分配；`pause` 暂停提供者启动新任务。在途任务继续运行。

算力提供者的终端支持 `status`、`jobs`、`cpus N`、`memory MiB`、`pause`、`resume`、`logs 任务ID`、`cancel 任务ID|all`、`delete 任务ID`、`quit`。空行保持状态；参数错误后继续接受操作；Ctrl+C 返回提示符。调度器 `quit` 取消本终端持有的单任务和批次；提供者 `quit` 停止本节点任务及其子进程。

## 分配策略

- `adaptive`：结合空闲 CPU、主机负载、可用内存、节点权重和同类任务实测耗时分配。首次接触节点时先取得样本，之后按耗时的指数移动平均调整选择。
- `balanced`：按相对 CPU 负载和权重均衡分配。
- `round_robin`：在满足资源条件的节点之间轮询，便于固定分配实验。

任务还可设置 `priority`、`node`、`requires`。较大的 `priority` 先启动；`node` 固定节点名称；`requires` 要求节点标签全部匹配。节点自动报告 Python、ngspice、Verilator 等可用工具，调度器据任务使用的工具筛选节点。多个批次共享同一调度器的资源账本，节点再按自身预算统一准入。

用 `label` 区分电路、保真度或相近工作量的批次，使同类耗时具有可比性。执行中的任务保留原节点；中断任务的状态和已有产物留在节点，便于查询和回收。
中心的工具变体包含节点名和可执行文件摘要，调用工程可核对固定节点的引擎身份。

## 脚本与 Python 接口

```text
python main.py --mode hybrid --cpus 2 batch examples/batch.json results/demo
python main.py --mode scheduler --nodes nodes.local.json run examples/spice/task.json --input examples/spice --output results/divider
python main.py --mode scheduler --url http://192.168.1.20:8766 jobs
python main.py --mode scheduler --url http://192.168.1.20:8766 fetch 任务ID --output results/recovered
python main.py --check
python main.py --test
python main.py --test-native
```

全局 `--cpus` / `--memory-mb` 是本机提供者预算；任务命令后面的同名选项是该任务的资源预留。单任务命令成功返回 0，执行失败返回 1，配置或通信错误返回 2。

```python
from remote_compute import Client, Pool, command, ngspice

with Pool([
    {"name": "local", "url": "http://127.0.0.1:8765"},
    {"name": "device2", "url": "http://192.168.1.20:8765", "weight": 2},
]) as pool:
    result = pool.run(
        ngspice("divider.spice"),
        inputs="examples/spice",
        output_dir="results/divider",
    )
    print(result["status"], result["assigned_node"])

# 不指定 output_dir 时，文件以 bytes 返回在 result["files"] 中。
client = Client("http://127.0.0.1:8766")
task = command(["${tool:python}", "runner.py", "--workers", "${cpus}"],
               cpus=4, memory_mb=1024, timeout=1800, outputs=["measurements.json"],
               label="candidate-batch", priority=5)
result = client.run(task, inputs="my-batch")
```

其他工程可把此工程目录加入 Python 的模块搜索路径，再导入 `remote_compute`。工程本身没有 pip 安装步骤。

## 任务与文件

一个任务由顺序执行的 `steps`、输入文件和输出模式组成。任何一步失败即结束该任务，保留退出码和日志。`timeout` 覆盖整条执行链。取消和超时会结束所持有的进程树，包括编译器子进程。

```json
{
  "label": "my-batch",
  "cpus": 2,
  "memory_mb": 512,
  "priority": 5,
  "timeout": 300,
  "steps": [["${tool:python}", "runner.py", "--workers", "${cpus}"]],
  "outputs": ["measurements.json"]
}
```

变量可以出现在命令参数、环境变量，以及 `render` 指定的 UTF-8 文本文件中：

| 变量 | 含义 |
| --- | --- |
| `${tool:python}` / `${tool:ngspice}` / `${tool:verilator}` | 节点配置的工具；多参数前缀放在每步第一个参数，单个可执行路径也可嵌入环境和输入文件 |
| `${work}` | 当前任务的独立工作目录 |
| `${cpus}` | 当前任务占用的 CPU 数，用于程序线程数或编译 `-j` |
| `${exe}` | Windows 为 `.exe`，Linux 为空串 |
| `${asset:models}` | 本任务引用的已同步只读资产目录 |
| `${shared:pdk}` | 节点预部署的共享目录 |

大模型或 PDK 可以预部署到各节点，通过 `worker.local.json` 的 `shared` 配置引用。小型模型包可以同步一次后复用：

```python
asset_id = client.sync("my-models")
task = ngspice("circuit.spice", assets={"models": asset_id}, render=["circuit.spice"])
result = client.run(task, files={
    "circuit.spice": '* deck\n.include "${asset:models}/device.lib"\n...'
})
```

资产按文件内容生成 ID；模型变动产生新 ID。节点部署路径分别配置，任务保持同一份逻辑输入。`shared` 中的预部署内容版本由调用工程维护。

默认工作目录优先使用已有 `Z:` 内存盘，否则使用系统临时目录；`--root` 可指定。输入、构建中间物和日志均在任务独立目录内。客户端收齐结果后自动删除节点任务目录，`--keep` 可显式保留。用户选择的结果保存在输出目录；同步资产保留供后续复用。

每份结果包含任务 ID、输入摘要、命令、退出码、CPU 数、时间和工具文件摘要。接受后的任务通过原节点和 ID 查询；相同 ID 在节点保留期间重复提交只执行一次。测量解析、科学评分、唯一设计预算与研究缓存由原工程管理。

## 现有工程接入位置

| 工程 | 建议接入粒度 |
| --- | --- |
| AOP_v2 | `--compute-center URL` 或 `compute.center`，每角点沿用原测量器、模型与结果解析；BGR 三工况21个指标与本地完全一致 |
| RGCN-v2 | 将生成后的 PRE/POST 网表及分析批次提交，模型用 shared/asset 引用；沿用物理测量与来源记录 |
| ARH-DSE-v2 | `main.py run --headless --compute-center URL --project mac --workers 1 --memory-budget-mib 1024`；按整轮运行调度，沿用原生持久进程/构建，支持原ID重连；MAC 30候选/2326原生调用完全一致 |
| Analog-Opt-Bench-v2 | 按种子或评价批次分配，保留原算法状态与唯一设计预算 |
| Rocket 神经网络工程 | 按硬件配置构建后，在同一任务内执行多程序和网络批次 |

现有工程默认保持独立本机计算；显式设置中心才接入。其他工程由各自正在工作的会话在运行边界适配，具体入口随验证结果更新。

## 部署到其他设备

复制工程源码，准备 Python 3.11+ 和该节点要使用的计算工具。运行 `python main.py --check` 检查，再以 worker 模式启动。`worker.example.json` 中各工程的 Python 别名要指向已安装相应依赖的解释器。Windows 的原生 Verilator 可通过 `env.MSYS2_ROOT` 指向已有 MSYS2；Linux 使用 PATH 中的 Verilator 和编译器。

按需在两端设置相同的 `REMOTE_COMPUTE_TOKEN`，启用单个共享 token。节点通信直接连接配置的地址。

本机验证使用 Python 3.14.4、ngspice 47、Verilator 5.050；`--test-native` 的29项回归全部通过。两个本机独立节点分发的四次 ngspice 均测得 1.2 V；RTL 完成编译和 4096 周期断言，成功回收 `metrics.json`。中心转发、资产复用、CPU/内存准入、同设备统计、终端批次取消和两侧回收均有核心回归。真实工程对照数据保存在本机 `results/`。

工具参数参考：[ngspice 文档](https://ngspice.sourceforge.io/docs.html)、[Verilator 参数](https://verilator.org/guide/latest/exe_verilator.html)。
