# Remote-Compute

简单局域网里的远程与分布式计算，以及远程电脑上的工程操作。用于 ngspice、Verilator、原生模拟器批次、Python 计算、执行指令和文件更新。服务、调度和终端只用 Python 3.11+ 标准库；节点复用各自已有的仿真器和工程环境。

## 同一仓库、设备独立配置

源码统一来自 https://github.com/Ayai01/Remote-Compute.git。两台设备保持同一个 main 提交；设备始终以操作系统主机名显示。首次 `git clone`，后续运行 `python main.py update`（快进更新）；`update --check` 查看远端版本。更新后从中心执行 `restart Aya1-TX` 重新加载。运行任务期间可先更新磁盘源码，空闲后重载。

`worker.local.json`、`nodes.local.json`、`worker.standalone.json`、`.tools/`、`.projects/`、`.runtime/` 和 `results/` 都不上传仓库。`.tools/` 放实际Python科学环境、ngspice、RTL编译链和内存盘驱动；任务源码与小模型按内容资产同步。只有确需常驻的大工程才放 `.projects/` 并写本机shared配置。

## 实时终端与运行中控制

Windows交互终端每秒刷新CPU负载、主机内存、任务RSS/预留/可分配内存、已分配/可用线程、设备总线程、任务标签、访问IP和CPU温度。温度读取已有LibreHardwareMonitor/OpenHardwareMonitor传感器，没有传感器时显示“未提供”。最近6条指令/文件操作滚动显示；命令输入同时保持可用。`--headless` 保留无界面服务；设备配置 `console: true` 可在原headless启动终端打开看板。

`cpus N`（worker）或 `cpus Aya1-TX N`（中心）在线调整并持久保存，现有任务自然完成。也可一次性运行：

```text
python main.py --url http://127.0.0.1:8766 control --node Aya1-TX --cpus 31
```

任务运行时的进度和停止文件使用 `Client.read_job_file(id, path)` / `write_job_file(id, path, data)`，相对任务输入目录。它们直接由中心转发，不占计算线程，满载时仍可操作。

## 每台设备唯一8 GiB共享内存盘

所有工程使用 `.codex-shared-ramdisk.json` 和 `Global\CodexSharedRamdiskV1`。创建前持有同一个系统锁并重新检测；已存在小盘原地扩容，已有盘扩容失败时保留它并报告原因，不另建第二块。调用 `python main.py ramdisk` 可创建或检查。Windows需安装一次ImDisk驱动，首次安装/扩容使用管理员权限；驱动包存放在 `.tools/imdisk/` 后运行 `python main.py ramdisk --install`。安装时登记系统启动任务 `RemoteCompute Shared RAM`，重启后重新创建唯一共享盘；日常工程启动直接复用，每个工程使用自己的子目录。当前实现是固定8GiB内存盘。

AOP、RGCN、ARH-DSE、Analog-Opt-Bench默认优先本机8766中心；中心未启动时沿用独立预算 `min(C,16)+floor(max(C-16,0)/2)`。worker子任务设置 `REMOTE_COMPUTE_WORKER=1` 防止递归提交。AOP网页只保留“本地调度器”和“本地工程”两个模式。

## 启动

在工程目录执行，唯一入口是 `main.py`：

Windows 双击 `start.cmd`，或在 PowerShell 运行 `.\start.cmd`；Linux 执行 `./start.sh`（也可 `sh start.sh`）。两个脚本都会切换到工程目录并调用同一个 `main.py`，读取设备配置：Aya1-PC 启动 hybrid，Aya1-TX 启动 worker。参数原样传递，例如 `start.cmd --mode worker --host 0.0.0.0`。Windows 优先使用工程内的 `.tools/python`，随后尝试 `py -3`、`python`。

启动终端保持运行，中心和 worker 随之持续工作。`python main.py --url http://127.0.0.1:8766` 只连接已经运行的中心；提示连接拒绝时先用上述启动脚本启动服务。

端口冲突会显示监听进程的 PID 和命令，并提供 `reuse`（复用已识别的中心，hybrid 也能复用已有本机 worker）、`kill`、`admin-kill`、`cancel`。Windows 的 `admin-kill` 通过 UAC 请求管理员权限并结束进程树；Linux 使用 `sudo kill`。取消保留已有进程。非交互运行可显式选择，例如 `python main.py --on-port-conflict reuse --headless` 或 `--on-port-conflict admin-kill`；默认取消并给出操作提示。

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

中心模式下，各工程的 workers/并发上限不再参与准入，由中心统一分配。单线程SPICE任务声明1线程；可并行的整轮任务声明 `cpus: 0`，中心在派发时授予目标节点当前空闲线程，并通过 `${cpus}` 传给任务。HTTP使用持久连接，避免高并发状态轮询耗尽Windows临时端口；提供者和中心均支持128个等待连接。

各工程配置 `resource_share`（默认1.0）表达相对资源分配度：AOP、ARH-DSE、Bench放在运行配置顶层，RGCN放在 `resources.resource_share`。中心按活跃工程的权重占比分配线程，使用最大余数补齐整数余量，例如58线程、三工程同权为20/19/19，2:1为39/19。需求不足的份额借给其他待运行工程；CPU/内存/工具与设备限制仍按实际节点执行。在途任务自然结束，后续派发随活跃工程和权重调整。看板显示各工程分配度、当前目标、已分配线程与排队数。

默认 CPU 预算为 `max(1, CPU_COUNT-1)`，也是本设备线程上限。Aya1-PC 默认27，Aya1-TX 默认31；`--cpus N` 可在上限内设置。每个任务声明自己的总 CPU 占用，节点将任务并发与数值内核线程共同计账。调低预算后，在途任务正常结束，后续任务按新预算启动。

`--memory-mb N` 设置提供者的任务内存总预算，单位 MiB；0/未设置使用物理内存减去预留量，`memory_reserve_mb` 默认512。任务的 `memory_mb` 默认256。中心同时参考任务预留、实际进程树 RSS 和系统可用内存，CPU与内存都够才派发；同一设备的多个 worker 共享同一份物理内存统计。`nodes` 展示主机 CPU 使用率、内存容量、占用和可用量，结果记录峰值 RSS 与 CPU 秒数。

`worker.local.json` 配置本机工具和可选共享目录。计算运行时可安装在本工程 `.tools/`，科研源码/PDK按任务闭包传输，worker无需克隆四个科研工程。它属于本机配置，Git 忽略。迁移时按 `worker.example.json` 填写目标设备的工具和路径。

worker 可以独立运行，只需 Python 和本工具源码。示例配置不引用任何其他工程；复制过来的配置中，缺失共享目录记录为 `unavailable_shared`，其余能力正常启动。调度器只将引用共享目录的任务分配到具备该目录的节点。Windows 的执行别名即使无法读取文件摘要，也可以启动基本服务。

## 终端操作

总调度器的操作不会阻塞后续输入，任务在后台执行：

```text
nodes
add http://192.168.1.10:8765
set Aya1-TX weight=2 limit=8 memory_limit_mb=4096 enabled=true
policy adaptive
spice examples/spice/divider.spice --output results/divider
rtl examples/rtl --top tb --sources tb.sv --output results/rtl
batch examples/batch.json results/batch
jobs
logs Aya1-TX 任务ID
cancel Aya1-TX 任务ID
pause Aya1-TX
resume Aya1-TX
cpus Aya1-TX 4
memory Aya1-TX 4096
save
wait
quit
```

`192.168.1.10` 替换为目标设备 IP。`save` 保存节点、权重、限额和策略到 `nodes.local.json`，下次自动加载。

`set 节点 limit=N memory_limit_mb=M` 控制**本调度器**向该节点分配的 CPU 和内存，0 跟随节点预算；`cpus 节点 N` 和 `memory 节点 MiB` 调整**提供者总预算**。`enabled=false` 暂停本调度器向该节点分配；`pause` 暂停提供者启动新任务。在途任务继续运行。

算力提供者的终端支持 `status`、`jobs`、`cpus N`、`memory MiB`、`pause`、`resume`、`logs 任务ID`、`cancel 任务ID|all`、`delete 任务ID`、`quit`。空行保持状态；参数错误后继续接受操作；Ctrl+C 返回提示符。通过 `--url` 连接已有中心的终端，`quit` 取消本终端持有的单任务和批次并断开连接；`--detach` 提交的远程操作留在服务中继续运行。托管中心或提供者的入口退出时，停止相应服务及它承接的在途任务和子进程。

## 从 Codex 操作另一台电脑

另一台电脑复制本工程后，先用示例配置启动 worker，在调度终端加入它并保存；随后可将工具路径写入那台电脑自己的 `worker.local.json`：

```text
python main.py --mode worker --config worker.example.json --host 0.0.0.0 --headless --cpus 8 --memory-mb 8192

add http://192.168.1.10:8765
save
```

中心能够访问该 worker 后，Codex 通过本工程的 Python 接口或 `main.py --url 中心地址` 直接操作它。远程端只需 Python 和本工程，全部操作沿用同一条 HTTP 通道，无需部署 SSH、远程桌面或额外代理。`nodes` 返回目标机的用户目录、共享工程路径和机器能力，便于定位工程。

调度终端新增以下命令；`--cwd` 是目标电脑的工程目录，默认 `~`：

```text
ls --node Aya1-TX D:/Projects
read --node Aya1-TX D:/Projects/MyProject/main.py
put --node Aya1-TX ./updated-main.py D:/Projects/MyProject/main.py
put --node Aya1-TX ./source D:/Projects/MyProject --pattern "*.py" --pattern "src/**/*.py"
exec --node Aya1-TX --cwd D:/Projects/MyProject -- git status --short
exec --node Aya1-TX --cwd D:/Projects/MyProject -- git pull --ff-only
python --node Aya1-TX --cwd D:/Projects/MyProject ./maintenance.py -- argument
get --node Aya1-TX D:/Projects/MyProject/results ./results/Aya1-TX
```

`exec` 直接传递程序和参数；需要目标机的 shell 时可执行 `cmd /c ...`、`powershell ...` 或 `sh -c ...`。`python` 将本地脚本发送到目标机，使用目标机 Python 执行，可导入 `--cwd` 工程中的模块。`--tool rgcn_python` 等选项使用节点配置的工程解释器。复杂操作可以直接用 Python 完成，目标机无需安装 shell 扩展。

文件和目录的上传、下载支持二进制与中文路径；上传目录按文件合并，单个文件原子替换。`--pattern` 可重复设置，默认传输所选目录的全部普通文件。工程文件保留在目标机，任务输入、脚本和临时日志在回收后清理。

一次性命令加 `--url` 会自动进入客户端模式：

```text
python main.py --url http://127.0.0.1:8766 exec --node Aya1-TX --cwd D:/Projects/MyProject -- git status --short
python main.py --url http://127.0.0.1:8766 python --node Aya1-TX --cwd D:/Projects/MyProject --timeout 7200 --detach ./long-operation.py
python main.py --url http://127.0.0.1:8766 jobs
python main.py --url http://127.0.0.1:8766 logs 任务ID
python main.py --url http://127.0.0.1:8766 fetch 任务ID --output results/operation
```

远程命令与计算任务共用 CPU／内存预算、排队、优先级、超时和进程树取消。`--stdin 本地文件` 传入标准输入，`--env NAME=VALUE` 设置环境，`--output 目录` 保存完整日志。结果和实时日志包含末尾16KiB；完整日志随结果回收。长任务加 `--detach` 立即返回 ID，退出连接后可用相同中心地址和 ID 查询、取消或回收。

Codex 或其他 Python 调用者只需这一个接口：

```python
from remote_compute import Client

center = Client("http://127.0.0.1:8766")
machine = center.machine("Aya1-TX", cwd="D:/Projects/MyProject")

print(machine.listdir())
source = machine.read_text("main.py")
machine.write_text("main.py", updated_source)
machine.upload("./patch-files", ".", patterns=["**/*.py"])
result = machine.execute(["git", "status", "--short"])
print(result["stdout"])
result = machine.python("import main; print(main.__file__)")

job = machine.python(script="./maintenance.py", args=["--check"],
                     timeout=7200, memory_mb=1024, wait=False)
print(job["id"])
# 后续连接使用原 ID，提交方退出后任务仍由服务持有。
print(center.logs(job["id"]))
center.wait(job["id"])
center.fetch(job["id"], "./results/maintenance")
machine.download("./results", "./results/Aya1-TX")
```

`execute` 和 `python` 接受 `job_id`，在结果保留期间用相同 ID 和相同请求重提交只执行一次。操作固定到显式选择的节点；源码更新、工程环境安装、Git 操作和检查可沿用同一接口。

以 `worker` 或 `worker --headless` 启动时，入口保留一个轻量 Python 启动进程，支持远程重新加载自身。通过 `machine.upload()` / `write_text()` 更新本工具源码或该节点配置后，调用 `machine.restart()`，或在中心终端执行 `restart Aya1-TX` / `python main.py --url 中心地址 restart --node Aya1-TX`。节点空闲时自动重启并重新读取磁盘上的代码和配置，地址保持原配置；在途任务存在时返回409，待任务结束后再操作。`health` 返回 `pid`、`tool_root` 和 `restart` 能力，便于核对更新已生效。启动终端保持运行，之后的文件修改和重启均可远程完成。

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
python main.py --mode scheduler --url http://192.168.1.10:8766 jobs
python main.py --mode scheduler --url http://192.168.1.10:8766 fetch 任务ID --output results/recovered
python main.py --check
python main.py --test
python main.py --test-native
```

全局 `--cpus` / `--memory-mb` 是本机提供者预算；任务命令后面的同名选项是该任务的资源预留。单任务命令成功返回 0，执行失败返回 1，配置或通信错误返回 2。

```python
from remote_compute import Client, Pool, command, ngspice

with Pool([
    {"name": "Aya1-PC", "url": "http://127.0.0.1:8765"},
    {"name": "Aya1-TX", "url": "http://192.168.1.10:8765", "weight": 2},
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

变量可以出现在命令参数、环境变量、`cwd`，以及 `render` 指定的 UTF-8 文本文件中：

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

普通计算任务继续使用独立工作目录。指定 `cwd` 的任务在目标机该目录中执行，`cwd` 接受绝对路径、`~` 或 `${shared:工程名}`；`${work}` 和 `outputs` 始终指向任务输入目录，可将需要回收的文件明确写入 `${work}`。任务 JSON 的 `stdin` 是输入包中的相对文件路径，逐步作为标准输入传入。

每份结果包含任务 ID、输入摘要、命令、退出码、CPU 数、时间和工具文件摘要。接受后的任务通过原节点和 ID 查询；相同 ID 在节点保留期间重复提交只执行一次。测量解析、科学评分、唯一设计预算与研究缓存由原工程管理。

## 现有工程接入位置

| 工程 | 入口与执行粒度 |
| --- | --- |
| AOP_v2 | `--compute-center URL` 或 `compute.center`，每角点沿用原测量器、模型与结果解析；BGR 三工况21个指标与本地完全一致 |
| RGCN-v2 | `config.py` 的 `COMPUTE_URL` 或 `circuit.pre.remote_compute.url`，完整候选PRE/POST分析为一任务；源码和34MiB原生模型闭包按内容同步，8组149项指标、67份波形数字摘要完全一致，保留后仿恢复与原任务ID |
| ARH-DSE-v2 | `main.py run --headless --compute-center URL --project mac --workers 1 --memory-budget-mib 1024`；按整轮运行调度，沿用原生持久进程/构建，支持原ID重连；MAC 30候选/2326原生调用完全一致 |
| Analog-Opt-Bench-v2 | `main.py run --remote-url URL` 或配置 `remote_compute.url`，按算法/种子批次分配；Class-AB与ZDT1四任务、每任务512次评价的科学数组完全一致，持久Numba缓存将同批热运行从61秒降至7.885秒 |
| Rocket 神经网络工程 | 按硬件配置构建后，在同一任务内执行多程序和网络批次 |

四个工程默认优先本机调度中心；中心缺席时使用原独立预算。AOP网页直接选本地调度器/本地工程，RGCN使用COMPUTE_URL=auto，ARH与Bench使用compute-mode=auto，显式local固定独立执行。

## 工程内工具安装

```text
python main.py setup --ngspice-source "D:/Program Files/ngspice" --rtl-source "D:/Projects/#4-Arclight/ARH-DSE-v2/.tools/msys64"
```

`setup` 安装固定版本的可移动Python3.12科学环境，验证Numba JIT、OpenCV与ONNX Runtime CPU推理后写入本机工具配置；首次可直接下载Python/ngspice，也可复用已有ngspice。`--rtl-source` 提取现有MSYS2中GCC/Verilator的实际依赖，排除编译链无关组件与下载缓存。

AOP/RGCN SPICE、Bench优化、ARH CPU推理共用 `.tools/python`（约565MiB）；`.tools/ngspice`约38MiB，精简RTL环境约608MiB。原工程模型训练仍使用各工程环境。跨机复制运行时后，用`setup`校验版本并重定位路径，再从中心重载worker。

## 部署到其他设备

复制工程源码，准备 Python 3.11+，即可使用 worker 模式提供远程 Python、文件访问和控制。`worker.example.json` 是独立节点的最小配置，不依赖 AOP、RGCN、ARH-DSE 或 Analog-Opt-Bench。计算任务需要的源码可以通过资产包发送，依赖和工具按实际任务安装；工程解释器别名可在 `tools` 中追加。`python main.py --check` 用于检查仿真工具。Windows 的原生 Verilator 可通过 `env.MSYS2_ROOT` 指向已有 MSYS2；Linux 使用 PATH 中的 Verilator 和编译器。

按需在两端设置相同的 `REMOTE_COMPUTE_TOKEN`，启用单个共享 token。节点通信直接连接配置的地址。

本机验证使用 Python 3.14.4、ngspice 47、Verilator 5.050，`--test-native` 覆盖核心回归。两个本机独立节点分发的四次 ngspice 均测得 1.2 V；RTL 完成编译和 4096 周期断言，成功回收 `metrics.json`。中心转发、资产复用、CPU/内存准入、同设备统计、终端批次取消和两侧回收均有核心回归。机器操作回归覆盖真实工程文件更新后执行、固定节点、原 ID 重连、标准输入、中文/二进制传输和子进程取消。独立 worker 缺少旧工程目录、Windows 执行别名不可读、远程更新后重启均已验证。通过运行中的中心读写并更新分压电路后，ngspice 输出从1.2V变为2.2V，下载文件逐字节一致；测试工程与任务临时文件已清理。真实工程对照数据保存在本机 `results/`。

工具参数参考：[ngspice 文档](https://ngspice.sourceforge.io/docs.html)、[Verilator 参数](https://verilator.org/guide/latest/exe_verilator.html)。
