# 启动、算力控制与维护

[返回首页](../README.md) · [调度与工程接入](integration.md)

## 启动

在工程目录执行，唯一入口是 `main.py`：

Windows 双击 `start.cmd`，或在 PowerShell 运行 `.\start.cmd`；Linux 执行 `./start.sh`（也可 `sh start.sh`）。两个脚本都会切换到工程目录并调用同一个 `main.py`，默认读取 `worker.local.json`。本机配置让 Aya1-PC 启动 hybrid、Aya1-TX 启动 worker；新克隆的工程默认 hybrid，需要独立提供算力时显式指定 worker。参数原样传递，例如 `start.cmd --mode worker --host 0.0.0.0 --port 26133`。Windows 优先使用工程内的 `.tools/python/python.exe`，随后尝试 `py -3`、`python`；Linux 优先 `.tools/python/bin/python3`，随后尝试 `python3`、`python`。

启动终端保持运行，中心和 worker 随之持续工作。`python main.py --url http://127.0.0.1:26133` 只连接已经运行的中心；提示连接拒绝时先用上述启动脚本启动服务。

端口冲突会显示监听进程的 PID 和命令，并提供 `reuse`（复用已识别的中心，hybrid 也能复用已有本机 worker）、`kill`、`admin-kill`、`cancel`。Windows 的 `admin-kill` 通过 UAC 请求管理员权限并结束进程树；Linux 使用 `sudo kill`。取消保留已有进程。非交互运行可显式选择，例如 `python main.py --on-port-conflict reuse --headless` 或 `--on-port-conflict admin-kill`；默认取消并给出操作提示。

```text
python main.py
python main.py --mode hybrid --cpus 8 --memory-mb 8192
python main.py --mode scheduler --host 0.0.0.0 --headless
python main.py --mode worker --host 0.0.0.0 --port 26133 --cpus 8 --memory-mb 8192
```

| 模式 | 用途 |
| --- | --- |
| `hybrid`，默认 | 总调度器和本机算力同时启动；本机也参与任务执行 |
| `scheduler` | 调度其他设备，本机仅负责分配和回收 |
| `worker` | 提供算力，启动本节点的操作终端 |

调度中心默认端口 `26133`，独立算力提供者默认端口 `26133`；hybrid 的本机 worker 仅在回环内部使用 `26134`，默认监听 `127.0.0.1`。`--center-port` 设置中心端口，worker / hybrid 的 `--port` 设置提供者端口；纯 scheduler 的 `--port` 也可设置中心端口。其他设备使用 `--host 0.0.0.0` 启动，再把设备 IP 加入总调度器。`--headless` 启动持续运行的后台服务。

调用工程连接中心的 `26133`，多个工程共享中心队列、资源预算和分配策略。中心将任务转交给配置的 worker，回收结果后同时清理两侧的任务文件；模型和源码资产按内容同步并复用。

### 地址和端口参数

| 模式 | `--host` | `--port` | `--center-port` |
| --- | --- | --- | --- |
| hybrid | 中心监听地址；内部 worker 固定回环 | 内部 worker 端口，默认 26134 | 中心端口，默认 26133 |
| scheduler | 中心监听地址 | 未指定 center-port 时作为中心端口 | 指定时优先作为中心端口 |
| worker | worker 监听地址 | 对外端口，默认 26133 | 不使用 |

日常配置统一使用 26133；调整中心端口后，调用工程的显式 URL 也要相应调整。`--port 0` 用于临时验证，由系统分配端口。

## 实时终端与运行中控制

Windows交互终端在窗口获得焦点时每秒刷新、失焦时每30秒刷新CPU负载、主机内存、任务RSS/预留/可分配内存、已分配/可用线程、设备总线程、任务标签、访问IP和CPU温度。温度读取已有LibreHardwareMonitor/OpenHardwareMonitor传感器，没有传感器时显示“未提供”。最近6条指令/文件操作滚动显示；命令输入同时保持可用。`--headless` 保留无界面服务；设备配置 `console: true` 可在原headless启动终端打开看板。

`cpus N`（worker）或 `cpus Aya1-TX N`（中心）在线调整并持久保存，现有任务自然完成。也可一次性运行：

```text
python main.py --url http://127.0.0.1:26133 control --node Aya1-TX --cpus 30
```

任务运行时的进度和停止文件使用 `Client.read_job_file(id, path)` / `write_job_file(id, path, data)`，相对任务输入目录。它们直接由中心转发，不占计算线程，满载时仍可操作。

## 设备预算与配置文件

当前设备配置如下。它们是各机的本地配置，不随 Git 更新覆盖。

| 设备 | 模式 | 地址 | 默认预算 | 硬上限 |
| --- | --- | --- | --- | --- |
| Aya1-PC | hybrid | 中心 `127.0.0.1:26133`；内部 worker `127.0.0.1:26134` | 24 | 24 |
| Aya1-TX | worker | `192.168.1.10:26133` | 30 | 31 |

新设备的通用默认预算和硬上限为 `max(1, CPU_COUNT-1)`。`cpu_limit` 在设备配置中设置硬上限，`cpus` 设置当前预算。在线 `cpus` 控制会写回当前使用的配置文件；调低预算后，在途任务自然结束，后续派发按新预算执行。改硬上限需要更新配置并重启。

`worker.local.json` 示例（工具路径按目标设备填写）：

```json
{
  "mode": "worker",
  "host": "0.0.0.0",
  "port": 26133,
  "cpus": 30,
  "cpu_limit": 31,
  "memory_mb": 0,
  "memory_reserve_mb": 512,
  "tools": {},
  "shared": {},
  "env": {}
}
```

`--config 路径` 选择其他配置；命令行显式选项覆盖配置。切换 mode 会保留配置中的 host/port，切为独立 worker 时同时指定 `--port 26133`，切为 hybrid 时使用内部 worker `--port 26134`。仅运行 `worker.example.json` 不会自动变成下一次默认配置，可先复制为 `worker.local.json`。worker 不依赖其他科研工程；缺失共享目录记录为 `unavailable_shared`，引用该目录的任务只匹配具备它的节点。

`memory_mb` / `--memory-mb` 单位为 MiB；0 使用物理内存减去预留量。中心同时核对任务内存预留、实际进程树 RSS 和系统可用内存。同一设备的多个 worker 共用一份物理内存统计。任务默认预留 256 MiB；大任务由调用者声明所需内存。

## 终端操作

总调度器的操作不会阻塞后续输入，任务在后台执行：

```text
nodes
add http://192.168.1.10:26133
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

add http://192.168.1.10:26133
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
python main.py --url http://127.0.0.1:26133 exec --node Aya1-TX --cwd D:/Projects/MyProject -- git status --short
python main.py --url http://127.0.0.1:26133 python --node Aya1-TX --cwd D:/Projects/MyProject --timeout 7200 --detach ./long-operation.py
python main.py --url http://127.0.0.1:26133 jobs
python main.py --url http://127.0.0.1:26133 logs 任务ID
python main.py --url http://127.0.0.1:26133 fetch 任务ID --output results/operation
```

远程命令与计算任务共用 CPU／内存预算、排队、优先级、超时和进程树取消。`--stdin 本地文件` 传入标准输入，`--env NAME=VALUE` 设置环境，`--output 目录` 保存完整日志。结果和实时日志包含末尾16KiB；完整日志随结果回收。长任务加 `--detach` 立即返回 ID，退出连接后可用相同中心地址和 ID 查询、取消或回收。

Codex 或其他 Python 调用者只需这一个接口：

```python
from remote_compute import Client

center = Client("http://127.0.0.1:26133")
machine = center.machine("Aya1-TX", cwd="D:/Projects/MyProject")

print(machine.listdir())
source = machine.read_text("main.py")
updated_source = source.replace("OLD_VALUE", "NEW_VALUE")
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

以 `worker` 或 `worker --headless` 启动时，入口保留一个轻量 Python 启动进程，支持远程重新加载自身。通过 `machine.upload()` / `write_text()` 更新本工具源码或该节点配置后，调用 `machine.restart()`，或在中心终端执行 `restart Aya1-TX` / `python main.py --url 中心地址 restart --node Aya1-TX`。节点空闲时重启并重新读取磁盘上的代码和配置，地址保持原配置；在途任务存在时返回409，待任务结束后再操作。`health` 返回 `pid`、`tool_root` 和 `restart` 能力，便于核对更新已生效。启动终端保持运行，之后的文件修改和重启均可远程完成。

## 同一仓库、设备独立配置

源码统一来自 https://github.com/Ayai01/Remote-Compute.git。两台设备保持同一个 main 提交；设备始终以操作系统主机名显示。首次 `git clone`，后续运行 `python main.py update`（快进更新）；`update --check` 查看远端版本。更新后从中心执行 `restart Aya1-TX` 重新加载。先确认节点空闲，再更新并重载。

`worker.local.json`、`nodes.local.json`、`worker.standalone.json`、`.tools/`、`.projects/`、`.runtime/` 和 `results/` 都不上传仓库。`.tools/` 放实际Python科学环境、ngspice、RTL编译链和内存盘驱动；任务源码与小模型按内容资产同步。只有确需常驻的大工程才放 `.projects/` 并写本机shared配置。

### 从当前中心更新 Aya1-TX

```python
from remote_compute import Client

center = Client()
node = next(n for n in center.nodes()["nodes"] if n["name"] == "Aya1-TX")
if node["active_jobs"] or node["queued"]:
    raise RuntimeError("Aya1-TX 仍有任务，待空闲后更新")
machine = center.machine("Aya1-TX", cwd="D:/Desktop/Remote-Compute")
result = machine.python("from remote_compute.repository import update; print(update())")
print(result["status"], result["stdout"], result["stderr"])
if result["status"] == "succeeded":
    print(machine.restart())
```

`update` 仅允许 main 快进更新；本地源码改动先提交再更新。更新后检查两端 `git rev-parse HEAD` 一致，`nodes` 中设备重新在线。`restart` 在节点仍有任务时返回 409，空闲后重试。hybrid 的本机服务由启动终端托管，更新本机后退出该终端并重新运行启动脚本。

## 每台设备唯一8 GiB共享内存盘

所有工程使用 `.codex-shared-ramdisk.json` 和 `Global\CodexSharedRamdiskV1`。创建前持有同一个系统锁并重新检测；已存在小盘原地扩容，已有盘扩容失败时保留它并报告原因，不另建第二块。调用 `python main.py ramdisk` 可创建或检查。Windows需安装一次ImDisk驱动，首次安装/扩容使用管理员权限；驱动包存放在 `.tools/imdisk/` 后运行 `python main.py ramdisk --install`。安装时登记系统启动任务 `RemoteCompute Shared RAM`，重启后重新创建唯一共享盘；日常工程启动直接复用，每个工程使用自己的子目录。目标容量固定为8GiB，命令成功后可检查实际盘容量；扩容失败时先处理现有盘，再继续使用。

Linux 复用已有 `/dev/shm`；该入口不修改系统 tmpfs 容量。当前 Windows ImDisk 使用固定容量 vm 模式，8 GiB 是盘容量，未提供按写入增长的动态内存盘模式。

## 工程内工具安装

```text
python main.py setup --ngspice-source "D:/Program Files/ngspice" --rtl-source "D:/Projects/#4-Arclight/ARH-DSE-v2/.tools/msys64"
```

Windows 的 `setup` 安装固定版本的可移动Python3.12科学环境，验证Numba JIT、OpenCV与ONNX Runtime CPU推理后写入本机工具配置；首次可直接下载Python/ngspice，也可复用已有ngspice。`--rtl-source` 提取现有MSYS2中GCC/Verilator的实际依赖，排除编译链无关组件与下载缓存。

AOP/RGCN SPICE、Bench优化、ARH CPU推理共用 `.tools/python`（约565MiB）；`.tools/ngspice`约38MiB，精简RTL环境约608MiB。原工程模型训练仍使用各工程环境。跨机复制运行时后，用`setup`校验版本并重定位路径，再从中心重载worker。

## 部署到其他设备

复制工程源码，准备 Python 3.11+，即可使用 worker 模式提供远程 Python、文件访问和控制。`worker.example.json` 是独立节点的最小配置，不依赖 AOP、RGCN、ARH-DSE 或 Analog-Opt-Bench。计算任务需要的源码可以通过资产包发送，依赖和工具按实际任务安装；工程解释器别名可在 `tools` 中追加。`python main.py --check` 用于检查仿真工具。Windows 的原生 Verilator 可通过 `env.MSYS2_ROOT` 指向已有 MSYS2；Linux 在 Python 环境中按调用工程安装科学依赖，配置 tools 指向解释器与 ngspice，Verilator 和编译器使用 PATH 中的工具；setup 当前提供 Windows 可移动工具链。

## 常见问题

| 现象 | 操作 |
| --- | --- |
| `--url` 连接被拒绝 | 先运行 `start.cmd` / `start.sh`；`--url` 只连接已有服务。默认中心端口为 26133。 |
| 地址仍是 8765 / 8766 | 更新设备配置、`nodes.local.json` 和调用工程的显式 URL，再重启；统一使用 26133，本机 hybrid 内部 worker 用 26134。 |
| 端口已占用 | 按提示选择 reuse / kill / admin-kill / cancel；提示会列出 PID 与命令。 |
| 找不到旧工程共享目录 | 独立 worker 可用 `worker.example.json`；删除迁移配置中不需要的 shared 条目，计算工具按实际任务配置。 |
| 工具路径指向另一台电脑 | 运行 setup 或修改该设备 tools/env 路径，然后重启 worker；源码仓库不包含这些运行时。 |
| 远端不能连接 | worker 使用 `--host 0.0.0.0`；管理员 PowerShell 放行 TCP 26133 入站，再从中心检查 nodes。 |
| 设置线程数被拒绝 | 检查设备 cpu_limit；Aya1-PC 上限 24，Aya1-TX 上限 31、默认 30。 |
| CPU 仍未满载 | 先查看工程排队量、份额、任务内存、能力匹配与整轮线程需求；线程预留与实际 CPU 利用率分别显示。 |

管理员 PowerShell 放行远端 worker 端口：

```powershell
New-NetFirewallRule -Name RemoteCompute-Control-26133 -DisplayName "Remote Compute 26133" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 26133
```

服务直接连接已配置的局域网地址；需要共享 token 时两端设置同一个 `REMOTE_COMPUTE_TOKEN`。工作目录与结果回收规则见[工程接入](integration.md#任务文件与结果)。
