# Distributed Compute Orchestrator（DCO）

DCO 提供统一计算调度、远程任务执行和设备运维。调用工程保留科学测量、评分、实验预算和结果缓存；DCO 负责能力匹配、CPU/RAM/加速器授权、任务生命周期与传输。服务、SDK、终端只依赖 Python 标准库，日常使用系统标准 GIL x64 CPython 3.14；源码兼容 3.7–3.15，科学工具另按任务安装。

仓库名为 `distributed-compute-orchestrator`，Python 包仍为 `remote_compute`，环境变量仍为 `REMOTE_COMPUTE_*`。源码来自 [Ayai01/distributed-compute-orchestrator](https://github.com/Ayai01/distributed-compute-orchestrator)。

## 启动

在工程目录的可见终端运行：

```text
start.cmd
sh start.sh
python -B main.py --help
```

按系统选择一个启动脚本；脚本切到工程根，优先系统 Python 3.14，再尝试当前 Python 与已有工程环境。Windows 双击 `start.cmd` 也可。包安装用对应解释器的 `python -m pip`。

| 模式 | 命令 | 用途 |
| --- | --- | --- |
| hybrid | `python -B main.py --mode hybrid` | 中心及进程内本机 worker；新克隆的默认模式 |
| scheduler | `python -B main.py --mode scheduler` | 仅调度其他 worker |
| worker | `python -B main.py --mode worker --host 0.0.0.0 --port 26133` | 独立算力及机器操作 |
| 连接已有中心 | `python -B main.py --url http://127.0.0.1:26133` | 打开操作看板 |

默认读取 `worker.local.json` 和 `nodes.local.json`，可用 `--config`、`--nodes` 指定。CLI 显式参数覆盖配置。独立 worker 只需 Python 和本工程，科研目录属于按需登记的共享能力。

服务保持一个可见前台终端，窗口可失焦或最小化。`--headless` / `--no-browser` 仅表示不弹浏览器；DCO本身为终端服务，这两个同义选项不改变看板或服务。只关闭交互看板使用 `--no-dashboard`，仍保留前台日志和终端要求。隐藏窗口、重定向输入输出、nohup 和无终端 systemd/计划任务会在启动检查中退出。一次性 CLI、Python API 和测试支持管道。桌面自启动使用登录后终端快捷方式；Linux `.desktop` 使用 `Terminal=true`。旧隐藏服务先保留在途任务和未领取结果，排空后从可见入口迁回。

Windows 常驻服务通过具名的 SYSTEM/EXECUTION 电源请求阻止自动休眠和执行挂起，显示器仍按系统设置熄屏；Linux 在有 systemd-logind 的环境持有 idle 抑制锁。请求由托管父进程与服务子进程分别持有，随进程退出释放。每个端点只开一个交互窗口，重复启动保留原服务。端口冲突提示显示 PID/命令，可选择 `reuse/kill/admin-kill/cancel`；日常优先连接现有服务，结束进程前核对任务。

### 地址和端口参数

| 模式 | `--host` | `--port` | `--center-port` |
| --- | --- | --- | --- |
| hybrid / scheduler | 中心监听，默认 `0.0.0.0` | 默认 26133 | 指定时覆盖中心的 port |
| worker | worker 监听 | 默认 26133 | 不使用 |

hybrid 本机 worker 由中心进程内调用，不开额外端口；`local://主机名` 是内部标记。工程连接中心，独立 worker 由中心节点配置连接。`--host 127.0.0.1` 限定中心为本机入口；远端资源借还仍经 worker 本地 lease 和中心已有连接完成。`--port 0` 供隔离验证使用。

## CLI 与自动化

先启动常驻服务，再从另一终端调用 `--url`。全局参数放在子命令前；任务参数放在 `run/spice/rtl` 后。全局 `--cpus/--memory-mb` 是提供者预算，任务同名参数是本次任务需求。

```text
python -B main.py --url http://127.0.0.1:26133 nodes
python -B main.py --url http://127.0.0.1:26133 jobs
python -B main.py --url http://127.0.0.1:26133 run examples/spice/task.json --input examples/spice --output results/divider
python -B main.py --url http://127.0.0.1:26133 spice examples/spice/divider.spice --output results/divider-direct
python -B main.py --url http://127.0.0.1:26133 rtl examples/rtl --top tb --sources tb.sv --output results/rtl
python -B main.py --url http://127.0.0.1:26133 batch examples/batch.json results/batch
python -B main.py --url http://127.0.0.1:26133 fetch TASK_ID --output results/recovered
```

`TASK_ID` 换成实际 ID。单任务成功返回 0，任务失败返回 1，配置/通信错误返回 2。自动化保存返回 ID，用原 ID 查状态、日志和取回结果；短暂断连继续查询原任务。成功取回默认清理服务端副本，`--keep` 用于显式保留。任务 JSON、SDK 与恢复语义见 [DESIGN](docs/DESIGN.md)。

<a id="终端操作"></a>

## 实时终端与运行中控制

下列命令用于启动后的 `调度>` 或 `worker>` 输入行；它们与外层 `main.py` 子命令的参数格式分别解析。

| 命令 | 作用 |
| --- | --- |
| `help` / `help all` / `help cpus` | 分组列表、全部命令、单命令用法 |
| `status` / `nodes` / `jobs` / `jobs ab12` | 资源、节点、任务及明细 |
| `cpus 16` / `memory 8G` | 当前本机预算，保存到设备配置 |
| `cpus Aya1-TX 16` / `memory Aya1-TX 8G` | 指定设备预算 |
| `calibrate` / `calibrate Aya1-TX` | 全部/指定设备的原生算力校准 |
| `logs ab12` / `cancel ab12` | 查看日志 / 取消任务 |
| `fetch ab12 "D:/结果"` | 取回并清理任务副本 |
| `pause Aya1-TX` / `resume Aya1-TX` | 停止/恢复接收新任务，已有任务继续 |
| `restart --wait` / `restart Aya1-TX --wait` | 等空闲后在原终端重载中心/worker |
| `add http://192.168.1.10:26133` / `save` | 加入节点 / 保存路由配置 |
| `policy adaptive` | 切换中心策略 |
| `set Aya1-TX limit=8 memory_limit_mb=4096 enabled=true` | 限制本中心向节点派发的资源 |
| `sync_cache all` / `sync_cache arh --kind research` | 手动增量同步，详见下节 |
| `wait` / `quit` | 等待本终端后台操作 / 退出 |

任务表的Tag保留对象、阶段/方法和必要预算，例如 `AB-warm/4alg/b24k`、`fold-open/FACE/b12k`、`FSA/train/PVT/phy0`、`Rocket/prep`。工程、CPU和任务ID已有单独列，Tag不重复这些字段或长句；完整原标签继续由 `jobs ab12` 明细、API和回执保存。

设备名换成 `nodes` 的实际名称，也可用 `#编号`。任务支持唯一的至少四位 ID 前缀，API 使用完整 ID。`memory` 无单位为 MiB，`8G/8GB/8GiB` 均为 8 GiB，0 自动预算。`set limit` 限制本中心派发量，`cpus` 修改提供者总预算。

Tab 在空输入时显示根命令候选，有输入时补全；Ctrl+Tab 翻任务页，空输入 Shift+Tab 反向翻页，PgUp/PgDn 翻输出，↑/↓ 选历史，Esc/Ctrl+C 清空输入，Enter 执行。补全只读当前快照。看板聚焦每秒刷新、失焦每 5 秒刷新；任务运行秒数从实际执行开始，线程预留与 CPU 使用率分列。

`--url` 看板退出会收尾本终端持有的普通操作并断开；`--detach` 任务留在服务中。托管服务的终端退出会停止其服务和在途进程，因此批处理客户端应连接已有服务。

## 设备预算与配置文件

动态 CPU 数字只在配置和看板维护；[STATUS](docs/STATUS.md) 保存带日期的核对记录。通用默认 `cpus/cpu_limit` 为逻辑线程数。`cpu_limit` 是硬上限，`cpus` 是当前预算；在线调低后让在途工作自然完成，后续派发使用新值。硬上限修改随重载生效。

```json
{
  "mode": "worker", "host": "0.0.0.0", "port": 26133,
  "cpus": 8, "cpu_limit": 8, "memory_mb": 0, "memory_reserve_mb": 16384,
  "memory_headroom_mb": 8192, "ramdisk_gib": 8,
  "tools": {}, "shared": {}, "env": {}
}
```

这是示例预算，按设备设置。复制 [worker.example.json](worker.example.json) 为 `worker.local.json` 后默认启动才会读取。`memory_mb=0` 默认使用物理内存减 16GiB，显式数值为设备计算预算；`memory_headroom_mb=8192` 为系统实时保留至少 8GiB 可用 RAM。运行任务首次取得进程树 RSS 后按实际占用准入；尚在派发或启动采样前按申报量预留。任务的阶段需求仍保留在回执中，实际用量增长继续计入设备预算。缺失共享路径标记为 `unavailable_shared`，其他能力继续提供。

持久任务、输入、资产及未领取结果放在普通磁盘 `.local/state`：中心为 `scheduler/`，hybrid 本机 worker 为 `local/`，独立 worker 按端口隔离。设备 JSON 的 `root` 只指定 worker 状态根；CLI `--root` 同时指定 worker 根和中心的 `scheduler/` 子目录。`temp_root` 单独指定任务 TMP，默认使用共享 RAM 盘。迁移目录前核对回执和联接，排空后搬完整状态、更新配置再重载。

部署改名后核对 tools/shared/env、`.tools/science.json`、`.tools/rtl.json` 和联接目标。`worker.local.json`、`nodes.local.json`、`.local/`、`.tools/`、`.projects/`、`.runtime/`、结果均留本机；令牌、证书私钥保留在设备配置。服务配置使用私密原子写入。

## 手动缓存同步

`sync_cache all|aop|rgcn|arh|bench` 通过原队列以低优先级单线程增量同步。`--kind research|assets|all` 选择科研缓存、内容资产或两者；不带 `--url` 的一次性 `python -B main.py sync_cache all` 连接本机已有中心。

先比摘要再补缺，科研记录只发给已配置该工程共享目录的消费者；内容资产预热到能力匹配的节点。AOP 传播原始角点，接收端重建聚合；RGCN 传播观测及其视图/来源，任务认领与 WAL 留原设备。ARH 允许共享目录仅有工具和缓存，写入接口来自当前源码资产。模型/源码沿各工程正常分包复用，不遍历历史实验和模型。

AOP 按明文执行/测量协议交换角点；未知版本和未验证旧协议不进入可复用同步。RGCN 的未知协议旧观测仍保留并同步，继续退出测量缓存命中。相同记录再次同步只比较清单，不重复导出数据。

任务日常按需传输继续运行。离线或合并失败显示部分完成，失败接收端停止后续批次，其他节点继续；`pending_jobs` 保留原任务 ID，再次同步按摘要续补。看板 `wait` 等本次同步完成，退出时收尾自己持有的任务。

## 从 Codex 操作另一台电脑

运维使用操作员身份，工程客户端查看汇总能力及经过筛选的任务状态。CLI 只对本机地址（`127.0.0.0/8`、`::1`、字面主机名 `localhost`）使用本机配置的 `operator_token` 或自动读取本机中心状态目录令牌，非默认根使用 `--root`。连接远端中心时，通过 `REMOTE_COMPUTE_OPERATOR_TOKEN` 显式提供该中心令牌；Python 使用 `operator_token`。先用 `nodes` 查目标和实际共享路径，再操作目标工程。

```text
python -B main.py --url http://127.0.0.1:26133 exec --node Aya1-TX --cwd D:/Projects/MyProject -- git status --short
python -B main.py --url http://127.0.0.1:26133 read --node Aya1-TX D:/Projects/MyProject/main.py
python -B main.py --url http://127.0.0.1:26133 put --node Aya1-TX ./source D:/Projects/MyProject --pattern "*.py"
python -B main.py --url http://127.0.0.1:26133 get --node Aya1-TX D:/Projects/MyProject/results ./results/Aya1-TX
python -B main.py --url http://127.0.0.1:26133 python --node Aya1-TX --cwd D:/Projects/MyProject --timeout 7200 --detach ./maintenance.py
```

```python
from pathlib import Path
from remote_compute import Client

center = Client(operator=True,
                operator_token=Path(".local/state/scheduler/operator.token").read_text().strip())
machine = center.machine("Aya1-TX", cwd="D:/Projects/MyProject")
print(machine.listdir())
print(machine.execute(["git", "status", "--short"])["stdout"])
job = machine.python(script="./maintenance.py", args=["--check"], wait=False)
center.wait(job["id"])
center.fetch(job["id"], "./results/maintenance")
```

`exec` 直接传 argv；需要 shell 语法时显式选择目标 shell。`python` 上传本地脚本，用目标机工具执行，可用 `--tool` 指定登记的 Python。`--stdin` 传文件，`--env NAME=VALUE` 设置变量，`--output` 保存完整日志。API 提供 read_text/write_text/upload/download；上传按文件合并并原子替换文件。日志摘要保留末尾 16 KiB，完整日志随结果取回。`--detach` / `wait=False` 返回任务 ID，调用者随后按原 ID 领取。运维默认 retries=0，避免重复文件副作用。

## 同一仓库、设备独立配置

`python -B main.py update --check` 查看更新，`update` 只允许统一仓库 main 快进；已有改动先完成其所属工作。源码更新后运行服务仍用已加载版本，空闲时从看板执行 `restart --wait`，远端用 `restart 节点 --wait`。Python 为 `center.restart(节点或None, wait=True)`；不带 wait 时忙碌服务返回 409。重载保持原终端、配置及未领取结果，通过操作员 health 的 pid/version 核对生效。

远端更新用上述 `machine(...).execute(["git", "pull", "--ff-only"])` 或项目的 update 入口；cwd 取 `nodes` 实际部署路径，先检查活跃任务与相关会话。先更新并重载 worker，再重载中心。

## 每台设备唯一共享内存盘

Windows 使用 `.codex-shared-ramdisk.json` 和 `Global\CodexSharedRamdiskV1` 在锁内检查/创建唯一共享盘，工程目录独立。`ramdisk_gib` 默认 8，可设为 16；`python -B main.py ramdisk` 检查/创建。ImDisk 驱动放 `.tools/imdisk/` 后，以管理员运行 `ramdisk --install`。安装登记开机重建任务 `RemoteCompute Shared RAM`。容量扩展失败保留现有盘，先处理它；现有较大盘不会自动缩小。

Linux `ramdisk` 入口创建独立 tmpfs，默认位于 `~/.cache/codex-shared-ramdisk`，可用 `ramdisk_root` 指定位置，容量遵循 `ramdisk_gib`；挂载需要对应权限。使用普通磁盘的 worker 保留显式 `root` 和 `temp_root`，配置 temp_root 后启动直接使用它。状态文件保留普通磁盘，临时目录生命周期见 DESIGN。

## 工程内工具安装

```text
python -B main.py setup --ngspice-source "D:/Program Files/ngspice" --rtl-source "D:/Projects/#4-Arclight/ARH-DSE-v2/.tools/msys64"
python -B main.py --check
```

setup 复用系统 Python 3.14，验证科学依赖后写本机 tools/env；Windows 原生工具放忽略的 `.tools/`。ngspice 可下载或复用，实际版本 47 才登记 ng47。RTL 从已有 MSYS2 提取 GCC/Verilator 闭包，保留标准库和必需运行依赖。Linux 直接配置已有 Python/ngspice/编译器；setup 的可移动工具链面向 Windows。

Python 能力按实际运行环境登记；新增算法先复用已有环境，不按算法或工程重复增加名字。

| 规范能力 / 工具 | 同一解释器内的就绪条件 |
| --- | --- |
| `python_numpy` | NumPy；用于 AOP、ARH 普通执行及 RGCN 原生测量 |
| `python_scientific` | NumPy、SciPy、Numba、pymoo、moocore、threadpoolctl、scikit-learn、umap-learn；用于完整科学计算环境 |
| `python_torch_pyg` | NumPy、Torch、PyG、SciPy、Numba、moocore、threadpoolctl；用于当前完整神经与数值环境 |
| `onnx` | 经调用工程数值验收的 NumPy、ONNX、ONNX Runtime CPU 与 OpenCV 环境；固定 INT8 继续使用已有兼容解释器 |
| `layout_python` | NumPy、gdstk、GLayout、gdsfactory、rectpack 及实际所需符号；PDK 由任务材料资产提供 |

每个环境分别用实际解释器验证，不能把不同解释器中的包合起来当成同一环境就绪。`python` 只承诺 Python 标准库，不隐含上述包。CatBoost 等可选后端按实际调用检查，不作为所有神经任务的统一门槛。Magic、Netgen、ng47、Verilator、CIRCT、Java/Scala 和 C++ 仍是独立的真实工具需求。

旧 `aop_python/arh_python/rgcn_python/aop_hspice_python` 等价于 `python_numpy`，`bench_python` 等价于 `python_scientific`，`rgcn_train/rgcn_fiber` 等价于 `python_torch_pyg`；`onnx_cpu/circt-translate/ngspice47` 分别等价于 `onnx/circt/ng47`。旧请求和模板继续兼容，已接受的原 ID、输入及执行绑定保持；新准入和能力汇总使用规范名。新部署先登记已实测的新绑定，忙碌实例保留旧绑定，空闲重载后再退出重复登记。

GPU 任务同时声明工具能力和 accelerator。支持 CPU/GPU 两条执行路径的调用方可协商 `accelerator_fallback`，具体请求和回执见 [DESIGN](docs/DESIGN.md#工程接口边界)；ARH 的 auto 使用30秒未派发队列阈值，显式 torch_cuda 保持强制GPU。`shared` 只登记真实部署目录，ARH 工具/模型及持久统计可通过 `shared:arh` 复用。

已验证工具可用操作员 `center.control(node=名称, worker={"tools": {别名: [可执行路径]}})` 在线登记，None 移除；后续任务用新别名。该操作更新运行进程，部署同时更新设备 `worker.local.json` 以便重载。临时能力验收后再登记正式别名。

ngspice 47 统一登记 `ng47`，旧 `ngspice47` 请求等价。能力名称不含哈希、设备或 OS 选择条件；注册和准入会拒绝这类名称。能力与执行引擎不生成或保存哈希，科研回执和科学缓存也不记录引擎哈希。数值兼容性使用明文引擎族/主版本、执行协议、测量协议和容差；源码、模型与输入包仍按内容复用和验证完整性。

看板短表头：`CPU U/B` 为已用/预算逻辑线程，`RAM U/T` 为主机已用/总内存，`Avail` 为可分配内存，`RSS/Res` 为进程实际/已预留内存，`1T/All` 为单线程/总基准，`Share/Pri` 为份额/优先级，`Jobs/CPU` 为任务/线程，`Sec` 为运行秒数。

## 部署到其他设备

源码和 Python 即可启动独立 worker；能力按实际任务补齐。设备使用系统 hostname，配置 host/port、CPU/RAM、tools/shared/env。Linux 普通磁盘示例为 `root=/home/user/.local/state/dco/worker`、`temp_root=/home/user/.local/state/dco/tmp`，在可见终端运行 `python3 -B main.py --config worker.local.json`。

独立 worker 支持原生 HTTPS：本机设置 certfile/keyfile，中心节点 URL 使用 HTTPS 26133 并设置 ca_file；私钥留 worker。CPU/RAM 借还通过 worker 本地 lease 与中心既有连接，公网 worker 直接使用该通路。Linux 额外看板可用 `--mode worker --url https://127.0.0.1:26133 --config worker.local.json`，原托管终端继续保持。

Windows 远端端口按部署需要用管理员 PowerShell 放行：

```powershell
New-NetFirewallRule -Name RemoteCompute-Control-26133 -DisplayName "DCO 26133" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 26133
```

## 常见问题

| 现象 | 检查与处理 |
| --- | --- |
| `--url` 连接拒绝 | 先启动对应服务；URL 指向中心的实际地址和端口。 |
| 启动要求可见终端 | 从 cmd/PowerShell/终端直接运行；headless 仍保留前台终端。 |
| 端口占用/重复窗口 | 核对 PID 和任务，连接已有服务或明确处理冲突。 |
| 工具/共享目录缺失 | 检查该设备 tools/shared/env 和能力，重新验证并登记正确路径。 |
| 线程数设置被拒绝 | 查看当前设备 cpu_limit，按硬上限调整。 |
| 排队或 CPU 未满 | 查看能力、就绪任务、工程份额、内存和任务实际线程需求。 |
| 断连后仍有任务 | 记录原 ID，恢复连接后查询并取回；结果持久保留。 |
| 旧端口或旧目录 | 同步本机配置与调用工程 URL，排空后重载，核对完整状态根。 |

维护验证入口：`python -B main.py --test` 是核心回归，`--test-native` 增加真实 ngspice/Verilator；按改动选择范围。当前已验证事实见 [STATUS](docs/STATUS.md)。

## 文档分工

[AGENTS](AGENTS.md) 管协作；[DESIGN](docs/DESIGN.md) 管协议、调度和内部机制；[STATUS](docs/STATUS.md) 管当前事实及验证；[HISTORY](docs/HISTORY.md) 管重要变化；[ROADMAP](docs/ROADMAP.md) 管已确认后续。README 集中实际使用入口，示例和配置另见 [examples](examples/)、[worker.example.json](worker.example.json)、[nodes.example.json](nodes.example.json)。
