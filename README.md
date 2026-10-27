# Distributed Compute Orchestrator（DCO）

DCO 提供统一计算调度、远程任务执行和设备运维。调用工程保留科学测量、评分、实验预算和结果缓存；DCO 负责能力匹配、CPU/RAM/加速器授权、任务生命周期与传输。服务、SDK、终端只依赖 Python 标准库，日常使用系统标准 GIL x64 CPython 3.14；源码兼容 3.7–3.15，科学工具另按任务安装。

仓库名为 `distributed-compute-orchestrator`，Python 包仍为 `remote_compute`，环境变量仍为 `REMOTE_COMPUTE_*`。源码来自 [Ayai01/distributed-compute-orchestrator](https://github.com/Ayai01/distributed-compute-orchestrator)。

## 科研计算的工作段

长科研任务按完整世代、训练阶段或原生批次安排执行段，单段计算最长30分钟；长段通常以20–25分钟为目标，为封存和退出留时间。自然短批次按实测吞吐保留。现有充足准备队列继续供给，只提交输入、依赖与预算已经就绪的段；依赖上一段状态的后续工作在完成事件后解锁。

科研总预算、种子和原科学任务身份跨段连续，新段执行ID显式映射，原接受请求及回执保留。受理未明时继续查询原执行ID。完整恢复状态、已付观测和费用账本在发起端持久化；远端只保留本段必要输入与未领取增量，模型、工具和只读资产沿原引用复用。

`write_job_file` 原子投递项目约定的控制文件，`read_job_file_changed` 按版本领取已封存状态。worker在安全边界保存完整状态并正常退出；DCO取消/超时保留进程树终止语义。既有冻结worker按实际支持的恢复边界接续。段终态按 `fetch(delete=False)` → 本地持久发布增量、恢复状态和费用 → `delete` 收齐，释放一次性目录；活跃引用继续保护。细节见[设计合同](docs/DESIGN.md#分段计算和本地持久结果)。

看板任务行的 `1/8` 表示1个任务、授予8核；实际并行度由项目执行器配置，并按进程/线程CPU样本核对。

## 启动

在工程目录的可见终端运行：

```text
start.cmd
sh start.sh
python -B main.py --help
```

按系统选择一个启动脚本；脚本切到工程根，优先系统 Python 3.14，再尝试当前 Python 与已有工程环境。Windows 双击 `start.cmd` 也可，支持Windows Terminal默认终端委托，不要求其窗口进程位于cmd父进程链。包安装用对应解释器的 `python -m pip`。

| 模式 | 命令 | 用途 |
| --- | --- | --- |
| hybrid | `python -B main.py --mode hybrid` | 中心及进程内本机 worker；新克隆的默认模式 |
| scheduler | `python -B main.py --mode scheduler` | 仅调度其他 worker |
| worker | `python -B main.py --mode worker --host 0.0.0.0 --port 26133` | 独立算力及机器操作 |
| 连接已有中心 | `python -B main.py --url http://127.0.0.1:26133` | 打开操作看板 |

所有设备使用同一份源码和启动脚本；角色、地址、端口、预算、工具及路径差异保存在 Git 忽略的 `worker.local.json` 和 `nodes.local.json`。启动优先级为显式 CLI 参数 > 本机配置 > 内置默认值，可用 `--config`、`--nodes` 指定文件；本机配置中的 `mode/host/port/center_port` 同样生效。独立 worker 只需 Python 和本工程，科研目录属于按需登记的共享能力。

重启维护使用 `python -B main.py --start-paused`，或在本机配置设置 `start_paused:true`。中心在启动调度线程前暂停新派发，仍可查询、领取和删除原ID；已受理任务可重连。一次性 `run/spice/rtl/batch` 若会创建暂停中心，立即提示退出。连接已有中心的 `--url` 不继承本机启动设置。`control(paused=False)` 只恢复本次运行；维护完成后显式清除配置中的 `start_paused`，以后启动才自动派发。配置保留期间，每次重启都回到维护暂停。

独立 worker 开始监听后，已登记它的中心即可自动重连并下发任务，无需在 worker 再手动连接；看板刷新不作为任务受理门槛。worker 重启只把旧未完成任务标记为 `interrupted`，原 ID 的恢复由中心协调。需要从首次受理就限定 CPU 时，在启动前设置本机配置的 `cpus` 或使用 `--cpus N`；运行中修改预算会保存，已接受任务保留原授予直至归还。

服务保持一个可见前台终端，窗口可失焦或最小化。聊天应用或编辑器主窗口可见，不代表其中的工具终端可见。`--headless` / `--no-browser` 仅表示不弹浏览器；DCO本身为终端服务，这两个同义选项不改变看板或服务。只关闭交互看板使用 `--no-dashboard`，仍保留前台日志和终端要求。隐藏窗口、重定向输入输出、nohup 和无终端 systemd/计划任务会在启动检查中退出。一次性 CLI、Python API 和测试支持管道。桌面自启动使用登录后终端快捷方式；Linux `.desktop` 使用 `Terminal=true`。旧隐藏服务先保留在途任务和未领取结果，排空后从可见入口迁回。

Windows 常驻服务通过具名的 SYSTEM/EXECUTION 电源请求阻止自动休眠和执行挂起，显示器仍按系统设置熄屏；Linux 在有 systemd-logind 的环境持有 idle 抑制锁。请求由托管父进程与服务子进程分别持有，随进程退出释放。Windows关闭服务窗口、POSIX终端挂断或SIGTERM会请求正常退出，取消本入口持有的任务并保留原ID回执；关闭另开的`--url`看板只结束该看板及其自有操作。每个端点只开一个交互窗口。端口冲突显示 PID/命令，可选择 `reuse/kill/admin-kill/cancel`；默认Cancel只取消本次启动，保留旧服务，显式reuse才连接旧实例。结束进程前核对其任务。

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

工程控制器可用 `client.window(project, ready_tasks=256, ...)` 设置整个工程的待收齐任务窗口，由中心继续分配实际 CPU/RAM/GPU。`window.available(len(pending))` 提供提交背压；pending 包括已排队、运行、尚未发布的终态及受理结果未明的原 ID。`client.statuses(ids)` 每128个已知 ID 合并查询，`wait_any(ids)` 在一个采集循环等终态，完整费用随 `fetch` 领取。旧中心继续兼容，接口、共享计数与结果发布示例见 [DESIGN：提交与查询窗口](docs/DESIGN.md#向中心提交与查询窗口)。

<a id="终端操作"></a>

## 实时终端与运行中控制

下列命令用于启动后的 `调度>` 或 `worker>` 输入行；它们与外层 `main.py` 子命令的参数格式分别解析。

| 命令 | 作用 |
| --- | --- |
| `help` / `help all` / `help cpus` | 分组列表、全部命令、单命令用法 |
| `status` / `nodes` / `jobs` / `jobs ab12` | 资源、节点、任务及明细 |
| `cpus 16` / `memory 8G` | 当前本机预算，保存到设备配置 |
| `cpus Aya1-TX 16` / `memory Aya1-TX 8G` | 指定设备预算 |
| `calibrate` / `calibrate Aya1-TX` | 人工触发全部/指定设备校准；启动、重连及定期更新均不自动校准 |
| `logs ab12` / `cancel ab12` | 查看日志 / 取消任务 |
| `fetch ab12 "D:/结果"` | 取回并清理任务副本 |
| `pause Aya1-TX` / `resume Aya1-TX` | 停止/恢复接收新任务，已有任务继续 |
| `restart --wait` / `restart Aya1-TX --wait` | 等空闲后在原终端重载中心/worker |
| `scan [IPv4-CIDR] [--add]` | 探测局域网可用计算节点；默认只列出，`--add`沿add接入并保留原预算 |
| `add 192.168.1.10` / `save` | 加入节点（默认26133）/ 保存路由配置 |
| `policy adaptive` | 切换中心策略 |
| `set Aya1-TX limit=8 memory_limit_mb=4096 enabled=true` | 限制本中心向节点派发的资源 |
| `sync_cache all` / `sync_cache arh --kind research` | 手动增量同步，详见下节 |
| `wait` / `quit` | 等待本终端后台操作 / 退出 |

任务Tag保留对象、方法/阶段和必要预算，例如 `TSA s06 FIBER 12k`、`AB warm 4alg`、`Rocket prep`。seed可只显示后两位；工程、CPU和ID使用各自列。缩写只影响展示，完整seed、原标签、输入与科学身份继续由 `jobs ab12`、API和回执保存。

设备名换成 `nodes` 的实际名称，也可用 `#编号`。任务支持唯一的至少四位 ID 前缀，API 使用完整 ID。`memory` 无单位为 MiB，`8G/8GB/8GiB` 均为 8 GiB，0 自动预算。`set limit` 限制本中心派发量，`cpus` 修改提供者总预算。

单设备独立验收先记录节点的启用状态和预算，用`set 节点 enabled=false`停止该中心的新派发，等待该节点`reserved=0`且`active_jobs`为空。已有任务、原ID、回执和其他节点继续保留；不用退出服务来排空。HTTP/TLS节点可通过已有操作员worker连接执行隔离测试，hybrid本机在排空后使用本机独立测试进程。禁用且已排空的节点停止周期健康探测，并从生产看板与通知中隐藏；设备配置仍保留，`set 节点 enabled=true` 后恢复发现，显式设备维护查询继续可用。恢复时先收齐测试输出、清理本次任务并恢复设备预算，刷新中心节点健康，最后`set 节点 enabled=true`。脚本用`control(node=名称,routing={"enabled":False/True})`并在finally恢复原状态；禁用路由后不再通过同一中心向该节点提交维护任务。

连续60秒没有成功健康握手的设备自动移出资源看板，随后每60秒重连一次，最多5次；成功恢复原预算，五次失败后等`add 设备IP`或`scan --add`。次数与下次时间保存在忽略配置，重开中心继续剩余次数；人工停用的设备不自动重连。原任务、回执、预算和凭据保留。`scan`只探测可用节点，`scan --add`批量接入。默认扫描本机IPv4局域网段，也可用`scan 192.168.1.0/24 --port 26133 --timeout 0.7`指定；一次最多4096地址，不发送唤醒包，也不周期扫描。一次性入口为`python -B main.py scan`，加`--add`连接已有中心。

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

持久任务、输入、资产及未领取结果放在普通磁盘 `.local/state`：中心为 `scheduler/`，hybrid 本机 worker 为 `local/`，独立 worker 按端口隔离。设备 JSON 的 `root` 只指定 worker 状态根；CLI `--root` 同时指定 worker 根和中心的 `scheduler/` 子目录。`temp_root` 缺省或null均使用共享RAM的DCO专属TMP；没有RAM盘时回落到工程`.local/tmp`。显式磁盘路径继续生效。

| DCO目录 | 内容与回收规则 |
| --- | --- |
| `.local/state/` | 原任务、回执、检查点和待领取结果；收妥后按原ID清理，不按年龄丢弃 |
| `.local/cache/` | 新建的编译、数值库及模型/依赖缓存；`cache_root`可配置，任务env覆盖设备env及默认值 |
| `.tools/` | 缺少时新安装的工具及下载缓存；已能工作的外部安装直接引用，不复制环境 |
| `.projects/` | DCO执行副本及其科研缓存；主工程工作区与正式发布成果继续由工程维护 |
| `.local/tmp/`、共享RAM中的DCO目录 | 可重建临时物，按任务所有权回收 |

以上目录与设备配置均被Git忽略。Python/Java/CUDA等系统运行环境及已有可用的外部工具、模型和依赖缓存原位复用；DCO新生成的信息集中在上述目录，显式缓存环境变量优先。未配置的Numba/Matplotlib/CUDA编译缓存默认写DCO；已有Coursier/Torch/HuggingFace依赖或模型缓存直接复用，避免再次下载。不要移动或清理在途任务正在使用的目录。

旧worker状态迁入DCO时，在本机配置中将`root`设为新的`.local/state/...`路径，并临时添加`migrate_root_from`旧根。排空后正常重载；启动会先验证旧worker已退出，再在同一文件系统整体重命名并移除迁移字段。目标目录须尚不存在，原ID、回执、输入及链接原样保留；不合并两套状态、不复制工具目录。

资源协商使用独立的 `lease_timeout`，默认取普通 `timeout` 与130秒中的较大值：远端 worker 的初始 lease 和本机直连中心的 resize 都使用这一期限，worker 可等待中心回复120秒。普通请求及中心的资源答复保持普通期限，`pool_timeout` 只管理本地连接排队。lease 或 resize 超时均不自动重发，资源变更可能已生效；先按原ID核对实际授予和任务状态。任务跟进使用 `status(id)`，全量 `status()` 读取全部保留历史，勿用于高频轮询或费用总账；已显式删除的任务由调用工程保存原回执与费用。

部署改名后核对 tools/shared/env、`.tools/science.json`、`.tools/rtl.json` 和联接目标。`worker.local.json`、`nodes.local.json`、`.local/`、`.tools/`、`.projects/`、`.runtime/`、结果均留本机；令牌、证书私钥保留在设备配置。服务配置使用私密原子写入。

临时大输入在准备阶段已确定原任务ID时，使用 `client.sync_files(files, job_id=原ID)`，提交也传同一 `job_id`；若准备取消，调用 `client.release_assets(原ID)`。引用覆盖同步至受理的间隔，任务收妥删除后按LRU回收；多生产者共享同一内容分别受保护。重开或交接1小时未更新后重新同步原冻结内容。固定源码/模型继续分包复用。协议和旧服务兼容规则见[DESIGN](docs/DESIGN.md#接入与传输约定)。

已有冻结ZIP的恢复入口用 `client.has_asset(资产ID, job_id=原ID)` 检查是否存在，返回布尔值；短暂读超时沿既有有限重连处理。存在时直接复用，缺失时才补传原ZIP，查询不打包、摘要或上传内容。

## 手动缓存同步

`sync_cache all|aop|rgcn|arh|bench` 通过原队列以低优先级单线程增量同步。`--kind research|assets|all` 选择科研缓存、内容资产或两者，省略时为 `all`；不带 `--url` 的一次性 `python -B main.py sync_cache all` 连接本机已有中心。

先比摘要再补缺，科研记录只发给已配置该工程共享目录的消费者；内容资产预热到能力匹配的节点。AOP 传播原始角点，接收端重建聚合；RGCN 传播观测及其视图/来源，任务认领与 WAL 留原设备。ARH 允许共享目录仅有工具和缓存，写入接口来自当前源码资产。模型/源码沿各工程正常分包复用，不遍历历史实验和模型。

`research` 的范围是生产者持久观测库，包含仍留在库中的历史实验条目；不扫描历史目录并不表示只复制当前运行。日常准备用 `sync_cache all --kind assets`，科研恢复由原任务输入携带所需检查点和已付观测。完整研究库同步用于明确需要的库迁移或对齐，不放入周期巡检。

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

`Client.wait` 默认从0.15秒轮询开始，同一状态持续较久时逐步放缓，最多5秒；状态或已报告阶段变化后恢复短间隔。`poll`指定最短间隔，`max_poll`指定最长间隔；需要固定节拍时令两者相等。可传入`cancel=stop_event.is_set`，本地等待取消时抛出`RequestCancelled`，远端原任务继续运行；需要结束任务时由调用方明确执行`cancel(原ID)`。`wait_timeout`仍为本次调用的绝对期限，终态返回完整原回执。

`exec` 直接传 argv；需要 shell 语法时显式选择目标 shell。`python` 上传本地脚本，用目标机工具执行，可用 `--tool` 指定登记的 Python。`--stdin` 传文件，`--env NAME=VALUE` 设置变量，`--output` 保存完整日志。API 提供 read_text/write_text/upload/download；上传按文件合并并原子替换文件。日志摘要保留末尾 16 KiB，完整日志随结果取回。`--detach` / `wait=False` 返回任务 ID，调用者随后按原 ID 领取。运维默认 retries=0，避免重复文件副作用。

## 任务CPU计量

新任务在 `task.env` 设置 `DCO_CPU_ACCOUNTING=1`，开启CPU秒与频率工作量计量。子进程读取自己的 `REMOTE_COMPUTE_JOB_DIR/result.json`，按 `REMOTE_COMPUTE_WORKER_JOB_ID` 核worker回执身份；中心原ID继续取 `REMOTE_COMPUTE_JOB_ID`。本地快照最多每秒更新一次且CPU计数变化才写，终态完整回执保留同一组累计值，简短进度轮询不增加字段。

`cumulative_cpu_seconds`与既有`cpu_seconds`一致；`cpu_work_ghz_seconds`单位为GHz·s，`cpu_frequency`给出来源、宿主聚合范围和覆盖量。频率部分缺测时主工作量为null，已覆盖的工作量单列；旧回执继续保持原记录。公式与来源见[DESIGN](docs/DESIGN.md#cpu工作量计量)。普通任务不启用此采集。

## 同一仓库、设备独立配置

`python -B main.py update --check` 查看更新，`update` 只允许统一仓库 main 快进；已有改动先完成其所属工作。源码更新后运行服务仍用已加载版本，空闲时从看板执行 `restart --wait`，远端用 `restart 节点 --wait`。Python 为 `center.restart(节点或None, wait=True)`；不带 wait 时忙碌服务返回 409。重载保持原终端、配置及未领取结果，通过操作员 health 的 pid/version 核对生效。

远端更新用上述 `machine(...).execute(["git", "pull", "--ff-only"])` 或项目的 update 入口；cwd 取 `nodes` 实际部署路径，先检查活跃任务与相关会话。先更新并重载 worker，再重载中心。

## 每台设备唯一共享内存盘

Windows 使用 `.codex-shared-ramdisk.json` 和 `Global\CodexSharedRamdiskV1` 在锁内检查/创建唯一共享盘，工程目录独立。`ramdisk_gib` 支持 4、8、16，默认 8；本机配置优先于科研调用的旧容量参数；`python -B main.py ramdisk` 检查/创建。ImDisk 驱动放 `.tools/imdisk/` 后，以管理员运行 `ramdisk --install`。安装登记开机重建任务 `RemoteCompute Shared RAM`。容量扩展失败保留现有盘，先处理它；缩容须先排空本机写入、保全文件，再正常卸载并重建同盘符；启动检查保留较大盘及其文件。

Linux `ramdisk` 入口创建独立 tmpfs，默认位于 `~/.cache/codex-shared-ramdisk`，可用 `ramdisk_root` 指定位置，容量遵循 `ramdisk_gib`；挂载需要对应权限。使用普通磁盘的 worker 保留显式 `root` 和 `temp_root`，配置 temp_root 后启动直接使用它。状态文件保留普通磁盘，临时目录生命周期见 DESIGN。

## 工程内工具安装

```text
python -B main.py setup --ngspice-source "D:/Program Files/ngspice" --rtl-source "D:/Projects/#4-Arclight/ARH-DSE-v2/.tools/msys64"
python -B main.py --check
```

setup复用系统Python，验证科学依赖后写本机tools/env。已有ngspice和MSYS2原位引用，保留其完整运行依赖；仅缺少的ngspice下载到忽略的`.tools/`，实际版本47才登记ng47。Linux直接配置已有Python/ngspice/编译器。

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

CUDA环境由操作员按实际消费者配置在忽略的`.tools`中，服务继续只使用标准库。`python_torch_pyg`必须绑定同一套完整数值环境；ARH另通过设备环境的`ARH_INFERENCE_PYTHON`复用已验证的Torch/OpenCV/ONNX解释器，量化推理保留工程固定的ONNX Runtime版本。安装后先做实际GPU及端到端验证，再登记`accelerators`并在空闲时重载；在途任务继续使用原环境。共享环境的有效消费者及部署证据见[STATUS](docs/STATUS.md)。

已验证工具可用操作员 `center.control(node=名称, worker={"tools": {别名: [可执行路径]}})` 在线登记，None 移除；后续任务用新别名。该操作更新运行进程，部署同时更新设备 `worker.local.json` 以便重载。临时能力验收后再登记正式别名。

Windows 节点接入时按上述规范环境和实际工具补齐能力，再比较看板“运行能力”行；路径及编译器实现保留设备的真实配置。已有 Cygwin 工具原位复用，迁移时同时带上动态库、Tcl 脚本和实际执行器。Magic 批处理使用实际安装的 `magicdnull.exe -nowrapper`，配套 Cygwin 运行库及对应安装目录的 `CAD_ROOT`；当前构建在通用 `tclsh` 加载入口中写 GDS 会异常退出。验收须由 Magic 写 GDS，再用独立解析器读取几何，并检查 LVS 报告。`layout_python` 的 PDK 由任务资产及 `PDK_ROOT` 提供，避免随环境反复复制完整模型。

ngspice 47 统一登记 `ng47`，旧 `ngspice47` 请求等价。能力名称不含哈希、设备或 OS 选择条件；注册和准入会拒绝这类名称。能力与执行引擎不生成或保存哈希，科研回执和科学缓存也不记录引擎哈希。数值兼容性使用明文引擎族/主版本、执行协议、测量协议和容差；源码（含生成RTL）、模型与输入包仍按内容复用和验证完整性。

看板短表头：`CPU U/B` 为已用/预算逻辑线程，`RAM U/T` 为主机已用/总内存，`Avail` 为可分配内存，`RSS/Res` 为进程实际/已预留内存，`1T/All` 为单线程/总基准，`Share/Pri` 为份额/优先级，`Jobs/CPU` 为任务/线程，`Sec` 为运行秒数。

## 部署到其他设备

源码和 Python 即可启动独立 worker；能力按实际任务补齐。设备使用系统 hostname，配置 host/port、CPU/RAM、tools/shared/env。Linux 普通磁盘示例为 `root=/home/user/.local/state/dco/worker`、`temp_root=/home/user/.local/state/dco/tmp`，在可见终端运行 `python3 -B main.py --config worker.local.json`。

局域网节点可直接 `add http://设备IP:26133`，不需要令牌；重复添加会恢复已有节点，保留其预算、权重和在途任务。默认按 TCP 对端的回环、RFC1918、IPv6 ULA/链路本地地址识别局域网，`trusted_networks` 可缩小免令牌网段，例如 `["127.0.0.0/8", "::1/128", "192.168.1.0/24"]`，空列表要求局域网也认证。转发头不改变对端身份。中心操作员权限仍使用独立的本机托管凭据。

非局域网连接必须使用原生 HTTPS 和非空令牌，未配置令牌则拒绝外网请求；SDK不会向公网HTTP地址发送凭据。本机配置 `certfile/keyfile/token`，中心 `nodes.local.json` 对应节点使用 HTTPS URL、`ca_file/token`；校验证书，私钥留 worker。`add 公网IP`自动选择HTTPS和26133端口，复用已保存同地址的证书及节点凭据；首次接入先配置可信CA与节点令牌。证书可放忽略的`.local/certs/`，节点配置继续留本机。显式公网HTTP不会降级接入。CPU/RAM 借还通过 worker 本地 lease 与中心既有连接，公网 worker 直接使用该通路。Linux 额外看板可用 `--mode worker --url https://127.0.0.1:26133 --config worker.local.json`，原托管终端继续保持。

`start.cmd update` / `python3 -B main.py update`支持Git和ZIP部署。Git部署沿原分支快进，保留本地改动与分支核对；Windows可复用`.tools/git/cmd/git.exe`。ZIP部署直接取得官方源码ZIP，替换包内的发布源文件，保留本机配置、运行目录和包外文件，不安装Git；相同文件不重写，下载包只在内存中消费。`update --check`只读检查。更新后在空闲边界重载，HTTPS初始化也可沿原窗口完成这一步。

首次公网配置在目标设备的工程目录执行，例如将同号端口映射到该设备的26143：

```powershell
.\start.cmd --port 26143 setup-https --public-address 203.0.113.42
```

替换为实际公网IP。命令保留已有CPU/RAM、工具、共享目录、任务根和令牌；缺失令牌时生成独立节点令牌。Windows使用系统PowerShell/.NET生成证书，其他系统使用已有OpenSSL；不下载工具。证书覆盖公网IP与回环，存放在不含主机名的固定路径，私钥保留本机。已有证书与令牌重复使用；证书缺少目标IP或已过期时给出具体错误，保留原配置。

空闲worker沿原可见终端重载并验证HTTPS；有在途任务时只保存配置，空闲后重跑同一命令完成重载。尚未运行服务时，配置完成后用`start.cmd`或`sh start.sh`在可见终端启动。返回的`connection_profile`是忽略的私有连接文件，含节点令牌和公钥证书；通过已有私有文件通路交给操作员，令牌不粘贴到聊天或Git，TLS私钥不随此文件导出。中心按完整HTTPS地址/端口登记该节点的CA与令牌，同一公网IP上的其他端口保留各自凭据。

修改系统主机名不会改变IP端口、证书、令牌或任务根；自动重连按已有机器标识的硬件段核对，当前会话保留原路由名和任务引用，新主机名成为可用别名。中心重新加载节点配置时使用实际主机名并保留旧名别名。

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
