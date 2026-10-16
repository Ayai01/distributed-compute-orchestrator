# 调度、任务接口与工程接入

[返回首页](../README.md) · [启动与维护](operations.md)

## 算力分配与提前排队

默认部署中，科研工程运行在 Aya1-PC，向本机中心 `http://127.0.0.1:26133` 提交任务。Aya1-TX 的同一端口提供 worker 服务；在该设备运行科研工程时，auto 会检查服务角色，未检测到本地中心便独立计算，需要接入 Aya1-PC 时显式填写其中心地址。中心模式下，工程原有 workers / 并发上限不参与 CPU 准入；有限的提交窗口用于提前准备任务，实际 CPU 和内存由中心统一分配。

每个工程设置 `resource_share`（默认 1.0），中心按活跃工程的相对权重分配整数线程，用最大余数补齐余量。当前 54 线程、三个工程同权且需求充足时为 18/18/18；两个工程权重 2:1 时为 36/18。不能整除时，例如 53 线程、三个工程同权，为 18/18/17。需求不足的份额供其他待运行任务使用。

单线程 SPICE 任务声明 `cpus: 1`。可并行的整轮任务声明 `cpus: 0`，中心按工程份额和可用节点授予实际线程数，并把 `${cpus}` 传给程序。`cpus: 0` 由中心处理，直接 worker 任务需声明正整数。整轮任务在单个节点执行；晚加入的整轮任务会等待所需份额释放，中心预留对应节点的线程，其余可运行任务继续填补资源。任务保留原节点；支持协作借还的同步内核可在安全批次边界调整授予，其余在途计算保持原授予。

自动线程任务按每台候选节点的实际容量计算所需线程；较大节点忙碌时，可在空闲的小节点取得相应授予，不为等待较大节点留下整台机器空闲。

`queue_ratio`（默认 1.0）控制提前排队比例。`Client.admission()` 返回当前窗口及 `max_in_flight` 安全上界；`Client.window()` 保存上界并每秒至多刷新一次当前份额：

```text
运行槽位 = max(1, ceil(目标线程 / 单任务线程))
预取任务 = ceil(运行槽位 × queue_ratio)
提交窗口 = 运行槽位 + 预取任务
```

整轮 `cpus: 0` 的运行槽位为可承接节点数，并受工程目标线程数限制，至少为 1；两台节点都可承接时，`queue_ratio: 0` 也可同时提交两项整轮任务。`queue_ratio: 0` 不额外预取，1 准备下一波已就绪任务。CPU、内存、工具、共享路径和指定节点仍决定实际派发；提交窗口不是 CPU 授权，也不是服务端全局队列硬上限。工程只预取已就绪工作，保留搜索批次和代际边界。

配置位置：AOP 原运行配置、ARH-DSE、Bench 顶层使用这两个字段；AOP 规范化配置在 `execution` 中映射，RGCN 放在 `resources` 中。

```json
{"resource_share": 1.0, "queue_ratio": 1.0}
```

AOP 按角点、RGCN 按候选/PVT、Bench 按算法与种子预取。调用方按窗口上界创建线程池，按当前窗口限制 pending；长任务等待期间也定期补位，其他工程结束后可扩大在途窗口。窗口缩小时已接受任务继续完成。RGCN 按输入顺序返回结果。ARH-DSE 整轮进入中心，轮内保留原生请求队列与构建复用机制。

Bench 默认每种子声明 1 CPU，由动态窗口决定并发，不受工程 workers 限制。同步Class-AB物理批次和方向输出内核在计算前借线程、finally归还至1；任务尾部可继续借用其他任务释放的线程。`remote_compute.max_task_cpus`可设单任务借用上界；异步求值池在启动前申请固定额度，pending收齐后归还。`remote_compute.task_cpus`指定初始额度，0表示中心初始自动授予；关闭顶层`adaptive_threads`后保持固定额度。取消批次会停止后续提交、取消并收齐已接受任务。

节点 CPU 预算、硬上限和内存配置见[算力控制](operations.md#设备预算与配置文件)。看板同时显示各工程的权重、目标份额、已分配线程和排队数。焦点刷新只影响终端展示，后台调度和资源探测持续工作。

调度思路参考 [Dask 排队与负载选择](https://distributed.dask.org/en/stable/scheduling-policies.html)、[Ray 有限待处理任务](https://docs.ray.io/en/latest/ray-core/patterns/limit-pending-tasks.html)、[Slurm 资源预留与回填](https://slurm.schedmd.com/sched_config.html)。实现使用 Python 标准库。

## 分配策略

- `adaptive`：先满足线程、内存、工具与工程份额，优先选择预计 CPU 负载较低的设备。候选负载相差不足一个逻辑线程时，再参考同类任务耗时的指数移动平均、节点权重和未采样节点。
- `balanced`：使用相同的 CPU 负载优先规则，接近时按线程占比与节点权重选择。
- `round_robin`：在满足资源条件的节点之间轮询，便于固定分配实验。

负载按设备的逻辑线程数归一化，并把尚未反映在采样中的派发和线程借还立即计入：

```text
背景占用线程 = max(0, 采样CPU比例 × 逻辑线程数 - worker快照已分配线程)
预计负载 = (背景占用线程 + 当前预留线程 + 外部任务线程 + 新任务线程) / 逻辑线程数
```

同设备的多个 worker 合并计算主机占用，避免重复计入 CPU。整轮 `cpus: 0` 比较节点时按一个启动线程计价，选定后仍按可用线程和工程份额授予。采样中的在途任务已包含在 CPU 使用率内，用当前账本替换其旧授予；连续提交不用等待下一次采样才更新负载。CPU 使用率参与节点选择，线程和内存预算决定准入；其余设备已满且该设备仍有可用额度时，可继续派发。指定 `node` 保持约束，已运行任务保持原节点。

worker 默认将 `OMP_NUM_THREADS`、`OPENBLAS_NUM_THREADS`、`MKL_NUM_THREADS`、`NUMEXPR_NUM_THREADS` 设为此次线程授予；任务 `env` 显式指定的值优先。例如外层并发处理多个候选时，可将这些数值库设为 `1`，避免嵌套并行；编译仍使用 `${cpus}`。外层任务与内核线程共同使用此次授予。

任务还可设置 `priority`、`node`、`requires`。较大的 `priority` 先启动；`node` 固定节点名称；`requires` 要求节点标签全部匹配。节点自动报告 Python、ngspice、Verilator 等可用工具，调度器据任务使用的工具筛选节点。多个批次共享同一调度器的资源账本，节点再按自身预算统一准入。

中心与直连 Pool 按任务 ID 区分本方预留和外部任务；慢健康快照中的旧授予不再占用第二份额度，内存同时扣除本方任务的预留或实际 RSS。任务取消只向 worker 发送一次请求，随后按正常间隔等待终态；空队列由新任务与资源事件唤醒。

用 `label` 区分电路、保真度或相近工作量的批次，使同类耗时具有可比性。执行中的任务保留原节点；中断任务的状态和已有产物留在节点，便于查询和回收。
中心的工具变体包含节点名和可执行文件摘要，调用工程可核对固定节点的引擎身份。

## 现有工程接入

四个工程默认优先本机 26133 中心。中心缺席时 auto 模式回到独立执行，沿用 `min(C,16)+floor(max(C-16,0)/2)`；RGCN 的显式中心 URL、ARH/Bench 的显式 scheduler 要求中心可用；AOP 的 scheduler 模式表示优先本地中心，显式 compute-center URL 才要求指定中心可用。显式 local 固定独立计算。worker 子任务设置 `REMOTE_COMPUTE_WORKER=1`，避免递归提交。

| 工程 | 选择中心 | 执行粒度与节点要求 |
| --- | --- | --- |
| AOP_v2 | `--compute-center URL` 或 `compute.center`；网页选本地调度器 / 本地工程 | 每角点任务；源码、模型按内容同步，沿用原测量与解析。 |
| RGCN-v2 | `config.py` 的 `COMPUTE_URL` 或 `circuit.pre.remote_compute.url` | 每候选 PRE/POST 分析；源码和模型闭包同步，保留后仿恢复与原任务 ID。 |
| ARH-DSE-v2 | `--compute-center URL` / `--compute-mode auto` | 整轮任务；执行节点需要 `shared.arh` 指向 ARH 工程及其原生计算环境。当前此节点为 Aya1-PC；Aya1-TX 独立 RTL 能力通过 Verilator 任务提供。 |
| Analog-Opt-Bench-v2 | `--remote-url URL` 或 `remote_compute.url` | 算法/种子任务；源码同步，需要 bench_python 科学环境。 |

仿真器、评分、实验预算、设计顺序、研究缓存和报告仍由调用工程管理。线程配置影响资源分配；中心不修改科研评价逻辑。验证任务使用独立临时产物目录，ARH 的 JobManager 可传 `artifact_root` 隔离结果、历史与报告；本次验证使用该隔离方式，正式结果与已有报告保持原样。日常正式运行仍按各工程规则更新当前成功报告。

## 脚本与 Python 接口

```text
python main.py --mode hybrid --cpus 2 batch examples/batch.json results/demo
python main.py --mode scheduler --nodes nodes.local.json run examples/spice/task.json --input examples/spice --output results/divider
python main.py --mode scheduler --url http://192.168.1.10:26133 jobs
python main.py --mode scheduler --url http://192.168.1.10:26133 fetch 任务ID --output results/recovered
python main.py --check
python main.py --test
python main.py --test-native
```

全局 `--cpus` / `--memory-mb` 是本机提供者预算；任务命令后面的同名选项是该任务的资源预留。单任务命令成功返回 0，执行失败返回 1，配置或通信错误返回 2。

```python
from remote_compute import Client, ngspice

result = Client().run(ngspice("divider.spice"), inputs="examples/spice")
print(result["status"], result["assigned_node"])
print(result["files"]["ngspice.log"].decode(errors="replace"))
```

指定 `output_dir` 时产物保存到该目录；省略时以 bytes 返回在 `result["files"]` 中。高级脚本可使用 `Pool` 直接连接独立 worker（例如 Aya1-TX 的 `192.168.1.10:26133`）；hybrid 本机 worker 仅供所属中心进程使用。接入统一调度使用 `Client(中心地址)`，让中心管理多工程份额。

其他工程可把此工程目录加入 Python 的模块搜索路径，再导入 `remote_compute`。工程本身没有 pip 安装步骤。

### 向中心提交与查询窗口

```python
from remote_compute import Client, command

center = Client()
window = center.window("my-project", resource_share=1.0, queue_ratio=1.0, cpus=1)
print(window.maximum, window.current())
task = command(["${tool:python}", "runner.py", "--workers", "${cpus}"],
               cpus=0, memory_mb=1024, project="my-project", resource_share=1.0,
               outputs=["measurements.json"])
result = center.run(task, inputs="my-batch")
print(result["status"], result["assigned_node"], result["cpus"])
```

同一工程的任务保持相同 project 和 resource_share；多个调用者各自的有限窗口由调用者维护，中心统一控制执行资源。

### 同步内核在批次边界借还线程

worker 任务环境提供 `REMOTE_COMPUTE_URL`、`REMOTE_COMPUTE_JOB_ID`、`REMOTE_COMPUTE_TOKEN`、`REMOTE_COMPUTE_HOME`，支持时 `REMOTE_COMPUTE_RESIZE=1`。SDK 路径指向执行设备上的本工具，回调指向所属调度中心；独立 worker 任务回调自身。中心默认监听 LAN 的26133，hybrid 的本机 worker 仍为进程内私有调用。指定中心仅监听回环时，远端任务保持静态授予。

```python
import os
from remote_compute import Client

center = Client(os.environ['REMOTE_COMPUTE_URL'], os.environ['REMOTE_COMPUTE_TOKEN'])
identity = os.environ['REMOTE_COMPUTE_JOB_ID']
grant = center.resize(identity, desired_threads)
try:
    compute_batch(threads=grant['cpus'])
finally:
    center.resize(identity, 1)
```

`resize` 保留原任务ID、节点和输入摘要，返回 `id/cpus/requested_cpus/node`。增长受工程权重、节点预算和当前空闲线程约束；调用方始终使用实际 `cpus`。同步批次结束后才归还，异步 pending 期间维持固定授予。BLAS 内层保持1，避免与外层并行相乘。worker 与中心同步计账，取消、异常和完成均按最后授予释放；回执保留 `initial_cpus/peak_cpus/resize_count`。旧节点未声明 resize 能力时继续原授予。

## 任务文件与结果

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

## 传输与本地执行

- hybrid 本机 worker 进程内调用，不经过第二层 HTTP；本机结果直接附加中心记录后打包一次。外部调用者仍统一访问中心 26133。
- 普通固定线程任务保持原始输入包直接转发；整轮自动线程任务仅在派发时写入实际授予的线程数。输入身份与重复提交语义保持。
- 模型和源码按内容标识同步；同一节点会话内每份资产只检查/上传一次，并发任务共用同步锁。worker 重启后重新确认资产。
- HTTP 使用持久连接。状态轮询只返回进度字段，完成后取完整资源与来源记录；支持 gzip 的客户端接收压缩 JSON，旧客户端继续接收普通 JSON。
- 文本包采用低开销压缩；已经压缩的 gz/npz/zip/7z 直接封装。仿真器构建和中间文件留在执行端，按任务 outputs 回收必要结果。

| 工程 | 默认回传内容 |
| --- | --- |
| AOP | 执行端测量与解析后的 aop-result.json；按配置选择日志。 |
| RGCN | measurement.json：指标、分析证据、波形统计及数值摘要，不传整套原始波形。 |
| Bench | result.pkl.gz：评分、前沿、轨迹等报告需要的科学数组。 |
| ARH-DSE | summary/report 与恢复、验证所需产物；保留报告校验，不回传整个编译环境。 |

通用 SPICE/RTL 命令的 outputs 由任务声明；需要原始波形时显式保留对应输出模式。调用工程的测量定义和正式产物契约保持。

本轮传输回归：8 个并发任务引用同一资产，只产生 1 次检查和 1 次上传；示例任务的完整状态为 1340 字节、精简进度为 187 字节，示例 worker 健康信息由 1693 字节压缩至 1043 字节。看板在 3 秒微测中获取状态 3 次，空闲循环复用已缓存画面。

## 验证入口

```text
python main.py --check
python main.py --test
python main.py --test-native
```

核心回归覆盖份额、预取窗口、整轮线程授予、CPU/内存准入、连接复用、取消、文件操作、重启与结果回收。原生检查需要节点的 ngspice 和 Verilator。

2026-10-17 的隔离实测：三工程持续提交 300 秒，AOP 完成 1188 个角点任务、RGCN 完成 638 个候选、ARH 完成 13 轮，均无错误；当时预算为 27+31，暖机后平均分配 51.8/58 线程，平均 CPU 为本机 95.4%、Aya1-TX 86.4%。随后按新预算 24+30 复查，达到 54/54 分配，AOP 125 个角点、RGCN 108 个候选、ARH 2 轮均无错误。两次检查保护的 16,252 个原结果/报告文件保持不变。Aya1-TX 的 RTL 编译与 4096 周期断言通过；Bench 的四个 ZDT1/Class-AB 配对任务、每任务 128 次评价，关键科学数组逐项一致。实测临时文件在完成后清理。

随后使用正式 Class-AB 模型核对两台实际设备：ZDT1 全字段逐位一致；Class-AB 的制造设计、前沿索引、128 次预算、可行性与完整性掩码一致。Aya1-TX 的 float64 目标、约束、物理指标最大绝对差分别为 2.29e-10、3.29e-8、5.92e-7，原始浮点值保留。24+30 预算下的另一次 90 秒提交检查完成 AOP 256 个角点、RGCN 216 个候选、ARH 6 轮，峰值 54/54；18,764 个原结果/报告文件未变。CLI 实时帧在两端各检查 90 帧，覆盖 44–140 列和 24–48 行，92 字符原始标签通过缩写与明细完整保留。

工具参数：[ngspice 文档](https://ngspice.sourceforge.io/docs.html)、[Verilator 参数](https://verilator.org/guide/latest/exe_verilator.html)。
