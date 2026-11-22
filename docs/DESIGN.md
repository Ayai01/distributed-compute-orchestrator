# DCO 设计合同

## 部署

中心管理原任务 ID、准入、调度和结果索引。Worker 执行、采样和结算资源。看板与网页使用运维接口，读取同一份状态。无图形界面的服务用 `--no-dashboard` 输出纯文本。

## 连接与身份

健康检查失败时，优先核对原实例和传输链路，从现有 worker 恢复接单。短暂失联期间保留实例、任务和费用账本。平台实例退役以学校侧的实际状态和运维确认为准。

可达设备使用直连。需要转发的设备经认证中转接入，中心沿稳定身份维护路由。节点接口地址与中转入口仅向运维人员显示。断线重连先按原 ID 查已受理状态，保留已付工作和已接收结果。

## 能力与资源准入

Python 是所有 worker 的基础环境。普通 SDK 省略 `requires` 即使用基础环境。需要专项能力时，用 `requires=["<包名>"]` 选择一个固定能力包。CPU 线程按资源数量申请。节点由中心分配，固定宽度整组获配，弹性任务按就绪宽度借用或归还资源。

任务的 `priority` 是正整数核心配额权重，默认 1。中心按工程份额分配可用核心，同一工程的弹性任务在安全调整边界按 `priority` 的比例规划核心。固定宽度任务按整组资源准入，空闲份额可借用。任务列表显示实际生效的权重。旧请求中的 0 和负值按 1 调度，冻结请求保留原值。

中心拒绝会缩窄公开能力池的普通原生制品或检查点引用。普通工程提交源码和冻结配方，所选 worker 负责构建并复用兼容的本机缓存。已受理原单保留原 ID 和请求摘要。

| 能力包 | 固定环境 |
| --- | --- |
| `ng47`、`hsp` | 基础环境及对应的 ngspice 47 或 HSPICE |
| `onnx` | 基础环境及 ONNX Runtime，含 INT8 整数卷积 |
| `rtl` | Chisel、Verilator、C++ 构建和 ONNX。面板显示 Chisel。 |
| `layout`、`timing` | 基础环境及 Magic/Netgen 版图，或 Yosys/OpenSTA/Icarus 时序 |
| `cuda`、`npu` | 对应的加速后端。CUDA 含 RTL 环境，NPU 含基础环境。 |

节点需通过包内全部探测才提供该包。`chisel`、旧的阶段交接和编译缓存提示归并到同一个 `rtl` 包。DCO 使用所选 worker 注册的 firtool，编译缓存按本机工具状态自动使用。普通工具引用与共享绑定具有相同的派发范围。ABI、任意工具标签、原生缓存定向和加速器子标签属于运维。任务可提交跨运行时输入，在 worker 按实际环境使用已验收的制品。

CPU PyTorch 省略 `requires` 和 `accelerator`，使用 `${tool:python}`，张量显式置于 `cpu`。旧 SDK 的 `python` 与 `ml` 请求仍映射到基础环境。聚合容量覆盖各平台注册的运行时。需要版本证据时，在任务输出记录实际 CPython、`SOABI`、PyTorch 和设备，费用可按 `step_phases=["load_check"]` 记录。同工程 `artifact_ref` 约束系统、架构和 Linux glibc。CPython 与 PyTorch 版本由任务内核验。[生产最小回执](../.local/migration/cpu-pytorch-qualification-20261121.json)

整任务 `memory_mb` 按标量报价，同一阶段可用实测值校准。只有声明了按 CPU 增长的内存模型才按宽度拟合。排队和资产交接发生在执行计时之前。Worker 启动准备拥有独立时限，进程启动后开始执行计时。精确宽度的资源握手也使用独立准备时限。

托管 Python 的精确阶段使用 `run_call(body, limits={"exact_cpus": n, "start_timeout": seconds})`，其中 `1 <= n <= max_cpus`，默认启动窗口 300 秒。任务 `timeout` 控制执行窗口。整单只有一个串行计算体时，用 `python_task("receiver:entry", max_cpus=1, ...)` 直接调用该计算体，沿用首档已分配的一核和整任务内存声明。原请求和费用按原 ID 保留。输入或任务入口变更须作为另一个原件登记。[生产版本隔离验收](../.local/migration/serial-start-qualification-20261121.json)

同阶段归还空闲 CPU 时，内存工作集可继续使用原额度。SDK 收到 worker 确认后才将资源归还视为完成。面板的线程预留表示授权额度，容器 CPU 使用率以其有效配额为分母。

`admission(project, activity=True)` 返回聚合 `cpu`、各可调度能力的 `capability_cpus`、提交窗口和所请求工程的在途任务。`dispatch_paused` 报告是否停派，`cpu.available` 是兼容池中未预留的逻辑核。工程用 `statuses(ids)` 短请求批量查询，按原 ID 领取结果。任务目录、ID 前缀搜索、节点名、设备 IP、中转地址和部署分组归运维端。

中心在短临界区读取任务和资源快照，在锁外排序并计算扩核配额。落位和扩核前核对任务状态、实时资源与暂停状态。资产传输只锁定摘要登记，以完成事件唤醒等待同一资产的任务。

## 传输与生命周期

普通客户端通过 `status(id)` 或 `statuses(ids)` 读取原单阶段。`dispatch_stage` 从组包、资产准备、派发间隔进入等待 worker 确认。worker 接单后，`preparation_stage` 给出当前准备步骤，`worker_sampled_at` 标记本次 worker 观察时间；`execution_started` 标记进程执行窗口起点。精简终态轮询 `compact=True` 仍只取状态。进度列使用任务报告的计数与分母或明确的百分比，命令步骤单列在 `step_progress`。

`asset_preparation_seconds` 计量中心保护并同步输入的时间，`handoff_seconds` 计量派发到中心确认的墙钟时间。`handoff_transport` 累计同一交接的连接槽等待、建连和发送、等待 HTTP 响应头。`worker_admission_seconds` 从 worker 收齐请求体计到原单登记，包含解包，并与中心等待回包重叠。研究费用沿用原单的实测 CPU 秒；需要比较仿真核时时按原单报告的计算阶段统计，同时保留编译费用与缓存命中记录。

`python_map` 的 `backend="process"` 设定任务内并行方式。需要复现数值环境时，在结果中记录实际导入的包版本。

任务和准入查询可同时声明 `executable_ram_mb=256`。中心选择已实测可从 RAM 执行且具备所需空闲 MiB 的 worker，开算前由 worker 再核对空间。`REMOTE_COMPUTE_JOB_TEMP` 和 `TMPDIR` 指向该任务的暂存目录。`memory_mb` 独立申报进程内存。省略字段的任务使用普通暂存和原有能力池。

Aya1-PC 的共享 RAM 盘配置为 4 GiB，其余 Windows worker 配置为 8 GiB。Linux 优先使用 `/run/user/<uid>` 的可执行 tmpfs。学校容器缺少该挂载时使用经核验的 `/dev/shm` 承载可执行 RAM，普通暂存沿用各 worker 配置。

内容摘要标识输入，先用 `sync_files` 发布一次，后续任务只引用摘要。共享缓存对同一内容合并传输，并在使用期间保留引用。常规作业通过 `outputs` 只声明需回传的 KiB 级摘要。大型检查点用缓存引用保留在算力端，再按需领取。

结果领取、资产上传和退休确认使用独立连接池。中转的上传和下载字节预算可配置，学校链路由实际链路容量决定速率。短派发可使用空闲连接，状态与授权查询保有控制槽。取消回调、网络探针等待和传输统计采样在调度临界区外执行。结果按原 ID 校验并持久化。退休请求原子登记后返回，后台回收运输副本。重复或缺失的原 ID 幂等成功。

中心在 `asset-variants.json` 登记同一逻辑输入的不同系统与架构制品。完整对应组以独立环境变量挂载时，中心选定 worker 后只发送匹配的制品。原请求保留全部资产引用，普通准入只依据能力和资源。大资产正文已发送而回执丢失时，先复核发布状态，再决定是否重发正文。

学校共享资产卷按原 ZIP 字节确认偏移。64 KiB 起启用续传，双方支持时仅对能明显缩小的分块使用 XZ。接收端切换后先查询同卷断点，完整 ZIP 通过内容摘要和清单检查后原子发布。回执的 `wire_body_bytes` 计入重试的上传分块请求体，热命中为 0。

工程已有确定性 ZIP 时可调用 `Client.stage_archive(payload, job_id=...)` 取得摘要、包长和命中状态。工程可用 `Client(collection_workers=...)` 指定本机领取线程数，默认 2；准备和提交使用独立线程池。

资产缓存每十分钟维护。交接、排队和执行期间保护资产引用。终态任务在有效期内保护输入，超过七天未命中的输入可回收。超出 64 GiB 时，按最后命中时间回收可清理条目。解包缓存后台校准实际大小，包含任务写入的构建文件。中心和进程内 worker 共用本机额度，独立 worker 各自管理解包缓存，同一挂载的学校 worker 共用一个共享 ZIP 缓存。原任务、结果和费用账本独立保存。

调用方只提交可复现的输入和生成器，临时中间件留在 worker 作业目录。SDK 缓存未命中时重新生成并发布输入，worker 优先读取共享副本，否则从中心恢复。SDK 的内容索引只记摘要，最多 128 条。依赖安装和项目自己的长期缓存由其所有者管理。

## 制品、检查点和短任务

中心公布 `artifact_cas_v1`、`warm_affinity_v1` 和 `staged_cpu_accounting_v1`。可分配线程继续从 `admission(project, activity=True)` 的聚合容量读取。新增字段需要 1.0.34 或更新的中心与兼容 worker，省略时沿用现有请求。

- 冷构建用 `produces_artifact_ref={"recipe_sha256": digest, "paths": ["build.zip"]}` 声明精确的输出文件。配方摘要应覆盖输入源码、工具链、编译选项和 ABI。中心把声明文件按内容入库，并在构建完成回执中返回 `artifact_ref`。SDK 对完全相同的构建请求复用原 ID。显式传入的 `job_id` 仍为原 ID。
- 消费用 `artifact_ref` 和 `checkpoint_ref` 引用同工程的制品。挂载变量分别为 `${asset:dco_artifact}`、`${asset:dco_checkpoint}`。中心按引用登记的 ABI 硬性校验平台、规范架构和 Linux glibc 版本，其他动态依赖应随制品带齐或由能力包提供。热亲和在满足能力、ABI 和资源条件后生效，兼容冷端可接单并拉取。`affinity_keys` 省略时自动使用本任务的引用。普通回执以 `cache_hit.artifact` 记录制品命中，`cache_hit.worker_id` 等于原任务 ID。
- `step_phases=["compile","load_check","projection","sim"]` 按步骤声明 CPU 阶段，与 `steps` 一一对应。回执使用 `cpu_breakdown_s`，各阶段之和等于本次执行的进程树 CPU。重试保留在 `attempts`，原任务总费用沿用 `total_cpu_seconds`。固定单核、无加速器的短任务可传 `short_job=True`，中心和 worker 使用 100 毫秒启动间隔。若涉及 CPU 超发仍走一秒间隔与现有内存保护。
- `timeout_grace_s` 预留原执行预算的最后一段。程序在 `checkpoint_requested()` 成立后保存声明的文件，调用 `report_checkpoint(paths)` 提交摘要，并用 `report_progress` 报告当前进度。任务声明 `checkpoint_output={"recipe_sha256": digest, "paths": ["state.json"]}` 和对应的 `outputs`。中心验证摘要后在终态返回 `partial_checkpoint_ref`，缺少已提交的完整快照时字段为 `null`。
- 在退休运输副本前可用 `Client.publish_reference(id, project=..., paths=..., recipe_sha256=...)` 补登记。引用保留原 ID 和内容绑定，缓存文件服从七天和 64 GiB 覆盖策略。调用方应持久保存复现实验所需的原始产物与配方。

```python
producer = command(build_argv, project="demo", requires=["rtl"], max_cpus=1,
                   outputs=["build.zip"], step_phases=["compile"],
                   produces_artifact_ref={"recipe_sha256": digest, "paths": ["build.zip"]})
receipt = client.wait(client.submit(producer, files=inputs)["id"])
ref = receipt["artifact_ref"]
consumer = command(sim_argv, project="demo", requires=["rtl"], max_cpus=1,
                   artifact_ref=ref, affinity_keys=[ref], step_phases=["sim"],
                   env={"BUILD": "${asset:dco_artifact}/build.zip"})
consumer_id = client.submit(consumer, files=sim_inputs)["id"]
```

已有制品可用 `probe(client, "demo", ref, "build.zip")` 走一次公开加载验证。示例在任务进程内读取实际 ABI，并保存原 ID、制品命中、四阶段和总 CPU。正式比较把冻结的加载、投影和仿真命令按 `step_phases` 逐步声明。[最小 SDK 示例](../examples/artifact_reuse_probe.py)

**原生构建与热缓存。** 新的正式任务申报能力、资源和冻结构建配方。DCO 先派发，worker 再按本机 ABI、已注册工具和配方摘要复用缓存或构建。热缓存可用于兼容节点之间的排序，冷缓存照常接单。已受理的任务继续按其原始制品和原 ID 结算。候选版本将在受理环节拦截缩窄能力池的原生制品引用。[当前推进状态](STATUS.md)
