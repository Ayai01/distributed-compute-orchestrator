# DCO 设计合同

## 部署

中心管理原任务 ID、准入、调度和结果索引。Worker 执行、采样和结算资源。看板与网页使用运维接口，读取同一份状态。无图形界面的服务用 `--no-dashboard` 输出纯文本。

## 连接与身份

可达设备使用直连。需要转发的设备经认证中转接入，中心沿稳定身份维护路由。节点接口地址与中转入口仅向运维人员显示。断线重连先按原 ID 查已受理状态，保留已付工作和已接收结果。

## 能力与资源准入

普通 SDK 用 `requires=["<包名>"]` 选一个固定能力包，省略时为 `python`。CPU 线程按资源数量申请。节点由中心分配，固定宽度整组获配，弹性任务按就绪宽度借用或归还资源。

| 能力包 | 固定环境 |
| --- | --- |
| `python` | 固定的 Python、PyTorch 基础环境 |
| `ng47`、`hsp` | 基础环境及对应的 ngspice 47 或 HSPICE |
| `onnx` | 基础环境及 ONNX Runtime，含 INT8 整数卷积 |
| `rtl` | Chisel、Verilator、C++ 构建和 ONNX。面板显示 Chisel。 |
| `layout`、`timing` | 基础环境及 Magic/Netgen 版图，或 Yosys/OpenSTA/Icarus 时序 |
| `cuda`、`npu` | 对应的加速后端。CUDA 含 RTL 环境，NPU 含基础环境。 |

节点需通过包内全部探测才提供该包。`chisel`、旧的阶段交接和编译缓存提示归并到同一个 `rtl` 包。DCO 使用所选 worker 注册的 firtool，编译缓存按本机工具状态自动使用。普通工具引用与共享绑定具有相同的派发范围。ABI、任意工具标签、原生缓存定向和加速器子标签属于运维。任务可提交跨运行时输入，在 worker 按实际环境使用已验收的制品。

worker 在启动进程前写好该任务的工具表。`REMOTE_COMPUTE_TOOLS_FILE` 指向任务临时目录内的 JSON 对象，内容来自原回执的 `tools` 字段。环境路径就是就绪信号，预检步骤和后续进程可直接读取同一文件。

整任务 `memory_mb` 按标量报价，同一阶段可用实测值校准。只有声明了按 CPU 增长的内存模型才按宽度拟合。排队和资产交接发生在执行计时之前。Worker 启动准备拥有独立时限，进程启动后开始执行计时。精确宽度的资源握手也使用独立准备时限。

普通 `command` 和 `python_task` 可声明 `min_start_cpus`，取值为 `1..max_cpus` 内的整数。大于一的下限由兼容 worker 在首次启动前足额持有，排队等待满足同台资源。首档核数由回执 `initial_cpus` 和进程环境 `REMOTE_COMPUTE_CPUS` 提供。后续阶段仍可协商并归还 CPU。启动下限大于一和工具表需要 1.0.36；省略下限的任务兼容旧 worker。

同阶段归还空闲 CPU 时，内存工作集可继续使用原额度。SDK 收到 worker 确认后才将资源归还视为完成。面板的线程预留表示授权额度，容器 CPU 使用率以其有效配额为分母。

同一租约请求的回信可原样重发。开算前发现授权已变更时，SDK 以零活跃线程重新申请，确认后再执行。已运行的工作继续按其真实活跃线程保护。

`execution_timing` 以 worker 单调时钟记录接收到进程启动和到首次原生派发的时间。托管 Python 阶段另记原生执行、空等资源、其余执行墙钟。并行原生区间合并，与原生执行重叠的等待计入执行区间。`cpu_seconds` 继续计量进程树实际 CPU。首次派发或执行区间缺少观测时对应值为 `null`。

`admission(project, activity=True)` 返回聚合 `cpu`、各可调度能力的 `capability_cpus`、提交窗口和所请求工程的在途任务。容量与当前可分配量分别记录，当前池未提供的能力配额为零。工程用 `statuses(ids)` 短请求批量查询，按原 ID 领取结果。任务目录、ID 前缀搜索、节点名、设备 IP、中转地址和部署分组归运维端。

`activity.active_cpus` 对当前运行原单的最近上报取样。`lower_bound` 是已观测活跃宽度之和，`coverage` 是 `observed_jobs / running_jobs`。零宽度是有效观测，缺报的任务仍返回 `null`。无运行任务时宽度为零且覆盖率为 1。

## 传输与生命周期

内容摘要标识输入，先用 `sync_files` 发布一次，后续任务只引用摘要。共享缓存对同一内容合并传输，并在使用期间保留引用。常规作业通过 `outputs` 只声明需回传的 KiB 级摘要。大型检查点用缓存引用保留在算力端，再按需领取。

结果领取、资产上传和退休确认使用独立连接池。中转可分别配置上传和下载预算，短任务派发和状态查询拥有各自连接。结果按原 ID 校验并持久化。退休请求原子登记后返回，后台回收运输副本。重复或缺失的原 ID 幂等成功。

中心在 `asset-variants.json` 登记同一逻辑输入的不同系统与架构制品。完整对应组以独立环境变量挂载时，中心选定 worker 后只发送匹配的制品。原请求保留全部资产引用，普通准入只依据能力和资源。大资产正文已发送而回执丢失时，先复核发布状态，再决定是否重发正文。

共享资产卷可由支持续传的 worker 接收 8 MiB 起的 ZIP。每个分块完整写入和持久化后确认偏移。接收端传输中断时优先切换到可用的同卷接收端，先查询同一摘要的持久偏移，再传未确认的尾部。共享盘状态短暂滞后时继续查询已知确认位置。完整字节通过 SHA256 和内容清单检查后原子发布，原计算 worker 继续通过既有接口消费。

资产缓存每十分钟维护。交接、排队和执行期间保护资产引用。终态任务在有效期内保护输入，超过七天未命中的输入可回收。超出 64 GiB 时，按最后命中时间回收可清理条目。解包缓存后台校准实际大小，包含任务写入的构建文件。中心和进程内 worker 共用本机额度，独立 worker 各自管理解包缓存，同一挂载的学校 worker 共用一个共享 ZIP 缓存。原任务、结果和费用账本独立保存。

调用方只提交可复现的输入和生成器，临时中间件留在 worker 作业目录。SDK 缓存未命中时重新生成并发布输入，worker 优先读取共享副本，否则从中心恢复。SDK 的内容索引只记摘要，最多 128 条。依赖安装和项目自己的长期缓存由其所有者管理。

## 制品、检查点和短任务

中心公布 `artifact_cas_v1`、`warm_affinity_v1` 和 `staged_cpu_accounting_v1`。可分配线程继续从 `admission(project, activity=True)` 的聚合容量读取。新增字段需要 1.0.34 或更新的中心与兼容 worker，省略时沿用现有请求。

- 冷构建用 `produces_artifact_ref={"recipe_sha256": digest, "paths": ["build.zip"]}` 声明精确的输出文件。配方摘要应覆盖输入源码、工具链、编译选项和 ABI。中心把声明文件按内容入库，并在构建完成回执中返回 `artifact_ref`。SDK 对完全相同的构建请求复用原 ID。显式传入的 `job_id` 仍为原 ID。
- 消费用 `artifact_ref` 和 `checkpoint_ref` 引用同工程的制品。挂载变量分别为 `${asset:dco_artifact}`、`${asset:dco_checkpoint}`。中心验证制品所属工程和 worker ABI，在已满足能力与资源条件的节点中优先使用热缓存，满载时可到兼容节点拉取。Linux 制品要求平台、架构和 glibc 一致，其他动态依赖应随制品带齐或由公开能力包提供。普通回执的 `cache_hit.worker_id` 是随原任务变化的标识。
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
