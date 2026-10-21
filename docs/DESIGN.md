# DCO 设计与接入契约

[运行与CLI](../README.md) · [当前状态](STATUS.md) · [协作约定](../AGENTS.md)

## 责任与代码入口

DCO 管调度、授权、任务与传输；调用工程管测量、评分、唯一设计预算、模型和科研缓存。任务中声明能力和资源，中心选择执行设备；运维显式选设备并使用操作员令牌。

| 模块 | 责任 |
| --- | --- |
| `cli.py` / `startup.py` / `terminal.py` | 参数、可见终端检查、服务托管与重载 |
| `commands.py` / `console.py` / `view.py` | 命令语义、键盘交互、看板格式 |
| `coordinator.py` / `allocation.py` / `admission.py` | 中心队列、份额/资源授予、有限提交窗口 |
| `worker.py` / `process.py` / `resources.py` | 执行端任务、进程树所有权、CPU/RAM资源 |
| `client.py` / `tasks.py` / `machine.py` | SDK、任务构造、操作员机器接口 |
| `bundle.py` / `cleanup.py` / `cache_*.py` | 内容资产、生命周期回收、手动科研缓存同步 |

入口顺序为可见终端检查 → 加载设备配置 → 托管服务；一次性命令和测试走对应短路径。中心和 worker 的本地配置、运行状态各自持有，源码更新不覆盖它们。

## 工程接口边界

工程客户端只表达通用算力需求：`requires` 能力标签、`cpus/max_cpus`、`memory_mb`、`resource_share`，以及有界的 `priority`、`queue_ratio` 和 `single_thread_weight`。`resource_share` 为 `(0,16]`，`queue_ratio` 为 `[0,4]`，`single_thread_weight` 为 `(0,8]`，`priority` 为 `-1000..1000`。`node`、IP、节点权重、节点预算和完整 `/health` 只属于 DCO 操作端；操作端令牌保存在中心状态目录，普通工程令牌不能提升权限。

CLI 的本机操作员令牌只适用于 `127.0.0.0/8`、`::1` 和字面主机名 `localhost`；其他主机名即使解析到本机也按远端处理。该边界同时约束设备配置的 `operator_token` 和 `--root`/默认状态目录自动读取的令牌。远端使用显式 `REMOTE_COMPUTE_OPERATOR_TOKEN` 或 SDK 参数 `operator_token`，令牌归属该目标中心；本机启动新服务继续使用其配置令牌。

工程查看能力时使用：

```python
from remote_compute import Client
center = Client()
print(center.capabilities())  # tags/tools/CPU/RAM 的汇总视图
```

能力摘要只返回汇总标签、可用工具名称、CPU/RAM 总量与单位、以及去掉设备 ID 的加速器档案，不含设备名、URL、路径、活动任务或工程配额。大输入先用 `sync_files(..., content_id=...)` 内容寻址同步，任务只引用资产 ID；不要把模型、源码或完整健康快照随每个任务重复上传。

GPU 和其他加速器使用同一套汇总能力协议。`capabilities()['accelerators']` 返回去掉设备 ID 后的能力档案，例如：

```json
{"kind":"cuda", "count":2, "memory_mb":24576, "tags":["gpu","gpu:training","gpu:general"]}
```

任务声明语义需求，不指定卡号或设备名：

```python
from remote_compute import command

task = command(["${tool:python}", "train.py"], cpus=4, memory_mb=8192,
               project="rgcn", accelerator={"kind":"cuda", "memory_mb":8192,
                                             "requires":["gpu:training"]})
```

调度器按配置档案中的类型、每卡容量与标签匹配，并以整卡单元独占预留；`memory_mb` 表示每张卡须满足的最低容量，计账按占用卡数，实际显存用量由执行后端管理。卡号来自设备配置的 `ids`，同一物理卡只登记一次。运行环境提供 `DCO_ACCELERATOR_KIND`、`DCO_ACCELERATOR_COUNT`、`DCO_ACCELERATOR_MEMORY_MB`、`DCO_ACCELERATOR_IDS`；CUDA 任务同时得到 `CUDA_VISIBLE_DEVICES`。worker 每次执行都从本次授权重写这些变量：无授权时分别为空、0、0、空，并将 `CUDA_VISIBLE_DEVICES` 置空，避免继承调用者或设备环境中的GPU权限。工程依据本任务授权选择后端。训练、GPU 仿真和通用 GPU 任务可用不同标签；无GPU设备配置空档案，CPU任务照常使用。

兼容CPU执行的任务先检查中心是否声明 `accelerator_fallback`，再在同一份 admission/任务 `accelerator` 对象中加入 `fallback_after_seconds`（有限非负秒数）。该字段由调用方显式提供；省略时保持强制加速器。无匹配在线GPU时立即改为CPU；仍未派发的任务到达队列阈值时改为CPU，同一任务ID、请求包和输入摘要保持不变。首次派发后执行选择冻结，重试和恢复沿用该选择。CPU回退回执含 `accelerator: null`、原需求 `requested_accelerator`，以及 `accelerator_fallback: {"reason":"unavailable"或"queue_timeout","at":Unix秒}`。admission 的 `fallback_capable_cpus` 表示CPU路径匹配的在线预算。

ARH 的 `tensor_backend=auto` 声明一张CUDA卡和30秒阈值；旧中心未声明上述能力时，ARH提示并以CPU执行。显式 `torch_cuda` 省略阈值、保持强制GPU。DCO负责授予资源，调用工程负责CPU/GPU计算路径及结果身份。

默认部署中，科研工程运行在 Aya1-PC，向本机中心 `http://127.0.0.1:26133` 提交任务。Aya1-TX 的同一端口提供 worker 服务；在该设备运行科研工程时，auto 会检查服务角色，未检测到本地中心便独立计算，需要接入 Aya1-PC 时显式填写其中心地址。中心模式下，工程原有 workers / 并发上限不参与 CPU 准入；有限的提交窗口用于提前准备任务，实际 CPU 和内存由中心统一分配。

每个工程设置 `resource_share`（默认 1.0），中心按活跃工程的相对权重分配整数线程，用最大余数补齐余量。例如可承接54线程、三个工程同权且需求充足时为18/18/18；两个工程权重2:1时为36/18。不能整除时，例如53线程、三个工程同权，为18/18/17。份额和预取按实际匹配能力计算：工具、能力标签、共享路径和RAM不匹配的供给不抬高项目目标。需求不足的份额供其他待运行任务使用。

单线程 SPICE 任务声明 `cpus: 1`。可并行的整轮任务声明 `cpus: 0`，中心按工程份额和可用节点授予实际线程数，并把 `${cpus}` 传给程序。`max_cpus` 限定任务有用的并行规模，同时约束初始自动授予和后续借核；省略时沿用节点上限。`cpus: 0` 由中心处理，直接 worker 任务需声明正整数。整轮任务在单个节点执行；公平轮先安排各工程能运行的固定任务，自动任务最多等待一秒凑齐目标，之后可用部分授予启动。等待CPU或内存的任务不跨工程锁住空闲线程。支持协作借还的同步内核可在安全批次边界调整授予，其余在途计算保持原授予；工程端不提交节点约束。

自动线程任务按每台候选节点的实际容量计算所需线程；较大节点忙碌时，可在空闲的小节点取得相应授予。同优先级、同固定/自动线程模式的就绪任务，先安排可运行设备较少的任务，把其他设备留给适用范围更广的任务。

调用工程通过`capabilities()`或`health(brief=True)`识别汇总协议能力，再用`admission`/`window`按requires声明工具及实验环境。能力名称在请求与设备两侧统一转换，例如`ngspice47`与`ng47`等价；历史平台命名运行时别名只在迁移边界归一为`rgcn_python`，不再发布为平台能力。`capable_cpus`是匹配能力且可接单的在线预算，排除暂停设备及线程数不足以承接固定任务的设备（0表示暂无供给）；`target_cpus`是当前工程份额，查询不登记活跃需求。科学缓存按物理输入、明文引擎族/主版本、执行/测量协议和容差复用。工程不枚举nodes、不绑定设备名，也不读取分配详情，设备配置与选择由中心管理。

能力与执行引擎不生成或保存哈希，科研回执和科学缓存也不记录引擎哈希或其派生分区。`ng47` 是唯一语义能力名，旧 `ngspice47` 归一到该名称。工具注册、任务和准入拒绝哈希或 OS 选择标签；worker 不把操作系统发布为科研能力。执行兼容性由明文引擎族/主版本、执行/测量协议和数值容差确认，兼容设备共同参与。源码、PDK、模型和输入内容摘要继续承担数据身份、传输完整性与资产复用；不得将其反向变成设备选择条件。

中心声明`admission_job`时，`admission(project, job_id=原任务ID)`返回该任务的`recommended_cpus`、`current_cpus`、`current_memory_mb`和`available_memory_mb`，供阶段边界先判断是否值得调整工作池。此查询不改变授权；需要调整时排空当前池，再调用`resize`并按实际回执建池。运行中的worker任务通过同一lease通路查询。

`queue_ratio`（默认 1.0）控制提前排队比例。`Client.admission()` 返回当前窗口及 `max_in_flight` 安全上界；`Client.window()` 每秒至多刷新一次当前份额和上界，节点新增、能力登记和预算变化都会反映到窗口：

```text
固定线程波次槽位 = max(1, sum(floor(各设备份额 / 单任务线程)))
有上限的自动线程波次槽位 = max(1, sum(ceil(各设备份额 / max_cpus)))
预取任务 = ceil(完整波次槽位 × queue_ratio)
提交窗口 = 当前运行槽位 + 预取任务
```

整轮 `cpus: 0` 未设上限时，初始运行槽位为取得份额的设备数，至少为 1；设置 `max_cpus` 时逐设备计算完整波次。例如24、30、96线程三台设备和`max_cpus=96`需要三个任务覆盖；固定线程任务的零散余量也按设备分别计算，不跨主机拼接。自动任务在编译后归还线程时，窗口计入仍在运行的任务，按目标剩余线程和设备空闲槽位补充下一波；预取数量继续按完整波次乘`queue_ratio`计算，避免把每个已退核任务再乘一次排队比例。最大窗口覆盖每任务退到1CPU的情况，工程端提交池应允许窗口增长，实际待处理数量始终受`window.current()`约束。两台节点都可承接时，`queue_ratio: 0`也可同时提交两项整轮任务。CPU、内存、工具、能力标签和共享路径仍决定实际派发；提交窗口不是CPU授权，也不是服务端全局队列硬上限。工程只预取已就绪工作，保留搜索批次和代际边界。已启动的旧控制器和在途任务在自然边界更新。

配置位置：AOP 原运行配置、ARH-DSE、Bench 顶层使用这两个字段；AOP 规范化配置在 `execution` 中映射，RGCN 放在 `resources` 中。

```json
{"resource_share": 1.0, "queue_ratio": 1.0}
```

AOP 按角点、RGCN 按候选/PVT、Bench 按算法与种子预取。调用方按窗口上界创建线程池，按当前窗口限制已送中心且未收齐回执的任务；本地准备future另有队列界限，AOP为2×maximum。长任务等待期间也定期补位，其他工程结束后可扩大在途窗口。窗口缩小时已接受任务继续完成。RGCN 按输入顺序返回结果。ARH-DSE 整轮进入中心，轮内保留原生请求队列与构建复用机制。

Bench 默认每种子声明 1 CPU，由动态窗口决定并发，不受工程 workers 限制。同步Class-AB物理批次和方向输出内核在计算前借线程、finally归还至1；任务尾部可继续借用其他任务释放的线程。`remote_compute.max_task_cpus`可设单任务借用上界；异步求值池在启动前申请固定额度，pending收齐后归还。`remote_compute.task_cpus`指定初始额度，0表示中心初始自动授予；关闭顶层`adaptive_threads`后保持固定额度。取消批次会停止后续提交、取消并收齐已接受任务。

节点 CPU 预算、硬上限和内存配置见[算力控制](../README.md#设备预算与配置文件)。看板同时显示各工程的权重、目标份额、已分配线程和排队数。焦点刷新只影响终端展示，后台调度和资源探测持续工作。SDK首次借还资源用`health(brief=True)`协商协议和能力，省去设备历史与看板内容；旧服务忽略该请求头时仍可使用完整响应。

调度思路参考 [Dask 排队与负载选择](https://distributed.dask.org/en/stable/scheduling-policies.html)、[Ray 有限待处理任务](https://docs.ray.io/en/latest/ray-core/patterns/limit-pending-tasks.html)、[Slurm 资源预留与回填](https://slurm.schedmd.com/sched_config.html)。实现使用 Python 标准库。

## 分配策略

- `adaptive`：先满足线程、内存、工具与工程份额，优先选择预计CPU负载较低的设备。候选负载相差不足一个逻辑线程时，用相同版本启动基准的单线程/总吞吐组合先验，再以同类任务真实耗时校准和替代，结合节点权重与未采样节点。基准组成与持久化见下文“启动算力校准”。
- `balanced`：使用相同的 CPU 负载优先规则，接近时使用单线程/总吞吐组合先验及节点权重，不采用同标签的历史耗时。
- `round_robin`：在满足资源条件的节点之间轮询，便于固定分配实验。

负载按设备的逻辑线程数归一化，并把尚未反映在采样中的派发和线程借还立即计入：

```text
背景占用线程 = max(0, 采样CPU比例 × 逻辑线程数 - worker快照已分配线程)
预计负载 = (背景占用线程 + 当前预留线程 + 外部任务线程 + 新任务线程) / 逻辑线程数
```

同设备的多个 worker 合并计算主机占用，避免重复计入 CPU。尚未取得CPU样本时，直接按当前授权账本估算负载。整轮 `cpus: 0` 比较节点时按一个启动线程计价，选定后仍按可用线程和工程份额授予。采样中的在途任务已包含在 CPU 使用率内，用当前账本替换其旧授予；连续提交不用等待下一次采样才更新负载。CPU 使用率参与节点选择，线程和内存预算决定准入；其余设备已满且该设备仍有可用额度时，可继续派发。`node` 仅供操作员运维和设备验收使用；科研任务按能力选点，已运行任务保持原节点。

任务可设置 `single_thread_weight`（有限正数，默认1），也可用 `run/spice/rtl --single-thread-weight`。这项设置只决定负载接近时选哪台主机：1对单线程延迟和并行总吞吐无明显偏向；2适合串行仿真占主导的任务；0.5适合大量独立评价的搜索。它与工程份额 `resource_share`、排队顺序 `priority` 分开，不增加线程、不改变正在运行任务的节点。单线程占比为 `w/(1+w)`，按该占比组合归一化的单线程分数与配置线程下总分的几何均值；节点weight保持生效，adaptive的同标签实测耗时校准该先验。balanced使用相同负载和算力先验、不学习历史；round_robin保持轮转。缺少同版本原生总分时仅按可用配置槽数估计总量；单线程分数不全时该分量保持中性，旧哈希/解释器分数始终不参与比较。

中心 `Client.capabilities()["capabilities"]` 包含 `single_thread_weight` 时，适配器可传此字段；旧中心省略它。普通默认1任务保持原请求格式；中心保留偏好元数据，并在派发时移除该中心提示，已有worker无需重启即可执行。启动原生估算的单线程结果位于 `benchmark.single`，使用同一混合工作单位；CLI以“单/总 Miter/s”展示。

中心直接连接设备IP的26133端口；HTTPS节点同时配置`ca_file`。worker通过本地lease接口提交CPU/RAM请求，由中心现有连接取回并授权，无需worker反向访问中心。可用`center.control(add={"name":"compute-linux","url":"https://203.0.113.42:26133","token":worker_token,"ca_file":"worker.crt"})`加入。节点配置和令牌保存在本机`nodes.local.json`，不放入聊天或Git。旧反向回调入口已退役。

节点`timeout`默认10秒，跨网慢连接可通过`control(node=名字,routing={"timeout":120})`在线调整。任务SDK通过`REMOTE_COMPUTE_TIMEOUT`继承连接时限，显式指定超时的客户端保持原设置；计算执行时限仍由`task.timeout`控制。

中心并行探测设备，启动首轮最多等待1秒；未就绪节点保持检查中，由后台继续探测，已就绪设备先提供看板及能力。单设备控制与新增设备只同步刷新对应节点，其它离线节点的探测继续在后台；控制返回后，该设备的新工具和预算已进入匹配视图。

同一任务的资源调整串行处理，网络调用不持有中心或资源池的全局锁。增加的CPU/RAM先占额度，缩减的额度等worker确认后归还；实际授权未变化时直接返回。回执丢失后查询原任务，暂时无法确认的增加额度继续保留，直到下次确认或任务正常结束，避免重复分配和退出泄漏。

自动任务的初始授权不是后续并发上限。池/原生进程已收齐的阶段或自然批次边界可调用 `client.resize(id, 0, memory_mb=下一阶段总MiB)`，重新按当前所选节点预算和工程份额申请，实际CPU仍受`max_cpus`限制；以返回的`cpus`、`memory_mb`重建或调整池，再启动下一批。`resize_auto`能力表示支持0；显式正数仍表示有用CPU需求。缩减到1后也保留自动需求意图在调用工程内，下次边界再次申请，中心不在后台改动运行中的池。

RAM随并发线性增长时，使用 `client.resize(id, 0, base_memory_mb=1024, memory_per_cpu_mb=512, memory_limit_mb=8192)`；这表示下一阶段的基座+每CPU需求，中心结合当前CPU/RAM和可选RAM上限，一次返回匹配的授权。系数模式与固定总量`memory_mb`互斥；`resize_memory_model`能力表示支持。SDK面对缺少能力的旧中心保留当前授权并返回`supported=False`，调用工程按旧路径运行。同步内核完成后可归还资源，异步在途工作继续使用已有授权，只在自然收齐边界更新，不强制排空来改变算法执行次序。

设备地址显示实际节点URL的IP，`local://`私有worker显示本机。可通过`control(node=名字,routing={"host_ip":"203.0.113.42"})`补充展示IP；它不改变连接地址。

worker 默认将 `OMP_NUM_THREADS`、`OPENBLAS_NUM_THREADS`、`MKL_NUM_THREADS`、`NUMEXPR_NUM_THREADS` 设为此次线程授予；任务 `env` 显式指定的值优先。例如外层并发处理多个候选时，可将这些数值库设为 `1`，避免嵌套并行；编译仍使用 `${cpus}`。外层任务与内核线程共同使用此次授予。

任务还可设置 `priority` 和 `requires`。较大的 `priority` 先启动；`requires` 要求汇总能力标签全部匹配。节点自动报告 Python、ngspice、Verilator 等可用工具，调度器据任务使用的工具筛选供给。`resource_share`、`queue_ratio`、`single_thread_weight` 有界且只影响中心内部公平、预取和近负载先验，不能指定设备或绕过 CPU/RAM 准入。

中心与直连 Pool 按任务 ID 区分本方预留和外部任务；慢健康快照中的旧授予不再占用第二份额度。新 worker 声明 `memory_accounting=rss`：首次进程树采样前及未反映到健康快照的派发按申报量预留，采样后按当前 RSS 记账；设备预算与系统实时 8GiB 可用余量共同约束新任务准入。旧 worker 沿用其预留规则，允许逐节点在空闲边界更新。任务取消只向 worker 发送一次请求，随后按正常间隔等待终态；空队列由新任务与资源事件唤醒。

用 `label` 区分电路、保真度或相近工作量的批次，使同类耗时具有可比性。执行中的任务保留原节点。短暂断连时保留原任务ID和资源预留，状态中的 `connection_error` 显示连接问题；重连后继续查询该任务，不重新执行。已知节点暂时离线时，匹配该节点的待执行工作继续排队，其余在线节点照常服务。

`project`承担来源工程，`label`只描述工作，如`TSA/nn-prev/s42`、`class_ab/FACE-MO/b96k`、`BOOM/YOLOv8n/kitten`。目录、长哈希、运行批次和模型身份留在请求/回执；同类任务使用稳定标签以便耗时校准。看板将已有长标签格式化为简短描述，不修改已接受任务的原标签、缓存身份或输入摘要；完整内容通过任务API查看。

健康接口在节点离线时仍返回有效响应；上游通信失败返回HTTP 502。历史版本或中心重启留下的 `interrupted` 任务可直接按原ID `fetch`：worker仍在运行时暂缓领取，完成后以worker的实际结果恢复中心记录。调用工程保留已接受的ID，避免因超时改在本机重复启动。

差网络下，每个连接池的调用线程共享最多8条连接，运行中的简要状态复用不超过2秒的已有worker健康快照；终态仍实时读取。现代连接空闲10秒回收，正在传输的请求不受此空闲限制；旧客户端使用显式Connection: close，保持其POST兼容性。中心按节点串行取回结果，同ID请求合并到作业目录中的一份原ZIP，领取删除作业时一并清理。二进制GET使用Range/ETag续传，断线保留当前请求内已收字节，最多4次尝试；ETag变化时重新取完整结果。任务提交、资源调整、取消和内容寻址资产上传按原ID安全重试，避免重复计算。

二进制上传保留明确的Content-Length并分块写入，连接超时分别约束各块写入及响应读取，整包持续上传可超过该时限。资产按内容哈希在各节点缓存；缓存revision变化只失效存在记录，在途同哈希上传继续共用一把锁，等待调用者结束后回收锁。上传丢失回执时，先用原有GET确认exists，服务端校验内容哈希并发布成功后才返回存在，已收到的资产直接复用；真正未收齐或确认失败时，仍以同一内容ID从头重发。依赖同一资产的任务共享上传，完成后继续原任务派发；结果下载沿用Range/ETag续传。

Client.fetch与中心向worker取回原ZIP统一调用Client.artifacts，复用各自独立的结果Client和连接池；两跳下载的响应头与接收空闲超时均至少120秒，节点状态/提交沿用原短超时，调用工程无需改调用方式。中心首次返回结果头之前先取回远端ZIP。排队等待时发现调用端已断开，就放弃该次排队取回；已经开始的有效传输仍完成并留下缓存。正常持续收数不以总耗时截断；失败保留原任务和结果，之后仍用原ID领取。

工程份额按各请求实际可运行的节点计算加权整数最大最小分配，含固定节点、工具、共享目录和CPU需求上限；同一节点的线程不会同时算给多个工程，只计算本任务实际兼容的设备预算。单节点沿用最大余数，多个节点用逐线程增广补齐可行份额。admission、排队分配和阶段增借使用同一份额；任务在关闭旧池后的cpus=0请求会按当前份额重新授予，包括归还超额CPU。已运行池保持原授权直至调用方发起安全阶段协商。

Windows新建作业目录设置可继承的Low完整性标签，允许现有带Low标签的firtool、GSIM等工具写入本作业的输出和TEMP。处理发生在解包和创建子目录之前，使用Windows原生API；原DACL及继承标记保持不变，现有作业、父目录、共享盘根和工具文件保持原样。Linux沿用普通目录逻辑。
操作员视图中的工具变体包含节点名、可执行入口和可用性，不包含引擎文件哈希；普通工程工具视图只表示可用性。科学输出使用明文引擎族/版本、执行/测量协议与容差。

## 现有工程接入

四个工程默认优先本机 26133 中心。中心缺席时 auto 模式回到独立执行，沿用 `min(C,16)+floor(max(C-16,0)/2)`；RGCN 的显式中心 URL、ARH/Bench 的显式 scheduler 要求中心可用；AOP 的 scheduler 模式表示优先本地中心，显式 compute-center URL 才要求指定中心可用。显式 local 固定独立计算。worker 子任务设置 `REMOTE_COMPUTE_WORKER=1`，避免递归提交。

| 工程 | 选择中心 | 执行粒度与节点要求 |
| --- | --- | --- |
| AOP_v2 | `--compute-center URL` 或 `compute.center`；网页选本地调度器 / 本地工程 | 每角点任务；源码、模型按内容同步，沿用原测量与解析。 |
| RGCN-v2 | `config.py` 的 `COMPUTE_URL` 或 `circuit.pre.remote_compute.url` | PRE/POST使用`rgcn_python`；神经训练使用已验证Torch/PyG及检查点续跑的`rgcn_train`。源码和模型闭包同步，保留后仿恢复与原任务ID。 |
| ARH-DSE-v2 | `--compute-center URL` / `--compute-mode auto` | 整轮任务；`run_remote`提交冻结源码资产，执行与源码校验使用同一资产。需要`arh_python`与`shared.arh`复用预部署工具、模型和持久统计缓存。Aya1-TX部署见[工具部署](../README.md#工程内工具安装)；声明`shared:arh`及相应编译器能力，由中心自动匹配。 |
| Analog-Opt-Bench-v2 | `--remote-url URL` 或 `remote_compute.url` | 算法/种子任务；源码同步，需要 bench_python 科学环境。 |

仿真器、评分、实验预算、设计顺序、研究缓存和报告仍由调用工程管理。线程配置影响资源分配；中心不修改科研评价逻辑。验证任务使用独立临时产物目录，ARH 的 JobManager 可传 `artifact_root` 隔离结果、历史与报告，将验证结果与正式结果分开。日常正式运行仍按各工程规则更新当前成功报告。

ONNX CPU推理任务额外声明`onnx_cpu`：此工具别名指向已验证同时具备OpenCV、ONNX Runtime及CPUExecutionProvider的Python。模型、输入数据与descriptor另作为内容资产同步。GPU硬件存在不等于CUDA推理后端可用；需要GPU的工程必须同时声明`accelerator`和对应的已验证工具标签。

## Python 接口

CLI、退出码和自动化示例集中在 [README](../README.md#cli-与自动化)。

```python
from remote_compute import Client, ngspice

result = Client().run(ngspice("divider.spice"), inputs="examples/spice")
print(result["status"])
print(result["files"]["ngspice.log"].decode(errors="replace"))
```

指定 `output_dir` 时产物保存到该目录；省略时以 bytes 返回在 `result["files"]` 中。科研入口统一使用`Client(中心地址)`。`Pool`是中心内部调度实现；hybrid本机worker仅供所属中心进程使用。指定设备仅用于`machine()`运维操作和明确的设备验收，不用于生产科研选点。

其他工程可把此工程目录加入 Python 的模块搜索路径，再导入 `remote_compute`。工程本身没有 pip 安装步骤。

### 向中心提交与查询窗口

```python
from remote_compute import Client, command

center = Client()
window = center.window("my-project", resource_share=1.0, queue_ratio=1.0, cpus=0, max_cpus=8)
print(window.maximum, window.current())
task = command(["${tool:python}", "runner.py", "--workers", "${cpus}"],
               cpus=0, max_cpus=8, memory_mb=1024, project="my-project", resource_share=1.0,
               outputs=["measurements.json"])
result = center.run(task, inputs="my-batch")
print(result["status"], result["cpus"])
```

同一工程的任务保持相同 project 和 resource_share；多个调用者各自的有限窗口由调用者维护，中心统一控制执行资源。

### 同步内核在阶段边界借还资源

全中心 `control(paused=True)` 只暂停新任务派发，已运行任务仍可按原 ID 在安全边界 `resize`；节点 worker 的 `paused` 则同时限制该节点增长。全中心暂停影响所有工程的新增派发，接口自身没有 TTL 或窗口所有权；临时维护执行方负责计时、记录原暂停状态并在结束或异常时恢复。公平份额、实际可用 CPU 和 RAM 准入始终有效。

已确认的自动阶段增长需求若因满载只得到部分授予，后续派发为其保留节点公平份额内新释放的空核，避免空核持续被其它排队任务重新借走。计算保护量时沿用同工程就绪任务分配和原阶段内存模型；较高优先级任务仍可派发，内存不足的增长不锁住空核。降低需求、取消或结束后立即解除保护。保护不修改活动池或 worker 授予，调用工程在下一次池已关闭的安全边界通过原 ID 的 `resize` 领取；这些空核在领取前可暂时空闲。

worker 任务环境提供 `REMOTE_COMPUTE_URL`、`REMOTE_COMPUTE_JOB_ID`、`REMOTE_COMPUTE_TOKEN`、`REMOTE_COMPUTE_HOME`，支持时 `REMOTE_COMPUTE_RESIZE=1`。SDK 使用执行设备的 DCO 包；远端请求先到本机 worker 的 lease 接口，再由中心的既有轮询连接转交授权。SDK 根据 `REMOTE_COMPUTE_LEASE` 和 worker 任务 ID 路由，调用工程直接使用下面的 Client 接口。中心仅监听回环时，已配置 worker 仍可通过这条单向通路借还资源。

```python
import os
from remote_compute import Client

center = Client(os.environ['REMOTE_COMPUTE_URL'], os.environ['REMOTE_COMPUTE_TOKEN'])
identity = os.environ['REMOTE_COMPUTE_JOB_ID']
grant = center.resize(identity, desired_threads, memory_mb=phase_ram_mb)
try:
    compute_batch(threads=grant['cpus'])
finally:
    center.resize(identity, 1, memory_mb=resident_ram_mb)
```

`resize` 保留原任务ID和输入摘要，返回 `id/cpus/memory_mb/requested_cpus`。增长受工程权重、任务 `max_cpus`、当前可用线程和RAM约束；调用方始终使用实际 `cpus`。初始以 `cpus=0` 提交的自动任务，在已排空阶段申报正整数有用并行量时也参与同工程公平分配；重复当前线程数仍重新分配，申报值保持上限，归还到1核走直接路径。初始固定线程任务保留其显式调整语义。可选 `memory_mb` 在同一请求中调整内存预留，例如编译结束、子进程退出后 `center.resize(identity, 1, memory_mb=512)`。内存准入失败返回409并保留原CPU/内存授予；实际RSS仍参与准入，降低申报不隐藏真实占用。两端声明 `resize_memory` 能力后使用联合调整，旧节点返回 `supported=False` 并保留原授予。同步批次结束后才归还，异步 pending 期间维持固定授予。BLAS 内层保持1，避免与外层并行相乘。worker 与中心同步计账，取消、异常和完成均按最后授予释放；回执保留 `initial_cpus/peak_cpus/resize_count`。旧节点未声明 resize 能力时继续原授予。

DCO负责跨工程份额、CPU/内存准入、实际授予和原ID资源账本；调用工程负责判断是否有可伸缩内核，并在并行池/子进程已收齐的安全边界借还。YOLO包审计默认每包1CPU；对约50毫秒的串行尾段增加线程会增加通信和调度开销，较大通道块才按实测启用多核。Rocket/BOOM可在编译结束后按实际仿真并发和DDR规模同时收缩CPU/内存，ARH常驻池保持初始授予直到池关闭。工程资源配置不参与科学结果缓存身份。

RAM申报对应当前阶段仍需承载的对象和并发子进程。编译器/并行池退出且其对象释放后，联合归还CPU和RAM；下一阶段增加峰值前先申请并检查实际授予。付费账本、模型和数据仍常驻时保留其需求；DCO 保留任务申报与原请求身份，已运行任务按实际 RSS 计入设备占用，空预留可供其他就绪任务使用。`/health`的节点`active_jobs`同时提供`memory_mb`与worker最近采样的`rss_mb`，`memory_requested_mb`与`memory_reserved_mb`分别表示申报合计和当前计账量。

## 任务文件与结果

一个任务由顺序执行的 `steps`、输入文件和输出模式组成。任何一步失败即结束该任务，保留退出码和日志。`timeout` 覆盖整条执行链。取消和超时会结束所持有的进程树，包括编译器子进程。

普通计算的 `retries` 默认2：提供者停止、原作业明确丢失或重启后报告interrupted时，中心保留原提交ID、输入摘要、种子、资产与引擎约束，重新入队；每次执行使用独立worker_id，历史写入attempts。重试不向客户端短暂公布终态，不重复归还资源。普通网络超时继续查询原任务，其他在线设备照常工作；程序报错、科学不可行、任务超时和用户取消均不自动重算。运维machine命令默认retries=0，避免文件修改等副作用重复执行。

中心和worker默认将任务、资产、回执及声明输出保存在工程 `.local/state` 普通磁盘目录；仅TMP/TEMP使用共享RAM盘。worker重启保留已完成结果，未完成结果带原ID标中断，由中心按重试预算恢复。中心重启重新连接原任务，已排队请求继续排队，已完成未领取结果仍可取回。调用工程把长期训练checkpoint、已付费评价账本声明为outputs，并在安全阶段原子发布；恢复任务沿用既有账本，避免重复已完成求值。

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

资产按文件内容生成 ID；模型变动产生新 ID。提交前每项引用必须存在于中心；缺失时返回 `Synchronize missing asset first`，不创建任务或占用线程。客户端重新同步缺失资产后可用同一 ID 和原输入提交；长驻生产者在重连、重载或缓存回收后重验静态引用。节点部署路径分别配置，任务保持同一份逻辑输入。`shared` 中的预部署内容版本由调用工程维护。

任务输入和输出位于普通磁盘的持久状态根，`root/--root` 与任务 TMP 的 `temp_root` 分开。编译或仿真可重建临时物使用任务独立 TMP；客户端收齐结果后默认清理节点任务目录，显式 keep 可保留。共享资产按引用和缓存策略复用；目录参数及迁移方式见 [README](../README.md#设备预算与配置文件)。

普通计算任务继续使用独立工作目录。指定 `cwd` 的任务在目标机该目录中执行，`cwd` 接受绝对路径、`~` 或 `${shared:工程名}`；`${work}` 和 `outputs` 始终指向任务输入目录，可将需要回收的文件明确写入 `${work}`。任务 JSON 的 `stdin` 是输入包中的相对文件路径，逐步作为标准输入传入。

中心内部回执包含任务 ID、输入摘要、命令、退出码、资源、时间和工具登记快照。普通工程视图保留任务状态与本任务资源，去掉设备名、URL、工具路径和命令路径；`tools` 仅保留可用性。科学输出使用明文数值协议与容差，不生成或保留能力/执行引擎哈希。科学缓存依据物理输入、数据内容、角点与测量协议复用，不按工具构建或平台分区。接受后的任务通过原 ID 查询，相同 ID 和输入在保留期间只执行一次。

### 手动缓存交换

`sync_cache`先比较记录清单，再按最多1024条或约1MiB的批次补缺；相同清单不再导出记录，源码/模型资产继续按内容复用。科研数据交换不复制SQLite、WAL、任务认领或共享压缩布局表。

AOP交换键明文携带科学命名空间、五字段执行环境和原始角点数据ID。执行环境不求摘要；记录摘要只覆盖宽度与数值payload。目标通过生产者的`semantic_cache_units`查找或分配自己的UUID目录，保存相同`contract.json`并使用原`cache-unit`/`append`锁。仅传播kind0原始角点，聚合由目标重建；unknown版本或legacy-unvalidated协议退出可复用交换。

RGCN先由生产者解码共享布局记录，再以独立完整逻辑JSON压缩交换，保留原始数值、物理签名、角色、PVT和溯源。未知协议旧观测保持`legacy-observation-UUID`，通过固定的`observation_physical_signature`保留来源边界；仅有一个签名的旧库可使用该唯一值，多签名来源未定时留原库待判定。可同步的旧观测仍由生产者`find/cached`排除测量命中。该观测ID不是能力或引擎标识。

## 传输与本地执行

- hybrid 本机 worker 进程内调用，不经过第二层 HTTP；本机结果直接附加中心记录后打包一次。外部调用者仍统一访问中心 26133。
- 普通固定线程任务保持原始输入包直接转发；整轮自动线程任务仅在派发时写入实际授予的线程数。输入身份与重复提交语义保持。
- 模型和源码按内容标识同步；同一节点缓存版本内每份资产只检查/上传一次，并发任务共用同步锁。worker 重启或回收旧资产后重新确认；命中回收与派发的交错时，中心重新同步缺失资产并沿用原任务ID。
- HTTP 使用持久连接。状态轮询只返回进度字段，完成后取本任务资源和科学结果；支持 gzip 的客户端接收压缩 JSON，旧客户端继续接收普通 JSON。
- 文本包采用低开销压缩；已经压缩的 gz/npz/zip/7z 直接封装。仿真器构建和中间文件留在执行端，按任务 outputs 回收必要结果。

| 工程 | 默认回传内容 |
| --- | --- |
| AOP | 执行端测量与解析后的 aop-result.json；按配置选择日志。 |
| RGCN | measurement.json：指标、分析证据、波形统计及数值摘要，不传整套原始波形。 |
| Bench | result.pkl.gz：评分、前沿、轨迹等报告需要的科学数组。 |
| ARH-DSE | summary/report 与恢复、验证所需产物；保留报告校验，不回传整个编译环境。 |

通用 SPICE/RTL 命令的 outputs 由任务声明；需要原始波形时显式保留对应输出模式。调用工程的测量定义和正式产物契约保持。

声明`outputs`的任务在正常、失败、超时或取消后，Python自动删除已消费的输入、构建物及未声明的仿真原始文件，只保留声明的结果和诊断日志等待取回；未声明输出的通用任务保留原按名称读取job-file的行为，取回后一起回收。排队取消立即回收未执行的输入，退出再次收尾。待取回结果不作为垃圾清理。

worker的可重建资产按LRU管理，启动、任务取回和退出时回收未被任何在途或待取回任务引用的旧版本，空闲目标上限512MiB；仍被引用的大资产可暂时超过该值。检查/上传后的资产保留5秒交接窗口，供紧接着的submit引用，避免活跃资产超过缓存目标时反复被其他任务回收；之后按正常LRU清理。健康接口的`asset_cache`提供字节数、上限和数量，统计只扫描新资产，不在刷新时遍历磁盘。共享盘的其他工程目录不参与此回收。

中心保存的资产ZIP也在启动和退出时按512MiB目标回收无回执引用的旧缓存。运行期间保留已同步但尚未提交的ZIP，避免多份资产上传与提交之间发生回收竞争；退出时不再保留5秒上传交接窗口。未领取或中断待恢复的回执所引用资产始终受保护，可能超过缓存目标。中心`health.asset_cache`显示占用，`retained_jobs`显示仍待领取/处理的终态记录数量。

`Client.run`、`Client.fetch`和`machine`默认在成功取回后清理服务端任务；有意保留时才使用`keep=True`或`delete=False`。任务由调用工程收齐并持久化输出后，服务端副本即可回收。服务退出负责清理自有TMP和过期缓存，未领取结果继续留在普通磁盘，重开后用原ID领取。DCO不会因任务结束或超时而自动丢弃研究回执；“不保存实验记录”应由调用工程控制正式报告，并继续正常取回清理DCO任务。

原生HTTPS worker支持单向资源协商：任务访问本机lease接口，中心按节点合并轮询待处理请求。存在此类在途任务时约每0.2秒检查一次，空闲节点沿用常规采样周期，避免每任务各建高频轮询连接。网络短暂中断保留原授权。

## 接入与传输约定

- `requires`声明实际工具及`shared:<别名>`数据位置能力，步骤以`${shared:别名}`引用；同名工程目录不代表数据已同步。冻结数据与模型作为内容资产，结果取回后由工程发布到持久目录。
- 数据集使用语义 `requires` 声明能力，不绑定设备、OS 或引擎构建身份；回执与科学缓存按数据和测量协议维护，能力/执行引擎不生成或保存哈希。
- 科研工程不另建节点池、反向代理或隧道。中心内部回执保留来源设备；工程任务输出保留科学输入和明文引擎族/版本与执行、测量协议，不保存引擎哈希，调度不依赖特定主机名称。
- 只提交已就绪、能消费结果的工作；源码/模型每内容版本同步一次，重复候选复用工程现有科学缓存。连续小内核合并阶段授权，阶段结束及时归还；等待/借还耗时与CPU计算分开统计。
- `read_job_file_changed(id,path,version=None)`返回`version/data/changed`；后续传回版本，未更新返回304且data为None，中心与worker两跳都不重传文件。需要恢复状态时按原子发布批次更新，不反复下载未改变的检查点。
- 节点动态资源独立采样，慢节点不阻塞其他设备；首次连接/worker换PID取完整工具元数据，日常采样只取动态资源、活动任务和有界通知。简要健康查询直接返回身份与协议能力。

静态大输入已有内容摘要时，复用同一个 SDK Client，调用
`client.sync_files(files_or_factory, content_id=摘要)`。摘要覆盖包内相对路径、内容和执行权限；
SDK 先确认已有资产，命中时不生成文件、不压缩，缺失时才调用零参数工厂并重新上传。
索引只存最近128个摘要与资产ID，不保留ZIP副本；不改变中心资产引用/回收规则。

## 启动算力校准

固定 `native-mixed-v4` 负载包含批量 FP64 Newton、uint32 逻辑/访存和 512×512 矩阵乘法。启动采样 CPU≤20% 且没有匹配缓存时，分别测 60 秒单线程、60 秒预算线程；导入/分配/预热不计分，BLAS 内层固定 1。CPU 身份、可见 CPU、线程预算匹配时复用 `.local/benchmarks/<线程数>.json`，`DCO_BENCHMARK_DIR` 可改位置；繁忙时保留业务任务，待空闲启动或手动 calibrate。

Miter/s 是该固定混合负载的工作单位，不是 FLOPS。只有同版本基准参与比较；同标签真实任务耗时用指数移动平均逐步替代先验。AVX 等指令集差异由实测反映，不套人为倍率。

## 生命周期与终端实现

持久状态根保存输入、回执、检查点、资产和未领取输出；可重建 TMP 放共享 RAM 或显式磁盘 temp_root。任务成功、异常、取消及服务退出都按进程/任务所有权清理；待领取结果与中断恢复引用继续保留。迁移前核对 reparse point，普通文件与链接元数据分别处理，不沿链接目标移动共享工具。

Windows 外部任务 TMP 设置与输入相同的低完整性写入标签，父目录 ACL 保持。Linux 进程监控共用 250ms 快照，退出/取消使用请求后开始的新扫描并跟踪另建会话的原生子进程。连接槽按 FIFO 排队，本地 PoolBusy 表示尚未发出请求：保留原 ID 和授权等待，取消时释放。

看板宽度达到 120 列时左侧资源/份额、右侧逐任务，窄窗改上下分区；每任务独立一行，4位ID仅作展示。底部固定用法/候选/分隔/输入四行，极矮窗口优先输入。使用真实视口高度、ASCII 边框、实际字符单元宽度，按绝对坐标只画变化行。Windows 经典控制台启动尝试 160×50，并尊重屏幕上限、已有大窗口及 ConPTY 宿主尺寸。

资源列用完整短表头“已用/预算”“进程/预留”，逻辑线程含义与 GiB 单位放在面板标题。任务表仅压缩显示：Bench 父标签保留场景编号和每项预算，如 `bookshelf_tall#5/3x12k`，不重复显示乘积总预算；ARH 原生门禁使用短描述。原任务标签及请求保持，通过 `jobs ID` 查看完整记录。

Windows 的 CPU 校准缓存身份通过本机注册表和 `GetNativeSystemInfo` 读取，终端可见性与焦点识别共用 Toolhelp 进程快照。两条启动路径不查询 WMI，避免 Python 3.14 的 `platform.machine()` 或 PowerShell CIM 等待阻塞初始化与首帧；架构名称保持与已有校准缓存一致。

托管父进程和服务子进程各自持有防休眠请求，覆盖子进程重载边界。Windows 使用 `PowerCreateRequest` 持有 SystemRequired/ExecutionRequired，操作员可用 `powercfg /requests` 核对具名 DCO 原因；Linux 的 systemd-logind 环境使用 `systemd-inhibit --what=idle --mode=block`，由随上下文退出的标准输入管道管理辅助进程。异常路径归还已取得的请求与句柄，显示器不要求常亮，一次性客户端操作不持有抑制请求。

耗时命令和文件操作后台执行，完成后及时释放 future/句柄；补全只读快照，Enter 才执行。键位与实际操作集中在 README。
