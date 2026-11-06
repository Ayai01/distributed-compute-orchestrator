# DCO 接口与设计

## 责任与入口

DCO负责准备、排队、能力路由、CPU/RAM/加速器分配、实际执行、领取和原ID恢复。工程提供业务工作、冻结输入、真实有用并行上限、能力和内存需求，并负责科学测量、评分、有限预算、结果解释与持久发布。

| 模块 | 职责 |
| --- | --- |
| `tasks/client/pipeline` | 任务声明、工程SDK、持久准备与收齐 |
| `execution` | 映射及数值内核执行，实际进程/线程宽度和尾项回收 |
| `coordinator/allocation/admission` | 队列、兼容资源、工程份额和准入窗口 |
| `worker/process/resources` | 进程树、工具环境、真实用量、临时文件 |
| `leases` | DCO执行器内部资源协商 |
| `bundle/acceptance/cleanup` | 不可变内容、原ID受理、引用与领取清理 |
| `commands/console/view` | 命令语义、交互和展示 |
| `cli/startup/terminal` | 启动、可见终端和重载 |

服务、SDK和终端仅依赖Python标准库；计算函数使用节点已登记的科学环境。工程通过中心提交，不读取节点清单或指定设备。操作员维护节点、预算、工具、解释器绑定、证书和共享目录。

## Python接口

公开任务只声明`max_cpus`，省略时为1024。串行工作声明1；单代、种群及有限输入集合声明真实有用上限。任务没有`cpus`、最低核数或auto开关。实际`cpus`只出现在DCO授权、状态和回执中。

```python
from remote_compute import Client, python_map

client = Client('http://127.0.0.1:26133')
plan = {
    'seed-1': {'task': python_map(
        'business:evaluate', inputs,
        max_cpus=len(inputs), requires=['ng47'],
        base_memory_mb=128, memory_per_cpu_mb=768,
        timeout=3600),
        'files': {'business.py': business_source}},
}
results = client.execute(plan, project='my-project',
                         output_dir='runs/current/dco', publish=publish)
```

`business:evaluate(item, **kwargs)`是可导入函数。业务文件作为输入或内容资产提供；源码、模型和数据分别复用。`python_map`保留输入顺序，默认每个输入运行于独立子进程，每项使用1核；完成即退出进程并释放其分配器/模型内存。DCO自动调整同时在途的输入数，工程不维护执行池或借核循环。单项多线程数值内核使用`python_call`，或由业务定义可复用的数值内核边界。

`python_map(..., backend='thread')`使用同一解释器内的线程，适合释放GIL的原生调用及必须共享的缓存；线程数仍由DCO管理，单项数值库限制为1线程。线程后端中的闭包、模块状态和对象缓存可共享；进程后端需要可导入顶层函数或`module:function`。纯Python CPU计算使用进程后端。

已有业务入口可以用`command(['${tool:python}', 'runner.py', '--workers', '${cpus}'], max_cpus=...)`。DCO在启动每一步前取得授权，再展开实际核数及数值库环境。任意外部命令在该命令边界使用其授权；需要运行中增减实际并发的输入流使用DCO映射执行器。

任务的`env`同样展开工具占位符；例如`env={'NATIVE_CXX':'${tool:cxx}'}`让Python入口使用当前节点已登记的编译器。原生源码、构建参数和二进制ABI分别记录；在执行节点生成本机库，或选择相同ABI的预编译资产，再由业务加载。基础C/C++不另申请能力，OS不包装成语义能力标签；普通客户端的任务不指定`node`，平台专属冻结输入由操作员按原ID受理。

在DCO任务内，业务入口可调用：

```python
from remote_compute import map_items, run_call
values = map_items('business:evaluate', ready_inputs, kwargs={'mode': 'native'},
                   limits={'base_memory_mb': 512, 'memory_per_cpu_mb': 896})
result = run_call('business:train', limits={'max_cpus': 8}, config=config)
```

`map_items`可提供`on_result(index, value, error)`，逐项持久化业务检查点；`python_map`在`.dco-results.json`写入已完成项及最终状态。直接任务声明的返回值须可JSON序列化，大数组、模型及科学记录写声明输出，返回其必要索引。映射项内部的嵌套调用共用该项的1核，不额外借核。

任务`timeout`覆盖整个运输的执行墙钟时间，与网络RPC超时分开。worker向整个子进程树提供同一单调时钟期限`REMOTE_COMPUTE_DEADLINE`，回执`deadline`记录对应UTC时间戳。`remaining_time()`返回剩余秒数，在DCO任务外返回`None`；`check_deadline(reserve_seconds=...)`在剩余时间进入业务落盘预留时抛出`TimeoutError`。业务在真实评价与记账边界检查，沿既有异常路径发布已付前缀、原ID和恢复索引。预留属于原任务时限，硬时限保持。

`.dco-results.json.items`保存已返回结果；独立的`item_states`按原输入索引记录`pending/running/succeeded/failed`及起止时间。SDK在启动前持久标记该输入的执行尝试；进程树退出后，worker将仍在运行的项标为`interrupted`、未有启动尝试的项标为`not_started`，保留完成结果，顶层状态与运输终态一致。评价次数沿业务账本读取。框架记录执行身份和输出，科学数组与付费前缀由业务及时发布；硬终止后的未知付费量保留为未知。

中心操作员使用`client.control(job=原ID, priority=整数)`调整已受理排队任务的调度优先级，取值范围沿任务priority规则。`submitted_priority`保留原声明，`dispatch_priority`记录有效覆盖；覆盖持久保存并在中心恢复时生效。原request.zip、请求SHA、科学输入、种子、费用和原ID保持。运行中及终态任务保持其调度与执行状态。priority在工程之间全局排序，同级再按resource_share分配；工程内分层安排须同时核对跨工程份额。

`python_task(function, kwargs=..., max_cpus=...)`运行串行业务入口：初始只占1核。入口在独立GP、角点或其它真实并行阶段调用`map_items(..., backend="thread")`，DCO按输入数动态授权，结束后归还至1核再执行串行训练等步骤。它适合一个搜索内部有多个并行阶段的工作流；`python_map`用于相互独立的完整输入，单项固定1核，`python_call`用于一个多线程数值内核。工程只表达业务阶段，不维护租约或进程池。

`iter_items(function, items, backend=..., stop_when=..., limits=...)`提供完成顺序的`index/value/error`流，用`with`管理。在途项与尚未消费的结果合计受实际取得过的并行窗口限制，不按输入总数积压。消费者发布和业务判定不阻塞空槽回收，结果窗口满时停止新增并归还空闲CPU。预算门触发调用`stream.stop()`停止新增，继续迭代收齐已在途结果。

提前离开上下文时，SDK收齐已开始的工作，将未消费记录的原索引、输入、结果和错误保存到声明输出`.dco-streams/`，并抛出`StreamResultsPending`；其`results/recovery_dir`供业务继续发布。正常消费完毕不生成该目录。恢复记录为本任务可信pickle文件，工程持久发布后随运输副本回收。

串行业务编排使用`python_task`；在原有排空边界进入训练时，`run_call`先等该原ID的SDK stream完成资源归还并检查错误，再取得数值阶段授权。仍有未启动输入的活跃流应先收齐或停止并排空，避免等待消费者自身而死锁。`run_call(..., limits={"max_cpus":8, ...})`声明阶段有用上限和可选内存需求，不改变父任务上限。

`map_items`、`iter_items`和`run_call`均接受阶段`limits`：`max_cpus`取父任务上限内的有用宽度；内存使用固定`memory_mb`或`base_memory_mb + memory_per_cpu_mb × 实际授权CPU`模型，线性模型可加`memory_limit_mb`。省略内存字段时继承父任务模型，显式内存字段替换本阶段模型；父任务声明和后续阶段默认值保持。映射在第一项启动前完成该阶段资源协商，随后按真实在途与就绪项扩缩；尾项结束归还空槽及对应RAM。已知冷编译峰值须由父任务首槽容纳，数值阶段按实测工作集声明，避免把编译预留沿用到每个并行数值项。

## 持久准备与收齐

`Client.execute(requests, project=..., output_dir=..., prepare=None, publish=None, ready_tasks=None, stop_when=None, cancel_when=None)`同步执行有限业务计划，直到已受理原ID收齐。独立output_dir在同进程或不同进程中并发调用；相同调用目录、中心URL和project共用待收窗口。共享状态索引位于调用目录`.local/dco-client/`，工程文件锁只覆盖该索引的短读改写。准备、计算、网络查询、领取与业务发布回调均在工程锁外执行；每个output_dir保留原ID恢复所需的单写入者锁。不同批次保留同一project及公平份额。

- `requests`为业务键到请求的映射，或`(key, spec)`序列；业务键非空且唯一。
- 业务请求为`{'task': task, 'inputs': directory, 'files': mapping}`，也可由`prepare(spec, original_id)`返回。依赖包装为`{'spec': business_input, 'after': [keys]}`。依赖图在受理前校验。
- `after`等待前序结果完成持久发布，之后才准备后续输入。独立任务继续准备和执行。
- `Deferred(reason)`表示输入暂未就绪，立即归还准备线程；同一次`execute`中，该项检查完成后至少间隔1秒沿原ID重试，其他项完成不缩短此间隔。重新启动管线时沿原ID重新检查业务门。`Skipped(reason)`表示业务门已决定跳过，在结果的`skipped`中登记，不生成科学回执。
- 新满足依赖的项和到期重试按原catalog位置合入就绪队列。有首次就绪项等待时，最多连续准备3个重试项，再给首次就绪项一次机会；准备完成和远端执行仍可并行。
- 同进程同工程共用4个准备、2个提交、2个领取/发布线程；跨进程共享待收额度。每次调用只等待自己拥有的工作收尾，发布慢不会阻止其他批次完成。`publish(receipt)`可能并发调用，须按原ID幂等、线程安全，成功返回前持久发布科学输出、费用和恢复状态。准备/发布回调不递归执行同工程计划；业务依赖使用after表达。
- 默认窗口覆盖兼容线程并超过128；`ready_tasks`可设1–4096，并发调用须取相同值，通常全部省略。准备中、受理未明及已受理未完成发布/删除的原ID共用额度；发布失败的原ID跨控制器恢复继续计入。接续待恢复目录领取原结果后再补位。工程只提供已授权且真实就绪的工作，不为填队列增加评价或越过依赖。

共享索引保存各输出目录的在途原ID及其状态位置。新控制器在原输出目录无人持有时核对持久状态，归还已退出准备者的未用额度，继续保留受理未明和未发布结果；冷状态读取在工程锁外进行。进程退出由系统释放文件句柄，锁文件本身保留复用。

`output_dir`保存计划、按业务键划分的状态、原请求ID、冻结请求、回执和已领取输出。原ID先持久化，再准备/提交。相同输出目录只能接续同一业务键、声明输入、原ID及依赖计划，变更会在提交前拒绝；准备完成的输入以冻结请求为准。路径表示输入位置，内容身份由业务spec或资产ID声明；接续不重新调用准备函数改写已冻结字节。

收齐顺序为`fetch(delete=False)`、持久回执及输出、业务`publish`、远端`delete`、回收已消费请求包。发布失败保留已领取原结果；接续只重试发布，不重算。提交回应丢失查询同一ID及输入摘要，恢复保持原请求字节。已受理任务消失时保留失败和恢复事实，不创建新科学身份。

远端明确以4xx拒绝，且原ID查询确认为404时，立即归还准备额度，冻结请求保留供同目录接续。受理未明及已有ID身份冲突继续保留原状态；缺失的历史原ID须提供其持久回执后再完成领取和释放额度。

已有ID可声明`{'job_id': original_id}`。若原ID已领取并删除，同时提供`receipt`字典或路径；业务负责该回执引用的持久成果。SDK不重新准备或提交这些原ID。

`stop_when()`返回真、中心关闭新增受理，或Ctrl+C时，管线停止新增，继续收齐已受理任务，再抛`ExecutionPaused(results, remaining)`。恢复使用相同计划和输出目录。执行或发布失败抛`ExecutionFailure(failures, results)`；已有完成项保留。`ExecutionResults`为回执列表，含`skipped`业务键。维护门关闭期间也能领取恢复的原ID。准备、提交和发布阶段的中断同样保留原ID；已领取但发布中断的项留在remaining，接续完成发布后再删除远端回执。

`cancel_when()`返回真后，本次调用停止新增，并沿自己已受理及提交竞态中受理的原ID请求取消；同工程其他调用继续。锁存取消时，提交中与已受理原ID的取消意图先写入状态，POST前再次核对；恢复时仍会完成取消。带取消标志且原ID查询404的项保留remaining，释放未受理额度，保持原ID和请求供业务核对，不重新POST。所有终态回执继续fetch、publish和delete，声明的部分输出与费用保留。主动取消的cancelled回执计入已收齐结果，其他执行失败和发布错误仍抛ExecutionFailure；若有未受理项，抛ExecutionPaused并列出remaining，全部收齐则返回回执。stop_when、维护及Ctrl+C本身不请求取消。业务条件和发布回调须支持并发调用。

## CPU与内存

中心相邻任务派发至少间隔100ms，每个worker的任务首次进程创建也至少间隔100ms。启动等待使用独立互斥与可取消等待，不持全局调度锁；已运行计算、资源协商和结果领取继续并行。同一任务的后续命令不额外等待启动间隔。

调度顺序为：已有弹性任务先取得最多4个有用核；新就绪任务以1核进入；没有可进入的新任务时，剩余兼容核心继续分给已有有用工作。工程份额、优先级、真实能力、RAM及加速器共同参加准入；多节点份额使用兼容设备上的加权整数最大最小分配。

映射执行器在完成事件和最长1秒的等待间隔报告实际在途数与就绪数；中心只回收未被在途计算占用的槽。资源RPC在执行器独立管理线程中进行，已授权输入继续计算。外层并发和内层数值库线程共用一份预算。单个剩余映射输入只保留1核，单个多线程内核在其安全边界取得有用宽度，并登记实际活动核数；嵌套内核沿用外层所有权。

内存使用以下两种声明之一：

```python
python_map('business:evaluate', items, memory_mb=4096)
python_map('business:evaluate', items,
           base_memory_mb=128, memory_per_cpu_mb=768,
           memory_limit_mb=8192)
```

固定`memory_mb`默认为256MiB。模型使用`base_memory_mb + 实际授权核数 × memory_per_cpu_mb`，可选`memory_limit_mb`限制单任务。首个1核槽必须能容纳；内存限制会缩小实际并行宽度。模型预留与实际RSS取较大值参加准入，避免启动或尾项小RSS覆盖有用预留。进程映射完成项退出后，其内存归还；线程共享缓存仍由实际RSS计账。Windows在现有进程采样中读取PrivateUsage，物理RAM与提交容量分别扣除各自尚未落实的预留；固定任务启动期保留声明峰值，稳定采样后使用下述实测包络。已私有提交但暂未驻留的页不再按RSS差额重复预留；主机调度累加worker逐任务计算的未来提交量，某任务超额提交不抵消其他任务的未来需求。设备RAM预算为自动或显式上限，系统保留与实时headroom单独保留。看板RAM U/T为物理用量，Admit为可派发额度。

`memory_mb`是启动调度估计，`memory_limit_mb`是阶段模型的调度上界；二者均不设置进程的OS硬内存限额。每条外部命令启动时按完整估计预留，取得至少2秒、3次有效任务树量测后，固定任务改用近期实测包络。采样间隔至少250ms，保留最近10秒RSS与Windows private各自的峰值；包络取峰值的125%、峰值加32MiB、峰值加最近2秒正增长速率的2秒增量三者最大值，并始终覆盖当前实占。声明的阶段base/per-CPU模型继续保留其已知工作集下界；显式阶段切换和内存调整重新准备启动预留，模型任务仅调整CPU时也按当前模型增减RAM。固定任务仅调整CPU不重新扣留已校正的原申请差额。原请求及其声明量保持，实际调度预留在回执和活动任务中分别记录为`memory_reservation_mb`与`memory_commit_reservation_mb`。

物理准入扣除包络尚未驻留的部分，Windows提交准入扣除提交包络尚未物化为private的部分，均逐任务计算。终态任务退出活动集合，释放CPU及两种预留；排队任务不占运行预留，可用内存为0仍可受理到队列。扩容只使用实际仍保有的预留信用，已回收的原申请额度需重新准入；中心在RPC期间保留回收差额，收到确认后的新worker采样再释放该临时信用保护。

CPU/RAM增长先预留，远端确认后提交；减少确认后归还。同一原ID串行资源变更，网络RPC不持全局调度锁。资源协商明确返回内存不足且保留原授权时，SDK读取原ID的当前授权，按该授权CPU和阶段内存要求计算可用宽度；映射与单次内核在已获足够CPU/RAM时继续执行，并按原间隔重试扩核。原授权不足以覆盖活跃线程、阶段固定内存或一个有用执行单元时，保留资源错误；终态及其他冲突同样直接返回。利用率用于放置与诊断，不把低利用率当作可强抢在途核心的依据。真实有用需求和执行器边界负责释放闲槽。

`resource_share`为`(0,16]`，`single_thread_weight`为`(0,8]`，`priority`为`-1000..1000`。窗口查询的`queue_ratio`为`[0,4]`；默认供给深度让就绪工作持续排队。

同一规划操作复用相同任务形状的能力匹配、RAM计算与分配度数；缓存不跨提供者状态更新。语法解析LRU有界为256条。

## 运行能力

| 能力 | 含义与独立理由 |
| --- | --- |
| `ng47` | ngspice 47数值协议；可按需安装的电路仿真后端 |
| `hsp` | HSPICE后端；独立安装和许可条件 |
| `chisel` | 完整Chisel生成工具链，包含JVM/Scala及RTL生成依赖 |
| `ml` | Torch及图学习环境；依赖体积较大，可在最小节点省略 |
| `cuda` | 可分配CUDA硬件和对应运行环境；与CPU Torch区分 |
| `layout` | GLayout/GDSFactory、Magic、Netgen的版图与后仿工具链 |
| `rtl` | Verilator或Icarus独立RTL仿真工具；已有RTL可直接执行，省去JVM/Scala依赖 |
| `onnx` | ONNX Runtime/OpenCV CPU推理环境；无需完整Torch训练依赖 |

Python/C++为基础环境，不列能力。禁止包组合、设备、OS、工程名或构建后缀。工具链必须在单节点完整，不能跨节点拼凑可用性。Python统一`${tool:python}`，由操作员`python_runtimes`绑定CPU、ML或已授权CUDA解释器；路径和工具argv只在操作员视图及原任务回执保留。

普通工程查询`client.capabilities()`，返回汇总语义能力、CPU/RAM总量和无设备ID的加速器档案。`shared:alias`是明确的数据位置要求，用于`${shared:alias}`；源码、数据及模型优先作为内容资产，不把共享目录名称包装成运行能力。

GPU任务声明`requires=['ml']`及`accelerator={'kind':'cuda', ...}`。默认整卡独占；`sharing:'memory', max_jobs:2, memory_mb:...`表示每任务显存预留，同时受卡容量余量和参与者最小max_jobs限制。CPU回退显式声明`fallback_after_seconds`；无在线兼容GPU时立即回退，排队达到阈值时沿同ID回退。首次派发后执行选择冻结，回执保留请求及实际加速器。

## 任务、内容与结果

公开任务包含`steps/max_cpus/timeout/outputs/env/assets/render/label/priority/requires/memory_mb/project`等业务需求。内存模型保存在`memory_model`，托管函数保存在`execution`。`node/cwd`及机器操作仅供操作员。

| 占位符 | 含义 |
| --- | --- |
| `${tool:python}` / `${tool:ngspice}` | 任务所需环境中的实际工具前缀 |
| `${cpus}` | 本步骤实际授权CPU数，在启动前展开 |
| `${work}` | 本任务输入/输出目录 |
| `${asset:name}` | 引用的不可变内容资产 |
| `${shared:name}` | 操作员登记的共享位置 |
| `${dco}` / `${exe}` | 平台源码目录 / 当前系统可执行扩展名 |

命令、环境及内容引用在受理时冻结；核数在步骤启动时按真实授权展开。映射子进程继承该任务冻结的完整Python启动前缀。`render`仅渲染声明输入文件。`outputs`相对任务目录，支持通配符。普通计算使用独立工作目录，`stdin`为输入包内相对文件。工具链、模型及大数据不随每次源码快照重复复制。

`sync_files(..., content_id=..., job_id=...)`按不可变内容同步。并发准备相同内容共享一次打包和传输，各原ID分别登记引用。SDK管线将原ID传给`prepare`供资产同步使用；未受理准备放弃时释放其引用。已受理和未领取内容持续保留。状态、引用资产和结果在普通磁盘，可重建工作与TMP使用任务专属RAM目录，正常、失败、取消均回收自有临时物。

接受后相同ID和同输入只执行一次。状态查询使用原ID的`statuses/wait_any`或有界分页；普通工程结果去掉设备名、URL、工具路径，保留本任务科学身份、资源、状态、时间、输出和费用。任务日志与声明输出按原ID领取。Windows领取与持久状态使用扩展路径命名空间，覆盖超过260字符的嵌套输出；领取失败保留原ID，修复路径后领取，不重算。

`kill <id>`请求终止任务及整个子进程树，支持完整ID或唯一的至少4位活动任务前缀。原ID、输入身份、声明输出、日志及累计CPU费用保留。运维端重启使用`restart --wait`或`Client.restart(wait=True)`，在排空边界重载同版源码并保留原终端及状态根。

## 门控、断连与回执

`control(accepting=False)`关闭新的工程受理，已受理队列继续派发、执行和领取；原ID幂等查询及操作员运维继续可用。`control(paused=True)`暂停派发，运行中的工作及资源协商继续。新增受理门原子保存到中心状态根，重启后继续生效。维护通常先关闭新增并排空，再暂停派发进行统一部署。`start_paused`在进程调度线程启动前生效；维护完成清除配置并恢复保存的状态。

持久回执损坏按原ID修复：同目录隔离坏副本，保留输入、输出、引用及恢复来源；无可恢复完整回执时记录`failed/error_kind=receipt_corrupt`、未知费用和`cost_incomplete`。冷回执按原ID锁读取，内存最多缓存64份，目录递归操作在全局锁外。

设备60秒无成功握手后退出路由，停止普通任务轮询，保留原任务和授予；核心每60秒握手，最多5次，成功后沿原ID接续。人工停用不自动重连。未确认远端取消保留`remote_cancel_pending/cost_incomplete`，不把未知费用记0。`scan`是操作员显式局域网发现，最多4096地址、32并发，不参与工程设备选择。

任务环境`DCO_CPU_ACCOUNTING=1`启用已有进程组CPU采样，`cumulative_cpu_seconds`等于本任务用户与内核CPU秒；可用硬件频率时积分`Σ ΔCPU秒 × GHz`。来源、覆盖CPU秒和已观察工作量随回执保留，完整覆盖才给完整工作量。重试分项费用和总费用分别保留，缺测保留null。等待及空闲不产生CPU工作量。余额、credits和额度API不参与运行。

## 科研缓存与工程接入

AOP按原始角点及明文执行/测量协议交换；RGCN按完整逻辑观测交换；ARH与Bench沿各自生产者的缓存记录。`sync_cache`先比较清单，再以最多1024条或约1MiB补缺，不复制SQLite/WAL、任务认领或内部共享布局表。缓存键依据科学输入、协议与数据，不生成能力或引擎哈希。工程持久数据库、冻结观察和未验证历史身份继续由所属工程管理。

AOP按阶段选`ng47/hsp`；RGCN按阶段选`ml/ng47/layout`；ARH-DSE按阶段选`rtl/chisel/onnx/ml`；FIBER普通数值使用基础环境，Torch加`ml`，真实SPICE加`ng47`。设备覆盖和实际工具验收见[STATUS](STATUS.md)。各工程使用SDK管理准备与执行，科学输入、seed、评价次数、成本、已付结果和恢复身份跨段连续。

服务启动、设备配置、可见终端、TLS和运维命令见[README](../README.md)。运行预算以设备私有配置为准，实际版本以health和新PID核对。算力校准仅由人工命令触发，启动、重连和采样不运行基准。

启动通过原状态索引恢复任务；索引只保存紧凑身份及共享的资产字符串，终态回执留在原磁盘路径。不可变资产的展开大小保存在有界压缩的元数据索引中，重启不重复遍历每个内容文件；缺失的大小按实际内容补算。无本地监听者的空闲端口直接进入启动，已有监听者仍完成原服务识别。每个状态根的`startup-current.json`记录环境、索引、恢复及资产阶段耗时。

未被活跃任务、未领取回执或生产者引用的资产遵守512MiB暖缓存目标。淘汰先在同一文件系统原子移入私有`.trash`，后台回收实际文件，状态查询、授权和新内容同步继续流动。进程重启后接续回收已退役内容。服务取得状态根独占锁后，将遗留的资产上传目录及本worker临时命名空间中的原ID目录移交同一路径；其他工程命名空间、未知文件、科研回执和未领取成果保留。回收只删除入口自身，不遍历符号链接或目录联接的目标；待回收数量及有界错误保留在health。

## 任务显示规范

当前任务显示`CPU/max`，分别为实际授权和任务声明的max_cpus；排队未授权为0。工程汇总的任务数/CPU用于比较工程负载。Tag最多24终端格，按对象、方法/阶段、必要设置、种子末2或4位、仿真/评价量或预计CPU秒组合最短业务摘要，例如`BGR PSL s26 ev:10k`、`Rocket RTL s01 ~90s`。CPU配置单列，Tag不重复项目列、不放整段或纯hash。显示按实际列宽作语义缩写并保留任务词，完整标签、输入和原ID仍在明细及回执。
