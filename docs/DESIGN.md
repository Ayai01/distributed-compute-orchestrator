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

在DCO任务内，业务入口可调用：

```python
from remote_compute import map_items, run_call
values = map_items('business:evaluate', ready_inputs, kwargs={'mode': 'native'})
result = run_call('business:train', config=config)
```

`map_items`可提供`on_result(index, value, error)`，逐项持久化业务检查点；`python_map`在`.dco-results.json`写入已完成项及最终状态。直接任务声明的返回值须可JSON序列化，大数组、模型及科学记录写声明输出，返回其必要索引。映射项内部的嵌套调用共用该项的1核，不额外借核。

## 持久准备与收齐

`Client.execute(requests, project=..., output_dir=..., prepare=None, publish=None, ready_tasks=None, stop_when=None)`同步执行有限业务计划，直到已受理原ID收齐。一个工程只使用一个管线和pending计数。工程级锁位于调用目录`.local/dco-client/`。

- `requests`为业务键到请求的映射，或`(key, spec)`序列；业务键非空且唯一。
- 业务请求为`{'task': task, 'inputs': directory, 'files': mapping}`，也可由`prepare(spec, original_id)`返回。依赖包装为`{'spec': business_input, 'after': [keys]}`。依赖图在受理前校验。
- `after`等待前序结果完成持久发布，之后才准备后续输入。独立任务继续准备和执行。
- `Deferred(reason)`表示输入暂未就绪，立即归还准备线程；发布事件或有界间隔触发重试。`Skipped(reason)`表示业务门已决定跳过，在结果的`skipped`中登记，不生成科学回执。
- 准备、提交、领取/发布使用独立线程组，发布慢不会堵住独立输入的准备。`publish(receipt)`可能并发调用，须按原ID幂等、线程安全，成功返回前持久发布科学输出、费用和恢复状态。
- 默认窗口覆盖兼容线程并超过128；`ready_tasks`可设1–4096。工程只提供已授权且真实就绪的工作，不为填队列增加评价或越过依赖。

`output_dir`保存计划、按业务键划分的状态、原请求ID、冻结请求、回执和已领取输出。原ID先持久化，再准备/提交。相同输出目录只能接续同一业务键、声明输入、原ID及依赖计划，变更会在提交前拒绝；准备完成的输入以冻结请求为准。路径表示输入位置，内容身份由业务spec或资产ID声明；接续不重新调用准备函数改写已冻结字节。

收齐顺序为`fetch(delete=False)`、持久回执及输出、业务`publish`、远端`delete`、回收已消费请求包。发布失败保留已领取原结果；接续只重试发布，不重算。提交回应丢失查询同一ID及输入摘要，恢复保持原请求字节。已受理任务消失时保留失败和恢复事实，不创建新科学身份。

已有ID可声明`{'job_id': original_id}`。若原ID已领取并删除，同时提供`receipt`字典或路径；业务负责该回执引用的持久成果。SDK不重新准备或提交这些原ID。

`stop_when()`返回真、中心关闭新增受理，或Ctrl+C时，管线停止新增，继续收齐已受理任务，再抛`ExecutionPaused(results, remaining)`。恢复使用相同计划和输出目录。执行或发布失败抛`ExecutionFailure(failures, results)`；已有完成项保留。`ExecutionResults`为回执列表，含`skipped`业务键。维护门关闭期间也能领取恢复的原ID。准备、提交和发布阶段的中断同样保留原ID；已领取但发布中断的项留在remaining，接续完成发布后再删除远端回执。

## CPU与内存

调度顺序为：已有弹性任务先取得最多4个有用核；新就绪任务以1核进入；没有可进入的新任务时，剩余兼容核心继续分给已有有用工作。工程份额、优先级、真实能力、RAM及加速器共同参加准入；多节点份额使用兼容设备上的加权整数最大最小分配。

映射执行器在完成事件和最长1秒的等待间隔报告实际在途数与就绪数；中心只回收未被在途计算占用的槽。资源RPC在执行器独立管理线程中进行，已授权输入继续计算。外层并发和内层数值库线程共用一份预算。单个剩余映射输入只保留1核，单个多线程内核在其安全边界取得有用宽度，并登记实际活动核数；嵌套内核沿用外层所有权。

内存使用以下两种声明之一：

```python
python_map('business:evaluate', items, memory_mb=4096)
python_map('business:evaluate', items,
           base_memory_mb=128, memory_per_cpu_mb=768,
           memory_limit_mb=8192)
```

固定`memory_mb`默认为256MiB。模型使用`base_memory_mb + 实际授权核数 × memory_per_cpu_mb`，可选`memory_limit_mb`限制单任务。首个1核槽必须能容纳；内存限制会缩小实际并行宽度。模型预留与实际RSS取较大值参加准入，避免启动或尾项小RSS覆盖有用预留。进程映射完成项退出后，其内存归还；线程共享缓存仍由实际RSS计账。

CPU/RAM增长先预留，远端确认后提交；减少确认后归还。同一原ID串行资源变更，网络RPC不持全局调度锁。利用率用于放置与诊断，不把低利用率当作可强抢在途核心的依据。真实有用需求和执行器边界负责释放闲槽。

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

接受后相同ID和同输入只执行一次。状态查询使用原ID的`statuses/wait_any`或有界分页；普通工程结果去掉设备名、URL、工具路径，保留本任务科学身份、资源、状态、时间、输出和费用。任务日志与声明输出按原ID领取。

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
