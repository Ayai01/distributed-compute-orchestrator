# DCO 当前事实

- 当前源码版本：**0.11.23**；各服务实际运行版本及空闲重载状态见维护入口。
- 资产缓存首次统计及恢复索引统一使用Windows扩展路径，深层输入解包后可正常登记大小并进入执行；原资产字节、请求及科学输入保持。复现、42项定向检查和原生空闲上线状态见`.local/maintenance/deep-asset-startup-20261111/current.json`。
- 内存警戒先限制新增并发并在安全边界等待，持续真实临界或耗尽才保护性终止；可重入SDK计算的资源恢复独立于retries，沿原ID有界退避并保留已成功map项。验证、连续中断及部署状态见`.local/maintenance/resource-recovery-20261109/current.json`。
- SDK收集保持每工程2线程；本地状态记录排队、ZIP获取/解包、发布及删除分项，原回执保持。ZIP准备轮询间隔最多0.5秒。已有批次积压审计、隔离验证及SDK源码部署见`.local/maintenance/collection-latency-20261109/current.json`，新调用进程读取当前SDK。
- worker在准备命令与正文的切换处，容量调整被明确拒绝并保留原授权时，继续使用原CPU/RAM槽位，不将资源暂时不足记为业务失败。源码、隔离三步入口回归及实际生效状态见`.local/maintenance/automatic-grant-ram-20261109/current.json`。
- SDK提供`python_map/map_items(item_max_cpus=...)`与子项`run_call`阶段授权；父租约保证完整lane优先及长尾内核扩展。按SDK签名及源码revision识别该接口；交付、隔离保存输入0FE验证和五台源码部署入口为`.local/maintenance/nested-item-cpu-20261109/current.json`，服务引擎无需重载。
- 固定数值核入口在首次授权不足时，保留未满足需求并在启动前有界复核一次；冷内存单槽探测后可取得空闲核心，忙设备仍按实际授权执行。隔离复现、回归和SDK实际分发状态见`.local/maintenance/kernel-entry-grant-20261110/current.json`；在途固定线程原生核继续原宽度。
- 确定性执行前资产引用拒绝的零CPU回执机制、历史原ID独立结清证明及维护生效状态见`.local/maintenance/pre-execution-accounting-20261110/current.json`；原历史回执及科学输入保持。
- lease转发TLS意外EOF的已确认授权保留、未知空槽限制、隔离HTTP注入验证及SDK分发状态见`.local/maintenance/forwarded-lease-eof-20261110/current.json`。原已完成子项与累计CPU费用保留；在途进程继续使用其已导入SDK。
- 已校准准备阶段切换到较大内存阶段时，SDK在启动正文前有界等待首槽授权；验证、原ID费用审计及实际分发状态见`.local/maintenance/phase-admission-20261110/current.json`。
- SDK阶段降需求保留原单槽探测，资源等待服从原期限，实际标定沿已确认阶段记录；MAC八个原失败ID、费用审计、HTTP复现及SDK分发见`.local/maintenance/phase-downshift-20261110/current.json`。
- 固定线程的顶层`run_call`可用阶段`exact_cpus`及`start_timeout`等待完整真实授权，再启动一次正文；等待与HTTP协商共用启动窗口，原执行期限保持。隔离检查及SDK分发状态见`.local/maintenance/exact-kernel-start-20261110/current.json`。
- CPU U/B按已授权/基础预算（系统逻辑线程）显示；动态准入容量单独保留在API、nodes明细及cpus回执。源码验证和各窗口实际重载状态见`.local/maintenance/cpu-budget-label-20261109/current.json`。
- 终端统一使用`kill <id|all>`，补全ID统一青色；`clean`删除未启动排队任务及其记录、临时物和独占缓存。源码验证、部署及窗口实际重载状态见`.local/maintenance/kill-all-20261109/current.json`。
- hybrid同时提供调度中心和进程内本机计算，看板标题为“混合模式”；API逻辑角色为scheduler，操作员明细另给出mode。进程内worker不另开服务端口。
- 私有远程展示的独立发布器源码为1.0.3；`cpu_total`为固定逻辑线程总量，`cpu_base`为基础预算，`cpu_capacity`为动态准入，`cpu_max_capacity`为按实际倍率计算的最大超发容量。节点内存提供整机物理已用、任务树RSS、原阶段申报、校准预留、预算保留及实际物理警戒线。CBOR+gzip传输、语义合同、验证及实际发布状态见`.local/maintenance/display-transport-20261109/current.json`和`contract.md`。原展示归属和入口见`.local/maintenance/liveboard-20261108/`。任务比例只使用显式进度，缺失的历史完成数及中心uptime为null。
- 当前操作员`01a1a83d-ce20-78e2-bc01-13eba8bca9d6`；维护及放行状态：`.local/maintenance/automatic-contract-20261104/current.json`，与实际health交叉核对。维护入口保存实际运行版本、源码部署、原状态恢复及受理/派发状态；兼容更新按设备自然空闲重载，原ID继续执行和领取。
- 普通客户端生产缓存引用入口为`Client.cache_ref`和任务`cache_ref`；运行中心已公开`cache_references`协议能力，普通客户端可取回已迁入的C910成功生产引用，node/platform权限保持。隔离验证、原ID领取及费用核对、worker进程保持和实际生效证据见`.local/maintenance/cache-reference-api-20261110/current.json`。
- 使用规范：[README](../README.md)；接口及机制：[DESIGN](DESIGN.md)；协作要求：[AGENTS](../AGENTS.md)。

## 能力与环境

运行能力为`ng47、hsp、chisel、ml、cuda、layout、rtl、onnx`。Python/C++是默认基础环境。Chisel及layout按单节点完整工具链登记，具体工具、路径及argv保留在操作员明细和本任务回执中。

普通任务禁止指定platform、node和外部cwd；普通能力与准入查询不接受平台筛选。操作员调试保留这些放置功能。工程目标线程数不再作为准入、派发和扩核门槛，同级新任务按占用线程数及启动宽度排序。验证、实际上线和接口审计入口：`.local/maintenance/project-admission-policy-20261108/current.json`。

Python任务统一使用`${tool:python}`。普通科学计算使用cpu后端，Torch任务声明ml，取得CUDA授权后使用cuda后端。设备私有python_runtimes维护完整解释器前缀，项目不选择解释器或设备。新算法补齐所用环境的依赖，禁止扩充Python组合后缀。

通用C++构建使用`${tool:cxx}`。GNU/ELF原生扩展使用`requires=['gxx']`和`${tool:gxx}`；该逻辑工具表达GNU C++17、ELF64目标与原生扩展ABI需求，作为可选构建工具登记。当前实际GNU编译、ELF64/x86_64执行及普通SDK路由资格见`.local/maintenance/gxx-contract-20261107/current.json`；执行能力大类保持上述八项，Python后端沿原语义选择。

2026-11-05现场配置：五个提供者合计182个逻辑线程预算；五台CPU/ML环境、两台CUDA环境已完成逐项依赖、数值接口及最小计算验收。实际预算以运行配置为准。环境、路由和源码部署的原ID回执保存在`.local/maintenance/layout-ku26s-20261104/`，当前维护状态使用页首入口。

## 工程需求与设备覆盖

| 工程 | 按阶段声明的能力 | CPU执行覆盖 |
| --- | --- | --- |
| AOP | ngspice仿真使用`ng47`；HSPICE仿真使用`hsp` | ng47为5/5；hsp为ku26s |
| RGCN | 数据/代理/训练使用`ml`，原生测量加`ng47`，版图与后仿阶段加`layout` | 5/5 |
| ARH-DSE | RTL仿真使用`rtl`，Chisel生成加`chisel`，ONNX推理加`onnx`，Torch执行加`ml` | 5/5 |
| FIBER | 普通数值使用基础环境，Torch算法加`ml`，真实SPICE任务加`ng47` | 5/5 |

表中能力按实际阶段选择，不把所有需求合并为每个任务的硬条件。CUDA计算覆盖Aya1-PC、Aya1-TX两台，当前兼容CPU预算54；显式CPU回退仍可使用全部五台。HSPICE只在ku26s配置，其他设备使用ng47。Python/C++、ng47、chisel、ml、rtl和onnx均覆盖五台。

`layout`覆盖五台，当前合计182线程预算。ku26s使用独立GLayout/GDSFactory环境、Magic 8.3.684及Netgen 1.5.323；已验证真实SKY130材料的GDS生成与读回、NMOS/PMOS提取、寄生电容提取及LVS正反例。PDK由任务内容资产提供，环境不保存实验副本。

## 调度与任务

弹性任务先取得最多4个有用核，新任务随后以1核进入，剩余兼容核心用于已有任务扩核。一般搜索上限1024；有限种群按真实并行宽度限定。默认准备窗口超过128并覆盖兼容CPU数；工程以真实就绪工作持续补位。

公开任务省略cpus，只声明max_cpus及能力/内存；DCO通过python_map/python_call和client.execute管理准备、排队、实际并发、扩缩核和持久收齐。映射完成项退出进程并释放内存，线程后端保留共享缓存。after等待持久发布，Deferred/Skipped管理业务门。原ID接续及已付结果保持。所有工程使用标准语义能力，设备选择和共享服务修改归DCO操作员。

map_items、iter_items及run_call的limits按阶段声明有用CPU上限和固定或线性内存需求。映射首项启动前协商阶段内存，随后按真实在途/就绪需求扩缩；父任务模型保留为未覆盖阶段的默认值。

整个运输及其子进程共用执行期限。业务在评价与记账边界使用remaining_time/check_deadline，为原时限内的失败包及已付前缀发布留出时间。映射完成结果与逐项状态分别持久化，硬超时/取消后保留完成值并标明中断及未启动项；未知付费量保持未知。维护与验证入口为`.local/maintenance/execution-deadline-20261106/`。

固定内存估计在有效采样后按任务树近期RSS/private峰值和增长余量校正；原声明、实际物理占用、提交量与调度包络分开记录。Windows文件映射RSS不重复计为private提交。显式阶段模型保留已知峰值，模型任务的CPU增减同步调整RAM；固定任务仅调整CPU不重新扣留已回收差额；线性模型按稳定任务树实测与实际活跃宽度校正扩容预留。初次准入支持有资金的冷探测，实际RAM或提交余量危险时按低优先级及当前占用终止任务树，保留原ID、部分输出与费用。零内存余量仍接受排队，成功、失败、取消及超时释放运行预留。当前证据入口：`.local/maintenance/observed-memory-20261106/`。

单一活跃宽度的线性内存标定按完整实测包络筹资每个槽位，取得不同宽度样本后再分解共享基线及增量。多活跃槽的冷探测按完整活跃宽度筹资；已知实际RSS/private超过中心预算时保留原授权并拒绝本轮调整。申报估计与每槽冷探测上限保持软约束，压力保护及资源恢复使用上述当前维护入口；标定与分项费用证据见`.local/maintenance/actual-memory-retry-20261108/current.json`。

SDK在资源调整被暂时拒绝时保留原授权内已完整筹资的宽度；相同冻结父阶段的已有活跃工作不被冷首槽退路缩为1核，初始未启动阶段保留已筹资的单槽探测。polling按当前worker尝试ID读取授权。查询暂时故障时，同阶段沿用有效授权；调整结果未明时仅保留活跃底线，首次授权及新阶段有界重读并服从原期限。DELETE按原ID幂等确认，丢响应后先核对，必要时仅补一次删除。授权、真实HTTP读超时、期限及执行边界39项定向检查和原进程保持的SDK分发证据见`.local/maintenance/lease-status-outage-20261108/current.json`；DELETE及既有宽度边界证据见`.local/maintenance/delete-and-running-lease-20261108/current.json`。


资源POST回复丢失或超时后，SDK先核对当前执行尝试，等待未决租约结束，再有界重试同一资源调整；终态、权限及永久资源错误直接返回，原期限和科研重试预算保持。真实丢响应、原ID恢复及查询故障的14项隔离检查通过，记录见`.local/maintenance/project-admission-policy-20261108/resource-reconciliation.json`。

同工程独立output_dir在同进程或不同进程中并发execute，共享待收窗口；同进程还共享4/2/2准备、提交、领取线程。工程锁只保护共享索引的短读改写，每个输出目录保留恢复所需的单写入者。准备进程退出时归还未用额度，发布失败的原ID跨进程恢复继续占用待收额度。并发调用保持相同ready_tasks及原project。

stop_when只停止新增并排空；cancel_when取消本次调用的在途原ID，继续发布部分结果与费用，覆盖提交回应丢失和中断后的原ID排空。明确拒绝且未受理的请求立即释放窗口额度。

`kill <id|all>`统一终端任务终止入口，支持完整ID或唯一的至少4位活动任务短ID；补全中的任务ID统一为青色。`all`取消执行时所连接端点的全部排队及运行任务，返回完成及待确认原ID。新提交、已结束任务和受理/暂停设置保持。SDK及HTTP取消接口保留。原ID、输入身份、声明输出、日志及累计费用继续保留，领取后按任务生命周期清理。

## 验证与证据

- 当前源码的全量核心/原生基线及定向复验累计覆盖903项，899通过、4项按平台跳过；实际ngspice与Verilator通过。本轮198项相关检查中197通过、1项按平台跳过，最后43项定向核验通过。当前超发、启动间隔、发布与核验入口见`.local/maintenance/cpu-overcommit-20261108/current.json`；三步顺序见`.local/maintenance/strict-order-20261108/current.json`；显示链路与独立实测见`.local/maintenance/ram-pair-phase-20261108/current.json`；全量基线日志见`.local/maintenance/preflight-01115-20261108/validation.json`。覆盖4核优先与空闲扩核、54/1024上限、CPU超发及90%门槛、普通/超发任务0.5/1秒启动间隔、共享内存与实际并行增量、固定/线性校正、冷探测准入、真实RAM/commit压力终止、原ID及付费前缀、超时续行、独立失败隔离、原ID恢复、kill与取消，以及Tag与RAM实/报的窄栏显示。
- Windows长路径贯穿成果枚举、输入保留、领取和原ID删除，保留链接自身的清理边界；26项文件生命周期检查通过，日志`patch-01101-long-output-lifecycle-final.log`。
- 托管映射、数值内核、内存模型、线程共享缓存、依赖发布、非阻塞准备、维护/Ctrl+C排空和原ID恢复均有隔离检查。
- 输入提示与硬件光标位于同一可编辑行，底部空白保护行保留；状态刷新与窗口缩放保持输入位置。70项终端检查及Windows原生离屏控制台验证见`.local/maintenance/prompt-cursor-20261108/`。
- 部署、现场执行、实际版本/PID/状态根与原状态恢复以本次维护目录`current.json`及对应证明为准。
- 能力与环境原生资格：`.local/maintenance/layout-ku26s-20261104/`中的`layout-qualify-final.json`、`admission-after.json`及逐设备现场验收；CPU/ML/CUDA环境记录见`.local/maintenance/capability-contract-20261104/`。
- 集中本机DSH Flash/max审查的原回执和Owner裁定保存在当前维护目录。规划性能重放见`.local/maintenance/lease-scale-20261104/`：352个真实任务、27次规划，授权与资源逐项相同，耗时降低5.60倍。
- 194006个原ID、706份资产的同索引回放保持身份摘要一致；紧凑加载的`tracemalloc`峰值78.01MiB，完成加载后54.52MiB。不可变资产大小索引避免重复遍历内容树，原子退役和后台回收保留未领取回执及其引用；实际启动分阶段耗时保存在各状态根`startup-current.json`。

科学计划、输入、评价及恢复账本由所属工程维护。源码变化由Git记录，正式文档说明当前事实、要求及设计。
