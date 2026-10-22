# DCO 当前状态

更新：2026-10-22（UTC）。版本 0.8.7；实时任务、占用与已加载进程由看板和操作员 `health` 查询。操作见[README](../README.md)，机制见[DESIGN](DESIGN.md)，后续验收见[ROADMAP](ROADMAP.md)。

## 当前预算与能力

| 设备 | CPU预算 / 逻辑线程 | 计算RAM | RAM盘配置 |
| --- | ---: | ---: | ---: |
| Aya1-PC | 16 / 28 | 16GiB | 8GiB |
| Aya1-TX | 24 / 32 | 16GiB | 8GiB |
| ku26s | 52 / 104 | 64GiB | 16GiB |
| Aya1-ITX | 8 / 24 | 总内存减16GiB，约15.8GiB | 8GiB |

四机配置预算按用户确认的16/24/52/8保存，合计100；2026-10-22 ITX重新接入后又由用户正常关机，目前停用，在线三机预算92。原任务及预算保留。ku26s任务TMP目前使用普通磁盘；独立tmpfs列入空闲维护。RAM准入使用进程树RSS及系统实时8GiB余量，未采样派发暂按申报量预留。

| 语义能力 / 工具 | 已验证供给 |
| --- | --- |
| python_numpy、python_scientific、python_torch_pyg、onnx | 已配置四设备；目前在线三机92核，各环境所需包位于实际执行的同一解释器 |
| ng47、Verilator、CIRCT及相应编译器 | 按完整工具组合匹配；GSIM联合要求当前76核，普通任务按各自语义能力准入 |
| layout_python、Magic、Netgen及布局材料 | Aya1-PC、Aya1-ITX环境就绪；ITX关机期间在线16核，材料仍由任务内容资产提供 |
| HSPICE | 当前ku26s环境就绪，52核 |

以上为当前工具供给，不是设备或OS绑定策略。旧工程名和rgcn_fiber仅作原请求/模板兼容，公开能力使用规范环境名；已接受任务保持原绑定和回执。

## 验收与证据入口

- 公平等待：现场复现Bench固定6–7核排队，而刚释放的核被单核任务连续借走。整组积累修复已在三节点排空后加载，最新中心35744，在线92核，远端worker进程保持。Bench旧整组恢复链已停用，首批独立1核/2GiB搜索已在PC/TX/ku运行，源码与参考资产复用；后续RAM按搜索及捕获峰值细化，已受理任务保持原申报。重载后的真实AOP/ARH积压持续补位，RGCN训练已实测阶段8↔1借还。证据见`.local/maintenance/bench-task-shape-20261022/`及`.local/maintenance/fair-queue-20261022/`。
- 单核起步：满载复现固定1核入口申报更大阶段需求仍被连续小任务抢走空槽，已补同样的增长积累保护并加载accdac0。8项隔离HTTP/执行回归覆盖整组积累、优先级、内存、取消、多节点、1→2→4→1借还、四工程3轮补位和满载单核逐步增核；全量459项，455通过、4项环境跳过。调用方保持有用需求，按实际授予建池，串行阶段归还；原任务不改输入。日志见上述目录`single-entry-growth-{before,after,full}.log`。
- 失联设备：60秒移出资源后每60秒重连、最多5次；成功恢复原预算，人工停用保持停用。裸地址add默认26133并复用已保存的HTTPS证书。30项定向、全量470项（466通过、4项环境跳过）完成，覆盖真实TLS、5次上限、跨重启额度、晚到回复、并发配置写及API人工停用；DSH两项持久化意见已复核修正。ku26s经既有SSH映射核对终端/worker正常，公网恢复后原52核已接回，无任务重算。ARH固定cohort在途期间由独立临时监护承接ku26s重连，4项隔离验收通过；中心待自然终态加载，加载后监护自动退出。证据见`.local/maintenance/node-reconnect-20261022/`。原ID取消与费用不完整标记沿用既有恢复规则。
- Tag/ID：补齐Share/Pri布局与线程数量识别，修正纯数字ID黄色、Tag内FACE和数字误染；43项控制台回归含192种真实行组合通过。冗长阶段、实例序号、预算及seed仅压缩显示，原输入保留。

- 慢派发资产交接：隔离夹具复现5秒保护过期后，两次原ID派发均被并发LRU回收打断。新增按原ID暂存引用的补传交接，HTTP和hybrid同链路通过；正常缓存命中不增加请求，旧worker兼容。全量437项、4项环境跳过，其余通过；日志`.local/maintenance/resource-audit-20261022/asset-handoff-full.log`。在线三节点已加载，中心正常终端重开，8条ITX原中断回执保全，三工程维护门已释放；生效证据见本轮`center-final-live-proof.json`。

证据位于既有 `results/repair-20261020/`，完整日志位于 `.local/maintenance/`。表中计数对应各自输入与源码范围，不合并成一次实验。

| 范围 | 当前结论 | 证据 |
| --- | --- | --- |
| 局域网、外网认证与启动配置 | 核心421项：417通过、4项环境跳过；局域网免令牌、外网HTTPS/令牌/证书校验、重复接入保留预算及配置优先级通过 | `.local/maintenance/lan-admission-20261022/full-tests.log`、`access-tests.log` |
| 四机源码及LAN/WAN实测 | 四机统一main源码，设备差异留忽略配置；TX与ITX匿名HTTP200并承接科研，ku26s匿名401、HTTPS及令牌200；中心原进程和在途任务保留 | `.local/maintenance/lan-admission-20261022/` |
| TSA并发与单设备隔离 | 三机完成20个滚动观测点，1787个真实corner；排空尾部独立计时，原生产任务保留、节点恢复；CPU满线程可接近满载 | `results/tsa-pool-scaling-20261021/report.html`、`scaling.csv`、`isolation-ledger.json` |
| ITX部署与真实执行 | 标准worker、24逻辑CPU；科学/神经/ONNX、真实TSA、原生RTL编译运行、SKY130几何及8GiB物理RAM盘均通过；重新接入时科学Python及ngspice实测通过，现已关机、保存8核配置 | `itx-onboarding-current.json`、`.local/maintenance/itx-real-canary/`、`itx-sky130-geometry/`、`.local/maintenance/lan-admission-20261022/live-canaries.json` |
| 八字符设备名与离线节点 | 资源、明细、任务列保留八字符完整名称，兼容宽字符；原44项回归通过。本轮60秒失联机制进一步隐藏残留快照、停止探测，并保留原任务恢复记录 | `tests/test_console.py`、`tests/test_discovery.py`、`.local/maintenance/offline-nodes-20261022/` |
| 自然批次边界重载 | 01:16 UTC中心排空重载，当时在线92核；四项AOP沿原UUID恢复。后续ITX重新接入至100核沿用原中心进程 | `overnight-8h-current.json`、`.local/maintenance/center-reload-20261022-resume-proof.json` |
| 能力、工具模板、缓存消费者及混合版本 | 全量401项：397通过、4项环境跳过；原ID与数值保持 | `.local/maintenance/capability-unification-release-full.log`；`capability-profiles-live-proof.json` |
| 规范能力部署与真实供给 | 三机自然空闲重载，60核授予及真实排队通过；Make联合要求60核 | `capability-idle-reload.json`、`capability-unification-after-reload.json`、`make-unification-admission.json` |
| 启动、独立服务、重载 | 35项：34通过、1项环境跳过；headless与看板开关独立 | `.local/maintenance/headless-startup-lifecycle.log`；`headless-policy-deployment-jobs.json` |
| 高并发客户端传输与恢复 | 26项通过；128并发共享8连接，本地FIFO等待独立于网络timeout，原任务不重复 | `.local/maintenance/client-pool-backpressure-tests.log` |
| 任务终态与迟到资源回执 | 95项通过；失败/取消期间的lease响应保持原结局、费用及资源释放 | `.local/maintenance/lease-terminal-preservation-tests.log` |
| 资源协商期限与丢失回执 | 22项HTTP传输及7项授权/回执回归通过；直连resize与初始lease共用独立期限，超时不自动重发，按原ID确认已生效授予 | `.local/maintenance/resize-ack-20261022/transport-tests.log`、`center-tests.log` |
| 终端与短标签 | 39项通过；表头、多宽度、数字ID颜色及原标签保持 | `compact-task-tags-proof.json` |
| 语义缓存交换 | 80项通过；原生缓存命中、逻辑记录完整及重复同步零导出 | `.local/maintenance/semantic-cache-final.log` |
| 内存准入与迁移 | 实际RSS、阶段CPU/RAM借还、原检查点接续通过 | `production-policy-canaries.json`、`temporary-ku-memory-migration.json`、`ku-training-multicore-live.json` |
| Windows/Linux生命周期 | 可见终端、防自动休眠、进程树回收、端口所有权及原ID恢复通过 | `maintenance-reload-state.json`、`live-linux-power-after-reload.json`、`tx-wake-verified.json` |
| 跨工程真实执行 | AOP角点、RGCN训练/物理链、ARH原生块与Bench科学数组均有实测；科研结论归所属工程 | `aop-replica-placement-audit.json`、`arh-native64-warm-throughput.json`、`rgcn-layout-pc-readiness.json`、`cli-web-parity-current.json` |
| 人类约束与独立交叉审查 | 最新确认及逐项源代码核对已保存 | `design-intent-confirmed.json`、`dsh-cross-check-current.json` |

## 当前维护边界

2026-10-22资源审计已完成四节点120秒采样：本机物理上传均值0.098MiB/s；另一次10秒ETW记录DCO向ku约1.34MiB/s的任务传输突发，未复现持续5MiB/s。Windows节点磁盘健康均正常。4秒零丢事件写入归因发现AOP BGR协调SQLite进程约5.15MiB/s、DCO自身约0.10MiB/s；AOP调用层由其负责人处理，不改在途科学输入。

中心约7万份终态记录引起内存膨胀和调度全历史扫描，源码已改为终态按需读取及活跃索引。原70055份磁盘回执的只读装载实测新增RSS25.91MiB，完整历史常驻缓存0，原记录无改写。终态最近缓存上限64；`temp_root:null`恢复RAM默认，新建库缓存集中DCO。具体节点迁移、重载生效及最终回归记录见`.local/maintenance/resource-audit-20261022/`和维护账本。

中心已完成2026-10-22多次自然边界维护，加载源码和各节点进程以维护证据及实时健康信息为准；当前三机在线预算92核，ITX停用。原输入、回执、成本、检查点和未领取成果保留；后续科研进度沿原ID记录在`overnight-8h-current.json`。八小时全局截止已按用户新指令解除，各实验保留原科学预算。

全量历史列举仅锁定ID快照，逐项复制不占批次全局锁；SDK初始lease与直连resize共用独立资源协商期限。本轮中心已加载这些修复，原ID和已付结果继续保留。验收与生效范围记录在上述任务账本。

四科研工程默认Web端口为8000/8001/8002/8003，CLI与Web生命周期及当前研究分别由各工程README/STATUS说明。已完成入口验收与后续运行关联记录在 `cli-web-parity-current.json`；当前任务数量直接从服务查询。

GPU已具备静态档案、每卡容量匹配、整卡独占与显式CPU回退合同；真实GPU计算和Linux独立tmpfs的后续验收见ROADMAP。详细历史实测保留在原证据目录及Git，不在本页逐轮复述。
