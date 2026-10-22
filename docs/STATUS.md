# DCO 当前状态

更新：2026-10-22（UTC）。版本 0.8.7；实时任务、占用与已加载进程由看板和操作员 `health` 查询。操作见[README](../README.md)，机制见[DESIGN](DESIGN.md)，后续验收见[ROADMAP](ROADMAP.md)。

## 当前预算与能力

| 设备 | CPU预算 / 逻辑线程 | 计算RAM | RAM盘配置 |
| --- | ---: | ---: | ---: |
| Aya1-PC | 16 / 28 | 16GiB | 8GiB |
| Aya1-TX | 24 / 32 | 16GiB | 8GiB |
| ku26s | 52 / 104 | 64GiB | 16GiB |
| Aya1-ITX | 8 / 24 | 总内存减16GiB，约15.8GiB | 8GiB |

四机在线CPU预算合计100，按用户确认的16/24/52/8保存；2026-10-22 ITX已按新指令重新接入并实际执行科研任务。调低额度保留在途任务并自然收敛。ku26s任务TMP目前使用普通磁盘；独立tmpfs列入空闲维护。RAM准入使用进程树RSS及系统实时8GiB余量，未采样派发暂按申报量预留。

| 语义能力 / 工具 | 已验证供给 |
| --- | --- |
| python_numpy、python_scientific、python_torch_pyg、onnx | 四设备合计100核；各环境所需包位于实际执行的同一解释器 |
| ng47、Verilator、CIRCT及相应编译器 | 按已登记工具匹配；包含make的ARH联合要求覆盖100核 |
| layout_python、Magic、Netgen及布局材料 | Aya1-PC、Aya1-ITX环境就绪，合计24核；材料仍由任务内容资产提供 |
| HSPICE | 当前ku26s环境就绪，52核 |

以上为当前工具供给，不是设备或OS绑定策略。旧工程名和rgcn_fiber仅作原请求/模板兼容，公开能力使用规范环境名；已接受任务保持原绑定和回执。

## 验收与证据入口

- 慢派发资产交接：隔离夹具复现5秒保护过期后，两次原ID派发均被并发LRU回收打断。新增按原ID暂存引用的补传交接，HTTP和hybrid同链路通过；正常缓存命中不增加请求，旧worker兼容。全量437项、4项环境跳过，其余通过；日志`.local/maintenance/resource-audit-20261022/asset-handoff-full.log`。现有忙碌服务继续运行，待自然空闲边界加载。

证据位于既有 `results/repair-20261020/`，完整日志位于 `.local/maintenance/`。表中计数对应各自输入与源码范围，不合并成一次实验。

| 范围 | 当前结论 | 证据 |
| --- | --- | --- |
| 局域网、外网认证与启动配置 | 核心421项：417通过、4项环境跳过；局域网免令牌、外网HTTPS/令牌/证书校验、重复接入保留预算及配置优先级通过 | `.local/maintenance/lan-admission-20261022/full-tests.log`、`access-tests.log` |
| 四机源码及LAN/WAN实测 | 四机统一main源码，设备差异留忽略配置；TX与ITX匿名HTTP200并承接科研，ku26s匿名401、HTTPS及令牌200；中心原进程和在途任务保留 | `.local/maintenance/lan-admission-20261022/` |
| TSA并发与单设备隔离 | 三机完成20个滚动观测点，1787个真实corner；排空尾部独立计时，原生产任务保留、节点恢复；CPU满线程可接近满载 | `results/tsa-pool-scaling-20261021/report.html`、`scaling.csv`、`isolation-ledger.json` |
| ITX部署与真实执行 | 标准worker、24逻辑CPU；科学/神经/ONNX、真实TSA、原生RTL编译运行、SKY130几何及8GiB物理RAM盘均通过；当前8核在线，重新接入后科学Python及ngspice实测通过 | `itx-onboarding-current.json`、`.local/maintenance/itx-real-canary/`、`itx-sky130-geometry/`、`.local/maintenance/lan-admission-20261022/live-canaries.json` |
| 八字符设备名与离线节点 | 资源、明细、任务列保留八字符完整名称，兼容宽字符；针对性回归44项通过，运行看板已加载。禁用且排空的节点停止后台探测并隐藏，仍在排空的任务保持可见 | `tests/test_console.py`、`tests/test_center.py`、`.local/maintenance/center-reload-20261022-proof.json` |
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

中心已于2026-10-22 01:16 UTC在自然批次边界加载源码`4bb0301`，此后保留原进程；当前四机接单预算100核。ITX与TX分别在自身空闲边界加载LAN认证修复，其他节点任务继续。原输入、回执、成本、检查点和未领取成果保留；后续科研进度沿原ID记录在`overnight-8h-current.json`。八小时全局截止已按用户新指令解除，各实验保留原科学预算。

新补修复已加入源码：全量历史列举仅锁定ID快照，逐项复制不占批次全局锁；SDK初始lease与直连resize共用独立资源协商期限。SDK增量同步供新进程使用，中心的列表修复留后续空闲启动加载，在途科研继续。验收与生效范围继续记录在上述任务账本。

四科研工程默认Web端口为8000/8001/8002/8003，CLI与Web生命周期及当前研究分别由各工程README/STATUS说明。已完成入口验收与后续运行关联记录在 `cli-web-parity-current.json`；当前任务数量直接从服务查询。

GPU已具备静态档案、每卡容量匹配、整卡独占与显式CPU回退合同；真实GPU计算和Linux独立tmpfs的后续验收见ROADMAP。详细历史实测保留在原证据目录及Git，不在本页逐轮复述。
