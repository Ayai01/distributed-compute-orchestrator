# DCO 当前状态

更新：2026-10-23（UTC）。源码版本0.8.13。运行入口见[README](../README.md)，机制见[DESIGN](DESIGN.md)，历史见[HISTORY](HISTORY.md)。实时任务、设备预算和已加载版本由操作员小型health/capabilities及看板查询，不把一次采样的核数写成长期配置。

## 设备与能力

192.168.1.11为Aya1-ITX，已由用户重新启用。ITX设备16线程曾受中心遗留limit=8截断，已将该路由上限恢复为自动；配置和在线中心均已更新，实测capacity/reserved/used均达到16。PC、TX与ITX的Windows能力页一致：`ng47 RTL Chisel CIRCT C++ ONNX Py ML Layout bison flex m4 make`；ku26s保留其Linux工具差异，按完整语义能力组合准入。

四节点已有科学Python、PyG、ONNX、SPICE及RTL实测。Windows版图环境复用已有安装，ku26s额外提供HSPICE。能力不携带引擎哈希，不按设备或OS限制科研路由；旧别名仅服务原请求兼容。证明见`.local/maintenance/continuous-supervision-20261023/capability-alignment-proof.json`与`state.json`。

本机DCO引用RGCN的Windows EDA/Cygwin与ARH的ONNX环境，作为共享在用依赖维护；协调归DCO，科研源码、模型与观测归各工程。沿原路径复用，不复制出另一套环境。

Windows Magic GDS入口已修复：ITX/TX的通用Tcl入口在最小矩形写出时异常退出，改为同一安装的`magicdnull.exe`原生批处理入口；TX同时修正安装目录对应的`CAD_ROOT`及在线/持久工具绑定。PC原入口正常。三台用原58.53µm MiM脚本及冻结PDK技术文件均写出1,355,458字节GDS，独立读取为1cell/21,176polygons且边界相同；本次0SPICE。证据见`.local/maintenance/continuous-supervision-20261023/magic-gds-repair.json`，能力验收已补Magic写出后的独立GDS读取。

## 本轮实现与验证

- 0.8.13补齐POSIX终端挂断及SIGTERM的正常退出路径；ku26s隔离实测旧实现终端挂断后远端任务仍为running，修复后真实PTY挂断会取消原任务、保留回执并释放状态锁。ku26s启动回归16项（13通过、3项Windows环境跳过，3.492秒）；源码已同步四节点，在线科研服务继续保留到空闲边界。
- 0.8.12补齐Windows关窗退出：旧实现原生关窗后，隔离远端任务仍为running；修复后，有看板和no-dashboard两种真实控制台关窗均取消原远端任务、持久化原ID回执并释放状态锁。启动与生命周期共34项（33通过、1环境跳过）。当前中心保留原在途任务，先由独立操作看板连接；用户已选择空闲后迁移服务核心，实际迁移状态见维护`state.json`。
- 0.8.11修复ConPTY把可见聊天/编辑器主窗口误当作服务终端的问题，只接受本终端祖先链中的真实可见终端宿主。另在真实Codex工具PTY中确认拒绝启动，独立经典控制台窗口确认可见。
- 0.8.10合并SDK同内容并发准备，8个请求只打包/摘要/上传一次，各原ID分别持有交接引用；缓存缺失不重复查询。`fetch(output_dir=...)`直接解包到目录，省去全部文件的重复内存读取。Bench冻结原ZIP直接上传，每包客户端两次打包/三次摘要缩为各一次，原内容身份保持。
- 0.8.9修复强制校准被缓存截住、`cpus 0`拒绝自动预算、CLI只显示设置值而隐藏中心截断。校准缓存最多16项，显式强制请求每次执行；CPU回执显示设备配置、中心上限和有效预算。
- 同标签耗时只保留最近256项，精确标签仍用于同工作量估算；普通节点快照仅报告条目数量。旧运行实例曾累计2006项，约占146KB节点响应的87%。
- 0.8.8将大文件I/O移出调度全局锁，按原任务ID交接临时资产，结果领取与删除共享单任务锁；8条传输连接内保留控制通路，TLS握手不堵接收循环。现有原ID、冻结输入和未领取结果继续使用。
- 新动态输入使用`sync_files(..., job_id=原ID)`，提交成功转为任务引用，放弃准备时`release_assets(原ID)`；静态共享源码/模型继续复用。ARH/RGCN/Bench的在改适配文件由现有负责人接入，避免并行覆盖。
- AOP的TSA宽度下限、未引用资产回收以及Bench返回包内存峰值已由本轮直接整改，具体回归与修改归各自工程状态。

0.8.10完整497项（493通过、4环境跳过，257.972秒）及39模块Python3.7语法检查通过；Bench传输87项（86通过、1环境跳过）。0.8.8/0.8.9完整回归与Linux生命周期/HTTP证据沿用原日志。日志与各节点实际加载记录保存在`.local/maintenance/continuous-supervision-20261023/`；部署只在真实空闲边界加载，不中断科研任务。

## 存储与收尾

运行状态只索引活跃任务，终态回执按原ID读磁盘，内存最近缓存64份。ARH已保存结果却未删除远端原任务是当前终态目录积压的主要来源；负责人正在补齐持久化后清理和历史批次收束。完成的清理不重复运行。

科研观测、模型、分区、原ID与费用保留一份有效实体；临时运输包、无消费者的旧生成物和重复副本按实际引用回收。统计扣除硬链接重复计数，同卷改名按文件ID核验，不额外全量hash。旧8.87GiB编译物迁移、Bench约550GiB实际空间回收和AOP模型硬链接已有独立证据，分别见`.local/maintenance/storage-audit-20261022/`、`quarantine-cleanup-20261023/`及AOP的STATUS，不重跑这些批次。

## 维护入口

现有维护策略、负责人、会话游标及部署状态统一保存在`.local/maintenance/continuous-supervision-20261023/policy.json`和`state.json`。本会话负责DCO、算力调度和跨工程接口；AOP_v2、FIBER-MO、ARH-DSE、RGCN继续由现有负责人推进科研。消息只发具体增量与证据，不重复长上下文；不设全局科研截止。

GPU实际计算与Linux独立tmpfs验收仍按[ROADMAP](ROADMAP.md)推进；旧完整历史和原始实测保留在原证据目录与Git。
