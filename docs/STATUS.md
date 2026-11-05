# DCO 当前事实

- 当前源码版本：**0.11.2**。
- 当前操作员`01a1a83d-ce20-78e2-bc01-13eba8bca9d6`；维护及放行状态：`.local/maintenance/automatic-contract-20261104/current.json`，与实际health交叉核对。当前维护入口保存实际运行版本、统一部署、原状态恢复及放行事实；维护门关闭新增工程受理，原ID继续执行和领取。
- 使用规范：[README](../README.md)；接口及机制：[DESIGN](DESIGN.md)；协作要求：[AGENTS](../AGENTS.md)。

## 能力与环境

运行能力为`ng47、hsp、chisel、ml、cuda、layout、rtl、onnx`。Python/C++是默认基础环境。Chisel及layout按单节点完整工具链登记，具体工具、路径及argv保留在操作员明细和本任务回执中。

Python任务统一使用`${tool:python}`。普通科学计算使用cpu后端，Torch任务声明ml，取得CUDA授权后使用cuda后端。设备私有python_runtimes维护完整解释器前缀，项目不选择解释器或设备。新算法补齐所用环境的依赖，禁止扩充Python组合后缀。

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

`kill <id>`终止指定任务及其子进程，支持完整ID或唯一的至少4位活动任务短ID。原ID、输入身份、声明输出、日志及累计费用继续保留，领取后按任务生命周期清理。

## 验证与证据

- 完整核心、原生套件、工程fixture及平台跳过项由当前维护入口的`tests`逐项登记。执行授权、原ID顺序和管线48项通过；启动、历史索引及清理专项59项完成，其中1项按平台跳过；损坏回执及清理恢复补验22项通过。原始日志在`.local/maintenance/lease-release-20261105/`。
- Windows长路径贯穿成果枚举、输入保留、领取和原ID删除，保留链接自身的清理边界；26项文件生命周期检查通过，日志`patch-01101-long-output-lifecycle-final.log`。
- 托管映射、数值内核、内存模型、线程共享缓存、依赖发布、非阻塞准备、维护/Ctrl+C排空和原ID恢复均有隔离检查。
- 部署、现场执行、实际版本/PID/状态根与原状态恢复以本次维护目录`current.json`及对应证明为准。
- 能力与环境原生资格：`.local/maintenance/layout-ku26s-20261104/`中的`layout-qualify-final.json`、`admission-after.json`及逐设备现场验收；CPU/ML/CUDA环境记录见`.local/maintenance/capability-contract-20261104/`。
- 集中本机DSH Flash/max审查的原回执和Owner裁定保存在当前维护目录。规划性能重放见`.local/maintenance/lease-scale-20261104/`：352个真实任务、27次规划，授权与资源逐项相同，耗时降低5.60倍。
- 194006个原ID、706份资产的同索引回放保持身份摘要一致；紧凑加载的`tracemalloc`峰值78.01MiB，完成加载后54.52MiB。不可变资产大小索引避免重复遍历内容树，原子退役和后台回收保留未领取回执及其引用；实际启动分阶段耗时保存在各状态根`startup-current.json`。

科学计划、输入、评价及恢复账本由所属工程维护。源码变化由Git记录，正式文档说明当前事实、要求及设计。
