# DCO 当前事实

- 当前源码版本：**0.10.0**。
- 实际加载版本、当前操作员、维护及放行状态：`.local/maintenance/capability-contract-20261104/current.json`；与实际health交叉核对。
- 使用规范：[README](../README.md)；接口及机制：[DESIGN](DESIGN.md)；协作要求：[AGENTS](../AGENTS.md)。

## 能力与环境

运行能力为`ng47、hsp、chisel、ml、cuda、layout、rtl、onnx`。Python/C++是默认基础环境。Chisel及layout按单节点完整工具链登记，具体工具、路径及argv保留在操作员明细和本任务回执中。

Python任务统一使用`${tool:python}`。普通科学计算使用cpu后端，Torch任务声明ml，取得CUDA授权后使用cuda后端。设备私有python_runtimes维护完整解释器前缀，项目不选择解释器或设备。新算法补齐所用环境的依赖，禁止扩充Python组合后缀。

2026-11-04现场记录：五个提供者合计174个逻辑线程预算；五台CPU/ML环境、两台CUDA环境已完成逐项依赖、数值接口及最小计算验收，现有依赖版本保持。环境、路由和源码部署的原ID回执保存在上述维护目录。

## 调度与任务

弹性任务先取得最多4个有用核，新任务随后以1核进入，剩余兼容核心用于已有任务扩核。一般搜索上限1024；有限种群按真实并行宽度限定。默认准备窗口超过128并覆盖兼容CPU数；工程以真实就绪工作持续补位。

Client.cpu_lease按在途已授权线程及就绪需求借还核心，长尾归还空槽，部分授权可领取其他任务释放的核心。稳定且已无借还空间的活动段不发送资源心跳。所有工程使用标准语义能力，设备选择和共享服务修改归DCO操作员。

`kill <id>`终止指定任务及其子进程，支持完整ID或唯一的至少4位活动任务短ID。原ID、输入身份、声明输出、日志及累计费用继续保留，领取后按任务生命周期清理。

## 验证与证据

- 完整核心及原生回归入口：`python -B main.py --test-native`。结果见`native-core-final-tests.log`。
- 环境及路由证明：`*-environments.json`、`routing-final-tests.log`及`live-acceptance.json`。
- 一次集中DSH Flash/max只读审查已完成并由Owner逐项裁定：`review-adjudication.json`。
- 统一源码清单、版本/PID/状态根核对、原ID领取及任务终止证明保存在同一维护目录。

科学计划、输入、评价及恢复账本由所属工程维护。源码变化由Git记录，正式文档说明当前事实、要求及设计。
