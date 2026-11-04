# DCO 当前事实

- 当前源码版本：**0.9.0**。
- 实际加载版本、当前操作员、维护状态和放行状态：`.local/maintenance/scheduling-20261104/current.json`；与实际health交叉核对。
- 使用规范：[README](../README.md)；接口及机制：[DESIGN](DESIGN.md)；协作要求：[AGENTS](../AGENTS.md)。

## 调度与接入

弹性任务先取得最多4个有用核，新任务随后以1核进入，剩余兼容核心用于已有任务扩核。一般搜索上限1024；有限种群按真实并行宽度限定。默认准备窗口超过128并覆盖兼容CPU数；工程以真实就绪工作持续补位。

Client.cpu_lease按在途已授权线程及就绪需求借还核心，长尾归还空槽，部分授权可领取其他任务释放的核心。稳定且已无借还空间的活动段不发送资源心跳。所有工程使用标准语义能力，设备选择和共享服务修改归DCO操作员。

科学输入、原任务ID、结果、费用和恢复证据沿所属工程持久保留。当前正式文档说明使用、事实、设计与约束，源码变化由Git记录。

## 验证与现场记录

- 核心入口 `python -B main.py --test`：691项完成，5项环境跳过，326.374秒，成功。日志：`core-tests.log`。
- 最终租约、活动线程和准备窗口：24项通过，2.724秒。日志：`lease-final-tests.log`。
- 一次集中DSH Flash/max只读审查已完成并由Owner逐项裁定：`review-adjudication.json`。
- 统一源码、节点重启、原ID收齐和现场租约验收记录保存在同一维护目录。维护验收使用独立运维任务，科学计划由所属工程继续执行。

维护日志、源码清单、原ID回执与实际加载事实均在上述权威目录，不将维护过程追加到日常使用规范。

## Torch与科学优化组合

`python_torch_scientific`在同一解释器内提供Torch、scikit-learn、pymoo、moocore、Numba、SciPy、NumPy和threadpoolctl。五节点逐项导入及Torch/NumPy桥接、scikit-learn核函数、pymoo指标接口已验收。组合能力、持久绑定、实际依赖版本及原ID回执保存在`.local/maintenance/torch-scientific-20261104/`。

完整环境优先复用现有解释器；现有库版本保持，缺失依赖在对应Torch环境补齐。运行中的科研任务持续执行，科学评价次数为0。
