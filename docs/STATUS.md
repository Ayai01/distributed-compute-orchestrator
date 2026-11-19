# DCO 当前状态

2026-11-19（PST），**1.0.12** 修复大估值阶段的内存校准和核宽扩张。生产中心和本次在线采样的 worker 运行 **1.0.8**，生产切换按现有任务排空流程另行进行。

2026-11-18 晚（PST），中心、终端与 19 台 worker 均运行 **1.0.8**，全部在线接单。总预算 **591 核／约 1345 GiB**，空闲采样可用 **591 核／约 1218 GiB**。实时容量由 `admission(activity=True)` 返回。[中心](../.local/migration/dco-108-production-current.json) · [滚动更新](../.local/migration/dco-108-remote-rollout-current.json)

| 能力 | 可用核数 |
| --- | ---: |
| ng47、Chisel、ML、RTL、ONNX | 各 591 |
| HSP | 96 |
| Layout | 63 |
| CUDA | 31 |
| NPU | 0 |

校内 AR 的上一实例曾接入 H，通过真实 RTL、Chisel 和 SPICE 验收。本次 AR 重建申请仍在 Scheduling。旧 AK 和旧 E 的 Terminated 环境已移除，原任务与费用已归档。[学校接入](../../../st-platform/docs/STATUS.md)

**待处理运维项**。学校 CPU 扩容。32 核／64 GiB 和 24 核／48 GiB 的提交校验均显示零台可放置宿主。等有实际位置后按标准规格补位。
