# DCO 当前状态

2026-11-18 晚（PST），中心、终端与 19 台 worker 均运行 **1.0.8**，全部在线接单。总预算 **591 核／约 1345 GiB**，空闲采样可用 **591 核／约 1218 GiB**。实时容量由 `admission(activity=True)` 返回。[中心](../.local/migration/dco-108-production-current.json) · [滚动更新](../.local/migration/dco-108-remote-rollout-current.json)

| 能力 | 可用核数 |
| --- | ---: |
| ng47、Chisel、ML、RTL、ONNX | 各 591 |
| HSP | 96 |
| Layout | 63 |
| CUDA | 31 |
| NPU | 0 |

校内 AR 已以 32 核／64 GiB 接替 H，并接上校内共享缓存。真实 RTL、Chisel 和 SPICE 通过。旧 AK 注册已撤下，旧申请已终止。[学校接入](../../../st-platform/docs/STATUS.md)

**待处理运维项**。学校 CPU 扩容。32 核／64 GiB 和 24 核／48 GiB 的提交校验均显示零台可放置宿主。等有实际位置后按标准规格补位。
