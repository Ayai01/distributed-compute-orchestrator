# DCO 当前状态

2026-11-18 23:15 PST 实测，中心、终端与 19 台 worker 均运行 **1.0.7**，全部在线接单。总预算 **591 核／1345 GiB**，采样可用 **591 核／约 1218 GiB**。[中心](../.local/migration/dco-107-production-current.json) · [滚动更新](../.local/migration/dco-107-remote-rollout-current.json)

| 能力 | 可用核数 |
| --- | ---: |
| ng47、Chisel、ML、RTL、ONNX | 各 591 |
| HSP | 96 |
| Layout | 63 |
| CUDA | 31 |
| NPU | 0 |

校内 AR 已以 32 核／64 GiB 接替 H，旧 AK 注册已撤下。真实 RTL MAC 与 SPICE 工作点通过，已按原 ID 收回结果和费用。[学校接入](../../../st-platform/docs/STATUS.md)

**待处理运维项**。学校 CPU 扩容。32 核／64 GiB 和 24 核／48 GiB 的提交校验均显示零台可放置宿主。等有实际位置后按标准规格补位。
