# DCO 当前状态

2026-11-19（PST），中心、终端及 **18 台 worker 全部运行 1.0.12**，正常接单。总预算 **559 核／约 1281 GiB**。实时可用量由 `admission(activity=True)` 返回。[生产验收](../.local/migration/dco-1012-production-current.json) · [逐台更新](../.local/migration/dco-1012-remote-rollout-current.json)

106 项定向回归通过。学校节点的精确四核 SDK 执行、结果领取和 SPICE 分压仿真均成功。固定内存保持整任务标量报价，实测工作集与已授予预算分别记账。新节点使用的源码包指针已更新到 1.0.12。

| 能力 | 可用核数 |
| --- | ---: |
| ng47、Chisel、ML、RTL、ONNX | 各 559 |
| HSP | 96 |
| Layout | 63 |
| CUDA | 31 |
| NPU | 0 |

原 H 对应的 AR 容器退出后，frpc 监听消失。AR 重建申请仍在 Scheduling，H 的失效注册已从持久配置与运行中心移除，七笔原任务回执保留。旧 AK 和旧 E 的 Terminated 环境已删除。[H 注销回执](../.local/migration/school-h-unregister-20261119.json) · [学校状态](../../../st-platform/docs/STATUS.md)

**待处理运维项**。学校 CPU 扩容。32 核／64 GiB 和 24 核／48 GiB 的提交校验均显示零台可放置宿主。等有实际位置后按标准规格补位。

**权限上线**。生产设备目录与资源控制已验证普通凭据返回 403。`v1.0.13` 新增全量任务目录和前缀搜索限制，按原 ID 查询、领取的回归已通过。待在途科研任务结清后切换。普通凭据仍由工程共用，跨工程原 ID 隔离需按工程发放独立凭据。
