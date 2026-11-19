# DCO 当前状态

2026-11-19（PST），中心运行 **1.0.12**，18 台 worker 在线，学校代表节点已运行 **1.0.15**。中心采样容量为 **671 核**，随节点分配浮动。实时可用量由 `admission(activity=True)` 返回。[学校逐台版本](../.local/migration/school-runtime-1015-rollout.json)

106 项定向回归通过。学校节点的精确四核 SDK 执行、结果领取和 SPICE 分压仿真均成功。固定内存保持整任务标量报价，实测工作集与已授予预算分别记账。新节点的源码包指针已更新到 1.0.15。[源码包](../.local/migration/school-cpu-source-current.json)

| 能力 | 采样容量（核） |
| --- | ---: |
| ng47、Chisel、ML、RTL、ONNX、ONNX INT8 | 各 671 |
| ARM64 ELF 载体（`elf-aapcs64`） | 学校 504 |
| x86-64 PE 载体（`pe64`） | 71 |
| HSP | 96 |
| Layout | 71 |
| CUDA | 31 |
| NPU | 0 |

原 H 对应的 AR 容器退出后，frpc 监听消失。AR 重建申请仍在 Scheduling，H 的失效注册已从持久配置与运行中心移除，七笔原任务回执保留。旧 AK 和旧 E 的 Terminated 环境已删除。[H 注销回执](../.local/migration/school-h-unregister-20261119.json) · [学校状态](../../../st-platform/docs/STATUS.md)

**待处理运维项**。学校 CPU 扩容。32 核／64 GiB 和 24 核／48 GiB 的提交校验均显示零台可放置宿主。等有实际位置后按标准规格补位。

**运行时验收**。13 台学校 ARM64 worker 共享已验收的 ONNX INT8 环境和 FIR 原生载体。冻结 YOLO 与已付结果一致。两台学校节点的 FIR 28 条控制和 40 项检查与 Windows 结果逐条一致。原 ID、费用与结果已保存。[学校 YOLO](../.local/migration/school-arm64-yolo-acceptance.json) · [ARM64 FIR](../.local/migration/school-arm64-fir-20261119.json) · [x86-64 验收](../.local/migration/runtime-workloads-20261119.json)

**中转**。学校大包共用 384 KiB/s 的 DCO 预算，各节点普通大包连接限 1 条。完整资产冷发时，Orbit 的资产准备中位数为 266.1 秒。使用单点资产投影并命中共享缓存后，26 个学校 Conv-Pool／RV32I 续验从中心提交到开始计算为 0.21–1.05 秒。当前学校 ARH 请求包抽样中位数为 18.6 KB。[传输回执](../.local/migration/arh-device-matrix-20261119/transport-current.json)

**版本上线**。`v1.0.15` 包含能力自检、ARM64 ELF 和任务目录权限修复。其余进程保留在途科研任务，待结清后滚动切换。普通凭据仍由工程共用，跨工程原 ID 隔离需按工程发放独立凭据。
