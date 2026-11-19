# DCO 当前状态

2026-11-19（PST），18 台 worker 在线。生产中心仍是 **1.0.12** 进程，已在线应用 **1.0.16** 的中转、缓存和面板修复。在途原任务继续运行。实时用量由 `admission(activity=True)` 返回。[生产回执](../.local/migration/relay-cache-1016/online-policy.json)

学校节点的精确四核 SDK 执行、结果领取和 SPICE 分压仿真均成功。固定内存保持整任务标量报价，实测工作集与已授予预算分别记账。[新节点源码包](../.local/migration/school-cpu-source-current.json)

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

**中转与缓存**。学校上传和下载各设 768 KiB/s，上限相互独立。每台学校 worker 有三条普通大包连接、独立短派发和结果通道。资产缓存每十分钟检查，按七天未命中和 64 GiB 回收。一次实际 SPICE 冷发 1.1 KB 输入，算力端生成等价的 246 KB 网表，两个节点均返回相同结果。[冷启动与复用](../.local/migration/relay-cache-1016/native-cache-proof.json) · [本机盘点](../.local/migration/relay-cache-1016/disk-before.json)

**面板**。弹性资源调整持续占用调度锁时，面板继续显示最近快照并排队刷新。生产修复后 6.3 秒内读到四个新样本，中心 PID 保持一致。[面板回执](../.local/migration/relay-cache-1016/view-after.json)
