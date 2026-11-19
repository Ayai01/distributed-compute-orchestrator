# DCO 当前状态

2026-11-19（PST），生产中心与 **18 台 worker 均运行 1.0.17**，其中学校节点 13 台。中心排空在途任务后切换并恢复派发，68 个排队任务按原 ID 续派，传输退休记录齐全。最后一台 worker 排空时暂停中心新派发，保持原任务可重新借核并继续收尾。实时用量由 `admission(activity=True)` 返回。[滚动升级回执](../.local/migration/rollout-1017/center.json)

学校节点的精确四核 SDK 执行、结果领取和 SPICE 分压仿真均成功。固定内存保持整任务标量报价，实测工作集与已授予预算分别记账。[新节点源码包](../.local/migration/school-cpu-source-current.json)

| 能力 | 采样容量（核） |
| --- | ---: |
| ng47、Chisel、ML、RTL、ONNX、ONNX INT8 | 各 578 |
| ARM64 ELF 载体（`elf-aapcs64`） | 学校 400 |
| x86-64 PE 载体（`pe64`） | 82 |
| HSP | 96 |
| Layout | 82 |
| CUDA | 50 |
| NPU | 0 |

原 H 对应的 AR 容器退出后，frpc 监听消失。AR 重建申请仍在 Scheduling，H 的失效注册已从持久配置与运行中心移除，七笔原任务回执保留。旧 AK 和旧 E 的 Terminated 环境已删除。[H 注销回执](../.local/migration/school-h-unregister-20261119.json) · [学校状态](../../../st-platform/docs/STATUS.md)

**待处理运维项**。学校 CPU 扩容。32 核／64 GiB 和 24 核／48 GiB 的提交校验均显示零台可放置宿主。等有实际位置后按标准规格补位。

**运行时验收**。13 台学校 ARM64 worker 共享已验收的 ONNX INT8 环境和 FIR 原生载体。冻结 YOLO 与已付结果一致。两台学校节点的 FIR 28 条控制和 40 项检查与 Windows 结果逐条一致。原 ID、费用与结果已保存。[学校 YOLO](../.local/migration/school-arm64-yolo-acceptance.json) · [ARM64 FIR](../.local/migration/school-arm64-fir-20261119.json) · [x86-64 验收](../.local/migration/runtime-workloads-20261119.json)

**中转与缓存**。学校上传和下载各设 768 KiB/s，上限相互独立。每台学校 worker 有三条普通大包连接、独立短派发和结果通道。资产缓存每十分钟检查，按七天未命中和 64 GiB 回收。实际 SPICE 冷发 1.1 KB 输入，算力端生成 246 KB 网表，两个节点返回相同结果。[冷启动与复用](../.local/migration/relay-cache-1016/native-cache-proof.json) · [本机清理](../.local/migration/relay-cache-1016/gc-live.json) · [共享盘验收](../.local/migration/relay-cache-1016/school-shared-policy.json)

**面板**。线程预留和 CPU 使用率分别显示。13 台学校节点均上报容器配额口径，24 核节点定向实测为 25.33%，同一计数区间直接计算为 25.35%。[容器采样和面板](../.local/migration/rollout-1017/cpu-cgroup-live-v1017.json)

**公网路径**。同一校内结果经 VPS 本机 Nginx WSS 为约 3.9 MiB/s，经本机公网链路为约 0.38 MiB/s。独立 SSH 流重现大量 TCP 重传，物理网卡直出及经 ku26s 绕行均未改善。[分段回执](../.local/migration/relay-cache-1016/segmented-az.json) · [公网传输](../.local/migration/relay-cache-1016/az-public-path.json)

**CPU 归还**。学校节点上的冻结 MAC 任务完成 12 项，中心和 worker 确认核数从 12 降至 11，任务收尾时降至 0，结果与 84.70 CPU 秒计量均已保存。满载定向验收中，原任务归还 7 核后，排队任务在 0.179 秒内取得这 7 核并启动。[真实 MAC](../.local/migration/rollout-1017/cpu-mac-live-v1017.json) · [派发复验](../.local/migration/rollout-1017/cpu-return-reuse-v1017.json)
