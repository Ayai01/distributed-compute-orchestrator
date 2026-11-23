# DCO 当前状态

**2026-11-23 00:05（太平洋时间）。1.0.57 已上线。** 中心与 22 台设备统一版本，627 个基础调度核，接单和派发已恢复。发布凭据续期后，内置浏览器核实看板重新收到实时采样。22 台设备的整数探针输出摘要一致，校内 AQ 同时运行 32 个独立工作项。ARH-DSE、AOP、RGCN、DCBFS-MO 和 Chiplet3D 的公开入口均通过。 [中心与设备验收](../.local/migration/rollout-1057/fleet-qualification.json) · [32 核验收](../.local/migration/rollout-1057/full-width.json) · [五工程公开入口](../.local/migration/rollout-1057/public-projects.json)

**两种调度权限。** 工程只声明通用能力包和资源需求，内容哈希只校验制品，设备由 DCO 分配。用户明确授权的场次由运维凭据写入 `placement_scope`，将完整许可名单和授权说明绑定到每个原 ID；范围内设备暂时不可用时继续排队。生产验收核实了公开 HTTP 入口拒收节点、平台、范围及设备专属能力，且单个许可节点停用期间始终保留待派发。 [权限与取消验收](../.local/migration/rollout-1057/cancel-and-width-qualification.json)

**拥塞与取消。** Rocket W4 在两分钟内提交 2,029 笔，随后 RV32I 每分钟最高 199 笔。学校中转瞬间复用工作连接不足，17 台受影响，峰值每分钟 1,256 次连接等待超时。102 个 Rocket 原 ID 已补齐终态和费用，其中 32 个成功产物已封存。新版按提交窗口节流，为中转心跳保留独立连接，并按 worker 合并取消。隔离验证排队 10,000 笔时健康请求 P99 为 117 毫秒，取消确认 67 毫秒。生产验证 17 笔运行和 120 笔排队全部取消，确认约 21 毫秒，约 2.5 秒归零。 [故障证据](../.local/network/frps-school-burst-buffer/portal-takeover-ay-aq.json) · [原单核对](../.local/migration/rollout-1056/reconciled-rocket-originals.json)

**核心缓存。** 学校共享卷已命中 1,024 份 Rocket W4、61 份历史 YOLO 资产，以及 RV32I、AOP、RGCN 的登记核心资产。19 个原 ZIP 通过执行端已有片段复原，88.29 MB 归档只通过链路补 7.08 MB；新拆出的 YOLO 固定模型与 9 个分片共 86.11 MB，差量约 178 KB。DCBFS 当前代码与数据、Chiplet 求解器和通用代码已准备并由两个校内路由交叉核实。后续复用登记项可走运维 `sync_cache 工程ID --kind assets`。首次出现的实验输入按冻结生成器、参数与校验摘要准备。 [缓存复核](../.local/migration/rollout-1057/cache-final-school.json) · [YOLO 分片](../.local/migration/rollout-1057/yolo-split-caches.json) · [通用资产](../.local/migration/rollout-1057/common-project-caches.json)

## 近期历史

**1.0.52 已完成隔离验收，生产仍运行 1.0.51。** 同一任务解包延迟 1.25 秒时，另一个任务接单由 1.269 秒降至 0.015 秒。72 项定向回归通过，公开轮询可读取解包和开算前的连续采样。人工切换后用自然原单核对学校链路的交接尾延迟。[隔离回执](../.local/migration/qualification-1052/handoff.json)

**2026-11-22 运维审计。** 3D chiplet 批次已完成 73,728/73,728 槽验证，2,432 次请求全部确认、结算并释放，DCO 无未结算任务或退休队列。重复恢复的根因是工程侧并行分析壳和恢复入口缺少进程 owner 保护，已在工程侧修复；DCO 的原 ID 幂等提交、回执恢复、缓存复用和异步退休路径保持有效。ARH、AOP、RGCN 普通入口审计通过能力包路由，不暴露节点、平台或拓扑选择器；历史校内 operator 路径仍受显式拒绝保护。当前无需 DCO 生产版本变更。

**生产版本。** 根目录源码、Windows/Linux 启动入口、中心、本机和 20 台远程 worker 已统一到 **1.0.51**。当前中心 PID 36732，接单开启，未暂停。配置池 21 台、基础容量 598 核。当前有效容量随可回收配额动态变化，面板按基础容量和有效容量分别显示。[逐台最终核对](../.local/migration/rollout-1051/final-health.json) · [中心激活回执](../.local/migration/rollout-1051/center-activation.json) · [根目录同步回执](../.local/migration/rollout-1051/root-source-sync.json)

**调度与采样。** 1.0.50 将续租留核判断限定在当前节点，密集队列不再为每次续租重算整张分配矩阵。五份生产队列快照逐格等价，旧分配耗时 3.78–4.70 秒，新完整分配 166–226 毫秒，单节点留核判断最长 0.049 毫秒。定向回归 33/33 通过，面板在双调度锁持有期间仍能返回新样本。自然采样请求约 0.02–0.20 秒，最终五次采样请求 0.005–0.018 秒、样本年龄约 1 秒，队列已清零，学校节点无离线项。[公平与面板验收](../.local/migration/rollout-1050/fairness-qualification.json) · [最终健康采样](../.local/migration/rollout-1050/final-health-samples.json)

**真实工程入口。** ARH-DSE、AOP、RGCN、DCBFS-MO、Chiplet3D-Plan 的真实 SDK 均通过公开能力准入和 activity 检查，返回不含节点拓扑。五工程回执统一记录 1.0.50。[公开入口验收](../.local/migration/rollout-1050/public-projects.json)

**学校平台。** 16 个 Running 环境已接入 16 台学校 worker，共 480 核、1209 GiB。AA、AQ 仍处 Scheduling，AP 仍在 In Queue，未把它们误接入。学校 worker 在 DCO 内按通用能力包派发。

**传输定向验收。** xjtu-ab 冷资产线传 193,708 B，准备 1.130 秒，接单到开算 2.362 秒，总取回 6.388 秒。xjtu-ac 热缓存线传 0 B，准备 0.169 秒，接单到开算 2.271 秒，总取回 4.959 秒。两笔均沿用原维护 ID、结果哈希一致并完成退休。[传输回执](../.local/migration/rollout-1050/targeted-transfer.json)

**后台回收。** 运输副本退休实时队列为 0。37 条已确认无活动引用的旧学校路由退休意图已完成审计、回收和中心重载，运行态等待集合归零。原任务审计保存在回执中，27 条历史中断记录仍保留费用未完成和结果缺失事实。[清理回执](../.local/migration/rollout-1050/legacy-route-cleanup-final.json)

**跨工程稳定性。** 五个工程公开入口沿用 1.0.50 已验收的能力合同，1.0.51 只改变深队列扩容预留策略。学校 ARM64 冷资产准备 1.130 秒、接单至开算 2.362 秒，热缓存传输 0 B。当前中转没有自限速，缓存 7.47 GiB、6437 个文件，最早文件未满 7 天，清理待回收为 0。[稳定性与性能回执](../.local/migration/rollout-1050/cross-engine-stability.json)

**深队列修复。** 1.0.51 在排队量达到活动任务规模时释放未提出资源请求的弹性扩容预留，保留当前核心和已发出的 resize 请求。此前面板曾出现 345/598 核、310 个排队任务，诊断有 47 个弹性任务和约 226 核推测性扩容空间。定向回归、allocation 5/5 和编译检查通过。上线后中心与 20 台 worker 均为 1.0.51，接单开启、队列 0、失联 0。普通 RTL 定向派发已成功，计算 0.016 秒、总耗时 0.200 秒，结果已取回并退休。[压力修复回执](../.local/migration/rollout-1051/queue-pressure-validation.json) · [RTL 派发回执](../.local/migration/rollout-1051/rtl-dispatch-probe.json)
