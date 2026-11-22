# DCO 当前状态

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
