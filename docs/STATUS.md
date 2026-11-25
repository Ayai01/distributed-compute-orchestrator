# DCO 当前状态

## 1.0.63 · 2026-11-25（太平洋时间）

中心和 15 台 worker 已统一到 1.0.63，西交 10 个实例继续提供 562 核，AZ 保持 24 核。AM＋AQ 与 AP＋N 的整块和均衡规格在 03:21、03:26 两次查询均无新增匹配。

已接单的 worker 失联时，中心把取消保留在原单上并释放中心占用。中心重启后继续跟踪。原 worker 回连时自动补发取消，再用实际终态结算。领取待确认回执保留原单，直接删除也会等待结算。54 项定向回归通过，覆盖中心重启、原单自动取消、费用核对和 5,000 笔排队。生产实测 2 笔运行和 24 笔排队在 3 ms 确认取消、2.12 秒清空，五个工程的公开入口均通过。[上线回执](../.local/migration/rollout-1063/final-acceptance.json)

## 1.0.62 · 2026-11-25（太平洋时间）

生产中心和 15 台 worker 已统一到 1.0.62。西交 10 个实例在 8 台物理宿主上提供 562 个 ARM64 vCPU 和 1,124 GiB，超发倍率为 1.0。Aya1-PC、Aya1-TX、Aya1-ITX、126kf、ku26s 保持在线，中心预算为 0 核，可通过运维资源控制恢复。9 条已释放设备路由退出设备池，14 个旧 visitor 端口退出转发，原任务和费用沿原账本保留。[中心和设备](../.local/migration/rollout-1062/final-acceptance.json) · [路由迁移](../.local/migration/rollout-1060/center-activation.json)

**重连。** 10 台校园 worker 重启后曾报告不同的 MAC 探测后缀。1.0.62 使用随状态目录持久化的 worker 身份判断原节点，把身份与自动重连状态一并保存。真实试验中，西交 AZ 重启后探测后缀发生变化。中转停止约 59 秒触发摘除，中转恢复 5.43 秒后，中心自动启用原 worker。旧地址接入另一份状态的情形由定向回归覆盖。[生产重连](../.local/migration/rollout-1062/visitor-same-id.json)

**排队与传输。** 5,000 个原 ID 下，隔离健康查询 P99 53 ms，批量取消确认 36 ms。生产验证同时领取两路共 16 MiB 的完整结果，并完成 12 笔短原单。205 次采样无节点失联，健康查询 P99 8.5 ms。20 笔运行和 300 笔排队任务的批量取消在 7.9 ms 确认，2.51 秒全部进入取消终态。工作进程批量停止时先发信号，任务正文接收和解包期间也可以登记原 ID 的取消，避免该请求在解除拥塞后启动。[生产取消及核宽](../.local/migration/rollout-1062/cancel-and-width-qualification.json) · [实际回传](../.local/migration/rollout-1062/live-transfer-overlap.json)

**运行环境与缓存。** 15 台逐台通过 SPICE、ONNX、PyTorch CPU 和 RTL 统一实算。ARH-DSE-v3、AOP、RGCN-v3、DCBFS-MO、Chiplet3D-Plan 的公开 SDK 入口均通过。西交复核了 1,101 个已登记共享资产，全部命中。校内新节点从同一源码发布件启动，优先使用可在执行端重建的输入和共享缓存。[工具和运行库](../.local/migration/rollout-1062/fleet-engines.json) · [共享资产](../.local/migration/rollout-1062/shared-cache-audit.json) · [工程入口](../.local/migration/rollout-1062/public-projects.json)

**调度权限。** 普通工程只申请公开能力包和资源，DCO 选择设备。公开入口拒收节点、平台、设备专属能力、原生缓存指纹和运维名单。经用户明确授权的运维请求按原 ID 固化 `placement_scope`。新任务从 1 核启动，请求只用 `max_cpus` 声明运行期扩容上限。公开和运维入口均拒收显式 `min_start_cpus`。[权限回归](../.local/migration/rollout-1062/public-boundary.json)

**此前拥塞。** Rocket W4 在两分钟内提交 2,029 笔，学校中转峰值一分钟有 1,256 次工作连接等待超时。Chiplet 历史 P2 复测的 12 笔原单各要求在单台设备 32 核起步，事发采样中没有满足这个宽度的空闲槽。ARH 七笔派发资产已命中，43–254 秒的交接主要发生在连接池等待、回包超时及重试。历史原单及其账本仍按原 ID 查询。[Rocket 事故](../.local/network/frps-school-burst-buffer/portal-takeover-ay-aq.json) · [Chiplet 和 ARH 事故](../.local/migration/rollout-1059/incident-2316.json)
