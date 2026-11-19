# DCO v1 当前状态

2026-11-18 21:20 PST，本机终端面板 **1.0.2** 已在原进程加载，调度中心仍报告 **1.0.1**。实际 205×60 窗口从第 1 行显示标题，第 3 行起显示 20 台设备，第 15 行显示当前任务，命令区仍可见。原中心 PID 25536 持续监听。[发布与窗口读回](../.local/migration/console-fleet-layout-current.json)

2026-11-16（美国太平洋时间）。五机已统一到v1原生入口；当前分支v1，运行版本1.0.0。架构为强机单中心、直连优先、nurikoi仅认证中转与资源控制。

## 已交付

| 设备 | 根目录 | 当前原生PID |
| --- | --- | ---: |
| Aya1-PC | D:/Projects/#4-Arclight/DCO_v1 | 52240，supervisor 9728 |
| Aya1-TX | D:/Projects/#4-Arclight/DCO_v1 | 16888 |
| Aya1-ITX | D:/Projects/#4-Arclight/DCO_v1 | 4740 |
| 126kf | C:/Projects/#4-Arclight/DCO_v1 | 5044 |
| ku26s | ~/Projects/#4-Arclight/DCO_v1 | 4082067 |

- SDK、工具、状态、生产索引及原operator身份已迁入标准根，节点和中心路由均恢复。远端磁盘源码68bb462；ku26s已加载该隧道启动修复，Windows健康进程沿现有代码继续，启动专用修复在下次自然启动加载。
- 无公网IP通路已完成真实TX任务：原ID80dd5e87960143bb99045cef1d8a008f，一次提交、7个成功RPC走nurikoi、结果与0.046875 CPU秒持久后回收。frp 0.71.1固定SHA；服务器两服务总约11MiB，CPU/内存有界，Nginx443精确WSS路径，无新增公网端口。ku26s已移除frpc继承的桌面http_proxy，真实relay健康。
- 固定宽度整组准入、等待0核、弹性扩缩、批尾归还与跨工程份额已生效。资源84项、缓存72+2项、CLI31项及原网络合同已通过；24项真实资源任务完成。已通过证据直接复用。
- 网站控制完成实际登录、读取、可恢复写入和最新联动验收。CPU硬限额H=floor(B×F)，H/F双向联动，设备cpu_limit只读约束基础预算。当前PC基础12、倍率1.25；其他基础与倍率保持用户配置。
- 物理保留及保护线：PC 12GiB，其他四台4GiB；停止扩展的主机利用率：PC80%、其他85%。控制面和展示面已对齐。
- SDK修复：resize内部内存值0/None均省略覆盖，正整数照常发送，避免弹性归还CPU时误发memory_budget_mb=0；中心PID52240已原位加载该函数，原节点会话与任务保持。fetch临时ZIP沿进程TEMP/TMPDIR，支持调用工程RAM盘生命周期；远端运行源码版本仍以部署回执为准。
- 展示发布器1.0.7 PID7568归supervisor9728持有；迁移后输出目录缺失问题已修复，当前状态在.local/liveboard。有人观看15秒、无人观看60秒；浏览租约75秒由网站worker共享，gzip+CBOR典型约5KB/次。浏览器实见11秒前采样、PC12.0GiB保留线。

## 科研衔接

AOP、RGCN、ARH-DSE、DCBFS已沿同一SDK、实际线程宽度和内容交换合同接入；科学缓存规则归各工程，DCO通过.local/projects.json统一注册。AOP/RGCN/DCBFS研究暂停保持。ARH独立服务沿原ID、已付窗口和检查点继续；三工程结果与优化状态见ARH-DSE-v3/docs/STATUS.md。

TX/ITX最初排空的33个原ID已结清：29取消条件从原检查点接续、4成功条件直接复用，17,600.953125 CPU秒保留。后续原生迁移采用自然边界；Linux末批保持enabled并短暂停中心新派发，原任务结清后重载再恢复派发。原校准缓存同卷迁入，没有重复校准。

## 退役与证据

本机旧DCO及DCO-arh-integration-fix、三Windows桌面旧根、Linux旧distributed-compute-orchestrator根和旧诊断launcher均已退役。源码历史用独立Git bundle保留；必要原ID、费用、科学参考与私有配置按清单归档到.local/history。

- 本机：17,518文件，逻辑457,882,616B、独立实体457,769,327B；必要历史压缩193,122,019B、Git bundle1,424,773B。
- 三Windows：8,118载荷文件，逻辑857,513,256B、独立实体857,479,781B；三必要历史压缩共175,083,884B。
- Linux：210旧文件、4,430,237B；保留32必要文件、220,916B。
- 迁移用一次性launcher/controller和空客户端账本共5文件、15,509B已统一回收，回执为helper-retirement.json。
- 原统一脚本的退出码2仅对应受保护批次：本机活动ARH批次、TX两个既有false批次、126kf来源未明docs批次。均已保护，未删除其内容。

部署与回收回执集中在.local/migration：node-migration、windows-retirement、local-retirement-preservation、website-deployment、resource-policy-current；网络真实任务在.local/network/relay-task-validation.json。各费用先持久再删除运输任务，一次性监视器已退出。

## 后续

DCO本轮迁移交付完成。后续围绕真实故障或新工程公共合同维护，不新增兼容层；ARH搜索、独立验证、阶段报告与后继供给沿其ROADMAP由独立服务持有。
