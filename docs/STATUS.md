# DCO v1 当前状态

2026-11-16（美国太平洋时间）。

- 已批准：强机单中心、直连优先、nurikoi仅中转及远程控制；四工程联合适配和迁移后清理。
- 新工程：D:/Projects/#4-Arclight/DCO_v1，独立Git克隆，当前分支v1，起点60b031b（已发布0.11.42）。源码实现提取与清理进行中。
- 旧生产入口：D:/Projects/#4-Arclight/DCO，五机0.11.42；新目录尚未替换任何生产进程。ARH服务与原科学任务继续运行。
- 六份文档已建立；具体实施和验收见ROADMAP，已生效项目逐项替换本页。
- 目录合同：Windows有D盘用D:/Projects/#4-Arclight/DCO_v1，否则C盘；Linux用~/Projects/#4-Arclight/DCO_v1。
- 当前主线接管v1实现与部署；旧维护会话只作来源参考。

## 四工程入口

| 工程 | 当前适配入口 | 迁移责任 |
| --- | --- | --- |
| AOP_v2 | compute/center.py | execute/python_call/python_task/iter_items及原始角点缓存 |
| RGCN-v2 | src/parallelism.py、remote_native.py、remote_research.py | 阶段资源、原生测量与逻辑观察缓存 |
| ARH-DSE-v3 | arh/dco.py、framework/distributed_tasks.py、remote_execution.py | 原ID、资产、原生阶段与独立服务连续运行 |
| DCBFS-MO | src/dcbfs/_runtime/parallel.py | map/run_call与完整reference缓存 |

## 后续

先登记旧五机与现有SDK消费者，再完成隧道、资源控制和网站的实际接入。当前运行身份与部署回执集中在忽略的 .local/migration/current.json。
