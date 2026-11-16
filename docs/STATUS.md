# DCO v1 当前状态

2026-11-16（美国太平洋时间）。

- 已批准：强机单中心、直连优先、nurikoi仅中转及远程控制；四工程联合适配和迁移后清理。
- 新工程：D:/Projects/#4-Arclight/DCO_v1，独立Git克隆，当前分支v1，起点60b031b（已发布0.11.42）。源码实现提取与清理进行中。
- 旧生产入口：D:/Projects/#4-Arclight/DCO，五机0.11.42；新目录尚未替换任何生产进程。ARH服务与原科学任务继续运行。
- 六份文档已建立；具体实施和验收见ROADMAP，已生效项目逐项替换本页。
- 目录合同：Windows有D盘用D:/Projects/#4-Arclight/DCO_v1，否则C盘；Linux用~/Projects/#4-Arclight/DCO_v1。
- 当前主线接管v1实现与部署；旧维护会话只作来源参考。
- TX、ITX 于 2026-11-16 03:12:42 PST 完成排空；随后用户重新接回，两机已恢复供给。33个原ID已结清：29取消条件全部从原检查点接续，4成功条件直接复用，17,600.953125 CPU秒完整保留；7个相关控制器已运行。后续迁移沿原生边界，不再为准备目录取消健康计算。
- 新资源合同已实现：固定宽度整组准入、等待0核、阶段/批尾归还、8项CPU/RAM政策与受限控制API。84个不同合同用例及24项真实任务验证通过，回执 `.local/migration/resources-validation.json`；生产仍加载旧版。
- 四工程SDK路径、实际线程宽度和科研缓存归属适配已完成；通用交换读取 `.local/projects.json`，72项缓存合同通过，另2项原生产者引用合同在更新凭据fixture后通过。ARH显式生产配置仍指向旧DCO，随服务原生切换迁移。
- 网站7个控制页文件已发布，Django原生重启一次后health 200；Nginx保持原进程。控制页面复用现有管理员认证与CSRF，实际资源控制等待v1中心接管。
- 路由、流式传输与CLI生命周期验证已通过。火绒信任范围确认为D:/Projects，安装器已改为官方包校验后直接写固定版本工具目录；Linux二进制已完成部署，服务器frps/site-visitor active，Nginx已有精确WSS路由，未新增公网端口。
- 四远端标准工程根已建立；原生产进程、工具及共享缓存仍保留。四份旧科研缓存模块已按统一流程回收41,448字节，原代码保存在Git，科研数据未移动。

## 四工程入口

| 工程 | 当前适配入口 | 迁移责任 |
| --- | --- | --- |
| AOP_v2 | compute/center.py | execute/python_call/python_task/iter_items及原始角点缓存 |
| RGCN-v2 | src/parallelism.py、remote_native.py、remote_research.py | 阶段资源、原生测量与逻辑观察缓存 |
| ARH-DSE-v3 | arh/dco.py、framework/distributed_tasks.py、remote_execution.py | 原ID、资产、原生阶段与独立服务连续运行 |
| DCBFS-MO | src/dcbfs/_runtime/parallel.py | map/run_call与完整reference缓存 |

## 后续

当前发布v1统一代码与各工程adapter，随后完成中心凭据/SDK维护交接、四远端原生目录切换和控制页联通。源码发布与进程实际加载分别登记；当前身份与部署回执集中在忽略的 .local/migration/。
