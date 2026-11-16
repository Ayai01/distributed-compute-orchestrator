# DCO v1 当前状态

2026-11-16（美国太平洋时间）。

- 已批准：强机单中心、直连优先、nurikoi仅中转及远程控制；四工程联合适配和迁移后清理。
- 当前工程：D:/Projects/#4-Arclight/DCO_v1，独立Git克隆，分支v1，1.0.0源码09ec7a7已发布。
- 本机中心已原生切换：PID52240，supervisor9728，scheduler及worker状态均在新根；19项工具、状态和生产索引同卷迁移，原operator身份保持。ku26s已从标准Linux新根运行1.0.0，PID4078434。
- 五机已从标准新根运行1.0.0：PC52240、TX16888、ITX4740、126kf5044、ku26s4078434。退出前后目录比较混入正常领取，导致迁移脚本在移动完成后停住；Linux修正边界恢复，三Windows由用户启动新入口后恢复。四远端旧根活跃消费者及当前配置旧路径引用均为0。
- 六份文档已建立；具体实施和验收见ROADMAP，已生效项目逐项替换本页。
- 目录合同：Windows有D盘用D:/Projects/#4-Arclight/DCO_v1，否则C盘；Linux用~/Projects/#4-Arclight/DCO_v1。
- 当前主线接管v1实现与部署；旧维护会话只作来源参考。
- TX、ITX 于 2026-11-16 03:12:42 PST 完成排空；随后用户重新接回，两机已恢复供给。33个原ID已结清：29取消条件全部从原检查点接续，4成功条件直接复用，17,600.953125 CPU秒完整保留；7个相关控制器已运行。后续迁移沿原生边界，不再为准备目录取消健康计算。
- 新资源合同已生效：固定宽度整组准入、等待0核、阶段/批尾归还、8项CPU/RAM政策与受限控制API。84个不同合同用例及24项真实任务验证通过，回执 `.local/migration/resources-validation.json`。
- 四工程SDK路径、实际线程宽度和科研缓存归属适配已完成；通用交换读取 `.local/projects.json`，72项缓存合同及另2项原生产者引用合同通过。ARH已改用DCO_v1 SDK，新独立服务PID44772，5个原run沿原ID恢复。
- 本次维护自然排空于04:13:58 PST完成，没有取消科学任务。C910两条缓存引用随TX、ITX上线已一次接续；Rocket继续运行。YOLO此前因256MiB控制器声明不足而暂停，正保持原身份修正运行额度并恢复，后继沿原依赖推进。
- 网站控制已完成真实浏览器验收：Aya1登录、5设备快照、利用率阈值90→89→90保存并恢复。代理补齐X-DCO-Role，对应单测通过；实际中转资源GET 200、约0.04秒。Nginx及FRP保持原进程。
- 路由、流式传输与CLI生命周期验证已通过。火绒信任范围确认为D:/Projects，安装器已改为官方包校验后直接写固定版本工具目录；Linux二进制已完成部署，服务器frps/site-visitor active，Nginx已有精确WSS路由，未新增公网端口。
- 真实跨nurikoi任务80dd5e87960143bb99045cef1d8a008f一次提交成功，7个成功RPC全走TX中转，CPU0.046875秒；持久结果/费用后删除原运输任务。ku26s继承桌面http_proxy导致隧道EOF，启动器已修为仅frpc排除该环境变量，单项合同通过；沿当前窗口自然边界应用，直连计算继续。
- 四远端标准工程根、源码、网络及私有配置已建立；工具、科学缓存和状态沿同卷保留式迁移。四份旧科研缓存模块已按统一流程回收41,448字节，原代码保存在Git。

## 四工程入口

| 工程 | 当前适配入口 | 迁移责任 |
| --- | --- | --- |
| AOP_v2 | compute/center.py | execute/python_call/python_task/iter_items及原始角点缓存 |
| RGCN-v2 | src/parallelism.py、remote_native.py、remote_research.py | 阶段资源、原生测量与逻辑观察缓存 |
| ARH-DSE-v3 | arh/dco.py、framework/distributed_tasks.py、remote_execution.py | 原ID、资产、原生阶段与独立服务连续运行 |
| DCBFS-MO | src/dcbfs/_runtime/parallel.py | map/run_call与完整reference缓存 |

## 后续

收尾YOLO运行额度与原检查点接续，应用Linux隧道启动环境修复；保留必要历史与独有科研成果后，统一退役无消费者旧目录和过期诊断启动器。部署事实保存在 .local/migration/，科研结果由原工程持久账本持有。
