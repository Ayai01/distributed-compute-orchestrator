# DCO v1

Distributed Compute Orchestrator：一个调度中心，统一 SDK、终端和远程控制，按实际就绪工作分配计算资源。

## 当前入口

本仓库当前分支为 `v1`。重构以已发布的 0.11.42 有效实现为基础；当前部署事实见 [STATUS](docs/STATUS.md)，实施顺序见 [ROADMAP](docs/ROADMAP.md)。在途计算沿原服务完成或在原生边界接续，原 ID、已付结果和检查点连续。

## 安装位置

- Windows：`D:\Projects\#4-Arclight\DCO_v1`；没有 D 盘时为 `C:\Projects\#4-Arclight\DCO_v1`。
- Linux：`~/Projects/#4-Arclight/DCO_v1`。
- `.local/` 保存设备配置、身份、状态和有界运维回执；`.tools/` 保存本机工具；可重建临时物使用设备 RAM 盘或系统临时目录中的所属任务目录。
- 科研输入、结果、优化器及科学缓存归 AOP、RGCN、ARH-DSE、DCBFS 各工程管理。

## 架构与使用原则

调度中心运行在资源充裕的常在线计算机器；nurikoi.com 提供中转和经过认证的控制界面。可达的 IP＋端口优先直连，无公网 IP 的节点通过出站隧道接入；两条路径共用设备身份、任务协议和原 ID。

服务拥有计算，SDK、CLI 和网页提供控制。控制界面显示实际 CPU、授权、就绪需求、内存及等待原因，修改 CPU 预算、内存政策和超发上限沿同一中心接口执行。科学任务正常完成后，调用工程先持久发布结果与费用，再领取清理运输副本。

现有 `Client.execute`、`python_map/python_call/python_task`、`map_items/iter_items/run_call`、内容资产及进度接口作为迁移合同。接口与资源语义见 [DESIGN](docs/DESIGN.md)；协作与验证见 [AGENTS](AGENTS.md)。
