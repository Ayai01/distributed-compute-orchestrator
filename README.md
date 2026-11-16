# DCO v1

Distributed Compute Orchestrator：一个调度中心，统一 SDK、终端和远程控制，按实际就绪工作分配计算资源。

## 当前入口

本仓库当前分支为 `v1`。重构以已发布的 0.11.42 有效实现为基础；当前部署事实见 [STATUS](docs/STATUS.md)，实施顺序见 [ROADMAP](docs/ROADMAP.md)。在途计算沿原服务完成或在原生边界接续，原 ID、已付结果和检查点连续。

Windows从当前目录运行 `start.cmd`，Linux运行 `./start.sh`；统一读取 `.local/worker.json`，中心同时读取 `.local/nodes.json`。独立worker可显式运行 `start.cmd --mode worker --config .local\worker.json`。服务保留原生终端，关闭Codex会话不结束计算；终端是人工停止入口。远程资源控制使用 [nurikoi控制页](https://nurikoi.com/manage/live/)。

## 安装位置

- Windows：`D:\Projects\#4-Arclight\DCO_v1`；没有 D 盘时为 `C:\Projects\#4-Arclight\DCO_v1`。
- Linux：`~/Projects/#4-Arclight/DCO_v1`。
- `.local/` 保存设备配置、身份、状态和有界运维回执；`.tools/` 保存本机工具；可重建临时物使用设备 RAM 盘或系统临时目录中的所属任务目录。
- 科研输入、结果、优化器及科学缓存归 AOP、RGCN、ARH-DSE、DCBFS 各工程管理。

## FRP 与火绒信任

隧道固定使用官方 frp 0.71.1，下载后先核对发布包 SHA256。Windows 计算节点使用 `.tools/frp/0.71.1/frpc.exe`；Linux 中转服务器使用 `frps` 和网站访问端 `frpc`。Windows 的 `frpc.exe` SHA256 为 `23d11640bee87207c7a99d006b84c554df997feeba7ac7243db20a88a9cb5e01`。

火绒 6.0：主界面底部或右上角菜单 → 信任区 → 添加 → 文件，选择上述具体文件，备注版本与用途。保留“改变文件内容后信任依旧生效”为未选中，版本升级重新核验。已隔离的文件从隔离区恢复，并核对信任项；依据 [火绒官方操作手册](https://cdn-www.huorong.cn/Public/Uploads/uploadfile/files/20261029/%E7%81%AB%E7%BB%92%E5%AE%89%E5%85%A8%E8%BD%AF%E4%BB%B6%206.0%20%E7%94%A8%E6%88%B7%E6%93%8D%E4%BD%9C%E6%89%8B%E5%86%8C%EF%BC%886.0.13.0%E7%89%88%E6%9C%AC%EF%BC%89.pdf) 的隔离区与信任区说明操作。

迁移时各 Windows 设备分别登记本机文件，C 盘设备替换盘符；复制工程不会携带火绒信任设置。本机管理员已设置D:/Projects信任，安装器在官方包校验后直接将二进制写入该工程固定工具目录；一次性脚本仍使用RAM盘。发生删除或隔离后记录具体文件，由设备管理员恢复/信任后继续。

## 架构与使用原则

调度中心运行在资源充裕的常在线计算机器；nurikoi.com 提供中转和经过认证的控制界面。可达的 IP＋端口优先直连，无公网 IP 的节点通过出站隧道接入；两条路径共用设备身份、任务协议和原 ID。

服务拥有计算，SDK、CLI 和网页提供控制。控制界面显示实际 CPU、授权、就绪需求、内存及等待原因，修改 CPU 预算、内存政策和超发上限沿同一中心接口执行。科学任务正常完成后，调用工程先持久发布结果与费用，再领取清理运输副本。

CPU总线程硬限额＝基础预算×超发倍率向下取整，硬限额和倍率双向联动。看板有人观看时每15秒发布，无人观看时每60秒发布；页面隐藏后停止轮询。

现有 `Client.execute`、`python_map/python_call/python_task`、`map_items/iter_items/run_call`、内容资产及进度接口作为迁移合同。接口与资源语义见 [DESIGN](docs/DESIGN.md)；协作与验证见 [AGENTS](AGENTS.md)。
