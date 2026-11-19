# DCO v1

Distributed Compute Orchestrator：一个调度中心，统一 SDK、终端和远程控制，按实际就绪工作分配计算资源。

## 当前入口

本仓库当前分支为 `v1`。终端和中心统一使用 DCO 版本，发布源码由 `v<版本>` Git 标签标识。终端标题和健康接口显示实际运行版本。[当前部署](docs/STATUS.md) · [版本更新日志](docs/HISTORY.md)

Windows从当前目录运行 `start.cmd`，Linux运行 `./start.sh`；统一读取 `.local/worker.json`，中心同时读取 `.local/nodes.json`。独立worker可显式运行 `start.cmd --mode worker --config .local\worker.json`。服务保留原生终端，关闭Codex会话不结束计算；终端是人工停止入口。远程资源控制使用 [nurikoi控制页](https://nurikoi.com/manage/live/)。

无图形界面的设备可用 `./start.sh --mode worker --no-dashboard` 运行纯文本服务。日志输出到标准输出和标准错误，可由系统服务收集。调度中心也可用同一选项运行。

首次部署先执行 `python -B main.py setup`。默认任务使用的 Python 会安装并验证 PyTorch 2.14.1。CPU 为默认构建，确认 CUDA 13.0 驱动兼容的环境可用 `setup --torch-build cu130`。任务直接 `import torch`。

运维终端显示节点能力与聚合容量。任务标签按栏宽显示，完整标签保存在原任务。F6/F7 翻节点，F8 切换总览与完整单节点详情，PgUp/PgDn 翻任务，Ctrl+PgUp/PgDn 翻命令输出。

## 安装位置

- Windows：`D:\Projects\#4-Arclight\DCO_v1`；没有 D 盘时为 `C:\Projects\#4-Arclight\DCO_v1`。
- Linux：`~/Projects/#4-Arclight/DCO_v1`。
- `.local/` 保存设备配置、身份、状态和有界运维回执；`.tools/` 保存本机工具；可重建临时物使用设备 RAM 盘或系统临时目录中的所属任务目录。
- 科研输入、结果、优化器及科学缓存由调用工程管理。

## FRP 与火绒信任

隧道固定使用官方 frp 0.71.1，下载后先核对发布包 SHA256。Windows 计算节点使用 `.tools/frp/0.71.1/frpc.exe`；Linux 中转服务器使用 `frps` 和网站访问端 `frpc`。Windows 的 `frpc.exe` SHA256 为 `23d11640bee87207c7a99d006b84c554df997feeba7ac7243db20a88a9cb5e01`。

火绒 6.0：主界面底部或右上角菜单 → 信任区 → 添加 → 文件，选择上述具体文件，备注版本与用途。保留“改变文件内容后信任依旧生效”为未选中，版本升级重新核验。已隔离的文件从隔离区恢复，并核对信任项；依据 [火绒官方操作手册](https://cdn-www.huorong.cn/Public/Uploads/uploadfile/files/20261029/%E7%81%AB%E7%BB%92%E5%AE%89%E5%85%A8%E8%BD%AF%E4%BB%B6%206.0%20%E7%94%A8%E6%88%B7%E6%93%8D%E4%BD%9C%E6%89%8B%E5%86%8C%EF%BC%886.0.13.0%E7%89%88%E6%9C%AC%EF%BC%89.pdf) 的隔离区与信任区说明操作。

迁移时各 Windows 设备分别登记本机文件，C 盘设备替换盘符；复制工程不会携带火绒信任设置。本机管理员已设置D:/Projects信任，安装器在官方包校验后直接将二进制写入该工程固定工具目录；一次性脚本仍使用RAM盘。发生删除或隔离后记录具体文件，由设备管理员恢复/信任后继续。

## 架构与使用原则

调度中心运行在资源充裕的常在线计算机器；nurikoi.com 提供中转和经过认证的控制界面。可达的 IP＋端口优先直连，无公网 IP 的节点通过出站隧道接入；两条路径共用设备身份、任务协议和原 ID。

服务拥有计算，SDK、CLI 和网页提供控制。控制界面显示实际 CPU、授权、就绪需求、内存及等待原因，修改 CPU 预算、内存政策和超发上限沿同一中心接口执行。科学任务正常完成后，调用工程先持久发布结果与费用，再领取清理运输副本。

CPU总线程硬限额＝基础预算×超发倍率向下取整，硬限额和倍率双向联动。看板有人观看时每15秒发布，无人观看时每60秒发布；页面隐藏后停止轮询。



现有 `Client.execute`、`python_map/python_call/python_task`、`map_items/iter_items/run_call`、内容资产及进度接口作为迁移合同。接口与资源语义见 [DESIGN](docs/DESIGN.md)；协作与验证见 [AGENTS](AGENTS.md)。

## 昇腾容器

学校开通、实例映射与接入验收见[学校接入入口](../../st-platform/README.md)。普通 SDK 按能力发单。

学校平台先分配 CPU/RAM/NPU，再在该容器内启动 worker。Linux 标准 `setup` 已在学校 ARM 隔离 venv 验证科学依赖及 ONNX 推理。Linux 的 CPU/RAM 读取按 cgroup 有效限额裁定；容器模板为 `examples/worker-ascend.json`，关闭 CPU 超发并使用适合小容器的内存保留线。

复用已有 PyTorch 昇腾镜像，在私有 worker 配置上执行 `python main.py --config <配置.json> setup --ascend`。命令加载镜像内 CANN 环境、验证实际设备和反向传播，并登记 `python_runtimes.npu`、设备 ID、容量和语义标签，不安装软件。可用 `--cann-env` 指定镜像的 set_env.sh。

普通任务使用既有 SDK：`accelerator={"kind":"npu","count":1,"requires":["npu:pytorch"]}`；任务代码使用 PyTorch 的 `npu:0`。DCO 按授予的 runtime ID 设置 `ASCEND_RT_VISIBLE_DEVICES`，保留外层容器的设备挂载设置。CPU 任务的 Ascend runtime 设备列表为空。

独立资格入口为 `python -m examples.ascend_qualification --config <配置.json> --smoke <ascend_smoke.py> --output <结果.json>`；只启动回环 worker，完成 CPU/NPU 任务、原 ID 与费用回执、结果回传后退出。平台资料和基础实测位于 `D:/Projects/st-platform`；长期节点沿现有私网中转接入，不改本地 VPN、TUN、DNS。
