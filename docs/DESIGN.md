# DCO v1 设计

## 责任与部署

一个强机调度中心负责队列、工程份额、CPU/RAM/GPU 和受理账本；worker负责本机实际执行、工具环境和成果。SDK、终端和网页共享任务及控制接口。nurikoi.com 负责网络中转、认证后的控制界面和紧凑当前快照，计算/科学缓存/大结果保存在计算侧。

Windows 安装根为 D:/Projects/#4-Arclight/DCO_v1，无 D 盘时使用 C 盘相同后缀；Linux 为 ~/Projects/#4-Arclight/DCO_v1。本机配置、状态、凭据统一归 .local；工具归 .tools。临时空间由本机配置解析，任务隔离，正常/异常共用回收路径。

## 连接与身份

设备身份独立于连接地址。直连成功时使用直连；不可达时使用 frp 私有出站隧道。管理器只负责连接路径，调度和任务状态机保持一份。网络错误查询同一原 ID，原请求幂等重放，已接受任务不会因切路产生第二份。

隧道和 DCO 控制均验证身份。回环来源不代替访问授权，远程控制只有网站授权管理员可用，写请求执行 CSRF 和字段白名单检查。服务器不提供任意 URL、命令或脚本执行入口。

## 任务与资源合同

保留 Client.execute、python_map/python_call/python_task、map_items/iter_items/run_call、report_progress、check_deadline 及资产引用。工程提供冻结输入、真实有用并行上限、阶段需求和工作集；DCO负责排队、执行、动态授权和逐项领取。四工程接入通过各自薄适配层完成。

弹性阶段从可用的有效宽度工作，按就绪项扩核，尾项完成及时归还空槽。固定线程内核必须整组取得所需核数后执行；等待完整组的任务不占用计算槽。所有活跃线程保持授予直至安全归还。跨工程按 resource_share 与实际可执行需求分配，暂时用不上的份额供其他工程使用。

固定内核用 `run_call(limits={'exact_cpus': N, ...})` 声明真实线程宽度，任务 `max_cpus` 至少为N。支持 `resize_min_cpus` 的节点通过 `min_cpus` 整组准入；等待时0核但保留驻留内存。弹性 map/iter 的上限来自真实就绪工作，不由某台机器瞬时空闲量决定。迁移中的旧节点沿原资源语义执行已冻结请求。

内存调度区分解释器与共享输入基线、每项增量、冷准备峰值及后处理。声明用于准入，完整进程树实测用于校准；实际物理余量和 Windows 私有提交余量决定压力处理。资源预算减少只影响后续准入和可归还空槽，不杀健康任务。

CPU基础预算B、总线程硬限额H和超发倍率F只有两个自由度：H=floor(B×F)，编辑H反算F，编辑F或B更新H。设备的cpu_limit是基础预算只读上界。超发停止利用率、内存预算与物理/提交保留线沿同一受限控制API持久保存。界面同时显示配置值、有效值和实际占用；维护更新沿原生重载边界进行。

控制入口为 operator 专用 `GET/POST /control/resources`，写入 `{node, changes}`；八字段为 `cpus`、`cpu_limit`、`cpu_overcommit_factor`、`cpu_overcommit_utilization`、`memory_mb`、`memory_reserve_mb`、`memory_headroom_mb`、`memory_commit_headroom_mb`。`cpu_limit` 限基础预算，超发上限为基础预算乘倍率（1–1.5）；利用率阈值取1–100。下调预算保留健康活跃授权，新增准入按新值执行。

管理代理同时发送operator Bearer和 `X-DCO-Role: operator`。frpc由原生服务生命周期持有，只使用自身transport配置，不继承桌面的 `http_proxy`；显式代理仍由profile控制。

## 传输、结果与生命周期

源码、模型和数据按不可变摘要分开复用。大包流式传输、支持已有续传合同；控制请求与大包分开背压，状态按有界批次读取，稳定状态降低采样频率。网页只传紧凑当前快照，页面隐藏时降低刷新；已压缩大包不重复压缩。

展示发布器随原生supervisor生命周期独立运行，当前采样及状态保存于.local/liveboard；新目录自动建立。服务器以75秒浏览租约区分是否有人观看，发布器有人观看每15秒、无人观看每60秒采样上传，使用gzip+CBOR；正常运行每次约5KB。页面隐藏或关闭后停止轮询，浏览租约由网站各worker共享。资源控制读取仍直达同一个中心。

fetch(delete=False) → 工程持久发布结果/费用/恢复引用 → delete。任务与 attempt 费用累积，未知费用保留为未知。科学缓存键和合并规则由工程持有，DCO只提供内容与清单交换。

`.local/projects.json` 注册 `projects:{id:{root,adapter,requires,producer_group?}}`；root为本机工程源码，adapter为其中相对路径。工程提供manifest/export/merge/groups，DCO按不可变内容发送adapter和必要helper，远端共享根只需缓存。源代码/模型分组在本机工程构建，按摘要复用；ARH的独立producer资产通过可选producer_group声明。DCBFS使用dcbfs身份及实际reference协议，旧Bench数据保留原身份。

## 迁移

从 0.11.42 的有效合同提取实现，v1 使用独立目录与 Git 分支。旧服务先保持运行；每台节点原生交接时迁入原状态、身份和必要资产，再由 v1 接管。迁移期间每台节点只有一个调度所有者。各工程切换 SDK/中心入口时沿原 ID 查询恢复，不重新提交已付条件。完成后退役旧目录、旧发布入口和无消费者兼容代码，恢复证据保留在当前账本。

节点disabled也停止后续阶段扩核；维护时不能把0核等待当作任务完成。末批需要收尾资源时，短暂暂停中心新派发并保持节点enabled，原ID仍可正常借核；原任务结清后重载，再恢复新派发。迁移快照取于旧owner退出和锁释放之后，原校准缓存与科学状态一起迁移。
