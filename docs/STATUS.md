# DCO 当前状态

更新：2026-10-21。版本 0.8.7；实时任务、占用与已加载进程由看板和操作员 `health` 查询。操作见[README](../README.md)，机制见[DESIGN](DESIGN.md)，后续验收见[ROADMAP](ROADMAP.md)。

## 当前预算与能力

| 设备 | CPU预算 / 逻辑线程 | 计算RAM | RAM盘配置 |
| --- | ---: | ---: | ---: |
| Aya1-PC | 16 / 28 | 16GiB | 8GiB |
| Aya1-TX | 24 / 32 | 16GiB | 8GiB |
| ku26s | 52 / 104 | 64GiB | 16GiB |
| Aya1-ITX | 16 / 24 | 总内存减16GiB，约15.8GiB | 8GiB |

CPU合计108，按2026-10-21用户确认的16/24/52/16设置；调低额度保留在途任务并自然收敛。ku26s任务TMP目前使用普通磁盘；独立tmpfs列入空闲维护。RAM准入使用进程树RSS及系统实时8GiB余量，未采样派发暂按申报量预留。

| 语义能力 / 工具 | 已验证供给 |
| --- | --- |
| python_numpy、python_scientific、python_torch_pyg、onnx | 四设备合计108核；各环境所需包位于实际执行的同一解释器 |
| ng47、Verilator、CIRCT及相应编译器 | 按已登记工具匹配；包含make的ARH联合要求现覆盖108核 |
| layout_python、Magic、Netgen及布局材料 | Aya1-PC、Aya1-ITX环境就绪，合计32核；材料仍由任务内容资产提供 |
| HSPICE | 当前ku26s环境就绪，52核 |

以上为当前工具供给，不是设备或OS绑定策略。旧工程名和rgcn_fiber仅作原请求/模板兼容，公开能力使用规范环境名；已接受任务保持原绑定和回执。

## 验收与证据入口

证据位于既有 `results/repair-20261020/`，完整日志位于 `.local/maintenance/`。表中计数对应各自输入与源码范围，不合并成一次实验。

| 范围 | 当前结论 | 证据 |
| --- | --- | --- |
| TSA并发与单设备隔离 | 三机完成20个滚动观测点，1787个真实corner；排空尾部独立计时，原生产任务保留、节点恢复；CPU满线程可接近满载 | `results/tsa-pool-scaling-20261021/report.html`、`scaling.csv`、`isolation-ledger.json` |
| ITX部署与真实执行 | 标准worker、24逻辑CPU；科学/神经/ONNX、真实TSA、原生RTL编译运行、SKY130几何及8GiB物理RAM盘均通过，按用户确认提供16核 | `itx-onboarding-current.json`、`.local/maintenance/itx-real-canary/`、`itx-sky130-geometry/` |
| 八字符设备名 | 资源、明细、任务列保留八字符完整名称，兼容宽字符；看板与隔离针对性回归41项通过。运行中的看板在自然空闲重载时加载 | `tests/test_console.py`、`tests/test_center.py` |
| 能力、工具模板、缓存消费者及混合版本 | 全量401项：397通过、4项环境跳过；原ID与数值保持 | `.local/maintenance/capability-unification-release-full.log`；`capability-profiles-live-proof.json` |
| 规范能力部署与真实供给 | 三机自然空闲重载，60核授予及真实排队通过；Make联合要求60核 | `capability-idle-reload.json`、`capability-unification-after-reload.json`、`make-unification-admission.json` |
| 启动、独立服务、重载 | 35项：34通过、1项环境跳过；headless与看板开关独立 | `.local/maintenance/headless-startup-lifecycle.log`；`headless-policy-deployment-jobs.json` |
| 高并发客户端传输与恢复 | 26项通过；128并发共享8连接，本地FIFO等待独立于网络timeout，原任务不重复 | `.local/maintenance/client-pool-backpressure-tests.log` |
| 任务终态与迟到资源回执 | 95项通过；失败/取消期间的lease响应保持原结局、费用及资源释放 | `.local/maintenance/lease-terminal-preservation-tests.log` |
| 终端与短标签 | 39项通过；表头、多宽度、数字ID颜色及原标签保持 | `compact-task-tags-proof.json` |
| 语义缓存交换 | 80项通过；原生缓存命中、逻辑记录完整及重复同步零导出 | `.local/maintenance/semantic-cache-final.log` |
| 内存准入与迁移 | 实际RSS、阶段CPU/RAM借还、原检查点接续通过 | `production-policy-canaries.json`、`temporary-ku-memory-migration.json`、`ku-training-multicore-live.json` |
| Windows/Linux生命周期 | 可见终端、防自动休眠、进程树回收、端口所有权及原ID恢复通过 | `maintenance-reload-state.json`、`live-linux-power-after-reload.json`、`tx-wake-verified.json` |
| 跨工程真实执行 | AOP角点、RGCN训练/物理链、ARH原生块与Bench科学数组均有实测；科研结论归所属工程 | `aop-replica-placement-audit.json`、`arh-native64-warm-throughput.json`、`rgcn-layout-pc-readiness.json`、`cli-web-parity-current.json` |
| 人类约束与独立交叉审查 | 最新确认及逐项源代码核对已保存 | `design-intent-confirmed.json`、`dsh-cross-check-current.json` |

## 当前维护边界

常驻中心及worker继续承接原任务；启动选项源码已增量同步三机，新的SDK客户端可使用独立的连接排队策略。服务端机制升级在自然空闲边界加载，输入、回执、成本、检查点和未领取成果保持。

四科研工程默认Web端口为8000/8001/8002/8003，CLI与Web生命周期及当前研究分别由各工程README/STATUS说明。已完成入口验收与后续运行关联记录在 `cli-web-parity-current.json`；当前任务数量直接从服务查询。

GPU已具备静态档案、每卡容量匹配、整卡独占与显式CPU回退合同；真实GPU计算和Linux独立tmpfs的后续验收见ROADMAP。详细历史实测保留在原证据目录及Git，不在本页逐轮复述。
