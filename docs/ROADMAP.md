# DCO 当前路线

1. **节点接入与可用性**。新节点以实配和语义能力进入统一池。优先修复正在影响接单的路由或工具初始化，用一笔真实工程检查结果和费用。
2. **并发调度与短轮询**。保持 600／800 个原 ID 的隔离负载验收。用真实短任务回归批量状态、吞吐及尾延迟。[隔离验收](../.local/migration/dco-108-concurrency-current.json) · [生产验收](../.local/migration/dco-108-production-load-current.json)
3. **缓存与繁忙链路**。继续减少跨网重复字节，验收大输入上传期间的小结果时延和跨节点缓存命中。[真实中转结果](../.local/migration/dco-108-relay-overlap-after-cache.json)

   1.0.52 已在隔离环境验证解包并行和分段状态。人工切换后用自然原单复核两种生产长尾：资产准备曾达 192–271 秒，热源码命中后的任务交接曾达 416.90 秒。按连接槽等待、发送、回包和 worker 解包数据定位剩余耗时。[隔离回执](../.local/migration/qualification-1052/handoff.json)

短任务和制品复用已有生产回执。后续版本按实际受影响的链路复验。[当前生产验收](STATUS.md)
