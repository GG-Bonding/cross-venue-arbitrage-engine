# Phase 1 开发记录

按用户附件的分阶段要求完成，不提前启用实盘。

1. 新增双向 Spread、pending spread、连续确认、Quote 边界测试，首次 pytest 因 domain 尚未实现失败；实现最小模块后 **14 passed**。
2. 新增 Maker 精度、Post Only 拒绝、撤单迟滞/超时、撤单确认、配置保护、SQLite WAL、重复 Tick、重启 SAFE_MODE 测试；首次因执行模块缺失失败，实现后 **25 passed**。
3. 新增行情解析、MT5 worker 线程、缓存 Tick、连接异常和静默队列 watchdog 测试；首次因行情模块缺失失败，实现后 **30 passed**。
4. 新增 CLI demo、状态查询、实盘启动保护测试；首次 **2 failed, 1 passed**（入口尚未实现），实现后 **33 passed**。
5. 增加本地 HTTP/WebSocket 集成检查，**34 passed**；实际 Binance REST 检查发现 XAUUSDT 返回 TRADIFI_PERPETUAL，先新增失败回归测试，再修复类型解析，**35 passed**。
6. 已验证 editable 安装、`arbitrage --demo`、只读 status、`pip check` 与 Ruff。WebSocket 握手及消息等待均设有明确期限。外部联调限制记录于 API_NOTES.md。

所有测试正常执行，不使用 skip/xfail，不删除或降低断言。真实订单、实际成交及其 Partial Fill / Cancel-Fill 竞态测试属于后续阶段，不以模拟挂撤单测试代替。
