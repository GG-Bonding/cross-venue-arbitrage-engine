# Phase 1 开发记录

按用户附件的分阶段要求完成，不提前启用实盘。

1. 新增双向 Spread、pending spread、连续确认、Quote 边界测试，首次 pytest 因 domain 尚未实现失败；实现最小模块后 **14 passed**。
2. 新增 Maker 精度、Post Only 拒绝、撤单迟滞/超时、撤单确认、配置保护、SQLite WAL、重复 Tick、重启 SAFE_MODE 测试；首次因执行模块缺失失败，实现后 **25 passed**。
3. 新增行情解析、MT5 worker 线程、缓存 Tick、连接异常和静默队列 watchdog 测试；首次因行情模块缺失失败，实现后 **30 passed**。
4. 新增 CLI demo、状态查询、实盘启动保护测试；首次 **2 failed, 1 passed**（入口尚未实现），实现后 **33 passed**。
5. 增加本地 HTTP/WebSocket 集成检查，**34 passed**；实际 Binance REST 检查发现 XAUUSDT 返回 TRADIFI_PERPETUAL，先新增失败回归测试，再修复类型解析，**35 passed**。
6. 已验证 editable 安装、`arbitrage --demo`、只读 status、`pip check` 与 Ruff。WebSocket 握手及消息等待均设有明确期限。外部联调限制记录于 API_NOTES.md。

所有测试正常执行，不使用 skip/xfail，不删除或降低断言。真实订单、实际成交及其 Partial Fill / Cancel-Fill 竞态测试属于后续阶段，不以模拟挂撤单测试代替。

## 用户提供已登录 MT5 后的增量

- 先写指定终端位置参数和 REST/WebSocket 代理转发失败测试，再修复，38 项通过。
- 真实联调发现 MT5 原始时钟约快 3 小时。先写时间归一化、原始值保留、禁止自动猜测、旧报价仍拒绝的回归测试，再实现显式偏移配置，41 项通过。
- 首次订阅实测返回全零 Tick；新增启动有限等待与运行期间空 Tick 报错的失败测试，再实现处理。
- 真实行情窗口、配置和限制见 MT5_INTEGRATION.md。测试中使用的模拟接口不发真实订单。

## 本地网页监控

先新增控制器与 HTTP 接口失败测试，再实现本地页面、生命周期和只读历史。
再新增会话锁失败测试并实现同库互斥。51 项自动测试通过。
实际 Edge 浏览器完成真实报价、停止/再启动、桌面/移动布局和 JavaScript 错误检查。
浏览器验证保持页面 CSP，不放宽生产页面限制；中文断言使用 UTF-8 文件执行。

## 循环挂撤单验收

只读审查本机真实行情下产生的模拟订单，补齐下单/撤单瞬间的审计证据。
新增 `python -m arbitrage.audit`，用 SQLite 只读事务取得一致快照，检查最新会话的生命周期和报价条件。
新测试覆盖两个方向的价格复核、只读保证、旧记录证据不足、价格/时间/确认/撤单原因被篡改和重复事件检测。
78 项自动测试通过。真实行情验收结果和不能证明的范围见 PAPER_AUDIT.md。
