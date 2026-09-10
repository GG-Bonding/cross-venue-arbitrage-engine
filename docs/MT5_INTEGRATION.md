# 已登录 MT5 终端接入验证

日期：2026-09-10。范围为 Phase 1，真实行情 + 本地模拟挂撤单。

## 本机接入

- Python 3.14，MetaTrader5 Python 包 5.0.6180，numpy 2.5.3。
- 指定当前项目的 `MetaTrader_init/terminal64.exe`，确认实际连接路径正确、终端在线且已有账户会话。本机其他终端未被操作。
- Broker 符号 `XAUUSD`：contract size=100，volume min=0.01，step=0.01，max=80。这些数值由 API 查询，不写死在执行器里。
- Binance XAUUSDT REST 和 `/public/ws/xauusdt@bookTicker` 通过本机已有 HTTP 代理接通。
- 不读取密码、不输出账户号/余额、不调用任何下单接口。终端文件及本机配置不提交 Git。

## 时间校验

首轮 60 秒中，58 个持久化样本的 MT5 原始时间比本机接收时间快约 10,799,880ms
（中位数），Binance 原始时间与本机时间相符。原有风控将这批报价判为 future，未创建模拟订单。

据此在本机配置中显式设置 `mt5.tick_time_offset_minutes: 180`，保留
`raw_exchange_ts_ms`，以 `raw - 180 * 60_000` 得到 UTC `exchange_ts_ms`。
库默认偏移仍为 0，不会通过不断拟合最新接收时间将旧报价变成“新报价”。
该偏移需要在 Broker/夏令时变化时重新核对。

## 归一化后的 60 秒运行

| 检查 | 结果 |
| --- | --- |
| 进程 | 正常退出，exit=0，无 stderr |
| 双市场快照 | 12,629 个（覆盖约 59.65 秒） |
| 有效快照 | 5,413 个，42.86% |
| Binance 接收时延 | 中位数 289ms，P95 1,988ms |
| MT5 接收时延 | 中位数 120ms，P95 136ms |
| 有效方向 A Spread | 3.17–4.08 |
| 入场阈值 | 4.20，保持原配置 |
| 模拟挂单 | 0：该窗口有效报价未达到阈值 |
| 行情保护 | stale/skew 正常拒绝，未放宽 300ms/200ms 阈值 |

这些指标仅描述本次窗口，不代表长期延迟或策略收益。接收时延包括网络和本地处理等待，
不能仅凭本次数据把全部延迟归因于网络。当前链路存在明显超时效报价，风控已经将其拦截。
模拟连续确认、挂撤单和断线处理由自动测试及离线演示另行验证，不能宣称该真实窗口发生了成交或套利。

本机原始日志和摘要位于被 Git 忽略的 `data/mt5-integration-utc.jsonl`、
`data/mt5-integration-summary.json`，订单和采样位于 `data/mt5-paper.db`。

## 继续运行

在项目根目录使用已配置好的本机配置：

```powershell
.\.venv\Scripts\python.exe -m arbitrage.main --config config/config.yaml --duration 60
.\.venv\Scripts\python.exe -m arbitrage.main --status --database data/mt5-paper.db
```

去掉 `--duration` 后持续观察，Ctrl+C 正常关闭模拟执行器；MT5 终端保持运行。
此阶段未实现真实 Binance/MT5 交易，完成行情连接不等于完成 Phase 2–4 实盘验收。
