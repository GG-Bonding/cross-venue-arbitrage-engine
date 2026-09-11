# 双边交易工作台

本版本增加真实 Binance USDⓈ-M Futures / MT5 接口。默认仍运行 Paper。
接口契约与状态机测试使用本地替身，**未完成真实账户成交联调**。

## 界面

左侧交易对显示 A/B 入场价差；点击方向按钮或「添加挂单」打开建单弹窗。
每笔配置方向、入场价差、撤单价差、Binance 数量、循环，以及实盘可选的平仓价差。
列表支持当前/已结束/全部、方向筛选、复制为新单、暂停、恢复、取消和查看完整编号。
复制只预填表单，仍需点击创建。没有全局自动开仓，也没有绕过条件的立即买入按钮。

实盘显示两账户权益、可用资金、浮盈、更新时间，以及双边持仓和逐笔平仓。
「一键平仓」只排队关闭点击时本系统已记录的 OPEN 持仓；不操作外部持仓。
平仓与开仓共用一个执行槽，平仓队列优先。可同时持有多个已开仓交易对，
但任何时刻最多只有一笔开仓、对冲或平仓操作在执行。

## 条件和数量

沿用原项目定义（不要按参考截图里的 A/B 平台顺序猜方向）：

| 方向 | 入场信号价差 | 平仓可执行价差 |
| --- | --- | --- |
| A：空 Binance / 多 MT5 | Binance Ask − MT5 Ask | Binance Ask − MT5 Bid |
| B：多 Binance / 空 MT5 | MT5 Bid − Binance Bid | MT5 Ask − Binance Bid |

入场价差 >= 本单入场阈值，通过有效行情与连续确认后提交 Binance GTX Post-only 限价单。
开仓成交不保证最终价差与信号相同：MT5 对冲有执行延迟和滑点。
平仓目标留空时逐笔手动平仓；填写时，可执行平仓价差 <= 目标并通过连续确认后排队平仓。
平仓目标是交易请求触发条件，不能保证市场单最终成交价差。

MT5 手数 = Binance 实际成交量 × 已核实的标的乘数 / MT5 合约大小。
必须完全符合 Broker 最小手数和步长；不通过四舍五入制造敞口。
当前只支持 MT5 对冲账户、支持 FOK 的品种、Binance 双向持仓且单资产保证金模式。
本品种不能与外部策略混用；每次执行前核对本地账本、双边仓位和现有 Binance 挂单。

## 成交与失败

1. 先持久化执行意图及唯一 Binance client order ID，再向平台提交一次。
2. 使用 REST 查询累计成交；首次部分成交立即撤销剩余量，并查询最终状态，处理撤单与成交竞争。
3. 按最终实际成交量精确对冲 MT5。成交量不能表示为 MT5 手数、或 MT5 下单前检查明确拒绝，
   则用反方向平仓指令平掉 Binance 实际成交量，并将本次标记失败。
4. HTTP 超时不能视为拒单：查询原 client ID，必要时撤销同一 ID，绝不重新提交入场。
   MT5 下单后超时、无回报或部分成交进入 REVIEW，禁止盲目重发或猜测敞口。
5. 逐笔平仓先关 Binance 对应方向的记录数量，再按 MT5 position ticket 平仓。
   任一腿结果不明会进入 SAFE_MODE，禁止后续交易。
6. 循环单在本轮未成交撤销，或完整平仓后重新排到队尾；失败/待核对不循环。

停止会话会停止新入场，等待当前执行完成必要撤单/对冲。
**停止监控不自动平掉已开的双边持仓**；停止后平仓目标不再监控。
关闭浏览器也不会停止后台会话。需要退出持仓时先使用平仓按钮并核对结果。

重启会暂停待入场条件单；已知 OPEN 持仓通过双边数量、方向、归属核对后可手动关闭。
未完成的执行意图进入 REVIEW，不自动重发。出现 REVIEW 时，在平台检查相应 client ID、
MT5 ticket 与成交回报；当前尚未实现自动恢复不确定交易或网页强制清除异常记录。
不可直接删除数据库或换库绕过账户敞口核对。

## 账户和收益

账户数据约每 3 秒查询并显示采样时间；失败保留旧数据并显示错误。
成交记录来自 Binance userTrades、MT5 history_deals_get；完整回报才能计算已实现收益。
Binance 与 MT5 分别按各自币种展示；非 USDT 手续费保留原币种记录。
Binance 资金费尚未分摊到单笔交易，不将未取得的费用写成零。
当前未实现按合并净利润自动平仓、外部持仓导入、补仓、强平价计算或多交易对账户管理。

## 本地配置与启动

复制 config/live.example.yaml 为被 Git 忽略的 config/live.yaml。
设置实际 MT5 终端路径、Broker 时间偏移、代理及已核实的合约乘数；
确认账户模式符合要求后设置 live.enabled: true。
单笔数量还受 live.max_binance_qty 限制。

凭据只通过启动进程的环境变量读取，不通过网页、Git 或聊天传递。
需要 BINANCE_API_KEY、BINANCE_API_SECRET，以及 CONFIRM_LIVE_TRADING=I_UNDERSTAND。
程序不会自动读取 .env 文件。TRADING_MODE 环境变量会覆盖 YAML mode。

配置齐全后由操作者显式启动：

```powershell
$env:TRADING_MODE = 'live'
$env:CONFIRM_LIVE_TRADING = 'I_UNDERSTAND'
.\.venv\Scripts\python.exe -m arbitrage.main --web --config config/live.yaml --port 8766
```

页面右上标记 LIVE，点击启动实盘会话后仍需手动创建条件单。
不要同时运行另一 Paper 进程连接同一 MT5 终端；先停掉该监控会话。
Paper 和 live 强制分库；live 账本还绑定 API Key 与 MT5 账户身份摘要。
更换 Key 或账户必须先核对原持仓，不能把旧账本直接用于另一个账户。

## 接口依据

- [Binance Futures 交易 REST API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)
- [Binance 账户 REST API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account)
- [MT5 Python order_send](https://www.mql5.com/en/docs/python_metatrader5/mt5ordersend_py)

实现采用独立订单轮询，不是用户数据流。REST 延迟或限流可能导致响应迟滞；
当前不承诺最大裸露时间，不应把本地接口测试视为真实账户验收。
