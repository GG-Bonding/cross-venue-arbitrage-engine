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
平仓与开仓共用一个执行槽，平仓队列优先。V1 的 live.max_open_pairs 固定为 1；已有 Pair 时其余条件等待。
旧库若有多个已知 OPEN 仍可串行平仓，但不新增 Pair。一次只处理一个 Pair 的执行，
其 Binance 撤单与 MT5 增量对冲可以重叠，MT5 自身仍单线程。

## 条件和数量

沿用原项目定义（不要按参考截图里的 A/B 平台顺序猜方向）：

| 方向 | 入场信号价差 | 平仓可执行价差 |
| --- | --- | --- |
| A：空 Binance / 多 MT5 | Binance Ask − MT5 Ask | Binance Buy Maker Bid − MT5 Bid |
| B：多 Binance / 空 MT5 | MT5 Bid − Binance Bid | MT5 Ask − Binance Sell Maker Ask |

入场价差 >= 本单入场阈值，通过有效行情与连续确认后提交 Binance GTX Post-only 限价单。
开仓成交不保证最终价差与信号相同：MT5 对冲有执行延迟和滑点。
平仓目标留空时逐笔手动平仓；填写时，可执行平仓价差 <= 目标并通过连续确认后排队平仓。
Maker 平仓价格按 tick 对齐。平仓阈值只控制触发/撤单，MT5 实际成交仍可能滑点。

MT5 手数 = Binance 实际成交量 × 已核实的标的乘数 / MT5 合约大小。
必须完全符合 Broker 最小手数和步长；不通过四舍五入制造敞口。
当前只支持 MT5 对冲账户、支持 FOK 的品种、Binance 双向持仓且单资产保证金模式。
本品种不能与外部策略混用；每次执行前核对本地账本、双边仓位和现有 Binance 挂单。

## 成交与失败

### 当前增量：平仓优先与事件唤醒

Maker 等待期间仍累计已有 OPEN 的平仓确认。确认后先撤新挂单、核对累计成交，
完成必要对冲/补偿，再由唯一执行槽关闭旧持仓。发平仓前复核最新有效行情和绝对阈值，
机会消失则保持 OPEN；人工平仓不被自动阈值覆盖。
未知结果保持 REVIEW/SAFE_MODE，本次不实现自动恢复，不盲目补单。

Live 使用私有订单推送；重复/乱序不重复对冲，健康流每秒 REST 补查，断流立即唤醒 REST。
行情/watchdog 可在慢 GET 期间判断撤单，按原 client ID 核对最终成交量。
执行期间推迟非必要账户/历史采样，MT5 仍使用同一单线程。

离线耗时报告：`python -m arbitrage.latency data/your-live.db`。
报告读取记录的单调时钟差值，包含本地等待/落盘/发送前检查，不包含网络与交易端成交耗时。
没有真实样本时不能据此判断实际性能；本轮只进行了离线验证。

### Maker ACK 与 Market RESULT

- Maker 入场调用 `submit_maker(..., price=...)`，发送 LIMIT、GTX、newOrderRespType=ACK，
  price 必填。ACK 只代表接单，不代表成交；缺少 status 或 executedQty 时查询原 client ID，
  不把缺失字段默认为 FILLED 或已成交数量。
- 风险补偿调用 `submit_market(...)`，发送 MARKET、newOrderRespType=RESULT，
  不携带 price 或 timeInForce。仍验证真实状态和累计成交数量，RESULT 也不能替代结果核对。
- 每个下单意图只发送一次 POST。超时或结果未知后查询原 client ID，不重新提交原订单。
  ACK 后查询失败（包括订单不存在）进入原 ID 撤单及最终成交量核对流程；
  撤单与成交竞争时以最终实际累计成交量决定对冲数量，无法确认保持 REVIEW/SAFE_MODE。

Maker 挂单期间撤单点差使用已记录的实际挂单价格：SHORT 为挂单卖价 − MT5 ask，
LONG 为 MT5 bid − 挂单买价。最新 Binance 盘口不改变已挂订单的价格。
仍使用原有报价有效性检查、撤单确认时长、超时、人工取消和停止行为。

ACK 拆分明确接口语义，可能额外增加一次查询；未实测下单延迟，不承诺性能提升，
不声称 GTX + RESULT 一定阻塞到成交。后续耗时测量、行情唤醒撤单、并行检查、用户数据流及
未知结果恢复的当前实现及边界见 [DEVELOPMENT.md](DEVELOPMENT.md)。

1. 意图和唯一 Binance client ID 先落盘，每个下单意图最多发送一次 POST。
2. Maker 累计成交增加时，按 cumulative_filled − processed_filled 计算未处理量。
   可表示为 MT5 最小手数/步长的增量立即对冲，同时撤 Binance 剩余单，不等撤单终态才开始对冲。
   撤单期间的新成交推送可继续触发增量；重复推送不重发。最后核对累计量不能倒退。
3. 每次 MT5 发送前记录唯一 tag 和 INTENT；成功回报且 ticket 持仓核对后才推进 processed_filled。
   一笔 Pair 可以对应多张 mt5_legs，记录每张 lots、closed_lots、ticket、identifier。
   下单结果未知保留 REVIEW/SAFE_MODE，不将未知量当成已对冲或未成交，不自动重发。
4. 开仓终态存在不可精确对冲的剩余量，只用 Market 平掉该剩余；保留已对冲配对量。
   不到 MT5 最小单位时不会勉强下单。若 Binance 剩余量也不符合过滤器或补偿结果不明，保持 REVIEW。
5. 正常平仓为反方向 LIMIT/GTX/ACK，累计成交驱动 MT5 ticket 增量关闭。
   平仓未成交或只完成部分时保留 OPEN，下一次自动触发重新连续确认；手动平仓需再次点击。
   撤单检查使用该张 Close Maker 的固定价格和 MT5 对手价，保留报价/时间/停止检查。
   平仓碎片无法精确关闭 MT5，或 MT5 下单前明确拒绝时，仅恢复 Binance 已确认、尚未关闭 MT5 的数量。
   MT5 结果未知时禁止这种恢复，以免反向制造敞口。
6. 明确 LIMIT GTX POST 返回 -5022 映射 PostOnlyWouldMatch，表示本次零成交拒单。
   入场回 WAITING、重置确认、下次新机会生成新 ID；普通拒单及未知结果不走此路径。
   不通过“查询不到订单”推断零成交。
7. 循环仍只在未成交撤销或完整平仓后排队；失败/REVIEW 不循环。

停止会话会停止新入场，等待当前执行完成必要撤单/对冲。
**停止监控不自动平掉已开的双边持仓**；停止后平仓目标不再监控。
关闭浏览器也不会停止后台会话。需要退出持仓时先使用平仓按钮并核对结果。

重启会暂停待入场条件单；已知 OPEN 持仓通过双边数量、方向、归属核对后可手动关闭。
未完成的执行意图进入 REVIEW，不自动重发。出现 REVIEW 时，在平台检查相应 client ID、
MT5 ticket 与成交回报；本次没有实现自动恢复，也不提供网页强制清除异常记录。
不可直接删除数据库或换库绕过账户敞口核对。

## 账户和收益

空闲时约每 3 秒采样账户，执行期间推迟；显示实际采样时间，失败保留旧数据并显示错误。
成交记录来自 Binance userTrades、MT5 history_deals_get；完整回报才能计算已实现收益。
Binance 与 MT5 分别按各自币种展示；非 USDT 手续费保留原币种记录。
记录区可查看信号、实际开/平仓点差和入场损耗，缺失成交价格时留空。
OPEN 列表报价估算收益点差 = 实际开仓点差 − 最新可平仓点差；过期报价不显示估算。
两腿数量和计价单位对齐后，毛收益 = 配对标的数量 ×（实际开仓点差 − 实际平仓点差）。
FAILED 补偿同样收集 Binance 开/平成交并进入 settlement_totals；不只统计成功交易。
汇总按币种显示完整结算，未结算有成交尝试单列，不计作零收益；USD 与 USDT 不直接相加。
Binance 资金费尚未分摊到单笔交易，不将未取得的费用写成零。
未实现自动账户费率/资金费归集、外部持仓导入、通用恢复、强平价或多交易对账户管理。

### 可选预计净利润平仓

条件单新增 min_net_profit（页面“预计净利润 ≥”），可以与 exit_threshold 同时填写；
同时填写时必须同时满足。固定点差模式保留。发单前及挂单期间重新检查。
费用先使用 live.profit_budget 的整笔预算：open_fees、close_fees（双平台合计）、funding、swap。
四项均要明确填写，quote_units_aligned=true 表示操作者已确认两腿计价单位及换算。
默认 null/false 表示未知，不是零：净利润条件不会触发。配置参考 config/live.example.yaml。
这些是人工预算，不是自动查询到账户费率；预算不能保证覆盖之后的资金费或滑点。

预计净利润 = 已取得的成交现金流 + 剩余配对按 Close Maker/当前 MT5 对手价退出的估算 − 整笔费用预算。
未发生部分平仓/补偿时等价于 配对标的数量 ×（实际入场点差 − 当前 Maker 退出点差）− 预算。
部分平仓、补偿及多个 MT5 ticket 的已知现金流全部归入原 Pair，不只统计成功部分。
缺少真实价格/旧库必要字段时估算留空；实际结算仍单独按币种展示，不以预算冒充已实现净收益。
停止监控仍不自动平持仓；本轮没有新增强制 Market 平仓按钮或自动恢复。

## 本地配置与启动

复制 config/live.example.yaml 为被 Git 忽略的 config/live.yaml。
设置实际 MT5 终端路径、Broker 时间偏移、代理及已核实的合约乘数；
确认账户模式符合要求后设置 live.enabled: true。
单笔数量还受 live.max_binance_qty 限制。

凭据可以直接填写在被 Git 忽略的本地 config/config.yaml 或 config/live.yaml：

```yaml
binance:
  api_key: "你的 API Key"
  api_secret: "你的 API Secret"
```

完整的配置文件密钥优先；两项都留空时才读取 BINANCE_API_KEY、BINANCE_API_SECRET 环境变量。
只填一项会拒绝执行，不会混用配置文件与环境变量的密钥。
密钥字段不进入配置序列化、监控响应或配置错误日志。不要在 example 文件填写真实密钥。
配置修改需要重新加载会话配置；目前最直接的方式是重启项目。
仍需要 CONFIRM_LIVE_TRADING=I_UNDERSTAND 才能启用交易执行。
填写密钥不会自动切换 Paper、Demo 或正式交易环境；Demo 接入需另行配置端点。
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

实现采用用户数据流加 REST 异常补查。REST 延迟或限流仍可能导致响应迟滞；
当前不承诺最大裸露时间，不应把本地接口测试视为真实账户验收。
