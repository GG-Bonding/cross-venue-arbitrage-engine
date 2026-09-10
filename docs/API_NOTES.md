# API 核对记录

核对日期：2026-09-10。代码仅对接公共行情和本机终端只读接口。

- [Binance USDⓈ-M 公共 WebSocket](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public)：Individual Symbol Book Ticker Streams，使用 `wss://fstream.binance.com/public/ws/{symbol}@bookTicker`；`b/a` 为价格、`B/A` 为数量、`T` 为原始交易时间、`u` 为更新 ID。读取交易时间用于 Quote，保留本机接收时间。
- [Binance exchangeInfo](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)：公共 `GET /fapi/v1/exchangeInfo`。precision 不是 tickSize/stepSize 的替代品；解析 PRICE_FILTER、LOT_SIZE、MIN_NOTIONAL。
  实际访问该公共端点确认 XAUUSDT 的 `contractType=TRADIFI_PERPETUAL`，解析器同时识别普通 PERPETUAL 和 TradFi 永续，仍拒绝交割合约。已加入回归测试。
- [Binance 交易接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)：当前 LIMIT 的 timeInForce 枚举包含 GTX；立即成交冲突不得改为 Taker。本阶段仅在模拟订单上记录 GTX，不调用该接口。
- [MT5 symbol_info](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfo_py)：读取 trade_contract_size、volume_min、volume_max、volume_step。
- [MT5 symbol_info_tick](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfotick_py)：读取 bid、ask、time_msc；返回 None 时结合 last_error 报错。

公开 API 文档不能验证本机 Broker 的具体合约单位、当前账户权限或交易环境。
本次通过本地 HTTP/WebSocket 服务与 MT5 测试适配器验证解析、断连、重复事件和线程行为；
这些测试不代表真实账户联调或 Phase 2–4 的成交验证。

首次交付时 Binance 公共 REST 可用，WebSocket 超时，MT5 包尚未安装。
后续使用用户提供的已登录终端完成双市场真实行情联调：显式代理使 Binance WebSocket
正常收到报价，MT5 Python 包安装成功，确认指定终端和 XAUUSD 合约规格。
详细采样结果及仍存在的报价时延见 [MT5_INTEGRATION.md](MT5_INTEGRATION.md)。

[MT5 initialize 官方说明](https://www.mql5.com/en/docs/python_metatrader5/mt5initialize_py)
规定终端 EXE 路径为首个位置参数；接入代码按此传递，省略登录参数以复用既有会话。
本机采样发现 Broker 原始 Tick 时间与 UTC 存在固定约 3 小时差，这是本机观测，
不是所有 MT5 Broker 的统一规定。通过显式配置归一化，同时保留原始 time_msc。
