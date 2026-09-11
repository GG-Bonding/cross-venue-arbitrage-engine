# Binance Futures + MT5 黄金跨平台点差套利系统

Python 3.12+，单进程 asyncio，Decimal 价格/数量，SQLite WAL。

当前交付 **Phase 1：只读实时行情 + Paper 挂撤单**，遵循需求“首先只开始 Phase 1”。
Binance 和 MT5 的真实下单接口均未实现。`TRADING_MODE=live` 未确认时启动失败；
即使设置 `CONFIRM_LIVE_TRADING=I_UNDERSTAND`，Phase 1 仍拒绝 live。

## 本地网页监控

双向实时入场价差显示在顶部报价下方。启动网页行情会话后默认仅监控；选择方向后点击“创建单次挂单”
或“启动循环挂单”，条件满足后才创建 Paper 模拟单。“停止挂单并撤单”保留行情显示。
当前没有模拟成交，循环会在模拟撤单后重新等待下一次入场机会；重启行情会话不会自动恢复挂单任务。

页面可选择“仅 A”“仅 B”或“双向自动”允许模拟挂单，两个方向的入场价差均保留显示。
运行中切换会重新累计确认，并先撤销不再允许方向的模拟挂单。配置默认方向使用
`entry.direction_mode: a` / `b` / `both`；网页选择保留到本次 Python 服务结束。

安装依赖并配置好 MT5 后，在项目根目录运行：

```powershell
.\.venv\Scripts\python.exe -m arbitrage.main --web --config config/config.yaml
```

浏览器打开 **http://127.0.0.1:8765**，点击“启动 Paper 监控”。
服务默认等待操作；“停止监控”走模拟订单清理流程，页面保留可查看的最后报价和历史订单。
重新启动时先核对数据库，异常不会自动重试。需要更换端口时添加 `--port 8766`。
`--web` 不与 `--demo`、`--status` 或 `--duration` 一起使用。

页面提供双边 Bid/Ask、接收延迟/报价年龄、双向 Spread/Edge、确认次数与时长、
价差曲线、当前模拟挂单、最近 50 笔订单和最近运行事件。支持桌面及手机尺寸布局。
浏览器每约 0.5 秒采样内存状态，历史订单每约 5 秒刷新；页面采样不会增加策略 Tick 次数。
行情过期、断线或引擎停止时不再展示有效信号；旧报价会变灰并显示状态。
图表的无效采样为断档，不当作 0，也不用于收益计算。

**当前 Paper 模式的含义：** 读取真实 Binance/MT5 行情，模拟挂单和撤单，但订单只存在于
本地 SQLite，不向 Binance/MT5 发送交易请求。当前阶段尚未模拟成交、执行 MT5 对冲、
计算手续费后的 PnL；页面不会展示虚构成交或收益。它也不是 Binance 测试网交易。

网页仅绑定 `127.0.0.1`，不开放局域网和公网，不依赖外部 CDN。
控制接口验证本地 Host、Origin 和会话 token；行情 JSON 不包含账户凭据或终端路径。
同一数据库的实时 Paper 会话由操作系统锁互斥，网页与另一个命令行不能同时运行该策略。
异常退出后操作系统释放锁，锁文件保留不代表仍被占用。旧版本进程应先停止再使用本版本。
关闭浏览器不会停止后台监控，请使用页面停止按钮；关闭网页服务时 Ctrl+C 会清理 Paper 会话。
详见 [docs/WEB_MONITOR.md](docs/WEB_MONITOR.md)。

## 快速运行

Windows PowerShell，在仓库根目录执行：

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.lock
.\.venv\Scripts\python.exe -m pip install -e . --no-deps
.\.venv\Scripts\python.exe -m arbitrage.main --demo
```

如果本机只有 Python 3.14，第一行改为 `py -3.14 -m venv .venv`。
Linux/macOS 可使用 `python3 -m venv .venv` 和 `.venv/bin/python`；MT5 行情需要 Windows。

离线演示使用打包的**合成报价和合成合约规格**，不联网，不需要账户。
按事件时间推进：连续三次确认 → SELL Maker → MT5 ask 上升使价差失效 →
持续 50ms → 请求撤单 → 模拟确认。默认数据库为 `data/demo.db`，与实时行情模式隔离。
演示不模拟成交，因此不会把挂单成功或盘口触价当作真实 Fill，不会生成虚假 PnL。

```powershell
.\.venv\Scripts\python.exe -m arbitrage.main --status --database data/demo.db
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check src tests
```

## Binance + MT5 实时行情

先安装并登录 Windows MT5 终端，确认 Broker 的黄金品种名称（可能带后缀），再执行：

```powershell
.\.venv\Scripts\python.exe -m pip install -e '.[mt5]'
Copy-Item config/config.example.yaml config/config.yaml
$env:TRADING_MODE = 'paper'
.\.venv\Scripts\python.exe -m arbitrage.main --config config/config.yaml --duration 60
```

不指定 `--duration` 时持续监控，Ctrl+C 关闭并撤销本地模拟挂单。
MetaTrader5 二进制包需要与你的 Windows/Python 版本匹配；若安装失败，可用 Python 3.12 环境。
通过 `mt5.terminal_path` 选择终端，通过 `symbol.mt5` 设置 Broker 实际符号。
程序不读取 Binance API key，不发送签名请求，也不调用 MT5 下单函数。

本地已有 `MetaTrader_init/terminal64.exe` 时，在 `config/config.yaml` 的
`mt5.terminal_path` 填写该文件的绝对路径，程序复用终端保存的登录会话。
首次订阅品种返回全零 Tick 时最多等待 `mt5.quote_startup_timeout_ms`（默认 5000ms），
期间不生成 Quote；超过期限或已接收到行情后再出现空 Tick 时停止并报错。
`MetaTrader_init/` 整个目录和 `config/config.yaml` 均被 Git 忽略。

如果 Binance 需要本机 HTTP 代理，设置 `market.binance_proxy_url`，例如
`http://127.0.0.1:7890`。该代理用于 Binance REST 和 WebSocket，不改变系统代理或 MT5 设置。

有些 Broker 的 `time_msc` 带服务器时区偏移。确认后设置 `mt5.tick_time_offset_minutes`，
含义是 **原始时间 − UTC**，例如原始时间比 UTC 快 3 小时则填 `180`。
默认值为 `0`，程序不会自动估算偏移或用接收时间替换报价时间。
Quote 的 `exchange_ts_ms` 保存归一化后的 UTC，`raw_exchange_ts_ms` 保留 MT5 原始值。
freshness/skew 检查仍使用原有阈值；Broker 切换夏令时后需要重新核对偏移。
本机联调结果见 [docs/MT5_INTEGRATION.md](docs/MT5_INTEGRATION.md)。

启动时从 Binance `exchangeInfo` 读取 tickSize、stepSize、minQty、maxQty、minNotional 和
quantityPrecision/pricePrecision；从 MT5 `symbol_info()` 读取 contract size 和手数限制。
缺少过滤器、品种不存在或暂停交易时失败退出，不替换成其他品种或猜测规格。
所有 MT5 API 操作都在一个独立工作线程中执行。

Binance 数量与 MT5 lot 不等价。`HedgeCalculator` 按显式 underlying multiplier 和实际
MT5 contract size 转换；不能精确表示的手数报错。`exchangeInfo` 不保证提供盎司换算单位，
因此 `binance_underlying_per_qty` 默认留空，不能仅凭 XAU 名称推断。Phase 1 不执行对冲。

## 策略行为

| 项目 | 实现 |
| --- | --- |
| 方向 A | Binance ask − MT5 ask；Binance SELL Maker |
| 方向 B | MT5 bid − Binance bid；Binance BUY Maker |
| Edge | RawSpread − entry.threshold |
| 连续确认 | 同时满足 min_ticks 和 min_duration_ms；Edge < 0 或无效行情复位 |
| 行情有效性 | 本机接收时间、交易所原始时间均检查 age，并检查两交易所时间 skew |
| Maker 价格 | SELL ask 向上按 tickSize 取整；BUY bid 向下取整；数量按 stepSize 向下取整 |
| 挂单价差 | 使用已创建订单的价格和当前 MT5 Bid/Ask |
| 撤单迟滞 | pending_spread < cancel_threshold 连续达到 cancel_confirm_ms |
| 挂单超时 | 独立 watchdog 执行 max_pending_ms；无新 Tick 也能撤单 |
| 断流/过期 | 复位确认并取消本地挂单；连接异常终止进程 |
| 容量 | Phase 1 固定一个模拟挂单槽，禁止同一 Tick 重复触发 |
| 成交 | 本阶段不模拟 Fill，不执行 Hedge，不报告交易收益 |

重复 Binance update ID、旧 MT5 Tick 不会刷新有效性；定时器不增加确认 Tick 数。
无效行情不计算新 Spread/Edge。行情时钟要求 UTC 同步；时间超前也按无效处理。
连接关闭、行情队列溢出、API 异常、关键 SQLite 写入失败均报上下文并终止，未做无限重连。
这意味着 Binance 定期断开连接后需重新启动并检查状态。

日志为英文 JSON Lines，含 UTC 时间、两侧 Bid/Ask、两个方向的 RawSpread/Edge、确认 count、
duration_ms、确认状态和策略状态。示例中的 Decimal 会作为字符串保存：

```json
{"SHORT_BINANCE":{"raw_spread":"4.36","edge":"0.16","state":"CONFIRMED","count":3,"duration_ms":200}}
```

Metrics 包含行情 latency/skew、stale/skew counters、两个方向 Edge、确认耗时、
pending_spread/cancel_edge、挂单等待和撤单耗时；关闭时写入 `paper_shutdown` 事件。
后续成交/PnL metrics 要等真实成交管线实现后才产生。

## 持久化与恢复

SQLite 开启 WAL 和 synchronous=FULL。挂单状态及对应事件使用同一个事务提交。
保留 `quotes_sample`、`strategy_events`、`maker_orders`、`maker_fills`、`mt5_orders`、
`pairs`、`pair_events`、`fees`、`risk_events` 表；后续阶段用表当前为空，使用事件 payload 结构预留。
行情快照按 `database.quote_sample_ms` 采样，确认开始/复位/完成和每次挂撤单都完整记录。
数据库和本地配置被 Git 忽略；运行时间较长时需自行归档行情样本，本阶段不自动删除历史。

重启查到 `MAKER_PENDING` / `CANCELING` 等未完成本地记录时进入 `SAFE_MODE`，只显示行情。
`--status --database <path>` 用 SQLite 只读模式查看，不创建文件。
正常退出会确认取消模拟单。若发生强制终止，先检查旧库；确认是本版本的纯模拟记录后，
可保留原库作审计并用 `--database data/new-paper-session.db` 开始新的独立模拟会话。
没有自动清空订单或猜测状态的恢复开关。

**此恢复仅适用于 Phase 1 本地模拟订单**。Binance 账户订单/持仓、MT5 持仓、DB Pair 的
跨账户 reconciliation 尚未实现，不可拿本版本的 SAFE_MODE 作为实盘仓位保护。

## 目录和开发阶段

```text
config/                  示例配置
src/arbitrage/
  main.py                CLI：实时行情、离线 demo、只读 status
  config.py              配置验证与模式保护
  domain/                Quote、方向/状态、MakerOrder、合约规格
  market/                Binance WebSocket、MT5 worker、队列消费、demo
  monitor/               本地网页、Paper 会话控制、状态和历史接口
  strategy/              Spread、连续确认、单槽策略
  execution/             本地 Maker 与撤单迟滞
  risk/                  行情有效性检查
  persistence/           SQLite 事件和订单事务
  observability.py       JSON 日志、Decimal 序列化、metrics
tests/                   核心计算、执行、线程/传输、CLI 测试
docs/                    API 核对与开发验证记录
```

Phase 2 才增加 Binance 真实 Maker、User Data Stream、Partial Fill/重复事件幂等、
Cancel/Fill 竞态和真实账户 reconciliation；Phase 3 增加真实 MT5 Hedge、有限重试和
Emergency Risk Handler；Phase 4 增加 Maker 平仓、真实费用/funding/swap、USD/USDT 换算和净收益；
Phase 5 在稳定验证后增加多档位。本次没有提前实现这些实盘能力。

官方接口核对链接见 [docs/API_NOTES.md](docs/API_NOTES.md)，测试先行记录见
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)。GitHub Actions 在 Linux Python 3.12 和 Windows
Python 3.14 上执行静态检查、测试和离线演示；测试不使用真实账户。
