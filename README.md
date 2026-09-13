# Binance Futures + MT5 黄金跨平台点差套利系统

Python 3.12+，单进程 asyncio，Decimal 价格/数量，SQLite WAL。

默认提供真实行情 + Paper 挂撤单。新增交易工作台与显式启用的双边实盘执行：
Binance Post-only 入场、实际成交量 MT5 对冲、逐笔/批量平仓、条件平仓及成交回报。
实盘接口已有本地替身测试，尚未通过真实账户联调；当前本地监控仍为 Paper。
配置、账户模式要求、失败恢复和未实现项见 [实盘说明](docs/LIVE_TRADING.md)。

## 本地网页与手动条件挂单

```powershell
.\.venv\Scripts\python.exe -m arbitrage.main --web --config config/config.yaml
```

打开 http://127.0.0.1:8765，启动行情监控后手动创建条件单。没有条件单时，即使市场价差达到默认阈值，
系统也不会创建执行订单。全局方向选择和自动循环接口已停用。

每笔条件单独立设置 A/B 方向、入场价差、撤单价差、数量及是否循环。对应方向实时价差 ≥ 本单阈值时，
还须通过报价年龄/时间差检查和连续确认。价差阈值允许负数，撤单阈值不能高于入场阈值。

多笔同时满足时按队列顺序一次执行一笔。不合格的队首不阻塞后面的合格单。
当前 Maker 单撤销确认前不释放执行槽；循环属于具体条件单，每轮结束后排到队尾。
可以逐笔取消；刷新页面保留队列，停止/重启行情后待执行条件单暂停，必须逐笔恢复。

当前是 Paper：条件触发只代表创建模拟 Maker 挂单，尚不模拟成交、对冲或收益。
详见 [docs/WEB_MONITOR.md](docs/WEB_MONITOR.md)。关闭浏览器不会停止后台任务，请使用页面停止按钮清理。

挂撤单只读验收：

```powershell
.\.venv\Scripts\python.exe -m arbitrage.audit --database data/mt5-paper.db --output data/paper-audit.json
```

新执行订单保存触发瞬间的报价、条件单关联和规则；旧记录证据不足时显示 `limited`。
此前 Paper 验收结果见 [docs/PAPER_AUDIT.md](docs/PAPER_AUDIT.md)。

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
Paper 模式不发送 Binance 签名请求，也不调用 MT5 下单函数。
API Key / Secret 可填写在本地 config/config.yaml 的 binance.api_key / binance.api_secret；
本地配置被 Git 忽略，密钥不进入监控响应。配置方式见 [实盘说明](docs/LIVE_TRADING.md)。

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
因此 `binance_underlying_per_qty` 默认留空，不能仅凭 XAU 名称推断。实盘必须配置已核实乘数。

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

**以上换库恢复仅适用于本地模拟订单**。实盘启动核对双边持仓、方向与本地账本；
不确定执行不会自动重发，处理方式见 [实盘恢复说明](docs/LIVE_TRADING.md)。

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

实盘实现位于 execution/live_venues.py 与 strategy/live_orders.py，使用独立配置及数据库。
后续仍需真实账户联调、用户数据流、资金费归属、币种换算和不确定执行的恢复工具。
正常开/平仓均使用 Binance Maker，成交驱动 MT5 增量处理；Market 仅用于已确认敞口补偿。
V1 限制一笔 OPEN Pair；预计净利润模式需要明确配置费用预算。不得把离线测试作为收益或实盘稳定性证明。

官方接口核对链接见 [docs/API_NOTES.md](docs/API_NOTES.md)，测试先行记录见
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)。GitHub Actions 在 Linux Python 3.12 和 Windows
Python 3.14 上执行静态检查、测试和离线演示；测试不使用真实账户。
