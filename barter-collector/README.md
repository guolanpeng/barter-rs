# Barter Collector Redpanda 对接说明

本文档说明 `barter-collector` 对接 Redpanda 时，各 Topic 的用途、消息格式建议、运行验证方式，以及当前采集器写入 Redpanda 的实现方式。

## Redpanda Topic 规划

当前 Redpanda 中已创建以下 Topic：

```text
NAME             PARTITIONS  REPLICAS
book_ticker      1           1
funding_rate     1           1
klines           1           1
liquidations     1           1
orderbook_delta  1           1
test-topic       1           1
trades           1           1
```

各 Topic 建议按行情数据类型分工：

| Topic | 用途 | 存储数据 |
| --- | --- | --- |
| `trades` | 逐笔成交 | 实时成交明细，例如成交 ID、价格、数量、成交方向、成交时间、交易所、交易对。 |
| `book_ticker` | 最优买卖盘口 | L1 行情，即 best bid / best ask，例如买一价、买一量、卖一价、卖一量、事件时间。 |
| `orderbook_delta` | 订单簿增量 | L2 深度增量，例如 bids/asks 档位变化、更新序号、事件时间。用于维护本地 order book。 |
| `klines` | K 线 | OHLCV 蜡烛图数据，例如开盘价、最高价、最低价、收盘价、成交量、周期、开始/结束时间。 |
| `funding_rate` | 资金费率 | 永续合约资金费率，例如当前资金费率、预测资金费率、下一次结算时间、交易对。 |
| `liquidations` | 强平事件 | 合约强制平仓数据，例如方向、价格、数量、成交时间、交易所、交易对。 |
| `test-topic` | 连通性测试 | 仅用于 Producer/Consumer 链路验证，不存储正式行情数据。 |

当前所有 Topic 都是 `PARTITIONS=1`、`REPLICAS=1`：

- `PARTITIONS=1`：同一个 Topic 内全局有序，适合本地开发和小流量验证。
- `REPLICAS=1`：没有副本容灾，适合开发环境；生产环境建议根据集群节点数提升副本数。

## 消息 Key 规范

建议使用统一的消息 Key，保证同一交易所、同一交易对、同一数据类型进入同一分区：

```text
<exchange>:<symbol>:<kind>
```

示例：

```text
binance-futures-usd:btcusdt:trades
binance-futures-usd:btcusdt:book_ticker
binance-futures-usd:btcusdt:orderbook_delta
binance-futures-usd:btcusdt:funding_rate
binance-futures-usd:btcusdt:liquidations
```

如果后续某个 Topic 增加多个分区，Redpanda 会根据 Key 做分区路由。同一个 Key 的消息会进入同一个分区，从而保持该交易对在该数据类型下的顺序。

## Payload 格式建议

开发阶段可以先使用 JSON，便于排查和消费：

```json
{
  "exchange": "binance-futures-usd",
  "symbol": "btcusdt",
  "kind": "trades",
  "event_time": "2026-06-14T12:00:00Z",
  "data": {}
}
```

字段建议：

- `exchange`：交易所或市场，例如 `binance-futures-usd`。
- `symbol`：交易对，例如 `btcusdt`。
- `kind`：数据类型，应与 Topic 语义一致，例如 `trades`、`book_ticker`。
- `event_time`：交易所事件时间。
- `data`：原始或标准化后的行情主体。

后续如果吞吐量变大，可以再从 JSON 切换到 Protobuf、Avro 或 MessagePack。

## 连接配置

`barter-collector` 使用 `rdkafka` 通过 Kafka 协议连接 Redpanda：

```toml
rdkafka = { version = "0.39.0", features = ["cmake-build"] }
```

Producer 配置位于 `src/msg_publisher/publisher.rs`，默认从环境变量读取：

```rust
let brokers = std::env::var("REDPANDA_BROKERS")
    .unwrap_or_else(|_| "127.0.0.1:9092".to_owned());
```

可配置项：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REDPANDA_BROKERS` | `127.0.0.1:9092` | Redpanda broker 地址。 |
| `REDPANDA_MESSAGE_TIMEOUT_MS` | `5000` | librdkafka 消息超时时间。 |
| `REDPANDA_DELIVERY_TIMEOUT_SECS` | `5` | `FutureProducer::send` 等待投递结果的超时时间。 |
| `REDPANDA_TOPIC_TRADES` | `trades` | 逐笔成交 Topic。 |
| `REDPANDA_TOPIC_BOOK_TICKER` | `book_ticker` | L1 最优盘口 Topic。 |
| `REDPANDA_TOPIC_ORDERBOOK_DELTA` | `orderbook_delta` | L2 订单簿增量 Topic。 |
| `REDPANDA_TOPIC_KLINES` | `klines` | K 线 Topic。 |
| `REDPANDA_TOPIC_LIQUIDATIONS` | `liquidations` | 强平事件 Topic。 |

运行时可以指定 broker：

```bash
REDPANDA_BROKERS=<redpanda-host>:<port> cargo run -p barter-collector
```

## 连通性验证

查看 Topic：

```bash
rpk topic list
```

消费测试 Topic：

```bash
rpk topic consume test-topic
```

运行发送测试：

```bash
cargo run -p barter-collector --example redpanda-connect
```

示例会向 `test-topic` 发送一条测试消息：

```rust
FutureRecord::to("test-topic")
    .key("ping")
    .payload("hello redpanda second")
```

如果发送成功，终端会输出类似：

```text
send ok: (0, 0)
```

## 行情采集器

启动采集器：

```bash
cargo run -p barter-collector
```

默认采集配置位于 `barter-collector/config/collector.json`：

```json
{
  "instruments": [
    {
      "exchange": "binance_futures_usd",
      "base": "btc",
      "quote": "usdt",
      "instrument_kind": "perpetual",
      "subscription_kind": "order_books_l2"
    }
  ]
}
```

当前采集器会读取 Binance USD-M Futures 的 BTC-USDT 永续合约 L2 订单簿数据，并发布到 Redpanda。

可以通过环境变量指定其他配置文件：

```bash
BARTER_COLLECTOR_CONFIG=/path/to/collector.json cargo run -p barter-collector
```

## 写入 Redpanda 的实现

`BarterCollector::run` 会创建 `Publisher`，并在事件循环中发布行情事件：

```rust
while let Some(event) = merged.next().await {
    match event {
        Event::Item(event) => {
            publisher.publish_market_event(&event).await?;
        }
        Event::Reconnecting(exchange) => {
            warn!(%exchange, "market stream reconnecting");
        }
    }
}
```

`Publisher` 会根据 `DataKind` 自动路由到对应 Topic：

| `DataKind` | Topic |
| --- | --- |
| `Trade` | `trades` |
| `OrderBookL1` | `book_ticker` |
| `OrderBook` | `orderbook_delta` |
| `Candle` | `klines` |
| `Liquidation` | `liquidations` |

说明：当前 `barter-data::DataKind` 还没有 `FundingRate` 变体，所以 `funding_rate` Topic 已规划但尚未由采集器写入。

## 后续建议

- 为 `funding_rate` 增加数据源、标准化事件类型和 Topic 路由。
- 为 Redpanda 发送失败增加分类处理：可重试错误继续重试，不可重试错误写日志并告警。
- 增加发送指标，例如发送成功数、失败数、延迟、每个 Topic 的吞吐量。
- 生产环境根据吞吐量和容灾要求调整 Topic 分区数、副本数。
- 如果行情量变大，将 JSON Payload 切换到 Protobuf、Avro 或 MessagePack。

## 常见问题

连接失败时，先用 `rpk` 验证 Redpanda 地址：

```bash
rpk cluster info -X brokers=<host>:<port>
```

如果远程 Redpanda 无法连接，重点检查：

- `bootstrap.servers` 是否填写了客户端可访问的地址。
- Redpanda `advertise-kafka-addr` 是否配置为外部可访问地址。
- 云服务器安全组或防火墙是否放通 broker 端口。
- 集群是否启用了 SASL/TLS；如果启用，需要在 `ClientConfig` 中补充认证配置。
