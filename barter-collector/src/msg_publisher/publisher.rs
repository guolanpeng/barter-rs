use std::time::Duration;

use anyhow::Context;
use barter_data::event::DataKind;
use barter_instrument::instrument::market_data::MarketDataInstrument;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use tracing::info;

use crate::data::CollectorMarketEvent;

const DEFAULT_BROKERS: &str = "127.0.0.1:9092";
const DEFAULT_MESSAGE_TIMEOUT_MS: &str = "5000";
const DEFAULT_DELIVERY_TIMEOUT_SECS: u64 = 5;

#[derive(Debug, Clone)]
pub struct PublisherConfig {
    pub brokers: String,
    pub message_timeout_ms: String,
    pub delivery_timeout: Duration,
    pub topics: TopicConfig,
}

impl PublisherConfig {
    pub fn from_env() -> Self {
        Self {
            brokers: std::env::var("REDPANDA_BROKERS")
                .unwrap_or_else(|_| DEFAULT_BROKERS.to_owned()),
            message_timeout_ms: std::env::var("REDPANDA_MESSAGE_TIMEOUT_MS")
                .unwrap_or_else(|_| DEFAULT_MESSAGE_TIMEOUT_MS.to_owned()),
            delivery_timeout: Duration::from_secs(
                std::env::var("REDPANDA_DELIVERY_TIMEOUT_SECS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(DEFAULT_DELIVERY_TIMEOUT_SECS),
            ),
            topics: TopicConfig::from_env(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub trades: String,
    pub book_ticker: String,
    pub orderbook_delta: String,
    pub klines: String,
    pub liquidations: String,
}

impl TopicConfig {
    pub fn from_env() -> Self {
        Self {
            trades: topic_from_env("REDPANDA_TOPIC_TRADES", "trades"),
            book_ticker: topic_from_env("REDPANDA_TOPIC_BOOK_TICKER", "book_ticker"),
            orderbook_delta: topic_from_env("REDPANDA_TOPIC_ORDERBOOK_DELTA", "orderbook_delta"),
            klines: topic_from_env("REDPANDA_TOPIC_KLINES", "klines"),
            liquidations: topic_from_env("REDPANDA_TOPIC_LIQUIDATIONS", "liquidations"),
        }
    }

    fn topic_for_kind(&self, kind: &DataKind) -> &str {
        match kind {
            DataKind::Trade(_) => &self.trades,
            DataKind::OrderBookL1(_) => &self.book_ticker,
            DataKind::OrderBook(_) => &self.orderbook_delta,
            DataKind::Candle(_) => &self.klines,
            DataKind::Liquidation(_) => &self.liquidations,
        }
    }
}

fn topic_from_env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

pub struct Publisher {
    pub producer: FutureProducer,
    config: PublisherConfig,
}

impl Publisher {
    pub fn new() -> anyhow::Result<Self> {
        Self::with_config(PublisherConfig::from_env())
    }

    pub fn with_config(config: PublisherConfig) -> anyhow::Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", &config.message_timeout_ms)
            .create()
            .with_context(|| {
                format!(
                    "failed to create Redpanda producer for brokers {}",
                    config.brokers
                )
            })?;

        Ok(Self { producer, config })
    }

    pub async fn publish_market_event(&self, event: &CollectorMarketEvent) -> anyhow::Result<()> {
        let topic = self.config.topics.topic_for_kind(&event.kind);
        let key = market_event_key(event);
        let payload = serde_json::to_string(&event).context("failed to serialise market event")?;

        let delivery = self
            .producer
            .send(
                FutureRecord::to(topic).key(&key).payload(&payload),
                self.config.delivery_timeout,
            )
            .await
            .map_err(|(error, _message)| error)
            .with_context(|| format!("failed to publish market event to topic {topic}"))?;

        info!(topic, key, ?delivery, "published market event to Redpanda");
        Ok(())
    }
}

fn market_event_key(event: &CollectorMarketEvent) -> String {
    let exchange = event.exchange.as_str().replace('_', "-");

    format!(
        "{}:{}{}:{}",
        exchange,
        event.instrument.base,
        event.instrument.quote,
        event_topic_kind(&event.kind),
    )
}

fn event_topic_kind(kind: &DataKind) -> &'static str {
    match kind {
        DataKind::Trade(_) => "trades",
        DataKind::OrderBookL1(_) => "book_ticker",
        DataKind::OrderBook(_) => "orderbook_delta",
        DataKind::Candle(_) => "klines",
        DataKind::Liquidation(_) => "liquidations",
    }
}
