use std::path::Path;

use anyhow::Context;
use barter_data::subscription::SubKind;
use barter_instrument::{
    asset::name::AssetNameInternal, exchange::ExchangeId,
    instrument::market_data::kind::MarketDataInstrumentKind,
};
use serde::{Deserialize, Serialize};

pub type CollectorSubscription = (
    ExchangeId,
    AssetNameInternal,
    AssetNameInternal,
    MarketDataInstrumentKind,
    SubKind,
);

#[derive(Debug, Clone, Serialize)]
pub struct BarterCollectorConfig {
    pub instruments: Vec<CollectorSubscription>,
}

impl BarterCollectorConfig {
    pub fn load_from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read collector config {}", path.display()))?;

        let raw: RawBarterCollectorConfig = serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse collector config {}", path.display()))?;

        raw.try_into()
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawBarterCollectorConfig {
    instruments: Vec<RawCollectorSubscription>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawCollectorSubscription {
    exchange: ExchangeId,
    base: AssetNameInternal,
    quote: AssetNameInternal,
    instrument_kind: MarketDataInstrumentKind,
    subscription_kind: RawSubKind,
}

impl TryFrom<RawBarterCollectorConfig> for BarterCollectorConfig {
    type Error = anyhow::Error;

    fn try_from(value: RawBarterCollectorConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            instruments: value
                .instruments
                .into_iter()
                .map(CollectorSubscription::from)
                .collect(),
        })
    }
}

impl From<RawCollectorSubscription> for CollectorSubscription {
    fn from(value: RawCollectorSubscription) -> Self {
        (
            value.exchange,
            value.base,
            value.quote,
            value.instrument_kind,
            value.subscription_kind.into(),
        )
    }
}

#[derive(Debug, Copy, Clone)]
struct RawSubKind(SubKind);

impl From<RawSubKind> for SubKind {
    fn from(value: RawSubKind) -> Self {
        value.0
    }
}

impl<'de> Deserialize<'de> for RawSubKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        match input.as_str() {
            "public_trades" | "PublicTrades" => Ok(Self(SubKind::PublicTrades)),
            "order_books_l1" | "OrderBooksL1" => Ok(Self(SubKind::OrderBooksL1)),
            "order_books_l2" | "OrderBooksL2" => Ok(Self(SubKind::OrderBooksL2)),
            "order_books_l3" | "OrderBooksL3" => Ok(Self(SubKind::OrderBooksL3)),
            "liquidations" | "Liquidations" => Ok(Self(SubKind::Liquidations)),
            "candles" | "Candles" => Ok(Self(SubKind::Candles)),
            _ => Err(serde::de::Error::unknown_variant(
                input.as_str(),
                &[
                    "public_trades",
                    "order_books_l1",
                    "order_books_l2",
                    "order_books_l3",
                    "liquidations",
                    "candles",
                ],
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use barter_data::subscription::SubKind;
    use barter_instrument::{
        exchange::ExchangeId, instrument::market_data::kind::MarketDataInstrumentKind,
    };

    use super::*;

    #[test]
    fn parses_json_config() {
        let input = r#"
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
        "#;

        let raw: RawBarterCollectorConfig = serde_json::from_str(input).unwrap();
        let config = BarterCollectorConfig::try_from(raw).unwrap();

        assert_eq!(config.instruments.len(), 1);
        assert_eq!(config.instruments[0].0, ExchangeId::BinanceFuturesUsd);
        assert_eq!(config.instruments[0].3, MarketDataInstrumentKind::Perpetual);
        assert_eq!(config.instruments[0].4, SubKind::OrderBooksL2);
    }
}
