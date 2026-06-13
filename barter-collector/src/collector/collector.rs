use barter_data::{
    event::DataKind,
    streams::{
        builder::dynamic::DynamicStreams, consumer::MarketStreamResult,
        reconnect::stream::ReconnectingStream,
    },
};
use barter_instrument::instrument::market_data::MarketDataInstrument;
use futures_util::StreamExt;
use tracing::{info, warn};

use crate::collector::config::BarterCollectorConfig;

pub struct BarterCollector {
    config: BarterCollectorConfig,
}

impl BarterCollector {
    pub fn new(config: BarterCollectorConfig) -> BarterCollector {
        Self { config }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let streams = DynamicStreams::init([self.config.instruments.clone()]).await?;
        let mut merged = streams
            .select_all::<MarketStreamResult<MarketDataInstrument, DataKind>>()
            .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

        while let Some(event) = merged.next().await {
            info!(?event, "received market event");
        }

        Ok(())
    }
}
