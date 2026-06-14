use crate::collector::config::BarterCollectorConfig;
use crate::msg_publisher::publisher::Publisher;
use barter_data::{
    event::DataKind,
    streams::{
        builder::dynamic::DynamicStreams,
        consumer::MarketStreamResult,
        reconnect::{Event, stream::ReconnectingStream},
    },
};
use barter_instrument::instrument::market_data::MarketDataInstrument;
use futures_util::StreamExt;
use tracing::{info, warn};

pub struct BarterCollector {
    config: BarterCollectorConfig,
    pub publisher: Publisher,
}

impl BarterCollector {
    pub fn new(config: BarterCollectorConfig) -> anyhow::Result<Self> {
        Ok(Self {
            config,
            publisher: Publisher::new()?,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let streams = DynamicStreams::init([self.config.instruments.clone()]).await?;
        let mut merged = streams
            .select_all::<MarketStreamResult<MarketDataInstrument, DataKind>>()
            .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

        while let Some(event) = merged.next().await {
            match event {
                Event::Item(event) => {
                    info!(?event, "received market event");
                    self.publisher.publish_market_event(&event).await?;
                }
                Event::Reconnecting(exchange) => {
                    warn!(%exchange, "market stream reconnecting");
                }
            }
        }

        Ok(())
    }
}
