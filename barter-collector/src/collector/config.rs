use barter_data::streams::builder::dynamic::DynamicStreams;
use barter_instrument::instrument::market_data::MarketDataInstrument;

pub struct BarterCollectorConfig {
    stream: DynamicStreams<MarketDataInstrument>,
}



