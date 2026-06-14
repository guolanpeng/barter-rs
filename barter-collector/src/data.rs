use barter_data::event::{DataKind, MarketEvent};
use barter_instrument::instrument::market_data::MarketDataInstrument;

pub type CollectorMarketEvent = MarketEvent<MarketDataInstrument, DataKind>;
