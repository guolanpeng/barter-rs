use barter_data::subscription::SubKind;
use barter_instrument::{
    exchange::ExchangeId, instrument::market_data::kind::MarketDataInstrumentKind,
};

pub type CollectorSubscription = (
    ExchangeId,
    &'static str,
    &'static str,
    MarketDataInstrumentKind,
    SubKind,
);

pub struct BarterCollectorConfig {
    pub instruments: Vec<CollectorSubscription>,
}
