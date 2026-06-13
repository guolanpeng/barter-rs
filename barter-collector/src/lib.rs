pub mod collector;

use barter_data::{
    error::DataError,
    exchange::binance::futures::BinanceFuturesUsd,
    streams::{Streams, consumer::MarketStreamEvent, reconnect::stream::ReconnectingStream},
    subscription::{
        Subscription,
        trade::{PublicTrade, PublicTrades},
    },
};
use barter_instrument::instrument::market_data::{
    MarketDataInstrument, kind::MarketDataInstrumentKind,
};
use futures_util::Stream;
use std::pin::Pin;
use tracing::warn;

pub type PublicTradeReceiver =
    Pin<Box<dyn Stream<Item = MarketStreamEvent<MarketDataInstrument, PublicTrade>> + Send>>;

pub async fn subscribe_binance_futures_public_trades(
    instruments: impl IntoIterator<Item = MarketDataInstrument>,
) -> Result<PublicTradeReceiver, DataError> {
    let subscriptions: Vec<Subscription<BinanceFuturesUsd, MarketDataInstrument, PublicTrades>> =
        instruments
            .into_iter()
            .map(|instrument| {
                Subscription::new(BinanceFuturesUsd::default(), instrument, PublicTrades)
            })
            .collect();

    let streams = Streams::<PublicTrades>::builder()
        .subscribe(subscriptions)
        .init()
        .await?;

    let receiver = streams
        .select_all()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    Ok(Box::pin(receiver))
}

pub async fn default_public_trade_receiver() -> Result<PublicTradeReceiver, DataError> {
    subscribe_binance_futures_public_trades([
        MarketDataInstrument::new("btc", "usdt", MarketDataInstrumentKind::Perpetual),
        MarketDataInstrument::new("eth", "usdt", MarketDataInstrumentKind::Perpetual),
    ])
    .await
}
