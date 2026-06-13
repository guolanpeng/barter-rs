use barter_collector::collector::{collector::BarterCollector, config::BarterCollectorConfig};
use barter_data::subscription::SubKind::OrderBooksL2;
use barter_instrument::{
    exchange::ExchangeId::BinanceFuturesUsd,
    instrument::market_data::kind::MarketDataInstrumentKind::Perpetual,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let instruments = vec![
        // (BinanceFuturesUsd, "btc", "usdt", Perpetual, PublicTrades),
        (BinanceFuturesUsd, "btc", "usdt", Perpetual, OrderBooksL2),
        // (BinanceFuturesUsd, "btc", "usdt", Perpetual, Liquidations),
    ];

    let config = BarterCollectorConfig { instruments };
    let mut collector = BarterCollector::new(config);
    collector.run().await?;

    Ok(())
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with_ansi(cfg!(debug_assertions))
        .json()
        .init()
}
