use barter_collector::collector::{collector::BarterCollector, config::BarterCollectorConfig};

const DEFAULT_CONFIG_PATH: &str = "barter-collector/config/collector.json";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv()?;
    init_logging();

    let config_path =
        std::env::var("BARTER_COLLECTOR_CONFIG").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_owned());
    let config = BarterCollectorConfig::load_from_path(config_path)?;

    let mut collector = BarterCollector::new(config)?;
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
