use std::time::Duration;

use barter_collector::msg_publisher::publisher::Publisher;
use rdkafka::producer::FutureRecord;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let publisher = Publisher::new()?;

    let result = publisher
        .producer
        .send(
            FutureRecord::to("test-topic")
                .key("ping")
                .payload("hello redpanda second"),
            Duration::from_secs(5),
        )
        .await;

    match result {
        Ok(delivery) => {
            println!("send ok: {:?}", delivery);
        }
        Err((error, _message)) => {
            eprintln!("send failed: {error:?}");
        }
    }

    Ok(())
}
