use ferrissearch::config::AppConfig;
use ferrissearch::node::Node;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set default tracing subscriber");

    // Load config from defaults, config/ferrissearch.yml, and FERRISSEARCH_* env vars
    let config = AppConfig::load()?;

    // Initialize and start the node
    let node = Node::new(config).await?;
    node.start().await?;

    Ok(())
}
