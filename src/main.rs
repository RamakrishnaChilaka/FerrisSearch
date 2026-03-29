#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use ferrissearch::config::AppConfig;
use ferrissearch::node::Node;
use tracing::Level;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::prelude::*;

/// Custom event formatter that prepends `[node-id]` to every log line,
/// regardless of which task or thread emitted it.
struct NodeFormatter {
    node_id: String,
}

impl<S, N> tracing_subscriber::fmt::FormatEvent<S, N> for NodeFormatter
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        use tracing_subscriber::fmt::time::FormatTime;

        // Timestamp
        tracing_subscriber::fmt::time::SystemTime.format_time(&mut writer)?;

        // Level with ANSI colors
        let level = *event.metadata().level();
        if writer.has_ansi_escapes() {
            let (start, end) = match level {
                Level::ERROR => ("\x1b[31m\x1b[1m", "\x1b[0m"),
                Level::WARN => ("\x1b[33m\x1b[1m", "\x1b[0m"),
                Level::INFO => ("\x1b[32m", "\x1b[0m"),
                Level::DEBUG => ("\x1b[34m", "\x1b[0m"),
                Level::TRACE => ("\x1b[35m", "\x1b[0m"),
            };
            write!(writer, " {}{:>5}{}", start, level, end)?;
        } else {
            write!(writer, " {:>5}", level)?;
        }

        // Node ID — always present on every line
        write!(writer, " [{}]", self.node_id)?;

        // Log message and fields
        write!(writer, " ")?;
        ctx.format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load config first so we can include the node name in every log line
    let config = AppConfig::load()?;

    // Initialize tracing — every log line gets [node-id] prefix via custom formatter
    // Filter out noisy Tantivy/internal crate logs (they emit INFO for every segment file op)
    let filter = tracing_subscriber::filter::EnvFilter::builder()
        .with_default_directive(Level::INFO.into())
        .from_env_lossy()
        .add_directive("tantivy=warn".parse().unwrap())
        .add_directive("h2=warn".parse().unwrap())
        .add_directive("hyper=warn".parse().unwrap())
        .add_directive("tower=warn".parse().unwrap())
        .add_directive("tonic=warn".parse().unwrap())
        .add_directive("reqwest=warn".parse().unwrap());
    let fmt_layer = tracing_subscriber::fmt::layer().event_format(NodeFormatter {
        node_id: config.node_name.clone(),
    });
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(filter)
        .init();

    // Initialize and start the node
    let node = Node::new(config).await?;
    node.start().await?;

    Ok(())
}
