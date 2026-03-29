use anyhow::{Context, Result, bail};
use clap::Parser;
use colored::Colorize;
use comfy_table::{
    Cell, CellAlignment, Color, ContentArrangement, Table, modifiers::UTF8_ROUND_CORNERS,
    presets::UTF8_FULL,
};
use reqwest::Client;
use rustyline::DefaultEditor;
use serde_json::Value;
use std::time::{Duration, Instant};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const MAX_COLUMN_WIDTH: usize = 60;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Parser)]
#[command(
    name = "ferris-cli",
    about = "FerrisSearch SQL Console — interactive SQL shell for FerrisSearch",
    version
)]
struct Args {
    /// FerrisSearch host
    #[arg(short = 'H', long, default_value = "localhost")]
    host: String,

    /// FerrisSearch HTTP port
    #[arg(short, long, default_value_t = 9200)]
    port: u16,

    /// Execute a single SQL query and exit
    #[arg(short = 'c', long)]
    command: Option<String>,
}

struct FerrisClient {
    client: Client,
    base_url: String,
    host: String,
    port: u16,
}

impl FerrisClient {
    fn new(host: &str, port: u16) -> Self {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("Failed to create HTTP client");
        Self {
            client,
            base_url: format!("http://{}:{}", host, port),
            host: host.to_string(),
            port,
        }
    }

    fn connection_label(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    async fn health(&self) -> Result<Value> {
        let resp = self
            .client
            .get(format!("{}/_cluster/health", self.base_url))
            .send()
            .await
            .context("Failed to connect to FerrisSearch")?;
        Ok(resp.json().await?)
    }

    async fn execute_sql(&self, query: &str) -> Result<(Value, f64)> {
        let start = Instant::now();
        let resp = self
            .client
            .post(format!("{}/_sql", self.base_url))
            .json(&serde_json::json!({"query": query}))
            .send()
            .await
            .context("Query execution failed")?;

        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        let status = resp.status();
        let body: Value = resp.json().await.context("Failed to parse response")?;

        if !status.is_success() {
            let reason = body
                .pointer("/error/reason")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            bail!("{}", reason);
        }

        Ok((body, elapsed))
    }

    async fn explain_sql(&self, query: &str, index: &str) -> Result<(Value, f64)> {
        let start = Instant::now();
        let resp = self
            .client
            .post(format!("{}/{}/_sql/explain", self.base_url, index))
            .json(&serde_json::json!({"query": query, "analyze": true}))
            .send()
            .await
            .context("EXPLAIN ANALYZE failed")?;

        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        let status = resp.status();
        let body: Value = resp.json().await.context("Failed to parse response")?;

        if !status.is_success() {
            let reason = body
                .pointer("/error/reason")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            bail!("{}", reason);
        }

        Ok((body, elapsed))
    }
}

fn print_welcome(cluster_name: &str, node_count: usize, connection: &str) {
    println!();
    let art = [
        r"    ______               _      _____                      __  ",
        r"   / ____/__  __________(_)____/ ___/___  ____ ___________/ /_ ",
        r"  / /_  / _ \/ ___/ ___/ / ___/\__ \/ _ \/ __ `/ ___/ ___/ __ \",
        r" / __/ /  __/ /  / /  / (__  )___/ /  __/ /_/ / /  / /__/ / / /",
        r"/_/    \___/_/  /_/  /_/____//____/\___/\__,_/_/   \___/_/ /_/ ",
    ];
    for line in &art {
        println!("{}", line.bright_yellow());
    }
    println!();
    println!(
        " {} v{}",
        "FerrisSearch SQL Console".bright_white().bold(),
        VERSION,
    );
    println!(
        " Connected to {} — {} cluster, {} node(s)",
        connection.bright_cyan(),
        cluster_name.bright_cyan(),
        node_count.to_string().bright_green(),
    );
    println!();
    println!(
        " Type {} for commands, {} to exit. End queries with {}",
        "\\help".bright_cyan(),
        "\\q".bright_cyan(),
        ";".bright_cyan(),
    );
    println!();
}

fn print_help() {
    println!();
    println!(" {}", "SQL Commands:".bright_white().bold());
    println!(
        "   {}                   — list all indices with doc counts",
        "SHOW TABLES".bright_cyan()
    );
    println!(
        "   {}              — list all indices (alias)",
        "SHOW INDICES".bright_cyan()
    );
    println!(
        "   {}            — show field mappings",
        "DESCRIBE <index>".bright_cyan()
    );
    println!(
        "   {}               — show field mappings (alias)",
        "DESC <index>".bright_cyan()
    );
    println!(
        "   {}  — show index creation JSON",
        "SHOW CREATE TABLE <index>".bright_cyan()
    );
    println!(
        "   {}               — run a SQL query",
        "SELECT ...".bright_cyan()
    );
    println!();
    println!(" {}", "Special Commands:".bright_white().bold());
    println!(
        "   {}        — run SQL with execution breakdown",
        "EXPLAIN <query>".bright_cyan()
    );
    println!(
        "   {}     — list indices (shortcut)",
        "\\tables / \\indices".bright_cyan()
    );
    println!(
        "   {}                    — show this help",
        "\\help".bright_cyan()
    );
    println!(
        "   {}                    — clear the screen",
        "\\clear".bright_cyan()
    );
    println!(
        "   {}                    — exit the console",
        "\\quit".bright_cyan()
    );
    println!(
        "   {} / {}                — exit the console",
        "\\q".bright_cyan(),
        "exit".bright_cyan()
    );
    println!();
    println!(" {}", "Examples:".bright_white().bold());
    println!(
        "   {}",
        r#"SELECT * FROM "hackernews" WHERE text_match(title, 'rust') LIMIT 10;"#.dimmed()
    );
    println!("   {}", r#"SELECT author, count(*) AS posts FROM "hackernews" WHERE text_match(title, 'python') GROUP BY author ORDER BY posts DESC LIMIT 5;"#.dimmed());
    println!("   {}", r#"EXPLAIN SELECT title, upvotes FROM "hackernews" WHERE text_match(title, 'startup') AND upvotes > 100 ORDER BY upvotes DESC LIMIT 10;"#.dimmed());
    println!();
}

fn render_table(columns: &[String], rows: &[Value]) {
    if columns.is_empty() || rows.is_empty() {
        println!(" {}", "(empty result set)".dimmed());
        return;
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic);

    // Header row
    let header: Vec<Cell> = columns
        .iter()
        .map(|col| {
            Cell::new(col)
                .set_alignment(CellAlignment::Center)
                .fg(Color::Cyan)
        })
        .collect();
    table.set_header(header);

    // Data rows
    for row in rows {
        let cells: Vec<Cell> = columns
            .iter()
            .map(|col| {
                let val = row.get(col).unwrap_or(&Value::Null);
                let text = format_value(val);
                let cell = Cell::new(&text);
                // Right-align numbers
                if val.is_number() || val.is_f64() || val.is_i64() || val.is_u64() {
                    cell.set_alignment(CellAlignment::Right)
                } else {
                    cell
                }
            })
            .collect();
        table.add_row(cells);
    }

    println!("{table}");
}

fn format_value(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::String(s) => truncate_string(s, MAX_COLUMN_WIDTH),
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f == f.trunc() && f.abs() < 1e15 {
                    format!("{}", f as i64)
                } else {
                    format!("{:.2}", f)
                }
            } else {
                n.to_string()
            }
        }
        Value::Bool(b) => b.to_string(),
        _ => truncate_string(&val.to_string(), MAX_COLUMN_WIDTH),
    }
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}…", &s[..max_len - 1])
    }
}

fn render_metadata(body: &Value, client_ms: f64) {
    let mut parts = Vec::new();

    // Row count
    if let Some(rows) = body.get("rows").and_then(|v| v.as_array()) {
        parts.push(format!("{} row(s)", rows.len().to_string().bright_white()));
    }

    // Execution mode
    if let Some(mode) = body.get("execution_mode").and_then(|v| v.as_str()) {
        let mode_colored = match mode {
            "count_star_fast" => mode.bright_green().to_string(),
            "tantivy_fast_fields" => mode.bright_green().to_string(),
            "tantivy_grouped_partials" => mode.bright_yellow().to_string(),
            "materialized_hits_fallback" => mode.bright_red().to_string(),
            _ => mode.to_string(),
        };
        parts.push(format!("mode: {}", mode_colored));
    }

    // Matched hits
    if let Some(hits) = body.get("matched_hits").and_then(|v| v.as_u64()) {
        parts.push(format!("matched: {}", format_number(hits)));
    }

    // Shard info
    if let Some(shards) = body.get("_shards") {
        let total = shards.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
        let success = shards
            .get("successful")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let failed = shards.get("failed").and_then(|v| v.as_u64()).unwrap_or(0);
        if failed > 0 {
            parts.push(format!(
                "shards: {}/{} ({} {})",
                success,
                total,
                failed,
                "failed".bright_red()
            ));
        } else {
            parts.push(format!("shards: {}/{}", success, total));
        }
    }

    // Timing
    parts.push(format!("{}", format!("{:.1}ms", client_ms).bright_green()));

    println!();
    println!(" {}", parts.join(" | ").dimmed());
}

fn render_explain(body: &Value, client_ms: f64) {
    println!();
    println!(" {}", "EXPLAIN ANALYZE".bright_white().bold().underline());
    println!();

    // Execution mode
    if let Some(mode) = body.get("execution_mode").and_then(|v| v.as_str()) {
        let mode_colored = match mode {
            "count_star_fast" => mode.bright_green().bold().to_string(),
            "tantivy_fast_fields" => mode.bright_green().bold().to_string(),
            "tantivy_grouped_partials" => mode.bright_yellow().bold().to_string(),
            "materialized_hits_fallback" => mode.bright_red().bold().to_string(),
            _ => mode.bold().to_string(),
        };
        println!(" {} {}", "Execution Mode:".bright_white(), mode_colored);
    }

    // Strategy reason
    if let Some(reason) = body.get("strategy_reason").and_then(|v| v.as_str()) {
        println!(" {}  {}", "Strategy:".bright_white(), reason.dimmed());
    }

    // Matched hits
    if let Some(hits) = body.get("matched_hits").and_then(|v| v.as_u64()) {
        println!(
            " {} {}",
            "Matched Docs:".bright_white(),
            format_number(hits).bright_cyan()
        );
    }

    // Timings
    if let Some(timings) = body.get("timings") {
        println!();
        println!(" {}", "Timing Breakdown:".bright_white().bold());

        let stages = [
            ("planning_ms", "Planning", "░"),
            ("search_ms", "Search", "█"),
            ("collect_ms", "Collect", "▓"),
            ("merge_ms", "Merge", "▒"),
            ("datafusion_ms", "DataFusion", "░"),
        ];

        let total_ms = timings
            .get("total_ms")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);

        for (key, label, block) in &stages {
            if let Some(ms) = timings.get(key).and_then(|v| v.as_f64())
                && ms > 0.0
            {
                let bar_width = ((ms / total_ms) * 40.0).max(1.0) as usize;
                let bar: String = block.repeat(bar_width);
                println!("   {:<12} {:>7.1}ms  {}", label, ms, bar.bright_cyan());
            }
        }

        println!(
            "   {:<12} {:>7.1}ms",
            "Total".bright_white().bold(),
            total_ms
        );
    }

    // Pipeline stages
    if let Some(stages) = body.get("pipeline").and_then(|v| v.as_array()) {
        println!();
        println!(" {}", "Pipeline:".bright_white().bold());
        for stage in stages {
            let num = stage.get("stage").and_then(|v| v.as_u64()).unwrap_or(0);
            let name = stage
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let desc = stage
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            println!(
                "   {}  {} — {}",
                format!("{}.", num).bright_yellow(),
                name.bright_white(),
                desc.dimmed()
            );
        }
    }

    // Shards
    if let Some(shards) = body.get("_shards") {
        let total = shards.get("total").and_then(|v| v.as_u64()).unwrap_or(0);
        let success = shards
            .get("successful")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        println!();
        println!(
            " {} {}/{} successful",
            "Shards:".bright_white(),
            success,
            total
        );
    }

    // Print the results table
    if let (Some(columns), Some(rows)) = (
        body.get("columns").and_then(|v| v.as_array()).map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        }),
        body.get("rows").and_then(|v| v.as_array()),
    ) && !rows.is_empty()
    {
        println!();
        render_table(&columns, rows);
    }

    println!();
    println!(" {} {:.1}ms", "Client round-trip:".dimmed(), client_ms);
}

fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Check if a query string is an EXPLAIN query and extract the inner SQL.
fn parse_explain(input: &str) -> Option<String> {
    let trimmed = input.trim().trim_end_matches(';').trim();
    if trimmed.len() > 8
        && trimmed[..7].eq_ignore_ascii_case("EXPLAIN")
        && trimmed.as_bytes()[7].is_ascii_whitespace()
    {
        let inner = trimmed[7..].trim();
        if inner.to_ascii_uppercase().starts_with("SELECT") {
            return Some(inner.to_string());
        }
    }
    None
}

/// Try to extract the table name from a SELECT query for the EXPLAIN endpoint.
fn extract_table_for_explain(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let from_pos = upper.find(" FROM ")?;
    let after_from = sql[from_pos + 6..].trim();
    // Handle quoted and unquoted table names
    if after_from.starts_with('"') {
        let end = after_from.strip_prefix('"')?.find('"')?;
        Some(after_from[1..=end].to_string())
    } else {
        let end = after_from
            .find(|c: char| c.is_whitespace() || c == ';')
            .unwrap_or(after_from.len());
        Some(after_from[..end].to_string())
    }
}

async fn run_interactive(client: &FerrisClient) -> Result<()> {
    // Connect and get cluster info
    let health = client
        .health()
        .await
        .context("Could not connect to FerrisSearch. Is the server running?")?;

    let cluster_name = health
        .get("cluster_name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let node_count = health
        .get("number_of_nodes")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as usize;

    print_welcome(cluster_name, node_count, &client.connection_label());

    let mut rl = DefaultEditor::new()?;
    let history_path = dirs_home().join(".ferris_history");
    let _ = rl.load_history(&history_path);

    let mut buffer = String::new();

    loop {
        let prompt = if buffer.is_empty() {
            format!("{} ", "ferris>".bright_yellow().bold())
        } else {
            format!("{} ", "     ->".bright_yellow())
        };

        match rl.readline(&prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // Handle backslash commands immediately (no buffering)
                if buffer.is_empty() {
                    match trimmed.to_lowercase().as_str() {
                        "\\quit" | "\\q" | "exit" | "quit" => {
                            println!(" {}", "Goodbye! 🦀".bright_yellow());
                            break;
                        }
                        "\\help" | "\\h" | "help" => {
                            print_help();
                            continue;
                        }
                        "\\clear" => {
                            print!("\x1B[2J\x1B[H");
                            continue;
                        }
                        "\\tables" | "\\indices" => {
                            // Shortcut for SHOW TABLES
                            buffer = "SHOW TABLES".to_string();
                        }
                        _ => {}
                    }
                }

                // Accumulate input
                if !buffer.is_empty() {
                    buffer.push(' ');
                }
                buffer.push_str(trimmed);

                // Check if the statement is complete (ends with ; or is a command)
                let complete = buffer.ends_with(';') || is_instant_command(&buffer);

                if !complete {
                    continue;
                }

                let query = buffer.trim_end_matches(';').trim().to_string();
                buffer.clear();

                if query.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(&query);

                // Auto-quote unquoted table names that contain hyphens
                let query = auto_quote_table_names(&query);

                // Execute
                if let Some(inner_sql) = parse_explain(&query) {
                    // EXPLAIN path
                    if let Some(table) = extract_table_for_explain(&inner_sql) {
                        match client.explain_sql(&inner_sql, &table).await {
                            Ok((body, elapsed)) => render_explain(&body, elapsed),
                            Err(e) => print_error(&e.to_string()),
                        }
                    } else {
                        print_error(
                            "Could not determine index from SQL. Use: EXPLAIN SELECT ... FROM \"index\" ...",
                        );
                    }
                } else {
                    // Regular SQL / SHOW / DESCRIBE
                    match client.execute_sql(&query).await {
                        Ok((body, elapsed)) => {
                            // Extract columns and rows
                            let columns = body
                                .get("columns")
                                .and_then(|v| v.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|v| v.as_str().map(String::from))
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default();

                            let rows = body
                                .get("rows")
                                .and_then(|v| v.as_array())
                                .cloned()
                                .unwrap_or_default();

                            // Special handling for SHOW CREATE TABLE
                            if let Some(create_stmt) = body.get("create_statement") {
                                println!();
                                if let Some(index) = body.get("index").and_then(|v| v.as_str()) {
                                    println!(
                                        " {} {}",
                                        "Index:".bright_white().bold(),
                                        index.bright_cyan()
                                    );
                                }
                                println!();
                                let pretty =
                                    serde_json::to_string_pretty(create_stmt).unwrap_or_default();
                                for line in pretty.lines() {
                                    println!("   {}", line.dimmed());
                                }
                                println!();
                                println!(
                                    " {}",
                                    format!("{:.1}ms", elapsed).bright_green().dimmed()
                                );
                            } else {
                                render_table(&columns, &rows);
                                render_metadata(&body, elapsed);
                            }
                        }
                        Err(e) => print_error(&e.to_string()),
                    }
                }
                println!();
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                buffer.clear();
                println!(" {}", "(query cancelled)".dimmed());
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!(" {}", "Goodbye! 🦀".bright_yellow());
                break;
            }
            Err(e) => {
                print_error(&format!("Input error: {}", e));
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
    Ok(())
}

fn is_instant_command(input: &str) -> bool {
    let trimmed = input.trim();
    let upper = trimmed.to_ascii_uppercase();
    upper.starts_with("SHOW ")
        || upper.starts_with("DESCRIBE ")
        || upper.starts_with("DESC ")
        || upper == "SHOW TABLES"
        || upper == "SHOW INDICES"
}

/// Auto-quote unquoted table names that contain hyphens so the SQL parser
/// doesn't interpret them as subtraction (e.g. `FROM nyc-taxis` → `FROM "nyc-taxis"`).
fn auto_quote_table_names(sql: &str) -> String {
    // Regex-free approach: find FROM followed by an unquoted identifier with a hyphen
    let upper = sql.to_ascii_uppercase();
    let mut result = sql.to_string();

    // Find all FROM occurrences
    let mut search_from = 0;
    while let Some(from_pos) = upper[search_from..].find("FROM ") {
        let abs_pos = search_from + from_pos + 5; // skip "FROM "
        let rest = &sql[abs_pos..];
        let trimmed = rest.trim_start();
        let skip = rest.len() - trimmed.len();
        let start = abs_pos + skip;

        // If already quoted, skip
        if trimmed.starts_with('"') || trimmed.starts_with('`') {
            search_from = start + 1;
            continue;
        }

        // Collect the identifier (alphanumeric + hyphens + underscores)
        let end = trimmed
            .find(|c: char| !c.is_alphanumeric() && c != '-' && c != '_')
            .unwrap_or(trimmed.len());
        let ident = &trimmed[..end];

        if ident.contains('-') {
            let quoted = format!("\"{}\"", ident);
            result = format!("{}{}{}", &result[..start], quoted, &result[start + end..]);
            search_from = start + quoted.len();
        } else {
            search_from = start + end;
        }
    }

    result
}

fn print_error(msg: &str) {
    println!();
    println!(" {} {}", "Error:".bright_red().bold(), msg);
}

fn dirs_home() -> std::path::PathBuf {
    std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
}

async fn run_single_command(client: &FerrisClient, query: &str) -> Result<()> {
    let query = auto_quote_table_names(query);
    if let Some(inner_sql) = parse_explain(&query) {
        if let Some(table) = extract_table_for_explain(&inner_sql) {
            let (body, elapsed) = client.explain_sql(&inner_sql, &table).await?;
            render_explain(&body, elapsed);
        } else {
            bail!("Could not determine index from SQL");
        }
    } else {
        let (body, elapsed) = client.execute_sql(&query).await?;

        let columns = body
            .get("columns")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let rows = body
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        if let Some(create_stmt) = body.get("create_statement") {
            if let Some(index) = body.get("index").and_then(|v| v.as_str()) {
                println!("Index: {}", index);
            }
            println!(
                "{}",
                serde_json::to_string_pretty(create_stmt).unwrap_or_default()
            );
        } else {
            render_table(&columns, &rows);
            render_metadata(&body, elapsed);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = FerrisClient::new(&args.host, args.port);

    if let Some(command) = args.command {
        run_single_command(&client, &command).await
    } else {
        run_interactive(&client).await
    }
}
