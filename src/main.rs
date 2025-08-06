use clap::Parser;

use rusqlite::Connection;

mod balance;
use balance::{get_balance_map, Addresses};

mod output;
use output::{write_results, OutputConfig};

#[derive(Parser)]
#[command(name = "balance_gettor")]
#[command(about = "A CLI tool to get balance information from StarkNet")]
struct Args {
    /// Path to the addresses JSON file
    #[arg(short, long, env = "INPUT_FILE")]
    input_file: String,

    /// Path to the database
    #[arg(short, long, env = "DB_PATH")]
    db_path: String,

    /// Output results as CSV
    #[arg(long)]
    csv: bool,

    /// Output results as JSON
    #[arg(long)]
    json: bool,

    /// Output results to SQLite database
    #[arg(long)]
    sqlite: bool,
}

fn main() -> eyre::Result<()> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    // Parse command line arguments
    let args = Args::parse();

    // Create output configuration from CLI arguments
    let output_config = OutputConfig {
        csv: args.csv,
        json: args.json,
        sqlite: args.sqlite,
    };

    // Read and parse the JSON file
    let file_content = std::fs::read_to_string(&args.input_file)
        .map_err(|e| eyre::eyre!("Failed to read JSON file '{}': {}", args.input_file, e))?;
    let addresses: Addresses = serde_json::from_str(&file_content)
        .map_err(|e| eyre::eyre!("Failed to parse JSON file '{}': {}", args.input_file, e))?;

    // Open a connection to the SQLite database
    let conn = Connection::open(&args.db_path)
        .map_err(|e| eyre::eyre!("Failed to open database '{}': {}", args.db_path, e))?;

    let token_map = get_balance_map(&conn, &addresses)?;

    // Write results using the new output module
    write_results(&token_map, &output_config)?;

    Ok(())
}
