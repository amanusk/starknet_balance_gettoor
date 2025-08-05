use std::collections::HashMap;
use std::fs::File;

use clap::Parser;
use csv::Writer;
use eyre::Result;

// use bigdecimal::BigDecimal;
use rusqlite::Connection;
use starknet::core::types::Felt;

mod balance;
use balance::{get_balance_map, Addresses};

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
}

fn store_map_as_csv(
    token_map: &HashMap<Felt, HashMap<Felt, Felt>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create("token_map.csv")?;
    let mut wtr = Writer::from_writer(file);

    // Write header row
    wtr.write_record(["Token", "Account", "Balance"])?;

    // Write each (token, account, balance) tuple to the CSV
    for (token, sub_map) in token_map {
        for (account, balance) in sub_map {
            wtr.write_record(&[
                format!("{token:#064x}"),
                format!("{account:#064x}"),
                balance.to_string(),
            ])?;
        }
    }

    wtr.flush()?;
    Ok(())
}

fn store_map_as_json(
    token_map: &HashMap<Felt, HashMap<Felt, Felt>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create("token_map.json")?;
    serde_json::to_writer_pretty(file, &token_map)?;
    Ok(())
}

fn store_map_in_sqlite(token_map: &HashMap<Felt, HashMap<Felt, Felt>>) -> eyre::Result<()> {
    let conn = Connection::open("token_map.db")
        .map_err(|e| eyre::eyre!("Failed to open SQLite database: {}", e))?;

    // Create the table if it doesn't exist
    conn.execute(
        "CREATE TABLE IF NOT EXISTS token_map (
            token TEXT NOT NULL,
            account TEXT NOT NULL,
            balance TEXT NOT NULL
        )",
        [],
    )
    .map_err(|e| eyre::eyre!("Failed to create table: {}", e))?;

    // Prepare the insertion statement
    let mut stmt = conn
        .prepare(
            "INSERT INTO token_map (token, account, balance)
             VALUES (?1, ?2, ?3)",
        )
        .map_err(|e| eyre::eyre!("Failed to prepare insert statement: {}", e))?;

    // Insert each row
    for (token, sub_map) in token_map {
        for (account, balance) in sub_map {
            stmt.execute(rusqlite::params![
                format!("{:#064x}", token),
                format!("{:#064x}", account),
                balance.to_string()
            ])
            .map_err(|e| eyre::eyre!("Failed to insert row: {}", e))?;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    // Parse command line arguments
    let args = Args::parse();

    // Read and parse the JSON file
    let file_content = std::fs::read_to_string(&args.input_file)
        .map_err(|e| eyre::eyre!("Failed to read JSON file '{}': {}", args.input_file, e))?;
    let addresses: Addresses = serde_json::from_str(&file_content)
        .map_err(|e| eyre::eyre!("Failed to parse JSON file '{}': {}", args.input_file, e))?;

    // Open a connection to the SQLite database
    let conn = Connection::open(&args.db_path)
        .map_err(|e| eyre::eyre!("Failed to open database '{}': {}", args.db_path, e))?;

    let token_map = get_balance_map(&conn, &addresses)?;

    store_map_as_csv(&token_map).map_err(|e| eyre::eyre!("Failed to store map as CSV: {}", e))?;
    store_map_as_json(&token_map).map_err(|e| eyre::eyre!("Failed to store map as JSON: {}", e))?;
    store_map_in_sqlite(&token_map)?;

    Ok(())
}
