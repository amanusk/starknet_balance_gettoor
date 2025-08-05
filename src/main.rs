use std::collections::HashMap;
use std::fs::File;

use clap::Parser;
use csv::Writer;
use eyre::Result;
use rayon::prelude::*;

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
    let csv_start = std::time::SystemTime::now();
    
    let file = File::create("token_map.csv")?;
    let mut wtr = Writer::from_writer(file);

    // Write header row
    wtr.write_record(["Token", "Account", "Balance"])?;

    // Collect all records in parallel, then write sequentially
    let records: Vec<[String; 3]> = token_map
        .par_iter()
        .flat_map(|(token, sub_map)| {
            sub_map.par_iter().map(|(account, balance)| {
                [
                    format!("{token:#064x}"),
                    format!("{account:#064x}"),
                    balance.to_string(),
                ]
            }).collect::<Vec<_>>()
        })
        .collect();

    // Write each record to the CSV
    for record in records {
        wtr.write_record(&record)?;
    }

    wtr.flush()?;
    
    let csv_end = std::time::SystemTime::now();
    let csv_time = csv_end.duration_since(csv_start).unwrap();
    println!("CSV generation time: {:?}", csv_time.as_millis());
    
    Ok(())
}

fn store_map_as_json(
    token_map: &HashMap<Felt, HashMap<Felt, Felt>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let json_start = std::time::SystemTime::now();
    
    let file = File::create("token_map.json")?;
    serde_json::to_writer_pretty(file, &token_map)?;
    
    let json_end = std::time::SystemTime::now();
    let json_time = json_end.duration_since(json_start).unwrap();
    println!("JSON generation time: {:?}", json_time.as_millis());
    
    Ok(())
}

fn store_map_in_sqlite(token_map: &HashMap<Felt, HashMap<Felt, Felt>>) -> eyre::Result<()> {
    let sqlite_start = std::time::SystemTime::now();
    
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

    // Prepare all data in parallel, then insert sequentially
    let data_prep_start = std::time::SystemTime::now();
    let records: Vec<(String, String, String)> = token_map
        .par_iter()
        .flat_map(|(token, sub_map)| {
            sub_map.par_iter().map(|(account, balance)| {
                (
                    format!("{:#064x}", token),
                    format!("{:#064x}", account),
                    balance.to_string(),
                )
            }).collect::<Vec<_>>()
        })
        .collect();
    
    let data_prep_end = std::time::SystemTime::now();
    let data_prep_time = data_prep_end.duration_since(data_prep_start).unwrap();
    println!("SQLite data preparation time: {:?}", data_prep_time.as_millis());

    // Prepare the insertion statement
    let insert_start = std::time::SystemTime::now();
    let mut stmt = conn
        .prepare(
            "INSERT INTO token_map (token, account, balance)
             VALUES (?1, ?2, ?3)",
        )
        .map_err(|e| eyre::eyre!("Failed to prepare insert statement: {}", e))?;

    // Insert each row
    for (token, account, balance) in records {
        stmt.execute(rusqlite::params![token, account, balance])
            .map_err(|e| eyre::eyre!("Failed to insert row: {}", e))?;
    }

    let insert_end = std::time::SystemTime::now();
    let insert_time = insert_end.duration_since(insert_start).unwrap();
    println!("SQLite insertion time: {:?}", insert_time.as_millis());
    
    let sqlite_end = std::time::SystemTime::now();
    let sqlite_time = sqlite_end.duration_since(sqlite_start).unwrap();
    println!("Total SQLite generation time: {:?}", sqlite_time.as_millis());

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

    let overall_start = std::time::SystemTime::now();
    let token_map = get_balance_map(&conn, &addresses)?;
    let overall_end = std::time::SystemTime::now();
    let overall_time = overall_end.duration_since(overall_start).unwrap();
    println!("Total balance map creation time: {:?}", overall_time.as_millis());

    store_map_as_csv(&token_map).map_err(|e| eyre::eyre!("Failed to store map as CSV: {}", e))?;
    store_map_as_json(&token_map).map_err(|e| eyre::eyre!("Failed to store map as JSON: {}", e))?;
    store_map_in_sqlite(&token_map)?;

    Ok(())
}
