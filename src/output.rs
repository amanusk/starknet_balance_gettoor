use csv::Writer;
use eyre::Result;
use rayon::prelude::*;
use rusqlite::Connection;
use starknet::core::types::Felt;
use std::collections::HashMap;
use std::fs::File;

/// Configuration for output formats
#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub csv: bool,
    pub json: bool,
    pub sqlite: bool,
}

impl OutputConfig {
    pub fn new() -> Self {
        Self {
            csv: false,
            json: false,
            sqlite: false,
        }
    }

    /// Returns true if at least one output format is selected
    pub fn has_any_output(&self) -> bool {
        self.csv || self.json || self.sqlite
    }
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Write results to all enabled output formats
pub fn write_results(
    token_map: &HashMap<Felt, HashMap<Felt, Felt>>,
    config: &OutputConfig,
) -> eyre::Result<()> {
    if !config.has_any_output() {
        println!(
            "No output format selected. Use --csv, --json, or --sqlite to specify output formats."
        );
        return Ok(());
    }

    // Calculate total records for performance reporting
    let total_records: usize = token_map.values().map(|m| m.len()).sum();
    println!("Writing {} total records across {} tokens", total_records, token_map.len());

    if config.csv {
        let csv_start = std::time::SystemTime::now();
        store_map_as_csv(token_map)
            .map_err(|e| eyre::eyre!("Failed to store map as CSV: {}", e))?;
        let csv_end = std::time::SystemTime::now();
        let csv_time = csv_end.duration_since(csv_start).unwrap();
        println!("Results written to token_map.csv in {:?} ms", csv_time.as_millis());
    }

    if config.json {
        let json_start = std::time::SystemTime::now();
        store_map_as_json(token_map)
            .map_err(|e| eyre::eyre!("Failed to store map as JSON: {}", e))?;
        let json_end = std::time::SystemTime::now();
        let json_time = json_end.duration_since(json_start).unwrap();
        println!("Results written to token_map.json in {:?} ms", json_time.as_millis());
    }

    if config.sqlite {
        let sqlite_start = std::time::SystemTime::now();
        store_map_in_sqlite(token_map)?;
        let sqlite_end = std::time::SystemTime::now();
        let sqlite_time = sqlite_end.duration_since(sqlite_start).unwrap();
        println!("Results written to token_map.db in {:?} ms", sqlite_time.as_millis());
    }

    Ok(())
}

/// Store the token map as a CSV file with parallel record generation
fn store_map_as_csv(
    token_map: &HashMap<Felt, HashMap<Felt, Felt>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create("token_map.csv")?;
    let mut wtr = Writer::from_writer(file);

    // Write header row
    wtr.write_record(["Token", "Account", "Balance"])?;

    // Generate all records in parallel, then write sequentially
    let parallel_start = std::time::SystemTime::now();
    
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

    let parallel_end = std::time::SystemTime::now();
    let parallel_time = parallel_end.duration_since(parallel_start).unwrap();
    println!("CSV parallel record generation time: {:?} ms", parallel_time.as_millis());

    // Write all records sequentially (CSV writer isn't thread-safe)
    let write_start = std::time::SystemTime::now();
    for record in records {
        wtr.write_record(&record)?;
    }
    let write_end = std::time::SystemTime::now();
    let write_time = write_end.duration_since(write_start).unwrap();
    println!("CSV sequential write time: {:?} ms", write_time.as_millis());

    wtr.flush()?;
    Ok(())
}

/// Store the token map as a JSON file
fn store_map_as_json(
    token_map: &HashMap<Felt, HashMap<Felt, Felt>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create("token_map.json")?;
    serde_json::to_writer_pretty(file, &token_map)?;
    Ok(())
}

/// Store the token map in SQLite database with optimized batch insertions
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

    // Generate all records in parallel first
    let parallel_start = std::time::SystemTime::now();
    
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

    let parallel_end = std::time::SystemTime::now();
    let parallel_time = parallel_end.duration_since(parallel_start).unwrap();
    println!("SQLite parallel record generation time: {:?} ms", parallel_time.as_millis());

    // Use transaction for better performance
    let tx_start = std::time::SystemTime::now();
    let tx = conn.unchecked_transaction()
        .map_err(|e| eyre::eyre!("Failed to begin transaction: {}", e))?;

    // Prepare the insertion statement once
    let mut stmt = tx
        .prepare(
            "INSERT INTO token_map (token, account, balance)
             VALUES (?1, ?2, ?3)",
        )
        .map_err(|e| eyre::eyre!("Failed to prepare insert statement: {}", e))?;

    // Insert all records in the transaction
    for (token, account, balance) in records {
        stmt.execute(rusqlite::params![token, account, balance])
            .map_err(|e| eyre::eyre!("Failed to insert row: {}", e))?;
    }

    // Commit the transaction
    drop(stmt);
    tx.commit()
        .map_err(|e| eyre::eyre!("Failed to commit transaction: {}", e))?;

    let tx_end = std::time::SystemTime::now();
    let tx_time = tx_end.duration_since(tx_start).unwrap();
    println!("SQLite transaction time: {:?} ms", tx_time.as_millis());

    Ok(())
}
