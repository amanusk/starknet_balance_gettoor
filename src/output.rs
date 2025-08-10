use csv::Writer;
use eyre::Result;
use rayon::prelude::*;
use rusqlite::Connection;
use starknet::core::types::Felt;
use std::collections::HashMap;
use std::fs::File;
use std::time::SystemTime;
use crossbeam_channel::bounded;
use std::thread;

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
    println!(
        "Writing {} total records across {} tokens",
        total_records,
        token_map.len()
    );

    if config.csv {
        let csv_start = SystemTime::now();
        store_map_as_csv(token_map)
            .map_err(|e| eyre::eyre!("Failed to store map as CSV: {}", e))?;
        let csv_time = csv_start.elapsed().unwrap();
        println!("Results written to token_map.csv in {:?} ms", csv_time.as_millis());
    }

    if config.json {
        let json_start = SystemTime::now();
        store_map_as_json(token_map)
            .map_err(|e| eyre::eyre!("Failed to store map as JSON: {}", e))?;
        let json_time = json_start.elapsed().unwrap();
        println!(
            "Results written to token_map.json in {:?} ms",
            json_time.as_millis()
        );
    }

    if config.sqlite {
        let sqlite_start = SystemTime::now();
        store_map_in_sqlite(token_map)?;
        let sqlite_time = sqlite_start.elapsed().unwrap();
        println!(
            "Results written to token_map.db in {:?} ms",
            sqlite_time.as_millis()
        );
    }

    Ok(())
}

/// Store the token map as a CSV file with parallel generation streamed to a single writer
fn store_map_as_csv(
    token_map: &HashMap<Felt, HashMap<Felt, Felt>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create("token_map.csv")?;
    let mut wtr = Writer::from_writer(file);

    // Write header row
    wtr.write_record(["Token", "Account", "Balance"])?;

    // Bounded channel to backpressure producers if writing is slower
    let (tx, rx) = bounded::<Vec<[String; 3]>>(64);

    // Writer thread consumes records and writes to CSV
    let writer_handle = thread::spawn(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let write_start = SystemTime::now();
        for batch in rx {
            for record in batch {
                // CSV writer isn't thread-safe; only this thread writes
                wtr.write_record(&record)?;
            }
        }
        wtr.flush()?;
        let write_time = write_start.elapsed().unwrap();
        println!("CSV streamed write time: {:?} ms", write_time.as_millis());
        Ok(())
    });

    // Producers generate batches per token in parallel
    token_map.par_iter().for_each(|(token, sub_map)| {
        let batch: Vec<[String; 3]> = sub_map
            .par_iter()
            .map(|(account, balance)| {
                [
                    format!("{token:#064x}"),
                    format!("{account:#064x}"),
                    balance.to_string(),
                ]
            })
            .collect();
        let _ = tx.send(batch);
    });

    // Close channel to signal writer completion
    drop(tx);

    // Propagate writer errors
    match writer_handle.join() {
        Ok(result) => result?,
        Err(_) => return Err(Box::<dyn std::error::Error>::from("CSV writer thread panicked")),
    }

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

/// Store the token map in SQLite database with streaming batch insertions
fn store_map_in_sqlite(token_map: &HashMap<Felt, HashMap<Felt, Felt>>) -> eyre::Result<()> {
    let conn = Connection::open("token_map.db")
        .map_err(|e| eyre::eyre!("Failed to open SQLite database: {}", e))?;

    // PRAGMAs to speed up bulk insert (acceptable for generated artifacts)
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;\nPRAGMA synchronous = NORMAL;\nPRAGMA temp_store = MEMORY;",
    )
    .map_err(|e| eyre::eyre!("Failed to apply PRAGMAs: {}", e))?;

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

    // Channel for streaming batches into the writer loop
    let (tx, rx) = bounded::<Vec<(String, String, String)>>(64);

    // Move connection into writer thread
    let writer_handle = thread::spawn(move || -> eyre::Result<()> {
        let tx_start = SystemTime::now();
        let tx_sql = conn
            .unchecked_transaction()
            .map_err(|e| eyre::eyre!("Failed to begin transaction: {}", e))?;

        let mut stmt = tx_sql
            .prepare(
                "INSERT INTO token_map (token, account, balance)
                 VALUES (?1, ?2, ?3)",
            )
            .map_err(|e| eyre::eyre!("Failed to prepare insert statement: {}", e))?;

        for batch in rx {
            for (token, account, balance) in batch {
                stmt.execute(rusqlite::params![token, account, balance])
                    .map_err(|e| eyre::eyre!("Failed to insert row: {}", e))?;
            }
        }

        drop(stmt);
        tx_sql
            .commit()
            .map_err(|e| eyre::eyre!("Failed to commit transaction: {}", e))?;

        let tx_time = tx_start.elapsed().unwrap();
        println!("SQLite streamed transaction time: {:?} ms", tx_time.as_millis());

        Ok(())
    });

    // Producers generate batches per token in parallel
    token_map.par_iter().for_each(|(token, sub_map)| {
        let batch: Vec<(String, String, String)> = sub_map
            .par_iter()
            .map(|(account, balance)| {
                (
                    format!("{token:#064x}"),
                    format!("{account:#064x}"),
                    balance.to_string(),
                )
            })
            .collect();
        let _ = tx.send(batch);
    });

    // Close channel to signal writer completion
    drop(tx);

    // Propagate writer errors
    match writer_handle.join() {
        Ok(result) => result?,
        Err(_) => return Err(eyre::eyre!("SQLite writer thread panicked")),
    }

    Ok(())
}
