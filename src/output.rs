use csv::Writer;
use eyre::Result;
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

    if config.csv {
        store_map_as_csv(token_map)
            .map_err(|e| eyre::eyre!("Failed to store map as CSV: {}", e))?;
        println!("Results written to token_map.csv");
    }

    if config.json {
        store_map_as_json(token_map)
            .map_err(|e| eyre::eyre!("Failed to store map as JSON: {}", e))?;
        println!("Results written to token_map.json");
    }

    if config.sqlite {
        store_map_in_sqlite(token_map)?;
        println!("Results written to token_map.db");
    }

    Ok(())
}

/// Store the token map as a CSV file
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

/// Store the token map as a JSON file
fn store_map_as_json(
    token_map: &HashMap<Felt, HashMap<Felt, Felt>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create("token_map.json")?;
    serde_json::to_writer_pretty(file, &token_map)?;
    Ok(())
}

/// Store the token map in SQLite database
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
