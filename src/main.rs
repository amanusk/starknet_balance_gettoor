use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Write;

use csv::Writer;

// use bigdecimal::BigDecimal;
use rayon::prelude::*;
use reqwest::Error;
use rusqlite::{params, Connection, Result};
use serde::Deserialize;
use starknet::{core::crypto::pedersen_hash, core::types::Felt, core::utils::starknet_keccak};

use tokio;

#[derive(Deserialize)]
struct Addresses {
    accounts: Vec<Felt>,
    tokens: Vec<Felt>,
}

fn store_map_as_csv(
    token_map: &HashMap<Felt, HashMap<Felt, Felt>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create("token_map.csv")?;
    let mut wtr = Writer::from_writer(file);

    // Write header row
    wtr.write_record(&["Token", "Account", "Balance"])?;

    // Write each (token, account, balance) tuple to the CSV
    for (token, sub_map) in token_map {
        for (account, balance) in sub_map {
            wtr.write_record(&[
                format!("{:#064x}", token),
                format!("{:#064x}", account),
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

fn store_map_in_sqlite(token_map: &HashMap<Felt, HashMap<Felt, Felt>>) -> Result<()> {
    let conn = Connection::open("token_map.db")?;

    // Create the table if it doesn't exist
    conn.execute(
        "CREATE TABLE IF NOT EXISTS token_map (
            token TEXT NOT NULL,
            account TEXT NOT NULL,
            balance TEXT NOT NULL
        )",
        [],
    )?;

    // Prepare the insertion statement
    let mut stmt = conn.prepare(
        "INSERT INTO token_map (token, account, balance)
         VALUES (?1, ?2, ?3)",
    )?;

    // Insert each row
    for (token, sub_map) in token_map {
        for (account, balance) in sub_map {
            stmt.execute(params![
                format!("{:#064x}", token),
                format!("{:#064x}", account),
                balance.to_string()
            ])?;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    let input_file = env::var("INPUT_FILE").expect("INPUT_FILE not set");

    // Read and parse the JSON file
    let file_content = std::fs::read_to_string(input_file).expect("Failed to read JSON file");
    let addresses: Addresses =
        serde_json::from_str(&file_content).expect("Failed to parse JSON file");

    let db_path = env::var("DB_PATH").expect("DB_PATH not set");

    // Open a connection to the SQLite database
    let conn = Connection::open(&db_path).expect("Failed to open database");

    let balances_selector = starknet_keccak("ERC20_balances".as_bytes());

    let hashing_start = std::time::SystemTime::now();
    let accounts_hash_map: HashMap<Felt, Felt> = addresses
        .accounts
        .par_iter()
        .map(|item| {
            let val = pedersen_hash(&balances_selector, item);
            (val, item.clone())
        })
        .collect();

    let hashing_end = std::time::SystemTime::now();
    let hash_time = hashing_end.duration_since(hashing_start).unwrap();
    println!("Hashing time: {:?}", hash_time.as_millis());

    let mut token_map: HashMap<Felt, HashMap<Felt, Felt>> = HashMap::new();

    for token in addresses.tokens.iter() {
        let query_start = std::time::SystemTime::now();
        let parsed_token = format!("{:#064x}", token)[2..].to_string();
        // Prepare the SQL statement
        let mut stmt = conn
            .prepare(&format!(
                r#"
        SELECT
            hex(contract_addresses.contract_address),
            hex(storage_addresses.storage_address),
            hex(storage_value),
            MAX(block_number)
        FROM
            storage_updates
            JOIN storage_addresses
                ON storage_addresses.id = storage_updates.storage_address_id
            JOIN contract_addresses
                ON contract_addresses.id = storage_updates.contract_address_id
        WHERE
            contract_address = X'{}'
            GROUP BY
            contract_address_id,
            storage_address_id
        "#,
                parsed_token
            ))
            .expect("Failed to prepare SQL statement");
        let query_end = std::time::SystemTime::now();
        let query_time = query_end.duration_since(query_start).unwrap();
        println!("Query time: {:?}", query_time.as_millis());

        // Execute the query and collect results
        let rows = stmt
            .query_map(params![], |row| {
                // You can adapt the indices and types here:
                // The columns here are (0) hex(contract_address),
                //                      (1) hex(storage_address),
                //                      (2) hex(storage_value),
                //                      (3) max(block_number)
                let contract_address_hex: String = row.get(0)?;
                let storage_address_hex: String = row.get(1)?;
                let storage_value_hex: String = row.get(2)?;
                let max_block_number: i64 = row.get(3)?;

                Ok((
                    contract_address_hex,
                    storage_address_hex,
                    storage_value_hex,
                    max_block_number,
                ))
            })
            .expect("Failed to execute query");

        let create_rows_end = std::time::SystemTime::now();
        let rows_time = create_rows_end.duration_since(query_start).unwrap();
        println!("Rows time: {:?}", rows_time.as_millis());

        println!("#### Token: {:#064x} ######", token);

        let balance_map_start = std::time::SystemTime::now();

        let mut balance_map = HashMap::new();
        // Print out each row
        for row_result in rows {
            let (_, storage_addr, storage_val, _) = row_result.expect("Failed to get row");
            let storage_str = format!("0x{}", storage_addr);
            // println!("storage_str: {}", storage_addr);
            let storage_addr_felt =
                Felt::from_hex(&storage_str).expect("Failed to parse storage value");

            if let Some(account) = accounts_hash_map.get(&storage_addr_felt) {
                let balance_felt = Felt::from_hex(&storage_val).unwrap_or_else(|_| Felt::ZERO);
                balance_map.insert(account.clone(), balance_felt);
            }
        }
        let balance_map_end = std::time::SystemTime::now();
        let balance_map_time = balance_map_end.duration_since(balance_map_start).unwrap();
        println!("BalanceMap time: {:?}", balance_map_time.as_millis());

        token_map.insert(token.clone(), balance_map);
    }
    store_map_as_csv(&token_map).expect("Failed to store map as CSV");
    store_map_as_json(&token_map).expect("Failed to store map as JSON");
    store_map_in_sqlite(&token_map).expect("Failed to store map in SQLite");

    Ok(())
}
