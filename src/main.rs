use bigdecimal::BigDecimal;
use rayon::prelude::*;
use reqwest::Error;
use rusqlite::{params, Connection, Result};
use serde::Deserialize;
use starknet::{
    core::crypto::pedersen_hash,
    core::types::{BlockId, BlockTag, Felt},
    core::utils::starknet_keccak,
    providers::{
        jsonrpc::{HttpTransport, JsonRpcClient},
        Provider, Url,
    },
};
use std::time::Duration;

use std::collections::HashMap;
use std::{env, process};
use tokio;

#[derive(Deserialize)]
struct Addresses {
    accounts: Vec<Felt>,
    tokens: Vec<Felt>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    // Read rpc url
    let rpc_url = env::var("STARKNET_RPC_URL").expect("STARKNET_RPC_URL not set");
    println!("RPC URL: {}", rpc_url);

    let provider = JsonRpcClient::new(HttpTransport::new(Url::parse(&rpc_url).unwrap()));

    let input_file = env::var("INPUT_FILE").expect("INPUT_FILE not set");

    // Read and parse the JSON file
    let file_content = std::fs::read_to_string(input_file).expect("Failed to read JSON file");
    let addresses: Addresses =
        serde_json::from_str(&file_content).expect("Failed to parse JSON file");

    // Go over all tokens and make sure the are deployed
    for token_address in addresses.tokens.iter() {
        let token_class = provider
            .get_class_hash_at(BlockId::Tag(BlockTag::Latest), token_address)
            .await;
        match token_class {
            Ok(class_hash) => {
                println!("Token class hash: {:#064x}", class_hash);
            }
            Err(e) => {
                println!(
                    "Failed to get token class of: {:#064x}, {}",
                    token_address, e
                );
                process::exit(1);
            }
        }
    }

    let db_path = env::var("DB_PATH").expect("DB_PATH not set");

    // Open a connection to the SQLite database
    let conn = Connection::open(&db_path).expect("Failed to open database");

    // let mut accounts_hash_map: HashMap<Felt, Felt> = HashMap::new();

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

    let mut token_map = HashMap::new();

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
            if accounts_hash_map.contains_key(&storage_addr_felt) {
                let account = accounts_hash_map.get(&storage_addr_felt).unwrap();
                let balance_felt = Felt::from_hex(&storage_val);
                match balance_felt {
                    Ok(val) => {
                        // let balance_dec = BigDecimal::new(val.to_bigint(), 18);
                        // println!("Account: {:#064x}, Balance :{}", account, balance_dec);
                        balance_map.insert(account.clone(), val);
                    }
                    _ => {
                        balance_map.insert(account.clone(), Felt::ZERO);
                    }
                }
            }
        }
        let balance_map_end = std::time::SystemTime::now();
        let balance_map_time = balance_map_end.duration_since(balance_map_start).unwrap();
        println!("BalanceMap time: {:?}", balance_map_time.as_millis());

        token_map.insert(token.clone(), balance_map);
    }
    // TODO: filter all the addresses with 0 value
    // println!("{:#?}", token_map);

    Ok(())
}
