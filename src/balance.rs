use std::collections::HashMap;

use eyre::Result;
use rayon::prelude::*;
use rusqlite::Connection;
use serde::Deserialize;
use starknet::{core::crypto::pedersen_hash, core::types::Felt, core::utils::starknet_keccak};

#[derive(Deserialize)]
pub struct Addresses {
    pub accounts: Vec<Felt>,
    pub tokens: Vec<Felt>,
}

// Helper function to create a new database connection
fn create_connection(db_path: &str) -> Result<Connection> {
    Connection::open(db_path)
        .map_err(|e| eyre::eyre!("Failed to create database connection: {}", e))
}

// Process a database partition by storage_address range
fn process_storage_partition(
    db_path: &str,
    partition_id: usize,
    num_partitions: usize,
    tokens: &[Felt],
    accounts_hash_map: &HashMap<Felt, Felt>,
) -> Result<HashMap<Felt, HashMap<Felt, Felt>>> {
    // Create a connection for this partition
    let conn = create_connection(db_path)?;

    // Calculate storage_address range for this partition
    // We'll use modulo partitioning based on storage_address_id
    let min_storage_id = (partition_id * 1000000) / num_partitions; // Assuming ~1M storage addresses
    let max_storage_id = ((partition_id + 1) * 1000000) / num_partitions;

    println!(
        "Processing partition {} with storage_address_id range {} to {}",
        partition_id, min_storage_id, max_storage_id
    );

    // Get all token contract IDs first
    let mut token_contract_ids: Vec<i64> = Vec::new();
    for token in tokens {
        let token_hex = format!("{token:#064x}")[2..].to_string();
        let token_bytes = hex::decode(&token_hex).unwrap_or_default();

        let contract_id_query = "SELECT id FROM contract_addresses WHERE contract_address = ?";
        let mut contract_stmt = conn
            .prepare(contract_id_query)
            .map_err(|e| eyre::eyre!("Failed to prepare contract ID statement: {}", e))?;

        if let Ok(contract_id) =
            contract_stmt.query_row([&token_bytes as &dyn rusqlite::ToSql], |row| row.get(0))
        {
            token_contract_ids.push(contract_id);
        }
    }

    if token_contract_ids.is_empty() {
        return Ok(HashMap::new());
    }

    // Build the IN clause for contract IDs
    let contract_ids_placeholders = token_contract_ids
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");

    // Query for this partition - each partition processes ALL tokens
    let partition_query = format!(
        r#"
        SELECT
            ca.contract_address,
            sa.storage_address,
            su.storage_value,
            su.block_number
        FROM
            storage_updates su
            JOIN contract_addresses ca ON ca.id = su.contract_address_id
            JOIN storage_addresses sa ON sa.id = su.storage_address_id
        WHERE
            su.contract_address_id IN ({})
            AND sa.id BETWEEN ? AND ?
        ORDER BY
            ca.contract_address, sa.id, su.block_number DESC
        "#,
        contract_ids_placeholders
    );

    let mut stmt = conn
        .prepare(&partition_query)
        .map_err(|e| eyre::eyre!("Failed to prepare partition query: {}", e))?;

    // Build parameters for the query
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = token_contract_ids
        .iter()
        .map(|&id| Box::new(id) as Box<dyn rusqlite::ToSql>)
        .collect();
    params.push(Box::new(min_storage_id));
    params.push(Box::new(max_storage_id));

    // Execute the query
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |row| {
            let contract_address: Vec<u8> = row.get(0)?;
            let storage_address: Vec<u8> = row.get(1)?;
            let storage_value: Vec<u8> = row.get(2)?;
            let block_number: i64 = row.get(3)?;

            Ok((
                contract_address,
                storage_address,
                storage_value,
                block_number,
            ))
        })
        .map_err(|e| eyre::eyre!("Failed to execute partition query: {}", e))?;

    // Process rows for this partition
    let mut partition_results: HashMap<Felt, HashMap<Felt, Felt>> = HashMap::new();
    let mut current_contract = None;
    let mut current_storage_address = None;

    for row_result in rows {
        let (contract_addr_bytes, storage_addr_bytes, storage_val_bytes, _block_number) =
            row_result.map_err(|e| eyre::eyre!("Failed to get row: {}", e))?;

        // Convert bytes to hex strings
        let contract_addr_hex = hex::encode(&contract_addr_bytes);
        let storage_addr_hex = hex::encode(&storage_addr_bytes);
        let storage_val_hex = hex::encode(&storage_val_bytes);

        // Convert to Felt
        let contract_addr_felt = match Felt::from_hex(&format!("0x{contract_addr_hex}")) {
            Ok(f) => f,
            Err(_) => continue,
        };

        let storage_addr_felt = match Felt::from_hex(&format!("0x{storage_addr_hex}")) {
            Ok(f) => f,
            Err(_) => continue,
        };

        // Check if this is a token we're interested in
        if !tokens.contains(&contract_addr_felt) {
            continue;
        }

        // Get the account for this storage address
        let account = match accounts_hash_map.get(&storage_addr_felt) {
            Some(a) => a,
            None => continue,
        };

        // Since we ordered by contract_address, storage_address, and block_number DESC,
        // we only need to process the first row for each (contract, storage_address) pair
        if current_contract != Some(contract_addr_felt)
            || current_storage_address != Some(storage_addr_felt)
        {
            current_contract = Some(contract_addr_felt);
            current_storage_address = Some(storage_addr_felt);

            let balance_felt = Felt::from_hex(&storage_val_hex).unwrap_or(Felt::ZERO);

            // Add to results
            partition_results
                .entry(contract_addr_felt)
                .or_insert_with(HashMap::new)
                .insert(*account, balance_felt);
        }
    }

    println!(
        "Partition {} processed {} tokens with balances",
        partition_id,
        partition_results.len()
    );

    Ok(partition_results)
}

pub fn get_balance_map(
    conn: &Connection,
    addresses: &Addresses,
) -> Result<HashMap<Felt, HashMap<Felt, Felt>>> {
    let total_start = std::time::SystemTime::now();

    // Get the database path from the connection
    let db_path = conn
        .path()
        .ok_or_else(|| eyre::eyre!("Database connection has no path"))?
        .to_string();

    let balances_selector = starknet_keccak("ERC20_balances".as_bytes());

    // Step 1: Create accounts hash map (single-threaded, fast)
    let hashing_start = std::time::SystemTime::now();
    let accounts_hash_map: HashMap<Felt, Felt> = addresses
        .accounts
        .par_iter()
        .map(|item| {
            let val = pedersen_hash(&balances_selector, item);
            (val, *item)
        })
        .collect();
    let hashing_end = std::time::SystemTime::now();
    let hash_time = hashing_end.duration_since(hashing_start).unwrap();
    println!("Hashing time: {:?} ms", hash_time.as_millis());

    // Step 2: Process database partitions in parallel
    let parallel_processing_start = std::time::SystemTime::now();

    let num_tokens = addresses.tokens.len();
    let num_cores = rayon::current_num_threads();

    println!(
        "Processing {} tokens using {} CPU cores with storage-based partitioning",
        num_tokens, num_cores
    );

    // Process database partitions in parallel - each partition handles ALL tokens
    let partition_results: Vec<Result<HashMap<Felt, HashMap<Felt, Felt>>>> = (0..num_cores)
        .into_par_iter()
        .map(|partition_id| {
            process_storage_partition(
                &db_path,
                partition_id,
                num_cores,
                &addresses.tokens,
                &accounts_hash_map,
            )
        })
        .collect();

    let parallel_processing_end = std::time::SystemTime::now();
    let parallel_processing_time = parallel_processing_end
        .duration_since(parallel_processing_start)
        .unwrap();
    println!(
        "Parallel processing time: {:?} ms",
        parallel_processing_time.as_millis()
    );

    // Step 3: Merge results from all partitions
    let merging_start = std::time::SystemTime::now();

    let mut final_token_map: HashMap<Felt, HashMap<Felt, Felt>> = HashMap::new();

    // Initialize all tokens with empty balances
    for token in &addresses.tokens {
        final_token_map.insert(*token, HashMap::new());
    }

    for partition_result in partition_results {
        let partition_tokens = partition_result?;
        for (token, balances) in partition_tokens {
            // Merge balances for this token
            let token_balances = final_token_map.entry(token).or_insert_with(HashMap::new);
            for (account, balance) in balances {
                token_balances.insert(account, balance);
            }
        }
    }

    let merging_end = std::time::SystemTime::now();
    let merging_time = merging_end.duration_since(merging_start).unwrap();
    println!("Result merging time: {:?} ms", merging_time.as_millis());

    let total_end = std::time::SystemTime::now();
    let total_time = total_end.duration_since(total_start).unwrap();
    println!("Total function time: {:?} ms", total_time.as_millis());

    // Print summary for each token
    for token in &addresses.tokens {
        let balance_count = final_token_map.get(token).map(|m| m.len()).unwrap_or(0);
        println!("#### Token: {token:#064x} - {balance_count} balances ######");
    }

    Ok(final_token_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection as TestConnection;
    use tempfile::NamedTempFile;

    fn create_test_database() -> eyre::Result<(TestConnection, NamedTempFile)> {
        let temp_file = NamedTempFile::new()?;
        let conn = TestConnection::open(temp_file.path())?;

        // Create the tables according to the schema used in the query
        conn.execute(
            "CREATE TABLE contract_addresses (
                id INTEGER PRIMARY KEY,
                contract_address BLOB NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE storage_addresses (
                id INTEGER PRIMARY KEY,
                storage_address BLOB NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE storage_updates (
                id INTEGER PRIMARY KEY,
                contract_address_id INTEGER NOT NULL,
                storage_address_id INTEGER NOT NULL,
                storage_value BLOB NOT NULL,
                block_number INTEGER NOT NULL,
                FOREIGN KEY (contract_address_id) REFERENCES contract_addresses(id),
                FOREIGN KEY (storage_address_id) REFERENCES storage_addresses(id)
            )",
            [],
        )?;

        Ok((conn, temp_file))
    }

    fn insert_test_data(conn: &TestConnection) -> eyre::Result<()> {
        // Insert contract addresses
        conn.execute(
            "INSERT INTO contract_addresses (id, contract_address) VALUES (1, ?)",
            [vec![
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
                0x1d, 0x1e, 0x1f, 0x20,
            ]],
        )?;

        // Insert storage addresses (these will be the hashed account addresses)
        // Account 1: 0x00fb35d68faa85e6a5d2f4ec1bb996928942ab831783b0220d7074cef42a0de1
        conn.execute(
            "INSERT INTO storage_addresses (id, storage_address) VALUES (1, ?)",
            [vec![
                0x00, 0xfb, 0x35, 0xd6, 0x8f, 0xaa, 0x85, 0xe6, 0xa5, 0xd2, 0xf4, 0xec, 0x1b, 0xb9,
                0x96, 0x92, 0x89, 0x42, 0xab, 0x83, 0x17, 0x83, 0xb0, 0x22, 0x0d, 0x70, 0x74, 0xce,
                0xf4, 0x2a, 0x0d, 0xe1,
            ]],
        )?;

        // Account 2: 0x0777e9a0c5c09c2862d1697e51dc5d7dd5a3a98f50250be5debb7f49c2109ede
        conn.execute(
            "INSERT INTO storage_addresses (id, storage_address) VALUES (2, ?)",
            [vec![
                0x07, 0x77, 0xe9, 0xa0, 0xc5, 0xc0, 0x9c, 0x28, 0x62, 0xd1, 0x69, 0x7e, 0x51, 0xdc,
                0x5d, 0x7d, 0xd5, 0xa3, 0xa9, 0x8f, 0x50, 0x25, 0x0b, 0xe5, 0xde, 0xbb, 0x7f, 0x49,
                0xc2, 0x10, 0x9e, 0xde,
            ]],
        )?;

        // Insert storage updates
        // Token 1, Account 1, Balance 1000
        conn.execute(
            "INSERT INTO storage_updates (contract_address_id, storage_address_id, storage_value, block_number) VALUES (1, 1, ?, 100)",
            [vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8]], // 1000 in little endian
        )?;

        // Token 1, Account 2, Balance 2000
        conn.execute(
            "INSERT INTO storage_updates (contract_address_id, storage_address_id, storage_value, block_number) VALUES (1, 2, ?, 100)",
            [vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xd0]], // 2000 in little endian
        )?;

        Ok(())
    }

    #[test]
    fn test_get_balance_map() -> eyre::Result<()> {
        // Create test database with temporary file
        let (conn, _temp_file) = create_test_database()?;
        insert_test_data(&conn)?;

        // Create test addresses with only one token to simplify the test
        let addresses = Addresses {
            accounts: vec![
                Felt::from_hex(
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                )?,
                Felt::from_hex(
                    "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                )?,
            ],
            tokens: vec![Felt::from_hex(
                "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
            )?],
        };

        // Call get_balance_map
        let result = get_balance_map(&conn, &addresses)?;

        // Verify the results
        assert_eq!(result.len(), 1, "Should have 1 token");

        // Check token 1 balances
        let token1 =
            Felt::from_hex("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")?;
        let token1_balances = result.get(&token1).expect("Token 1 should exist");
        assert_eq!(token1_balances.len(), 2, "Token 1 should have 2 accounts");

        let account1 =
            Felt::from_hex("0x0234567890abcdcd1234567890abcdef1234567890abcdef1234567890abcded")?;
        let account2 =
            Felt::from_hex("0x03cdef123456772babcdef1234567890abcdef1234567890abcdef123456787b")?;

        let balance1 = token1_balances
            .get(&account1)
            .expect("Account 1 should exist in token 1");
        let balance2 = token1_balances
            .get(&account2)
            .expect("Account 2 should exist in token 1");

        assert_eq!(
            balance1.to_string(),
            "1000",
            "Account 1 should have balance 1000"
        );
        assert_eq!(
            balance2.to_string(),
            "2000",
            "Account 2 should have balance 2000"
        );

        println!("Test passed successfully!");
        Ok(())
    }

    #[test]
    fn test_get_balance_map_empty_result() -> eyre::Result<()> {
        // Create test database with temporary file
        let (conn, _temp_file) = create_test_database()?;
        insert_test_data(&conn)?;

        // Create test addresses with non-existent token
        let addresses = Addresses {
            accounts: vec![Felt::from_hex(
                "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            )?],
            tokens: vec![Felt::from_hex(
                "0x9999999999999999999999999999999999999999999999999999999999999999",
            )?],
        };

        // Call get_balance_map
        let result = get_balance_map(&conn, &addresses)?;

        // Verify the results - should be empty for non-existent token
        assert_eq!(result.len(), 1, "Should have 1 token entry");
        let token =
            Felt::from_hex("0x9999999999999999999999999999999999999999999999999999999999999999")?;
        let balances = result.get(&token).expect("Token should exist");
        assert_eq!(
            balances.len(),
            0,
            "Should have no balances for non-existent token"
        );

        println!("Empty result test passed successfully!");
        Ok(())
    }
}
