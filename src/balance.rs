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

    // Step 2: Process each token in parallel
    let parallel_processing_start = std::time::SystemTime::now();

    let num_tokens = addresses.tokens.len();
    let num_cores = rayon::current_num_threads();
    println!("Processing {num_tokens} tokens using {num_cores} CPU cores");
    // Determine how many shards (DB partitions) to use per token to saturate all cores
    let shards_per_token = std::cmp::max(1, num_cores / std::cmp::max(1, num_tokens));
    println!(
        "Using {} shards per token ({} total concurrent DB connections)",
        shards_per_token,
        shards_per_token * std::cmp::max(1, num_tokens)
    );

    let token_results: Vec<Result<(Felt, HashMap<Felt, Felt>)>> = addresses
        .tokens
        .par_iter()
        .map(|token| {
            // Create placeholders for this token
            let token_hex = format!("{token:#064x}")[2..].to_string();
            let token_bytes = hex::decode(&token_hex).unwrap_or_default();

            // Run shards in parallel for this token
            type ShardRow = (String, String, String, i64);
            let shard_results: Vec<Result<Vec<ShardRow>>> = (0..shards_per_token)
                .into_par_iter()
                .map(|shard_idx| {
                    // Each shard uses its own DB connection
                    let shard_conn = create_connection(&db_path)?;

                    // Partition on storage_addresses.id modulo shards_per_token
                    let batch_query = r#"
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
                                contract_address = ?1
                                AND (storage_addresses.id % ?2) = ?3
                            GROUP BY
                                contract_address_id,
                                storage_address_id
                        "#;

                    let mut stmt = shard_conn
                        .prepare(batch_query)
                        .map_err(|e| eyre::eyre!("Failed to prepare SQL statement: {}", e))?;

                    let rows = stmt
                        .query_map(
                            rusqlite::params![
                                &token_bytes,
                                shards_per_token as i64,
                                shard_idx as i64
                            ],
                            |row| {
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
                            },
                        )
                        .map_err(|e| eyre::eyre!("Failed to execute query: {}", e))?;

                    // Collect all rows for this shard
                    let all_rows: Result<Vec<_>, _> = rows.collect();
                    let all_rows =
                        all_rows.map_err(|e| eyre::eyre!("Failed to collect rows: {}", e))?;
                    Ok(all_rows)
                })
                .collect();

            // Process rows for this token aggregated from all shards
            let mut token_balances: HashMap<Felt, Felt> = HashMap::new();
            for shard_rows in shard_results {
                for (_contract_address_hex, storage_addr, storage_val, _max_block) in shard_rows? {
                    let storage_str = format!("0x{storage_addr}");
                    let storage_addr_felt = match Felt::from_hex(&storage_str) {
                        Ok(f) => f,
                        Err(_) => continue,
                    };

                    let account = match accounts_hash_map.get(&storage_addr_felt) {
                        Some(a) => a,
                        None => continue,
                    };

                    let balance_felt = Felt::from_hex(&storage_val).unwrap_or(Felt::ZERO);
                    token_balances.insert(*account, balance_felt);
                }
            }

            Ok((*token, token_balances))
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

    // Step 3: Merge results from all tokens
    let merging_start = std::time::SystemTime::now();

    let mut final_token_map: HashMap<Felt, HashMap<Felt, Felt>> = HashMap::new();

    for token_result in token_results {
        let (token, balances) = token_result?;
        final_token_map.insert(token, balances);
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

    #[test]
    fn test_sharding_max_block_number_issue() -> eyre::Result<()> {
        // Create test database with temporary file
        let (conn, _temp_file) = create_test_database()?;

        // Insert contract address
        conn.execute(
            "INSERT INTO contract_addresses (id, contract_address) VALUES (1, ?)",
            [vec![
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
                0x1d, 0x1e, 0x1f, 0x20,
            ]],
        )?;

        // Insert storage addresses with IDs that will be distributed across shards
        // For 2 shards: id=1 % 2 = 1, id=2 % 2 = 0, id=3 % 2 = 1, id=4 % 2 = 0
        conn.execute(
            "INSERT INTO storage_addresses (id, storage_address) VALUES (1, ?)",
            [vec![
                0x00, 0xfb, 0x35, 0xd6, 0x8f, 0xaa, 0x85, 0xe6, 0xa5, 0xd2, 0xf4, 0xec, 0x1b, 0xb9,
                0x96, 0x92, 0x89, 0x42, 0xab, 0x83, 0x17, 0x83, 0xb0, 0x22, 0x0d, 0x70, 0x74, 0xce,
                0xf4, 0x2a, 0x0d, 0xe1,
            ]],
        )?;
        conn.execute(
            "INSERT INTO storage_addresses (id, storage_address) VALUES (2, ?)",
            [vec![
                0x07, 0x77, 0xe9, 0xa0, 0xc5, 0xc0, 0x9c, 0x28, 0x62, 0xd1, 0x69, 0x7e, 0x51, 0xdc,
                0x5d, 0x7d, 0xd5, 0xa3, 0xa9, 0x8f, 0x50, 0x25, 0x0b, 0xe5, 0xde, 0xbb, 0x7f, 0x49,
                0xc2, 0x10, 0x9e, 0xde,
            ]],
        )?;
        conn.execute(
            "INSERT INTO storage_addresses (id, storage_address) VALUES (3, ?)",
            [vec![
                0x08, 0x88, 0xfa, 0xb1, 0xd6, 0xc1, 0x0d, 0x39, 0x73, 0xe2, 0x7a, 0x8f, 0x62, 0xed,
                0x6e, 0x8e, 0xe6, 0xb4, 0xba, 0xa0, 0x61, 0x36, 0x1c, 0xf6, 0xef, 0xcc, 0x90, 0x5a,
                0xd3, 0x21, 0xaf, 0xef,
            ]],
        )?;
        conn.execute(
            "INSERT INTO storage_addresses (id, storage_address) VALUES (4, ?)",
            [vec![
                0x09, 0x99, 0x0b, 0xc2, 0xe7, 0xd2, 0x1e, 0x4a, 0x84, 0xf3, 0x8b, 0xa0, 0x73, 0xfe,
                0x7f, 0x9f, 0xf7, 0xc5, 0xcb, 0xb1, 0x72, 0x47, 0x2d, 0x07, 0x00, 0xdd, 0xa1, 0x6b,
                0xe4, 0x32, 0xc0, 0x00,
            ]],
        )?;

        // Insert multiple storage updates for the same storage_address_id with different block numbers
        // This tests if sharding correctly finds the MAX(block_number) across all shards

        // Storage address 1 (id=1, shard 1): old update
        conn.execute(
            "INSERT INTO storage_updates (contract_address_id, storage_address_id, storage_value, block_number) VALUES (1, 1, ?, 100)",
            [vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8]], // 1000
        )?;

        // Storage address 1 (id=1, shard 1): newer update - should be the one returned
        conn.execute(
            "INSERT INTO storage_updates (contract_address_id, storage_address_id, storage_value, block_number) VALUES (1, 1, ?, 200)",
            [vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xd0]], // 2000
        )?;

        // Storage address 2 (id=2, shard 0): old update
        conn.execute(
            "INSERT INTO storage_updates (contract_address_id, storage_address_id, storage_value, block_number) VALUES (1, 2, ?, 150)",
            [vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0xb8]], // 3000
        )?;

        // Storage address 2 (id=2, shard 0): newer update - should be the one returned
        conn.execute(
            "INSERT INTO storage_updates (contract_address_id, storage_address_id, storage_value, block_number) VALUES (1, 2, ?, 250)",
            [vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0x88]], // 5000
        )?;

        // Create test addresses
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

        let token =
            Felt::from_hex("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")?;
        let token_balances = result.get(&token).expect("Token should exist");

        // Should have 2 accounts with the NEWER balances (2000 and 5000)
        assert_eq!(
            token_balances.len(),
            2,
            "Should have 2 accounts with latest balances"
        );

        // Check that we got the newer values (2000 and 5000), not the older ones (1000 and 3000)
        let account1 =
            Felt::from_hex("0x0234567890abcdcd1234567890abcdef1234567890abcdef1234567890abcded")?;
        let account2 =
            Felt::from_hex("0x03cdef123456772babcdef1234567890abcdef1234567890abcdef123456787b")?;

        let balance1 = token_balances
            .get(&account1)
            .expect("Account 1 should exist");
        let balance2 = token_balances
            .get(&account2)
            .expect("Account 2 should exist");

        // These should be the NEWER balances from block 200 and 250
        assert_eq!(
            balance1.to_string(),
            "2000",
            "Account 1 should have NEWER balance 2000"
        );
        assert_eq!(
            balance2.to_string(),
            "5000",
            "Account 2 should have NEWER balance 5000"
        );

        println!("Sharding max block number test passed successfully!");
        Ok(())
    }
}
