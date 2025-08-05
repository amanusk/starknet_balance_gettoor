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

pub fn get_balance_map(
    conn: &Connection,
    addresses: &Addresses,
) -> Result<HashMap<Felt, HashMap<Felt, Felt>>> {
    let balances_selector = starknet_keccak("ERC20_balances".as_bytes());

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
    println!("Hashing time: {:?}", hash_time.as_millis());

    // Optimize: Single batched query for all tokens instead of sequential queries
    let batch_query_start = std::time::SystemTime::now();
    
    // Create placeholders for all tokens in the IN clause
    let token_placeholders = addresses.tokens.iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");
    
    let batch_query = format!(
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
            contract_address IN ({})
            GROUP BY
            contract_address_id,
            storage_address_id
        "#,
        token_placeholders
    );

    let mut stmt = conn
        .prepare(&batch_query)
        .map_err(|e| eyre::eyre!("Failed to prepare batched SQL statement: {}", e))?;

    // Convert tokens to hex bytes for the query parameters
    let token_params: Vec<Vec<u8>> = addresses.tokens.iter()
        .map(|token| {
            let token_hex = format!("{token:#064x}")[2..].to_string();
            hex::decode(&token_hex).unwrap_or_default()
        })
        .collect();

    // Create parameter references for the query
    let param_refs: Vec<&dyn rusqlite::ToSql> = token_params.iter()
        .map(|p| p as &dyn rusqlite::ToSql)
        .collect();

    let batch_query_end = std::time::SystemTime::now();
    let batch_query_time = batch_query_end.duration_since(batch_query_start).unwrap();
    println!("Batch query preparation time: {:?}", batch_query_time.as_millis());

    // Execute the batched query
    let execution_start = std::time::SystemTime::now();
    let rows = stmt
        .query_map(&param_refs[..], |row| {
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
        .map_err(|e| eyre::eyre!("Failed to execute batched query: {}", e))?;

    let execution_end = std::time::SystemTime::now();
    let execution_time = execution_end.duration_since(execution_start).unwrap();
    println!("Batch query execution time: {:?}", execution_time.as_millis());

    // Process results and group by token
    let processing_start = std::time::SystemTime::now();
    
    // Create a map from hex contract address to token Felt
    let contract_to_token: HashMap<String, Felt> = addresses.tokens.iter()
        .map(|token| {
            let token_hex = format!("{token:#064x}")[2..].to_string();
            (token_hex.to_uppercase(), *token)
        })
        .collect();

    // Collect all rows first to enable parallel processing
    let all_rows: Result<Vec<_>, _> = rows.collect();
    let all_rows = all_rows.map_err(|e| eyre::eyre!("Failed to collect rows: {}", e))?;

    println!("Collected {} total rows for processing", all_rows.len());

    // Process all rows in parallel and group by token
    let parallel_processing_start = std::time::SystemTime::now();
    
    let grouped_results: HashMap<Felt, Vec<(Felt, Felt)>> = all_rows
        .par_iter()
        .filter_map(|(contract_address_hex, storage_addr, storage_val, _)| {
            // Find which token this row belongs to
            let token = contract_to_token.get(&contract_address_hex.to_uppercase())?;
            
            let storage_str = format!("0x{storage_addr}");
            let storage_addr_felt = Felt::from_hex(&storage_str).ok()?;
            let account = accounts_hash_map.get(&storage_addr_felt)?;
            let balance_felt = Felt::from_hex(&storage_val).unwrap_or(Felt::ZERO);
            
            Some((*token, (*account, balance_felt)))
        })
        .fold(
            HashMap::new,
            |mut acc: HashMap<Felt, Vec<(Felt, Felt)>>, (token, account_balance)| {
                acc.entry(token).or_insert_with(Vec::new).push(account_balance);
                acc
            }
        )
        .reduce(
            HashMap::new,
            |mut acc, local_map| {
                for (token, mut balances) in local_map {
                    acc.entry(token).or_insert_with(Vec::new).append(&mut balances);
                }
                acc
            }
        );

    let parallel_processing_end = std::time::SystemTime::now();
    let parallel_processing_time = parallel_processing_end.duration_since(parallel_processing_start).unwrap();
    println!("Parallel processing time: {:?}", parallel_processing_time.as_millis());

    // Convert grouped results to final HashMap structure
    let conversion_start = std::time::SystemTime::now();
    
    let mut token_map: HashMap<Felt, HashMap<Felt, Felt>> = addresses.tokens.iter()
        .map(|token| (*token, HashMap::new()))
        .collect();

    for (token, account_balances) in grouped_results {
        if let Some(balance_map) = token_map.get_mut(&token) {
            for (account, balance) in account_balances {
                balance_map.insert(account, balance);
            }
        }
    }

    let conversion_end = std::time::SystemTime::now();
    let conversion_time = conversion_end.duration_since(conversion_start).unwrap();
    println!("Conversion time: {:?}", conversion_time.as_millis());

    let processing_end = std::time::SystemTime::now();
    let processing_time = processing_end.duration_since(processing_start).unwrap();
    println!("Total result processing time: {:?}", processing_time.as_millis());

    // Print summary for each token
    for token in &addresses.tokens {
        let balance_count = token_map.get(token).map(|m| m.len()).unwrap_or(0);
        println!("#### Token: {token:#064x} - {} balances ######", balance_count);
    }

    Ok(token_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection as TestConnection;

    fn create_test_database() -> eyre::Result<TestConnection> {
        let conn = TestConnection::open_in_memory()?;

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

        Ok(conn)
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
        // Create test database
        let conn = create_test_database()?;
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
        // Create test database
        let conn = create_test_database()?;
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
