# Performance Optimizations with Rayon

This document outlines the performance optimizations implemented using Rayon for parallel processing in the balance getter application.

## Original Implementation

Initially, only the Pedersen hash function computation was parallelized using Rayon:

```rust
let accounts_hash_map: HashMap<Felt, Felt> = addresses
    .accounts
    .par_iter()
    .map(|item| {
        let val = pedersen_hash(&balances_selector, item);
        (val, *item)
    })
    .collect();
```

## Optimizations Implemented

### 1. Parallel Processing of Query Results (`balance.rs`)

**Before**: Sequential processing of database query results
```rust
let mut balance_map = HashMap::new();
for row_result in rows {
    let (_, storage_addr, storage_val, _) = row_result?;
    // Sequential processing of each row
    let storage_str = format!("0x{storage_addr}");
    let storage_addr_felt = Felt::from_hex(&storage_str)?;
    if let Some(account) = accounts_hash_map.get(&storage_addr_felt) {
        let balance_felt = Felt::from_hex(&storage_val).unwrap_or(Felt::ZERO);
        balance_map.insert(*account, balance_felt);
    }
}
```

**After**: Parallel processing with Rayon
```rust
// Collect all rows first for parallel processing
let row_data: Vec<(String, String, String, i64)> = rows
    .collect::<Result<Vec<_>, _>>()
    .map_err(|e| eyre::eyre!("Failed to collect rows: {}", e))?;

// Process balance calculations in parallel using rayon
let balance_entries: Vec<(Felt, Felt)> = row_data
    .par_iter()
    .filter_map(|(_, storage_addr, storage_val, _)| {
        let storage_str = format!("0x{storage_addr}");
        
        if let Ok(storage_addr_felt) = Felt::from_hex(&storage_str) {
            if let Some(account) = accounts_hash_map.get(&storage_addr_felt) {
                let balance_felt = Felt::from_hex(&storage_val).unwrap_or(Felt::ZERO);
                return Some((*account, balance_felt));
            }
        }
        None
    })
    .collect();

// Convert to HashMap
let balance_map: HashMap<Felt, Felt> = balance_entries.into_iter().collect();
```

**Benefits**:
- Parallelizes hex string parsing and hash map lookups
- Utilizes multiple CPU cores for data processing
- Maintains data integrity by collecting first, then processing

### 2. Parallel CSV Data Preparation (`main.rs`)

**Before**: Sequential nested loop processing
```rust
for (token, sub_map) in token_map {
    for (account, balance) in sub_map {
        wtr.write_record(&[
            format!("{token:#064x}"),
            format!("{account:#064x}"),
            balance.to_string(),
        ])?;
    }
}
```

**After**: Parallel data preparation with sequential writing
```rust
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
```

**Benefits**:
- Parallelizes string formatting operations
- Uses nested parallelism for token and account processing
- Maintains CSV writing order integrity

### 3. Parallel SQLite Data Preparation (`main.rs`)

**Before**: Sequential nested loop with immediate database insertion
```rust
for (token, sub_map) in token_map {
    for (account, balance) in sub_map {
        stmt.execute(rusqlite::params![
            format!("{:#064x}", token),
            format!("{:#064x}", account),
            balance.to_string()
        ])?;
    }
}
```

**After**: Parallel data preparation with batched insertion
```rust
// Prepare all data in parallel, then insert sequentially
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

// Insert each row
for (token, account, balance) in records {
    stmt.execute(rusqlite::params![token, account, balance])?;
}
```

**Benefits**:
- Parallelizes string formatting operations
- Separates data preparation from database operations
- Maintains transaction integrity

## Additional Timing Measurements

Enhanced timing measurements have been added to track performance:

- **Individual token processing**: Separate timing for each token's query and balance map creation
- **Data fetch timing**: Separate measurement for database data collection vs. processing
- **Output generation timing**: Separate timing for CSV, JSON, and SQLite output generation
- **Data preparation timing**: Timing for parallel data preparation phases

## Performance Considerations

### Database Limitations
- **SQLite Connection Thread Safety**: Database queries remain sequential because SQLite connections are not thread-safe by default
- **I/O Bound Operations**: Database queries and file I/O operations cannot be easily parallelized

### CPU-Intensive Operations Optimized
- **String formatting**: Hex formatting operations are now parallelized
- **Hash computations**: Already optimized with Rayon
- **Data structure operations**: HashMap lookups and data transformations are parallelized
- **Data parsing**: Hex string parsing is now done in parallel

## Expected Performance Improvements

The optimizations should provide the most benefit when:

1. **Large datasets**: More accounts and tokens to process
2. **Multi-core systems**: Systems with 4+ CPU cores will see significant improvements
3. **CPU-intensive workloads**: When string formatting and parsing become bottlenecks

The actual performance improvement will depend on:
- Dataset size
- CPU core count
- Memory bandwidth
- Database query response times

## Usage

The optimized version maintains the same command-line interface:

```bash
./target/release/balance_gettor --input-file sample_addresses.json --db-path your_database.db
```

All timing information is printed to stdout, allowing you to measure and compare performance before and after optimizations.