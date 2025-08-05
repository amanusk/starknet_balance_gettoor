# Starknet Pathfinder Balance Query

Quickly query the balance of multiple addresses and tokens on Starknet, using the Pathfinder DB.

Instead of using an RPC call, this reads balances directly from the Pathfinder DB.


## Instructions

### Build

```
cargo build --release
```

### Setup

- Copy the `addresses.example.json` file to `addresses.json` and add the addresses and tokens you would like to query
- Copy the .env.example file to .env and add your RPC provider
- Run with `cargo run --release`

## Example output

```
{
    0x4718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d: {
        0x3a08ecef30eaef46780a5167eac194d7cf0407356dccdc7393f851dfc164fd6: 0xe6c85f07294ac8790,
        0x7c1cbbafca15fec62f943de72793ddd40c0ae92884354e301cdd610f7c90106: 0xb0397a59483a216972,
        0x5c6a836fc25536d24dffb4c3c7fc1cfd3e2b5925669e51ab2b59c4ceb4cd25e: 0xd38af56dcfbfc42aea2d,
    },
    0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7: {
        0x3a08ecef30eaef46780a5167eac194d7cf0407356dccdc7393f851dfc164fd6: 0x58ca828c2b176b1,
        0x5c6a836fc25536d24dffb4c3c7fc1cfd3e2b5925669e51ab2b59c4ceb4cd25e: 0x3da3fe89965d8d457f,
    },
}
```


## CI/CD

This project uses GitHub Actions for continuous integration. The CI pipeline runs on:

- **Push to main branch**: Automatically runs tests, formatting checks, and linting
- **Pull requests**: Ensures code quality before merging

### CI Checks

The following checks are performed:

- ✅ **Code formatting**: Ensures code follows Rust formatting standards
- ✅ **Linting**: Runs clippy with strict warnings enabled
- ✅ **Tests**: Runs all unit tests
- ✅ **Build**: Ensures the project builds successfully in release mode

### Local Development

To run the same checks locally:

```bash
# Format code
cargo fmt --all

# Check formatting
cargo fmt --all -- --check

# Run linter
cargo clippy --all-targets --all-features -- -D warnings

# Run tests
cargo test

# Build release
cargo build --release
```


If you like it then you shoulda put a ⭐ on it