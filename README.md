# Ellipsis Client

Lightweight unifying client for RPC + BanksClient

There are no good unifying traits for Solana Rust clients. `EllipsisClient` creates a simple interface for sending transactions, reading data, and fetching transaction data. The ulitmate goal is to encourage as much reusable code as possible among both client apps and testing logic.

`EllipsisClient` has an identical feature set to the native `RpcClient` because it implements the `Deref` trait. This means that it can do everything that `RpcClient` can do and more.

```rust
impl Deref for EllipsisClient {
    type Target = Arc<RpcClient>;
    fn deref(&self) -> &Self::Target {
        if self.is_bank_client {
            panic!("Cannot deref a BanksClient")
        }
        self.rpc_client.as_ref().unwrap()
    }
}
```
