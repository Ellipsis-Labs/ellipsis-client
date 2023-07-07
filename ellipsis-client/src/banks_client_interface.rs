#![allow(deprecated)]

use ellipsis_transaction_utils::ParsedTransaction;
use {
    serde::{Deserialize, Serialize},
    solana_sdk::{
        account::Account,
        clock::Slot,
        commitment_config::CommitmentLevel,
        fee_calculator::FeeCalculator,
        hash::Hash,
        message::Message,
        pubkey::Pubkey,
        signature::Signature,
        transaction::{self, Transaction, TransactionError},
        transaction_context::TransactionReturnData,
    },
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionConfirmationStatus {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionStatus {
    pub slot: Slot,
    pub confirmations: Option<usize>, // None = rooted
    pub err: Option<TransactionError>,
    pub confirmation_status: Option<TransactionConfirmationStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSimulationDetails {
    pub logs: Vec<String>,
    pub units_consumed: u64,
    pub return_data: Option<TransactionReturnData>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BanksTransactionResultWithSimulation {
    pub result: Option<transaction::Result<()>>,
    pub simulation_details: Option<TransactionSimulationDetails>,
}

#[tarpc::service]
pub trait Banks {
    async fn send_transaction_with_context(transaction: Transaction);
    #[deprecated(
        since = "1.9.0",
        note = "Please use `get_fee_for_message_with_commitment_and_context` instead"
    )]
    async fn get_fees_with_commitment_and_context(
        commitment: CommitmentLevel,
    ) -> (FeeCalculator, Hash, Slot);
    async fn get_transaction_status_with_context(signature: Signature)
        -> Option<TransactionStatus>;
    async fn get_slot_with_context(commitment: CommitmentLevel) -> Slot;
    async fn get_block_height_with_context(commitment: CommitmentLevel) -> u64;
    async fn process_transaction_with_preflight_and_commitment_and_context(
        transaction: Transaction,
        commitment: CommitmentLevel,
    ) -> BanksTransactionResultWithSimulation;
    async fn process_transaction_with_commitment_and_context(
        transaction: Transaction,
        commitment: CommitmentLevel,
    ) -> Option<transaction::Result<()>>;
    async fn simulate_transaction_with_commitment_and_context(
        transaction: Transaction,
        commitment: CommitmentLevel,
    ) -> BanksTransactionResultWithSimulation;
    async fn get_account_with_commitment_and_context(
        address: Pubkey,
        commitment: CommitmentLevel,
    ) -> Option<Account>;
    async fn get_latest_blockhash_with_context() -> Hash;
    async fn get_latest_blockhash_with_commitment_and_context(
        commitment: CommitmentLevel,
    ) -> Option<(Hash, u64)>;
    async fn get_fee_for_message_with_commitment_and_context(
        commitment: CommitmentLevel,
        message: Message,
    ) -> Option<u64>;
    async fn get_transaction_with_commitment(signature: Signature) -> Option<ParsedTransaction>;
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        tarpc::{client, transport},
    };

    #[test]
    fn test_banks_client_new() {
        let (client_transport, _server_transport) = transport::channel::unbounded();
        BanksClient::new(client::Config::default(), client_transport);
    }
}
