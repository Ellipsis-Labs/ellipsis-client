use anyhow::anyhow;
use async_trait::async_trait;
use ellipsis_transaction_utils::{parse_transaction, ParsedTransaction};
use itertools::Itertools;
use solana_banks_client::BanksClient;
use solana_banks_client::BanksClientError;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_program::{
    hash::Hash, instruction::Instruction, program_error::ProgramError, pubkey::Pubkey, rent::Rent,
};
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
    transport::TransportError,
};
use solana_transaction_status::UiTransactionEncoding;
use std::collections::{HashMap, HashSet};
use std::{
    ops::Deref,
    sync::{Arc, PoisonError},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::time::timeout;

pub type EllipsisClientResult<T = ()> = std::result::Result<T, EllipsisClientError>;

#[derive(Error, Debug)]
pub enum EllipsisClientError {
    #[error("Missing signer for transaction")]
    MissingSigner { signer: Pubkey },
    #[error("Transaction timed out")]
    TransactionTimeout { elapsed_ms: u64 },
    #[error("Action is not suppported")]
    UnsupportedAction,
    #[error("Solana client error")]
    SolanaClient(#[from] solana_client::client_error::ClientError),
    #[error("Some other error")]
    Other(#[from] anyhow::Error),
    #[error("Transaction Failed")]
    TransactionFailed {
        signature: Signature,
        logs: Vec<String>,
    },
    #[error("Transport Error")]
    TransportError(#[from] TransportError),
    #[error("Program Error")]
    ProgramError(#[from] ProgramError),
}

impl From<Box<dyn std::error::Error>> for EllipsisClientError {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        EllipsisClientError::Other(anyhow::Error::msg(e.to_string()))
    }
}

impl<T> From<PoisonError<T>> for EllipsisClientError {
    fn from(e: PoisonError<T>) -> Self {
        EllipsisClientError::Other(anyhow::Error::msg(e.to_string()))
    }
}

impl From<BanksClientError> for EllipsisClientError {
    fn from(e: BanksClientError) -> Self {
        EllipsisClientError::Other(anyhow::Error::msg(e.to_string()))
    }
}

impl From<std::io::Error> for EllipsisClientError {
    fn from(e: std::io::Error) -> Self {
        EllipsisClientError::TransportError(TransportError::from(e))
    }
}

pub fn clone_keypair(keypair: &Keypair) -> Keypair {
    Keypair::from_bytes(&keypair.to_bytes()).unwrap()
}

#[async_trait]
pub trait ClientSubset {
    async fn process_transaction(
        &self,
        mut tx: Transaction,
        signers: &[&Keypair],
    ) -> EllipsisClientResult<Signature>;
    async fn fetch_latest_blockhash(&self) -> EllipsisClientResult<Hash>;
    async fn fetch_transaction(
        &self,
        signature: &Signature,
    ) -> EllipsisClientResult<ParsedTransaction>;
    async fn fetch_account(&self, key: Pubkey) -> EllipsisClientResult<Account>;
}

pub struct EllipsisClient {
    /// The client used to interact with the cluster. Can be either a BanksClient or an RpcClient.
    /// This is wrapped in an Arc so that it can be cloned and used in multiple threads.
    /// The interface enables it to be used both in production and in tests.
    pub client: Arc<dyn ClientSubset + 'static + Sync + Send>,
    pub is_bank_client: bool,
    /// In the case that the client is an RpcClient, there should be flexibility to fall back on all of the methods
    /// available in the default RpcClient.
    rpc_client: Option<Arc<RpcClient>>,
    /// Primary payer for the client
    pub payer: Keypair,
    /// Keys that are allowed to sign for transactions
    keys: Vec<Keypair>,
    /// Default timeout in ms
    timeout_ms: u64,
}

impl Clone for EllipsisClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            is_bank_client: self.is_bank_client,
            rpc_client: self.rpc_client.clone(),
            payer: clone_keypair(&self.payer),
            keys: self.keys.iter().map(clone_keypair).collect(),
            timeout_ms: self.timeout_ms,
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.client = source.client.clone();
        self.is_bank_client = source.is_bank_client;
        self.rpc_client = source.rpc_client.clone();
        self.payer = clone_keypair(&source.payer);
        self.keys = self.keys.iter().map(clone_keypair).collect();
        self.timeout_ms = source.timeout_ms;
    }
}

impl EllipsisClient {
    pub fn new(url: &str) -> std::result::Result<Self, EllipsisClientError> {
        let rpc = RpcClient::new(url.to_string());
        let payer = Keypair::new();
        Self::from_rpc_with_timeout(rpc, &payer, 10000)
    }

    pub async fn from_banks(
        client: &BanksClient,
        payer: &Keypair,
    ) -> std::result::Result<Self, EllipsisClientError> {
        Self::from_banks_with_timeout(client, payer, 10000).await
    }

    pub async fn from_banks_with_timeout(
        client: &BanksClient,
        payer: &Keypair,
        timeout_ms: u64,
    ) -> std::result::Result<Self, EllipsisClientError> {
        let client = client.clone();
        Ok(Self {
            client: Arc::new(RwLock::new(client)),
            is_bank_client: true,
            rpc_client: None,
            payer: clone_keypair(payer),
            keys: vec![clone_keypair(payer)],
            timeout_ms,
        })
    }

    pub fn from_rpc(
        rpc: RpcClient,
        payer: &Keypair,
    ) -> std::result::Result<Self, EllipsisClientError> {
        Self::from_rpc_with_timeout(rpc, payer, 10000)
    }

    pub fn from_rpc_with_timeout(
        rpc: RpcClient,
        payer: &Keypair,
        timeout_ms: u64,
    ) -> std::result::Result<Self, EllipsisClientError> {
        let client = Arc::new(rpc);
        Ok(Self {
            client: Arc::new(client.clone()),
            is_bank_client: false,
            rpc_client: Some(client),
            payer: clone_keypair(payer),
            keys: vec![clone_keypair(payer)],
            timeout_ms,
        })
    }

    pub fn set_payer(&mut self, payer_key: &Pubkey) -> EllipsisClientResult<()> {
        if let Some(payer) = self.keys.iter().find(|key| key.pubkey() == *payer_key) {
            self.payer = clone_keypair(payer);
            Ok(())
        } else {
            Err(EllipsisClientError::MissingSigner { signer: *payer_key })
        }
    }

    pub fn add_keypair(&mut self, keypair: &Keypair) {
        if !self.keys.iter().any(|k| k.pubkey() == keypair.pubkey()) {
            self.keys.push(clone_keypair(keypair));
        }
    }

    pub fn remove_keypair(&mut self, keypair: &Keypair) {
        // You cannot remove the payer keypair
        if self.payer.pubkey() != keypair.pubkey() {
            self.keys.retain(|k| k.pubkey() != keypair.pubkey());
        }
    }

    pub async fn get_transaction(
        &self,
        signature: &Signature,
    ) -> EllipsisClientResult<ParsedTransaction> {
        self.client.fetch_transaction(signature).await
    }

    pub async fn sign_send_instructions_with_payer(
        &self,
        instructions: Vec<Instruction>,
        mut signers: Vec<&Keypair>,
    ) -> EllipsisClientResult<Signature> {
        signers.insert(0, &self.payer);
        self.send_sign_instructions_with_timeout(instructions, signers, Some(self.timeout_ms))
            .await
    }

    pub async fn sign_send_instructions(
        &self,
        instructions: Vec<Instruction>,
        signers: Vec<&Keypair>,
    ) -> EllipsisClientResult<Signature> {
        self.send_sign_instructions_with_timeout(instructions, signers, Some(self.timeout_ms))
            .await
    }

    pub async fn send_sign_instructions_with_timeout(
        &self,
        instructions: Vec<Instruction>,
        mut signers: Vec<&Keypair>,
        timeout_ms: Option<u64>,
    ) -> EllipsisClientResult<Signature> {
        let required_signers = instructions
            .iter()
            .flat_map(|i| {
                i.accounts
                    .iter()
                    .filter_map(|am| if am.is_signer { Some(am.pubkey) } else { None })
                    .collect::<Vec<Pubkey>>()
            })
            .unique()
            .collect::<Vec<Pubkey>>();

        let available_signers = self
            .keys
            .iter()
            .map(|k| (k.pubkey(), k))
            .collect::<HashMap<Pubkey, &Keypair>>();

        let existing_signers = signers
            .iter()
            .map(|k| k.pubkey())
            .unique()
            .collect::<HashSet<Pubkey>>();

        for required_signer in required_signers.iter() {
            if !existing_signers.contains(required_signer) {
                if available_signers.get(required_signer).is_some() {
                    signers.push(available_signers.get(required_signer).unwrap());
                } else {
                    return Err(EllipsisClientError::MissingSigner {
                        signer: *required_signer,
                    });
                }
            }
        }

        // Ensure that the payer is always the first signer
        let payer = {
            signers.retain(|k| k.pubkey() != self.payer.pubkey());
            signers.insert(0, &self.payer);
            self.payer.pubkey()
        };

        if let Some(ms) = timeout_ms {
            timeout(
                Duration::from_millis(ms),
                self.client.process_transaction(
                    Transaction::new_with_payer(&instructions, Some(&payer)),
                    &signers,
                ),
            )
            .await
            .unwrap_or(Err(EllipsisClientError::TransactionTimeout {
                elapsed_ms: ms,
            }))
        } else {
            self.client
                .process_transaction(
                    Transaction::new_with_payer(&instructions, Some(&payer)),
                    &signers,
                )
                .await
        }
    }

    pub async fn get_latest_blockhash(&self) -> EllipsisClientResult<Hash> {
        self.client.fetch_latest_blockhash().await
    }

    pub fn rent_exempt(&self, size: usize) -> u64 {
        Rent::default().minimum_balance(size)
    }

    pub async fn get_account(&self, key: &Pubkey) -> EllipsisClientResult<Account> {
        self.client.fetch_account(*key).await
    }

    pub async fn get_account_data(&self, key: &Pubkey) -> EllipsisClientResult<Vec<u8>> {
        Ok(self.get_account(key).await?.data)
    }
}

impl Deref for EllipsisClient {
    type Target = Arc<RpcClient>;
    fn deref(&self) -> &Self::Target {
        if self.is_bank_client {
            panic!("Cannot deref a BanksClient")
        }
        self.rpc_client.as_ref().unwrap()
    }
}

#[async_trait]
impl ClientSubset for Arc<RpcClient> {
    async fn process_transaction(
        &self,
        mut tx: Transaction,
        signers: &[&Keypair],
    ) -> EllipsisClientResult<Signature> {
        let blockhash = self
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .await?
            .0;
        tx.partial_sign(&signers.to_vec(), blockhash);
        let signature = self
            .send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: None,
                    encoding: None,
                    max_retries: None,
                    min_context_slot: None,
                },
            )
            .await?;
        Ok(signature)
    }

    /// Fetch transaction with 3 retries on failure
    async fn fetch_transaction(
        &self,
        signature: &Signature,
    ) -> EllipsisClientResult<ParsedTransaction> {
        let mut retries = 0;
        let tx = loop {
            match self
                .get_transaction_with_config(
                    signature,
                    RpcTransactionConfig {
                        encoding: Some(UiTransactionEncoding::Base58),
                        commitment: Some(CommitmentConfig::confirmed()),
                        max_supported_transaction_version: Some(0),
                    },
                )
                .await
            {
                Ok(res) => break Ok(res),
                Err(e) => {
                    retries += 1;
                    if retries == 3 {
                        return Err(EllipsisClientError::from(anyhow::Error::msg(format!(
                            "Failed to fetch transaction ({}): {}",
                            signature, e
                        ))));
                    }
                    continue;
                }
            }
        };
        tx.map(parse_transaction)
    }

    async fn fetch_latest_blockhash(&self) -> EllipsisClientResult<Hash> {
        Ok(self
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .await
            .map(|(hash, _)| hash)?)
    }

    async fn fetch_account(&self, key: Pubkey) -> EllipsisClientResult<Account> {
        Ok(self
            .get_account_with_commitment(&key, CommitmentConfig::confirmed())
            .await?
            .value
            .ok_or_else(|| anyhow!("Failed to get account"))?)
    }
}

#[async_trait]
impl ClientSubset for RwLock<BanksClient> {
    async fn process_transaction(
        &self,
        mut tx: Transaction,
        signers: &[&Keypair],
    ) -> EllipsisClientResult<Signature> {
        tx.partial_sign(&signers.to_vec(), self.fetch_latest_blockhash().await?);
        let sig = tx.signatures[0];
        self.write()
            .await
            .process_transaction_with_commitment(tx, CommitmentLevel::Confirmed)
            .await?;
        Ok(sig)
    }

    /// This is not supported by BanksClient
    async fn fetch_transaction(
        &self,
        _signature: &Signature,
    ) -> EllipsisClientResult<ParsedTransaction> {
        unimplemented!();
    }

    async fn fetch_latest_blockhash(&self) -> std::result::Result<Hash, EllipsisClientError> {
        self.write()
            .await
            .get_latest_blockhash()
            .await
            .map_err(EllipsisClientError::from)
    }

    async fn fetch_account(
        &self,
        key: Pubkey,
    ) -> std::result::Result<Account, EllipsisClientError> {
        self.write()
            .await
            .get_account_with_commitment(key, CommitmentLevel::Confirmed)
            .await?
            .ok_or_else(|| anyhow!("Failed to get account").into())
    }
}
