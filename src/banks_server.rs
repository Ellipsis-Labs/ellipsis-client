use std::collections::HashMap;

use solana_program_runtime::timings::ExecuteTimings;
use solana_runtime::bank::{TransactionExecutionDetails, TransactionExecutionResult};
use solana_sdk::{clock::MAX_PROCESSING_AGE, transaction::VersionedTransaction};

use crate::transaction_utils::{ParsedInnerInstruction, ParsedInstruction, ParsedTransaction};

use {
    crate::banks_client_interface::{
        Banks, BanksRequest, BanksResponse, BanksTransactionResultWithSimulation,
        TransactionConfirmationStatus, TransactionSimulationDetails, TransactionStatus,
    },
    bincode::{deserialize, serialize},
    crossbeam_channel::{unbounded, Receiver, Sender},
    futures::{future, prelude::stream::StreamExt},
    solana_client::connection_cache::ConnectionCache,
    solana_runtime::{
        bank::{Bank, TransactionSimulationResult},
        bank_forks::BankForks,
        commitment::BlockCommitmentCache,
    },
    solana_sdk::{
        account::Account,
        clock::Slot,
        commitment_config::CommitmentLevel,
        feature_set::FeatureSet,
        fee_calculator::FeeCalculator,
        hash::Hash,
        message::{Message, SanitizedMessage},
        pubkey::Pubkey,
        signature::Signature,
        transaction::{self, SanitizedTransaction, Transaction},
    },
    solana_send_transaction_service::{
        send_transaction_service::{SendTransactionService, TransactionInfo},
        tpu_info::NullTpuInfo,
    },
    std::{
        convert::TryFrom,
        io,
        net::{Ipv4Addr, SocketAddr},
        sync::{Arc, RwLock},
        thread::Builder,
        time::Duration,
    },
    tarpc::{
        context::Context,
        serde_transport::tcp,
        server::{self, incoming::Incoming, Channel},
        transport::{self, channel::UnboundedChannel},
        ClientMessage, Response,
    },
    tokio::time::sleep,
    tokio_serde::formats::Bincode,
};

fn get_parsed_transaction(
    details: TransactionExecutionDetails,
    tx: VersionedTransaction,
    slot: u64,
) -> ParsedTransaction {
    let accounts = tx
        .message
        .static_account_keys()
        .iter()
        .map(|pk| pk.to_string())
        .collect::<Vec<_>>();
    let instructions = tx
        .message
        .instructions()
        .iter()
        .map(|ix| ParsedInstruction {
            program_id: accounts[ix.program_id_index as usize].clone(),
            accounts: ix
                .accounts
                .iter()
                .map(|i| accounts[*i as usize].clone())
                .collect(),
            data: ix.data.clone(),
        })
        .collect::<Vec<_>>();

    let signature = tx.signatures[0].to_string();
    let inner_instructions = details
        .inner_instructions
        .unwrap_or_default()
        .iter()
        .enumerate()
        .filter_map(|(i, ixs)| {
            if ixs.len() == 0 {
                None
            } else {
                Some(
                    ixs.iter()
                        .map(|ix| ParsedInnerInstruction {
                            parent_index: i,
                            instruction: ParsedInstruction {
                                program_id: accounts[ix.program_id_index as usize].clone(),
                                accounts: ix
                                    .accounts
                                    .iter()
                                    .map(|i| accounts[*i as usize].clone())
                                    .collect(),
                                data: ix.data.clone(),
                            },
                        })
                        .collect::<Vec<_>>(),
                )
            }
        })
        .collect::<Vec<_>>();
    ParsedTransaction {
        slot,
        block_time: None,
        instructions: instructions.clone(),
        inner_instructions,
        signature,
        logs: details.log_messages.unwrap_or_default(),
        is_err: false,
    }
}

#[derive(Clone)]
struct BanksServer {
    bank_forks: Arc<RwLock<BankForks>>,
    block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    transaction_sender: Sender<TransactionInfo>,
    poll_signature_status_sleep_duration: Duration,
    transaction_map: Arc<RwLock<HashMap<Signature, ParsedTransaction>>>,
}

impl BanksServer {
    /// Return a BanksServer that forwards transactions to the
    /// given sender. If unit-testing, those transactions can go to
    /// a bank in the given BankForks. Otherwise, the receiver should
    /// forward them to a validator in the leader schedule.
    fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        transaction_sender: Sender<TransactionInfo>,
        poll_signature_status_sleep_duration: Duration,
        transaction_map: Arc<RwLock<HashMap<Signature, ParsedTransaction>>>,
    ) -> Self {
        Self {
            bank_forks,
            block_commitment_cache,
            transaction_sender,
            poll_signature_status_sleep_duration,
            transaction_map,
        }
    }

    fn run(
        bank_forks: Arc<RwLock<BankForks>>,
        transaction_receiver: Receiver<TransactionInfo>,
        transaction_map: Arc<RwLock<HashMap<Signature, ParsedTransaction>>>,
    ) {
        while let Ok(info) = transaction_receiver.recv() {
            let signature = info.signature;
            let mut transaction_infos = vec![info];
            while let Ok(info) = transaction_receiver.try_recv() {
                transaction_infos.push(info);
            }
            let bank = bank_forks.read().unwrap().working_bank();
            let transactions: Vec<VersionedTransaction> = transaction_infos
                .into_iter()
                .map(|info| deserialize(&info.wire_transaction).unwrap())
                .collect();

            let transactions_to_check = transactions.clone();

            let batch = match bank.prepare_entry_batch(transactions) {
                Ok(batch) => batch,
                Err(_) => continue,
            };
            let slot = bank.slot();
            let execution_results = bank
                .load_execute_and_commit_transactions(
                    &batch,
                    MAX_PROCESSING_AGE,
                    false,
                    true,
                    true,
                    false,
                    &mut ExecuteTimings::default(),
                    None,
                )
                .0
                .execution_results;
            for (result, tx) in execution_results.into_iter().zip(transactions_to_check) {
                if let TransactionExecutionResult::Executed { details, .. } = result {
                    transaction_map
                        .write()
                        .unwrap()
                        .insert(signature, get_parsed_transaction(details, tx, slot));
                }
            }
        }
    }

    /// Useful for unit-testing
    fn new_loopback(
        bank_forks: Arc<RwLock<BankForks>>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        poll_signature_status_sleep_duration: Duration,
    ) -> Self {
        let (transaction_sender, transaction_receiver) = unbounded();
        let bank = bank_forks.read().unwrap().working_bank();
        let slot = bank.slot();
        {
            // ensure that the commitment cache and bank are synced
            let mut w_block_commitment_cache = block_commitment_cache.write().unwrap();
            w_block_commitment_cache.set_all_slots(slot, slot);
        }
        let server_bank_forks = bank_forks.clone();
        let transaction_map = Arc::new(RwLock::new(HashMap::new()));
        let bank_transactions = transaction_map.clone();
        Builder::new()
            .name("solBankForksCli".to_string())
            .spawn(move || Self::run(server_bank_forks, transaction_receiver, bank_transactions))
            .unwrap();
        Self::new(
            bank_forks,
            block_commitment_cache,
            transaction_sender,
            poll_signature_status_sleep_duration,
            transaction_map,
        )
    }

    fn slot(&self, commitment: CommitmentLevel) -> Slot {
        self.block_commitment_cache
            .read()
            .unwrap()
            .slot_with_commitment(commitment)
    }

    fn bank(&self, commitment: CommitmentLevel) -> Arc<Bank> {
        self.bank_forks.read().unwrap()[self.slot(commitment)].clone()
    }

    async fn poll_signature_status(
        self,
        signature: &Signature,
        blockhash: &Hash,
        last_valid_block_height: u64,
        commitment: CommitmentLevel,
    ) -> Option<transaction::Result<()>> {
        let mut status = self
            .bank(commitment)
            .get_signature_status_with_blockhash(signature, blockhash);
        while status.is_none() {
            sleep(self.poll_signature_status_sleep_duration).await;
            let bank = self.bank(commitment);
            if bank.block_height() > last_valid_block_height {
                break;
            }
            status = bank.get_signature_status_with_blockhash(signature, blockhash);
        }
        status
    }
}

fn verify_transaction(
    transaction: &Transaction,
    feature_set: &Arc<FeatureSet>,
) -> transaction::Result<()> {
    if let Err(err) = transaction.verify() {
        Err(err)
    } else if let Err(err) = transaction.verify_precompiles(feature_set) {
        Err(err)
    } else {
        Ok(())
    }
}

fn simulate_transaction(
    bank: &Bank,
    transaction: Transaction,
) -> BanksTransactionResultWithSimulation {
    let sanitized_transaction = match SanitizedTransaction::try_from_legacy_transaction(transaction)
    {
        Err(err) => {
            return BanksTransactionResultWithSimulation {
                result: Some(Err(err)),
                simulation_details: None,
            };
        }
        Ok(tx) => tx,
    };
    let TransactionSimulationResult {
        result,
        logs,
        post_simulation_accounts: _,
        units_consumed,
        return_data,
    } = bank.simulate_transaction_unchecked(sanitized_transaction);
    let simulation_details = TransactionSimulationDetails {
        logs,
        units_consumed,
        return_data,
    };
    BanksTransactionResultWithSimulation {
        result: Some(result),
        simulation_details: Some(simulation_details),
    }
}

#[tarpc::server]
impl Banks for BanksServer {
    async fn send_transaction_with_context(self, _: Context, transaction: Transaction) {
        let blockhash = &transaction.message.recent_blockhash;
        let last_valid_block_height = self
            .bank_forks
            .read()
            .unwrap()
            .root_bank()
            .get_blockhash_last_valid_block_height(blockhash)
            .unwrap();
        let signature = transaction.signatures.get(0).cloned().unwrap_or_default();
        let info = TransactionInfo::new(
            signature,
            serialize(&transaction).unwrap(),
            last_valid_block_height,
            None,
            None,
            None,
        );
        self.transaction_sender.send(info).unwrap();
    }

    async fn get_fees_with_commitment_and_context(
        self,
        _: Context,
        commitment: CommitmentLevel,
    ) -> (FeeCalculator, Hash, u64) {
        let bank = self.bank(commitment);
        let blockhash = bank.last_blockhash();
        let lamports_per_signature = bank.get_lamports_per_signature();
        let last_valid_block_height = bank
            .get_blockhash_last_valid_block_height(&blockhash)
            .unwrap();
        (
            FeeCalculator::new(lamports_per_signature),
            blockhash,
            last_valid_block_height,
        )
    }

    async fn get_transaction_status_with_context(
        self,
        _: Context,
        signature: Signature,
    ) -> Option<TransactionStatus> {
        let bank = self.bank(CommitmentLevel::Processed);
        let (slot, status) = bank.get_signature_status_slot(&signature)?;
        let r_block_commitment_cache = self.block_commitment_cache.read().unwrap();

        let optimistically_confirmed_bank = self.bank(CommitmentLevel::Confirmed);
        let optimistically_confirmed =
            optimistically_confirmed_bank.get_signature_status_slot(&signature);

        let confirmations = if r_block_commitment_cache.root() >= slot
            && r_block_commitment_cache.highest_confirmed_root() >= slot
        {
            None
        } else {
            r_block_commitment_cache
                .get_confirmation_count(slot)
                .or(Some(0))
        };
        Some(TransactionStatus {
            slot,
            confirmations,
            err: status.err(),
            confirmation_status: if confirmations.is_none() {
                Some(TransactionConfirmationStatus::Finalized)
            } else if optimistically_confirmed.is_some() {
                Some(TransactionConfirmationStatus::Confirmed)
            } else {
                Some(TransactionConfirmationStatus::Processed)
            },
        })
    }

    async fn get_slot_with_context(self, _: Context, commitment: CommitmentLevel) -> Slot {
        self.slot(commitment)
    }

    async fn get_block_height_with_context(self, _: Context, commitment: CommitmentLevel) -> u64 {
        self.bank(commitment).block_height()
    }

    async fn process_transaction_with_preflight_and_commitment_and_context(
        self,
        ctx: Context,
        transaction: Transaction,
        commitment: CommitmentLevel,
    ) -> BanksTransactionResultWithSimulation {
        let mut simulation_result =
            simulate_transaction(&self.bank(commitment), transaction.clone());
        // Simulation was ok, so process the real transaction and replace the
        // simulation's result with the real transaction result
        if let Some(Ok(_)) = simulation_result.result {
            simulation_result.result = self
                .process_transaction_with_commitment_and_context(ctx, transaction, commitment)
                .await;
        }
        simulation_result
    }

    async fn simulate_transaction_with_commitment_and_context(
        self,
        _: Context,
        transaction: Transaction,
        commitment: CommitmentLevel,
    ) -> BanksTransactionResultWithSimulation {
        simulate_transaction(&self.bank(commitment), transaction)
    }

    async fn process_transaction_with_commitment_and_context(
        self,
        _: Context,
        transaction: Transaction,
        commitment: CommitmentLevel,
    ) -> Option<transaction::Result<()>> {
        if let Err(err) = verify_transaction(&transaction, &self.bank(commitment).feature_set) {
            return Some(Err(err));
        }

        let blockhash = &transaction.message.recent_blockhash;
        let last_valid_block_height = self
            .bank(commitment)
            .get_blockhash_last_valid_block_height(blockhash)
            .unwrap();
        let signature = transaction.signatures.get(0).cloned().unwrap_or_default();
        let info = TransactionInfo::new(
            signature,
            serialize(&transaction).unwrap(),
            last_valid_block_height,
            None,
            None,
            None,
        );
        self.transaction_sender.send(info).unwrap();
        self.poll_signature_status(&signature, blockhash, last_valid_block_height, commitment)
            .await
    }

    async fn get_account_with_commitment_and_context(
        self,
        _: Context,
        address: Pubkey,
        commitment: CommitmentLevel,
    ) -> Option<Account> {
        let bank = self.bank(commitment);
        bank.get_account(&address).map(Account::from)
    }

    async fn get_latest_blockhash_with_context(self, _: Context) -> Hash {
        let bank = self.bank(CommitmentLevel::default());
        bank.last_blockhash()
    }

    async fn get_latest_blockhash_with_commitment_and_context(
        self,
        _: Context,
        commitment: CommitmentLevel,
    ) -> Option<(Hash, u64)> {
        let bank = self.bank(commitment);
        let blockhash = bank.last_blockhash();
        let last_valid_block_height = bank.get_blockhash_last_valid_block_height(&blockhash)?;
        Some((blockhash, last_valid_block_height))
    }

    async fn get_fee_for_message_with_commitment_and_context(
        self,
        _: Context,
        commitment: CommitmentLevel,
        message: Message,
    ) -> Option<u64> {
        let bank = self.bank(commitment);
        let sanitized_message = SanitizedMessage::try_from(message).ok()?;
        bank.get_fee_for_message(&sanitized_message)
    }

    async fn get_transaction_with_commitment(
        self,
        _: Context,
        signature: Signature,
    ) -> Option<ParsedTransaction> {
        let tx = self
            .transaction_map
            .read()
            .unwrap()
            .get(&signature)
            .map(|p| p.clone());
        if let Some(t) = tx.clone() {
            println!("{:?}", t);
        }
        tx
    }
}

pub async fn start_local_server(
    bank_forks: Arc<RwLock<BankForks>>,
    block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    poll_signature_status_sleep_duration: Duration,
) -> UnboundedChannel<Response<BanksResponse>, ClientMessage<BanksRequest>> {
    let banks_server = BanksServer::new_loopback(
        bank_forks,
        block_commitment_cache,
        poll_signature_status_sleep_duration,
    );
    let (client_transport, server_transport) = transport::channel::unbounded();
    let server = server::BaseChannel::with_defaults(server_transport).execute(banks_server.serve());
    tokio::spawn(server);
    client_transport
}

pub async fn start_tcp_server(
    listen_addr: SocketAddr,
    tpu_addr: SocketAddr,
    bank_forks: Arc<RwLock<BankForks>>,
    block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    connection_cache: Arc<ConnectionCache>,
) -> io::Result<()> {
    // Note: These settings are copied straight from the tarpc example.
    let server = tcp::listen(listen_addr, Bincode::default)
        .await?
        // Ignore accept errors.
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // Limit channels to 1 per IP.
        .max_channels_per_key(1, |t| {
            t.as_ref()
                .peer_addr()
                .map(|x| x.ip())
                .unwrap_or_else(|_| Ipv4Addr::new(0, 0, 0, 0).into())
        })
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated Banks trait.
        .map(move |chan| {
            let (sender, receiver) = unbounded();

            SendTransactionService::new::<NullTpuInfo>(
                tpu_addr,
                &bank_forks,
                None,
                receiver,
                &connection_cache,
                5_000,
                0,
            );

            let server = BanksServer::new(
                bank_forks.clone(),
                block_commitment_cache.clone(),
                sender,
                Duration::from_millis(200),
                Arc::new(RwLock::new(HashMap::new())),
            );
            chan.execute(server.serve())
        })
        // Max 10 channels.
        .buffer_unordered(10)
        .for_each(|_| async {});

    server.await;
    Ok(())
}
