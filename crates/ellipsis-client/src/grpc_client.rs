use std::time::Duration;

use backoff::backoff::Backoff;
use itertools::Itertools;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, info, warn};
use {
    backoff::{future::retry, ExponentialBackoff},
    futures::stream::StreamExt,
    std::collections::HashMap,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterTransactions,
    },
};

use solana_sdk::commitment_config::CommitmentLevel as SolanaCommitmentLevel;

use borsh::BorshDeserialize;
use ellipsis_transaction_utils::{ParsedInnerInstruction, ParsedInstruction, ParsedTransaction};
use yellowstone_grpc_proto::prelude::{Message as YellowstoneMessage, TransactionStatusMeta};

struct YellowstoneTransaction {
    slot: u64,
    meta: TransactionStatusMeta,
    signature: Signature,
    message: YellowstoneMessage,
}

impl YellowstoneTransaction {
    pub fn parse_message(
        &self,
        loaded_addresses: &[String],
    ) -> (Vec<String>, Vec<ParsedInstruction>) {
        let mut keys = self
            .message
            .account_keys
            .iter()
            .map(|pk| Pubkey::try_from_slice(pk).unwrap().to_string())
            .collect_vec();
        keys.extend_from_slice(loaded_addresses);
        let instructions = self
            .message
            .instructions
            .iter()
            .map(|instruction| ParsedInstruction {
                program_id: keys[instruction.program_id_index as usize].clone(),
                accounts: instruction
                    .accounts
                    .iter()
                    .map(|i| keys[*i as usize].clone())
                    .collect(),
                data: instruction.data.clone(),
            })
            .collect_vec();
        (keys, instructions)
    }

    pub fn to_parsed_transaction(&self) -> ParsedTransaction {
        let loaded_addresses = [
            self.meta
                .loaded_writable_addresses
                .iter()
                .map(|x| Pubkey::try_from_slice(x).unwrap().to_string())
                .collect_vec(),
            self.meta
                .loaded_readonly_addresses
                .iter()
                .map(|x| Pubkey::try_from_slice(x).unwrap().to_string())
                .collect_vec(),
        ]
        .concat();

        let (keys, instructions) = self.parse_message(&loaded_addresses);
        let is_err = self.meta.err.is_some();
        let logs = self.meta.log_messages.clone();

        let inner_instructions = self
            .meta
            .inner_instructions
            .iter()
            .map(|ii| {
                ii.instructions
                    .iter()
                    .map(|i| ParsedInnerInstruction {
                        parent_index: ii.index as usize,
                        instruction: ParsedInstruction {
                            program_id: keys[i.program_id_index as usize].clone(),
                            accounts: i
                                .accounts
                                .iter()
                                .map(|i| keys[*i as usize].clone())
                                .collect(),
                            data: i.data.clone(),
                        },
                    })
                    .collect::<Vec<ParsedInnerInstruction>>()
            })
            .collect::<Vec<Vec<ParsedInnerInstruction>>>();

        let fee_payer = keys[0].clone();

        ParsedTransaction {
            fee_payer,
            slot: self.slot,
            block_time: None,
            signature: self.signature.to_string(),
            instructions,
            inner_instructions,
            logs,
            is_err,
        }
    }
}

struct ExponentialBackoffWrapper {
    backoff: ExponentialBackoff,
}

impl Backoff for ExponentialBackoffWrapper {
    fn next_backoff(&mut self) -> std::option::Option<std::time::Duration> {
        let next_backoff = self.backoff.next_backoff();
        if let Some(duration) = next_backoff {
            warn!(
                "Restarting retry loop. Next exponential backoff: {:?}ms",
                duration.as_millis()
            );
        }
        next_backoff
    }

    fn reset(&mut self) {
        self.backoff.reset()
    }
}

pub async fn transaction_subscribe(
    endpoint: String,
    x_token: Option<String>,
    sender: UnboundedSender<ParsedTransaction>,
    accounts_to_include: Vec<Pubkey>,
    accounts_to_exclude: Vec<Pubkey>,
    commitment_level: Option<SolanaCommitmentLevel>,
) -> anyhow::Result<()> {
    let mut transactions = HashMap::new();
    transactions.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: Some(false),
            signature: None,
            account_include: accounts_to_include
                .into_iter()
                .map(|x| x.to_string())
                .collect(),
            account_exclude: accounts_to_exclude
                .into_iter()
                .map(|x| x.to_string())
                .collect(),
            account_required: vec![],
        },
    );

    let mut backoff = ExponentialBackoffWrapper {
        backoff: ExponentialBackoff::default(),
    };
    backoff.next_backoff();
    backoff.reset();

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(backoff, move || {
        let (endpoint, x_token) = (endpoint.clone(), x_token.clone());
        let transactions = transactions.clone();
        let sender = sender.clone();
        async move {
            info!("Reconnecting to the gRPC server");
            let mut client = GeyserGrpcClient::build_from_shared(endpoint.clone())
                .and_then(|builder| builder.x_token(x_token.clone()))
                .map(|builder| builder.connect_timeout(Duration::from_secs(10)))
                .map(|builder| builder.timeout(Duration::from_secs(10)))
                .map_err(|e| anyhow::Error::msg(format!("Failed to create builder: {}", e)))?
                .connect()
                .await
                .map_err(|e| {
                    anyhow::Error::msg(format!(
                        "Failed to connected to endpoint: {} ({})",
                        endpoint, e
                    ))
                })?;

            // Need to map the SolanaCommitmentLevel to corresponding i32 value in the protobuf
            let commitment = commitment_level
                .map(|c| match c {
                    SolanaCommitmentLevel::Processed => CommitmentLevel::Processed as i32,
                    SolanaCommitmentLevel::Confirmed => CommitmentLevel::Confirmed as i32,
                    SolanaCommitmentLevel::Finalized => CommitmentLevel::Finalized as i32,
                    _ => CommitmentLevel::Confirmed as i32,
                })
                .unwrap_or(CommitmentLevel::Confirmed as i32);
            println!("Subscribing with commitment level: {:?}", commitment);
            let subscribe_request = SubscribeRequest {
                slots: HashMap::new(),
                accounts: HashMap::new(),
                transactions,
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                commitment: Some(commitment),
                accounts_data_slice: vec![],
                transactions_status: HashMap::new(),
                ping: None,
                entry: HashMap::new(),
            };
            let (_, mut stream) = client
                .subscribe_with_request(Some(subscribe_request))
                .await
                .map_err(|e| anyhow::Error::msg(format!("Failed to subscribe: {}", e)))?;

            while let Some(message) = stream.next().await {
                let parsed_tx = message.map(|msg| match msg.update_oneof {
                    Some(UpdateOneof::Transaction(transaction)) => {
                        let slot = transaction.slot;

                        transaction
                            .transaction
                            .and_then(|tx| {
                                if tx.meta.is_none() {
                                    warn!("Transaction meta is empty");
                                }
                                if tx.transaction.is_none() {
                                    warn!("Transaction is empty");
                                }
                                let message = tx.transaction.and_then(|x| x.message);
                                if message.is_none() {
                                    warn!("Transaction message is empty");
                                }
                                Some(YellowstoneTransaction {
                                    slot,
                                    meta: tx.meta?,
                                    signature: Signature::try_from(tx.signature).ok()?,
                                    message: message?,
                                })
                            })
                            .map(|tx| tx.to_parsed_transaction())
                    }
                    _ => None,
                });
                if let Ok(Some(tx)) = parsed_tx {
                    if sender.send(tx).is_err() {
                        error!("Failed to send transaction update");
                    }
                } else {
                    continue;
                }
            }
            Ok(())
        }
    })
    .await
    .map_err(Into::into)
}
