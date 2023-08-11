use borsh::BorshDeserialize;
use itertools::Itertools;
use solana_sdk::signature::Signature;
use tokio::sync::mpsc::Sender;
use yellowstone_grpc_proto::prelude::{Message, TransactionStatusMeta};

use ellipsis_transaction_utils::{ParsedInnerInstruction, ParsedInstruction, ParsedTransaction};

use {
    backoff::{future::retry, ExponentialBackoff},
    futures::{sink::SinkExt, stream::StreamExt},
    solana_sdk::pubkey::Pubkey,
    std::collections::HashMap,
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError},
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterTransactions,
    },
};

struct YellowstoneTransaction {
    slot: u64,
    meta: TransactionStatusMeta,
    signature: Signature,
    message: Message,
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

        ParsedTransaction {
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

pub async fn transaction_subscribe(
    endpoint: String,
    x_token: Option<String>,
    sender: Sender<ParsedTransaction>,
    accounts_to_include: Vec<Pubkey>,
    accounts_to_exclude: Vec<Pubkey>,
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

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(ExponentialBackoff::default(), move || {
        let (endpoint, x_token) = (endpoint.clone(), x_token.clone());
        let transactions = transactions.clone();
        let sender = sender.clone();
        async move {
            println!("Reconnecting to the gRPC server");
            let mut client = GeyserGrpcClient::connect(endpoint, x_token, None)?;
            let (mut subscribe_tx, mut stream) = client.subscribe().await?;
            subscribe_tx
                .send(SubscribeRequest {
                    slots: HashMap::new(),
                    accounts: HashMap::new(),
                    transactions,
                    blocks: HashMap::new(),
                    blocks_meta: HashMap::new(),
                    commitment: None,
                    accounts_data_slice: vec![],
                })
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;

            while let Some(message) = stream.next().await {
                let parsed_tx = message.map(|msg| match msg.update_oneof {
                    Some(UpdateOneof::Transaction(transaction)) => {
                        let slot = transaction.slot;

                        transaction
                            .transaction
                            .and_then(|tx| {
                                if tx.meta.is_none() {
                                    println!("Transaction meta is empty");
                                }
                                if tx.transaction.is_none() {
                                    println!("Transaction is empty");
                                }
                                let message = tx.transaction.and_then(|x| x.message);
                                if message.is_none() {
                                    println!("Transaction message is empty");
                                }
                                Some(YellowstoneTransaction {
                                    slot,
                                    meta: tx.meta?,
                                    signature: Signature::new(&tx.signature),
                                    message: message?,
                                })
                            })
                            .map(|tx| tx.to_parsed_transaction())
                    }
                    _ => None,
                });
                if let Ok(Some(tx)) = parsed_tx {
                    if sender.send(tx).await.is_err() {
                        println!("Failed to send transaction update");
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
