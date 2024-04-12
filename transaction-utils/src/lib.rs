use itertools::Itertools;
use serde::{Deserialize, Serialize};
use solana_sdk::{
    clock::UnixTimestamp, instruction::CompiledInstruction, message::VersionedMessage,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
    EncodedTransaction, EncodedTransactionWithStatusMeta, UiCompiledInstruction, UiInstruction,
    UiMessage, UiParsedInstruction, VersionedTransactionWithStatusMeta,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ParsedTransaction {
    pub slot: u64,
    pub block_time: Option<UnixTimestamp>,
    pub instructions: Vec<ParsedInstruction>,
    pub inner_instructions: Vec<Vec<ParsedInnerInstruction>>,
    pub logs: Vec<String>,
    pub is_err: bool,
    pub signature: String,
    pub fee_payer: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParsedInstruction {
    pub program_id: String,
    pub accounts: Vec<String>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParsedInnerInstruction {
    pub parent_index: usize,
    pub instruction: ParsedInstruction,
}

pub fn parse_ui_compiled_instruction(
    c: &UiCompiledInstruction,
    account_keys: &[String],
) -> ParsedInstruction {
    ParsedInstruction {
        program_id: account_keys[c.program_id_index as usize].clone(),
        accounts: c
            .accounts
            .iter()
            .map(|i| account_keys[*i as usize].clone())
            .collect(),
        data: bs58::decode(c.data.clone()).into_vec().unwrap(),
    }
}

pub fn parse_ui_instruction(
    ui_instruction: &UiInstruction,
    account_keys: &[String],
) -> ParsedInstruction {
    match ui_instruction {
        UiInstruction::Compiled(c) => parse_ui_compiled_instruction(c, account_keys),
        UiInstruction::Parsed(p) => match p {
            UiParsedInstruction::PartiallyDecoded(pd) => ParsedInstruction {
                program_id: pd.program_id.clone(),
                accounts: pd.accounts.clone(),
                data: bs58::decode(&pd.data.clone()).into_vec().unwrap(),
            },
            _ => panic!("Unsupported instruction encoding"),
        },
    }
}

pub fn parse_compiled_instruction(
    instruction: &CompiledInstruction,
    account_keys: &[String],
) -> ParsedInstruction {
    ParsedInstruction {
        program_id: account_keys[instruction.program_id_index as usize].clone(),
        accounts: instruction
            .accounts
            .iter()
            .map(|i| account_keys[*i as usize].clone())
            .collect(),
        data: instruction.data.clone(),
    }
}

pub fn parse_ui_message(
    message: UiMessage,
    loaded_addresses: &[String],
) -> (Vec<String>, Vec<ParsedInstruction>) {
    match message {
        UiMessage::Parsed(p) => {
            let mut keys = p
                .account_keys
                .iter()
                .map(|k| k.pubkey.to_string())
                .collect::<Vec<String>>();
            keys.extend_from_slice(loaded_addresses);
            (
                keys.clone(),
                p.instructions
                    .iter()
                    .map(|i| parse_ui_instruction(i, &keys))
                    .collect::<Vec<ParsedInstruction>>(),
            )
        }
        UiMessage::Raw(r) => {
            let mut keys = r.account_keys.clone();
            keys.extend_from_slice(loaded_addresses);
            (
                keys.clone(),
                r.instructions
                    .iter()
                    .map(|i| parse_ui_compiled_instruction(i, &keys))
                    .collect::<Vec<ParsedInstruction>>(),
            )
        }
    }
}

pub fn parse_versioned_message(
    message: VersionedMessage,
    loaded_addresses: &[String],
) -> (Vec<String>, Vec<ParsedInstruction>) {
    let mut keys = message
        .static_account_keys()
        .into_iter()
        .map(|pk| pk.to_string())
        .collect_vec();
    keys.extend_from_slice(loaded_addresses);
    let instructions = message
        .instructions()
        .iter()
        .map(|i| parse_compiled_instruction(i, &keys))
        .collect_vec();
    (keys, instructions)
}

pub fn parse_transaction(tx: EncodedConfirmedTransactionWithStatusMeta) -> ParsedTransaction {
    let slot = tx.slot;
    let block_time = tx.block_time;

    let tx_meta = tx.transaction.meta.unwrap();
    let loaded_addresses = match tx_meta.loaded_addresses {
        OptionSerializer::Some(l) => [l.writable, l.readonly].concat(),
        _ => vec![],
    };

    let (keys, instructions, signature) = match tx.transaction.transaction {
        EncodedTransaction::Json(t) => {
            let (keys, instructions) = parse_ui_message(t.message, &loaded_addresses);
            let signature = t.signatures[0].to_string();
            (keys, instructions, signature)
        }
        _ => {
            let versioned_tx = tx
                .transaction
                .transaction
                .decode()
                .expect("Failed to decode transaction");
            let (keys, instructions) =
                parse_versioned_message(versioned_tx.message, &loaded_addresses);
            (keys, instructions, versioned_tx.signatures[0].to_string())
        }
    };

    let is_err = tx_meta.err.is_some();
    let logs = match tx_meta.log_messages {
        OptionSerializer::Some(l) => l,
        _ => vec![],
    };
    let inner_instructions = match tx_meta.inner_instructions {
        OptionSerializer::Some(inner) => inner
            .iter()
            .map(|ii| {
                ii.instructions
                    .iter()
                    .map(|i| ParsedInnerInstruction {
                        parent_index: ii.index as usize,
                        instruction: parse_ui_instruction(i, &keys),
                    })
                    .collect::<Vec<ParsedInnerInstruction>>()
            })
            .collect::<Vec<Vec<ParsedInnerInstruction>>>(),
        _ => vec![],
    };
    ParsedTransaction {
        slot,
        block_time,
        instructions,
        inner_instructions,
        logs,
        is_err,
        signature,
        fee_payer: keys[0].clone(),
    }
}

pub fn parse_versioned_transaction(
    slot: u64,
    block_time: Option<i64>,
    tx: VersionedTransactionWithStatusMeta,
) -> Option<ParsedTransaction> {
    let tx_meta = tx.meta;
    let is_err = tx_meta.status.is_err();
    if is_err {
        return None;
    }
    let loaded_addresses = [
        tx_meta.loaded_addresses.writable,
        tx_meta.loaded_addresses.readonly,
    ]
    .concat()
    .iter()
    .map(|x| x.to_string())
    .collect::<Vec<String>>();

    let (keys, instructions) =
        { parse_versioned_message(tx.transaction.message, loaded_addresses.as_slice()) };

    let logs = tx_meta.log_messages.unwrap_or_default();
    let inner_instructions = tx_meta
        .inner_instructions
        .unwrap_or_default()
        .iter()
        .map(|ii| {
            ii.instructions
                .iter()
                .map(|i| ParsedInnerInstruction {
                    parent_index: ii.index as usize,
                    instruction: parse_compiled_instruction(&i.instruction, &keys),
                })
                .collect::<Vec<ParsedInnerInstruction>>()
        })
        .collect::<Vec<Vec<ParsedInnerInstruction>>>();
    Some(ParsedTransaction {
        slot,
        signature: tx.transaction.signatures[0].to_string(),
        block_time,
        instructions,
        inner_instructions,
        logs,
        is_err,
        fee_payer: keys[0].clone(),
    })
}

pub fn parse_encoded_transaction_with_status_meta(
    slot: u64,
    block_time: Option<i64>,
    tx: EncodedTransactionWithStatusMeta,
) -> Option<ParsedTransaction> {
    let tx_meta = tx.meta?;
    let loaded_addresses = match tx_meta.loaded_addresses {
        OptionSerializer::Some(la) => [la.writable, la.readonly].concat(),
        _ => vec![],
    };

    let versioned_tx = tx.transaction.decode()?;
    let (keys, instructions) =
        { parse_versioned_message(versioned_tx.message, loaded_addresses.as_slice()) };

    let is_err = tx_meta.status.is_err();
    let logs = match tx_meta.log_messages {
        OptionSerializer::Some(lm) => lm,
        _ => vec![],
    };
    let inner_instructions = match tx_meta.inner_instructions {
        OptionSerializer::Some(inner_instructions) => inner_instructions
            .iter()
            .map(|ii| {
                ii.instructions
                    .iter()
                    .map(|i| ParsedInnerInstruction {
                        parent_index: ii.index as usize,
                        instruction: parse_ui_instruction(i, &keys),
                    })
                    .collect::<Vec<ParsedInnerInstruction>>()
            })
            .collect::<Vec<Vec<ParsedInnerInstruction>>>(),
        _ => vec![],
    };
    Some(ParsedTransaction {
        slot,
        signature: versioned_tx.signatures[0].to_string(),
        block_time,
        instructions,
        inner_instructions,
        logs,
        is_err,
        fee_payer: keys[0].clone(),
    })
}
