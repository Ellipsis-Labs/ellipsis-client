use solana_program::clock::UnixTimestamp;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction, UiCompiledInstruction,
    UiInstruction, UiMessage, UiParsedInstruction,
};

#[derive(Clone)]
pub struct ParsedTransaction {
    pub slot: u64,
    pub block_time: Option<UnixTimestamp>,
    pub instructions: Vec<ParsedInstruction>,
    pub inner_instructions: Vec<Vec<ParsedInnerInstruction>>,
    pub logs: Vec<String>,
}

#[derive(Clone)]
pub struct ParsedInstruction {
    pub program_id: String,
    pub accounts: Vec<String>,
    pub data: Vec<u8>,
}

#[derive(Clone)]
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

pub fn parse_transaction(tx: EncodedConfirmedTransactionWithStatusMeta) -> ParsedTransaction {
    let slot = tx.slot;
    let block_time = tx.block_time;
    let ui_transaction = match tx.transaction.transaction {
        EncodedTransaction::Json(t) => t,
        _ => panic!("Unsupported transaction encoding"),
    };
    let (keys, instructions) = match ui_transaction.message {
        UiMessage::Parsed(p) => {
            let keys = p
                .account_keys
                .iter()
                .map(|k| k.pubkey.to_string())
                .collect::<Vec<String>>();
            (
                keys.clone(),
                p.instructions
                    .iter()
                    .map(|i| parse_ui_instruction(i, &keys))
                    .collect::<Vec<ParsedInstruction>>(),
            )
        }
        UiMessage::Raw(r) => (
            r.account_keys.clone(),
            r.instructions
                .iter()
                .map(|i| parse_ui_compiled_instruction(i, &r.account_keys))
                .collect::<Vec<ParsedInstruction>>(),
        ),
    };
    let tx_meta = tx.transaction.meta.unwrap();
    let logs = tx_meta.log_messages.unwrap_or(vec![]);
    let inner_instructions = tx_meta
        .inner_instructions
        .unwrap_or(vec![])
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
        .collect::<Vec<Vec<ParsedInnerInstruction>>>();

    ParsedTransaction {
        slot,
        block_time,
        instructions,
        inner_instructions,
        logs,
    }
}
