use crate::pb::sol::transactions::v1::{Instruction, Transaction, Transactions};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use substreams_solana::pb::sf::solana::r#type::v1::{Block, CompiledInstruction};

use substreams_entity_change::pb::entity::EntityChanges;
use substreams_entity_change::tables::Tables;
use serde_json;

#[derive(Deserialize, Debug)]
struct TransactionFilterParams {
    signature: Option<String>,
}

#[derive(Deserialize, Debug)]
struct InstructionFilterParams {
    program_id: Option<String>,
}

#[derive(Serialize)]
struct SerializableInstruction {
    program_id: String,
    accounts: Vec<String>,
    data: String,
}

#[substreams::handlers::map]
fn map_filter_transactions(params: String, blk: Block) -> Result<Transactions, Vec<substreams::errors::Error>> {
    let filters = parse_instruction_filters_from_params(params)?;

    let timestamp = blk.block_time
        .as_ref()
        .map(|ts| ts.timestamp as u64)
        .unwrap_or_default();

    let mut transactions: Vec<Transaction> = Vec::new();

    blk.transactions
        .iter()
        .filter(|tx| {
            let msg = tx.transaction.as_ref().unwrap().message.as_ref().unwrap();
            let acct_keys: Vec<Vec<u8>> = tx.resolved_accounts().iter().map(|k| (*k).clone()).collect(); // 转换为 Vec<Vec<u8>>
            msg.instructions.iter().any(|inst| apply_filter(inst, &filters, &acct_keys))
        })
        .for_each(|tx| {
            let msg = tx.transaction.as_ref().unwrap().message.as_ref().unwrap();
            let acct_keys: Vec<Vec<u8>> = tx.resolved_accounts().iter().map(|k| (*k).clone()).collect(); // 转换为 Vec<Vec<u8>>

            let insts: Vec<Instruction> = msg
                .instructions
                .iter()
                .filter(|inst| apply_filter(inst, &filters, &acct_keys))
                .map(|inst| Instruction {
                    program_id: bs58::encode(acct_keys[inst.program_id_index as usize].to_vec()).into_string(),
                    accounts: inst
                        .accounts
                        .iter()
                        .map(|acct| bs58::encode(acct_keys[*acct as usize].to_vec()).into_string())
                        .collect(),
                    data: bs58::encode(&inst.data).into_string(),
                })
                .collect();

            if !insts.is_empty() {
                let t = Transaction {
                    signatures: tx
                        .transaction
                        .as_ref()
                        .unwrap()
                        .signatures
                        .iter()
                        .map(|sig| bs58::encode(sig).into_string())
                        .collect(),
                    instructions: insts,
                    timestamp: timestamp,
                };
                transactions.push(t);
            }
        });

    Ok(Transactions { transactions })
}

fn parse_instruction_filters_from_params(params: String) -> Result<InstructionFilterParams, Vec<substreams::errors::Error>> {
    let parsed_result = serde_qs::from_str(&params);
    if parsed_result.is_err() {
        return Err(Vec::from([anyhow!("Unexpected error while parsing parameters")]));
    }

    let filters = parsed_result.unwrap();
    Ok(filters)
}

fn apply_filter(instruction: &CompiledInstruction, filters: &InstructionFilterParams, account_keys: &Vec<Vec<u8>>) -> bool {
    if let Some(ref program_id_filter) = filters.program_id {
        if let Some(program_account_key) = account_keys.get(instruction.program_id_index as usize) {
            let program_account_key_val = bs58::encode(program_account_key).into_string();
            if &program_account_key_val != program_id_filter {
                // console::log!("program_account_key_val: {:?}", program_account_key_val);
                return false;
            }
        } else {
            // console::log!("program_account_key is None");
            return false;
        }
    }
    true
}

#[substreams::handlers::map]
fn graph_transactions_out(transactions: Transactions) -> Result<EntityChanges, substreams::errors::Error> {
    let mut tables = Tables::new();

    for transaction in transactions.transactions {
        let tx_id = &transaction.signatures[0]; // Create a unique transaction ID by joining signatures

        let row = tables.create_row("Transaction", tx_id.clone());
        row.set("signatures", serde_json::to_string(&transaction.signatures[0]).unwrap_or_default());

        for instruction in transaction.instructions {
            let inst_id = format!("{}_{}", tx_id, instruction.program_id);
            let mut inst_row = tables.create_row("Instruction", inst_id);
            inst_row.set("transaction_id", tx_id.clone());
            inst_row.set("program_id", instruction.program_id);
            inst_row.set("accounts", serde_json::to_string(&instruction.accounts).unwrap_or_default());
            inst_row.set("data", instruction.data);
        }
    }

    Ok(tables.to_entity_changes())
}
