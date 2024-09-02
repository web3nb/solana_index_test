use substreams_solana::pb::sf::solana::r#type::v1::Block;
use crate::pb::sol::block::v1::BlockMeta;

use substreams_entity_change::pb::entity::EntityChanges;
use substreams_entity_change::tables::Tables;

#[substreams::handlers::map]
fn map_block_meta(blk: Block) -> Result<BlockMeta, substreams::errors::Error> {
    let timestamp = blk.block_time
        .as_ref()
        .map(|ts| ts.timestamp as u64)
        .unwrap_or_default(); 

    Ok(BlockMeta {
        slot: blk.slot,
        hash: blk.blockhash,
        parent_hash: blk.previous_blockhash,
        timestamp: timestamp,
        transactions_count: blk.transactions.iter().count() as u64
    })
}

#[substreams::handlers::map]
fn graph_out(block_meta: BlockMeta) -> Result<EntityChanges, substreams::errors::Error> {
    // 创建一个名为 tables 的哈希表，用于存储表数据
    let mut tables = Tables::new();

    // 为 BlockMeta 创建一行数据，并设置相应的字段
    tables
        .create_row("BlockMeta", block_meta.hash.clone())
        .set("slot", block_meta.slot.to_string())
        .set("hash", block_meta.hash)
        .set("parentHash", block_meta.parent_hash)
        .set("timestamp", block_meta.timestamp)
        .set("transactions_count", block_meta.transactions_count);

    // 将表数据转换为 EntityChanges 并返回
    Ok(tables.to_entity_changes())
}
