#[path = "../../warp-sync/src/polkadot.rs"]
mod warp_sync_polkadot;

pub use warp_sync_polkadot::*;

/// Well-known Polkadot AssetHub (system parachain, para_id 1000) genesis hash.
pub const ASSETHUB_GENESIS_HASH: [u8; 32] = hex_literal::hex!(
    "68d56f15f85d3136970ec16946040bc1752654e906147f7e43e9d539d7c3de2f"
);

/// AssetHub's para_id on Polkadot.
pub const ASSETHUB_PARA_ID: u32 = 1000;
