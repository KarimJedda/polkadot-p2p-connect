//! Chain-specific constants for this example.
//!
//! Substrate-generic types (`BlockHeader`, `BlockDigest`, etc.) live in
//! the `warp-sync` crate; this module only carries the values that pin
//! the example to a specific chain.

/// Polkadot relay-chain genesis hash.
pub const GENESIS_HASH: [u8; 32] =
    hex_literal::hex!("91b171bb158e2d3848fa23a9f1c25182fb8e20313b2c1eb49219da7a70ce90c3");

/// Polkadot AssetHub (system parachain) genesis hash.
pub const ASSETHUB_GENESIS_HASH: [u8; 32] = hex_literal::hex!(
    "68d56f15f85d3136970ec16946040bc1752654e906147f7e43e9d539d7c3de2f"
);

/// AssetHub's para_id on Polkadot.
pub const ASSETHUB_PARA_ID: u32 = 1000;
