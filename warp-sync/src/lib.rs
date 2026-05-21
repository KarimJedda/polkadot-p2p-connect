//! # warp-sync
//!
//! Reusable building blocks for trustless GRANDPA warp-sync against
//! Substrate-based chains. Sits one layer above
//! [`polkadot_p2p_connect`] (which handles the per-peer noise + yamux +
//! multistream + request-response lifecycle) and gives you:
//!
//! * `GrandpaState` — finalized-block + authority-set tracking, with
//!   `update_with_warp_sync_response` to fold in `/sync/warp` responses
//!   and advance finality (validating justifications, set-id rotations,
//!   and the trust chain along the way).
//! * `checkpoint::load` — decode a Substrate chain-spec's
//!   `lightSyncState` block into a `GrandpaState` so callers can skip
//!   the slow walk from genesis.
//! * Substrate-wire-format types — `BlockHeader`, `BlockHash`,
//!   `BlockDigest`, `ConsensusEngineId`. Chain-agnostic; chain-specific
//!   constants (genesis hashes, bootnode lists) belong in the consumer.

pub mod checkpoint;
pub mod grandpa;
pub mod substrate;

pub use checkpoint::load as load_checkpoint;
pub use grandpa::{AuthorityId, GrandpaState};
pub use substrate::{
    BlockDigest, BlockDigestItem, BlockHash, BlockHeader, ConsensusEngineId, Hash,
};
