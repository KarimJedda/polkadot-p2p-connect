//! Hardcoded configuration and types for the Polkadot Relay Chain.

use parity_scale_codec::{Decode, Encode};

/// Polkadot genesis hash.
pub const GENESIS_HASH: [u8; 32] =
    hex_literal::hex!("91b171bb158e2d3848fa23a9f1c25182fb8e20313b2c1eb49219da7a70ce90c3");

pub type BlockHash = [u8; 32];
pub type Hash = [u8; 32];

/// Polkadot block headers.
#[derive(Clone, Debug, Encode, Decode)]
pub struct BlockHeader {
    pub parent_hash: BlockHash,
    #[codec(compact)]
    pub number: u32,
    pub state_root: Hash,
    pub extrinsics_root: Hash,
    pub digest: BlockDigest,
}

impl BlockHeader {
    pub fn hash(&self) -> BlockHash {
        use blake2::digest::consts::U32;
        use blake2::{Blake2b, Digest};
        Blake2b::<U32>::digest(self.encode()).into()
    }
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct BlockDigest {
    pub logs: Vec<BlockDigestItem>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub enum BlockDigestItem {
    #[codec(index = 0)]
    Other(Vec<u8>),
    #[codec(index = 4)]
    Consensus(ConsensusEngineId, Vec<u8>),
    #[codec(index = 5)]
    Seal(ConsensusEngineId, Vec<u8>),
    #[codec(index = 6)]
    PreRuntime(ConsensusEngineId, Vec<u8>),
    #[codec(index = 8)]
    RuntimeEnvironmentUpdated,
}

/// Consensus engine ID.
#[derive(Clone, Copy, Debug, Encode, Decode)]
pub struct ConsensusEngineId([u8; 4]);

impl ConsensusEngineId {
    pub fn is_grandpa(&self) -> bool {
        self.0 == *b"FRNK"
    }
}
