//! Generic Substrate block header / digest types and the GRANDPA
//! consensus-engine ID. These are chain-agnostic — Polkadot, Kusama,
//! Paseo, AssetHub etc. all share this wire format. Chain-specific
//! constants (genesis hashes, bootnodes) belong in the consumer.

use parity_scale_codec::{Decode, Encode};

pub type BlockHash = [u8; 32];
pub type Hash = [u8; 32];

/// Substrate block header — `parent_hash` + compact `number` + `state_root`
/// + `extrinsics_root` + `digest`. Hashable via blake2b-256.
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

/// Four-byte consensus engine identifier present in digest items.
/// Substrate convention: `b"FRNK"` is GRANDPA, `b"BABE"` is BABE, etc.
#[derive(Clone, Copy, Debug, Encode, Decode)]
pub struct ConsensusEngineId([u8; 4]);

impl ConsensusEngineId {
    pub fn is_grandpa(&self) -> bool {
        self.0 == *b"FRNK"
    }
}
