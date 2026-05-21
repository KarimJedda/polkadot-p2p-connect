//! Load a GRANDPA bootstrap checkpoint from a Substrate sync spec's
//! `lightSyncState` block. The caller supplies the JSON — either the
//! `lightSyncState` object on its own (as produced by some tooling) or
//! a full chain-spec document with a `lightSyncState` key (the shape
//! emitted by `chain-spec-builder`).

use crate::grandpa::{AuthorityId, GrandpaState};
use crate::substrate::BlockHeader;
use parity_scale_codec::Decode;
use serde::Deserialize;

#[derive(Deserialize)]
struct LightSyncState {
    #[serde(rename = "finalizedBlockHeader")]
    finalized_block_header: HexString,
    #[serde(rename = "grandpaAuthoritySet")]
    grandpa_authority_set: HexString,
}

struct HexString(Vec<u8>);

impl<'de> Deserialize<'de> for HexString {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <&str>::deserialize(d)?;
        let stripped = s
            .strip_prefix("0x")
            .ok_or_else(|| serde::de::Error::custom("expected 0x-prefixed hex string"))?;
        hex::decode(stripped)
            .map(HexString)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Decode)]
struct AuthoritySetPrefix {
    current_authorities: Vec<(AuthorityId, u64)>,
    set_id: u64,
}

/// Parse a `lightSyncState` JSON document and return the GRANDPA state
/// it encodes. Accepts either the bare `lightSyncState` object or a
/// full chain-spec JSON with a top-level `lightSyncState` key.
pub fn load(json: &str) -> anyhow::Result<GrandpaState> {
    let value: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| anyhow::anyhow!("failed to parse JSON: {e}"))?;
    let lss_value = match value.get("lightSyncState") {
        Some(v) => v.clone(),
        None => value,
    };
    let lss: LightSyncState = serde_json::from_value(lss_value)
        .map_err(|e| anyhow::anyhow!("malformed lightSyncState: {e}"))?;

    let header = BlockHeader::decode(&mut &lss.finalized_block_header.0[..])
        .map_err(|e| anyhow::anyhow!("failed to decode finalizedBlockHeader: {e}"))?;
    let finalized_hash = header.hash();

    let prefix = AuthoritySetPrefix::decode(&mut &lss.grandpa_authority_set.0[..])
        .map_err(|e| anyhow::anyhow!("failed to decode grandpaAuthoritySet: {e}"))?;

    Ok(GrandpaState {
        authorities: prefix.current_authorities,
        set_id: prefix.set_id,
        finalized_number: header.number,
        finalized_hash,
        finalized_state_root: header.state_root,
    })
}
