//! Load a GRANDPA bootstrap checkpoint from a Polkadot sync spec's `lightSyncState` field.

use crate::grandpa::{AuthorityId, GrandpaState};
use crate::polkadot::BlockHeader;
use parity_scale_codec::Decode;
use serde::Deserialize;

const LIGHT_SYNC_STATE_JSON: &str = include_str!("../polkadot-lightsync.json");

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

pub fn load() -> anyhow::Result<GrandpaState> {
    let lss: LightSyncState = serde_json::from_str(LIGHT_SYNC_STATE_JSON)?;

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
