# warp-sync

Reusable building blocks for trustless GRANDPA warp-sync against
Substrate-based chains. Sits one layer above
[`polkadot-p2p-connect`](https://github.com/paritytech/polkadot-p2p-connect),
which handles the per-peer noise + yamux + multistream + request-response
lifecycle.

## What's exported

| Item                                    | What it does                                                                                                                |
|-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `GrandpaState`                          | Tracks finalized block + authority set; advance with `update_with_warp_sync_response(&bytes)` against a `/sync/warp` reply. |
| `checkpoint::load(&str)` / `load_checkpoint(&str)` | Decode a Substrate chain-spec's `lightSyncState` block into a `GrandpaState`. Accepts either the inner block or a full chain spec.   |
| `BlockHeader`, `BlockDigest`, `BlockDigestItem`, `ConsensusEngineId`, `BlockHash`, `Hash`, `AuthorityId` | Substrate wire-format types. Chain-agnostic.                                                                                |

Chain-specific things (genesis hashes, bootnode lists, storage keys)
intentionally live in the consumer crate. The same `warp-sync` build
works against Polkadot, Kusama, Paseo, AssetHub, etc.

## Using it from another crate

```toml
# Cargo.toml
[dependencies]
warp-sync = { path = "../warp-sync" }  # or { git = "..." }
```

```rust
use warp_sync::{checkpoint, BlockHeader, GrandpaState};

const LIGHT_SYNC_STATE_JSON: &str = include_str!("paseo-lightsync.json");

let mut grandpa: GrandpaState = checkpoint::load(LIGHT_SYNC_STATE_JSON)?;

// later, after a /sync/warp response arrives:
let done = grandpa.update_with_warp_sync_response(&bytes)?;
```

## Running the bundled demo

The crate also ships a `warp-sync` binary that connects to Polkadot
mainnet bootnodes over TCP and walks the checkpoint forward to the tip:

```sh
cargo run -p warp-sync
```

The demo embeds `polkadot-lightsync.json`, a recent Polkadot
`lightSyncState` snapshot. Starting from a recent checkpoint keeps the
first warp proof small enough to complete quickly — asking a peer to
serve the full proof from genesis can take a long time and may exceed
practical response limits.

To refresh the bundled checkpoint:

```sh
curl -sS -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"sync_state_genSyncSpec","params":[true]}' \
  https://rpc.polkadot.io \
| jq '{finalizedBlockHeader: .result.lightSyncState.finalizedBlockHeader, grandpaAuthoritySet: .result.lightSyncState.grandpaAuthoritySet}' \
  > warp-sync/polkadot-lightsync.json
```
