# Warp sync example

This example makes repeated requests to a node to obtain warp sync information,
verifying it as it goes until we are up to date.

Use `cargo run` in this folder to run the example.

The example bootstraps from the embedded `polkadot-lightsync.json` checkpoint,
which contains the `finalizedBlockHeader` and `grandpaAuthoritySet` fields from
Polkadot's `lightSyncState`. Starting from a recent checkpoint keeps the first
warp proof small enough for the example to complete quickly; asking a peer to
serve the full proof from genesis can take a long time and may exceed practical
response limits.

To refresh the checkpoint:

```sh
curl -sS -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"sync_state_genSyncSpec","params":[true]}' \
  https://rpc.polkadot.io \
| jq '{finalizedBlockHeader: .result.lightSyncState.finalizedBlockHeader, grandpaAuthoritySet: .result.lightSyncState.grandpaAuthoritySet}' \
  > polkadot-lightsync.json
```
