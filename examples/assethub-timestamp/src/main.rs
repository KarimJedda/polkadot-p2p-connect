mod checkpoint;
mod grandpa;
mod polkadot;

use anyhow::Context;
use core::pin::Pin;
use core::time::Duration;
use parity_scale_codec::{Decode, Encode};
use polkadot_p2p_connect::{
    AsyncRead, AsyncReadError, AsyncWrite, AsyncWriteError, Configuration, Connection, Message,
    PlatformT, RequestProtocol, RequestResponse, SubscriptionProtocol, SubscriptionResponse,
};
use sp_core::Blake2Hasher;
use sp_core::hashing::{twox_64, twox_128};
use sp_state_machine::read_proof_check;
use sp_trie::CompactProof;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use polkadot::{ASSETHUB_GENESIS_HASH, ASSETHUB_PARA_ID, BlockHeader, GENESIS_HASH};

/// Provide a tokio-based [`AsyncRead`] implementation.
struct TokioTcpReader(tokio::net::tcp::OwnedReadHalf);
impl AsyncRead for TokioTcpReader {
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), AsyncReadError> {
        AsyncReadExt::read_exact(&mut self.0, buf)
            .await
            .map(|_| ())
            .map_err(AsyncReadError::new)
    }
}

/// Provide a tokio-based [`AsyncWrite`] implementation.
struct TokioTcpWriter(tokio::net::tcp::OwnedWriteHalf);
impl AsyncWrite for TokioTcpWriter {
    async fn write_all(&mut self, data: &[u8]) -> Result<(), AsyncWriteError> {
        AsyncWriteExt::write_all(&mut self.0, data)
            .await
            .map_err(AsyncWriteError::new)
    }
}

/// Provide a tokio-based [`PlatformT`] implementation.
struct TokioPlatform;
impl PlatformT for TokioPlatform {
    type Sleep = Pin<Box<tokio::time::Sleep>>;

    fn fill_with_random_bytes(bytes: &mut [u8]) {
        use rand::RngCore;
        rand::thread_rng().fill_bytes(bytes);
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        Box::pin(tokio::time::sleep(duration))
    }
}

type TokioConnection = Connection<TokioTcpReader, TokioTcpWriter, TokioPlatform>;

const RELAY_BOOTNODES: &[(&str, u16)] = &[
    ("polkadot-bootnode-1.polkadot.io", 30333),
    ("polkadot-bootnode-0.polkadot.io", 30333),
    ("boot-node.helikon.io", 7070),
];

const ASSETHUB_BOOTNODES: &[(&str, u16)] = &[
    ("polkadot-asset-hub-connect-0.polkadot.io", 30334),
    ("polkadot-asset-hub-connect-1.polkadot.io", 30334),
    ("boot-node.helikon.io", 10220),
    ("statemint-bootnode.turboflakes.io", 30315),
    ("asset-hub-polkadot.bootnode.amforc.com", 30007),
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // --- Phase 1: warp sync the relay chain and learn AssetHub's head via Paras::Heads ---
    let (assethub_head_hash, assethub_head_number, assethub_state_root) =
        learn_assethub_head().await?;

    eprintln!(
        "[parachain] AssetHub head from relay: #{} hash=0x{} state_root=0x{}",
        assethub_head_number,
        hex::encode(assethub_head_hash),
        hex::encode(assethub_state_root),
    );

    // --- Phase 2: connect to AssetHub, request Timestamp.Now, verify against
    //              the state root we just got from the relay chain. ---
    let now_ms = fetch_assethub_timestamp(
        &assethub_head_hash,
        &assethub_state_root,
        assethub_head_number,
    )
    .await?;

    println!(
        "Polkadot AssetHub Timestamp.Now at finalized-on-relay block #{} is {} ms since Unix epoch",
        assethub_head_number, now_ms
    );

    Ok(())
}

/// Warp-sync the Polkadot relay chain, then read `Paras::Heads[1000]` from
/// the verified finalized state. Returns the AssetHub head hash, number, and
/// state root extracted from the parachain header.
async fn learn_assethub_head() -> anyhow::Result<([u8; 32], u32, [u8; 32])> {
    let genesis_hex = hex::encode(GENESIS_HASH);

    let mut config: Configuration<TokioPlatform> = Configuration::new();
    let block_announce_id = config.add_protocol(SubscriptionProtocol::new(
        format!("/{genesis_hex}/block-announces/1"),
        (2u8, 0u32, GENESIS_HASH, GENESIS_HASH).encode(),
        move |remote_hs| remote_hs.len() >= 69 && remote_hs[37..69] == GENESIS_HASH,
    ));
    let _grandpa_id = config.add_protocol(SubscriptionProtocol::new(
        format!("/{genesis_hex}/grandpa/1"),
        vec![2u8],
        |remote_hs| remote_hs.len() == 1,
    ));
    let warp_sync_id = config.add_protocol(
        RequestProtocol::new(format!("/{genesis_hex}/sync/warp"))
            .with_max_response_size(32 * 1024 * 1024)
            .with_timeout(Duration::from_secs(60)),
    );
    let state_id = config.add_protocol(
        RequestProtocol::new(format!("/{genesis_hex}/state/2"))
            .with_max_response_size(16 * 1024 * 1024)
            .with_timeout(Duration::from_secs(60)),
    );

    let mut grandpa_state = checkpoint::load()?;
    eprintln!(
        "[relay] starting from checkpoint #{} hash=0x{}",
        grandpa_state.finalized_number,
        hex::encode(grandpa_state.finalized_hash),
    );

    let mut bootnode_n = 0;
    let storage_key = paras_heads_key(ASSETHUB_PARA_ID);

    loop {
        let bootnode_addr = RELAY_BOOTNODES[bootnode_n % RELAY_BOOTNODES.len()];
        bootnode_n += 1;

        let mut conn = match connect_tcp(&config, bootnode_addr).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[relay] connect to {} failed: {e}; trying next", bootnode_addr.0);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        eprintln!("[relay] connected to {}", bootnode_addr.0);

        conn.subscribe(block_announce_id)?;

        let mut warp_sync_done = false;
        let outcome: anyhow::Result<([u8; 32], u32, [u8; 32])> = async {
            while let Some(result) = conn.next().await {
                match result? {
                    Message::Notification {
                        protocol_id,
                        res: SubscriptionResponse::Opened,
                    } if protocol_id == block_announce_id => {
                        eprintln!(
                            "[relay] requesting warp sync from #{}",
                            grandpa_state.finalized_number
                        );
                        conn.request(warp_sync_id, grandpa_state.finalized_hash.to_vec())?;
                    }
                    Message::Notification {
                        protocol_id,
                        res: SubscriptionResponse::Closed,
                    } if protocol_id == block_announce_id => {
                        anyhow::bail!("relay block-announce subscription closed by peer");
                    }
                    Message::Notification {
                        protocol_id,
                        res: SubscriptionResponse::Error(e),
                    } if protocol_id == block_announce_id => {
                        anyhow::bail!("relay block-announce subscription error: {e}");
                    }
                    Message::Response {
                        protocol_id,
                        res: RequestResponse::Value(bytes),
                        ..
                    } if protocol_id == warp_sync_id => {
                        let is_finished = grandpa_state
                            .update_with_warp_sync_response(&bytes)
                            .map_err(|e| anyhow::anyhow!(e))?;

                        eprintln!(
                            "[relay] warp progress: #{}, set_id={}, {} authorities",
                            grandpa_state.finalized_number,
                            grandpa_state.set_id,
                            grandpa_state.authorities.len(),
                        );

                        if !is_finished {
                            conn.request(warp_sync_id, grandpa_state.finalized_hash.to_vec())?;
                            continue;
                        }

                        warp_sync_done = true;
                        eprintln!(
                            "[relay] warp complete: finalized #{} hash=0x{} state_root=0x{}",
                            grandpa_state.finalized_number,
                            hex::encode(grandpa_state.finalized_hash),
                            hex::encode(grandpa_state.finalized_state_root),
                        );

                        eprintln!(
                            "[relay] requesting Paras::Heads[{}] via /state/2",
                            ASSETHUB_PARA_ID
                        );
                        let req = encode_state_request(
                            &grandpa_state.finalized_hash,
                            storage_key.as_slice(),
                        );
                        conn.request(state_id, req)?;
                    }
                    Message::Response {
                        protocol_id,
                        res: RequestResponse::Error(e),
                        ..
                    } if protocol_id == warp_sync_id => {
                        anyhow::bail!("warp sync error from relay peer: {e}");
                    }
                    Message::Response {
                        protocol_id,
                        res: RequestResponse::Value(bytes),
                        ..
                    } if protocol_id == state_id => {
                        let value = verify_state_proof(
                            &bytes,
                            &grandpa_state.finalized_state_root,
                            &storage_key,
                        )?;
                        // Storage value at Paras::Heads is `HeadData = Vec<u8>`, which is
                        // itself a SCALE-encoded parachain `BlockHeader`.
                        let head_data: Vec<u8> = Decode::decode(&mut &value[..])
                            .context("decoding HeadData (outer Vec<u8>) from storage value")?;
                        let parachain_header: BlockHeader = Decode::decode(&mut &head_data[..])
                            .context("decoding AssetHub BlockHeader from HeadData bytes")?;
                        return Ok((
                            parachain_header.hash(),
                            parachain_header.number,
                            parachain_header.state_root,
                        ));
                    }
                    Message::Response {
                        protocol_id,
                        res: RequestResponse::Error(e),
                        ..
                    } if protocol_id == state_id => {
                        anyhow::bail!("relay /state/2 error: {e}");
                    }
                    _ => {}
                }
            }
            anyhow::bail!("relay connection closed before we got our answer");
        }
        .await;

        match outcome {
            Ok(out) => return Ok(out),
            Err(e) => {
                let stage = if warp_sync_done {
                    "Paras::Heads read"
                } else {
                    "warp sync"
                };
                eprintln!(
                    "[relay] {stage} failed on {}: {e}; trying another peer in 10s",
                    bootnode_addr.0
                );
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

/// Connect to an AssetHub peer, subscribe to block-announces (so the peer keeps
/// the link), then request and verify `Timestamp.Now` against the state root we
/// learned from the relay chain.
async fn fetch_assethub_timestamp(
    head_hash: &[u8; 32],
    state_root: &[u8; 32],
    head_number: u32,
) -> anyhow::Result<u64> {
    let genesis_hex = hex::encode(ASSETHUB_GENESIS_HASH);

    let mut config: Configuration<TokioPlatform> = Configuration::new();
    let block_announce_id = config.add_protocol(SubscriptionProtocol::new(
        format!("/{genesis_hex}/block-announces/1"),
        (2u8, 0u32, ASSETHUB_GENESIS_HASH, ASSETHUB_GENESIS_HASH).encode(),
        move |remote_hs| remote_hs.len() >= 69 && remote_hs[37..69] == ASSETHUB_GENESIS_HASH,
    ));
    let _grandpa_id = config.add_protocol(SubscriptionProtocol::new(
        format!("/{genesis_hex}/grandpa/1"),
        vec![2u8],
        |remote_hs| remote_hs.len() == 1,
    ));
    let state_id = config.add_protocol(
        RequestProtocol::new(format!("/{genesis_hex}/state/2"))
            .with_max_response_size(16 * 1024 * 1024)
            .with_timeout(Duration::from_secs(60)),
    );

    let key = timestamp_now_key();
    let mut bootnode_n = 0;

    loop {
        let bootnode_addr = ASSETHUB_BOOTNODES[bootnode_n % ASSETHUB_BOOTNODES.len()];
        bootnode_n += 1;

        let mut conn = match connect_tcp(&config, bootnode_addr).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "[assethub] connect to {} failed: {e}; trying next",
                    bootnode_addr.0
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        eprintln!("[assethub] connected to {}", bootnode_addr.0);

        conn.subscribe(block_announce_id)?;

        let outcome: anyhow::Result<u64> = async {
            while let Some(result) = conn.next().await {
                match result? {
                    Message::Notification {
                        protocol_id,
                        res: SubscriptionResponse::Opened,
                    } if protocol_id == block_announce_id => {
                        eprintln!(
                            "[assethub] requesting Timestamp.Now at #{} via /state/2",
                            head_number
                        );
                        let req = encode_state_request(head_hash, key.as_slice());
                        conn.request(state_id, req)?;
                    }
                    Message::Notification {
                        protocol_id,
                        res: SubscriptionResponse::Closed,
                    } if protocol_id == block_announce_id => {
                        anyhow::bail!("assethub block-announce subscription closed");
                    }
                    Message::Notification {
                        protocol_id,
                        res: SubscriptionResponse::Error(e),
                    } if protocol_id == block_announce_id => {
                        anyhow::bail!("assethub block-announce subscription error: {e}");
                    }
                    Message::Response {
                        protocol_id,
                        res: RequestResponse::Value(bytes),
                        ..
                    } if protocol_id == state_id => {
                        let value = verify_state_proof(&bytes, state_root, &key)?;
                        let now_ms = u64::decode(&mut &value[..])
                            .context("decoding Timestamp.Now as u64")?;
                        return Ok(now_ms);
                    }
                    Message::Response {
                        protocol_id,
                        res: RequestResponse::Error(e),
                        ..
                    } if protocol_id == state_id => {
                        anyhow::bail!("assethub /state/2 error: {e}");
                    }
                    _ => {}
                }
            }
            anyhow::bail!("assethub connection closed before response");
        }
        .await;

        match outcome {
            Ok(v) => return Ok(v),
            Err(e) => {
                eprintln!(
                    "[assethub] state read failed on {}: {e}; trying another peer in 10s",
                    bootnode_addr.0
                );
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

async fn connect_tcp(
    config: &Configuration<TokioPlatform>,
    addr: (&str, u16),
) -> anyhow::Result<TokioConnection> {
    let tcp = TcpStream::connect(addr).await?;
    let (read_half, write_half) = tcp.into_split();
    let conn = config
        .connect(TokioTcpReader(read_half), TokioTcpWriter(write_half))
        .await?;
    Ok(conn)
}

/// Take a raw `StateResponse` payload, expand the compact proof against
/// `expected_state_root`, run `read_proof_check` for `key`, and return the
/// raw storage value (errors if absent).
fn verify_state_proof(
    response_bytes: &[u8],
    expected_state_root: &[u8; 32],
    key: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let proof_bytes = extract_state_response_proof(response_bytes)
        .context("decoding /state/2 protobuf response")?;
    let encoded_nodes = <Vec<Vec<u8>>>::decode(&mut &proof_bytes[..])
        .context("SCALE-decoding compact proof as Vec<Vec<u8>>")?;
    let compact = CompactProof { encoded_nodes };
    let expected_root = sp_core::H256(*expected_state_root);
    let (storage_proof, _) = compact
        .to_storage_proof::<Blake2Hasher>(Some(&expected_root))
        .map_err(|e| anyhow::anyhow!("failed to expand compact proof: {e:?}"))?;
    let verified = read_proof_check::<Blake2Hasher, _>(expected_root, storage_proof, [key])
        .map_err(|e| anyhow::anyhow!("storage proof verification failed: {e}"))?;
    verified
        .get(key)
        .and_then(|v| v.clone())
        .context("key was missing from the verified proof")
}

fn timestamp_now_key() -> Vec<u8> {
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(&twox_128(b"Timestamp"));
    key.extend_from_slice(&twox_128(b"Now"));
    key
}

/// Storage key for `Paras::Heads(para_id)` on the relay chain:
/// `twox_128("Paras") ++ twox_128("Heads") ++ twox_64_concat(para_id)`.
fn paras_heads_key(para_id: u32) -> Vec<u8> {
    let id_encoded = para_id.encode();
    let mut key = Vec::with_capacity(16 + 16 + 8 + 4);
    key.extend_from_slice(&twox_128(b"Paras"));
    key.extend_from_slice(&twox_128(b"Heads"));
    key.extend_from_slice(&twox_64(&id_encoded));
    key.extend_from_slice(&id_encoded);
    key
}

/// Encode a Substrate `StateRequest` protobuf:
///
/// ```proto
/// message StateRequest {
///     bytes block = 1;
///     repeated bytes start = 2;
///     bool no_proof = 3;
/// }
/// ```
fn encode_state_request(block_hash: &[u8; 32], start_key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + block_hash.len() + start_key.len());
    encode_len_delim(&mut out, 1, block_hash);
    encode_len_delim(&mut out, 2, start_key);
    out
}

fn encode_len_delim(out: &mut Vec<u8>, field_id: u32, data: &[u8]) {
    encode_varint(out, ((field_id as u64) << 3) | 2);
    encode_varint(out, data.len() as u64);
    out.extend_from_slice(data);
}

fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Walk a `StateResponse` protobuf and return the bytes of field 2 (`bytes proof`).
fn extract_state_response_proof(mut input: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut proof: Option<Vec<u8>> = None;
    while !input.is_empty() {
        let tag = decode_varint(&mut input)?;
        let field_id = tag >> 3;
        let wire_type = tag & 0x07;
        match (field_id, wire_type) {
            (2, 2) => {
                let len = decode_varint(&mut input)? as usize;
                let bytes = take_bytes(&mut input, len)?;
                proof = Some(bytes.to_vec());
            }
            (_, 2) => {
                let len = decode_varint(&mut input)? as usize;
                take_bytes(&mut input, len)?;
            }
            (_, 0) => {
                decode_varint(&mut input)?;
            }
            (_, w) => {
                anyhow::bail!("unexpected protobuf wire type {w} for field {field_id}");
            }
        }
    }
    proof.context("StateResponse contained no `proof` field")
}

fn decode_varint(input: &mut &[u8]) -> anyhow::Result<u64> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = *input.first().context("unexpected end of input in varint")?;
        *input = &input[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            anyhow::bail!("varint exceeds 64 bits");
        }
    }
}

fn take_bytes<'a>(input: &mut &'a [u8], n: usize) -> anyhow::Result<&'a [u8]> {
    if input.len() < n {
        anyhow::bail!("unexpected end of input: need {n} bytes, have {}", input.len());
    }
    let (head, rest) = input.split_at(n);
    *input = rest;
    Ok(head)
}

