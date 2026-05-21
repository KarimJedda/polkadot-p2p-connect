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
use sp_core::hashing::twox_128;
use sp_state_machine::read_proof_check;
use sp_trie::CompactProof;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use warp_sync::{BlockHeader, checkpoint};

use polkadot::{ASSETHUB_GENESIS_HASH, ASSETHUB_PARA_ID, GENESIS_HASH};

/// Bundled Polkadot relay-chain checkpoint (the `lightSyncState` block
/// from a polkadot raw chain-spec). The example warp-syncs the relay
/// chain from here before reading AssetHub state on top.
const LIGHT_SYNC_STATE_JSON: &str = include_str!("../../../warp-sync/polkadot-lightsync.json");

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

#[derive(Decode, Debug)]
struct AccountInfo {
    nonce: u32,
    consumers: u32,
    providers: u32,
    sufficients: u32,
    data: AccountData,
}

#[derive(Decode, Debug)]
struct AccountData {
    free: u128,
    reserved: u128,
    frozen: u128,
    #[allow(dead_code)]
    flags: u128,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let address_arg = std::env::args().nth(1).ok_or_else(|| {
        anyhow::anyhow!(
            "usage: assethub-balance <SS58_address_or_0x_hex>\n\
             example: assethub-balance 13UVJyLnbVp9RBZYFwFGyDvVd1y27Tt8tkntv6Q7JVPhFsTB"
        )
    })?;
    let account_id = parse_account_id(&address_arg)?;
    eprintln!("[address] querying account 0x{}", hex::encode(account_id));

    // --- Phase 1: warp sync the relay chain and learn AssetHub's head ---
    let (assethub_head_hash, assethub_head_number, assethub_state_root) =
        learn_assethub_head().await?;
    eprintln!(
        "[parachain] AssetHub head from relay: #{} hash=0x{} state_root=0x{}",
        assethub_head_number,
        hex::encode(assethub_head_hash),
        hex::encode(assethub_state_root),
    );

    // --- Phase 2: connect to AssetHub, request System::Account[id] ---
    let info = fetch_assethub_account_info(
        &account_id,
        &assethub_head_hash,
        &assethub_state_root,
        assethub_head_number,
    )
    .await?;

    println!();
    println!(
        "Polkadot AssetHub account 0x{} at finalized-on-relay block #{}",
        hex::encode(account_id),
        assethub_head_number
    );
    println!("  nonce       = {}", info.nonce);
    println!("  consumers   = {}", info.consumers);
    println!("  providers   = {}", info.providers);
    println!("  sufficients = {}", info.sufficients);
    println!("  free        = {} ({} plancks)", format_dot(info.data.free), info.data.free);
    println!("  reserved    = {} ({} plancks)", format_dot(info.data.reserved), info.data.reserved);
    println!("  frozen      = {} ({} plancks)", format_dot(info.data.frozen), info.data.frozen);

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

    let mut grandpa_state = checkpoint::load(LIGHT_SYNC_STATE_JSON)?;
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
                        let head_data: Vec<u8> = Decode::decode(&mut &value[..])
                            .context("decoding HeadData from Paras::Heads value")?;
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

async fn fetch_assethub_account_info(
    account_id: &[u8; 32],
    head_hash: &[u8; 32],
    state_root: &[u8; 32],
    head_number: u32,
) -> anyhow::Result<AccountInfo> {
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

    let key = system_account_key(account_id);
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

        let outcome: anyhow::Result<AccountInfo> = async {
            while let Some(result) = conn.next().await {
                match result? {
                    Message::Notification {
                        protocol_id,
                        res: SubscriptionResponse::Opened,
                    } if protocol_id == block_announce_id => {
                        eprintln!(
                            "[assethub] requesting System::Account at #{} via /state/2",
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
                        let mut cursor = &value[..];
                        let info = AccountInfo::decode(&mut cursor)
                            .context("decoding AccountInfo from System::Account value")?;
                        if !cursor.is_empty() {
                            anyhow::bail!(
                                "AccountInfo schema drift: {} leftover bytes after decode",
                                cursor.len()
                            );
                        }
                        return Ok(info);
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
        .context("key was missing from the verified proof (account may not exist)")
}

/// Parse a Polkadot/AssetHub address into a 32-byte AccountId32. Accepts either:
///  - SS58 string (e.g. starting with `1...` for Polkadot prefix 0)
///  - 0x-prefixed hex of the raw 32-byte public key
fn parse_account_id(input: &str) -> anyhow::Result<[u8; 32]> {
    if let Some(stripped) = input.strip_prefix("0x") {
        let bytes = hex::decode(stripped).context("invalid hex in 0x address")?;
        if bytes.len() != 32 {
            anyhow::bail!("hex account id must be 32 bytes, got {}", bytes.len());
        }
        return Ok(bytes.try_into().unwrap());
    }
    let decoded = bs58::decode(input)
        .into_vec()
        .context("invalid base58 in SS58 address")?;
    if decoded.len() != 35 {
        anyhow::bail!(
            "expected 35-byte SS58 (1-byte prefix + 32-byte pubkey + 2-byte checksum), got {}",
            decoded.len()
        );
    }
    let body = &decoded[..33];
    let checksum_received = &decoded[33..35];
    use blake2::{Blake2b512, Digest};
    let mut hasher = Blake2b512::new();
    hasher.update(b"SS58PRE");
    hasher.update(body);
    let hash = hasher.finalize();
    if checksum_received != &hash[..2] {
        anyhow::bail!("SS58 checksum mismatch");
    }
    let pubkey: [u8; 32] = decoded[1..33].try_into().unwrap();
    Ok(pubkey)
}

/// Storage key for `System::Account[account_id]`:
/// `twox_128("System") ++ twox_128("Account") ++ blake2_128_concat(account_id)`.
/// AccountId uses blake2_128_concat (not twox_64_concat) because account ids
/// are user-controlled and twox is collision-prone for adversarial inputs.
fn system_account_key(account_id: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + 16 + 16 + 32);
    key.extend_from_slice(&twox_128(b"System"));
    key.extend_from_slice(&twox_128(b"Account"));
    key.extend_from_slice(&blake2_128(account_id));
    key.extend_from_slice(account_id);
    key
}

fn blake2_128(data: &[u8]) -> [u8; 16] {
    use blake2::digest::consts::U16;
    use blake2::{Blake2b, Digest};
    let mut hasher = Blake2b::<U16>::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Storage key for `Paras::Heads(para_id)` on the relay chain:
/// `twox_128("Paras") ++ twox_128("Heads") ++ twox_64_concat(para_id)`.
fn paras_heads_key(para_id: u32) -> Vec<u8> {
    use sp_core::hashing::twox_64;
    let id_encoded = para_id.encode();
    let mut key = Vec::with_capacity(16 + 16 + 8 + 4);
    key.extend_from_slice(&twox_128(b"Paras"));
    key.extend_from_slice(&twox_128(b"Heads"));
    key.extend_from_slice(&twox_64(&id_encoded));
    key.extend_from_slice(&id_encoded);
    key
}

/// Format Plancks as DOT with 10 decimals.
fn format_dot(plancks: u128) -> String {
    let whole = plancks / 10_000_000_000;
    let frac = plancks % 10_000_000_000;
    format!("{whole}.{frac:010} DOT")
}

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
