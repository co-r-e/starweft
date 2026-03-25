//! Peer-to-peer transport layer for Starweft message delivery.
//!
//! Provides two transport backends: a file-based local mailbox for
//! single-machine setups and a libp2p TCP transport for networked
//! multi-node deployments with automatic seed peer reconnection.

use anyhow::{Result, anyhow, bail};
use libp2p::futures::StreamExt;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{Multiaddr, PeerId, StreamProtocol, Swarm};
use multiaddr::Protocol;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{Duration, Instant};
use zeroize::{Zeroize, Zeroizing};

const MAX_INBOX_CAPACITY: usize = 10_000;
const MAX_DISCOVERED_PEERS: usize = 1_000;
const STARWEFT_PROTOCOL_ID: &str = "/starweft/0.1";
const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
const MAX_ACK_SIZE: usize = 16 * 1024;

/// Circuit Relay v2 operating mode.
#[derive(Clone, Copy, Debug)]
pub enum RelayMode {
    /// No relay functionality.
    Disabled,
    /// Act as a relay server for other peers.
    Server {
        max_reservations: u32,
        max_circuits_per_peer: u32,
        reservation_duration: Duration,
    },
    /// Act as a relay client (request relay reservations when behind NAT).
    Client,
}

/// NAT reachability status as reported by AutoNAT.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NatStatus {
    /// Reachability has not yet been determined.
    Unknown,
    /// The node is directly reachable from the public internet.
    Public,
    /// The node is behind a NAT and requires relay or hole-punching.
    Private,
}

/// A peer discovered via mDNS on the local network.
#[derive(Clone, Debug)]
pub struct DiscoveredPeer {
    /// The peer's libp2p peer ID string.
    pub peer_id: String,
    /// Multiaddrs where the peer can be reached.
    pub addresses: Vec<Multiaddr>,
}

/// A remote peer's multiaddr endpoint used for dialing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerEndpoint {
    /// The multiaddr string (e.g. `/ip4/127.0.0.1/tcp/4001/p2p/<peer-id>`).
    pub address: String,
}

impl PeerEndpoint {
    /// Parses and validates the address as a multiaddr.
    pub fn validate(&self) -> Result<Multiaddr> {
        parse_starweft_multiaddr(&self.address)
    }
}

/// A validated listen address for the local transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListenAddress {
    /// The original address string.
    pub raw: String,
    /// The parsed multiaddr.
    pub multiaddr: Multiaddr,
}

impl ListenAddress {
    /// Parses a raw address string, validating transport support.
    pub fn parse(raw: impl Into<String>) -> Result<Self> {
        let raw = raw.into();
        let multiaddr = parse_starweft_multiaddr(&raw)?;
        validate_supported_transport(&multiaddr)?;
        Ok(Self { raw, multiaddr })
    }
}

fn parse_starweft_multiaddr(raw: &str) -> Result<Multiaddr> {
    if let Some(path) = raw.strip_prefix("/unix//") {
        return Ok(Multiaddr::empty().with(Protocol::Unix(format!("/{path}").into())));
    }
    raw.parse::<Multiaddr>().map_err(Into::into)
}

/// Network topology describing local listen addresses and remote seed peers.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RuntimeTopology {
    /// Addresses this node listens on.
    pub listen_addresses: Vec<ListenAddress>,
    /// Known seed peers to connect to on startup.
    pub seed_peers: Vec<PeerEndpoint>,
}

impl RuntimeTopology {
    /// Validates and constructs a topology from raw address strings.
    pub fn validate(
        listen_addresses: impl IntoIterator<Item = String>,
        seed_peers: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        let listen_addresses = listen_addresses
            .into_iter()
            .map(ListenAddress::parse)
            .collect::<Result<Vec<_>>>()?;
        if listen_addresses.is_empty() {
            bail!("at least one listen address is required");
        }

        let seed_peers = seed_peers
            .into_iter()
            .map(|address| {
                let endpoint = PeerEndpoint { address };
                endpoint.validate()?;
                Ok(endpoint)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            listen_addresses,
            seed_peers,
        })
    }

    /// Returns filesystem paths for any `/unix` mailbox listen addresses.
    pub fn local_mailbox_paths(&self) -> Vec<PathBuf> {
        self.listen_addresses
            .iter()
            .filter_map(|address| mailbox_path_from_multiaddr(&address.multiaddr))
            .collect()
    }
}

/// The kind of transport backend in use.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportKind {
    /// File-based local mailbox (uses `/unix` multiaddr paths).
    LocalMailbox,
    /// Networked libp2p TCP transport.
    Libp2p,
}

/// Report returned after successfully delivering a message.
#[derive(Clone, Debug)]
pub struct DeliveryReport {
    /// The target address or path the message was delivered to.
    pub target: String,
}

/// Trait for sending and receiving protocol messages over a transport.
pub trait TransportDriver {
    /// Returns the kind of transport this driver implements.
    fn kind(&self) -> TransportKind;
    /// Drains all pending incoming messages from the transport.
    fn receive(&self, topology: &RuntimeTopology) -> Result<Vec<String>>;
    /// Delivers a payload to the given multiaddr; returns `None` if the address is unsupported.
    fn deliver(&self, multiaddr: &Multiaddr, payload: &str) -> Result<Option<DeliveryReport>>;
}

/// A polymorphic transport that dispatches to either local mailbox or libp2p.
pub enum RuntimeTransport {
    /// File-based local mailbox transport.
    LocalMailbox(LocalMailboxTransport),
    /// Networked libp2p TCP transport.
    Libp2p(Libp2pTransport),
}

impl RuntimeTransport {
    /// Creates a local mailbox transport.
    pub fn local_mailbox() -> Self {
        Self::LocalMailbox(LocalMailboxTransport)
    }

    /// Creates a libp2p transport from the given topology and Ed25519 private key.
    pub fn libp2p(
        topology: &RuntimeTopology,
        private_key: [u8; 32],
        mdns_enabled: bool,
        nat_traversal_enabled: bool,
        relay_mode: RelayMode,
    ) -> Result<Self> {
        Ok(Self::Libp2p(Libp2pTransport::new(
            topology,
            private_key,
            mdns_enabled,
            nat_traversal_enabled,
            relay_mode,
        )?))
    }

    /// Returns the libp2p peer ID if using the libp2p transport.
    pub fn peer_id_hint(&self) -> Option<&str> {
        match self {
            Self::LocalMailbox(_) => None,
            Self::Libp2p(driver) => Some(driver.peer_id()),
        }
    }

    /// Drains peers discovered via mDNS since the last call.
    pub fn discovered_peers(&self) -> Vec<DiscoveredPeer> {
        match self {
            Self::LocalMailbox(_) => Vec::new(),
            Self::Libp2p(driver) => driver.discovered_peers(),
        }
    }

    /// Returns the announced listen addresses if using libp2p transport.
    pub fn listen_addr_hints(&self) -> Vec<String> {
        match self {
            Self::LocalMailbox(_) => Vec::new(),
            Self::Libp2p(driver) => driver
                .listen_addresses()
                .iter()
                .map(ToString::to_string)
                .collect(),
        }
    }

    /// Returns the current NAT reachability status.
    pub fn nat_status(&self) -> NatStatus {
        match self {
            Self::LocalMailbox(_) => NatStatus::Public,
            Self::Libp2p(driver) => driver.nat_status(),
        }
    }

    /// Returns externally observed addresses (from Identify, UPnP, or relay reservations).
    pub fn external_addresses(&self) -> Vec<Multiaddr> {
        match self {
            Self::LocalMailbox(_) => Vec::new(),
            Self::Libp2p(driver) => driver.external_addresses(),
        }
    }

    /// Returns the number of currently connected peers if the transport can report it.
    pub fn connected_peer_count(&self) -> Option<u64> {
        match self {
            Self::LocalMailbox(_) => None,
            Self::Libp2p(driver) => Some(driver.connected_peer_count()),
        }
    }
}

/// Derives the libp2p peer ID string from a 32-byte Ed25519 private key.
pub fn libp2p_peer_id_from_private_key(private_key: [u8; 32]) -> Result<String> {
    let mut private_key = private_key;
    let identity = libp2p::identity::Keypair::ed25519_from_bytes(private_key)
        .map_err(|error| anyhow!("failed to decode libp2p identity: {error}"))?;
    private_key.zeroize();
    Ok(identity.public().to_peer_id().to_string())
}

impl TransportDriver for RuntimeTransport {
    fn kind(&self) -> TransportKind {
        match self {
            Self::LocalMailbox(driver) => driver.kind(),
            Self::Libp2p(driver) => driver.kind(),
        }
    }

    fn receive(&self, topology: &RuntimeTopology) -> Result<Vec<String>> {
        match self {
            Self::LocalMailbox(driver) => driver.receive(topology),
            Self::Libp2p(driver) => driver.receive(topology),
        }
    }

    fn deliver(&self, multiaddr: &Multiaddr, payload: &str) -> Result<Option<DeliveryReport>> {
        match self {
            Self::LocalMailbox(driver) => driver.deliver(multiaddr, payload),
            Self::Libp2p(driver) => driver.deliver(multiaddr, payload),
        }
    }
}

fn validate_supported_transport(address: &Multiaddr) -> Result<()> {
    let protocols = address.iter().collect::<Vec<_>>();
    let has_ip = protocols
        .iter()
        .any(|protocol| matches!(protocol, Protocol::Ip4(_) | Protocol::Ip6(_)));
    let has_tcp = protocols
        .iter()
        .any(|protocol| matches!(protocol, Protocol::Tcp(_)));
    let has_unix = protocols
        .iter()
        .any(|protocol| matches!(protocol, Protocol::Unix(_)));

    if has_unix {
        return Ok(());
    }

    if !has_ip {
        return Err(anyhow!("listen address must include /ip4 or /ip6"));
    }

    if has_tcp {
        return Ok(());
    }

    Err(anyhow!("unsupported transport; expected /tcp"))
}

/// Extracts the filesystem path from a `/unix` multiaddr, if present.
pub fn mailbox_path_from_multiaddr(address: &Multiaddr) -> Option<PathBuf> {
    address.iter().find_map(|protocol| match protocol {
        Protocol::Unix(path) => Some(PathBuf::from(path.to_string())),
        _ => None,
    })
}

/// File-based transport that uses Unix domain socket paths as mailboxes.
#[derive(Clone, Debug, Default)]
pub struct LocalMailboxTransport;

impl TransportDriver for LocalMailboxTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::LocalMailbox
    }

    fn receive(&self, topology: &RuntimeTopology) -> Result<Vec<String>> {
        let mut messages = Vec::new();
        for mailbox in topology.local_mailbox_paths() {
            let tmp = mailbox.with_extension("recv");
            // Recover from a previous crash that left a temp file
            if tmp.exists() {
                read_mailbox_messages(&tmp, &mut messages)?;
                let _ = fs::remove_file(&tmp);
            }
            // Atomically claim the mailbox
            if fs::rename(&mailbox, &tmp).is_err() {
                continue;
            }
            read_mailbox_messages(&tmp, &mut messages)?;
            let _ = fs::remove_file(&tmp);
        }
        Ok(messages)
    }

    fn deliver(&self, multiaddr: &Multiaddr, payload: &str) -> Result<Option<DeliveryReport>> {
        let Some(mailbox_path) = mailbox_path_from_multiaddr(multiaddr) else {
            return Ok(None);
        };

        validate_message_size(payload.len(), "送信")?;

        if let Some(parent) = mailbox_path.parent() {
            fs::create_dir_all(parent)?;
        }

        use fs2::FileExt;
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&mailbox_path)?;
        file.lock_exclusive()?;
        writeln!(file, "{payload}")?;
        file.sync_data()?;
        fs2::FileExt::unlock(&file)?;
        Ok(Some(DeliveryReport {
            target: mailbox_path.display().to_string(),
        }))
    }
}

/// Acknowledgment sent back to the sender after receiving an envelope.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnvelopeAck {
    /// Whether the receiver accepted the message.
    pub accepted: bool,
}

/// Composed libp2p behaviour for Starweft request-response protocol.
#[derive(NetworkBehaviour)]
pub struct StarweftBehaviour {
    /// CBOR-encoded request-response sub-behaviour.
    pub request_response: request_response::cbor::Behaviour<String, EnvelopeAck>,
    /// Optional mDNS sub-behaviour for local network peer discovery.
    pub mdns: Toggle<libp2p::mdns::tokio::Behaviour>,
    /// Optional Identify protocol for exchanging peer metadata.
    pub identify: Toggle<libp2p::identify::Behaviour>,
    /// Optional AutoNAT behaviour for determining NAT reachability.
    pub autonat: Toggle<libp2p::autonat::Behaviour>,
    /// Optional UPnP behaviour for automatic port mapping.
    pub upnp: Toggle<libp2p::upnp::tokio::Behaviour>,
    /// Optional Circuit Relay v2 server behaviour.
    pub relay_server: Toggle<libp2p::relay::Behaviour>,
    /// Optional Circuit Relay v2 client behaviour.
    pub relay_client: Toggle<libp2p::relay::client::Behaviour>,
    /// Optional DCUtR (Direct Connection Upgrade through Relay) behaviour.
    pub dcutr: Toggle<libp2p::dcutr::Behaviour>,
}

enum Libp2pCommand {
    DrainInbox {
        reply_tx: Sender<Result<Vec<String>>>,
    },
    Deliver {
        multiaddr: Multiaddr,
        payload: String,
        reply_tx: Sender<Result<Option<DeliveryReport>>>,
    },
    DrainDiscoveredPeers {
        reply_tx: Sender<Vec<DiscoveredPeer>>,
    },
    QueryNatStatus {
        reply_tx: Sender<NatStatus>,
    },
    QueryExternalAddresses {
        reply_tx: Sender<Vec<Multiaddr>>,
    },
    QueryConnectedPeerCount {
        reply_tx: Sender<u64>,
    },
    Shutdown,
}

struct Libp2pInit {
    peer_id: String,
    listen_addresses: Vec<Multiaddr>,
}

/// Libp2p TCP transport backed by a dedicated worker thread running a Tokio runtime.
pub struct Libp2pTransport {
    command_tx: tokio::sync::mpsc::UnboundedSender<Libp2pCommand>,
    peer_id: String,
    listen_addresses: Vec<Multiaddr>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Libp2pTransport {
    /// Creates a new libp2p transport, spawning a background worker thread.
    pub fn new(
        topology: &RuntimeTopology,
        private_key: [u8; 32],
        mdns_enabled: bool,
        nat_traversal_enabled: bool,
        relay_mode: RelayMode,
    ) -> Result<Self> {
        let private_key = Zeroizing::new(private_key);
        let listen_addresses = topology
            .listen_addresses
            .iter()
            .map(|address| address.multiaddr.clone())
            .collect::<Vec<_>>();
        let seed_peers = topology.seed_peers.clone();
        if listen_addresses.is_empty() {
            bail!("libp2p transport requires at least one listen address");
        }
        if listen_addresses
            .iter()
            .any(|address| mailbox_path_from_multiaddr(address).is_some())
        {
            bail!("libp2p transport does not support /unix mailbox addresses");
        }
        if listen_addresses.iter().any(|address| {
            !address
                .iter()
                .any(|protocol| matches!(protocol, Protocol::Tcp(_)))
        }) {
            bail!("libp2p transport currently supports only /tcp listen addresses");
        }
        validate_libp2p_seed_peers(&seed_peers)?;

        let expected_listens = listen_addresses.len();
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        let (init_tx, init_rx) = mpsc::channel();
        let init_tx_failure = init_tx.clone();
        let worker = thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = init_tx.send(Err(anyhow!(error)));
                    return;
                }
            };

            let result = runtime.block_on(async move {
                let is_relay_client = matches!(relay_mode, RelayMode::Client);
                let (mut swarm, announced) = build_swarm(SwarmConfig {
                    listen_addresses,
                    expected_listens,
                    private_key,
                    mdns_enabled,
                    nat_traversal_enabled,
                    relay_mode,
                })
                .await?;
                let peer_id = swarm.local_peer_id().to_string();
                init_tx
                    .send(Ok(Libp2pInit {
                        peer_id,
                        listen_addresses: announced,
                    }))
                    .map_err(|_| anyhow!("init channel closed"))?;
                libp2p_event_loop(&mut swarm, command_rx, seed_peers, is_relay_client).await
            });

            if let Err(error) = result {
                let _ = init_tx_failure.send(Err(anyhow!("{error:#}")));
                tracing::error!("libp2p worker stopped: {error:#}");
            }
        });

        let init = init_rx.recv()??;
        Ok(Self {
            command_tx,
            peer_id: init.peer_id,
            listen_addresses: init.listen_addresses,
            worker: Some(worker),
        })
    }

    /// Returns the local peer ID.
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// Returns the announced listen addresses.
    pub fn listen_addresses(&self) -> &[Multiaddr] {
        &self.listen_addresses
    }

    /// Drains peers discovered via mDNS since the last call.
    pub fn discovered_peers(&self) -> Vec<DiscoveredPeer> {
        let (reply_tx, reply_rx) = mpsc::channel();
        if self
            .command_tx
            .send(Libp2pCommand::DrainDiscoveredPeers { reply_tx })
            .is_err()
        {
            return Vec::new();
        }
        reply_rx.recv().unwrap_or_default()
    }

    /// Returns the current NAT reachability status.
    pub fn nat_status(&self) -> NatStatus {
        let (reply_tx, reply_rx) = mpsc::channel();
        if self
            .command_tx
            .send(Libp2pCommand::QueryNatStatus { reply_tx })
            .is_err()
        {
            return NatStatus::Unknown;
        }
        reply_rx.recv().unwrap_or(NatStatus::Unknown)
    }

    /// Returns externally observed addresses.
    pub fn external_addresses(&self) -> Vec<Multiaddr> {
        let (reply_tx, reply_rx) = mpsc::channel();
        if self
            .command_tx
            .send(Libp2pCommand::QueryExternalAddresses { reply_tx })
            .is_err()
        {
            return Vec::new();
        }
        reply_rx.recv().unwrap_or_default()
    }

    /// Returns the number of currently connected peers.
    pub fn connected_peer_count(&self) -> u64 {
        let (reply_tx, reply_rx) = mpsc::channel();
        if self
            .command_tx
            .send(Libp2pCommand::QueryConnectedPeerCount { reply_tx })
            .is_err()
        {
            return 0;
        }
        reply_rx.recv().unwrap_or(0)
    }
}

impl std::fmt::Debug for Libp2pTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Libp2pTransport")
            .field("peer_id", &self.peer_id)
            .finish()
    }
}

impl TransportDriver for Libp2pTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::Libp2p
    }

    fn receive(&self, _topology: &RuntimeTopology) -> Result<Vec<String>> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.command_tx
            .send(Libp2pCommand::DrainInbox { reply_tx })
            .map_err(|_| anyhow!("libp2p worker is not running"))?;
        reply_rx.recv()?
    }

    fn deliver(&self, multiaddr: &Multiaddr, payload: &str) -> Result<Option<DeliveryReport>> {
        validate_message_size(payload.len(), "送信")?;
        let (reply_tx, reply_rx) = mpsc::channel();
        self.command_tx
            .send(Libp2pCommand::Deliver {
                multiaddr: multiaddr.clone(),
                payload: payload.to_owned(),
                reply_tx,
            })
            .map_err(|_| anyhow!("libp2p worker is not running"))?;
        reply_rx.recv()?
    }
}

fn validate_message_size(size: usize, direction: &str) -> Result<()> {
    if size > MAX_MESSAGE_SIZE {
        bail!(
            "[E_MESSAGE_TOO_LARGE] {direction}メッセージがサイズ制限を超過: size={size} limit={MAX_MESSAGE_SIZE}"
        );
    }
    Ok(())
}

fn read_mailbox_messages(path: &Path, messages: &mut Vec<String>) -> Result<()> {
    read_mailbox_messages_with_limit(path, messages, MAX_MESSAGE_SIZE)
}

fn read_mailbox_messages_with_limit(
    path: &Path,
    messages: &mut Vec<String>,
    max_message_size: usize,
) -> Result<()> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut line_size = 0usize;
    let mut over_limit = false;

    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            if line_size > 0 {
                finish_mailbox_line(&mut line, line_size, over_limit, max_message_size, messages)?;
            }
            return Ok(());
        }

        let newline_pos = buffer.iter().position(|byte| *byte == b'\n');
        let consumed = newline_pos.map_or(buffer.len(), |pos| pos + 1);
        let chunk = &buffer[..consumed];
        let ends_with_newline = newline_pos.is_some();
        let line_chunk = if ends_with_newline {
            &chunk[..chunk.len().saturating_sub(1)]
        } else {
            chunk
        };

        line_size += line_chunk.len();
        if !over_limit {
            let remaining = max_message_size.saturating_sub(line.len());
            let keep = remaining.min(line_chunk.len());
            line.extend_from_slice(&line_chunk[..keep]);
            if line_size > max_message_size {
                over_limit = true;
            }
        }

        reader.consume(consumed);

        if ends_with_newline {
            finish_mailbox_line(&mut line, line_size, over_limit, max_message_size, messages)?;
            line_size = 0;
            over_limit = false;
        }
    }
}

fn finish_mailbox_line(
    line: &mut Vec<u8>,
    line_size: usize,
    over_limit: bool,
    max_message_size: usize,
    messages: &mut Vec<String>,
) -> Result<()> {
    if over_limit {
        tracing::warn!(
            "受信メッセージがサイズ制限を超過: size={line_size} limit={max_message_size}"
        );
        line.clear();
        return Ok(());
    }

    let bytes = std::mem::take(line);
    let mut value = String::from_utf8(bytes).map_err(|error| {
        anyhow!(
            "[E_MESSAGE_ENCODING] ローカルメールボックスの payload が UTF-8 ではありません: {error}"
        )
    })?;
    if value.ends_with('\r') {
        value.pop();
    }
    if value.trim().is_empty() {
        return Ok(());
    }
    messages.push(value);
    Ok(())
}

impl Drop for Libp2pTransport {
    fn drop(&mut self) {
        let _ = self.command_tx.send(Libp2pCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn build_behaviour(
    key: &libp2p::identity::Keypair,
    mdns_enabled: bool,
    nat_traversal_enabled: bool,
    relay_mode: &RelayMode,
    relay_client: Option<libp2p::relay::client::Behaviour>,
) -> std::result::Result<StarweftBehaviour, Box<dyn std::error::Error + Send + Sync>> {
    let protocols = std::iter::once((
        StreamProtocol::new(STARWEFT_PROTOCOL_ID),
        ProtocolSupport::Full,
    ));
    let config = request_response::Config::default();
    let codec = request_response::cbor::codec::Codec::<String, EnvelopeAck>::default()
        .set_request_size_maximum(MAX_MESSAGE_SIZE as u64)
        .set_response_size_maximum(MAX_ACK_SIZE as u64);
    let mdns = if mdns_enabled {
        Some(libp2p::mdns::tokio::Behaviour::new(
            libp2p::mdns::Config::default(),
            key.public().to_peer_id(),
        )?)
    } else {
        None
    };
    let identify = if nat_traversal_enabled {
        Some(libp2p::identify::Behaviour::new(
            libp2p::identify::Config::new(STARWEFT_PROTOCOL_ID.to_owned(), key.public()),
        ))
    } else {
        None
    };
    let autonat = if nat_traversal_enabled {
        Some(libp2p::autonat::Behaviour::new(
            key.public().to_peer_id(),
            libp2p::autonat::Config::default(),
        ))
    } else {
        None
    };
    let upnp = if nat_traversal_enabled {
        Some(libp2p::upnp::tokio::Behaviour::default())
    } else {
        None
    };
    let relay_server_behaviour = match relay_mode {
        RelayMode::Server {
            max_reservations,
            max_circuits_per_peer,
            reservation_duration,
        } => {
            let relay_config = libp2p::relay::Config {
                max_reservations: *max_reservations as usize,
                max_circuits_per_peer: *max_circuits_per_peer as usize,
                reservation_duration: *reservation_duration,
                ..Default::default()
            };
            Some(libp2p::relay::Behaviour::new(
                key.public().to_peer_id(),
                relay_config,
            ))
        }
        _ => None,
    };
    let dcutr = if relay_client.is_some() {
        Some(libp2p::dcutr::Behaviour::new(key.public().to_peer_id()))
    } else {
        None
    };
    Ok(StarweftBehaviour {
        request_response: request_response::cbor::Behaviour::with_codec(codec, protocols, config),
        mdns: mdns.into(),
        identify: identify.into(),
        autonat: autonat.into(),
        upnp: upnp.into(),
        relay_server: relay_server_behaviour.into(),
        relay_client: relay_client.into(),
        dcutr: dcutr.into(),
    })
}

struct SwarmConfig {
    listen_addresses: Vec<Multiaddr>,
    expected_listens: usize,
    private_key: Zeroizing<[u8; 32]>,
    mdns_enabled: bool,
    nat_traversal_enabled: bool,
    relay_mode: RelayMode,
}

async fn build_swarm(
    mut config: SwarmConfig,
) -> Result<(Swarm<StarweftBehaviour>, Vec<Multiaddr>)> {
    let private_key = std::mem::take(&mut config.private_key);
    let mut key_bytes = [0_u8; 32];
    key_bytes.copy_from_slice(&private_key[..]);
    drop(private_key);
    let identity = libp2p::identity::Keypair::ed25519_from_bytes(key_bytes)
        .map_err(|error| anyhow!("failed to decode libp2p identity: {error}"))?;
    key_bytes.zeroize();

    let use_relay_client = matches!(config.relay_mode, RelayMode::Client);

    // Two branches are required because `with_relay_client()` changes the SwarmBuilder type,
    // making the `with_behaviour` closure accept a different number of arguments.
    // This is a libp2p builder type-system constraint and cannot be unified.
    let mdns_enabled = config.mdns_enabled;
    let nat_traversal_enabled = config.nat_traversal_enabled;
    let relay_mode = config.relay_mode;

    let mut swarm = if use_relay_client {
        libp2p::SwarmBuilder::with_existing_identity(identity)
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default().nodelay(true),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )?
            .with_dns()?
            .with_relay_client(libp2p::noise::Config::new, libp2p::yamux::Config::default)?
            .with_behaviour(|key, relay_client| {
                build_behaviour(
                    key,
                    mdns_enabled,
                    nat_traversal_enabled,
                    &relay_mode,
                    Some(relay_client),
                )
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(300)))
            .build()
    } else {
        libp2p::SwarmBuilder::with_existing_identity(identity)
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default().nodelay(true),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )?
            .with_dns()?
            .with_behaviour(|key| {
                build_behaviour(key, mdns_enabled, nat_traversal_enabled, &relay_mode, None)
            })?
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(300)))
            .build()
    };

    for address in config.listen_addresses {
        swarm.listen_on(address)?;
    }

    let mut announced = Vec::new();
    while announced.len() < config.expected_listens {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => announced.push(address),
            other => tracing::debug!("libp2p init swarm event: {other:?}"),
        }
    }

    Ok((swarm, announced))
}

async fn libp2p_event_loop(
    swarm: &mut Swarm<StarweftBehaviour>,
    mut command_rx: tokio::sync::mpsc::UnboundedReceiver<Libp2pCommand>,
    seed_peers: Vec<PeerEndpoint>,
    use_relay_client: bool,
) -> Result<()> {
    let mut inbox = Vec::new();
    let mut pending = HashMap::new();
    let mut discovered_peers: Vec<DiscoveredPeer> = Vec::new();
    let mut mdns_seen_peers = HashSet::new();
    let seed_targets = parse_seed_targets(&seed_peers);
    let mut connected_seed_peers = HashSet::new();
    let mut pending_seed_dials = HashSet::new();
    let mut redial_interval = tokio::time::interval(Duration::from_secs(5));
    let mut pending_cleanup_interval = tokio::time::interval(Duration::from_secs(30));
    let mut nat_status = NatStatus::Unknown;
    let mut relay_reservation_attempted = HashSet::new();

    dial_seed_peers(
        swarm,
        &seed_targets,
        &connected_seed_peers,
        &mut pending_seed_dials,
    );

    loop {
        tokio::select! {
            Some(command) = command_rx.recv() => {
                match command {
                    Libp2pCommand::DrainInbox { reply_tx } => {
                        let drained = std::mem::take(&mut inbox);
                        let _ = reply_tx.send(Ok(drained));
                    }
                    Libp2pCommand::Deliver { multiaddr, payload, reply_tx } => {
                        let peer_id = match extract_peer_id(&multiaddr) {
                            Ok(id) => id,
                            Err(e) => { let _ = reply_tx.send(Err(e)); continue; }
                        };
                        swarm.add_peer_address(peer_id, multiaddr.clone());
                        let request_id = swarm.behaviour_mut().request_response.send_request(&peer_id, payload);
                        pending.insert(request_id, (Instant::now(), reply_tx, multiaddr));
                    }
                    Libp2pCommand::DrainDiscoveredPeers { reply_tx } => {
                        let drained = std::mem::take(&mut discovered_peers);
                        let _ = reply_tx.send(drained);
                    }
                    Libp2pCommand::QueryNatStatus { reply_tx } => {
                        let _ = reply_tx.send(nat_status);
                    }
                    Libp2pCommand::QueryExternalAddresses { reply_tx } => {
                        let addrs = swarm.external_addresses().cloned().collect();
                        let _ = reply_tx.send(addrs);
                    }
                    Libp2pCommand::QueryConnectedPeerCount { reply_tx } => {
                        let _ = reply_tx.send(swarm.connected_peers().count() as u64);
                    }
                    Libp2pCommand::Shutdown => break,
                }
            }
            _ = redial_interval.tick() => {
                dial_seed_peers(swarm, &seed_targets, &connected_seed_peers, &mut pending_seed_dials);
            }
            _ = pending_cleanup_interval.tick() => {
                let now = Instant::now();
                let expired: Vec<_> = pending.iter()
                    .filter(|(_, (created, _, _))| now.duration_since(*created) >= Duration::from_secs(30))
                    .map(|(k, _)| *k)
                    .collect();
                for key in expired {
                    if let Some((_, reply_tx, _)) = pending.remove(&key) {
                        let _ = reply_tx.send(Err(anyhow!("request timed out")));
                    }
                }
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        tracing::debug!("libp2p listen address ready: {address}");
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        if seed_targets.contains_key(&peer_id) {
                            pending_seed_dials.remove(&peer_id);
                            connected_seed_peers.insert(peer_id);
                        }
                    }
                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        num_established,
                        ..
                    } => {
                        if num_established == 0 && seed_targets.contains_key(&peer_id) {
                            connected_seed_peers.remove(&peer_id);
                            pending_seed_dials.remove(&peer_id);
                        }
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        if let Some(peer_id) = peer_id {
                            pending_seed_dials.remove(&peer_id);
                            if seed_targets.contains_key(&peer_id) {
                                tracing::debug!("libp2p seed dial failed for {peer_id}: {error}");
                            }
                        }
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::RequestResponse(
                        request_response::Event::Message { message, .. }
                    )) => {
                        match message {
                            request_response::Message::Request { request, channel, .. } => {
                                let message_size = request.len();
                                let accepted = if message_size > MAX_MESSAGE_SIZE {
                                    tracing::warn!(
                                        "受信メッセージがサイズ制限を超過: size={message_size} limit={MAX_MESSAGE_SIZE}"
                                    );
                                    false
                                } else if inbox.len() >= MAX_INBOX_CAPACITY {
                                    tracing::warn!(
                                        "inbox full ({MAX_INBOX_CAPACITY}), rejecting incoming request"
                                    );
                                    false
                                } else {
                                    inbox.push(request);
                                    true
                                };
                                let _ = swarm.behaviour_mut().request_response.send_response(channel, EnvelopeAck { accepted });
                            }
                            request_response::Message::Response { request_id, response } => {
                                if let Some((_, reply_tx, multiaddr)) = pending.remove(&request_id) {
                                    let report = DeliveryReport {
                                        target: multiaddr.to_string(),
                                    };
                                    let result = if response.accepted {
                                        Ok(Some(report))
                                    } else {
                                        Err(anyhow!(
                                            "[E_MESSAGE_REJECTED] リモートピアがメッセージを拒否しました。サイズ制限超過または受信キュー満杯の可能性があります"
                                        ))
                                    };
                                    let _ = reply_tx.send(result);
                                }
                            }
                        }
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::RequestResponse(
                        request_response::Event::OutboundFailure { request_id, error, .. }
                    )) => {
                        if let Some((_, reply_tx, _)) = pending.remove(&request_id) {
                            let _ = reply_tx.send(Err(anyhow!("outbound failure: {error}")));
                        }
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::RequestResponse(
                        request_response::Event::InboundFailure { error, .. }
                    )) => {
                        tracing::warn!("libp2p inbound failure: {error}");
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::Mdns(
                        libp2p::mdns::Event::Discovered(peers)
                    )) => {
                        let mut batch = HashSet::new();
                        for (peer_id, address) in &peers {
                            swarm.add_peer_address(*peer_id, address.clone());
                            batch.insert(*peer_id);
                        }
                        for peer_id in batch {
                            let addresses: Vec<Multiaddr> = peers
                                .iter()
                                .filter(|(pid, _)| *pid == peer_id)
                                .map(|(_, addr)| addr.clone())
                                .collect();
                            tracing::info!("mDNS discovered peer {peer_id} at {addresses:?}");
                            queue_discovered_peer(
                                &mut discovered_peers,
                                &mut mdns_seen_peers,
                                peer_id,
                                addresses,
                            );
                        }
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::Mdns(
                        libp2p::mdns::Event::Expired(peers)
                    )) => {
                        for (peer_id, address) in &peers {
                            tracing::debug!("mDNS expired peer {peer_id} at {address}");
                            mdns_seen_peers.remove(peer_id);
                        }
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::Identify(
                        libp2p::identify::Event::Received { peer_id, info, .. }
                    )) => {
                        // Don't blindly trust observed_addr from peers — Starweft nodes
                        // configure listen addresses explicitly via config.toml.
                        tracing::debug!(
                            "identify received from {peer_id}: {} observed at {}",
                            info.protocol_version,
                            info.observed_addr
                        );
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::Identify(_)) => {}
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::Autonat(event)) => {
                        use libp2p::autonat;
                        match event {
                            autonat::Event::StatusChanged { old, new } => {
                                nat_status = match new {
                                    autonat::NatStatus::Public(_) => NatStatus::Public,
                                    autonat::NatStatus::Private => NatStatus::Private,
                                    autonat::NatStatus::Unknown => NatStatus::Unknown,
                                };
                                tracing::info!("AutoNAT status changed: {old:?} → {new:?}");
                                // When behind NAT with relay client, listen on relay circuits
                                if nat_status == NatStatus::Private && use_relay_client {
                                    for (peer_id, addr) in &seed_targets {
                                        if relay_reservation_attempted.contains(peer_id) {
                                            continue;
                                        }
                                        relay_reservation_attempted.insert(*peer_id);
                                        let relay_addr = addr.clone()
                                            .with(Protocol::P2pCircuit);
                                        if let Err(error) = swarm.listen_on(relay_addr.clone()) {
                                            tracing::debug!("relay listen on {relay_addr} failed: {error}");
                                        } else {
                                            tracing::info!("requesting relay reservation via {peer_id}");
                                        }
                                    }
                                }
                            }
                            _ => {
                                tracing::debug!("autonat event: {event:?}");
                            }
                        }
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::Upnp(event)) => {
                        use libp2p::upnp;
                        match event {
                            upnp::Event::NewExternalAddr(addr) => {
                                tracing::info!("UPnP mapped external address: {addr}");
                                swarm.add_external_address(addr);
                            }
                            upnp::Event::GatewayNotFound => {
                                tracing::debug!("UPnP gateway not found");
                            }
                            upnp::Event::NonRoutableGateway => {
                                tracing::debug!("UPnP gateway is non-routable");
                            }
                            upnp::Event::ExpiredExternalAddr(addr) => {
                                tracing::debug!("UPnP mapping expired: {addr}");
                            }
                        }
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::RelayServer(event)) => {
                        tracing::debug!("relay server event: {event:?}");
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::RelayClient(
                        libp2p::relay::client::Event::ReservationReqAccepted { relay_peer_id, .. }
                    )) => {
                        tracing::info!("relay reservation accepted by {relay_peer_id}");
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::RelayClient(event)) => {
                        tracing::debug!("relay client event: {event:?}");
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::Dcutr(
                        libp2p::dcutr::Event { remote_peer_id, result }
                    )) => {
                        match result {
                            Ok(_) => tracing::info!("DCUtR direct connection upgraded with {remote_peer_id}"),
                            Err(error) => tracing::debug!("DCUtR upgrade failed with {remote_peer_id}: {error}"),
                        }
                    }
                    other => {
                        tracing::debug!("libp2p swarm event: {other:?}");
                    }
                }
            }
        }
    }

    Ok(())
}

fn extract_peer_id(address: &Multiaddr) -> Result<PeerId> {
    address
        .iter()
        .find_map(|protocol| match protocol {
            Protocol::P2p(peer_id) => Some(peer_id),
            _ => None,
        })
        .ok_or_else(|| anyhow!("target multiaddr must include /p2p/<peer-id>"))
}

fn validate_libp2p_seed_peers(seed_peers: &[PeerEndpoint]) -> Result<()> {
    for endpoint in seed_peers {
        let multiaddr = endpoint.validate()?;
        extract_peer_id(&multiaddr).map_err(|error| {
            anyhow!(
                "libp2p seed peer must include /p2p/<peer-id>: {} ({error})",
                endpoint.address
            )
        })?;
    }
    Ok(())
}

fn parse_seed_targets(seed_peers: &[PeerEndpoint]) -> HashMap<PeerId, Multiaddr> {
    let mut targets = HashMap::new();
    for endpoint in seed_peers {
        match endpoint
            .validate()
            .and_then(|multiaddr| extract_peer_id(&multiaddr).map(|peer_id| (peer_id, multiaddr)))
        {
            Ok((peer_id, multiaddr)) => {
                targets.insert(peer_id, multiaddr);
            }
            Err(error) => {
                tracing::warn!("ignoring invalid seed peer {}: {error}", endpoint.address);
            }
        }
    }
    targets
}

fn dial_seed_peers(
    swarm: &mut Swarm<StarweftBehaviour>,
    seed_targets: &HashMap<PeerId, Multiaddr>,
    connected_seed_peers: &HashSet<PeerId>,
    pending_seed_dials: &mut HashSet<PeerId>,
) {
    for (peer_id, address) in seed_targets {
        if connected_seed_peers.contains(peer_id) || pending_seed_dials.contains(peer_id) {
            continue;
        }
        if let Err(error) = swarm.dial(address.clone()) {
            tracing::debug!("libp2p seed dial skipped for {peer_id}: {error}");
            continue;
        }
        pending_seed_dials.insert(*peer_id);
    }
}

fn queue_discovered_peer(
    discovered_peers: &mut Vec<DiscoveredPeer>,
    mdns_seen_peers: &mut HashSet<PeerId>,
    peer_id: PeerId,
    addresses: Vec<Multiaddr>,
) -> bool {
    if mdns_seen_peers.contains(&peer_id) {
        return false;
    }
    if discovered_peers.len() >= MAX_DISCOVERED_PEERS {
        tracing::warn!(
            "mDNS discovered peer queue full ({MAX_DISCOVERED_PEERS}), deferring {peer_id}"
        );
        return false;
    }
    mdns_seen_peers.insert(peer_id);
    discovered_peers.push(DiscoveredPeer {
        peer_id: peer_id.to_string(),
        addresses,
    });
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loopback_tcp_available() -> bool {
        std::net::TcpListener::bind(("127.0.0.1", 0)).is_ok()
    }

    #[test]
    fn can_create_libp2p_transport_for_tcp_listen() {
        if !loopback_tcp_available() {
            return;
        }
        let topology = RuntimeTopology::validate(
            ["/ip4/127.0.0.1/tcp/0".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect("topology");
        let transport = Libp2pTransport::new(&topology, [7; 32], false, false, RelayMode::Disabled)
            .expect("transport");
        assert!(!transport.peer_id().is_empty());
        assert!(!transport.listen_addresses().is_empty());
    }

    #[test]
    fn libp2p_transport_can_deliver_between_two_nodes() {
        if !loopback_tcp_available() {
            return;
        }
        let topology_a = RuntimeTopology::validate(
            ["/ip4/127.0.0.1/tcp/0".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect("topology a");
        let topology_b = RuntimeTopology::validate(
            ["/ip4/127.0.0.1/tcp/0".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect("topology b");
        let transport_a =
            Libp2pTransport::new(&topology_a, [11; 32], false, false, RelayMode::Disabled)
                .expect("transport a");
        let transport_b =
            Libp2pTransport::new(&topology_b, [12; 32], false, false, RelayMode::Disabled)
                .expect("transport b");

        let mut target = transport_b.listen_addresses()[0].clone();
        target.push(Protocol::P2p(
            transport_b.peer_id().parse::<PeerId>().expect("peer id"),
        ));

        let payload = serde_json::to_string(&serde_json::json!({
            "msg": "hello-libp2p"
        }))
        .expect("payload");
        transport_a
            .deliver(&target, &payload)
            .expect("deliver")
            .expect("delivery report");

        let mut received = Vec::new();
        for _ in 0..20 {
            let batch = transport_b.receive(&topology_b).expect("receive");
            if !batch.is_empty() {
                received = batch;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        assert_eq!(received, vec![payload]);
    }

    #[test]
    fn runtime_topology_rejects_quic_listen_until_supported() {
        let error = RuntimeTopology::validate(
            ["/ip4/127.0.0.1/udp/4001/quic-v1".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect_err("quic should be rejected");
        assert!(error.to_string().contains("/tcp"));
    }

    #[test]
    fn libp2p_transport_rejects_seed_without_peer_id() {
        let topology = RuntimeTopology::validate(
            ["/ip4/127.0.0.1/tcp/0".to_owned()],
            ["/ip4/127.0.0.1/tcp/4001".to_owned()],
        )
        .expect("topology");

        let error = Libp2pTransport::new(&topology, [21; 32], false, false, RelayMode::Disabled)
            .expect_err("seed should fail");
        assert!(error.to_string().contains("/p2p/<peer-id>"));
    }

    #[test]
    fn mdns_toggle_off_produces_disabled_behaviour() {
        if !loopback_tcp_available() {
            return;
        }
        let topology = RuntimeTopology::validate(
            ["/ip4/127.0.0.1/tcp/0".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect("topology");
        let transport =
            Libp2pTransport::new(&topology, [30; 32], false, false, RelayMode::Disabled)
                .expect("transport");
        assert!(transport.discovered_peers().is_empty());
    }

    #[test]
    fn build_swarm_with_mdns_enabled() {
        if !loopback_tcp_available() {
            return;
        }
        let topology = RuntimeTopology::validate(
            ["/ip4/127.0.0.1/tcp/0".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect("topology");
        let transport = Libp2pTransport::new(&topology, [31; 32], true, false, RelayMode::Disabled)
            .expect("transport");
        assert!(!transport.peer_id().is_empty());
        assert!(transport.discovered_peers().is_empty());
    }

    #[test]
    fn nat_status_serialize_deserialize_roundtrip() {
        for status in [NatStatus::Unknown, NatStatus::Public, NatStatus::Private] {
            let json = serde_json::to_string(&status).expect("serialize");
            let parsed: NatStatus = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn build_swarm_with_nat_traversal_enabled() {
        if !loopback_tcp_available() {
            return;
        }
        let topology = RuntimeTopology::validate(
            ["/ip4/127.0.0.1/tcp/0".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect("topology");
        let transport = Libp2pTransport::new(&topology, [32; 32], false, true, RelayMode::Disabled)
            .expect("transport");
        assert!(!transport.peer_id().is_empty());
        assert_eq!(transport.nat_status(), NatStatus::Unknown);
    }

    #[test]
    fn parse_seed_targets_keeps_only_peer_annotated_multiaddrs() {
        let peer_id = libp2p_peer_id_from_private_key([13; 32]).expect("peer id");
        let targets = parse_seed_targets(&[
            PeerEndpoint {
                address: "/ip4/127.0.0.1/tcp/4001".to_owned(),
            },
            PeerEndpoint {
                address: format!("/ip4/127.0.0.1/tcp/4002/p2p/{peer_id}"),
            },
        ]);

        assert_eq!(targets.len(), 1);
    }

    #[test]
    fn full_mdns_queue_does_not_mark_peer_as_seen() {
        let mut discovered_peers = (0..MAX_DISCOVERED_PEERS)
            .map(|index| DiscoveredPeer {
                peer_id: format!("peer-{index}"),
                addresses: Vec::new(),
            })
            .collect::<Vec<_>>();
        let mut mdns_seen_peers = HashSet::new();
        let peer_id = libp2p_peer_id_from_private_key([33; 32])
            .expect("peer id")
            .parse::<PeerId>()
            .expect("parse peer id");
        let addresses = vec![
            "/ip4/127.0.0.1/tcp/4001"
                .parse::<Multiaddr>()
                .expect("multiaddr"),
        ];

        assert!(!queue_discovered_peer(
            &mut discovered_peers,
            &mut mdns_seen_peers,
            peer_id,
            addresses.clone(),
        ));
        assert!(!mdns_seen_peers.contains(&peer_id));

        discovered_peers.pop();

        assert!(queue_discovered_peer(
            &mut discovered_peers,
            &mut mdns_seen_peers,
            peer_id,
            addresses,
        ));
        assert!(mdns_seen_peers.contains(&peer_id));
    }

    #[test]
    fn mailbox_reader_skips_oversized_lines_and_keeps_valid_messages() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("unix time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("starweft-mailbox-{unique}.recv"));
        let oversized = "x".repeat(12);
        fs::write(&path, format!("first\n{oversized}\nsecond\n")).expect("write mailbox");

        let mut messages = Vec::new();
        read_mailbox_messages_with_limit(&path, &mut messages, 10).expect("read mailbox");
        let _ = fs::remove_file(&path);

        assert_eq!(messages, vec!["first".to_owned(), "second".to_owned()]);
    }

    #[test]
    fn local_mailbox_transport_rejects_oversized_payload() {
        let transport = LocalMailboxTransport;
        let target = Multiaddr::from(Protocol::Unix("/tmp/starweft-mailbox".into()));
        let payload = "x".repeat(MAX_MESSAGE_SIZE + 1);

        let error = transport
            .deliver(&target, &payload)
            .expect_err("oversized payload should fail");

        assert!(error.to_string().contains("E_MESSAGE_TOO_LARGE"));
    }

    #[test]
    fn libp2p_transport_rejects_oversized_payload_before_enqueue() {
        if !loopback_tcp_available() {
            return;
        }
        let topology = RuntimeTopology::validate(
            ["/ip4/127.0.0.1/tcp/0".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect("topology");
        let transport =
            Libp2pTransport::new(&topology, [34; 32], false, false, RelayMode::Disabled)
                .expect("transport");
        let mut target = transport.listen_addresses()[0].clone();
        target.push(Protocol::P2p(
            transport.peer_id().parse::<PeerId>().expect("peer id"),
        ));
        let payload = "x".repeat(MAX_MESSAGE_SIZE + 1);

        let error = transport
            .deliver(&target, &payload)
            .expect_err("oversized payload should fail");

        assert!(error.to_string().contains("E_MESSAGE_TOO_LARGE"));
    }
}
