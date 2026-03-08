use anyhow::{Result, anyhow, bail};
use libp2p::futures::StreamExt;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{Multiaddr, PeerId, StreamProtocol, Swarm};
use multiaddr::Protocol;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::thread;

const MAX_INBOX_CAPACITY: usize = 10_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerEndpoint {
    pub address: String,
}

impl PeerEndpoint {
    pub fn validate(&self) -> Result<Multiaddr> {
        self.address.parse().map_err(Into::into)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListenAddress {
    pub raw: String,
    pub multiaddr: Multiaddr,
}

impl ListenAddress {
    pub fn parse(raw: impl Into<String>) -> Result<Self> {
        let raw = raw.into();
        let multiaddr = raw.parse::<Multiaddr>()?;
        validate_supported_transport(&multiaddr)?;
        Ok(Self { raw, multiaddr })
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RuntimeTopology {
    pub listen_addresses: Vec<ListenAddress>,
    pub seed_peers: Vec<PeerEndpoint>,
}

impl RuntimeTopology {
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

    pub fn local_mailbox_paths(&self) -> Vec<PathBuf> {
        self.listen_addresses
            .iter()
            .filter_map(|address| mailbox_path_from_multiaddr(&address.multiaddr))
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportKind {
    LocalMailbox,
    Libp2p,
}

#[derive(Clone, Debug)]
pub struct DeliveryReport {
    pub target: String,
}

pub trait TransportDriver {
    fn kind(&self) -> TransportKind;
    fn receive(&self, topology: &RuntimeTopology) -> Result<Vec<String>>;
    fn deliver(&self, multiaddr: &Multiaddr, payload: &str) -> Result<Option<DeliveryReport>>;
}

pub enum RuntimeTransport {
    LocalMailbox(LocalMailboxTransport),
    Libp2p(Libp2pTransport),
}

impl RuntimeTransport {
    pub fn local_mailbox() -> Self {
        Self::LocalMailbox(LocalMailboxTransport)
    }

    pub fn libp2p(topology: &RuntimeTopology, private_key: [u8; 32]) -> Result<Self> {
        Ok(Self::Libp2p(Libp2pTransport::new(topology, private_key)?))
    }

    pub fn peer_id_hint(&self) -> Option<&str> {
        match self {
            Self::LocalMailbox(_) => None,
            Self::Libp2p(driver) => Some(driver.peer_id()),
        }
    }

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
}

pub fn libp2p_peer_id_from_private_key(private_key: [u8; 32]) -> Result<String> {
    let identity = libp2p::identity::Keypair::ed25519_from_bytes(private_key)
        .map_err(|error| anyhow!("failed to decode libp2p identity: {error}"))?;
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
    let has_udp_quic = protocols
        .iter()
        .any(|protocol| matches!(protocol, Protocol::Udp(_)))
        && protocols
            .iter()
            .any(|protocol| matches!(protocol, Protocol::QuicV1));
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

    if has_udp_quic || has_tcp {
        return Ok(());
    }

    Err(anyhow!(
        "unsupported transport; expected /tcp or /udp/.../quic-v1"
    ))
}

pub fn mailbox_path_from_multiaddr(address: &Multiaddr) -> Option<PathBuf> {
    address.iter().find_map(|protocol| match protocol {
        Protocol::Unix(path) => Some(PathBuf::from(path.to_string())),
        _ => None,
    })
}

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
                let content = fs::read_to_string(&tmp)?;
                let _ = fs::remove_file(&tmp);
                messages.extend(
                    content
                        .lines()
                        .filter(|line| !line.trim().is_empty())
                        .map(ToOwned::to_owned),
                );
            }
            // Atomically claim the mailbox
            if fs::rename(&mailbox, &tmp).is_err() {
                continue;
            }
            let content = fs::read_to_string(&tmp)?;
            let _ = fs::remove_file(&tmp);
            messages.extend(
                content
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .map(ToOwned::to_owned),
            );
        }
        Ok(messages)
    }

    fn deliver(&self, multiaddr: &Multiaddr, payload: &str) -> Result<Option<DeliveryReport>> {
        let Some(mailbox_path) = mailbox_path_from_multiaddr(multiaddr) else {
            return Ok(None);
        };

        if let Some(parent) = mailbox_path.parent() {
            fs::create_dir_all(parent)?;
        }

        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&mailbox_path)?;
        writeln!(file, "{payload}")?;
        Ok(Some(DeliveryReport {
            target: mailbox_path.display().to_string(),
        }))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnvelopeAck {
    pub accepted: bool,
}

#[derive(NetworkBehaviour)]
pub struct StarweftBehaviour {
    pub request_response: request_response::cbor::Behaviour<String, EnvelopeAck>,
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
    Shutdown,
}

struct Libp2pInit {
    peer_id: String,
    listen_addresses: Vec<Multiaddr>,
}

pub struct Libp2pTransport {
    command_tx: tokio::sync::mpsc::UnboundedSender<Libp2pCommand>,
    peer_id: String,
    listen_addresses: Vec<Multiaddr>,
    _worker: thread::JoinHandle<()>,
}

impl Libp2pTransport {
    pub fn new(topology: &RuntimeTopology, private_key: [u8; 32]) -> Result<Self> {
        let listen_addresses = topology
            .listen_addresses
            .iter()
            .map(|address| address.multiaddr.clone())
            .collect::<Vec<_>>();
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

        let expected_listens = listen_addresses.len();
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        let (init_tx, init_rx) = mpsc::channel();
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
                let (mut swarm, announced) =
                    build_swarm(listen_addresses, expected_listens, private_key).await?;
                let peer_id = swarm.local_peer_id().to_string();
                init_tx
                    .send(Ok(Libp2pInit {
                        peer_id,
                        listen_addresses: announced,
                    }))
                    .map_err(|_| anyhow!("init channel closed"))?;
                libp2p_event_loop(&mut swarm, command_rx).await
            });

            if let Err(error) = result {
                tracing::error!("libp2p worker stopped: {error:#}");
            }
        });

        let init = init_rx.recv()??;
        Ok(Self {
            command_tx,
            peer_id: init.peer_id,
            listen_addresses: init.listen_addresses,
            _worker: worker,
        })
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    pub fn listen_addresses(&self) -> &[Multiaddr] {
        &self.listen_addresses
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

impl Drop for Libp2pTransport {
    fn drop(&mut self) {
        let _ = self.command_tx.send(Libp2pCommand::Shutdown);
    }
}

async fn build_swarm(
    listen_addresses: Vec<Multiaddr>,
    expected_listens: usize,
    private_key: [u8; 32],
) -> Result<(Swarm<StarweftBehaviour>, Vec<Multiaddr>)> {
    let identity = libp2p::identity::Keypair::ed25519_from_bytes(private_key)
        .map_err(|error| anyhow!("failed to decode libp2p identity: {error}"))?;
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(identity)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default().nodelay(true),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|_| {
            let protocols =
                std::iter::once((StreamProtocol::new("/starweft/0.1"), ProtocolSupport::Full));
            let config = request_response::Config::default();
            Ok(StarweftBehaviour {
                request_response: request_response::cbor::Behaviour::new(protocols, config),
            })
        })?
        .build();

    for address in listen_addresses {
        swarm.listen_on(address)?;
    }

    let mut announced = Vec::new();
    while announced.len() < expected_listens {
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
) -> Result<()> {
    let mut inbox = Vec::new();
    let mut pending = HashMap::new();

    loop {
        tokio::select! {
            Some(command) = command_rx.recv() => {
                match command {
                    Libp2pCommand::DrainInbox { reply_tx } => {
                        let drained = std::mem::take(&mut inbox);
                        let _ = reply_tx.send(Ok(drained));
                    }
                    Libp2pCommand::Deliver { multiaddr, payload, reply_tx } => {
                        let peer_id = extract_peer_id(&multiaddr)?;
                        swarm.add_peer_address(peer_id, multiaddr.clone());
                        let request_id = swarm.behaviour_mut().request_response.send_request(&peer_id, payload);
                        pending.insert(request_id, (reply_tx, multiaddr));
                    }
                    Libp2pCommand::Shutdown => break,
                }
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        tracing::debug!("libp2p listen address ready: {address}");
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::RequestResponse(
                        request_response::Event::Message { message, .. }
                    )) => {
                        match message {
                            request_response::Message::Request { request, channel, .. } => {
                                let accepted = inbox.len() < MAX_INBOX_CAPACITY;
                                if accepted {
                                    inbox.push(request);
                                }
                                let _ = swarm.behaviour_mut().request_response.send_response(channel, EnvelopeAck { accepted });
                            }
                            request_response::Message::Response { request_id, response } => {
                                if let Some((reply_tx, multiaddr)) = pending.remove(&request_id) {
                                    let report = DeliveryReport {
                                        target: multiaddr.to_string(),
                                    };
                                    let result = if response.accepted {
                                        Ok(Some(report))
                                    } else {
                                        Err(anyhow!("remote peer rejected request"))
                                    };
                                    let _ = reply_tx.send(result);
                                }
                            }
                        }
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::RequestResponse(
                        request_response::Event::OutboundFailure { request_id, error, .. }
                    )) => {
                        if let Some((reply_tx, _)) = pending.remove(&request_id) {
                            let _ = reply_tx.send(Err(anyhow!("outbound failure: {error}")));
                        }
                    }
                    SwarmEvent::Behaviour(StarweftBehaviourEvent::RequestResponse(
                        request_response::Event::InboundFailure { error, .. }
                    )) => {
                        tracing::warn!("libp2p inbound failure: {error}");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_create_libp2p_transport_for_tcp_listen() {
        let topology = RuntimeTopology::validate(
            ["/ip4/127.0.0.1/tcp/0".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect("topology");
        let transport = Libp2pTransport::new(&topology, [7; 32]).expect("transport");
        assert!(!transport.peer_id().is_empty());
        assert!(!transport.listen_addresses().is_empty());
    }

    #[test]
    fn libp2p_transport_can_deliver_between_two_nodes() {
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
        let transport_a = Libp2pTransport::new(&topology_a, [11; 32]).expect("transport a");
        let transport_b = Libp2pTransport::new(&topology_b, [12; 32]).expect("transport b");

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
}
