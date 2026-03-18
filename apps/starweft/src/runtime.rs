use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use multiaddr::Multiaddr;
use serde_json::Value;
use starweft_crypto::{StoredKeypair, verifying_key_from_base64};
use starweft_id::{ActorId, NodeId, ProjectId, StopId, TaskId};
use starweft_observation::{PlannedTaskSpec, estimate_task_duration_sec};
use starweft_openclaw_bridge::{
    BridgeTaskRequest, BridgeTaskResponse, OpenClawAttachment, execute_task_with_cancel_flag,
};
use starweft_p2p::{RelayMode, RuntimeTopology, RuntimeTransport, TransportDriver};
use starweft_protocol::{
    ApprovalApplied, ApprovalGranted, ApprovalScopeType, CapabilityAdvertisement, CapabilityQuery,
    Envelope, EvaluationIssued, EvaluationPolicy, JoinAccept, JoinOffer, JoinReject, MsgType,
    PROTOCOL_VERSION, ParticipantPolicy, ProjectCharter, PublishIntentProposed,
    PublishIntentSkipped, PublishResultRecorded, RoutedBody, SnapshotRequest, SnapshotResponse,
    SnapshotScopeType, StopAck, StopAckState, StopComplete, StopFinalState, StopOrder,
    StopScopeType, TaskDelegated, TaskExecutionStatus, TaskProgress, TaskResultSubmitted,
    UnsignedEnvelope, VisionConstraints, VisionIntent, WireEnvelope,
};
use starweft_runtime::RuntimePipeline;
use starweft_stop::{classify_stop_impact, should_owner_emit_completion};
use starweft_store::{
    LocalIdentityRecord, PeerAddressRecord, STORE_SCHEMA_VERSION_LABEL, Store, VisionRecord,
};
use time::OffsetDateTime;

use crate::cli::RunArgs;
use crate::config::{
    Config, DataPaths, NodeRole, OwnerSection, P2pTransportKind, load_existing_config,
};
use crate::decision::{self, FailureAction};
use crate::helpers::{
    BootstrapPeerParams, budget_mode_is_minimal, configured_actor_key_path,
    local_advertised_capabilities, local_listen_addresses, local_stop_public_key_helper,
    persist_task_artifact, policy_blocking_reason, project_vision_constraints_or_default,
    read_keypair, sync_discovery_seed_placeholders, upsert_bootstrap_peer,
    worker_supported_capabilities, write_runtime_log,
};
use crate::project::approve_execution_scope;
use crate::registry::{next_registry_sync_at, sync_discovery_registry};

// ---------------------------------------------------------------------------
// RunTick / TaskCompletion / WorkerRuntimeState
// ---------------------------------------------------------------------------

pub(crate) struct RunTick {
    pub(crate) queued_outbox: u64,
    pub(crate) running_tasks: u64,
    pub(crate) outbox_preview: Vec<starweft_store::OutboxMessageRecord>,
}

pub(crate) struct TaskCompletionQueue<'a> {
    pub(crate) tx: &'a mpsc::Sender<TaskCompletion>,
    pub(crate) rx: &'a mpsc::Receiver<TaskCompletion>,
}

pub(crate) struct TaskCompletion {
    pub(crate) project_id: ProjectId,
    pub(crate) task_id: TaskId,
    pub(crate) delegator_actor_id: ActorId,
    pub(crate) task_status: TaskExecutionStatus,
    pub(crate) bridge_response: BridgeTaskResponse,
}

#[derive(Clone, Default)]
pub(crate) struct WorkerRuntimeState {
    pub(crate) inner: Arc<Mutex<WorkerRuntimeInner>>,
}

#[derive(Default)]
pub(crate) struct WorkerRuntimeInner {
    pub(crate) running_tasks: HashMap<String, Arc<AtomicBool>>,
    pub(crate) pending_stops: HashMap<String, PendingStopOrder>,
}

#[derive(Clone)]
pub(crate) struct PendingStopOrder {
    pub(crate) stop_id: StopId,
    pub(crate) project_id: ProjectId,
    pub(crate) notify_actor_id: ActorId,
    pub(crate) remaining_task_ids: HashSet<String>,
}

#[derive(Clone)]
pub(crate) struct ReadyStopCompletion {
    pub(crate) stop_id: StopId,
    pub(crate) project_id: ProjectId,
    pub(crate) notify_actor_id: ActorId,
}

impl WorkerRuntimeState {
    fn lock_inner(&self) -> std::sync::MutexGuard<'_, WorkerRuntimeInner> {
        self.inner.lock().unwrap_or_else(|e| {
            tracing::warn!("WorkerRuntimeState mutex was poisoned; recovering");
            e.into_inner()
        })
    }

    pub(crate) fn register_task(&self, task_id: &TaskId, cancel_flag: Arc<AtomicBool>) {
        let mut inner = self.lock_inner();
        inner.running_tasks.insert(task_id.to_string(), cancel_flag);
    }

    pub(crate) fn cancel_tasks(&self, task_ids: &[TaskId]) -> Vec<TaskId> {
        let inner = self.lock_inner();
        let mut running = Vec::new();
        for task_id in task_ids {
            if let Some(cancel_flag) = inner.running_tasks.get(task_id.as_str()) {
                cancel_flag.store(true, Ordering::SeqCst);
                running.push(task_id.clone());
            }
        }
        running
    }

    pub(crate) fn has_pending_stop_for(&self, task_id: &TaskId) -> bool {
        let inner = self.lock_inner();
        inner
            .pending_stops
            .values()
            .any(|pending| pending.remaining_task_ids.contains(task_id.as_str()))
    }

    pub(crate) fn stage_stop(
        &self,
        stop_id: &StopId,
        project_id: &ProjectId,
        notify_actor_id: &ActorId,
        running_task_ids: &[TaskId],
    ) {
        if running_task_ids.is_empty() {
            return;
        }

        let mut inner = self.lock_inner();
        let entry = inner
            .pending_stops
            .entry(stop_id.to_string())
            .or_insert_with(|| PendingStopOrder {
                stop_id: stop_id.clone(),
                project_id: project_id.clone(),
                notify_actor_id: notify_actor_id.clone(),
                remaining_task_ids: HashSet::new(),
            });
        for task_id in running_task_ids {
            entry.remaining_task_ids.insert(task_id.to_string());
        }
    }

    pub(crate) fn complete_task(&self, task_id: &TaskId) -> Vec<ReadyStopCompletion> {
        let mut inner = self.lock_inner();
        inner.running_tasks.remove(task_id.as_str());

        let mut ready_ids = Vec::new();
        for (stop_key, pending) in &mut inner.pending_stops {
            pending.remaining_task_ids.remove(task_id.as_str());
            if pending.remaining_task_ids.is_empty() {
                ready_ids.push(stop_key.clone());
            }
        }

        let mut ready = Vec::new();
        for stop_key in ready_ids {
            if let Some(pending) = inner.pending_stops.remove(&stop_key) {
                ready.push(ReadyStopCompletion {
                    stop_id: pending.stop_id,
                    project_id: pending.project_id,
                    notify_actor_id: pending.notify_actor_id,
                });
            }
        }
        ready
    }
}

// ---------------------------------------------------------------------------
// run_node / run_node_once / build_transport / validate / wire compat
// ---------------------------------------------------------------------------

pub(crate) fn run_node(args: RunArgs) -> Result<()> {
    let (mut config, paths) = load_existing_config(args.data_dir.as_ref())?;
    if let Some(role) = args.role {
        config.node.role = role;
    }
    if let Some(log_level) = args.log_level {
        config.node.log_level = log_level;
    }

    paths.ensure_layout()?;
    let store = Store::open(&paths.ledger_db)?;
    sync_discovery_seed_placeholders(&config, &store)?;
    if let Err(error) = sync_discovery_registry(&config, &paths, &store, None, None) {
        eprintln!("{error:#}");
    }
    let topology =
        RuntimeTopology::validate(config.node.listen.clone(), config.discovery.seeds.clone())
            .map_err(|error| {
                anyhow!("[E_INVALID_MULTIADDR] listen/discovery 設定が不正です: {error}")
            })?;
    validate_runtime_compatibility(&config)?;
    let transport = build_transport(&config, &topology)?;
    let actor_key = if config.node.role == NodeRole::Relay {
        None
    } else {
        Some(read_keypair(&configured_actor_key_path(&config, &paths)?)?)
    };
    let (task_completion_tx, task_completion_rx) = mpsc::channel::<TaskCompletion>();
    let task_completion_queue = TaskCompletionQueue {
        tx: &task_completion_tx,
        rx: &task_completion_rx,
    };
    let worker_runtime = WorkerRuntimeState::default();
    request_peer_capabilities(
        &config,
        &paths,
        &store,
        &topology,
        &transport,
        actor_key.as_ref(),
    )?;
    announce_local_capabilities(
        &config,
        &paths,
        &store,
        &topology,
        &transport,
        actor_key.as_ref(),
    )?;
    if let Err(error) =
        sync_discovery_registry(&config, &paths, &store, Some(&topology), Some(&transport))
    {
        eprintln!("{error:#}");
    }
    let mut next_registry_sync_deadline = next_registry_sync_at(&config, OffsetDateTime::now_utc());

    if args.foreground {
        let running = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::clone(&running);
        ctrlc::set_handler(move || {
            shutdown.store(false, Ordering::SeqCst);
        })?;
        let mut ready_written = false;
        while running.load(Ordering::SeqCst) {
            if next_registry_sync_deadline
                .is_some_and(|deadline| OffsetDateTime::now_utc() >= deadline)
            {
                if let Err(error) = sync_discovery_registry(
                    &config,
                    &paths,
                    &store,
                    Some(&topology),
                    Some(&transport),
                ) {
                    eprintln!("{error:#}");
                }
                next_registry_sync_deadline =
                    next_registry_sync_at(&config, OffsetDateTime::now_utc());
            }
            let inbox_ctx = InboxProcessingContext {
                config: &config,
                paths: &paths,
                store: &store,
                topology: &topology,
                transport: &transport,
                actor_key: actor_key.as_ref(),
                task_completion_tx: task_completion_queue.tx,
                worker_runtime: &worker_runtime,
            };
            if let Err(error) = run_node_once(&inbox_ctx, &task_completion_queue) {
                eprintln!("{error:#}");
            }
            if !ready_written {
                let _ = std::fs::write(paths.root.join(".ready"), "");
                ready_written = true;
            }
            thread::sleep(Duration::from_millis(200));
        }
        return Ok(());
    }

    let inbox_ctx = InboxProcessingContext {
        config: &config,
        paths: &paths,
        store: &store,
        topology: &topology,
        transport: &transport,
        actor_key: actor_key.as_ref(),
        task_completion_tx: task_completion_queue.tx,
        worker_runtime: &worker_runtime,
    };
    let tick = run_node_once(&inbox_ctx, &task_completion_queue)?;
    println!("role: {}", config.node.role);
    println!("mode: oneshot");
    println!("queued_outbox: {}", tick.queued_outbox);
    println!("running_tasks: {}", tick.running_tasks);
    if tick.outbox_preview.is_empty() {
        println!("outbox_preview: none");
    } else {
        println!("outbox_preview:");
        for message in tick.outbox_preview {
            println!(
                "  {} {} {}",
                message.msg_type, message.msg_id, message.delivery_state
            );
        }
    }
    Ok(())
}

pub(crate) fn run_node_once(
    ctx: &InboxProcessingContext<'_>,
    task_completion_queue: &TaskCompletionQueue<'_>,
) -> Result<RunTick> {
    process_mdns_discoveries(
        ctx.config,
        ctx.paths,
        ctx.store,
        ctx.topology,
        ctx.transport,
        ctx.actor_key,
    )?;
    process_local_inbox(ctx)?;
    process_completed_tasks(
        ctx.config,
        ctx.paths,
        ctx.store,
        ctx.actor_key,
        task_completion_queue.rx,
        ctx.worker_runtime,
    )?;
    flush_outbox(ctx.config, ctx.paths, ctx.store, ctx.transport)?;
    let stats = ctx.store.stats()?;
    let outbox_preview = ctx.store.queued_outbox_messages(10)?;
    Ok(RunTick {
        queued_outbox: stats.queued_outbox_count,
        running_tasks: stats.running_task_count,
        outbox_preview,
    })
}

pub(crate) fn build_transport(
    config: &Config,
    topology: &RuntimeTopology,
) -> Result<RuntimeTransport> {
    match config.p2p.transport {
        P2pTransportKind::LocalMailbox => Ok(RuntimeTransport::local_mailbox()),
        P2pTransportKind::Libp2p => {
            let actor_key_path =
                configured_actor_key_path(config, &DataPaths::from_config(config)?)?;
            let actor_key = read_keypair(&actor_key_path)?;
            let relay_mode = if config.node.role == NodeRole::Relay && config.p2p.relay_enabled {
                RelayMode::Server {
                    max_reservations: config.relay.max_reservations,
                    max_circuits_per_peer: config.relay.max_circuits_per_peer,
                    reservation_duration: Duration::from_secs(
                        config.relay.reservation_duration_sec,
                    ),
                }
            } else if config.p2p.relay_enabled && config.p2p.nat_traversal {
                RelayMode::Client
            } else {
                RelayMode::Disabled
            };
            RuntimeTransport::libp2p(
                topology,
                actor_key.secret_key_bytes()?,
                config.discovery.mdns,
                config.p2p.nat_traversal,
                relay_mode,
            )
        }
    }
}

pub(crate) fn validate_runtime_compatibility(config: &Config) -> Result<()> {
    if config.compatibility.protocol_version != PROTOCOL_VERSION
        && !config.compatibility.allow_legacy_protocols
    {
        bail!(
            "[E_PROTOCOL_VERSION] protocol_version が未対応です: configured={} runtime={}",
            config.compatibility.protocol_version,
            PROTOCOL_VERSION
        );
    }
    if config.compatibility.schema_version != STORE_SCHEMA_VERSION_LABEL {
        bail!(
            "[E_SCHEMA_VERSION] schema_version が未対応です: configured={} runtime={}",
            config.compatibility.schema_version,
            STORE_SCHEMA_VERSION_LABEL
        );
    }
    if config.openclaw.capability_version != config.compatibility.bridge_capability_version {
        bail!(
            "[E_BRIDGE_CAPABILITY_VERSION] openclaw.capability_version と compatibility.bridge_capability_version が一致しません: {} != {}",
            config.openclaw.capability_version,
            config.compatibility.bridge_capability_version
        );
    }
    Ok(())
}

pub(crate) fn ensure_wire_protocol_compatible(config: &Config, protocol: &str) -> Result<()> {
    if protocol == PROTOCOL_VERSION {
        return Ok(());
    }
    if config.compatibility.allow_legacy_protocols {
        return Ok(());
    }
    bail!(
        "[E_PROTOCOL_VERSION] 受信メッセージ protocol が未対応です: received={protocol} expected={PROTOCOL_VERSION}"
    );
}

// ---------------------------------------------------------------------------
// InboxProcessingContext
// ---------------------------------------------------------------------------

pub(crate) struct InboxProcessingContext<'a> {
    pub(crate) config: &'a Config,
    pub(crate) paths: &'a DataPaths,
    pub(crate) store: &'a Store,
    pub(crate) topology: &'a RuntimeTopology,
    pub(crate) transport: &'a RuntimeTransport,
    pub(crate) actor_key: Option<&'a StoredKeypair>,
    pub(crate) task_completion_tx: &'a mpsc::Sender<TaskCompletion>,
    pub(crate) worker_runtime: &'a WorkerRuntimeState,
}

// ---------------------------------------------------------------------------
// process_local_inbox / flush_outbox / delivery helpers / wire routing
// ---------------------------------------------------------------------------

const OUTBOX_RETRY_DELAY_MILLIS: i64 = 250;
const OUTBOX_MAX_DELIVERY_ATTEMPTS: u64 = 20;
const ENVELOPE_EXPIRY_SKEW_SEC: i64 = 30;

pub(crate) fn process_local_inbox(ctx: &InboxProcessingContext<'_>) -> Result<()> {
    let local_identity = ctx.store.local_identity()?;

    for line in ctx.transport.receive(ctx.topology)? {
        let wire: WireEnvelope = match serde_json::from_str(&line) {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!("skipping malformed inbox message: {e}");
                continue;
            }
        };
        if ctx.config.node.role == NodeRole::Relay {
            relay_incoming_wire(ctx.config, ctx.paths, ctx.store, ctx.transport, &wire)?;
            continue;
        }
        route_incoming_wire(
            &IncomingWireContext {
                config: ctx.config,
                paths: ctx.paths,
                store: ctx.store,
                topology: ctx.topology,
                transport: ctx.transport,
                local_identity: &local_identity,
                actor_key: ctx.actor_key,
                task_completion_tx: ctx.task_completion_tx,
                worker_runtime: ctx.worker_runtime,
            },
            wire,
        )?;
    }

    Ok(())
}

pub(crate) fn flush_outbox(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    transport: &RuntimeTransport,
) -> Result<()> {
    let queued = store.queued_outbox_messages(100)?;
    let peers = store.list_peer_addresses()?;
    let p2p_log = paths.logs_dir.join("p2p.log");

    for message in queued {
        let wire: WireEnvelope = serde_json::from_str(&message.raw_json)?;
        let targets = resolve_delivery_targets(config, &wire, &peers);
        store.sync_outbox_delivery_targets(&message.msg_id, &targets)?;
        let deliveries = store.ready_outbox_deliveries(&message.msg_id, 100)?;
        if deliveries.is_empty() {
            if !targets.is_empty() || !store.outbox_deliveries(&message.msg_id)?.is_empty() {
                continue;
            }
            let state = store.mark_outbox_delivery_failure(
                &message.msg_id,
                "no delivery targets available",
                OffsetDateTime::now_utc() + time::Duration::milliseconds(OUTBOX_RETRY_DELAY_MILLIS),
                OUTBOX_MAX_DELIVERY_ATTEMPTS,
            )?;
            write_runtime_log(
                config,
                &p2p_log,
                &format!(
                    "delivery_state_changed {} {} attempts={} reason=no_targets",
                    wire.msg_id,
                    state,
                    message.delivery_attempts + 1
                ),
            )?;
            continue;
        }
        let payload = serde_json::to_string(&wire)?;
        for target in deliveries {
            match transport.deliver(&target.multiaddr.parse::<Multiaddr>()?, &payload) {
                Ok(Some(delivery)) => {
                    let state = store.mark_outbox_delivery_target_delivered(
                        &message.msg_id,
                        &target.multiaddr,
                    )?;
                    write_runtime_log(
                        config,
                        &p2p_log,
                        &format!(
                            "delivered {} to {} aggregate_state={}",
                            wire.msg_id, delivery.target, state
                        ),
                    )?;
                }
                Ok(None) => {
                    let error = format!("unsupported delivery target {}", target.multiaddr);
                    let state = store.mark_outbox_delivery_target_failure(
                        &message.msg_id,
                        &target.multiaddr,
                        &error,
                        OffsetDateTime::now_utc()
                            + time::Duration::milliseconds(OUTBOX_RETRY_DELAY_MILLIS),
                        OUTBOX_MAX_DELIVERY_ATTEMPTS,
                    )?;
                    write_runtime_log(
                        config,
                        &p2p_log,
                        &format!(
                            "delivery_failed {} {}: {error} aggregate_state={state}",
                            wire.msg_id, target.multiaddr
                        ),
                    )?;
                }
                Err(error) => {
                    let state = store.mark_outbox_delivery_target_failure(
                        &message.msg_id,
                        &target.multiaddr,
                        &error.to_string(),
                        OffsetDateTime::now_utc()
                            + time::Duration::milliseconds(OUTBOX_RETRY_DELAY_MILLIS),
                        OUTBOX_MAX_DELIVERY_ATTEMPTS,
                    )?;
                    write_runtime_log(
                        config,
                        &p2p_log,
                        &format!(
                            "delivery_failed {} {}: {error} aggregate_state={state}",
                            wire.msg_id, target.multiaddr
                        ),
                    )?;
                }
            }
        }
    }

    Ok(())
}

pub(crate) fn resolve_delivery_targets(
    config: &Config,
    envelope: &WireEnvelope,
    peers: &[PeerAddressRecord],
) -> Vec<PeerAddressRecord> {
    let mut targets = match &envelope.to_actor_id {
        Some(actor_id) => peers
            .iter()
            .filter(|peer| &peer.actor_id == actor_id)
            .cloned()
            .collect(),
        None => peers.to_vec(),
    };
    // Relay nodes must not send messages back to the original sender.
    if config.node.role == NodeRole::Relay {
        targets.retain(|peer| peer.actor_id != envelope.from_actor_id);
    }
    filter_delivery_targets(config, targets)
}

pub(crate) fn relay_incoming_wire(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    _transport: &RuntimeTransport,
    wire: &WireEnvelope,
) -> Result<()> {
    if !config.p2p.relay_enabled {
        return Ok(());
    }
    store.save_inbox_wire(wire)?;
    if store.inbox_message_processed(&wire.msg_id)? {
        return Ok(());
    }
    let relay_log = paths.logs_dir.join("relay.log");
    let runtime = RuntimePipeline::new(store);
    runtime.queue_raw_wire(wire)?;
    runtime.mark_inbox_message_processed(&wire.msg_id)?;
    write_runtime_log(config, &relay_log, &format!("queued relay {}", wire.msg_id))?;
    Ok(())
}

pub(crate) fn filter_delivery_targets(
    config: &Config,
    mut targets: Vec<PeerAddressRecord>,
) -> Vec<PeerAddressRecord> {
    if !config.p2p.relay_enabled {
        targets.retain(|peer| !is_relay_address(&peer.multiaddr));
    }
    targets.sort_by_key(|peer| {
        let relay = is_relay_address(&peer.multiaddr);
        if config.p2p.direct_preferred {
            relay
        } else {
            !relay
        }
    });
    targets
}

pub(crate) fn is_relay_address(multiaddr: &str) -> bool {
    multiaddr.contains("/p2p-circuit")
}

pub(crate) fn verify_and_decode<T>(wire: WireEnvelope, public_key: &str) -> Result<Envelope<T>>
where
    T: RoutedBody + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let verifying_key = verifying_key_from_base64(public_key)?;
    let envelope: Envelope<T> = wire.decode()?;
    envelope.verify_with_key(&verifying_key)?;
    ensure_envelope_fresh(&envelope)?;
    Ok(envelope)
}

pub(crate) fn verify_bootstrap_capability_query(
    wire: WireEnvelope,
) -> Result<Envelope<CapabilityQuery>> {
    let envelope: Envelope<CapabilityQuery> = wire.decode()?;
    let verifying_key = verifying_key_from_base64(&envelope.body.public_key)?;
    envelope.verify_with_key(&verifying_key)?;
    ensure_envelope_fresh(&envelope)?;
    Ok(envelope)
}

pub(crate) fn verify_bootstrap_capability_advertisement(
    wire: WireEnvelope,
) -> Result<Envelope<CapabilityAdvertisement>> {
    let envelope: Envelope<CapabilityAdvertisement> = wire.decode()?;
    let verifying_key = verifying_key_from_base64(&envelope.body.public_key)?;
    envelope.verify_with_key(&verifying_key)?;
    ensure_envelope_fresh(&envelope)?;
    Ok(envelope)
}

pub(crate) fn ensure_envelope_fresh<T>(envelope: &Envelope<T>) -> Result<()> {
    let Some(expires_at) = envelope.expires_at else {
        return Ok(());
    };
    let cutoff = expires_at + time::Duration::seconds(ENVELOPE_EXPIRY_SKEW_SEC);
    if OffsetDateTime::now_utc() > cutoff {
        bail!(
            "[E_MESSAGE_EXPIRED] msg_id={} expires_at={} skew_sec={}",
            envelope.msg_id,
            expires_at,
            ENVELOPE_EXPIRY_SKEW_SEC
        );
    }
    Ok(())
}

pub(crate) struct IncomingWireContext<'a> {
    pub(crate) config: &'a Config,
    pub(crate) paths: &'a DataPaths,
    pub(crate) store: &'a Store,
    pub(crate) topology: &'a RuntimeTopology,
    pub(crate) transport: &'a RuntimeTransport,
    pub(crate) local_identity: &'a Option<LocalIdentityRecord>,
    pub(crate) actor_key: Option<&'a StoredKeypair>,
    pub(crate) task_completion_tx: &'a mpsc::Sender<TaskCompletion>,
    pub(crate) worker_runtime: &'a WorkerRuntimeState,
}

pub(crate) struct OwnerContext<'a> {
    pub(crate) store: &'a Store,
    pub(crate) actor_key: &'a StoredKeypair,
    pub(crate) owner_actor_id: &'a ActorId,
    pub(crate) project_id: &'a ProjectId,
}

pub(crate) fn route_incoming_wire(ctx: &IncomingWireContext<'_>, wire: WireEnvelope) -> Result<()> {
    ensure_wire_protocol_compatible(ctx.config, &wire.protocol)?;
    match wire.msg_type {
        MsgType::CapabilityQuery => {
            let envelope = verify_bootstrap_capability_query(wire)?;
            handle_capability_query(
                ctx.config,
                ctx.paths,
                ctx.store,
                ctx.topology,
                ctx.transport,
                ctx.actor_key,
                envelope,
            )?;
            return Ok(());
        }
        MsgType::CapabilityAdvertisement => {
            let envelope = verify_bootstrap_capability_advertisement(wire)?;
            handle_capability_advertisement(ctx.store, envelope)?;
            return Ok(());
        }
        _ => {}
    }
    if ctx.store.inbox_message_processed(&wire.msg_id)? {
        return Ok(());
    }
    let peer_identity = ctx
        .store
        .peer_identity(&wire.from_actor_id)?
        .ok_or_else(|| {
            anyhow!(
                "[E_SIGNATURE_VERIFY_FAILED] peer public key が未登録です: {}",
                wire.from_actor_id
            )
        })?;
    let runtime = RuntimePipeline::new(ctx.store);

    let pk = &peer_identity.public_key;
    match wire.msg_type {
        MsgType::CapabilityQuery | MsgType::CapabilityAdvertisement => {}
        MsgType::VisionIntent => {
            let envelope = verify_and_decode::<VisionIntent>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_vision(
                    ctx.config,
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                )?;
            }
        }
        MsgType::ProjectCharter => {
            let envelope = verify_and_decode::<ProjectCharter>(wire, pk)?;
            runtime.ingest_project_charter(&envelope)?;
        }
        MsgType::ApprovalGranted => {
            let envelope = verify_and_decode::<ApprovalGranted>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_approval_granted(
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                )?;
            } else if runtime.ingest_verified(&envelope)? {
                runtime.mark_inbox_message_processed(&envelope.msg_id)?;
            }
        }
        MsgType::ApprovalApplied => {
            let envelope = verify_and_decode::<ApprovalApplied>(wire, pk)?;
            runtime.ingest_approval_applied(&envelope)?;
        }
        MsgType::JoinOffer => {
            let envelope = verify_and_decode::<JoinOffer>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Worker {
                handle_worker_join_offer(
                    ctx.config,
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                )?;
            } else if runtime.ingest_verified(&envelope)? {
                runtime.mark_inbox_message_processed(&envelope.msg_id)?;
            }
        }
        MsgType::JoinAccept => {
            let envelope = verify_and_decode::<JoinAccept>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_join_accept(
                    ctx.config,
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                )?;
            } else if runtime.ingest_verified(&envelope)? {
                runtime.mark_inbox_message_processed(&envelope.msg_id)?;
            }
        }
        MsgType::JoinReject => {
            let envelope = verify_and_decode::<JoinReject>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_join_reject(
                    ctx.config,
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                )?;
            } else if runtime.ingest_verified(&envelope)? {
                runtime.mark_inbox_message_processed(&envelope.msg_id)?;
            }
        }
        MsgType::TaskDelegated => {
            let envelope = verify_and_decode::<TaskDelegated>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Worker {
                handle_worker_task(
                    ctx.config,
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                    ctx.task_completion_tx,
                    ctx.worker_runtime,
                )?;
            } else {
                runtime.ingest_task_delegated(&envelope)?;
            }
        }
        MsgType::TaskProgress => {
            let envelope = verify_and_decode::<TaskProgress>(wire, pk)?;
            runtime.ingest_task_progress(&envelope)?;
        }
        MsgType::TaskResultSubmitted => {
            let envelope = verify_and_decode::<TaskResultSubmitted>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_task_result(
                    ctx.config,
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                )?;
            } else {
                runtime.ingest_task_result_submitted(&envelope)?;
            }
        }
        MsgType::EvaluationIssued => {
            let envelope = verify_and_decode::<EvaluationIssued>(wire, pk)?;
            runtime.ingest_evaluation_issued(&envelope)?;
        }
        MsgType::PublishIntentProposed => {
            let envelope = verify_and_decode::<PublishIntentProposed>(wire, pk)?;
            runtime.ingest_publish_intent_proposed(&envelope)?;
        }
        MsgType::PublishIntentSkipped => {
            let envelope = verify_and_decode::<PublishIntentSkipped>(wire, pk)?;
            runtime.ingest_publish_intent_skipped(&envelope)?;
        }
        MsgType::PublishResultRecorded => {
            let envelope = verify_and_decode::<PublishResultRecorded>(wire, pk)?;
            runtime.ingest_publish_result_recorded(&envelope)?;
        }
        MsgType::SnapshotRequest => {
            let envelope = verify_and_decode::<SnapshotRequest>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_snapshot_request(
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                )?;
            }
        }
        MsgType::SnapshotResponse => {
            let envelope = verify_and_decode::<SnapshotResponse>(wire, pk)?;
            runtime.ingest_snapshot_response(&envelope)?;
        }
        MsgType::StopOrder => {
            let stop_key = match peer_identity.stop_public_key.as_deref() {
                Some(key) => key,
                None => {
                    tracing::warn!(
                        from_actor = %wire.from_actor_id,
                        "stop_public_key が未設定のため message signing key で StopOrder を検証します"
                    );
                    pk
                }
            };
            let envelope = verify_and_decode::<StopOrder>(wire, stop_key)?;
            match ctx.config.node.role {
                NodeRole::Owner => {
                    handle_owner_stop_order(ctx.store, ctx.local_identity, ctx.actor_key, envelope)?
                }
                NodeRole::Worker => handle_worker_stop_order(
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                    ctx.worker_runtime,
                )?,
                _ => runtime.ingest_stop_order(&envelope)?,
            }
        }
        MsgType::StopAck => {
            let envelope = verify_and_decode::<StopAck>(wire, pk)?;
            runtime.ingest_stop_ack(&envelope)?;
            if ctx.config.node.role == NodeRole::Owner {
                relay_stop_ack_to_principal(
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    &envelope,
                )?;
            }
        }
        MsgType::StopComplete => {
            let envelope = verify_and_decode::<StopComplete>(wire, pk)?;
            runtime.ingest_stop_complete(&envelope)?;
            if ctx.config.node.role == NodeRole::Owner {
                relay_stop_complete_to_principal(
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    &envelope,
                )?;
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Peer / owner resolution helpers
// ---------------------------------------------------------------------------

pub(crate) fn unique_peer_actor_id(store: &Store) -> Result<ActorId> {
    let mut actor_ids = store
        .list_peer_addresses()?
        .into_iter()
        .map(|peer| peer.actor_id)
        .collect::<Vec<_>>();
    actor_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    actor_ids.dedup();
    match actor_ids.as_slice() {
        [actor_id] => Ok(actor_id.clone()),
        [] => bail!("[E_OWNER_UNRESOLVED] owner peer が見つかりません。--owner を指定してください"),
        _ => bail!("[E_OWNER_UNRESOLVED] owner を一意に決定できません。--owner を指定してください"),
    }
}

pub(crate) fn resolve_default_owner_for_new_project(
    store: &Store,
    explicit_owner: Option<&str>,
) -> Result<ActorId> {
    match explicit_owner {
        Some(actor_id) => ActorId::new(actor_id.to_owned()).map_err(Into::into),
        None => unique_peer_actor_id(store),
    }
}

pub(crate) fn resolve_snapshot_owner_actor_id(
    store: &Store,
    scope_type: &SnapshotScopeType,
    scope_id: &str,
    explicit_owner: Option<&str>,
) -> Result<ActorId> {
    if let Some(actor_id) = explicit_owner {
        return ActorId::new(actor_id.to_owned()).map_err(Into::into);
    }

    let resolved = match scope_type {
        SnapshotScopeType::Project => {
            let project_id = ProjectId::new(scope_id.to_owned())?;
            store.project_owner_actor_id(&project_id)?
        }
        SnapshotScopeType::Task => {
            let task_id = TaskId::new(scope_id.to_owned())?;
            store.task_owner_actor_id(&task_id)?
        }
    };

    match resolved {
        Some(actor_id) => Ok(actor_id),
        None => unique_peer_actor_id(store),
    }
}

// ---------------------------------------------------------------------------
// Policy / approval / task planning / retry
// ---------------------------------------------------------------------------

pub(crate) struct PolicyBlockedTaskParams<'a> {
    pub(crate) store: &'a Store,
    pub(crate) actor_key: &'a StoredKeypair,
    pub(crate) owner_actor_id: &'a ActorId,
    pub(crate) project_id: &'a ProjectId,
    pub(crate) task_id: &'a TaskId,
    pub(crate) required_capability: &'a str,
    pub(crate) constraints: &'a VisionConstraints,
    pub(crate) policy_code: &'a str,
    pub(crate) summary: &'a str,
}

pub(crate) fn record_owner_policy_blocked_task_result(
    params: &PolicyBlockedTaskParams<'_>,
) -> Result<()> {
    let runtime = RuntimePipeline::new(params.store);
    let result = UnsignedEnvelope::new(
        params.owner_actor_id.clone(),
        Some(params.owner_actor_id.clone()),
        TaskResultSubmitted {
            status: TaskExecutionStatus::Failed,
            summary: params.summary.to_owned(),
            output_payload: serde_json::json!({
                "bridge_error": params.summary,
                "policy_code": params.policy_code,
                "required_capability": params.required_capability,
                "constraints": params.constraints,
            }),
            artifact_refs: Vec::new(),
            started_at: OffsetDateTime::now_utc(),
            finished_at: OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(params.project_id.clone())
    .with_task_id(params.task_id.clone())
    .sign(params.actor_key)?;
    runtime.record_local_task_result_submitted(&result)?;
    Ok(())
}

pub(crate) struct RemoteApprovalParams<'a> {
    pub(crate) store: &'a Store,
    pub(crate) actor_key: &'a StoredKeypair,
    pub(crate) approver_actor_id: &'a ActorId,
    pub(crate) owner_actor_id: ActorId,
    pub(crate) scope_type: ApprovalScopeType,
    pub(crate) scope_id: String,
    pub(crate) project_id: Option<ProjectId>,
    pub(crate) task_id: Option<TaskId>,
}

pub(crate) fn queue_remote_approval(
    params: RemoteApprovalParams<'_>,
) -> Result<RemoteApprovalOutput> {
    let approval = UnsignedEnvelope::new(
        params.approver_actor_id.clone(),
        Some(params.owner_actor_id.clone()),
        ApprovalGranted {
            scope_type: params.scope_type.clone(),
            scope_id: params.scope_id.clone(),
            approved_at: OffsetDateTime::now_utc(),
        },
    );
    let approval = match (params.project_id.clone(), params.task_id.clone()) {
        (Some(project_id), Some(task_id)) => {
            approval.with_project_id(project_id).with_task_id(task_id)
        }
        (Some(project_id), None) => approval.with_project_id(project_id),
        (None, Some(task_id)) => approval.with_task_id(task_id),
        (None, None) => approval,
    }
    .sign(params.actor_key)?;
    RuntimePipeline::new(params.store).queue_outgoing(&approval)?;
    Ok(RemoteApprovalOutput {
        scope_type: match params.scope_type {
            ApprovalScopeType::Project => "project".to_owned(),
            ApprovalScopeType::Task => "task".to_owned(),
        },
        scope_id: params.scope_id,
        owner_actor_id: params.owner_actor_id.to_string(),
        msg_id: approval.msg_id.to_string(),
        project_id: params.project_id.map(|value| value.to_string()),
        task_id: params.task_id.map(|value| value.to_string()),
    })
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct RemoteApprovalOutput {
    pub(crate) scope_type: String,
    pub(crate) scope_id: String,
    pub(crate) owner_actor_id: String,
    pub(crate) msg_id: String,
    pub(crate) project_id: Option<String>,
    pub(crate) task_id: Option<String>,
}

pub(crate) fn derive_planned_tasks(
    config: &Config,
    vision: &VisionIntent,
    required_capability: &str,
) -> Result<Vec<PlannedTaskSpec>> {
    decision::derive_planned_tasks(config, vision, required_capability)
}

pub(crate) fn queue_owner_planned_task(
    ctx: &OwnerContext<'_>,
    task_spec: PlannedTaskSpec,
    parent_task_id: Option<TaskId>,
    depends_on: Vec<TaskId>,
) -> Result<TaskId> {
    queue_owner_planned_task_with_id(
        ctx,
        task_spec,
        parent_task_id,
        depends_on,
        TaskId::generate(),
    )
}

fn queue_owner_planned_task_with_id(
    ctx: &OwnerContext<'_>,
    task_spec: PlannedTaskSpec,
    parent_task_id: Option<TaskId>,
    depends_on: Vec<TaskId>,
    task_id: TaskId,
) -> Result<TaskId> {
    let task = UnsignedEnvelope::new(
        ctx.owner_actor_id.clone(),
        Some(ctx.owner_actor_id.clone()),
        TaskDelegated {
            parent_task_id,
            depends_on,
            title: task_spec.title,
            description: task_spec.description,
            objective: task_spec.objective,
            required_capability: task_spec.required_capability,
            input_payload: task_spec.input_payload,
            expected_output_schema: task_spec.expected_output_schema,
        },
    )
    .with_project_id(ctx.project_id.clone())
    .with_task_id(task_id.clone())
    .sign(ctx.actor_key)?;
    RuntimePipeline::new(ctx.store).record_local_task_delegated(&task)?;
    Ok(task_id)
}

pub(crate) fn queue_retry_task(ctx: &OwnerContext<'_>, failed_task_id: &TaskId) -> Result<TaskId> {
    let blueprint = ctx
        .store
        .task_blueprint(failed_task_id)?
        .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] retry 元 task blueprint が見つかりません"))?;
    queue_owner_planned_task(
        ctx,
        PlannedTaskSpec {
            title: blueprint.title,
            description: blueprint.description,
            objective: blueprint.objective,
            required_capability: blueprint.required_capability,
            input_payload: blueprint.input_payload,
            expected_output_schema: blueprint.expected_output_schema,
            rationale: "retry cloned from failed task blueprint".to_owned(),
            depends_on_indices: Vec::new(),
        },
        Some(failed_task_id.clone()),
        Vec::new(),
    )
}

pub(crate) fn materialize_planner_child_tasks(
    ctx: &OwnerContext<'_>,
    planner_task_id: &TaskId,
    task_specs: Vec<PlannedTaskSpec>,
) -> Result<()> {
    if ctx.store.task_child_count(planner_task_id)? > 0 {
        return Ok(());
    }
    // Two-pass: pre-generate all TaskIds, then create tasks with resolved depends_on
    let task_ids: Vec<TaskId> = (0..task_specs.len()).map(|_| TaskId::generate()).collect();
    for (i, task_spec) in task_specs.into_iter().enumerate() {
        let depends_on = resolve_dependency_indices(&task_spec, i, &task_ids);
        queue_owner_planned_task_with_id(
            ctx,
            task_spec,
            Some(planner_task_id.clone()),
            depends_on,
            task_ids[i].clone(),
        )?;
    }
    Ok(())
}

fn resolve_dependency_indices(
    spec: &PlannedTaskSpec,
    task_index: usize,
    task_ids: &[TaskId],
) -> Vec<TaskId> {
    let num_tasks = task_ids.len();
    spec.depends_on_indices
        .iter()
        .filter_map(|&dep_index| {
            if dep_index >= num_tasks {
                tracing::warn!(
                    "depends_on_indices に範囲外のインデックス {} が含まれています (タスク数: {}, タスク: {})",
                    dep_index, num_tasks, task_index
                );
                return None;
            }
            if dep_index == task_index {
                tracing::warn!(
                    "depends_on_indices に自己参照インデックス {} が含まれています (タスク: {})",
                    dep_index, task_index
                );
                return None;
            }
            task_ids.get(dep_index).cloned()
        })
        .collect()
}

fn cascade_dependency_failure(ctx: &OwnerContext<'_>, failed_task_id: &TaskId) -> Result<()> {
    let runtime = RuntimePipeline::new(ctx.store);
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    queue.push_back(failed_task_id.clone());
    visited.insert(failed_task_id.clone());

    while let Some(current_failed_id) = queue.pop_front() {
        let dependents = ctx.store.task_dependents(&current_failed_id)?;
        for dep_id in dependents {
            if !visited.insert(dep_id.clone()) {
                continue;
            }
            let status = ctx.store.task_status(&dep_id)?;
            if status == Some(starweft_protocol::TaskStatus::Queued) {
                let result = UnsignedEnvelope::new(
                    ctx.owner_actor_id.clone(),
                    Some(ctx.owner_actor_id.clone()),
                    TaskResultSubmitted {
                        status: TaskExecutionStatus::Failed,
                        summary: format!("dependency failed: {current_failed_id}"),
                        output_payload: serde_json::json!(null),
                        artifact_refs: Vec::new(),
                        started_at: time::OffsetDateTime::now_utc(),
                        finished_at: time::OffsetDateTime::now_utc(),
                    },
                )
                .with_project_id(ctx.project_id.clone())
                .with_task_id(dep_id.clone())
                .sign(ctx.actor_key)?;
                runtime.ingest_task_result_submitted(&result)?;
                queue.push_back(dep_id);
            }
        }
    }
    Ok(())
}

pub(crate) fn emit_worker_stop_complete(
    _store: &Store,
    runtime: &RuntimePipeline<'_>,
    worker_actor_id: &ActorId,
    actor_key: &StoredKeypair,
    ready: ReadyStopCompletion,
) -> Result<()> {
    let complete = UnsignedEnvelope::new(
        worker_actor_id.clone(),
        Some(ready.notify_actor_id),
        StopComplete {
            stop_id: ready.stop_id,
            actor_id: worker_actor_id.clone(),
            final_state: StopFinalState::Stopped,
            completed_at: time::OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(ready.project_id)
    .sign(actor_key)?;
    runtime.record_local_stop_complete(&complete)?;
    runtime.queue_outgoing(&complete)?;
    Ok(())
}

pub(crate) fn handle_owner_vision(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<VisionIntent>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;

    store.save_inbox_message(&envelope)?;
    if store.inbox_message_processed(&envelope.msg_id)? {
        return Ok(());
    }
    let vision_id = envelope
        .vision_id
        .clone()
        .ok_or_else(|| anyhow!("vision_id is required"))?;
    store.save_vision(&VisionRecord {
        vision_id: vision_id.clone(),
        principal_actor_id: envelope.from_actor_id.clone(),
        title: envelope.body.title.clone(),
        raw_vision_text: envelope.body.raw_vision_text.clone(),
        constraints: serde_json::to_value(&envelope.body.constraints)?,
        status: "accepted".to_owned(),
        created_at: envelope.created_at,
    })?;

    let runtime = RuntimePipeline::new(store);
    let project_id = starweft_id::ProjectId::generate();
    let charter = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        ProjectCharter {
            project_id: project_id.clone(),
            vision_id: vision_id.clone(),
            principal_actor_id: envelope.from_actor_id.clone(),
            owner_actor_id: local_identity.actor_id.clone(),
            title: format!("{} project", envelope.body.title),
            objective: envelope.body.raw_vision_text.clone(),
            stop_authority_actor_id: envelope.from_actor_id.clone(),
            participant_policy: ParticipantPolicy {
                external_agents_allowed: envelope
                    .body
                    .constraints
                    .allow_external_agents
                    .unwrap_or(true),
            },
            evaluation_policy: EvaluationPolicy {
                quality_weight: 0.4,
                speed_weight: 0.2,
                reliability_weight: 0.2,
                alignment_weight: 0.2,
            },
        },
    )
    .with_vision_id(vision_id.clone())
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    runtime.record_local_project_charter(&charter)?;
    runtime.queue_outgoing(&charter)?;

    let initial_tasks = if decision::planning_runs_on_worker(config) {
        vec![decision::planner_task_spec(
            config,
            &envelope.body,
            &config.compatibility.bridge_capability_version,
        )]
    } else {
        derive_planned_tasks(
            config,
            &envelope.body,
            &config.compatibility.bridge_capability_version,
        )?
    };
    let owner_ctx = OwnerContext {
        store,
        actor_key,
        owner_actor_id: &local_identity.actor_id,
        project_id: &project_id,
    };

    // Pre-generate TaskIds for initial tasks to resolve dependency indices
    let task_ids: Vec<TaskId> = (0..initial_tasks.len())
        .map(|_| TaskId::generate())
        .collect();
    for (i, task_spec) in initial_tasks.into_iter().enumerate() {
        let depends_on = resolve_dependency_indices(&task_spec, i, &task_ids);
        queue_owner_planned_task_with_id(
            &owner_ctx,
            task_spec,
            None,
            depends_on,
            task_ids[i].clone(),
        )?;
    }

    if let Some(worker_actor_id) = select_next_worker_actor_id(
        store,
        &project_id,
        &local_identity.actor_id,
        &envelope.from_actor_id,
        &[],
    )? {
        dispatch_next_task_offer(&owner_ctx, worker_actor_id)?;
    }

    store.mark_inbox_message_processed(&envelope.msg_id)?;

    Ok(())
}

pub(crate) fn handle_worker_task(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<TaskDelegated>,
    task_completion_tx: &mpsc::Sender<TaskCompletion>,
    worker_runtime: &WorkerRuntimeState,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);

    runtime.ingest_task_delegated(&envelope)?;
    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let task_id = envelope
        .task_id
        .clone()
        .ok_or_else(|| anyhow!("task_id is required"))?;

    // Send initial progress synchronously
    let progress_started = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        TaskProgress {
            progress: 0.1,
            message: "task execution started".to_owned(),
            updated_at: time::OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(project_id.clone())
    .with_task_id(task_id.clone())
    .sign(actor_key)?;
    runtime.record_local_task_progress(&progress_started)?;
    runtime.queue_outgoing(&progress_started)?;

    // Spawn task execution in background thread
    let openclaw_enabled = config.openclaw.enabled;
    let attachment = OpenClawAttachment {
        bin: config.openclaw.bin.clone(),
        working_dir: config.openclaw.working_dir.clone(),
        timeout_sec: Some(config.openclaw.timeout_sec),
    };
    let task_title = envelope.body.title.clone();
    let task_input = envelope.body.input_payload.clone();
    let request = BridgeTaskRequest {
        title: envelope.body.title.clone(),
        description: envelope.body.description.clone(),
        objective: envelope.body.objective.clone(),
        required_capability: envelope.body.required_capability.clone(),
        input_payload: envelope.body.input_payload.clone(),
    };
    let delegator_actor_id = envelope.from_actor_id.clone();
    let tx = task_completion_tx.clone();
    let project_id_clone = project_id.clone();
    let task_id_clone = task_id.clone();
    let cancel_flag = Arc::new(AtomicBool::new(false));
    worker_runtime.register_task(&task_id, Arc::clone(&cancel_flag));

    thread::spawn(move || {
        let (task_status, bridge_response) = if openclaw_enabled {
            match execute_task_with_cancel_flag(&attachment, &request, cancel_flag.as_ref()) {
                Ok(response) => (TaskExecutionStatus::Completed, response),
                Err(error)
                    if error.to_string().contains("[E_TASK_CANCELLED]")
                        || cancel_flag.load(Ordering::SeqCst) =>
                {
                    (
                        TaskExecutionStatus::Stopped,
                        BridgeTaskResponse {
                            summary: "task cancelled".to_owned(),
                            output_payload: serde_json::json!({ "cancelled": true }),
                            artifact_refs: Vec::new(),
                            progress_updates: Vec::new(),
                            raw_stdout: String::new(),
                            raw_stderr: error.to_string(),
                        },
                    )
                }
                Err(error) => (
                    TaskExecutionStatus::Failed,
                    BridgeTaskResponse {
                        summary: format!("bridge failed: {error}"),
                        output_payload: serde_json::json!({ "bridge_error": error.to_string() }),
                        artifact_refs: Vec::new(),
                        progress_updates: Vec::new(),
                        raw_stdout: String::new(),
                        raw_stderr: error.to_string(),
                    },
                ),
            }
        } else {
            (
                TaskExecutionStatus::Failed,
                BridgeTaskResponse {
                    summary: format!("bridge failed: openclaw disabled for {task_title}"),
                    output_payload: serde_json::json!({
                        "bridge_error": "openclaw disabled",
                        "input": task_input,
                    }),
                    artifact_refs: Vec::new(),
                    progress_updates: Vec::new(),
                    raw_stdout: String::new(),
                    raw_stderr: "openclaw disabled".to_owned(),
                },
            )
        };

        let _ = tx.send(TaskCompletion {
            project_id: project_id_clone,
            task_id: task_id_clone,
            delegator_actor_id,
            task_status,
            bridge_response,
        });
    });

    Ok(())
}

// ---------------------------------------------------------------------------
// process_completed_tasks / output schema enforcement
// ---------------------------------------------------------------------------

pub(crate) fn process_completed_tasks(
    config: &Config,
    _paths: &DataPaths,
    store: &Store,
    actor_key: Option<&StoredKeypair>,
    task_completion_rx: &mpsc::Receiver<TaskCompletion>,
    worker_runtime: &WorkerRuntimeState,
) -> Result<()> {
    let actor_key = match actor_key {
        Some(key) => key,
        None => return Ok(()),
    };
    let local_identity = match store.local_identity()? {
        Some(identity) => identity,
        None => return Ok(()),
    };
    let runtime = RuntimePipeline::new(store);
    let data_paths = DataPaths::from_config(config)?;

    let bridge_log = data_paths.logs_dir.join("bridge.log");

    while let Ok(mut completion) = task_completion_rx.try_recv() {
        let task_stopped_by_control = worker_runtime.has_pending_stop_for(&completion.task_id);
        let ready_stop_completions = worker_runtime.complete_task(&completion.task_id);

        // Log bridge output
        if config.openclaw.enabled {
            if !completion.bridge_response.raw_stdout.trim().is_empty() {
                write_runtime_log(
                    config,
                    &bridge_log,
                    &format!(
                        "task_id={} stdout={}",
                        completion.task_id,
                        completion.bridge_response.raw_stdout.replace('\n', "\\n")
                    ),
                )?;
            }
            if !completion.bridge_response.raw_stderr.trim().is_empty() {
                write_runtime_log(
                    config,
                    &bridge_log,
                    &format!(
                        "task_id={} stderr={}",
                        completion.task_id,
                        completion.bridge_response.raw_stderr.replace('\n', "\\n")
                    ),
                )?;
            }
        }

        let current_status = store
            .task_snapshot(&completion.task_id)?
            .map(|snapshot| snapshot.status);
        let suppress_result = task_stopped_by_control
            || matches!(
                current_status,
                Some(
                    starweft_protocol::TaskStatus::Stopping
                        | starweft_protocol::TaskStatus::Stopped
                )
            )
            || matches!(completion.task_status, TaskExecutionStatus::Stopped);

        if suppress_result {
            for ready in ready_stop_completions {
                emit_worker_stop_complete(
                    store,
                    &runtime,
                    &local_identity.actor_id,
                    actor_key,
                    ready,
                )?;
            }
            continue;
        }

        let completion_task_id = completion.task_id.clone();
        enforce_task_output_schema(store, &completion_task_id, &mut completion)?;

        // Persist artifact
        let persisted_artifact = persist_task_artifact(
            config,
            &data_paths,
            &completion.task_id,
            &completion.bridge_response.output_payload,
        )?;
        let mut artifact_refs = completion.bridge_response.artifact_refs;
        artifact_refs.push(persisted_artifact);

        // Send progress updates from bridge
        for update in &completion.bridge_response.progress_updates {
            let progress = UnsignedEnvelope::new(
                local_identity.actor_id.clone(),
                Some(completion.delegator_actor_id.clone()),
                TaskProgress {
                    progress: update.progress,
                    message: update.message.clone(),
                    updated_at: time::OffsetDateTime::now_utc(),
                },
            )
            .with_project_id(completion.project_id.clone())
            .with_task_id(completion.task_id.clone())
            .sign(actor_key)?;
            runtime.record_local_task_progress(&progress)?;
            runtime.queue_outgoing(&progress)?;
        }

        // Send stderr lines as progress
        for line in completion
            .bridge_response
            .raw_stderr
            .lines()
            .filter(|line| !line.trim().is_empty())
        {
            let progress = UnsignedEnvelope::new(
                local_identity.actor_id.clone(),
                Some(completion.delegator_actor_id.clone()),
                TaskProgress {
                    progress: 0.85,
                    message: format!("stderr: {}", line.trim()),
                    updated_at: time::OffsetDateTime::now_utc(),
                },
            )
            .with_project_id(completion.project_id.clone())
            .with_task_id(completion.task_id.clone())
            .sign(actor_key)?;
            runtime.record_local_task_progress(&progress)?;
            runtime.queue_outgoing(&progress)?;
        }

        // Send finish progress
        let progress_finished = UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(completion.delegator_actor_id.clone()),
            TaskProgress {
                progress: 0.95,
                message: if matches!(completion.task_status, TaskExecutionStatus::Failed) {
                    "task execution failed".to_owned()
                } else {
                    "task execution finished".to_owned()
                },
                updated_at: time::OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(completion.project_id.clone())
        .with_task_id(completion.task_id.clone())
        .sign(actor_key)?;
        runtime.record_local_task_progress(&progress_finished)?;
        runtime.queue_outgoing(&progress_finished)?;

        // Send task result
        let result = UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(completion.delegator_actor_id),
            TaskResultSubmitted {
                status: completion.task_status,
                summary: completion.bridge_response.summary,
                output_payload: completion.bridge_response.output_payload,
                artifact_refs,
                started_at: time::OffsetDateTime::now_utc(),
                finished_at: time::OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(completion.project_id)
        .with_task_id(completion.task_id)
        .sign(actor_key)?;

        runtime.record_local_task_result_submitted(&result)?;
        runtime.queue_outgoing(&result)?;

        for ready in ready_stop_completions {
            emit_worker_stop_complete(store, &runtime, &local_identity.actor_id, actor_key, ready)?;
        }
    }

    Ok(())
}

pub(crate) fn enforce_task_output_schema(
    store: &Store,
    task_id: &TaskId,
    completion: &mut TaskCompletion,
) -> Result<()> {
    if !matches!(completion.task_status, TaskExecutionStatus::Completed) {
        return Ok(());
    }
    let Some(task_blueprint) = store.task_blueprint(task_id)? else {
        return Ok(());
    };

    let validation_errors = collect_json_schema_validation_errors(
        &task_blueprint.expected_output_schema,
        &completion.bridge_response.output_payload,
    );
    if validation_errors.is_empty() {
        return Ok(());
    }

    let original_summary = completion.bridge_response.summary.clone();
    let original_output = completion.bridge_response.output_payload.clone();
    let schema = task_blueprint.expected_output_schema;
    let validation_summary = format!("invalid output schema: {}", validation_errors.join(" | "));

    completion.task_status = TaskExecutionStatus::Failed;
    completion.bridge_response.summary = "invalid output schema".to_owned();
    completion.bridge_response.output_payload = serde_json::json!({
        "bridge_error": "invalid output schema",
        "validation_errors": validation_errors,
        "expected_output_schema": schema,
        "original_summary": original_summary,
        "worker_output": original_output,
    });
    completion.bridge_response.raw_stderr =
        append_stderr_line(&completion.bridge_response.raw_stderr, &validation_summary);
    Ok(())
}

pub(crate) fn append_stderr_line(stderr: &str, line: &str) -> String {
    if stderr.trim().is_empty() {
        line.to_owned()
    } else {
        format!("{stderr}\n{line}")
    }
}

pub(crate) fn collect_json_schema_validation_errors(
    schema: &Value,
    payload: &Value,
) -> Vec<String> {
    let mut errors = Vec::new();
    validate_json_schema_node(schema, payload, "$", &mut errors);
    errors
}

pub(crate) fn validate_json_schema_node(
    schema: &Value,
    payload: &Value,
    path: &str,
    errors: &mut Vec<String>,
) {
    let Some(schema_object) = schema.as_object() else {
        return;
    };
    if schema_object.is_empty() {
        return;
    }

    if let Some(expected_type) = schema_object.get("type").and_then(Value::as_str) {
        if !json_schema_type_matches(expected_type, payload) {
            errors.push(format!(
                "{path}: expected {expected_type}, got {}",
                json_value_type_name(payload)
            ));
            return;
        }
    }

    if let Some(required) = schema_object.get("required").and_then(Value::as_array) {
        if let Some(object) = payload.as_object() {
            for key in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(key) {
                    errors.push(format!("{path}: missing required property `{key}`"));
                }
            }
        } else if !required.is_empty() {
            errors.push(format!(
                "{path}: required properties are declared but payload is {}",
                json_value_type_name(payload)
            ));
        }
    }

    if let Some(properties) = schema_object.get("properties").and_then(Value::as_object) {
        if let Some(object) = payload.as_object() {
            for (key, property_schema) in properties {
                if let Some(value) = object.get(key) {
                    validate_json_schema_node(
                        property_schema,
                        value,
                        &format!("{path}.{key}"),
                        errors,
                    );
                }
            }
        }
    }

    if let Some(items_schema) = schema_object.get("items") {
        if let Some(items) = payload.as_array() {
            for (index, item) in items.iter().enumerate() {
                validate_json_schema_node(items_schema, item, &format!("{path}[{index}]"), errors);
            }
        }
    }
}

pub(crate) fn json_schema_type_matches(expected_type: &str, payload: &Value) -> bool {
    match expected_type {
        "object" => payload.is_object(),
        "array" => payload.is_array(),
        "string" => payload.is_string(),
        "boolean" => payload.is_boolean(),
        "number" => payload.is_number(),
        "integer" => {
            payload.as_i64().is_some()
                || payload.as_u64().is_some()
                || payload
                    .as_f64()
                    .is_some_and(|value| value.fract().abs() < f64::EPSILON)
        }
        "null" => payload.is_null(),
        _ => true,
    }
}

pub(crate) fn json_value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub(crate) fn handle_owner_task_result(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<TaskResultSubmitted>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    runtime.ingest_task_result_submitted(&envelope)?;
    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let task_id = envelope
        .task_id
        .clone()
        .ok_or_else(|| anyhow!("task_id is required"))?;
    let retry_attempt = store.task_retry_attempt(&task_id)?;
    let task_blueprint = store.task_blueprint(&task_id)?;
    let is_planner_task = task_blueprint
        .as_ref()
        .is_some_and(|task| decision::is_planner_task(config, task));

    let owner_ctx = OwnerContext {
        store,
        actor_key,
        owner_actor_id: &local_identity.actor_id,
        project_id: &project_id,
    };

    if matches!(
        envelope.body.status,
        TaskExecutionStatus::Failed | TaskExecutionStatus::Stopped
    ) {
        let failed_attempts = retry_attempt + 1;
        let mut failure_action =
            classify_task_failure_action_with_policy(&envelope, &config.owner, failed_attempts);

        match &failure_action {
            FailureAction::RetrySameWorker { .. } => {
                let eval_ctx = decision::EvaluationContext {
                    local_identity,
                    actor_key,
                    store,
                    envelope: &envelope,
                    retry_attempt,
                    failure_action: Some(&failure_action),
                };
                let evaluation = build_task_evaluation(config, &eval_ctx)?;
                runtime.ingest_evaluation_issued(&evaluation)?;
                runtime.queue_outgoing(&evaluation)?;
                queue_retry_task(&owner_ctx, &task_id)?;
                apply_owner_retry_cooldown(config);
                dispatch_next_task_offer(&owner_ctx, envelope.from_actor_id.clone())?;
                return Ok(());
            }
            FailureAction::RetryDifferentWorker { .. } => {
                let principal_actor_id = store
                    .project_principal_actor_id(&project_id)?
                    .ok_or_else(|| {
                        anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません")
                    })?;
                if let Some(next_worker_actor_id) = select_next_worker_actor_id(
                    store,
                    &project_id,
                    &local_identity.actor_id,
                    &principal_actor_id,
                    &[envelope.from_actor_id.clone()],
                )? {
                    let eval_ctx = decision::EvaluationContext {
                        local_identity,
                        actor_key,
                        store,
                        envelope: &envelope,
                        retry_attempt,
                        failure_action: Some(&failure_action),
                    };
                    let evaluation = build_task_evaluation(config, &eval_ctx)?;
                    runtime.ingest_evaluation_issued(&evaluation)?;
                    runtime.queue_outgoing(&evaluation)?;
                    queue_retry_task(&owner_ctx, &task_id)?;
                    apply_owner_retry_cooldown(config);
                    dispatch_next_task_offer(&owner_ctx, next_worker_actor_id)?;
                    return Ok(());
                }
                failure_action = FailureAction::NoRetry {
                    reason: "no alternate worker available".to_owned(),
                };
            }
            FailureAction::NoRetry { .. } => {}
        }

        let eval_ctx = decision::EvaluationContext {
            local_identity,
            actor_key,
            store,
            envelope: &envelope,
            retry_attempt,
            failure_action: Some(&failure_action),
        };
        let evaluation = build_task_evaluation(config, &eval_ctx)?;
        runtime.ingest_evaluation_issued(&evaluation)?;
        runtime.queue_outgoing(&evaluation)?;
        if is_planner_task
            && matches!(failure_action, FailureAction::NoRetry { .. })
            && config.observation.planner_fallback_to_heuristic
        {
            let planner_task = task_blueprint.as_ref().ok_or_else(|| {
                anyhow!("[E_TASK_NOT_FOUND] planner task blueprint が見つかりません")
            })?;
            materialize_planner_child_tasks(
                &owner_ctx,
                &task_id,
                decision::fallback_tasks_for_worker_planner(config, planner_task)?,
            )?;
        }
        // Cascade failure to dependent tasks when no retry is possible
        if matches!(failure_action, FailureAction::NoRetry { .. }) {
            cascade_dependency_failure(&owner_ctx, &task_id)?;
        }
        if matches!(envelope.body.status, TaskExecutionStatus::Failed)
            && store
                .next_queued_task_for_actor(&project_id, &local_identity.actor_id)?
                .is_some()
        {
            let principal_actor_id = store
                .project_principal_actor_id(&project_id)?
                .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;
            if let Some(next_worker_actor_id) = select_next_worker_actor_id(
                store,
                &project_id,
                &local_identity.actor_id,
                &principal_actor_id,
                &[],
            )? {
                dispatch_next_task_offer(&owner_ctx, next_worker_actor_id)?;
            }
        }
        return Ok(());
    }

    let eval_ctx = decision::EvaluationContext {
        local_identity,
        actor_key,
        store,
        envelope: &envelope,
        retry_attempt,
        failure_action: None,
    };
    let evaluation = build_task_evaluation(config, &eval_ctx)?;
    runtime.ingest_evaluation_issued(&evaluation)?;
    runtime.queue_outgoing(&evaluation)?;

    if is_planner_task {
        let planner_task = task_blueprint
            .as_ref()
            .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] planner task blueprint が見つかりません"))?;
        materialize_planner_child_tasks(
            &owner_ctx,
            &task_id,
            decision::planned_tasks_from_worker_result(
                config,
                planner_task,
                &envelope.body.output_payload,
            )?,
        )?;
    }

    let principal_actor_id = store
        .project_principal_actor_id(&project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;
    if store
        .next_queued_task_for_actor(&project_id, &local_identity.actor_id)?
        .is_some()
    {
        if let Some(next_worker_actor_id) = select_next_worker_actor_id(
            store,
            &project_id,
            &local_identity.actor_id,
            &principal_actor_id,
            &[],
        )? {
            dispatch_next_task_offer(&owner_ctx, next_worker_actor_id)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Failure classification / evaluation / join / capabilities / stop
// ---------------------------------------------------------------------------

pub(crate) fn classify_task_failure_action_with_policy(
    envelope: &Envelope<TaskResultSubmitted>,
    owner: &OwnerSection,
    failed_attempts: u64,
) -> FailureAction {
    decision::classify_task_failure_action_with_policy(envelope, owner, failed_attempts)
}

pub(crate) fn build_task_evaluation(
    config: &Config,
    ctx: &decision::EvaluationContext<'_>,
) -> Result<Envelope<EvaluationIssued>> {
    decision::build_task_evaluation(config, ctx)
}

pub(crate) fn handle_worker_join_offer(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<JoinOffer>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    if !runtime.ingest_verified(&envelope)? {
        return Ok(());
    }

    let required = envelope.body.required_capabilities;
    let (accepted, reject_reason) =
        evaluate_worker_join_offer(config, store, &local_identity.actor_id, &required)?;
    if accepted {
        let response = UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(envelope.from_actor_id),
            JoinAccept {
                accepted: true,
                capabilities_confirmed: worker_supported_capabilities(config),
            },
        )
        .with_project_id(
            envelope
                .project_id
                .clone()
                .ok_or_else(|| anyhow!("project_id is required"))?,
        )
        .sign(actor_key)?;
        runtime.queue_outgoing(&response)?;
    } else {
        let response = UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(envelope.from_actor_id),
            JoinReject {
                accepted: false,
                reason: reject_reason.unwrap_or_else(|| "capability mismatch".to_owned()),
            },
        )
        .with_project_id(
            envelope
                .project_id
                .clone()
                .ok_or_else(|| anyhow!("project_id is required"))?,
        )
        .sign(actor_key)?;
        runtime.queue_outgoing(&response)?;
    }

    runtime.mark_inbox_message_processed(&envelope.msg_id)?;

    Ok(())
}

fn process_mdns_discoveries(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
    actor_key: Option<&StoredKeypair>,
) -> Result<()> {
    let peers = transport.discovered_peers();
    if peers.is_empty() {
        return Ok(());
    }
    let Some(actor_key) = actor_key else {
        return Ok(());
    };
    let Some(identity) = store.local_identity()? else {
        return Ok(());
    };
    let listen_addresses = local_listen_addresses(topology, transport);
    let stop_public_key = local_stop_public_key_helper(config, paths);
    let capabilities = local_advertised_capabilities(config);
    let runtime = RuntimePipeline::new(store);
    for peer in peers {
        let actor_id = ActorId::new(format!("mdns_{}", peer.peer_id))?;
        let node_id = NodeId::new(format!("mdns_{}", peer.peer_id))?;
        for address in &peer.addresses {
            let full_addr = format!("{}/p2p/{}", address, peer.peer_id);
            store.add_peer_address(&PeerAddressRecord {
                actor_id: actor_id.clone(),
                node_id: node_id.clone(),
                multiaddr: full_addr,
                last_seen_at: Some(OffsetDateTime::now_utc()),
            })?;
        }
        let query = UnsignedEnvelope::new(
            identity.actor_id.clone(),
            Some(actor_id),
            CapabilityQuery {
                node_id: identity.node_id.clone(),
                public_key: identity.public_key.clone(),
                stop_public_key: stop_public_key.clone(),
                capabilities: capabilities.clone(),
                listen_addresses: listen_addresses.clone(),
                requested_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(actor_key)?;
        runtime.queue_outgoing(&query)?;
    }
    Ok(())
}

pub(crate) fn request_peer_capabilities(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
    actor_key: Option<&StoredKeypair>,
) -> Result<()> {
    let Some(actor_key) = actor_key else {
        return Ok(());
    };
    let Some(identity) = store.local_identity()? else {
        return Ok(());
    };
    let listen_addresses = local_listen_addresses(topology, transport);
    let stop_public_key = local_stop_public_key_helper(config, paths);
    let capabilities = local_advertised_capabilities(config);
    let runtime = RuntimePipeline::new(store);
    let mut seen_actor_ids = HashSet::new();
    for peer in store.list_peer_addresses()? {
        if !seen_actor_ids.insert(peer.actor_id.to_string()) {
            continue;
        }
        if peer.actor_id == identity.actor_id {
            continue;
        }
        let query = UnsignedEnvelope::new(
            identity.actor_id.clone(),
            Some(peer.actor_id),
            CapabilityQuery {
                node_id: identity.node_id.clone(),
                public_key: identity.public_key.clone(),
                stop_public_key: stop_public_key.clone(),
                capabilities: capabilities.clone(),
                listen_addresses: listen_addresses.clone(),
                requested_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(actor_key)?;
        runtime.queue_outgoing(&query)?;
    }
    Ok(())
}

pub(crate) fn announce_local_capabilities(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
    actor_key: Option<&StoredKeypair>,
) -> Result<()> {
    let Some(actor_key) = actor_key else {
        return Ok(());
    };
    let capabilities = local_advertised_capabilities(config);
    if capabilities.is_empty() {
        return Ok(());
    }
    let Some(identity) = store.local_identity()? else {
        return Ok(());
    };
    let listen_addresses = local_listen_addresses(topology, transport);
    let stop_public_key = local_stop_public_key_helper(config, paths);
    let runtime = RuntimePipeline::new(store);
    let mut seen_actor_ids = HashSet::new();
    for peer in store.list_peer_addresses()? {
        if !seen_actor_ids.insert(peer.actor_id.to_string()) {
            continue;
        }
        if peer.actor_id == identity.actor_id {
            continue;
        }
        let advertisement = UnsignedEnvelope::new(
            identity.actor_id.clone(),
            Some(peer.actor_id),
            CapabilityAdvertisement {
                node_id: identity.node_id.clone(),
                public_key: identity.public_key.clone(),
                stop_public_key: stop_public_key.clone(),
                capabilities: capabilities.clone(),
                listen_addresses: listen_addresses.clone(),
                advertised_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(actor_key)?;
        runtime.queue_outgoing(&advertisement)?;
    }
    Ok(())
}

pub(crate) fn handle_capability_advertisement(
    store: &Store,
    envelope: Envelope<CapabilityAdvertisement>,
) -> Result<()> {
    store.save_inbox_message(&envelope)?;
    if store.inbox_message_processed(&envelope.msg_id)? {
        return Ok(());
    }
    upsert_bootstrap_peer(BootstrapPeerParams {
        store,
        actor_id: &envelope.from_actor_id,
        node_id: envelope.body.node_id,
        public_key: envelope.body.public_key,
        stop_public_key: envelope.body.stop_public_key,
        capabilities: envelope.body.capabilities,
        listen_addresses: &envelope.body.listen_addresses,
        seen_at: envelope.body.advertised_at,
    })?;
    store.mark_inbox_message_processed(&envelope.msg_id)?;
    Ok(())
}

pub(crate) fn handle_capability_query(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<CapabilityQuery>,
) -> Result<()> {
    store.save_inbox_message(&envelope)?;
    if store.inbox_message_processed(&envelope.msg_id)? {
        return Ok(());
    }
    upsert_bootstrap_peer(BootstrapPeerParams {
        store,
        actor_id: &envelope.from_actor_id,
        node_id: envelope.body.node_id,
        public_key: envelope.body.public_key,
        stop_public_key: envelope.body.stop_public_key,
        capabilities: envelope.body.capabilities,
        listen_addresses: &envelope.body.listen_addresses,
        seen_at: envelope.body.requested_at,
    })?;
    let Some(actor_key) = actor_key else {
        store.mark_inbox_message_processed(&envelope.msg_id)?;
        return Ok(());
    };
    let capabilities = local_advertised_capabilities(config);
    let Some(identity) = store.local_identity()? else {
        store.mark_inbox_message_processed(&envelope.msg_id)?;
        return Ok(());
    };
    let listen_addresses = local_listen_addresses(topology, transport);
    let stop_public_key = local_stop_public_key_helper(config, paths);
    let advertisement = UnsignedEnvelope::new(
        identity.actor_id,
        Some(envelope.from_actor_id),
        CapabilityAdvertisement {
            node_id: identity.node_id.clone(),
            public_key: identity.public_key.clone(),
            stop_public_key,
            capabilities,
            listen_addresses,
            advertised_at: OffsetDateTime::now_utc(),
        },
    )
    .sign(actor_key)?;
    RuntimePipeline::new(store).queue_outgoing(&advertisement)?;
    store.mark_inbox_message_processed(&envelope.msg_id)?;
    Ok(())
}

pub(crate) fn handle_owner_approval_granted(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<ApprovalGranted>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    if !runtime.ingest_verified(&envelope)? {
        return Ok(());
    }

    let (state, project_id, task_id) = match envelope.body.scope_type {
        ApprovalScopeType::Project => {
            let project_id = envelope
                .project_id
                .clone()
                .unwrap_or(ProjectId::new(envelope.body.scope_id.clone())?);
            (
                approve_execution_scope(store, local_identity, actor_key, &project_id, None)?,
                Some(project_id),
                None,
            )
        }
        ApprovalScopeType::Task => {
            let task_id = envelope
                .task_id
                .clone()
                .unwrap_or(TaskId::new(envelope.body.scope_id.clone())?);
            let snapshot = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            (
                approve_execution_scope(
                    store,
                    local_identity,
                    actor_key,
                    &snapshot.project_id,
                    Some(&task_id),
                )?,
                Some(snapshot.project_id),
                Some(task_id),
            )
        }
    };

    let applied = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        ApprovalApplied {
            scope_type: envelope.body.scope_type,
            scope_id: envelope.body.scope_id.clone(),
            approval_granted_msg_id: envelope.msg_id.clone(),
            approval_updated: state.approval_updated,
            resumed_task_ids: state.resumed_task_ids.clone(),
            dispatched: state.dispatched,
            applied_at: OffsetDateTime::now_utc(),
        },
    );
    let applied = match (project_id, task_id) {
        (Some(project_id), Some(task_id)) => {
            applied.with_project_id(project_id).with_task_id(task_id)
        }
        (Some(project_id), None) => applied.with_project_id(project_id),
        (None, Some(task_id)) => applied.with_task_id(task_id),
        (None, None) => applied,
    }
    .sign(actor_key)?;
    runtime.record_local_approval_applied(&applied)?;
    runtime.queue_outgoing(&applied)?;

    runtime.mark_inbox_message_processed(&envelope.msg_id)?;
    Ok(())
}

pub(crate) fn evaluate_worker_join_offer(
    config: &Config,
    store: &Store,
    actor_id: &ActorId,
    required_capabilities: &[String],
) -> Result<(bool, Option<String>)> {
    if !config.worker.accept_join_offers {
        return Ok((false, Some("worker policy disabled".to_owned())));
    }
    if !config.openclaw.enabled {
        return Ok((false, Some("openclaw disabled".to_owned())));
    }
    let supported_capabilities = worker_supported_capabilities(config);
    if !required_capabilities.iter().any(|capability| {
        supported_capabilities
            .iter()
            .any(|supported| supported == capability)
    }) {
        return Ok((false, Some("capability mismatch".to_owned())));
    }
    let active_tasks = store.active_task_count_for_actor(actor_id)?;
    if active_tasks >= config.worker.max_active_tasks {
        return Ok((
            false,
            Some(format!(
                "worker overloaded: active_tasks={} max_active_tasks={}",
                active_tasks, config.worker.max_active_tasks
            )),
        ));
    }

    Ok((true, None))
}

pub(crate) fn apply_owner_retry_cooldown(_config: &Config) {
    // Intentionally no-op: the run_node_once 200ms sleep provides natural
    // retry cooldown without blocking the main event loop.
}

pub(crate) fn handle_owner_join_accept(
    _config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<JoinAccept>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    if !runtime.ingest_verified(&envelope)? {
        return Ok(());
    }

    if !envelope.body.accepted {
        runtime.mark_inbox_message_processed(&envelope.msg_id)?;
        return Ok(());
    }

    if let Some(mut peer_identity) = store.peer_identity(&envelope.from_actor_id)? {
        peer_identity.capabilities = envelope.body.capabilities_confirmed.clone();
        peer_identity.updated_at = time::OffsetDateTime::now_utc();
        store.upsert_peer_identity(&peer_identity)?;
    }

    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let Some(task_id) = store.next_queued_task_for_actor(&project_id, &local_identity.actor_id)?
    else {
        runtime.mark_inbox_message_processed(&envelope.msg_id)?;
        return Ok(());
    };
    let task_blueprint = store
        .task_blueprint(&task_id)?
        .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task blueprint が見つかりません"))?;
    let constraints = project_vision_constraints_or_default(store, &project_id)?;
    if let Some((policy_code, summary)) = policy_blocking_reason(&constraints) {
        record_owner_policy_blocked_task_result(&PolicyBlockedTaskParams {
            store,
            actor_key,
            owner_actor_id: &local_identity.actor_id,
            project_id: &project_id,
            task_id: &task_id,
            required_capability: &task_blueprint.required_capability,
            constraints: &constraints,
            policy_code,
            summary,
        })?;
        runtime.mark_inbox_message_processed(&envelope.msg_id)?;
        return Ok(());
    }

    let task = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id),
        task_blueprint,
    )
    .with_project_id(project_id)
    .with_task_id(task_id)
    .sign(actor_key)?;
    runtime.record_local_task_delegated(&task)?;
    runtime.queue_outgoing(&task)?;
    runtime.mark_inbox_message_processed(&envelope.msg_id)?;
    Ok(())
}

pub(crate) fn handle_owner_join_reject(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<JoinReject>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    if !runtime.ingest_verified(&envelope)? {
        return Ok(());
    }

    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let principal_actor_id = store
        .project_principal_actor_id(&project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;

    if let Some(next_worker_actor_id) = select_next_worker_actor_id(
        store,
        &project_id,
        &local_identity.actor_id,
        &principal_actor_id,
        &[envelope.from_actor_id.clone()],
    )? {
        apply_owner_retry_cooldown(config);
        let owner_ctx = OwnerContext {
            store,
            actor_key,
            owner_actor_id: &local_identity.actor_id,
            project_id: &project_id,
        };
        dispatch_next_task_offer(&owner_ctx, next_worker_actor_id)?;
    }

    runtime.mark_inbox_message_processed(&envelope.msg_id)?;

    Ok(())
}

pub(crate) fn dispatch_next_task_offer(
    ctx: &OwnerContext<'_>,
    worker_actor_id: ActorId,
) -> Result<bool> {
    let constraints = project_vision_constraints_or_default(ctx.store, ctx.project_id)?;
    if budget_mode_is_minimal(&constraints)
        && ctx
            .store
            .project_snapshot(ctx.project_id)?
            .map(|snapshot| {
                snapshot.task_counts.accepted
                    + snapshot.task_counts.running
                    + snapshot.task_counts.stopping
                    > 0
            })
            .unwrap_or(false)
    {
        return Ok(false);
    }

    loop {
        let Some(task_id) = ctx
            .store
            .next_queued_task_for_actor(ctx.project_id, ctx.owner_actor_id)?
        else {
            return Ok(false);
        };
        let task_blueprint = ctx
            .store
            .task_blueprint(&task_id)?
            .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task blueprint が見つかりません"))?;

        if let Some((policy_code, summary)) = policy_blocking_reason(&constraints) {
            record_owner_policy_blocked_task_result(&PolicyBlockedTaskParams {
                store: ctx.store,
                actor_key: ctx.actor_key,
                owner_actor_id: ctx.owner_actor_id,
                project_id: ctx.project_id,
                task_id: &task_id,
                required_capability: &task_blueprint.required_capability,
                constraints: &constraints,
                policy_code,
                summary,
            })?;
            continue;
        }

        let runtime = RuntimePipeline::new(ctx.store);
        let join_offer = UnsignedEnvelope::new(
            ctx.owner_actor_id.clone(),
            Some(worker_actor_id.clone()),
            JoinOffer {
                required_capabilities: vec![task_blueprint.required_capability.clone()],
                task_outline: task_blueprint.title,
                expected_duration_sec: estimate_task_duration_sec(&task_blueprint.objective),
            },
        )
        .with_project_id(ctx.project_id.clone())
        .sign(ctx.actor_key)?;
        ctx.store.append_task_event(&join_offer)?;
        runtime.queue_outgoing(&join_offer)?;
        return Ok(true);
    }
}

pub(crate) fn select_next_worker_actor_id(
    store: &Store,
    project_id: &starweft_id::ProjectId,
    owner_actor_id: &ActorId,
    principal_actor_id: &ActorId,
    excluded_actor_ids: &[ActorId],
) -> Result<Option<ActorId>> {
    let mut excluded = excluded_actor_ids.to_vec();
    excluded.push(owner_actor_id.clone());
    excluded.push(principal_actor_id.clone());

    let required_capability = store
        .next_queued_task_for_actor(project_id, owner_actor_id)?
        .map(|task_id| {
            store
                .task_blueprint(&task_id)?
                .map(|task| task.required_capability)
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task blueprint が見つかりません"))
        })
        .transpose()?;
    let mut seen_actor_ids = HashSet::new();
    let mut candidates = Vec::new();
    for peer in store.list_peer_addresses()? {
        if excluded.iter().any(|actor_id| actor_id == &peer.actor_id) {
            continue;
        }
        if !seen_actor_ids.insert(peer.actor_id.to_string()) {
            continue;
        }
        let capability_rank = match &required_capability {
            Some(required) => match store.peer_identity(&peer.actor_id)? {
                Some(identity) if !identity.capabilities.is_empty() => {
                    if identity
                        .capabilities
                        .iter()
                        .any(|capability| capability == required)
                    {
                        0_u8
                    } else {
                        2_u8
                    }
                }
                _ => 1_u8,
            },
            None => 0_u8,
        };
        candidates.push((
            capability_rank,
            store.active_task_count_for_actor(&peer.actor_id)?,
            peer.actor_id,
        ));
    }
    candidates.sort_by(
        |(left_rank, left_count, left_actor), (right_rank, right_count, right_actor)| {
            left_rank
                .cmp(right_rank)
                .then_with(|| left_count.cmp(right_count))
                .then_with(|| left_actor.as_str().cmp(right_actor.as_str()))
        },
    );
    Ok(candidates
        .into_iter()
        .find(|(rank, _, _)| *rank < 2)
        .map(|(_, _, actor_id)| actor_id))
}

pub(crate) fn handle_owner_snapshot_request(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<SnapshotRequest>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    if !runtime.ingest_verified(&envelope)? {
        return Ok(());
    }

    let (project_id, task_id, snapshot_value) = match envelope.body.scope_type {
        SnapshotScopeType::Project => {
            let project_id = starweft_id::ProjectId::new(envelope.body.scope_id.clone())?;
            let snapshot = store
                .project_snapshot(&project_id)?
                .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] project が見つかりません"))?;
            (Some(project_id), None, serde_json::to_value(snapshot)?)
        }
        SnapshotScopeType::Task => {
            let task_id = starweft_id::TaskId::new(envelope.body.scope_id.clone())?;
            let snapshot = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            (
                Some(snapshot.project_id.clone()),
                Some(task_id),
                serde_json::to_value(snapshot)?,
            )
        }
    };

    let mut response = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id),
        SnapshotResponse {
            scope_type: envelope.body.scope_type,
            scope_id: envelope.body.scope_id,
            snapshot: snapshot_value,
        },
    );
    response.project_id = project_id;
    response.task_id = task_id;
    let response = response.sign(actor_key)?;
    runtime.queue_outgoing(&response)?;
    runtime.mark_inbox_message_processed(&envelope.msg_id)?;
    Ok(())
}

pub(crate) fn relay_stop_ack_to_principal(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: &Envelope<StopAck>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let project_id = envelope
        .project_id
        .as_ref()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let principal_actor_id = store
        .project_principal_actor_id(project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;

    let forwarded = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(principal_actor_id),
        envelope.body.clone(),
    )
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    RuntimePipeline::new(store).queue_outgoing(&forwarded)?;
    Ok(())
}

pub(crate) fn relay_stop_complete_to_principal(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: &Envelope<StopComplete>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let project_id = envelope
        .project_id
        .as_ref()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let principal_actor_id = store
        .project_principal_actor_id(project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;

    let forwarded = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(principal_actor_id),
        envelope.body.clone(),
    )
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    RuntimePipeline::new(store).queue_outgoing(&forwarded)?;
    Ok(())
}

pub(crate) fn queue_snapshot_request(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    scope_type: SnapshotScopeType,
    scope_id: &str,
    owner_actor_id: Option<String>,
) -> Result<()> {
    let identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let owner_actor_id =
        resolve_snapshot_owner_actor_id(store, &scope_type, scope_id, owner_actor_id.as_deref())?;
    let actor_key = read_keypair(&configured_actor_key_path(config, paths)?)?;
    let request = UnsignedEnvelope::new(
        identity.actor_id,
        Some(owner_actor_id),
        SnapshotRequest {
            scope_type,
            scope_id: scope_id.to_owned(),
        },
    )
    .sign(&actor_key)?;
    RuntimePipeline::new(store).queue_outgoing(&request)?;
    Ok(())
}

pub(crate) fn handle_owner_stop_order(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<StopOrder>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    runtime.ingest_stop_order(&envelope)?;
    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;

    let principal_actor_id = envelope.from_actor_id.clone();
    let ack = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(principal_actor_id.clone()),
        StopAck {
            stop_id: envelope.body.stop_id.clone(),
            actor_id: local_identity.actor_id.clone(),
            ack_state: StopAckState::Stopping,
            acked_at: time::OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    runtime.queue_outgoing(&ack)?;

    // Stop the owner's own queued/active tasks and collect assignees for forwarding.
    let now = time::OffsetDateTime::now_utc();
    let assignees = match envelope.body.scope_type {
        StopScopeType::Project => {
            store.stop_project_tasks_for_actor(&project_id, &local_identity.actor_id, now)?;
            store.project_assignee_actor_ids(&project_id)?
        }
        StopScopeType::TaskTree => {
            let root_task_id = TaskId::new(envelope.body.scope_id.clone())?;
            store.stop_task_tree_tasks_for_actor(
                &project_id,
                &root_task_id,
                &local_identity.actor_id,
                now,
            )?;
            store.task_tree_assignee_actor_ids(&project_id, &root_task_id)?
        }
    };
    let peers = store.list_peer_addresses()?;
    let mut forwarded_workers = 0_usize;
    for assignee in assignees
        .into_iter()
        .filter(|actor_id| actor_id != &local_identity.actor_id)
    {
        if peers.iter().any(|peer| peer.actor_id == assignee) {
            let forwarded = UnsignedEnvelope::new(
                local_identity.actor_id.clone(),
                Some(assignee),
                envelope.body.clone(),
            )
            .with_project_id(project_id.clone())
            .sign(actor_key)?;
            runtime.queue_outgoing(&forwarded)?;
            forwarded_workers += 1;
        }
    }

    if should_owner_emit_completion(forwarded_workers) {
        let complete = UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(principal_actor_id),
            StopComplete {
                stop_id: envelope.body.stop_id.clone(),
                actor_id: local_identity.actor_id.clone(),
                final_state: StopFinalState::Stopped,
                completed_at: time::OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id)
        .sign(actor_key)?;
        runtime.record_local_stop_complete(&complete)?;
        runtime.queue_outgoing(&complete)?;
    }
    Ok(())
}

pub(crate) fn handle_worker_stop_order(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<StopOrder>,
    worker_runtime: &WorkerRuntimeState,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    runtime.ingest_stop_order(&envelope)?;

    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let affected_task_ids = match envelope.body.scope_type {
        StopScopeType::Project => {
            store.active_project_task_ids_for_actor(&project_id, &local_identity.actor_id)?
        }
        StopScopeType::TaskTree => {
            let root_task_id = TaskId::new(envelope.body.scope_id.clone())?;
            store.active_task_tree_task_ids_for_actor(
                &project_id,
                &root_task_id,
                &local_identity.actor_id,
            )?
        }
    };
    let running_task_ids = worker_runtime.cancel_tasks(&affected_task_ids);
    let stop_impact = classify_stop_impact(&affected_task_ids, &running_task_ids);
    let _ = store.stop_tasks(
        &stop_impact.immediately_stopped_task_ids,
        time::OffsetDateTime::now_utc(),
    )?;

    let ack = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        StopAck {
            stop_id: envelope.body.stop_id.clone(),
            actor_id: local_identity.actor_id.clone(),
            ack_state: StopAckState::Stopping,
            acked_at: time::OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    runtime.queue_outgoing(&ack)?;

    if stop_impact.completion_ready() {
        emit_worker_stop_complete(
            store,
            &runtime,
            &local_identity.actor_id,
            actor_key,
            ReadyStopCompletion {
                stop_id: envelope.body.stop_id.clone(),
                project_id,
                notify_actor_id: envelope.from_actor_id.clone(),
            },
        )?;
    } else {
        worker_runtime.stage_stop(
            &envelope.body.stop_id,
            &project_id,
            &envelope.from_actor_id,
            &stop_impact.running_task_ids,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        Config, DataPaths, NodeRole, OwnerRetryAction, OwnerRetryRule, OwnerSection,
    };
    use crate::decision::FailureAction;
    use crate::helpers::submission_is_approved;
    use starweft_crypto::StoredKeypair;
    use starweft_id::{ActorId, NodeId, ProjectId, TaskId, VisionId};
    use starweft_protocol::{
        ApprovalGranted, ApprovalScopeType, EvaluationPolicy, ParticipantPolicy, ProjectCharter,
        TaskDelegated, UnsignedEnvelope,
    };
    use starweft_store::{DeliveryState, PeerAddressRecord, PeerIdentityRecord, Store};
    use std::path::Path;
    use tempfile::TempDir;
    use time::OffsetDateTime;

    #[test]
    fn collect_json_schema_validation_errors_reports_nested_mismatches() {
        let errors = collect_json_schema_validation_errors(
            &serde_json::json!({
                "type": "object",
                "required": ["summary", "deliverables"],
                "properties": {
                    "summary": { "type": "string" },
                    "deliverables": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["title"],
                            "properties": {
                                "title": { "type": "string" }
                            }
                        }
                    }
                }
            }),
            &serde_json::json!({
                "summary": 42,
                "deliverables": [{ "title": 7 }, {}]
            }),
        );

        assert!(errors.iter().any(|error| error.contains("$.summary")));
        assert!(
            errors
                .iter()
                .any(|error| error.contains("$.deliverables[0].title"))
        );
        assert!(errors
            .iter()
            .any(|error| error.contains("$.deliverables[1]: missing required property `title`")));
    }

    #[test]
    fn worker_join_offer_rejects_when_active_tasks_reach_limit() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let actor_id = ActorId::generate();
        let owner_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let keypair = StoredKeypair::generate();
        let mut config = Config::for_role(NodeRole::Worker, temp.path(), None);
        config.openclaw.enabled = true;
        config.worker.max_active_tasks = 1;

        let delegated = UnsignedEnvelope::new(
            owner_actor,
            Some(actor_id.clone()),
            TaskDelegated {
                parent_task_id: None,
                depends_on: Vec::new(),
                title: "busy".to_owned(),
                description: "busy".to_owned(),
                objective: "busy".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
            },
        )
        .with_project_id(project_id)
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign task");
        store
            .apply_task_delegated(&delegated)
            .expect("apply delegated task");

        let (accepted, reason) = evaluate_worker_join_offer(
            &config,
            &store,
            &actor_id,
            &[String::from("openclaw.execution.v1")],
        )
        .expect("evaluate offer");

        assert!(!accepted);
        assert_eq!(
            reason.as_deref(),
            Some("worker overloaded: active_tasks=1 max_active_tasks=1")
        );
    }

    #[test]
    fn worker_join_offer_rejects_when_openclaw_is_disabled() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let actor_id = ActorId::generate();
        let config = Config::for_role(NodeRole::Worker, temp.path(), None);

        let (accepted, reason) = evaluate_worker_join_offer(
            &config,
            &store,
            &actor_id,
            &[config.compatibility.bridge_capability_version.clone()],
        )
        .expect("evaluate offer");

        assert!(!accepted);
        assert_eq!(reason.as_deref(), Some("openclaw disabled"));
    }

    #[test]
    fn select_next_worker_actor_id_prefers_matching_capability() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let keypair = StoredKeypair::generate();
        let owner_actor = ActorId::generate();
        let principal_actor = ActorId::generate();
        let planner_worker = ActorId::generate();
        let exec_worker = ActorId::generate();
        let project_id = ProjectId::generate();

        let delegated = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(owner_actor.clone()),
            TaskDelegated {
                parent_task_id: None,
                depends_on: Vec::new(),
                title: "planner".to_owned(),
                description: "planner".to_owned(),
                objective: "planner".to_owned(),
                required_capability: "openclaw.plan.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign task");
        store
            .apply_task_delegated(&delegated)
            .expect("apply delegated task");
        store
            .append_task_event(&delegated)
            .expect("append task event");

        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: planner_worker.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/planner.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("add planner peer");
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: exec_worker.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/exec.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("add exec peer");
        store
            .upsert_peer_identity(&PeerIdentityRecord {
                actor_id: planner_worker.clone(),
                node_id: NodeId::generate(),
                public_key: "planner-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.plan.v1".to_owned()],
                updated_at: OffsetDateTime::now_utc(),
            })
            .expect("upsert planner identity");
        store
            .upsert_peer_identity(&PeerIdentityRecord {
                actor_id: exec_worker.clone(),
                node_id: NodeId::generate(),
                public_key: "exec-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.execution.v1".to_owned()],
                updated_at: OffsetDateTime::now_utc(),
            })
            .expect("upsert exec identity");

        let selected =
            select_next_worker_actor_id(&store, &project_id, &owner_actor, &principal_actor, &[])
                .expect("select worker");
        assert_eq!(selected, Some(planner_worker));
    }

    #[test]
    fn dispatch_next_task_offer_fails_task_when_external_agents_are_disallowed() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let owner_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Policy Vision".to_owned(),
                raw_vision_text: "No external execution".to_owned(),
                constraints: serde_json::json!({
                    "allow_external_agents": false,
                }),
                status: "accepted".to_owned(),
                created_at: now,
            })
            .expect("save vision");

        let charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "Policy Project".to_owned(),
                objective: "No external execution".to_owned(),
                stop_authority_actor_id: principal_actor,
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: false,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");

        let owner_ctx = OwnerContext {
            store: &store,
            actor_key: &owner_key,
            owner_actor_id: &owner_actor,
            project_id: &project_id,
        };

        let task_id = queue_owner_planned_task(
            &owner_ctx,
            starweft_observation::PlannedTaskSpec {
                title: "blocked".to_owned(),
                description: "blocked".to_owned(),
                objective: "blocked".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
                depends_on_indices: Vec::new(),
            },
            None,
            Vec::new(),
        )
        .expect("queue task");

        let dispatched = dispatch_next_task_offer(&owner_ctx, worker_actor).expect("dispatch");
        assert!(!dispatched);

        let snapshot = store
            .task_snapshot(&task_id)
            .expect("snapshot")
            .expect("task exists");
        assert_eq!(snapshot.status, starweft_protocol::TaskStatus::Failed);
        assert_eq!(
            snapshot.result_summary.as_deref(),
            Some("unauthorized external agent by project policy")
        );
    }

    #[test]
    fn dispatch_next_task_offer_respects_minimal_budget_mode() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let owner_key = StoredKeypair::generate();
        let worker_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Minimal Budget".to_owned(),
                raw_vision_text: "Run sequentially".to_owned(),
                constraints: serde_json::json!({
                    "budget_mode": "minimal",
                }),
                status: "accepted".to_owned(),
                created_at: now,
            })
            .expect("save vision");

        let charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "Minimal Budget Project".to_owned(),
                objective: "Run sequentially".to_owned(),
                stop_authority_actor_id: principal_actor,
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");

        let active_task_id = TaskId::generate();
        let active_task = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(worker_actor.clone()),
            TaskDelegated {
                parent_task_id: None,
                depends_on: Vec::new(),
                title: "running".to_owned(),
                description: "running".to_owned(),
                objective: "running".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(active_task_id.clone())
        .sign(&owner_key)
        .expect("sign active task");
        store
            .apply_task_delegated(&active_task)
            .expect("apply active task");
        let active_progress = UnsignedEnvelope::new(
            worker_actor.clone(),
            Some(owner_actor.clone()),
            starweft_protocol::TaskProgress {
                progress: 0.4,
                message: "running".to_owned(),
                updated_at: now,
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(active_task_id)
        .sign(&worker_key)
        .expect("sign active progress");
        store
            .apply_task_progress(&active_progress)
            .expect("apply progress");

        let owner_ctx = OwnerContext {
            store: &store,
            actor_key: &owner_key,
            owner_actor_id: &owner_actor,
            project_id: &project_id,
        };

        let queued_task_id = queue_owner_planned_task(
            &owner_ctx,
            starweft_observation::PlannedTaskSpec {
                title: "queued".to_owned(),
                description: "queued".to_owned(),
                objective: "queued".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
                depends_on_indices: Vec::new(),
            },
            None,
            Vec::new(),
        )
        .expect("queue task");

        let dispatched = dispatch_next_task_offer(&owner_ctx, worker_actor).expect("dispatch");
        assert!(!dispatched);

        let snapshot = store
            .task_snapshot(&queued_task_id)
            .expect("snapshot")
            .expect("task exists");
        assert_eq!(snapshot.status, starweft_protocol::TaskStatus::Queued);
        assert!(snapshot.result_summary.is_none());
    }

    #[test]
    fn validate_runtime_compatibility_rejects_protocol_mismatch() {
        let mut config = Config::for_role(NodeRole::Owner, Path::new("/tmp/starweft"), None);
        config.compatibility.protocol_version = "starweft/9.9".to_owned();
        let error = validate_runtime_compatibility(&config).expect_err("mismatch should fail");
        assert!(error.to_string().contains("E_PROTOCOL_VERSION"));
    }

    #[test]
    fn validate_runtime_compatibility_rejects_schema_mismatch() {
        let mut config = Config::for_role(NodeRole::Owner, Path::new("/tmp/starweft"), None);
        config.compatibility.schema_version = "starweft-store/legacy".to_owned();
        let error =
            validate_runtime_compatibility(&config).expect_err("schema mismatch should fail");
        assert!(error.to_string().contains("E_SCHEMA_VERSION"));
    }

    #[test]
    fn ensure_wire_protocol_compatible_rejects_unexpected_protocol() {
        let config = Config::for_role(NodeRole::Owner, Path::new("/tmp/starweft"), None);
        let error =
            ensure_wire_protocol_compatible(&config, "starweft/legacy").expect_err("must fail");
        assert!(error.to_string().contains("E_PROTOCOL_VERSION"));
    }

    #[test]
    fn verify_and_decode_rejects_expired_message() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            ApprovalGranted {
                scope_type: ApprovalScopeType::Project,
                scope_id: "project_01".to_owned(),
                approved_at: OffsetDateTime::now_utc(),
            },
        )
        .with_expires_at(OffsetDateTime::now_utc() - time::Duration::minutes(1))
        .sign(&keypair)
        .expect("sign envelope");

        let error = verify_and_decode::<ApprovalGranted>(
            envelope.into_wire().expect("wire"),
            &keypair.public_key,
        )
        .expect_err("expired message should fail");
        assert!(error.to_string().contains("E_MESSAGE_EXPIRED"));
    }

    #[test]
    fn route_incoming_wire_skips_processed_duplicates_before_peer_lookup() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let config = Config::for_role(NodeRole::Owner, temp.path(), None);
        let topology = RuntimeTopology::default();
        let transport = RuntimeTransport::local_mailbox();
        let (task_completion_tx, _task_completion_rx) = mpsc::channel();
        let worker_runtime = WorkerRuntimeState::default();
        let paths = DataPaths::from_root(temp.path());
        let local_identity = None;
        let owner_key = StoredKeypair::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: ActorId::generate(),
                owner_actor_id: ActorId::generate(),
                title: "project".to_owned(),
                objective: "objective".to_owned(),
                stop_authority_actor_id: ActorId::generate(),
                participant_policy: ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 1.0,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_project_id(project_id)
        .with_vision_id(vision_id)
        .sign(&owner_key)
        .expect("sign project charter");
        store.save_inbox_message(&envelope).expect("save inbox");
        store
            .mark_inbox_message_processed(&envelope.msg_id)
            .expect("mark processed");

        route_incoming_wire(
            &IncomingWireContext {
                config: &config,
                paths: &paths,
                store: &store,
                topology: &topology,
                transport: &transport,
                local_identity: &local_identity,
                actor_key: None,
                task_completion_tx: &task_completion_tx,
                worker_runtime: &worker_runtime,
            },
            envelope.into_wire().expect("wire"),
        )
        .expect("duplicate should short-circuit");
    }

    #[test]
    fn relay_incoming_wire_queues_raw_wire_once_for_retryable_delivery() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let config = Config::for_role(NodeRole::Relay, temp.path(), None);
        let paths = DataPaths::from_root(temp.path());
        paths.ensure_layout().expect("layout");
        let transport = RuntimeTransport::local_mailbox();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            ApprovalGranted {
                scope_type: ApprovalScopeType::Project,
                scope_id: "project_01".to_owned(),
                approved_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(&StoredKeypair::generate())
        .expect("sign envelope");
        let wire = envelope.into_wire().expect("wire");

        relay_incoming_wire(&config, &paths, &store, &transport, &wire).expect("queue relay");
        relay_incoming_wire(&config, &paths, &store, &transport, &wire)
            .expect("duplicate relay should short-circuit");

        assert!(
            store
                .inbox_message_processed(&wire.msg_id)
                .expect("processed state"),
            "relay inbox should be marked processed after queuing"
        );
        let queued = store.queued_outbox_messages(10).expect("queued outbox");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].msg_id, wire.msg_id.to_string());
        assert_eq!(queued[0].msg_type, "ApprovalGranted");
    }

    #[test]
    fn classify_task_failure_action_retries_transient_failures() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Failed,
                summary: "bridge failed: openclaw process timed out".to_owned(),
                output_payload: serde_json::json!({
                    "bridge_error": "openclaw process timed out after 10 seconds"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        assert!(matches!(
            decision::classify_task_failure_action(&envelope, 1, 8),
            FailureAction::RetrySameWorker { .. }
        ));
    }

    #[test]
    fn classify_task_failure_action_retries_different_worker_on_process_failure() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Failed,
                summary: "bridge failed: openclaw process failed".to_owned(),
                output_payload: serde_json::json!({
                    "bridge_error": "openclaw process failed: status=1"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        assert!(matches!(
            decision::classify_task_failure_action(&envelope, 1, 8),
            FailureAction::RetryDifferentWorker { .. }
        ));
    }

    #[test]
    fn classify_task_failure_action_stops_on_permanent_failures() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Failed,
                summary: "invalid input schema".to_owned(),
                output_payload: serde_json::json!({
                    "bridge_error": "invalid input schema"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        assert!(matches!(
            decision::classify_task_failure_action(&envelope, 1, 8),
            FailureAction::NoRetry { .. }
        ));
    }

    #[test]
    fn classify_task_failure_action_stops_when_retry_limit_is_reached() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Failed,
                summary: "bridge failed: openclaw process timed out".to_owned(),
                output_payload: serde_json::json!({
                    "bridge_error": "openclaw process timed out after 10 seconds"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        assert!(matches!(
            decision::classify_task_failure_action(&envelope, 8, 8),
            FailureAction::NoRetry { .. }
        ));
    }

    #[test]
    fn classify_task_failure_action_uses_configured_retry_rules() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Failed,
                summary: "stderr from worker".to_owned(),
                output_payload: serde_json::json!({
                    "bridge_error": "stderr from worker"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        let owner = OwnerSection {
            retry_rules: vec![OwnerRetryRule {
                pattern: "stderr".to_owned(),
                action: OwnerRetryAction::NoRetry,
                reason: "configured override".to_owned(),
            }],
            ..OwnerSection::default()
        };

        assert!(matches!(
            classify_task_failure_action_with_policy(&envelope, &owner, 1),
            FailureAction::NoRetry { .. }
        ));
    }

    #[test]
    fn queue_remote_approval_queues_outbox_message_for_owner() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let principal_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let project_id = ProjectId::generate();

        let output = queue_remote_approval(RemoteApprovalParams {
            store: &store,
            actor_key: &principal_key,
            approver_actor_id: &principal_actor,
            owner_actor_id: owner_actor.clone(),
            scope_type: ApprovalScopeType::Project,
            scope_id: project_id.to_string(),
            project_id: Some(project_id.clone()),
            task_id: None,
        })
        .expect("queue remote approval");

        assert_eq!(output.scope_type, "project");
        assert_eq!(output.owner_actor_id, owner_actor.to_string());
        assert!(
            store
                .queued_outbox_messages(10)
                .expect("outbox")
                .iter()
                .any(|message| message.msg_type == "ApprovalGranted")
        );
    }

    #[test]
    fn flush_outbox_schedules_retry_for_unsupported_targets() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let config = Config::for_role(NodeRole::Owner, temp.path(), None);
        DataPaths::from_root(temp.path())
            .ensure_layout()
            .expect("layout");
        let owner_actor = ActorId::generate();
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: owner_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/ip4/127.0.0.1/tcp/9000".to_owned(),
                last_seen_at: None,
            })
            .expect("peer address");

        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(owner_actor),
            ApprovalGranted {
                scope_type: ApprovalScopeType::Project,
                scope_id: "project_01".to_owned(),
                approved_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(&StoredKeypair::generate())
        .expect("sign envelope");
        RuntimePipeline::new(&store)
            .queue_outgoing(&envelope)
            .expect("queue outbox");

        let paths = DataPaths::from_root(temp.path());
        flush_outbox(&config, &paths, &store, &RuntimeTransport::local_mailbox())
            .expect("flush outbox");

        let record = store
            .outbox_message(envelope.msg_id.as_str())
            .expect("outbox message")
            .expect("record exists");
        assert_eq!(
            record.delivery_state,
            starweft_store::DeliveryState::RetryWaiting
        );
        assert_eq!(record.delivery_attempts, 1);
        assert!(record.next_attempt_at.is_some());
        assert!(
            record
                .last_error
                .as_deref()
                .expect("last error")
                .contains("unsupported delivery target")
        );
    }

    #[test]
    fn flush_outbox_retries_only_failed_targets() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let config = Config::for_role(NodeRole::Owner, temp.path(), None);
        let paths = DataPaths::from_root(temp.path());
        paths.ensure_layout().expect("layout");
        let owner_actor = ActorId::generate();
        let success_mailbox = Path::new(&format!(
            "target-mailbox-{}.sock",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ))
        .to_path_buf();
        let success_addr = format!("/unix/{}", success_mailbox.display());
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: owner_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: success_addr.clone(),
                last_seen_at: None,
            })
            .expect("success peer");
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: owner_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/ip4/127.0.0.1/tcp/9000".to_owned(),
                last_seen_at: None,
            })
            .expect("retry peer");

        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(owner_actor),
            ApprovalGranted {
                scope_type: ApprovalScopeType::Project,
                scope_id: "project_01".to_owned(),
                approved_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(&StoredKeypair::generate())
        .expect("sign envelope");
        RuntimePipeline::new(&store)
            .queue_outgoing(&envelope)
            .expect("queue outbox");

        flush_outbox(&config, &paths, &store, &RuntimeTransport::local_mailbox())
            .expect("flush outbox");

        let deliveries = store
            .outbox_deliveries(envelope.msg_id.as_str())
            .expect("outbox deliveries");
        assert_eq!(deliveries.len(), 2);
        assert!(deliveries.iter().any(|record| {
            record.multiaddr == success_addr
                && record.delivery_state == DeliveryState::DeliveredLocal
        }));
        assert!(deliveries.iter().any(|record| {
            record.multiaddr == "/ip4/127.0.0.1/tcp/9000"
                && record.delivery_state == DeliveryState::RetryWaiting
        }));
        assert_eq!(
            std::fs::read_to_string(&success_mailbox)
                .expect("success mailbox")
                .lines()
                .count(),
            1
        );

        store.resume_pending_outbox().expect("resume outbox");
        flush_outbox(&config, &paths, &store, &RuntimeTransport::local_mailbox())
            .expect("flush resumed outbox");

        assert_eq!(
            std::fs::read_to_string(&success_mailbox)
                .expect("success mailbox")
                .lines()
                .count(),
            1
        );
        let _ = std::fs::remove_file(&success_mailbox);
    }

    #[test]
    fn handle_owner_approval_granted_resumes_blocked_project() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let principal_key = StoredKeypair::generate();
        let owner_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        let local_identity = starweft_store::LocalIdentityRecord {
            actor_id: owner_actor.clone(),
            node_id: NodeId::generate(),
            actor_type: "owner".to_owned(),
            display_name: "owner".to_owned(),
            public_key: owner_key.public_key.clone(),
            private_key_ref: "actor.key".to_owned(),
            created_at: now,
        };
        store
            .upsert_local_identity(&local_identity)
            .expect("local identity");
        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Remote Approval Vision".to_owned(),
                raw_vision_text: "Need approval".to_owned(),
                constraints: serde_json::json!({
                    "human_intervention": "required",
                }),
                status: "accepted".to_owned(),
                created_at: now,
            })
            .expect("save vision");
        let charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "Remote Approval Project".to_owned(),
                objective: "Need approval".to_owned(),
                stop_authority_actor_id: principal_actor.clone(),
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: worker_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/worker.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("peer address");
        store
            .upsert_peer_identity(&PeerIdentityRecord {
                actor_id: worker_actor,
                node_id: NodeId::generate(),
                public_key: "worker-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.execution.v1".to_owned()],
                updated_at: now,
            })
            .expect("peer identity");

        let owner_ctx = OwnerContext {
            store: &store,
            actor_key: &owner_key,
            owner_actor_id: &owner_actor,
            project_id: &project_id,
        };

        let blocked_task_id = queue_owner_planned_task(
            &owner_ctx,
            starweft_observation::PlannedTaskSpec {
                title: "remote-blocked".to_owned(),
                description: "remote-blocked".to_owned(),
                objective: "remote-blocked".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
                depends_on_indices: Vec::new(),
            },
            None,
            Vec::new(),
        )
        .expect("queue task");
        let dispatched =
            dispatch_next_task_offer(&owner_ctx, owner_actor.clone()).expect("dispatch");
        assert!(!dispatched);
        assert_eq!(
            store
                .task_snapshot(&blocked_task_id)
                .expect("snapshot")
                .expect("task")
                .status,
            starweft_protocol::TaskStatus::Failed
        );

        let approval = UnsignedEnvelope::new(
            principal_actor,
            Some(owner_actor),
            ApprovalGranted {
                scope_type: ApprovalScopeType::Project,
                scope_id: project_id.to_string(),
                approved_at: now,
            },
        )
        .with_project_id(project_id.clone())
        .sign(&principal_key)
        .expect("sign approval");
        handle_owner_approval_granted(&store, &Some(local_identity), Some(&owner_key), approval)
            .expect("handle approval");

        assert!(submission_is_approved(
            &store
                .project_vision_constraints(&project_id)
                .expect("constraints")
                .expect("project constraints")
        ));
        let project_snapshot = store
            .project_snapshot(&project_id)
            .expect("project snapshot")
            .expect("project exists");
        assert_eq!(project_snapshot.approval.state, "approved");
        assert!(project_snapshot.approval.updated_at.is_some());
        let outbox = store.queued_outbox_messages(20).expect("outbox");
        assert!(outbox.iter().any(|message| message.msg_type == "JoinOffer"));
        assert!(
            outbox
                .iter()
                .any(|message| message.msg_type == "ApprovalApplied")
        );
    }
}
