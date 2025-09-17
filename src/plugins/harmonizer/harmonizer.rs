use async_trait::async_trait;
use bincode;
use redb::{ReadTransaction, TableHandle};
use smallvec::smallvec;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{OnceLock, Weak};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc;

use crate::plugins::harmonizer::availability::AVAILABILITY_INCOMPLETE_BY_PEER;
use crate::plugins::harmonizer::availability::{
    AVAILABILITY_BY_PEER_AND_ENTITY, AVAILABILITY_BY_RANGE_MIN, roots_for_peer,
    roots_for_peer_and_entity,
};
use crate::plugins::harmonizer::availability_queries::{peer_contains_range_local, range_cover};
use crate::plugins::harmonizer::child_set::{
    CELLS_PER_CHUNK, Child, DigestChunk, Enc, STORED_CHUNKS,
};
use crate::plugins::harmonizer::optimizer::{Action as OptAction, AvailabilityDraft};
use crate::plugins::harmonizer::optimizer::{BasicOptimizer, Optimizer};
use crate::plugins::harmonizer::range_cube::RangeCube;
use crate::plugins::harmonizer::{
    child_set::ChildSet,
    protocol::{
        HarmonizerMsg, HarmonizerResp, PROTO_HARMONIZER, ProtoCommand, ReconcileAsk,
        UpdateResponse, UpdateWithAttestation, register_harmonizer_protocol,
    },
};
use crate::plugins::replay;
use crate::plugins::{
    communication::router::Router,
    harmonizer::availability::{AVAILABILITIES_META_TABLE, AVAILABILITIES_TABLE, Availability},
};
use crate::tables::TableHash;
use crate::{
    KuramotoDb, StaticTableDef, WriteBatch, WriteOrigin, plugins::Plugin,
    plugins::versioning::VERSIONING_AUX_ROLE,
    storage_entity::{AuxTableSpec, StorageEntity}, storage_error::StorageError, uuid_bytes::UuidBytes,
};
use crate::{
    database::{AuxOp, IndexPutRequest, WriteRequest},
    plugins::communication::transports::PeerId,
};
use crate::time::Instant;

#[derive(Clone, Debug)]
pub struct PeerContext {
    pub peer_id: UuidBytes,
}

/*──────────────────────── Outbox (network side-effects) ───────────────────*/

enum OutboxItem {
    Notify { peer: PeerId, msg: HarmonizerMsg },
}

/*────────────────────────────────────────────────────────────────────────────*/

pub struct Harmonizer {
    db: OnceLock<Weak<KuramotoDb>>,
    watched_tables: HashSet<&'static str>,
    router: Arc<Router>,
    optimizer: Arc<BasicOptimizer>,
    inbox_tx: mpsc::Sender<ProtoCommand>,
    outbox_tx: mpsc::Sender<OutboxItem>,
    peers: Arc<tokio::sync::RwLock<Vec<PeerId>>>, // dynamic peers
    peer_ctx: PeerContext,
    // Suppress outbox/send for the next before_update invocation(s) when we ingest
    // data originating from a remote peer or local Completer. Minimal cascade guard.
    suppress_outbox_once: Arc<AtomicBool>,
    in_flight_repairs: Arc<tokio::sync::RwLock<std::collections::HashSet<UuidBytes>>>,
    // Round-robin index for bootstrap fallback selection of a single peer.
    rr_idx: Arc<AtomicUsize>,
    // Set of availability UUIDs changed after local optimization (for batching/observability).
    changed_after_opt: Arc<tokio::sync::RwLock<std::collections::HashSet<UuidBytes>>>,
    // Flush cadence for UpdateHint batching (ticks at 1ms). If 0, disabled.
    flush_every_ticks: u64,
}

impl Harmonizer {
    pub fn new(
        router: Arc<Router>,
        optimizer: Arc<BasicOptimizer>,
        watched_tables: HashSet<&'static str>,
        peer_ctx: PeerContext,
    ) -> Arc<Self> {
        let (inbox_tx, inbox_rx) = mpsc::channel::<ProtoCommand>(256);
        let (outbox_tx, outbox_rx) = mpsc::channel::<OutboxItem>(256);

        register_harmonizer_protocol(router.clone(), inbox_tx.clone());

        let hz = Arc::new(Self {
            watched_tables: watched_tables,
            router: router.clone(),
            optimizer: optimizer.clone(),
            inbox_tx,
            outbox_tx,
            peers: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            db: OnceLock::new(),
            peer_ctx,
            suppress_outbox_once: Arc::new(AtomicBool::new(false)),
            in_flight_repairs: Arc::new(tokio::sync::RwLock::new(std::collections::HashSet::new())),
            rr_idx: Arc::new(AtomicUsize::new(0)),
            changed_after_opt: Arc::new(tokio::sync::RwLock::new(std::collections::HashSet::new())),
            flush_every_ticks: 8,
        });

        hz.start_inbox_worker(inbox_rx);
        hz.start_outbox_worker(outbox_rx);
        // Start the completer loop: only repair incomplete nodes via existing transport
        hz.start_completer(std::time::Duration::from_millis(500), 32);
        // Start batched UpdateHint flusher (tick-based)
        hz.start_hint_flusher();

        hz
    }

    pub async fn add_peer(&self, p: PeerId) {
        self.peers.write().await.push(p);
        // Log peer addition into replay (if DB is attached), so Web UI in file mode can discover peers.
        if let Some(dbw) = self.db.get() {
            if let Some(db) = dbw.upgrade() {
                let mut pid = [0u8; 16];
                pid.copy_from_slice(p.as_bytes());
                let _ = crate::plugins::replay::emit_custom(
                    &db,
                    crate::plugins::fnv1a_16("harmonizer"),
                    vec![crate::plugins::replay::LogWriteRequest::PeerAdded { peer_id: pid }],
                )
                .await;
                tracing::info!(target: "harmonizer", "replay: PeerAdded peer={:02x?}", pid);
                println!("[harmonizer] replay PeerAdded peer={:02x?}", pid);
            }
        }
    }

    pub fn peers_handle(&self) -> Arc<tokio::sync::RwLock<Vec<PeerId>>> {
        self.peers.clone()
    }

    fn start_inbox_worker(self: &Arc<Self>, mut inbox_rx: mpsc::Receiver<ProtoCommand>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(cmd) = inbox_rx.recv().await {
                match cmd {
                    ProtoCommand::HandleRequest { respond, msg, .. } => {
                        let resp = match msg {
                            HarmonizerMsg::GetChildrenByRange(req) => {
                                // Serve headers-only coverage for the requested range using local availability trees.
                                let db = match this.db.get().and_then(|w| w.upgrade()) {
                                    Some(db) => db,
                                    None => {
                                        let _ = respond.send(Err("no db".into()));
                                        continue;
                                    }
                                };
                                let txn = match db.begin_read_txn() {
                                    Ok(t) => t,
                                    Err(e) => {
                                        let _ = respond.send(Err(e.to_string()));
                                        continue;
                                    }
                                };
                                let peer = this.peer_ctx.peer_id;
                                // Scope roots per storage entity (first dimension) when available
                                let roots = if let Some(fd) = req.range.dims().first() {
                                    match roots_for_peer_and_entity(
                                        &db,
                                        Some(&txn),
                                        &peer,
                                        fd.hash(),
                                    )
                                    .await
                                    {
                                        Ok(v) => v,
                                        Err(e) => {
                                            let _ = respond.send(Err(e.to_string()));
                                            continue;
                                        }
                                    }
                                } else {
                                    match roots_for_peer(&db, Some(&txn), &peer).await {
                                        Ok(v) => v,
                                        Err(e) => {
                                            let _ = respond.send(Err(e.to_string()));
                                            continue;
                                        }
                                    }
                                };
                                let root_ids: Vec<_> = roots.into_iter().map(|r| r.key).collect();
                                let (frontier, _no_overlap) = match range_cover(
                                    &db,
                                    Some(&txn),
                                    &req.range,
                                    &root_ids,
                                    None,
                                    &vec![],
                                    &mut None,
                                )
                                .await
                                {
                                    Ok(v) => v,
                                    Err(e) => {
                                        let _ = respond.send(Err(e.to_string()));
                                        continue;
                                    }
                                };

                                // Build headers for nodes that fully contain the target and are complete.
                                let mut headers = Vec::new();
                                for a in frontier {
                                    if a.complete && a.range.contains(&req.range) {
                                        // count children via snapshot
                                        let cs = ChildSet::open_tx(&db, &txn, a.key)
                                            .await
                                            .unwrap_or(ChildSet {
                                                parent: a.key,
                                                children: vec![],
                                            });
                                        let hdr = crate::plugins::harmonizer::protocol::AvailabilityHeader {
                                            availability_id: a.key,
                                            level: a.level,
                                            range: a.range.clone(),
                                            child_count: cs.count() as u32,
                                            small_digest: 0,
                                            complete: a.complete,
                                        };
                                        headers.push(hdr);
                                        if (headers.len() as u32) >= req.max {
                                            break;
                                        }
                                    }
                                }
                                HarmonizerResp::Children(
                                    crate::plugins::harmonizer::protocol::ChildrenResponse {
                                        items: vec![],
                                        next: None,
                                        headers,
                                    },
                                )
                            }
                            HarmonizerMsg::GetChildrenDigest(req) => {
                                let db = match this.db.get().and_then(|w| w.upgrade()) {
                                    Some(db) => db,
                                    None => {
                                        let _ = respond.send(Err("no db".into()));
                                        continue;
                                    }
                                };
                                if req.chunk_no != 0 {
                                    // MVP supports only chunk 0
                                    let _ = respond.send(Err("unsupported chunk".into()));
                                    continue;
                                }
                                let bytes = match crate::plugins::harmonizer::child_set::get_digest_chunk0_tx(&db, None, req.parent_uuid).await {
                                    Ok(Some(ch)) => ch.bytes,
                                    Ok(None) => Vec::new(),
                                    Err(e) => {
                                        let _ = respond.send(Err(e.to_string()));
                                        continue;
                                    }
                                };
                                HarmonizerResp::Digest(
                                    crate::plugins::harmonizer::protocol::DigestChunkResponse {
                                        bytes,
                                    },
                                )
                            }
                            HarmonizerMsg::GetChildrenHeaders(req) => {
                                let db = match this.db.get().and_then(|w| w.upgrade()) {
                                    Some(db) => db,
                                    None => {
                                        let _ = respond.send(Err("no db".into()));
                                        continue;
                                    }
                                };
                                let start_ord: u32 = req
                                    .cursor
                                    .as_ref()
                                    .and_then(|v| {
                                        if v.len() == 4 {
                                            Some(u32::from_le_bytes([v[0], v[1], v[2], v[3]]))
                                        } else {
                                            None
                                        }
                                    })
                                    .unwrap_or(0);
                                let max = req.max.min(1024) as usize; // safety cap

                                // Scan child rows by PK prefix: parent • ordinal (le)
                                let mut lo = req.parent_uuid.as_bytes().to_vec();
                                lo.extend_from_slice(&start_ord.to_le_bytes());
                                let mut hi = req.parent_uuid.as_bytes().to_vec();
                                hi.push(0xFF);
                                let rows: Vec<Child> =
                                    match db.range_by_pk::<Child>(&lo, &hi, Some(max + 1)).await {
                                        Ok(v) => v,
                                        Err(e) => {
                                            let _ = respond.send(Err(e.to_string()));
                                            continue;
                                        }
                                    };
                                // Build headers for up to max children
                                let mut headers = Vec::new();
                                let mut next_cursor: Option<Vec<u8>> = None;
                                for (i, row) in rows.iter().take(max).enumerate() {
                                    // load availability for child_id
                                    let av: Availability = match db
                                        .get_data::<Availability>(row.child_id.as_bytes())
                                        .await
                                    {
                                        Ok(v) => v,
                                        Err(_) => continue,
                                    };
                                    // compute small_digest via child set of the child (not necessary for leafs; set 0)
                                    let cs = match ChildSet::open(&db, av.key).await {
                                        Ok(cs) => cs,
                                        Err(_) => ChildSet {
                                            parent: av.key,
                                            children: vec![],
                                        },
                                    };
                                    let hdr =
                                        crate::plugins::harmonizer::protocol::AvailabilityHeader {
                                            availability_id: av.key,
                                            level: av.level,
                                            range: av.range.clone(),
                                            child_count: cs.count() as u32,
                                            small_digest: 0,
                                            complete: av.complete,
                                        };
                                    headers.push(hdr);
                                    if i + 1 == max {
                                        // next cursor = last consumed ordinal + 1
                                        let next_ord = row.ordinal.saturating_add(1);
                                        next_cursor = Some(next_ord.to_le_bytes().to_vec());
                                    }
                                }
                                HarmonizerResp::Children(
                                    crate::plugins::harmonizer::protocol::ChildrenResponse {
                                        items: vec![],
                                        next: next_cursor,
                                        headers,
                                    },
                                )
                            }
                            HarmonizerMsg::GetChildrenByAvailability(_req) => {
                                // Not implemented in this step; return no headers.
                                HarmonizerResp::Children(
                                    crate::plugins::harmonizer::protocol::ChildrenResponse {
                                        items: vec![],
                                        next: None,
                                        headers: vec![],
                                    },
                                )
                            }
                            HarmonizerMsg::GetSymbolsByAvailability(_) => HarmonizerResp::Symbols(
                                crate::plugins::harmonizer::protocol::SymbolsResponse {
                                    cells: vec![],
                                    next: None,
                                    header: None,
                                },
                            ),
                            HarmonizerMsg::UpdateWithAttestation(_) => {
                                HarmonizerResp::Update(UpdateResponse {
                                    accepted: true,
                                    need: None::<ReconcileAsk>,
                                    headers: vec![],
                                })
                            }
                            HarmonizerMsg::UpdateHint(_) => {
                                // Accept hints but do not act in MVP
                                HarmonizerResp::Update(UpdateResponse {
                                    accepted: true,
                                    need: None,
                                    headers: vec![],
                                })
                            }
                        };
                        let _ = respond.send(Ok(resp));
                    }

                    ProtoCommand::HandleNotify { peer, msg } => {
                        match msg {
                            HarmonizerMsg::UpdateWithAttestation(req) => {
                                // Treat attestation as a hint: record the sender's leaf availability locally
                                // (for set reconciliation) but do not ingest entity bytes or modify our tree.
                                if let Some(db) = this.db.get().and_then(|w| w.upgrade()) {
                                    let now = db.get_clock().now();
                                    // 1) Record remote leaf availability for reconciliation
                                    let av = Availability {
                                        key: req.local_availability_id,
                                        peer_id: peer,
                                        range: req.range.clone(),
                                        level: req.level,
                                        schema_hash: 0,
                                        version: 0,
                                        updated_at: now,
                                        complete: true,
                                    };
                                    let _ = db.put(av).await;

                                    // 2) Consider adopting: seed the optimizer to decide if we should cover this range locally.
                                    let seed = AvailabilityDraft {
                                        level: 0,
                                        range: req.range.clone(),
                                        complete: true,
                                    };
                                    let mut adopted = false;
                                    if let Ok(txn) = db.begin_read_txn() {
                                        let t_adopt = Instant::now();
                                        if let Ok(Some(_plan)) = this
                                            .optimizer
                                            .propose(&db, &txn, &[seed], Some(req.range.clone()))
                                            .await
                                        {
                                            adopted = true;
                                            #[cfg(feature = "harmonizer_debug")]
                                            println!(
                                                "attest.receive: optimizer proposed adoption ({}ms)",
                                                t_adopt.elapsed().as_millis()
                                            );
                                        } else {
                                            #[cfg(feature = "harmonizer_debug")]
                                            println!(
                                                "attest.receive: optimizer did not propose ({}ms)",
                                                t_adopt.elapsed().as_millis()
                                            );
                                        }
                                    }
                                    // Ensure the new data gets covered locally only if the optimizer suggests adoption.
                                    if adopted {
                                        this.suppress_outbox_once.store(true, Ordering::SeqCst);
                                        let _ = db
                                            .put_by_table_bytes_with_origin(
                                                &req.table,
                                                &req.entity_bytes,
                                                crate::WriteOrigin::RemoteIngest,
                                            )
                                            .await;
                                    }
                                } else {
                                    tracing::warn!(
                                        "harmonizer: no DB attached; dropping attestation hint"
                                    );
                                }
                            }
                            HarmonizerMsg::UpdateHint(hint) => {
                                // Upsert headers only; trust remote 'complete' and store range/level.
                                // Do not fetch or reconcile children here.
                                if let Some(db) = this.db.get().and_then(|w| w.upgrade()) {
                                    let now = db.get_clock().now();
                                    for t in hint.touched {
                                        let av = Availability {
                                            key: t.uuid,
                                            peer_id: hint.peer_uuid,
                                            range: hint.range.clone(),
                                            level: t.level,
                                            schema_hash: 0,
                                            version: 0,
                                            updated_at: now,
                                            complete: t.complete,
                                        };
                                        let _ = db.put(av).await;
                                        // Optional: could persist t.child_count/t.cell0 in meta in future.
                                    }
                                }
                            }
                            _ => {
                                // ignore other notify types in v0
                            }
                        }
                    }
                }
            }
        });
    }

    fn start_outbox_worker(&self, mut outbox_rx: mpsc::Receiver<OutboxItem>) {
        let router = self.router.clone();
        tokio::spawn(async move {
            while let Some(item) = outbox_rx.recv().await {
                let OutboxItem::Notify { peer, msg } = item;
                let _ = router.notify_on(PROTO_HARMONIZER, peer, &msg).await;
            }
        });
    }

    /// Periodically flush `changed_after_opt` as an UpdateHint to peers, batching recent changes.
    fn start_hint_flusher(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(1));
            let mut since = 0u64;
            loop {
                ticker.tick().await;
                if this.flush_every_ticks == 0 {
                    continue;
                }
                since += 1;
                if since < this.flush_every_ticks {
                    continue;
                }
                since = 0;

                // Snapshot and clear changed set
                let uuids: Vec<UuidBytes> = {
                    let mut guard = this.changed_after_opt.write().await;
                    if guard.is_empty() {
                        continue;
                    }
                    let v: Vec<_> = guard.iter().copied().collect();
                    guard.clear();
                    v
                };

                // Build touched list from current DB state
                let Some(db) = this.db.get().and_then(|w| w.upgrade()) else {
                    continue;
                };
                let now = db.get_clock().now();
                let mut touched: Vec<crate::plugins::harmonizer::protocol::UpdateHintTouched> =
                    Vec::new();
                let mut hint_range: Option<RangeCube> = None;
                for id in uuids {
                    if let Ok(av) = db.get_data::<Availability>(id.as_bytes()).await {
                        // Count children
                        let child_count =
                            crate::plugins::harmonizer::child_set::ChildSet::open(&db, av.key)
                                .await
                                .map(|cs| cs.count() as u32)
                                .unwrap_or(0);
                        touched.push(crate::plugins::harmonizer::protocol::UpdateHintTouched {
                            uuid: av.key,
                            level: av.level,
                            complete: av.complete,
                            child_count,
                            cell0: 0,
                        });
                        if hint_range.is_none() {
                            hint_range = Some(av.range.clone());
                        }
                    } else {
                        // Deleted or not found; send a minimal tombstone touch
                        touched.push(crate::plugins::harmonizer::protocol::UpdateHintTouched {
                            uuid: id,
                            level: 0,
                            complete: false,
                            child_count: 0,
                            cell0: 0,
                        });
                    }
                }
                if touched.is_empty() {
                    continue;
                }

                let range = hint_range.unwrap_or_else(|| {
                    RangeCube::new(smallvec![], smallvec![], smallvec![]).unwrap()
                });
                let hint = crate::plugins::harmonizer::protocol::UpdateHint {
                    peer_uuid: this.peer_ctx.peer_id,
                    range,
                    epoch: now,
                    touched,
                };
                let msg = HarmonizerMsg::UpdateHint(hint);
                let peers = { this.peers.read().await.clone() };
                for &peer in &peers {
                    let _ = this
                        .outbox_tx
                        .send(OutboxItem::Notify {
                            peer,
                            msg: msg.clone(),
                        })
                        .await;
                }
            }
        });
    }

    fn is_watched(&self, tbl: StaticTableDef) -> bool {
        self.watched_tables.contains(tbl.name())
    }

    /// Build a multi-dim leaf: one dim for the PK axis + one per index table present in `index_puts`.
    /// Each dim is the tightest half-open interval that contains *all* keys emitted for that index.
    fn leaf_range_from_pk_and_indexes(
        data_table: StaticTableDef,
        pk: &[u8],
        index_puts: &[IndexPutRequest],
    ) -> RangeCube {
        // By-dimension accumulator, sorted by hash for deterministic output.
        // Value = (TableHash, raw_min_inclusive, raw_max_inclusive)
        let mut by_dim: BTreeMap<u64, (TableHash, Vec<u8>, Vec<u8>)> = BTreeMap::new();

        // 1) PK axis (always present)
        let pk_dim = TableHash::from(data_table);
        by_dim.insert(pk_dim.hash(), (pk_dim, pk.to_vec(), pk.to_vec()));

        // 2) Every index row → fold into that index’s min/max
        for ip in index_puts {
            let dim = TableHash::from(ip.table);
            let key_raw = Self::strip_pk_from_index_key(&ip.key, pk);

            let h = dim.hash();
            match by_dim.get_mut(&h) {
                Some((_d, lo, hi)) => {
                    if key_raw < *lo {
                        *lo = key_raw.clone();
                    }
                    if key_raw > *hi {
                        *hi = key_raw.clone();
                    }
                }
                None => {
                    by_dim.insert(h, (dim, key_raw.clone(), key_raw));
                }
            }
        }

        // 3) Materialize sorted dims/mins/maxs; make ranges half-open by bumping hi
        let mut dims = smallvec![];
        let mut mins = smallvec![];
        let mut maxs = smallvec![];
        for (_h, (dim, lo, mut hi_incl)) in by_dim.into_iter() {
            // Ensure non-empty: [lo, hi_incl + 0x01)
            if hi_incl <= lo {
                // Equal is fine; bump hi to keep non-empty.
                hi_incl = lo.clone();
            }
            let mut hi_excl = hi_incl.clone();
            hi_excl.push(0x01);

            dims.push(dim);
            mins.push(lo);
            maxs.push(hi_excl);
        }

        RangeCube::new(dims, mins, maxs).expect("leaf_range constructed with aligned dims")
    }

    /// Returns **owned** bytes (no lifetimes). If your index key encodes PK as suffix/prefix,
    /// strip it so the coordinate lives only in index-key space.
    fn strip_pk_from_index_key(stored: &[u8], pk: &[u8]) -> Vec<u8> {
        if stored.len() >= pk.len() && stored.ends_with(pk) {
            stored[..stored.len() - pk.len()].to_vec()
        } else if stored.len() >= pk.len() && stored.starts_with(pk) {
            stored[pk.len()..].to_vec()
        } else {
            stored.to_vec()
        }
    }

    /*──────────────────────────── Repair helpers ───────────────────────────*/
    /// Compute FNV-1a 64-bit hash over provided bytes (matches `small_digest_cell0_tx`).
    fn fnv1a64(bytes: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        for b in bytes {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    async fn fetch_remote_chunk0(
        &self,
        peer: PeerId,
        parent: UuidBytes,
    ) -> Result<Vec<u8>, StorageError> {
        let req = crate::plugins::harmonizer::protocol::HarmonizerMsg::GetChildrenDigest(
            crate::plugins::harmonizer::protocol::GetChildrenDigest {
                parent_uuid: parent,
                chunk_no: 0,
            },
        );
        let resp = self
            .router
            .request_on::<_, crate::plugins::harmonizer::protocol::HarmonizerResp>(
                PROTO_HARMONIZER,
                peer,
                &req,
                std::time::Duration::from_secs(2),
            )
            .await
            .map_err(|e| StorageError::Other(e.to_string()))?;
        match resp {
            crate::plugins::harmonizer::protocol::HarmonizerResp::Digest(d) => Ok(d.bytes),
            _ => Err(StorageError::Other("unexpected response type".into())),
        }
    }

    async fn fetch_remote_child_ids(
        &self,
        peer: PeerId,
        parent: UuidBytes,
    ) -> Result<Vec<UuidBytes>, StorageError> {
        let mut cursor: Option<Vec<u8>> = None;
        let mut out = Vec::<UuidBytes>::new();
        loop {
            let req = crate::plugins::harmonizer::protocol::HarmonizerMsg::GetChildrenHeaders(
                crate::plugins::harmonizer::protocol::GetChildrenHeaders {
                    parent_uuid: parent,
                    cursor: cursor.clone(),
                    max: 256,
                },
            );
            let resp = self
                .router
                .request_on::<_, crate::plugins::harmonizer::protocol::HarmonizerResp>(
                    PROTO_HARMONIZER,
                    peer,
                    &req,
                    std::time::Duration::from_secs(3),
                )
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;
            let r = match resp {
                crate::plugins::harmonizer::protocol::HarmonizerResp::Children(c) => c,
                _ => return Err(StorageError::Other("unexpected response type".into())),
            };
            for h in r.headers {
                out.push(h.availability_id);
            }
            cursor = r.next;
            if cursor.is_none() {
                break;
            }
        }
        Ok(out)
    }

    /// Try to reconcile local child set with remote peer for the given parent.
    /// Gate on cell0 equality; on mismatch attempt RIBLT using chunk0; then fallback to header-diff.
    pub async fn repair_parent_with_peer(
        &self,
        peer: PeerId,
        parent: UuidBytes,
    ) -> Result<(), StorageError> {
        let db = self
            .db
            .get()
            .and_then(|w| w.upgrade())
            .ok_or_else(|| StorageError::Other("no db".into()))?;

        // Local small digest (if any)
        let local_cell0 =
            crate::plugins::harmonizer::child_set::small_digest_cell0(&db, parent).await?;

        // Remote chunk0 and digest
        let remote_chunk0 = self
            .fetch_remote_chunk0(peer, parent)
            .await
            .unwrap_or_default();
        let remote_cell0 = if remote_chunk0.is_empty() {
            None
        } else {
            Some(Self::fnv1a64(&remote_chunk0))
        };

        if let (Some(lc), Some(rc)) = (local_cell0, remote_cell0) {
            if crate::plugins::harmonizer::child_set::cell0_eq(lc, rc) {
                return Ok(()); // fast-path: equal
            }
        }

        // Prepare local child set and attempt IBF using remote chunk0 symbols
        let mut local_cs = ChildSet::open(&db, parent).await?;
        if !remote_chunk0.is_empty() {
            if let Ok((remote_cells, _)) = bincode::decode_from_slice::<
                Vec<crate::plugins::harmonizer::child_set::Cell>,
                _,
            >(&remote_chunk0, bincode::config::standard())
            {
                if let Some((remote_only, local_only)) =
                    crate::plugins::harmonizer::riblt::decode_delta_16_uuid(
                        &local_cs.children,
                        &remote_cells,
                    )
                {
                    // Apply deletions first, then additions
                    for cid in local_only.into_iter() {
                        let _ = local_cs.remove_child(&db, cid).await;
                    }
                    for cid in remote_only.into_iter() {
                        let _ = local_cs.add_child(&db, cid).await;
                    }
                    return Ok(());
                }
            }
        }

        // Fallback: fetch headers and reconcile via set-diff
        let remote_ids = self
            .fetch_remote_child_ids(peer, parent)
            .await
            .unwrap_or_default();
        if remote_ids.is_empty() {
            return Ok(());
        }
        let local_set: std::collections::HashSet<UuidBytes> =
            local_cs.children.iter().copied().collect();
        let remote_set: std::collections::HashSet<UuidBytes> = remote_ids.iter().copied().collect();
        // Deletes
        for cid in local_set.difference(&remote_set) {
            let _ = local_cs.remove_child(&db, *cid).await;
        }
        // Adds
        for cid in remote_set.difference(&local_set) {
            let _ = local_cs.add_child(&db, *cid).await;
        }
        Ok(())
    }

    /// Structural completer MVP: repair a parent and descend up to `max_depth`.
    pub async fn complete_availability(
        &self,
        parent: UuidBytes,
        max_depth: usize,
        mut budget: Option<usize>,
    ) -> Result<(), StorageError> {
        if budget == Some(0) {
            return Ok(());
        }
        let peers = self.peers.read().await.clone();
        let Some(&peer) = peers.first() else {
            return Ok(());
        };
        let db = self
            .db
            .get()
            .and_then(|w| w.upgrade())
            .ok_or_else(|| StorageError::Other("no db".into()))?;

        // Iterative DFS with explicit stack to avoid recursive async.
        let mut stack: Vec<(UuidBytes, usize)> = vec![(parent, max_depth)];
        while let Some((pid, depth)) = stack.pop() {
            if budget == Some(0) {
                break;
            }
            self.repair_parent_with_peer(peer, pid).await?;
            if let Some(b) = budget.as_mut() {
                *b = b.saturating_sub(1);
            }
            if depth == 0 {
                continue;
            }
            let cs = ChildSet::open(&db, pid).await?;
            for cid in cs.children.iter().copied() {
                stack.push((cid, depth - 1));
            }
        }
        Ok(())
    }

    /// Scan for incomplete local nodes and trigger repairs using the first known peer.
    pub async fn tick_completer(&self, max: usize) -> Result<usize, StorageError> {
        let db = self
            .db
            .get()
            .and_then(|w| w.upgrade())
            .ok_or_else(|| StorageError::Other("no db".into()))?;
        let peers = self.peers.read().await.clone();
        let Some(&peer) = peers.first() else {
            return Ok(0);
        }; // nobody to ask

        // Build index prefix for (peer_id, complete=0)
        let mut lo = self.peer_ctx.peer_id.as_bytes().to_vec();
        lo.push(0); // incomplete flag
        let mut hi = lo.clone();
        hi.push(0x01); // half-open

        let pks = db
            .collect_pks_in_index_range_tx(
                None,
                AVAILABILITY_INCOMPLETE_BY_PEER,
                &lo,
                &hi,
                Some(max),
            )
            .await?;

        let mut scheduled = 0usize;
        for pk in pks {
            if pk.len() != 16 {
                continue;
            }
            let mut idb = [0u8; 16];
            idb.copy_from_slice(&pk);
            let id = UuidBytes::from_bytes(idb);
            // in-flight guard
            let mut guard = self.in_flight_repairs.write().await;
            if !guard.insert(id) {
                continue;
            }
            drop(guard);
            let _ = self.repair_parent_with_peer(peer, id).await;
            let mut g = self.in_flight_repairs.write().await;
            g.remove(&id);
            scheduled += 1;
        }
        Ok(scheduled)
    }

    /// Start a background task that periodically runs the completer tick with the
    /// given `interval` and `max_per_tick`.
    pub fn start_completer(self: &Arc<Self>, interval: std::time::Duration, max_per_tick: usize) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let _ = this.tick_completer(max_per_tick).await;
            }
        });
    }
}

#[async_trait]
impl Plugin for Harmonizer {
    async fn before_update(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        batch: &mut WriteBatch,
    ) -> Result<(), StorageError> {
        // Delegate to origin-aware path with default LocalCommit
        self.before_update_with_origin(db, txn, batch, WriteOrigin::LocalCommit)
            .await
    }

    async fn before_update_with_origin(
        &self,
        db: &KuramotoDb,
        txn: &ReadTransaction,
        batch: &mut WriteBatch,
        origin: WriteOrigin,
    ) -> Result<(), StorageError> {
        let dbg = std::env::var("KDB_DEBUG_RANGE").ok().is_some();
        if dbg {
            println!("harm.before_update: START origin={:?} batch_len={}", origin, batch.len());
        }
        let Some(first) = batch.get(0) else {
            if dbg { println!("harm.before_update: empty batch early-exit"); }
            return Ok(());
        };

        // Debug: batch size (behind feature flag)
        #[cfg(feature = "harmonizer_debug")]
        println!("harm.before_update: incoming batch_len={}", batch.len());

        // Only act on writes to watched data tables
        let (data_table, key_owned, entity_bytes, index_puts) = match first {
            WriteRequest::Put {
                data_table,
                key,
                value,
                index_puts,
                ..
            } if self.is_watched(*data_table) => {
                #[cfg(feature = "harmonizer_debug")]
                println!(
                    "harm.before_update: incoming batch_len={} | first table={} watched=true",
                    batch.len(),
                    data_table.name()
                );
                (*data_table, key.clone(), value.clone(), index_puts)
            }
            _ => return Ok(()),
        };

        let now = db.get_clock().now();

        let peer_id = self.peer_ctx.peer_id;

        // --- 1) compute leaf availability header (level 0) and persist it locally ---
        let avail_id = UuidBytes::new();
        let leaf_range = Self::leaf_range_from_pk_and_indexes(data_table, &key_owned, &index_puts);

        let avail = Availability {
            key: avail_id,
            peer_id,
            range: leaf_range,
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: now,
            complete: true,
        };

        if std::env::var("KDB_DEBUG_RANGE").ok().is_some() {
            println!(
                "harm.before_update: table={} pk_len={} seed_range_dims={} origin={:?}",
                data_table.name(),
                key_owned.len(),
                avail.range.dims().len(),
                origin
            );
        }

        // --- 2) broadcast inline attestation (v0) selectively ---
        // Origin gating: only emit for LocalCommit; suppress for Completer/RemoteIngest.
        // Only notify peers whose current roots already cover this range locally.
        let suppress = self.suppress_outbox_once.swap(false, Ordering::SeqCst);
        let peers = { self.peers.read().await.clone() }; // drop lock before awaits
        if !peers.is_empty() && !suppress && matches!(origin, WriteOrigin::LocalCommit) {
            // Build attestation payload once
            let b = avail_id.as_bytes();
            debug_assert_eq!(b.len(), 16, "UuidBytes must be 16 bytes");
            let hi = u64::from_le_bytes(b[0..8].try_into().unwrap());
            let lo = u64::from_le_bytes(b[8..16].try_into().unwrap());
            let digest = hi ^ lo;
            let att = UpdateWithAttestation {
                table: data_table.name().to_string(),
                pk: key_owned.clone(),
                range: avail.range.clone(),
                local_availability_id: avail_id,
                level: 0,
                child_count: 1,
                small_digest: digest,
                entity_bytes,
            };
            let msg = HarmonizerMsg::UpdateWithAttestation(att);

            // Select peers with local coverage of this range; if none qualify, fall back to all peers (bootstrap).
            let mut sent = 0usize;
            for &peer in &peers {
                if peer == self.peer_ctx.peer_id {
                    continue;
                }
                let mut qcache = None;
                let ok = peer_contains_range_local(
                    db,
                    Some(txn),
                    &peer,
                    &avail.range,
                    &vec![],
                    &mut qcache,
                )
                .await
                .unwrap_or(false);
                if !ok {
                    continue;
                }
                let _ = self
                    .outbox_tx
                    .send(OutboxItem::Notify {
                        peer,
                        msg: msg.clone(),
                    })
                    .await;
                sent += 1;
            }
            if sent == 0 {
                // Bootstrap fallback: notify a single peer to seed coverage; others learn via UpdateHint.
                let candidates: Vec<PeerId> = peers
                    .iter()
                    .copied()
                    .filter(|p| *p != self.peer_ctx.peer_id)
                    .collect();
                if !candidates.is_empty() {
                    let idx = self.rr_idx.fetch_add(1, Ordering::Relaxed) % candidates.len();
                    let peer = candidates[idx];
                    let _ = self
                        .outbox_tx
                        .send(OutboxItem::Notify {
                            peer,
                            msg: msg.clone(),
                        })
                        .await;
                }
            }
        }

        let seed = AvailabilityDraft {
            level: 0,
            range: avail.range.clone(),
            complete: true,
        };
        let inserts = vec![seed];

        #[cfg(feature = "harmonizer_debug")]
        println!("before_update: running optimizer.propose()");
        let _t_propose = Instant::now();

        // Collect touched parent nodes for hinting
        let mut touched: Vec<crate::plugins::harmonizer::protocol::UpdateHintTouched> = Vec::new();

        if matches!(origin, WriteOrigin::LocalCommit) {
            if let Some(plan) = self
                .optimizer
                .propose(db, txn, &inserts, Some(avail.range.clone()))
                .await?
            {
                if std::env::var("KDB_DEBUG_RANGE").ok().is_some() {
                    println!("harm.before_update: optimizer returned plan_len={}", plan.len());
                }
                #[cfg(feature = "harmonizer_debug")]
                println!(
                    "before_update: optimizer returned {} actions (took {}ms)",
                    plan.len(),
                    _t_propose.elapsed().as_millis()
                );

                for act in plan {
                    match act {
                        OptAction::Insert(d) => {
                            // Materialize the availability node (any level) with indexes.
                            let id = UuidBytes::new();
                            let now2 = db.get_clock().now();
                            let node = Availability {
                                key: id,
                                peer_id,
                                range: d.range.clone(),
                                level: d.level,
                                schema_hash: 0,
                                version: 0,
                                updated_at: now2,
                                complete: true,
                            };

                            let meta2 = bincode::encode_to_vec(
                                crate::meta::BlobMeta {
                                    version: 0,
                                    created_at: now2,
                                    updated_at: now2,
                                    deleted_at: None,
                                    region_lock: crate::region_lock::RegionLock::None,
                                },
                                bincode::config::standard(),
                            )
                            .map_err(|e| StorageError::Bincode(e.to_string()))?;

                            let node_pk = node.primary_key();
                            if dbg { println!("harm.before_update: STAGE avail.put level={} dims={}", node.level, node.range.dims().len()); }
                            batch.push(WriteRequest::Put {
                                data_table: AVAILABILITIES_TABLE,
                                key: node_pk.clone(),
                                value: node.to_bytes(),
                                aux_ops: vec![AuxOp {
                                    table: AVAILABILITIES_META_TABLE,
                                    key: node_pk.clone(),
                                    value: Some(meta2),
                                }],
                                index_removes: Vec::new(),
                                index_puts: {
                                    // (peer, complete_flag)
                                    let mut v = Vec::new();
                                    let mut raw = peer_id.as_bytes().to_vec();
                                    raw.push(1);
                                    let by_peer_comp =
                                        crate::database::encode_nonunique_index_key(&raw, &node_pk);
                                    v.push(crate::database::IndexPutRequest {
                                        table: AVAILABILITY_INCOMPLETE_BY_PEER,
                                        key: by_peer_comp,
                                        value: node_pk.clone(),
                                        unique: false,
                                    });
                                    // (peer, first_dim_hash)
                                    if let Some(fd) = node.range.dims().first() {
                                        let mut raw = peer_id.as_bytes().to_vec();
                                        raw.extend_from_slice(&fd.hash().to_be_bytes());
                                        let k = crate::database::encode_nonunique_index_key(
                                            &raw, &node_pk,
                                        );
                                        v.push(crate::database::IndexPutRequest {
                                            table: AVAILABILITY_BY_PEER_AND_ENTITY,
                                            key: k,
                                            value: node_pk.clone(),
                                            unique: false,
                                        });
                                    }
                                    // by_range_min: one per axis (axis_hash_be • min • 0x00 • pk)
                                    for (i, dim) in node.range.dims().iter().enumerate() {
                                        let mut raw = dim.hash().to_be_bytes().to_vec();
                                        raw.extend_from_slice(&node.range.mins()[i]);
                                        let k = crate::database::encode_nonunique_index_key(
                                            &raw, &node_pk,
                                        );
                                        v.push(crate::database::IndexPutRequest {
                                            table: AVAILABILITY_BY_RANGE_MIN,
                                            key: k,
                                            value: node_pk.clone(),
                                            unique: false,
                                        });
                                    }
                                    v
                                },
                            });

                            // Emit a lightweight context tag into the replay log for easier reconstruction.
                            // kind: 0 = own-tree delta (LocalCommit); 1 = mirror-tree delta (RemoteIngest/Completer)
                            let kind: u8 = match origin {
                                WriteOrigin::LocalCommit => 0,
                                WriteOrigin::RemoteIngest | WriteOrigin::Completer => 1,
                                WriteOrigin::Plugin(_) => 0,
                            };
                            let mut pidb = [0u8; 16];
                            pidb.copy_from_slice(peer_id.as_bytes());
                            let first_dim =
                                node.range.dims().first().map(|d| d.hash()).unwrap_or(0u64);
                            if dbg { println!("harm.before_update: (moved) replay context will be emitted in after_write"); }
                            tracing::info!(
                                target: "harmonizer",
                                "replay: HarmonizerContext kind={} peer={:02x?} entity=0x{:016x}",
                                kind,
                                pidb,
                                first_dim
                            );
                            println!(
                                "[harmonizer] replay HarmonizerContext kind={} peer={:02x?} entity=0x{:016x}",
                                kind, pidb, first_dim
                            );

                            // Track changed availability after optimization
                            {
                                let mut set = self.changed_after_opt.write().await;
                                set.insert(id);
                            }
                        }

                        OptAction::Replace { id, draft } => {
                            // Replace = Insert(new draft) + Delete(old id) in one batch (Insert first)
                            // 1) Insert the replacement node
                            let new_id = UuidBytes::new();
                            let now2 = db.get_clock().now();
                            let node = Availability {
                                key: new_id,
                                peer_id,
                                range: draft.range.clone(),
                                level: draft.level,
                                schema_hash: 0,
                                version: 0,
                                updated_at: now2,
                                complete: true,
                            };
                            let meta2 = bincode::encode_to_vec(
                                crate::meta::BlobMeta {
                                    version: 0,
                                    created_at: now2,
                                    updated_at: now2,
                                    deleted_at: None,
                                    region_lock: crate::region_lock::RegionLock::None,
                                },
                                bincode::config::standard(),
                            )
                            .map_err(|e| StorageError::Bincode(e.to_string()))?;
                            let node_pk = node.primary_key();
                            if dbg { println!("harm.before_update: STAGE avail.put (replace) level={} dims={}", node.level, node.range.dims().len()); }
                            batch.push(WriteRequest::Put {
                                data_table: AVAILABILITIES_TABLE,
                                key: node_pk.clone(),
                                value: node.to_bytes(),
                                aux_ops: vec![AuxOp {
                                    table: AVAILABILITIES_META_TABLE,
                                    key: node_pk.clone(),
                                    value: Some(meta2),
                                }],
                                index_removes: Vec::new(),
                                index_puts: {
                                    let mut v = Vec::new();
                                    // (peer, complete_flag)
                                    let mut raw = peer_id.as_bytes().to_vec();
                                    raw.push(1);
                                    let by_peer_comp =
                                        crate::database::encode_nonunique_index_key(&raw, &node_pk);
                                    v.push(crate::database::IndexPutRequest {
                                        table: AVAILABILITY_INCOMPLETE_BY_PEER,
                                        key: by_peer_comp,
                                        value: node_pk.clone(),
                                        unique: false,
                                    });
                                    // (peer, first_dim_hash)
                                    if let Some(fd) = node.range.dims().first() {
                                        let mut raw = peer_id.as_bytes().to_vec();
                                        raw.extend_from_slice(&fd.hash().to_be_bytes());
                                        let k = crate::database::encode_nonunique_index_key(&raw, &node_pk);
                                        v.push(crate::database::IndexPutRequest {
                                            table: AVAILABILITY_BY_PEER_AND_ENTITY,
                                            key: k,
                                            value: node_pk.clone(),
                                            unique: false,
                                        });
                                    }
                                    // by_range_min: one per axis (axis_hash_be • min • 0x00 • pk)
                                    for (i, dim) in node.range.dims().iter().enumerate() {
                                        let mut raw = dim.hash().to_be_bytes().to_vec();
                                        raw.extend_from_slice(&node.range.mins()[i]);
                                        let k = crate::database::encode_nonunique_index_key(&raw, &node_pk);
                                        v.push(crate::database::IndexPutRequest {
                                            table: AVAILABILITY_BY_RANGE_MIN,
                                            key: k,
                                            value: node_pk.clone(),
                                            unique: false,
                                        });
                                    }
                                    v
                                },
                            });

                            // Emit replay context (same as Insert; origin-aware tag)
                            let kind: u8 = match origin {
                                WriteOrigin::LocalCommit => 0,
                                WriteOrigin::RemoteIngest | WriteOrigin::Completer => 1,
                                WriteOrigin::Plugin(_) => 0,
                            };
                            let mut pidb = [0u8; 16];
                            pidb.copy_from_slice(peer_id.as_bytes());
                            let first_dim = node.range.dims().first().map(|d| d.hash()).unwrap_or(0u64);
                            if dbg { println!("harm.before_update: (replace) replay context will be emitted in after_write"); }
                            tracing::info!(
                                target: "harmonizer",
                                "replay: HarmonizerContext kind={} peer={:02x?} entity=0x{:016x}",
                                kind,
                                pidb,
                                first_dim
                            );
                            println!(
                                "[harmonizer] replay HarmonizerContext kind={} peer={:02x?} entity=0x{:016x}",
                                kind, pidb, first_dim
                            );
                            {
                                let mut set = self.changed_after_opt.write().await;
                                set.insert(new_id);
                            }

                            // 2) Delete the old node id (reuse Delete path, including L0 data eviction and child edge cleanup)
                            if let Ok(existing) = db.get_data::<Availability>(id.as_bytes()).await {
                                // If this is a leaf, evict underlying data first
                                if existing.level == 0 {
                                    if let Some(by_tbl) = crate::plugins::harmonizer::availability_queries::storage_atom_pks_in_cube_tx(
                                        db,
                                        Some(txn),
                                        &existing.range,
                                        None,
                                    )
                                    .await? {
                                        for (tbl, pks) in by_tbl.into_iter() {
                                            let tname = tbl.name().to_string();
                                            for pk in pks {
                                                let req = db.plan_delete_by_table_pk(&tname, &pk).await?;
                                                batch.push(req);
                                            }
                                        }
                                    }
                                }

                                // Delete child edges under this availability (if any)
                                {
                                    let mut hi = id.as_bytes().to_vec();
                                    hi.push(0xFF);
                                    let rows: Vec<Child> =
                                        db.range_by_pk::<Child>(id.as_bytes(), &hi, None).await?;
                                    let now = db.get_clock().now();
                                    let meta = bincode::encode_to_vec(
                                        crate::meta::BlobMeta {
                                            version: 0,
                                            created_at: now,
                                            updated_at: now,
                                            deleted_at: Some(now),
                                            region_lock: crate::region_lock::RegionLock::None,
                                        },
                                        bincode::config::standard(),
                                    )
                                    .map_err(|e| StorageError::Bincode(e.to_string()))?;
                                    for row in rows {
                                        // delete row + by_child index entry
                                        let pk = row.primary_key();
                                        let mut idx_key = row.child_id.as_bytes().to_vec();
                                        idx_key.push(0);
                                        idx_key.extend_from_slice(&pk);
                                        batch.push(WriteRequest::Delete {
                                            data_table: crate::plugins::harmonizer::child_set::AVAIL_CHILDREN_TBL,
                                            key: pk,
                                            aux_ops: vec![AuxOp {
                                                table: crate::plugins::harmonizer::child_set::AVAIL_CHILDREN_META_TBL,
                                                key: pk.clone(),
                                                value: Some(meta.clone()),
                                            }],
                                            index_removes: vec![crate::database::make_index_remove(
                                                crate::plugins::harmonizer::child_set::AVAIL_CHILDREN_BY_CHILD_TBL,
                                                idx_key,
                                            )],
                                        });
                                    }
                                }

                                // Delete digest chunks under this availability (if any)
                                {
                                    let mut hi = id.as_bytes().to_vec();
                                    hi.push(0xFF);
                                    let rows: Vec<DigestChunk> = db
                                        .range_by_pk::<DigestChunk>(id.as_bytes(), &hi, None)
                                        .await?;
                                    let now = db.get_clock().now();
                                    let meta = bincode::encode_to_vec(
                                        crate::meta::BlobMeta {
                                            version: 0,
                                            created_at: now,
                                            updated_at: now,
                                            deleted_at: Some(now),
                                            region_lock: crate::region_lock::RegionLock::None,
                                        },
                                        bincode::config::standard(),
                                    )
                                    .map_err(|e| StorageError::Bincode(e.to_string()))?;
                                    for row in rows {
                                        batch.push(WriteRequest::Delete {
                                            data_table: crate::plugins::harmonizer::child_set::AVAIL_DIG_CHUNK_TBL,
                                            key: row.primary_key(),
                                            aux_ops: vec![AuxOp {
                                                table: crate::plugins::harmonizer::child_set::AVAIL_DIG_CHUNK_META_TBL,
                                                key: row.primary_key(),
                                                value: Some(meta.clone()),
                                            }],
                                            index_removes: Vec::new(),
                                        });
                                    }
                                }

                                // Finally delete the availability row itself with proper index removal
                                let now2 = db.get_clock().now();
                                let meta2 = bincode::encode_to_vec(
                                    crate::meta::BlobMeta {
                                        version: existing.version.saturating_add(1),
                                        created_at: now2,
                                        updated_at: now2,
                                        deleted_at: Some(now2),
                                        region_lock: crate::region_lock::RegionLock::None,
                                    },
                                    bincode::config::standard(),
                                )
                                .map_err(|e| StorageError::Bincode(e.to_string()))?;
                                // Non-unique index remove for by_peer
                                let idx_key = crate::database::encode_nonunique_index_key(
                                    existing.peer_id.as_bytes(),
                                    id.as_bytes(),
                                );
                                // Non-unique index remove for by_peer_complete flag
                                let mut raw2 = existing.peer_id.as_bytes().to_vec();
                                raw2.push(if existing.complete { 1 } else { 0 });
                                let idx_key2 = crate::database::encode_nonunique_index_key(
                                    &raw2,
                                    id.as_bytes(),
                                );
                                // Non-unique index remove for (peer, first_dim_hash)
                                let mut idx_peer_ent = None;
                                if let Some(fd) = existing.range.dims().first() {
                                    let mut raw = existing.peer_id.as_bytes().to_vec();
                                    raw.extend_from_slice(&fd.hash().to_be_bytes());
                                    idx_peer_ent = Some(crate::database::encode_nonunique_index_key(
                                        &raw,
                                        id.as_bytes(),
                                    ));
                                }
                                // Index removes for by_range_min (one per axis)
                                let mut range_min_removes: Vec<(crate::StaticTableDef, Vec<u8>)> =
                                    Vec::new();
                                for (i, dim) in existing.range.dims().iter().enumerate() {
                                    let mut raw = dim.hash().to_be_bytes().to_vec();
                                    raw.extend_from_slice(&existing.range.mins()[i]);
                                    let k = crate::database::encode_nonunique_index_key(
                                        &raw,
                                        id.as_bytes(),
                                    );
                                    range_min_removes.push((AVAILABILITY_BY_RANGE_MIN, k));
                                }
                                batch.push(WriteRequest::Delete {
                                    data_table: AVAILABILITIES_TABLE,
                                    key: id.as_bytes().to_vec(),
                                    aux_ops: vec![AuxOp {
                                        table: AVAILABILITIES_META_TABLE,
                                        key: id.as_bytes().to_vec(),
                                        value: Some(meta2),
                                    }],
                                    index_removes: {
                                        let mut v = vec![
                                            crate::database::make_index_remove(
                                                AVAILABILITY_INCOMPLETE_BY_PEER,
                                                idx_key2,
                                            ),
                                            // keep this for completeness though by_peer index is not explicitly created above
                                            crate::database::make_index_remove(
                                                AVAILABILITY_INCOMPLETE_BY_PEER,
                                                idx_key,
                                            ),
                                        ];
                                        if !existing.range.dims().is_empty() {
                                            if let Some(k) = idx_peer_ent {
                                                v.push(crate::database::make_index_remove(
                                                    AVAILABILITY_BY_PEER_AND_ENTITY,
                                                    k,
                                                ));
                                            }
                                            for (_tbl, key) in range_min_removes.into_iter() {
                                                v.push(crate::database::make_index_remove(
                                                    AVAILABILITY_BY_RANGE_MIN,
                                                    key,
                                                ));
                                            }
                                        }
                                        v
                                    },
                                });
                            }
                        }

                        OptAction::Delete(id) => {
                            // Resolve the availability being deleted (snapshot consistent)
                            if let Ok(existing) = db.get_data::<Availability>(id.as_bytes()).await {
                                // If this is a leaf, evict underlying data first
                                if existing.level == 0 {
                                    if let Some(by_tbl) = crate::plugins::harmonizer::availability_queries::storage_atom_pks_in_cube_tx(
                                    db,
                                    Some(txn),
                                    &existing.range,
                                    None,
                                )
                                .await? {
                                    for (tbl, pks) in by_tbl.into_iter() {
                                        let tname = tbl.name().to_string();
                                        for pk in pks {
                                            // For correctness and ordering, plan deletes into this batch
                                            let req = db.plan_delete_by_table_pk(&tname, &pk).await?;
                                            batch.push(req);
                                        }
                                    }
                                }
                                }

                                // Delete child edges under this availability (if any)
                                {
                                    let mut hi = id.as_bytes().to_vec();
                                    hi.push(0xFF);
                                    let rows: Vec<Child> =
                                        db.range_by_pk::<Child>(id.as_bytes(), &hi, None).await?;
                                    let now = db.get_clock().now();
                                    let meta = bincode::encode_to_vec(
                                        crate::meta::BlobMeta {
                                            version: 0,
                                            created_at: now,
                                            updated_at: now,
                                            deleted_at: Some(now),
                                            region_lock: crate::region_lock::RegionLock::None,
                                        },
                                        bincode::config::standard(),
                                    )
                                    .map_err(|e| StorageError::Bincode(e.to_string()))?;
                                    for row in rows {
                                        // delete row + by_child index entry
                                        let pk = row.primary_key();
                                        let mut idx_key = row.child_id.as_bytes().to_vec();
                                        idx_key.push(0);
                                        idx_key.extend_from_slice(&pk);
                                        batch.push(WriteRequest::Delete {
                                            data_table: crate::plugins::harmonizer::child_set::AVAIL_CHILDREN_TBL,
                                            key: pk.clone(),
                                            aux_ops: vec![AuxOp {
                                                table: crate::plugins::harmonizer::child_set::AVAIL_CHILDREN_META_TBL,
                                                key: pk,
                                                value: Some(meta.clone()),
                                            }],
                                            index_removes: vec![crate::database::make_index_remove(
                                                crate::plugins::harmonizer::child_set::AVAIL_CHILDREN_BY_CHILD_TBL,
                                                idx_key,
                                            )],
                                        });
                                    }
                                }

                                // Delete digest chunks under this availability (if any)
                                {
                                    let mut hi = id.as_bytes().to_vec();
                                    hi.push(0xFF);
                                    let rows: Vec<DigestChunk> = db
                                        .range_by_pk::<DigestChunk>(id.as_bytes(), &hi, None)
                                        .await?;
                                    let now = db.get_clock().now();
                                    let meta = bincode::encode_to_vec(
                                        crate::meta::BlobMeta {
                                            version: 0,
                                            created_at: now,
                                            updated_at: now,
                                            deleted_at: Some(now),
                                            region_lock: crate::region_lock::RegionLock::None,
                                        },
                                        bincode::config::standard(),
                                    )
                                    .map_err(|e| StorageError::Bincode(e.to_string()))?;
                                    for row in rows {
                                        batch.push(WriteRequest::Delete {
                                            data_table: crate::plugins::harmonizer::child_set::AVAIL_DIG_CHUNK_TBL,
                                            key: row.primary_key(),
                                            aux_ops: vec![AuxOp {
                                                table: crate::plugins::harmonizer::child_set::AVAIL_DIG_CHUNK_META_TBL,
                                                key: row.primary_key(),
                                                value: Some(meta.clone()),
                                            }],
                                            index_removes: Vec::new(),
                                        });
                                    }
                                }

                                // Finally delete the availability row itself with proper index removal
                                let now2 = db.get_clock().now();
                                let meta2 = bincode::encode_to_vec(
                                    crate::meta::BlobMeta {
                                        version: existing.version.saturating_add(1),
                                        created_at: now2,
                                        updated_at: now2,
                                        deleted_at: Some(now2),
                                        region_lock: crate::region_lock::RegionLock::None,
                                    },
                                    bincode::config::standard(),
                                )
                                .map_err(|e| StorageError::Bincode(e.to_string()))?;
                                // Non-unique index remove for by_peer
                                let idx_key = crate::database::encode_nonunique_index_key(
                                    existing.peer_id.as_bytes(),
                                    id.as_bytes(),
                                );
                                // Non-unique index remove for by_peer_complete flag
                                let mut raw2 = existing.peer_id.as_bytes().to_vec();
                                raw2.push(if existing.complete { 1 } else { 0 });
                                let idx_key2 = crate::database::encode_nonunique_index_key(
                                    &raw2,
                                    id.as_bytes(),
                                );
                                // Non-unique index remove for (peer, first_dim_hash)
                                let mut idx_peer_ent = None;
                                if let Some(fd) = existing.range.dims().first() {
                                    let mut raw = existing.peer_id.as_bytes().to_vec();
                                    raw.extend_from_slice(&fd.hash().to_be_bytes());
                                    idx_peer_ent =
                                        Some(crate::database::encode_nonunique_index_key(
                                            &raw,
                                            id.as_bytes(),
                                        ));
                                }
                                // Index removes for by_range_min (one per axis)
                                let mut range_min_removes: Vec<(crate::StaticTableDef, Vec<u8>)> =
                                    Vec::new();
                                for (i, dim) in existing.range.dims().iter().enumerate() {
                                    let mut raw = dim.hash().to_be_bytes().to_vec();
                                    raw.extend_from_slice(&existing.range.mins()[i]);
                                    let k = crate::database::encode_nonunique_index_key(
                                        &raw,
                                        id.as_bytes(),
                                    );
                                    range_min_removes.push((AVAILABILITY_BY_RANGE_MIN, k));
                                }
                                batch.push(WriteRequest::Delete {
                                    data_table: AVAILABILITIES_TABLE,
                                    key: id.as_bytes().to_vec(),
                                    aux_ops: vec![AuxOp {
                                        table: AVAILABILITIES_META_TABLE,
                                        key: id.as_bytes().to_vec(),
                                        value: Some(meta2),
                                    }],
                                    index_removes: {
                                        let mut v = vec![crate::database::make_index_remove(
                                            AVAILABILITY_INCOMPLETE_BY_PEER,
                                            idx_key2,
                                        )];
                                        if !existing.range.dims().is_empty() {
                                            if let Some(k) = idx_peer_ent {
                                                v.push(crate::database::make_index_remove(
                                                    AVAILABILITY_BY_PEER_AND_ENTITY,
                                                    k,
                                                ));
                                            }
                                            for (_tbl, key) in range_min_removes.into_iter() {
                                                v.push(crate::database::make_index_remove(
                                                    AVAILABILITY_BY_RANGE_MIN,
                                                    key,
                                                ));
                                            }
                                        }
                                        v
                                    },
                                });

                                // Track deletion in changed set
                                {
                                    let mut set = self.changed_after_opt.write().await;
                                    set.insert(id);
                                }
                            }
                        }
                        // Overlay-only hint; no DB materialization required here.
                        OptAction::Atom(_r) => {
                            // no-op: Atom is used by the optimizer for adoption only
                        }
                    }
                }

                // Debug: print size of changed-after-opt set for observability
                #[cfg(feature = "harmonizer_debug")]
                {
                    let set = self.changed_after_opt.read().await;
                    println!("after_opt: changed set size={}", set.len());
                }
            } else if std::env::var("KDB_DEBUG_RANGE").ok().is_some() {
                println!("harm.before_update: optimizer returned None");
            }
        }
        if dbg { println!("harm.before_update: END origin={:?}", origin); }

        // Emit UpdateHint for parents when origin is LocalCommit (and not suppressed)
        let suppress = self.suppress_outbox_once.swap(false, Ordering::SeqCst);
        let peers = { self.peers.read().await.clone() };
        // Emit hints on LocalCommit (unless suppressed) AND on RemoteIngest (to propagate coverage),
        // so peers can converge replication decisions.
        let should_hint = (!suppress && matches!(origin, WriteOrigin::LocalCommit))
            || matches!(origin, WriteOrigin::RemoteIngest);
        if !touched.is_empty() && !peers.is_empty() && should_hint {
            let hint = crate::plugins::harmonizer::protocol::UpdateHint {
                peer_uuid: self.peer_ctx.peer_id,
                range: avail.range.clone(),
                epoch: now,
                touched,
            };
            let msg = HarmonizerMsg::UpdateHint(hint);
            // Broadcast hints to all peers (lightweight), regardless of coverage.
            for &peer in &peers {
                let _ = self
                    .outbox_tx
                    .send(OutboxItem::Notify {
                        peer,
                        msg: msg.clone(),
                    })
                    .await;
            }
        }

        Ok(())
    }

    async fn after_write(
        &self,
        db: &KuramotoDb,
        applied: &Vec<crate::database::WriteRequest>,
        origin: WriteOrigin,
    ) -> Result<(), StorageError> {
        let dbg = std::env::var("KDB_DEBUG_RANGE").ok().is_some();
        // Look for any Availability PUT to determine entity hash
        let mut first_dim: Option<u64> = None;
        for req in applied {
            if let crate::database::WriteRequest::Put { data_table, value, .. } = req {
                if std::ptr::eq(*data_table, AVAILABILITIES_TABLE) {
                    if let Ok(av) = Availability::load_and_migrate(value) {
                        first_dim = av.range.dims().first().map(|d| d.hash());
                        break;
                    }
                }
            }
        }
        if let Some(fd) = first_dim {
            let mut pidb = [0u8; 16];
            pidb.copy_from_slice(self.peer_ctx.peer_id.as_bytes());
            let kind: u8 = match origin {
                WriteOrigin::LocalCommit => 0,
                WriteOrigin::RemoteIngest | WriteOrigin::Completer => 1,
                WriteOrigin::Plugin(_) => 0,
            };
            if dbg {
                println!(
                    "harm.after_write: emitting replay context kind={} entity=0x{:016x}",
                    kind, fd
                );
            }
            let _ = crate::plugins::replay::emit_custom(
                db,
                crate::plugins::fnv1a_16("harmonizer"),
                vec![crate::plugins::replay::LogWriteRequest::HarmonizerContext {
                    kind,
                    peer_id: pidb,
                    first_dim_hash: fd,
                }],
            )
            .await;
        } else if dbg {
            println!("harm.after_write: no Availability puts found; skipping replay context");
        }
        Ok(())
    }

    fn attach_db(&self, db: Arc<KuramotoDb>) {
        db.create_table_and_indexes::<Availability>().unwrap();
        db.create_table_and_indexes::<Child>().unwrap();
        db.create_table_and_indexes::<DigestChunk>().unwrap();
        self.db.set(Arc::downgrade(&db)).unwrap();
        // Emit PeerAdded for already-configured peers (added before attach_db).
        let peers_handle = self.peers.clone();
        let db2 = db.clone();
        tokio::spawn(async move {
            let peers = { peers_handle.read().await.clone() };
            for p in peers {
                let mut pid = [0u8; 16];
                pid.copy_from_slice(p.as_bytes());
                let _ = crate::plugins::replay::emit_custom(
                    &db2,
                    crate::plugins::fnv1a_16("harmonizer"),
                    vec![crate::plugins::replay::LogWriteRequest::PeerAdded { peer_id: pid }],
                )
                .await;
                tracing::info!(target: "harmonizer", "replay: PeerAdded (on attach) peer={:02x?}", pid);
                println!(
                    "[harmonizer] replay PeerAdded (on attach) peer={:02x?}",
                    pid
                );
            }
        });
    }

    // (Removed unused try_complete_incomplete_nodes prototype)
}

/*───────────────────────────────────────────────────────────────*/
/* tests                                                         */
/*───────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Clock, MockClock};
    use crate::plugins::communication::router::{Handler, Router, RouterConfig};
    use crate::plugins::communication::transports::{
        Connector, PeerId, PeerResolver,
        inmem::{InMemConnector, InMemResolver},
    };
    use crate::plugins::harmonizer::availability;
    use crate::plugins::harmonizer::optimizer::ActionSet;
    use crate::plugins::harmonizer::optimizer::AvailabilityDraft;
    use crate::plugins::harmonizer::protocol::GetChildrenByRange;
    use crate::plugins::harmonizer::scorers::Scorer;
    use crate::storage_entity::*;

    use bincode::{Decode, Encode};
    use rand::Rng;
    use redb::TableDefinition;
    use std::collections::HashSet;

    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio::task::yield_now;
    use tokio::time::{Duration as TDuration, advance};

    /* ---------------------- Test Entity ---------------------- */

    #[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
    struct Foo {
        id: u32,
    }
    impl StorageEntity for Foo {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> {
            self.id.to_le_bytes().to_vec()
        }
        fn table_def() -> StaticTableDef {
            static TBL: TableDefinition<'static, &'static [u8], Vec<u8>> =
                TableDefinition::new("foo");
            &TBL
        }
        fn aux_tables() -> &'static [AuxTableSpec] {
            static META: TableDefinition<'static, &'static [u8], Vec<u8>> =
                TableDefinition::new("foo_meta");
            static AUX: &[AuxTableSpec] = &[AuxTableSpec {
                role: VERSIONING_AUX_ROLE,
                table: &META,
            }];
            AUX
        }
        fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
            if data.is_empty() {
                return Err(StorageError::Bincode("empty input".into()));
            }
            match data[0] {
                0 => bincode::decode_from_slice::<Self, _>(&data[1..], bincode::config::standard())
                    .map(|(v, _)| v)
                    .map_err(|e| StorageError::Bincode(e.to_string())),
                n => Err(StorageError::Bincode(format!("unknown version {n}"))),
            }
        }
        fn indexes() -> &'static [IndexSpec<Self>] {
            &[]
        }
    }

    #[derive(Clone, Debug, Encode, Decode, PartialEq, Eq)]
    struct Bar {
        id: u32,
    }
    impl StorageEntity for Bar {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> {
            self.id.to_le_bytes().to_vec()
        }
        fn table_def() -> StaticTableDef {
            static TBL: TableDefinition<'static, &'static [u8], Vec<u8>> =
                TableDefinition::new("bar");
            &TBL
        }
        fn aux_tables() -> &'static [AuxTableSpec] {
            static META: TableDefinition<'static, &'static [u8], Vec<u8>> =
                TableDefinition::new("bar_meta");
            static AUX: &[AuxTableSpec] = &[AuxTableSpec {
                role: VERSIONING_AUX_ROLE,
                table: &META,
            }];
            AUX
        }
        fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
            if data.is_empty() {
                return Err(StorageError::Bincode("empty input".into()));
            }
            match data[0] {
                0 => bincode::decode_from_slice::<Self, _>(&data[1..], bincode::config::standard())
                    .map(|(v, _)| v)
                    .map_err(|e| StorageError::Bincode(e.to_string())),
                n => Err(StorageError::Bincode(format!("unknown version {n}"))),
            }
        }
        fn indexes() -> &'static [IndexSpec<Self>] {
            &[]
        }
    }

    struct NullScore;
    #[async_trait]
    impl Scorer for NullScore {
        async fn score(
            &self,
            _db: &KuramotoDb,
            _txn: &ReadTransaction,
            _ctx: &PeerContext,
            _cand: &AvailabilityDraft,
            _overlay: &ActionSet,
        ) -> f32 {
            0.0
        }
    }

    /* ---------------------- Helpers ---------------------- */

    fn pid(x: u8) -> PeerId {
        let mut b = [0u8; 16];
        b[0] = x;
        PeerId::from_bytes(b)
    }

    async fn spin() {
        // drive router tasks deterministically (start_paused tests)
        yield_now().await;
        advance(TDuration::from_millis(1)).await;
        yield_now().await;
    }

    /* ---------------------- Tests ---------------------- */

    #[tokio::test(start_paused = true)]
    async fn replay_logs_context_and_snapshots_and_reconstructs() {
        use crate::plugins::replay::ReplayPlugin;
        use crate::plugins::replay::{self, LogWriteRequest, SnapshotAvailability, SnapshotChild};
        use crate::tables::TableHash;

        // Setup DB with ReplayPlugin only (we'll write Availability/Child rows directly)
        let clock = Arc::new(MockClock::new(100));
        let plugin = Arc::new(ReplayPlugin::new());
        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("replay_snap.redb").to_str().unwrap(),
            clock.clone(),
            vec![plugin],
        )
        .await;

        // Ensure tables
        db.create_table_and_indexes::<availability::Availability>()
            .unwrap();
        db.create_table_and_indexes::<crate::plugins::harmonizer::child_set::Child>()
            .unwrap();
        db.create_table_and_indexes::<crate::plugins::harmonizer::child_set::DigestChunk>()
            .unwrap();

        // Build a tiny tree: parent -> leaf1, leaf2
        let dim = TableHash { hash: 42 };
        let peer = UuidBytes::new();
        let leaf = |min: u8, max: u8| availability::Availability {
            key: UuidBytes::new(),
            peer_id: peer,
            range: crate::plugins::harmonizer::range_cube::RangeCube::new(
                smallvec::smallvec![dim],
                smallvec::smallvec![vec![min]],
                smallvec::smallvec![vec![max]],
            )
            .unwrap(),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: clock.now(),
            complete: true,
        };
        let mut l1 = leaf(10, 11);
        let mut l2 = leaf(11, 12);
        db.put(l1.clone()).await.unwrap();
        db.put(l2.clone()).await.unwrap();
        let parent = availability::Availability {
            key: UuidBytes::new(),
            peer_id: peer,
            range: crate::plugins::harmonizer::range_cube::RangeCube::new(
                smallvec::smallvec![dim],
                smallvec::smallvec![vec![10]],
                smallvec::smallvec![vec![12]],
            )
            .unwrap(),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: clock.now(),
            complete: true,
        };
        db.put(parent.clone()).await.unwrap();
        // Link children (logs deltas)
        let mut cs = crate::plugins::harmonizer::child_set::ChildSet::open(&db, parent.key)
            .await
            .unwrap();
        cs.add_child(&db, l1.key).await.unwrap();
        cs.add_child(&db, l2.key).await.unwrap();

        // Emit snapshot of current state
        clock.advance(1).await;
        let pid_arr = *peer.as_bytes();
        let avail_to_snap = |a: &availability::Availability| SnapshotAvailability {
            availability_id: *a.key.as_bytes(),
            peer_id: *a.peer_id.as_bytes(),
            level: a.level,
            complete: a.complete,
            dims: a.range.dims().iter().map(|d| d.hash).collect(),
            mins: a.range.mins().to_vec(),
            maxs: a.range.maxs().to_vec(),
        };
        let c_to_snap = |p: UuidBytes, ord: u32, c: UuidBytes| SnapshotChild {
            parent: *p.as_bytes(),
            ordinal: ord,
            child: *c.as_bytes(),
        };
        let items1 = vec![
            LogWriteRequest::HarmonizerSnapshotStart {
                peer_id: pid_arr,
                first_dim_hash: dim.hash,
                total_avails: 3,
                total_edges: 2,
                version: 1,
            },
            LogWriteRequest::HarmonizerSnapshotChunk {
                chunk_no: 0,
                availabilities: vec![
                    avail_to_snap(&parent),
                    avail_to_snap(&l1),
                    avail_to_snap(&l2),
                ],
                children: vec![
                    c_to_snap(parent.key, 0, l1.key),
                    c_to_snap(parent.key, 1, l2.key),
                ],
            },
            LogWriteRequest::HarmonizerSnapshotEnd {
                chunks: 1,
                checksum: 0,
            },
        ];
        replay::emit_custom(&db, crate::plugins::fnv1a_16("harmonizer"), items1)
            .await
            .unwrap();
        let t1 = clock.now();

        // Apply a delta: add a third leaf and link it
        clock.advance(1).await;
        let l3 = leaf(12, 13);
        db.put(l3.clone()).await.unwrap();
        let mut cs = crate::plugins::harmonizer::child_set::ChildSet::open(&db, parent.key)
            .await
            .unwrap();
        cs.add_child(&db, l3.key).await.unwrap();
        let t2 = clock.now();

        // Emit snapshot2 with the full state (parent + 3 leaves, 3 edges)
        clock.advance(1).await;
        let items2 = vec![
            LogWriteRequest::HarmonizerSnapshotStart {
                peer_id: pid_arr,
                first_dim_hash: dim.hash,
                total_avails: 4,
                total_edges: 3,
                version: 1,
            },
            LogWriteRequest::HarmonizerSnapshotChunk {
                chunk_no: 0,
                availabilities: vec![
                    avail_to_snap(&parent),
                    avail_to_snap(&l1),
                    avail_to_snap(&l2),
                    avail_to_snap(&l3),
                ],
                children: vec![
                    c_to_snap(parent.key, 0, l1.key),
                    c_to_snap(parent.key, 1, l2.key),
                    c_to_snap(parent.key, 2, l3.key),
                ],
            },
            LogWriteRequest::HarmonizerSnapshotEnd {
                chunks: 1,
                checksum: 0,
            },
        ];
        replay::emit_custom(&db, crate::plugins::fnv1a_16("harmonizer"), items2)
            .await
            .unwrap();
        let t3 = clock.now();

        // Helper: fetch replay events and filter by ts window
        let events: Vec<crate::plugins::replay::ReplayEvent> = db
            .range_by_pk::<crate::plugins::replay::ReplayEvent>(
                &0u64.to_be_bytes(),
                &u64::MAX.to_be_bytes(),
                None,
            )
            .await
            .unwrap();

        // Reconstruct from snapshot1 then apply deltas between t1..t3
        let mut av_set = std::collections::BTreeSet::<(
            [u8; 16],
            [u8; 16],
            u16,
            bool,
            Vec<u64>,
            Vec<Vec<u8>>,
            Vec<Vec<u8>>,
        )>::new();
        let mut edges = std::collections::BTreeSet::<([u8; 16], u32, [u8; 16])>::new();

        // parse snapshot1
        for ev in events.iter().filter(|e| e.ts == t1) {
            for it in &ev.batch {
                match it {
                    LogWriteRequest::HarmonizerSnapshotChunk {
                        availabilities,
                        children,
                        ..
                    } => {
                        for a in availabilities {
                            av_set.insert((
                                a.availability_id,
                                a.peer_id,
                                a.level,
                                a.complete,
                                a.dims.clone(),
                                a.mins.clone(),
                                a.maxs.clone(),
                            ));
                        }
                        for c in children {
                            edges.insert((c.parent, c.ordinal, c.child));
                        }
                    }
                    _ => {}
                }
            }
        }

        // apply deltas: any Put/Delete on Availability or Child between t1..t3
        let avail_th = TableHash::from(availability::AVAILABILITIES_TABLE).hash();
        let child_th =
            TableHash::from(crate::plugins::harmonizer::child_set::AVAIL_CHILDREN_TBL).hash();
        for ev in events.iter().filter(|e| e.ts > t1 && e.ts < t3) {
            for it in &ev.batch {
                match it {
                    LogWriteRequest::Put {
                        data_table_hash,
                        value,
                        ..
                    } if *data_table_hash == avail_th => {
                        let a: availability::Availability =
                            availability::Availability::load_and_migrate(&value).unwrap();
                        av_set.insert((
                            *a.key.as_bytes(),
                            *a.peer_id.as_bytes(),
                            a.level,
                            a.complete,
                            a.range.dims().iter().map(|d| d.hash).collect(),
                            a.range.mins().to_vec(),
                            a.range.maxs().to_vec(),
                        ));
                    }
                    LogWriteRequest::Delete {
                        data_table_hash,
                        key,
                        ..
                    } if *data_table_hash == avail_th => {
                        let mut idb = [0u8; 16];
                        idb.copy_from_slice(&key);
                        av_set.retain(|(aid, ..)| aid != &idb);
                    }
                    LogWriteRequest::Put {
                        data_table_hash,
                        value,
                        ..
                    } if *data_table_hash == child_th => {
                        let c: crate::plugins::harmonizer::child_set::Child =
                            crate::plugins::harmonizer::child_set::Child::load_and_migrate(&value)
                                .unwrap();
                        edges.insert((*c.parent.as_bytes(), c.ordinal, *c.child_id.as_bytes()));
                    }
                    LogWriteRequest::Delete {
                        data_table_hash,
                        key,
                        ..
                    } if *data_table_hash == child_th => {
                        // child pk = parent • ordinal (le)
                        if key.len() >= 20 {
                            let mut pb = [0u8; 16];
                            pb.copy_from_slice(&key[..16]);
                            let ord = u32::from_le_bytes([key[16], key[17], key[18], key[19]]);
                            edges.retain(|(p, o, _)| !(p == &pb && *o == ord));
                        }
                    }
                    _ => {}
                }
            }
        }

        // Extract snapshot2 content and compare
        let mut av_set2 = std::collections::BTreeSet::new();
        let mut edges2 = std::collections::BTreeSet::new();
        for ev in events.iter().filter(|e| e.ts == t3) {
            for it in &ev.batch {
                if let LogWriteRequest::HarmonizerSnapshotChunk {
                    availabilities,
                    children,
                    ..
                } = it
                {
                    for a in availabilities {
                        av_set2.insert((
                            a.availability_id,
                            a.peer_id,
                            a.level,
                            a.complete,
                            a.dims.clone(),
                            a.mins.clone(),
                            a.maxs.clone(),
                        ));
                    }
                    for c in children {
                        edges2.insert((c.parent, c.ordinal, c.child));
                    }
                }
            }
        }

        assert_eq!(
            av_set, av_set2,
            "availability sets should match snapshot2 after applying deltas"
        );
        assert_eq!(
            edges, edges2,
            "edge sets should match snapshot2 after applying deltas"
        );
    }

    /// Writing an entity should create at least one Availability (seed+optimize).
    #[tokio::test(start_paused = true)]
    async fn entity_write_creates_availability() {
        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());

        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let peer_id = local_peer.peer_id; // copy peer id before moving local_peer
        use crate::plugins::harmonizer::scorers::server_scorer::{
            ServerScorer, ServerScorerParams,
        };
        let optimizer = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            local_peer.clone(),
        ));
        let hz = Harmonizer::new(
            router,
            optimizer,
            HashSet::from([Foo::table_def().name()]),
            local_peer,
        );

        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("t.redb").to_str().unwrap(),
            clock,
            vec![hz.clone()],
        )
        .await;

        db.create_table_and_indexes::<Foo>().unwrap();

        // Insert one atom
        db.put(Foo { id: 1 }).await.unwrap();
        spin().await; // allow any background tasks to run

        // Assert at least one Availability row was created as a side-effect
        let avs: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();
        assert!(!avs.is_empty(), "expected at least one availability to be created");
        // Optional: ensure one availability belongs to the local peer
        assert!(
            avs.iter().any(|a| a.peer_id == peer_id),
            "at least one availability should belong to the local peer"
        );
    }

    /// Inserting adjacent atoms should lead the optimizer to create an L0 range that
    /// covers both; inserting a middle atom afterwards should be a no-op (already covered).
    #[tokio::test(start_paused = true)]
    async fn adjacent_atoms_expand_then_middle_is_covered() {
        use crate::plugins::harmonizer::scorers::server_scorer::{
            ServerScorer, ServerScorerParams,
        };

        // Tune scorer to encourage grouping and avoid replication penalties
        let params = ServerScorerParams {
            rent: 0.1,
            child_target: 2,
            child_weight: 2.0,
            replication_target: 0,
            replication_weight: 0.0,
            level_weight: 0.0001,
        };

        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());
        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let peer_id = local_peer.peer_id;
        let optimizer = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(params)),
            local_peer.clone(),
        ));
        let hz = Harmonizer::new(
            router,
            optimizer,
            HashSet::from([Foo::table_def().name()]),
            local_peer,
        );

        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("adj.redb").to_str().unwrap(),
            clock,
            vec![hz.clone()],
        )
        .await;
        db.create_table_and_indexes::<Foo>().unwrap();

        // A: insert first atom
        db.put(Foo { id: 0x10 }).await.unwrap();
        spin().await;
        // B: insert adjacent atom
        db.put(Foo { id: 0x30 }).await.unwrap();
        spin().await;

        // Collect availabilities
        let avs: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();

        // Expect at least one local L0 availability that covers both inserted atoms
        let pk_a = 0x10u32.to_le_bytes().to_vec();
        let pk_b = 0x30u32.to_le_bytes().to_vec();
        let r_a = Harmonizer::leaf_range_from_pk_and_indexes(Foo::table_def(), &pk_a, &[]);
        let r_b = Harmonizer::leaf_range_from_pk_and_indexes(Foo::table_def(), &pk_b, &[]);
        let has_covering_l0 = avs.iter().any(|a| {
            a.peer_id == peer_id
                && a.level == 0
                && a.range.contains(&r_a)
                && a.range.contains(&r_b)
        });
        assert!(
            has_covering_l0,
            "expected an L0 availability that covers both A and B"
        );

        // Record current count; inserting a middle atom C should be covered and result in no new rows
        let before_count = avs
            .iter()
            .filter(|a| a.peer_id == peer_id)
            .count();
        db.put(Foo { id: 0x20 }).await.unwrap(); // middle
        spin().await;

        let after_avs: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();
        let after_count = after_avs
            .iter()
            .filter(|a| a.peer_id == peer_id)
            .count();
        assert_eq!(before_count, after_count, "middle atom should be covered");
    }

    #[tokio::test(start_paused = true)]
    async fn router_probe_headers_smoke() {
        use crate::plugins::harmonizer::scorers::server_scorer::{
            ServerScorer, ServerScorerParams,
        };

        let clock = Arc::new(MockClock::new(0));
        let rtr_a = Router::new(RouterConfig::default(), clock.clone());
        let rtr_b = Router::new(RouterConfig::default(), clock.clone());
        let rtr_c = Router::new(RouterConfig::default(), clock.clone());

        // connect full mesh
        let ns: u64 = 42; // deterministic for test
        let pid_a = pid(1);
        let pid_b = pid(2);
        let pid_c = pid(3);
        let conn_a = InMemConnector::with_namespace(pid_a, ns);
        let conn_b = InMemConnector::with_namespace(pid_b, ns);
        let conn_c = InMemConnector::with_namespace(pid_c, ns);
        let resolver = InMemResolver;
        rtr_a.connect_peer(
            pid_b,
            conn_a
                .dial(&resolver.resolve(pid_b).await.unwrap())
                .await
                .unwrap(),
        );
        rtr_b.connect_peer(
            pid_a,
            conn_b
                .dial(&resolver.resolve(pid_a).await.unwrap())
                .await
                .unwrap(),
        );
        rtr_a.connect_peer(
            pid_c,
            conn_a
                .dial(&resolver.resolve(pid_c).await.unwrap())
                .await
                .unwrap(),
        );
        rtr_c.connect_peer(
            pid_a,
            conn_c
                .dial(&resolver.resolve(pid_a).await.unwrap())
                .await
                .unwrap(),
        );
        rtr_b.connect_peer(
            pid_c,
            conn_b
                .dial(&resolver.resolve(pid_c).await.unwrap())
                .await
                .unwrap(),
        );
        rtr_c.connect_peer(
            pid_b,
            conn_c
                .dial(&resolver.resolve(pid_b).await.unwrap())
                .await
                .unwrap(),
        );

        let params = ServerScorerParams {
            replication_target: 2,
            ..Default::default()
        };
        let sc_a = ServerScorer::new(params.clone());
        let sc_b = ServerScorer::new(params.clone());
        let sc_c = ServerScorer::new(params.clone());

        // Harmonizers
        let peer_a = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let peer_b = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let peer_c = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let opt_a = Arc::new(BasicOptimizer::new(Box::new(sc_a), peer_a.clone()));
        let opt_b = Arc::new(BasicOptimizer::new(Box::new(sc_b), peer_b.clone()));
        let opt_c = Arc::new(BasicOptimizer::new(Box::new(sc_c), peer_c.clone()));
        let hz_a = Harmonizer::new(
            rtr_a.clone(),
            opt_a,
            HashSet::from([Foo::table_def().name()]),
            peer_a.clone(),
        );
        let hz_b = Harmonizer::new(
            rtr_b.clone(),
            opt_b,
            HashSet::from([Foo::table_def().name()]),
            peer_b.clone(),
        );
        let hz_c = Harmonizer::new(
            rtr_c.clone(),
            opt_c,
            HashSet::from([Foo::table_def().name()]),
            peer_c,
        );

        // peers for notifications
        hz_a.add_peer(pid_b).await;
        hz_a.add_peer(pid_c).await;
        hz_b.add_peer(pid_a).await;
        hz_b.add_peer(pid_c).await;
        hz_c.add_peer(pid_a).await;
        hz_c.add_peer(pid_b).await;

        // DBs
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let dir_c = tempdir().unwrap();
        let db_a = KuramotoDb::new(
            dir_a.path().join("a.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_a.clone()],
        )
        .await;
        let db_b = KuramotoDb::new(
            dir_b.path().join("b.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_b.clone()],
        )
        .await;
        let db_c = KuramotoDb::new(
            dir_c.path().join("c.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_c.clone()],
        )
        .await;
        db_a.create_table_and_indexes::<Foo>().unwrap();
        db_b.create_table_and_indexes::<Foo>().unwrap();
        db_c.create_table_and_indexes::<Foo>().unwrap();

        // Seed
        db_a.put(Foo { id: 11 }).await.unwrap();
        spin().await;

        // Seed a local header on B so at least one remote reports coverage (no L0 auto-persist now)
        {
            let pk = 11u32.to_le_bytes().to_vec();
            let pk_dim = TableHash::from(Foo::table_def());
            let mut hi = pk.clone();
            hi.push(1);
            let cube =
                RangeCube::new(smallvec![pk_dim], smallvec![pk.clone()], smallvec![hi]).unwrap();
            let av = Availability {
                key: UuidBytes::new(),
                peer_id: peer_b.peer_id,
                range: cube.clone(),
                level: 0,
                schema_hash: 0,
                version: 0,
                updated_at: db_b.get_clock().now(),
                complete: true,
            };
            db_b.put(av).await.unwrap();
        }

        // Ask B and C for headers that cover a tiny window around id=11
        use crate::tables::TableHash;
        let pk_dim = TableHash::from(Foo::table_def());
        let cube = RangeCube::new(
            smallvec![pk_dim],
            smallvec![vec![11u8, 0, 0, 0]],
            smallvec![vec![11u8, 0, 0, 0, 1]],
        )
        .unwrap();
        let got_b = rtr_a
            .request_on::<HarmonizerMsg, HarmonizerResp>(
                PROTO_HARMONIZER,
                pid_b,
                &HarmonizerMsg::GetChildrenByRange(GetChildrenByRange {
                    range: cube.clone(),
                    cursor: None,
                    max: 1,
                }),
                RouterConfig::default().default_timeout,
            )
            .await
            .unwrap();
        let got_c = rtr_a
            .request_on::<HarmonizerMsg, HarmonizerResp>(
                PROTO_HARMONIZER,
                pid_c,
                &HarmonizerMsg::GetChildrenByRange(GetChildrenByRange {
                    range: cube.clone(),
                    cursor: None,
                    max: 1,
                }),
                RouterConfig::default().default_timeout,
            )
            .await
            .unwrap();
        let mut seen = 0;
        if let HarmonizerResp::Children(resp) = got_b {
            if !resp.headers.is_empty() {
                seen += 1;
            }
        }
        if let HarmonizerResp::Children(resp) = got_c {
            if !resp.headers.is_empty() {
                seen += 1;
            }
        }
        assert!(
            seen >= 1,
            "expected at least one remote peer to report coverage headers"
        );
    }

    /// When the optimizer proposes a parent, Harmonizer enqueues the parent and links children
    /// via the child table in the same batch; digest chunks are also written.
    #[tokio::test(start_paused = true)]
    async fn enqueues_parent_and_children_in_batch() {
        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());

        // A simple scorer that strongly prefers larger ranges and higher levels
        struct GrowingScorer;
        #[async_trait]
        impl Scorer for GrowingScorer {
            async fn score(
                &self,
                _db: &KuramotoDb,
                _txn: &ReadTransaction,
                _ctx: &PeerContext,
                cand: &AvailabilityDraft,
                _overlay: &ActionSet,
            ) -> f32 {
                cand.range.approx_volume() as f32 + (cand.level as f32) * 10.0
            }
        }
        let scorer = GrowingScorer;

        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let optimizer = Arc::new(BasicOptimizer::new(Box::new(scorer), local_peer.clone()));
        let hz = Harmonizer::new(
            router,
            optimizer,
            HashSet::from([Foo::table_def().name()]),
            local_peer,
        );

        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("t.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz.clone()],
        )
        .await;
        db.create_table_and_indexes::<Foo>().unwrap();

        // Seed two local leaves manually (no auto L0 persistence now)
        let peer_id = hz.peer_ctx.peer_id;
        let mk_leaf = |id: u32| -> Availability {
            let pk = id.to_le_bytes().to_vec();
            let r = Harmonizer::leaf_range_from_pk_and_indexes(Foo::table_def(), &pk, &[]);
            Availability {
                key: UuidBytes::new(),
                peer_id,
                range: r,
                level: 0,
                schema_hash: 0,
                version: 0,
                updated_at: clock.now(),
                complete: true,
            }
        };
        let leaf_a = mk_leaf(10);
        let leaf_b = mk_leaf(11);
        db.put(leaf_a.clone()).await.unwrap();
        db.put(leaf_b.clone()).await.unwrap();

        // Synthesize a parent adopting both leaves, including digest chunk
        let parent_id = UuidBytes::new();
        let parent = Availability {
            key: parent_id,
            peer_id,
            range: leaf_a.range.clone(),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: clock.now(),
            complete: true,
        };
        db.put(parent.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, parent_id).await.unwrap();
        cs.add_child(&db, leaf_a.key).await.unwrap();
        cs.add_child(&db, leaf_b.key).await.unwrap();
        // Write digest chunks for parent
        let mut enc = Enc::new(&vec![leaf_a.key, leaf_b.key]);
        for chunk_no in 0..STORED_CHUNKS {
            let mut buf = Vec::with_capacity(CELLS_PER_CHUNK);
            for _ in 0..CELLS_PER_CHUNK {
                buf.push(enc.next_coded());
            }
            let bytes = bincode::encode_to_vec(&buf, bincode::config::standard()).unwrap();
            let chunk = DigestChunk {
                parent: parent_id,
                chunk_no: chunk_no as u32,
                bytes,
            };
            db.put(chunk).await.unwrap();
        }

        // Query all availabilities
        let avs: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();
        let leaves: Vec<_> = avs.iter().filter(|a| a.level == 0).collect();
        let parents: Vec<_> = avs.iter().filter(|a| a.level >= 1).collect();
        assert!(leaves.len() >= 2, "expected at least two leaves");
        assert!(parents.len() >= 1, "expected at least one parent");

        // Pick a parent and verify child rows exist and match local leaves
        let mut found_any = false;
        for p in &parents {
            let mut lo = p.key.as_bytes().to_vec();
            let mut hi = lo.clone();
            hi.push(0xFF);
            let kids: Vec<Child> = db.range_by_pk::<Child>(&lo, &hi, None).await.unwrap();
            if !kids.is_empty() {
                found_any = true;
                for c in &kids {
                    assert_eq!(c.parent, p.key);
                    let child: Availability = db
                        .get_data::<Availability>(c.child_id.as_bytes())
                        .await
                        .unwrap();
                    assert_eq!(child.peer_id, p.peer_id);
                }
                // Digest chunk for parent must exist (chunk 0)
                let mut pk = p.key.as_bytes().to_vec();
                pk.extend_from_slice(&0u32.to_le_bytes());
                let _chunk: DigestChunk = db.get_data::<DigestChunk>(&pk).await.unwrap();
                break;
            }
        }
        assert!(found_any, "at least one parent should have linked children");

        // Roots: leaves adopted by this parent should not be roots; the parent should be a root
        let txn = db.begin_read_txn().unwrap();
        let roots = availability::roots_for_peer(&db, Some(&txn), &parents[0].peer_id)
            .await
            .unwrap();
        let root_ids: std::collections::HashSet<_> = roots.into_iter().map(|a| a.key).collect();
        // At least one parent should be a root, and its children should not be roots
        let mut asserted = false;
        for p in &parents {
            let mut lo = p.key.as_bytes().to_vec();
            let mut hi = lo.clone();
            hi.push(0xFF);
            let kids: Vec<Child> = db.range_by_pk::<Child>(&lo, &hi, None).await.unwrap();
            if !kids.is_empty() {
                assert!(root_ids.contains(&p.key), "parent should be a root");
                for c in &kids {
                    assert!(
                        !root_ids.contains(&c.child_id),
                        "adopted child should no longer be a root"
                    );
                }
                asserted = true;
                break;
            }
        }
        assert!(
            asserted,
            "no parent with children to assert root properties"
        );
    }

    /// Wiring test: outbox emits UpdateWithAttestation to a peer without a Harmonizer.
    /// We attach a tiny probe handler on the peer to capture the notify.
    #[tokio::test(start_paused = true)]
    async fn outbox_emits_update_notify_to_peer() {
        // shared clock
        let clock = Arc::new(MockClock::new(0));

        // routers
        let rtr_a = Router::new(RouterConfig::default(), clock.clone());
        let rtr_b = Router::new(RouterConfig::default(), clock.clone());

        // peer IDs + inmem transport
        let ns: u64 = rand::rng().random();
        let pid_a = pid(1);
        let pid_b = pid(2);
        let conn_a = InMemConnector::with_namespace(pid_a, ns);
        let conn_b = InMemConnector::with_namespace(pid_b, ns);
        let resolver = InMemResolver;

        let a_to_b = conn_a
            .dial(&resolver.resolve(pid_b).await.unwrap())
            .await
            .unwrap();
        let b_to_a = conn_b
            .dial(&resolver.resolve(pid_a).await.unwrap())
            .await
            .unwrap();

        rtr_a.connect_peer(pid_b, a_to_b);
        rtr_b.connect_peer(pid_a, b_to_a);

        // Build Harmonizer on A (will register its protocol on A’s router)
        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };
        use crate::plugins::harmonizer::scorers::server_scorer::{
            ServerScorer, ServerScorerParams,
        };
        let optimizer = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            local_peer.clone(),
        ));
        let hz_a = Harmonizer::new(
            rtr_a,
            optimizer,
            HashSet::from([Foo::table_def().name()]),
            local_peer,
        );
        hz_a.add_peer(pid_b).await; // tell A to notify B

        // DB A
        let dir_a = tempdir().unwrap();
        let db_a = KuramotoDb::new(
            dir_a.path().join("a.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_a.clone()],
        )
        .await;
        db_a.create_table_and_indexes::<Foo>().unwrap();

        // On B, install a probe handler (NO Harmonizer on B to avoid proto clash)
        #[derive(Clone)]
        struct Probe {
            tx: mpsc::Sender<UpdateWithAttestation>,
        }

        #[async_trait::async_trait]
        impl Handler for Probe {
            async fn on_request(&self, _peer: PeerId, _body: &[u8]) -> Result<Vec<u8>, String> {
                // Not used in this test
                Ok(Vec::new())
            }

            async fn on_notify(&self, _peer: PeerId, body: &[u8]) -> Result<(), String> {
                // decode and forward just UpdateWithAttestation
                if let Ok((msg, _)) = bincode::decode_from_slice::<HarmonizerMsg, _>(
                    body,
                    bincode::config::standard(),
                ) {
                    if let HarmonizerMsg::UpdateWithAttestation(att) = msg {
                        let _ = self.tx.send(att).await;
                    }
                }
                Ok(())
            }
        }

        // Register probe on B with the same proto id
        let (probe_tx, mut probe_rx) = mpsc::channel::<UpdateWithAttestation>(8);
        rtr_b.set_handler(PROTO_HARMONIZER, Arc::new(Probe { tx: probe_tx }));

        // Trigger: put Foo on A → Harmonizer emits UpdateWithAttestation → Probe should see it
        db_a.put(Foo { id: 42 }).await.unwrap();
        spin().await;

        let got = probe_rx.try_recv().ok();
        assert!(
            got.is_some(),
            "peer B should receive an UpdateWithAttestation notify"
        );

        let att = got.unwrap();
        assert_eq!(att.table, Foo::table_def().name().to_string());
        assert_eq!(att.pk, 42u32.to_le_bytes().to_vec());
        assert_eq!(att.level, 0);
        assert_eq!(att.child_count, 1);
        assert!(
            !att.entity_bytes.is_empty(),
            "v0 pushes entity bytes inline"
        );
    }

    /// RPC: GetChildrenDigest returns chunk0 bytes for a parent with children.
    #[tokio::test(start_paused = true)]
    async fn rpc_get_children_digest_chunk0_smoke() {
        let clock = Arc::new(MockClock::new(0));
        let rtr_a = Router::new(RouterConfig::default(), clock.clone());
        let rtr_b = Router::new(RouterConfig::default(), clock.clone());

        // in-mem link
        let ns: u64 = rand::rng().random();
        let pid_a = pid(1);
        let pid_b = pid(2);
        let conn_a = InMemConnector::with_namespace(pid_a, ns);
        let conn_b = InMemConnector::with_namespace(pid_b, ns);
        let resolver = InMemResolver;
        let a_to_b = conn_a
            .dial(&resolver.resolve(pid_b).await.unwrap())
            .await
            .unwrap();
        let b_to_a = conn_b
            .dial(&resolver.resolve(pid_a).await.unwrap())
            .await
            .unwrap();
        rtr_a.connect_peer(pid_b, a_to_b);
        rtr_b.connect_peer(pid_a, b_to_a);

        // Harmonizer A
        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };
        use crate::plugins::harmonizer::scorers::server_scorer::{
            ServerScorer, ServerScorerParams,
        };
        let optimizer = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            local_peer.clone(),
        ));
        let hz_a = Harmonizer::new(
            rtr_a.clone(),
            optimizer,
            HashSet::from([Foo::table_def().name()]),
            local_peer,
        );

        // DB + seed data
        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("chunk.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_a.clone()],
        )
        .await;
        db.create_table_and_indexes::<Foo>().unwrap();
        // Synthesize two leaves and a parent so chunk0 is non-empty
        let mk_leaf = |peer: UuidBytes, id: u32| -> Availability {
            let pk = id.to_le_bytes().to_vec();
            let r = Harmonizer::leaf_range_from_pk_and_indexes(Foo::table_def(), &pk, &[]);
            Availability {
                key: UuidBytes::new(),
                peer_id: peer,
                range: r,
                level: 0,
                schema_hash: 0,
                version: 0,
                updated_at: clock.now(),
                complete: true,
            }
        };
        let parent_peer = UuidBytes::new();
        let l1 = mk_leaf(parent_peer, 10);
        let l2 = mk_leaf(parent_peer, 11);
        db.put(l1.clone()).await.unwrap();
        db.put(l2.clone()).await.unwrap();
        let pid = UuidBytes::new();
        let parent = Availability {
            key: pid,
            peer_id: parent_peer,
            range: l1.range.clone(),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: clock.now(),
            complete: true,
        };
        db.put(parent.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, pid).await.unwrap();
        cs.add_child(&db, l1.key).await.unwrap();
        cs.add_child(&db, l2.key).await.unwrap();

        // request chunk0 from B -> A
        let req = crate::plugins::harmonizer::protocol::HarmonizerMsg::GetChildrenDigest(
            crate::plugins::harmonizer::protocol::GetChildrenDigest {
                parent_uuid: parent.key,
                chunk_no: 0,
            },
        );
        let bytes = rtr_b
            .request_on::<_, crate::plugins::harmonizer::protocol::HarmonizerResp>(
                PROTO_HARMONIZER,
                pid_a,
                &req,
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap();
        let resp = match bytes {
            crate::plugins::harmonizer::protocol::HarmonizerResp::Digest(d) => d,
            _ => panic!("wrong resp"),
        };
        assert!(!resp.bytes.is_empty(), "chunk0 should be non-empty");

        // verify matches DB read of chunk0
        let mut pk = parent.key.as_bytes().to_vec();
        pk.extend_from_slice(&0u32.to_le_bytes());
        let ch: DigestChunk = db.get_data::<DigestChunk>(&pk).await.unwrap();
        assert_eq!(resp.bytes, ch.bytes);
    }

    /// RPC: GetChildrenHeaders pages over child Availability headers for a parent.
    #[tokio::test(start_paused = true)]
    async fn rpc_get_children_headers_pages() {
        let clock = Arc::new(MockClock::new(0));
        let rtr_a = Router::new(RouterConfig::default(), clock.clone());
        let rtr_b = Router::new(RouterConfig::default(), clock.clone());

        // link
        let ns: u64 = rand::rng().random();
        let pid_a = pid(3);
        let pid_b = pid(4);
        let conn_a = InMemConnector::with_namespace(pid_a, ns);
        let conn_b = InMemConnector::with_namespace(pid_b, ns);
        let resolver = InMemResolver;
        let a_to_b = conn_a
            .dial(&resolver.resolve(pid_b).await.unwrap())
            .await
            .unwrap();
        let b_to_a = conn_b
            .dial(&resolver.resolve(pid_a).await.unwrap())
            .await
            .unwrap();
        rtr_a.connect_peer(pid_b, a_to_b);
        rtr_b.connect_peer(pid_a, b_to_a);

        // Harmonizer A
        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };
        use crate::plugins::harmonizer::scorers::server_scorer::{
            ServerScorer, ServerScorerParams,
        };
        let optimizer = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            local_peer.clone(),
        ));
        let hz_a = Harmonizer::new(
            rtr_a.clone(),
            optimizer,
            HashSet::from([Foo::table_def().name()]),
            local_peer,
        );

        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("headers.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_a.clone()],
        )
        .await;
        db.create_table_and_indexes::<Foo>().unwrap();
        for id in 20..=23 {
            db.put(Foo { id }).await.unwrap();
        }
        spin().await;

        // Synthesize a parent with two children for paging deterministically
        let mk_leaf = |peer: UuidBytes, id: u8| -> Availability {
            let pk = (id as u32).to_le_bytes().to_vec();
            let r = Harmonizer::leaf_range_from_pk_and_indexes(Foo::table_def(), &pk, &[]);
            Availability {
                key: UuidBytes::new(),
                peer_id: peer,
                range: r,
                level: 0,
                schema_hash: 0,
                version: 0,
                updated_at: clock.now(),
                complete: true,
            }
        };
        let parent_peer = UuidBytes::new();
        let l1 = mk_leaf(parent_peer, 21);
        let l2 = mk_leaf(parent_peer, 22);
        db.put(l1.clone()).await.unwrap();
        db.put(l2.clone()).await.unwrap();
        let pid = UuidBytes::new();
        let pav = Availability {
            key: pid,
            peer_id: parent_peer,
            range: l1.range.clone(),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: clock.now(),
            complete: true,
        };
        db.put(pav.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, pid).await.unwrap();
        cs.add_child(&db, l1.key).await.unwrap();
        cs.add_child(&db, l2.key).await.unwrap();
        let parent = pav;

        // page with max=2
        let mut cursor: Option<Vec<u8>> = None;
        let mut total = 0usize;
        for _ in 0..2 {
            let req = crate::plugins::harmonizer::protocol::HarmonizerMsg::GetChildrenHeaders(
                crate::plugins::harmonizer::protocol::GetChildrenHeaders {
                    parent_uuid: parent.key,
                    cursor: cursor.clone(),
                    max: 2,
                },
            );
            let resp = rtr_b
                .request_on::<_, crate::plugins::harmonizer::protocol::HarmonizerResp>(
                    PROTO_HARMONIZER,
                    pid_a,
                    &req,
                    std::time::Duration::from_secs(1),
                )
                .await
                .unwrap();
            let r = match resp {
                crate::plugins::harmonizer::protocol::HarmonizerResp::Children(c) => c,
                _ => panic!("wrong resp"),
            };
            total += r.headers.len();
            cursor = r.next;
            if cursor.is_none() {
                break;
            }
        }
        assert!(total >= 2, "should page headers");
    }

    /// RIBLT-based repair: B is missing one child under a known parent; compare with A using chunk0 only.
    #[tokio::test(start_paused = true)]
    async fn ibf_repairs_missing_child_with_chunk0() {
        let clock = Arc::new(MockClock::new(0));
        let rtr_a = Router::new(RouterConfig::default(), clock.clone());
        let rtr_b = Router::new(RouterConfig::default(), clock.clone());

        // connect A<->B
        let ns: u64 = rand::rng().random();
        let pid_a = pid(11);
        let pid_b = pid(12);
        let conn_a = InMemConnector::with_namespace(pid_a, ns);
        let conn_b = InMemConnector::with_namespace(pid_b, ns);
        let resolver = InMemResolver;
        let a_to_b = conn_a
            .dial(&resolver.resolve(pid_b).await.unwrap())
            .await
            .unwrap();
        let b_to_a = conn_b
            .dial(&resolver.resolve(pid_a).await.unwrap())
            .await
            .unwrap();
        rtr_a.connect_peer(pid_b, a_to_b);
        rtr_b.connect_peer(pid_a, b_to_a);

        // Harmonizers
        let peer_a = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let peer_b = PeerContext {
            peer_id: UuidBytes::new(),
        };
        use crate::plugins::harmonizer::scorers::server_scorer::{
            ServerScorer, ServerScorerParams,
        };
        let opt_a = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            peer_a.clone(),
        ));
        let opt_b = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            peer_b.clone(),
        ));
        let hz_a = Harmonizer::new(
            rtr_a.clone(),
            opt_a,
            HashSet::from([Foo::table_def().name()]),
            peer_a.clone(),
        );
        let hz_b = Harmonizer::new(
            rtr_b.clone(),
            opt_b,
            HashSet::from([Foo::table_def().name()]),
            peer_b.clone(),
        );

        // DBs
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let db_a = KuramotoDb::new(
            dir_a.path().join("a.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_a.clone()],
        )
        .await;
        let db_b = KuramotoDb::new(
            dir_b.path().join("b.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_b.clone()],
        )
        .await;
        db_a.create_table_and_indexes::<Foo>().unwrap();
        db_b.create_table_and_indexes::<Foo>().unwrap();

        // Synthesize on A: two leaves and one parent with both children
        let mk_leaf = |peer: UuidBytes, id: u32| -> availability::Availability {
            let pk = id.to_le_bytes().to_vec();
            let r = Harmonizer::leaf_range_from_pk_and_indexes(Foo::table_def(), &pk, &[]);
            availability::Availability {
                key: UuidBytes::new(),
                peer_id: peer,
                range: r,
                level: 0,
                schema_hash: 0,
                version: 0,
                updated_at: clock.now(),
                complete: true,
            }
        };
        let leaf1 = mk_leaf(peer_a.peer_id, 100);
        let leaf2 = mk_leaf(peer_a.peer_id, 101);
        db_a.put(leaf1.clone()).await.unwrap();
        db_a.put(leaf2.clone()).await.unwrap();
        let parent_id = UuidBytes::new();
        let parent = availability::Availability {
            key: parent_id,
            peer_id: peer_a.peer_id,
            range: leaf1.range.clone(),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: clock.now(),
            complete: true,
        };
        db_a.put(parent.clone()).await.unwrap();
        let mut cs_a = ChildSet::open(&db_a, parent_id).await.unwrap();
        cs_a.add_child(&db_a, leaf1.key).await.unwrap();
        cs_a.add_child(&db_a, leaf2.key).await.unwrap();
        let a_child_ids = vec![leaf1.key, leaf2.key];

        // B: materialize the same parent key/range/level (no children yet)
        let now = db_b.get_clock().now();
        let pav = availability::Availability {
            key: parent.key,
            peer_id: peer_b.peer_id,
            range: parent.range.clone(),
            level: parent.level,
            schema_hash: 0,
            version: 0,
            updated_at: now,
            complete: true,
        };
        db_b.put(pav).await.unwrap();
        // Seed children on B: all except last
        let mut cs_b = ChildSet::open(&db_b, parent.key).await.unwrap();
        for cid in a_child_ids.iter().take(a_child_ids.len() - 1) {
            cs_b.add_child(&db_b, *cid).await.unwrap();
        }

        // Sanity: B missing one child
        let cs_b2 = ChildSet::open(&db_b, parent.key).await.unwrap();
        assert_eq!(cs_b2.count() + 1, a_child_ids.len());

        // Repair from A
        hz_b.repair_parent_with_peer(pid_a, parent.key)
            .await
            .unwrap();

        let cs_b3 = ChildSet::open(&db_b, parent.key).await.unwrap();
        let got: std::collections::HashSet<_> = cs_b3.children.iter().copied().collect();
        let want: std::collections::HashSet<_> = a_child_ids.iter().copied().collect();
        assert_eq!(got, want, "B child set should match A after repair");
    }

    /// Ensure GetChildrenByRange scopes roots per storage entity (first dimension),
    /// i.e., trees are independent across entities.
    #[tokio::test(start_paused = true)]
    async fn get_children_scopes_per_entity() {
        let clock = Arc::new(MockClock::new(0));
        let router = Router::new(RouterConfig::default(), clock.clone());

        let local_peer = PeerContext {
            peer_id: UuidBytes::new(),
        };
        use crate::plugins::harmonizer::scorers::server_scorer::{
            ServerScorer, ServerScorerParams,
        };
        let optimizer = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            local_peer.clone(),
        ));
        let hz = Harmonizer::new(
            router.clone(),
            optimizer,
            HashSet::from([Foo::table_def().name(), Bar::table_def().name()]),
            local_peer,
        );

        // DB
        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("scope_per_entity.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz.clone()],
        )
        .await;
        db.create_table_and_indexes::<Foo>().unwrap();
        db.create_table_and_indexes::<Bar>().unwrap();

        // Build two separate trees for Foo and Bar (manually via Availability rows)
        let pid = hz.peer_ctx.peer_id;
        let pk_dim_foo = TableHash::from(Foo::table_def());
        let pk_dim_bar = TableHash::from(Bar::table_def());

        // Foo tree: root -> leaf
        let foo_leaf_pk = 7u32.to_le_bytes().to_vec();
        let mut hi = foo_leaf_pk.clone();
        hi.push(1);
        let foo_leaf_rng = RangeCube::new(
            smallvec![pk_dim_foo],
            smallvec![foo_leaf_pk.clone()],
            smallvec![hi],
        )
        .unwrap();
        let foo_root = Availability {
            key: UuidBytes::new(),
            peer_id: pid,
            range: RangeCube::new(
                smallvec![pk_dim_foo],
                smallvec![vec![0u8, 0, 0, 0]],
                smallvec![vec![255u8, 255, 255, 255, 1]],
            )
            .unwrap(),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: clock.now(),
            complete: true,
        };
        let foo_leaf = Availability {
            key: UuidBytes::new(),
            peer_id: pid,
            range: foo_leaf_rng.clone(),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: clock.now(),
            complete: true,
        };
        db.put(foo_root.clone()).await.unwrap();
        db.put(foo_leaf.clone()).await.unwrap();
        let mut cs_f = ChildSet::open(&db, foo_root.key).await.unwrap();
        cs_f.add_child(&db, foo_leaf.key).await.unwrap();

        // Bar tree: root -> leaf
        let bar_leaf_pk = 9u32.to_le_bytes().to_vec();
        let mut bhi = bar_leaf_pk.clone();
        bhi.push(1);
        let bar_leaf_rng = RangeCube::new(
            smallvec![pk_dim_bar],
            smallvec![bar_leaf_pk.clone()],
            smallvec![bhi],
        )
        .unwrap();
        let bar_root = Availability {
            key: UuidBytes::new(),
            peer_id: pid,
            range: RangeCube::new(
                smallvec![pk_dim_bar],
                smallvec![vec![0u8, 0, 0, 0]],
                smallvec![vec![255u8, 255, 255, 255, 1]],
            )
            .unwrap(),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: clock.now(),
            complete: true,
        };
        let bar_leaf = Availability {
            key: UuidBytes::new(),
            peer_id: pid,
            range: bar_leaf_rng.clone(),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: clock.now(),
            complete: true,
        };
        db.put(bar_root.clone()).await.unwrap();
        db.put(bar_leaf.clone()).await.unwrap();
        let mut cs_b = ChildSet::open(&db, bar_root.key).await.unwrap();
        cs_b.add_child(&db, bar_leaf.key).await.unwrap();

        // Fetch roots scoped by entity: only the Foo root should appear for Foo dim
        let txn = db.begin_read_txn().unwrap();
        let roots_foo =
            availability::roots_for_peer_and_entity(&db, Some(&txn), &pid, pk_dim_foo.hash())
                .await
                .unwrap();
        let roots_bar =
            availability::roots_for_peer_and_entity(&db, Some(&txn), &pid, pk_dim_bar.hash())
                .await
                .unwrap();
        assert!(roots_foo.iter().any(|a| a.key == foo_root.key));
        assert!(roots_bar.iter().any(|a| a.key == bar_root.key));
        // Cross-check: Foo scoped roots should not include Bar root
        assert!(!roots_foo.iter().any(|a| a.key == bar_root.key));
        assert!(!roots_bar.iter().any(|a| a.key == foo_root.key));
    }

    /// End-to-end: A inserts, Harmonizer sends UpdateWithAttestation,
    /// B ingests via proto handler and writes to its DB.
    #[tokio::test(start_paused = true)]
    async fn empty_to_sync_single_insert() {
        // shared clock + two routers
        let clock = Arc::new(MockClock::new(0));
        let rtr_a = Router::new(RouterConfig::default(), clock.clone());
        let rtr_b = Router::new(RouterConfig::default(), clock.clone());

        // connect peers via in-mem transport
        let ns: u64 = rand::rng().random();
        let pid_a = pid(1);
        let pid_b = pid(2);
        let conn_a = InMemConnector::with_namespace(pid_a, ns);
        let conn_b = InMemConnector::with_namespace(pid_b, ns);
        let resolver = InMemResolver;

        let a_to_b = conn_a
            .dial(&resolver.resolve(pid_b).await.unwrap())
            .await
            .unwrap();
        let b_to_a = conn_b
            .dial(&resolver.resolve(pid_a).await.unwrap())
            .await
            .unwrap();
        rtr_a.connect_peer(pid_b, a_to_b);
        rtr_b.connect_peer(pid_a, b_to_a);

        // Harmonizers on both sides (register their protocol with their routers)
        let peer_a = PeerContext {
            peer_id: UuidBytes::new(),
        };
        use crate::plugins::harmonizer::scorers::server_scorer::{
            ServerScorer, ServerScorerParams,
        };
        let opt_a = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            peer_a.clone(),
        ));
        let hz_a = Harmonizer::new(
            rtr_a.clone(),
            opt_a,
            HashSet::from([Foo::table_def().name()]),
            peer_a,
        );
        let peer_b = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let opt_b = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            peer_b.clone(),
        ));
        let hz_b = Harmonizer::new(
            rtr_b.clone(),
            opt_b,
            HashSet::from([Foo::table_def().name()]),
            peer_b,
        );

        // Tell A to notify B for this v0 flow (hacky bootstrap)
        hz_a.add_peer(pid_b).await;

        // DBs with harmonizer plugin installed
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let db_a = KuramotoDb::new(
            dir_a.path().join("a.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_a.clone()],
        )
        .await;
        let db_b = KuramotoDb::new(
            dir_b.path().join("b.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_b.clone()],
        )
        .await;

        db_a.create_table_and_indexes::<Foo>().unwrap();
        db_b.create_table_and_indexes::<Foo>().unwrap();

        // Insert on A → should arrive on B through UpdateWithAttestation path
        db_a.put(Foo { id: 7 }).await.unwrap();

        // Drive the async machinery deterministically
        spin().await;

        // Assert Foo exists on B
        let got_b: Foo = db_b.get_data::<Foo>(&7u32.to_le_bytes()).await.unwrap();
        assert_eq!(got_b, Foo { id: 7 });

        // (Optional) sanity: B produced at least one leaf availability
        let av_b: Vec<Availability> = db_b
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();
        assert!(
            !av_b.is_empty(),
            "receiver should materialize a leaf availability for the ingested entity"
        );
        assert!(av_b.iter().any(|a| a.level == 0));
    }

    /// Completer tick repairs an incomplete parent by pulling from a peer via existing RPCs.
    #[tokio::test(start_paused = true)]
    async fn completer_tick_repairs_incomplete_parent() {
        use crate::plugins::harmonizer::availability::Availability;
        // Routers and link
        let clock = Arc::new(MockClock::new(0));
        let rtr_a = Router::new(RouterConfig::default(), clock.clone());
        let rtr_b = Router::new(RouterConfig::default(), clock.clone());
        let ns: u64 = rand::rng().random();
        let pid_a = pid(51);
        let pid_b = pid(52);
        let conn_a = InMemConnector::with_namespace(pid_a, ns);
        let conn_b = InMemConnector::with_namespace(pid_b, ns);
        let resolver = InMemResolver;
        let a_to_b = conn_a
            .dial(&resolver.resolve(pid_b).await.unwrap())
            .await
            .unwrap();
        let b_to_a = conn_b
            .dial(&resolver.resolve(pid_a).await.unwrap())
            .await
            .unwrap();
        rtr_a.connect_peer(pid_b, a_to_b);
        rtr_b.connect_peer(pid_a, b_to_a);

        // Harmonizers
        let peer_a = PeerContext {
            peer_id: UuidBytes::new(),
        };
        let peer_b = PeerContext {
            peer_id: UuidBytes::new(),
        };
        use crate::plugins::harmonizer::scorers::server_scorer::{
            ServerScorer, ServerScorerParams,
        };
        let opt_a = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            peer_a.clone(),
        ));
        let opt_b = Arc::new(BasicOptimizer::new(
            Box::new(ServerScorer::new(ServerScorerParams::default())),
            peer_b.clone(),
        ));
        let hz_a = Harmonizer::new(
            rtr_a.clone(),
            opt_a,
            HashSet::from([Foo::table_def().name()]),
            peer_a,
        );
        let hz_b = Harmonizer::new(
            rtr_b.clone(),
            opt_b,
            HashSet::from([Foo::table_def().name()]),
            peer_b.clone(),
        );

        // DBs
        let dir_a = tempdir().unwrap();
        let dir_b = tempdir().unwrap();
        let db_a = KuramotoDb::new(
            dir_a.path().join("a.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_a.clone()],
        )
        .await;
        let db_b = KuramotoDb::new(
            dir_b.path().join("b.redb").to_str().unwrap(),
            clock.clone(),
            vec![hz_b.clone()],
        )
        .await;
        db_a.create_table_and_indexes::<Foo>().unwrap();
        db_b.create_table_and_indexes::<Foo>().unwrap();

        // A: synthesize a parent with two children and materialize chunk0
        let parent_id = UuidBytes::new();
        let now = db_a.get_clock().now();
        let leaf1 = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: RangeCube::new(
                smallvec![TableHash { hash: 77 }],
                smallvec![vec![1]],
                smallvec![vec![2]],
            )
            .unwrap(),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: now,
            complete: true,
        };
        let leaf2 = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: RangeCube::new(
                smallvec![TableHash { hash: 77 }],
                smallvec![vec![3]],
                smallvec![vec![4]],
            )
            .unwrap(),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: now,
            complete: true,
        };
        let parent = Availability {
            key: parent_id,
            peer_id: UuidBytes::new(),
            range: leaf1.range.clone(),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: now,
            complete: true,
        };
        db_a.put(leaf1.clone()).await.unwrap();
        db_a.put(leaf2.clone()).await.unwrap();
        db_a.put(parent.clone()).await.unwrap();
        let mut cs_a = ChildSet::open(&db_a, parent_id).await.unwrap();
        cs_a.add_child(&db_a, leaf1.key).await.unwrap();
        cs_a.add_child(&db_a, leaf2.key).await.unwrap();

        // B: same parent id but incomplete and without children
        let parent_b = Availability {
            key: parent_id,
            peer_id: peer_b.peer_id,
            range: parent.range.clone(),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: now,
            complete: false,
        };
        db_b.put(parent_b).await.unwrap();

        // Tell B to reach A
        hz_b.add_peer(pid_a).await;

        // Run completer tick to repair
        let scheduled = hz_b.tick_completer(10).await.unwrap();
        assert!(scheduled >= 1, "tick should schedule at least one repair");

        // Verify B now has the same children set as A
        let cs_b = ChildSet::open(&db_b, parent_id).await.unwrap();
        let got: std::collections::HashSet<_> = cs_b.children.into_iter().collect();
        let expect: std::collections::HashSet<_> = [leaf1.key, leaf2.key].into_iter().collect();
        assert_eq!(got, expect, "completer should repair children via RPCs");
    }
}
