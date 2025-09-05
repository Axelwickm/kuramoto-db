#![cfg(feature = "web_admin")]

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, RawQuery};
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::get,
};
use redb::{ReadTransaction, TableHandle};
use tokio::net::TcpListener;

use crate::{KuramotoDb, WriteBatch, WriteOrigin, plugins::Plugin, storage_error::StorageError};

const DEFAULT_REPLAY_CAP: usize = 1024;

#[derive(Clone)]
pub struct WebAdminConfig {
    pub bind: SocketAddr,
    pub replay_capacity: usize,
}

impl Default for WebAdminConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".parse().unwrap(),
            replay_capacity: DEFAULT_REPLAY_CAP,
        }
    }
}

#[derive(Clone)]
pub struct WebAdminPlugin {
    cfg: WebAdminConfig,
    state: Arc<InnerState>,
}

struct InnerState {
    db: RwLock<Option<Arc<KuramotoDb>>>,
    // Decoded datasets (e.g., attached/live)
    datasets: RwLock<HashMap<String, Vec<crate::plugins::replay::ReplayEvent>>>,
    // File-backed datasets: raw bytes + index of per-event start offsets
    file_bytes: RwLock<HashMap<String, Arc<Vec<u8>>>>,
    file_index: RwLock<HashMap<String, Vec<usize>>>,
    // Per-dataset table name mappings (hash -> name)
    table_names: RwLock<HashMap<String, HashMap<u64, String>>>,
    seq: AtomicU64,
    server_started: RwLock<bool>,
}

impl WebAdminPlugin {
    pub fn new(cfg: WebAdminConfig) -> Self {
        let cap = cfg.replay_capacity.max(16);
        Self {
            cfg,
            state: Arc::new(InnerState {
                db: RwLock::new(None),
                datasets: RwLock::new(HashMap::new()),
                file_bytes: RwLock::new(HashMap::new()),
                file_index: RwLock::new(HashMap::new()),
                table_names: RwLock::new(HashMap::new()),
                seq: AtomicU64::new(0),
                server_started: RwLock::new(false),
            }),
        }
    }
}

// No serde: build JSON strings by hand in handlers

#[async_trait]
impl Plugin for WebAdminPlugin {
    fn attach_db(&self, db: Arc<KuramotoDb>) {
        {
            let mut w = self.state.db.write().unwrap();
            *w = Some(db.clone());
        }

        // Start server only once
        let already = { *self.state.server_started.read().unwrap() };
        if !already {
            let mut w = self.state.server_started.write().unwrap();
            if !*w {
                *w = true;
                let state = self.state.clone();
                let bind = self.cfg.bind;
                tokio::spawn(async move {
                    let app = Router::new()
                        .route("/", get(handler_index_html))
                        .route("/static/app.js", get(handler_app_js))
                        .route("/static/styles.css", get(handler_styles_css))
                        .route("/api/health", get(handler_health))
                        .route("/api/mode", get(handler_mode))
                        .route("/api/stats", get(handler_stats))
                        .route("/api/replay_stats", get(handler_replay_stats))
                        .route("/api/replay", get(handler_replay))
                        .route("/api/replay/files", get(handler_replay_files))
                        .route("/api/replay/load", get(handler_replay_load))
                        .route(
                            "/api/replay/upload",
                            axum::routing::post(handler_replay_upload),
                        )
                        .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
                        .with_state(state);
                    let listener = tokio::net::TcpListener::bind(bind)
                        .await
                        .expect("bind web_admin listen");
                    println!("web_admin listening on http://{}", bind);
                    if let Err(e) = axum::serve(listener, app).await {
                        tracing::error!("web_admin server stopped: {}", e);
                    }
                });
            }
        }
    }

    async fn before_update(
        &self,
        db: &KuramotoDb,
        _txn: &ReadTransaction,
        _batch: &mut WriteBatch,
    ) -> Result<(), StorageError> {
        // default path delegates to the origin-aware variant; no-op here
        let _ = db;
        Ok(())
    }

    async fn before_update_with_origin(
        &self,
        db: &KuramotoDb,
        _txn: &ReadTransaction,
        batch: &mut WriteBatch,
        origin: WriteOrigin,
    ) -> Result<(), StorageError> {
        // Convert batch to a ReplayEvent using the same format as the replay plugin
        let now = db.get_clock().now();
        let id = self.state.seq.fetch_add(1, Ordering::Relaxed);
        let batch = crate::plugins::replay::ReplayPlugin::map_batch_to_log(batch);
        let id_bytes = crate::plugins::self_identity::SelfIdentity::get_peer_id(db).await?;
        let mut peer_id = [0u8; 16];
        peer_id.copy_from_slice(id_bytes.as_bytes());
        let event = crate::plugins::replay::ReplayEvent {
            id,
            ts: now,
            origin: match origin {
                crate::WriteOrigin::Plugin(id) => id,
                crate::WriteOrigin::LocalCommit => 1,
                crate::WriteOrigin::Completer => 2,
                crate::WriteOrigin::RemoteIngest => 3,
            },
            peer_id,
            batch,
        };
        {
            let mut ds = self.state.datasets.write().unwrap();
            let e = ds.entry("attached".into()).or_insert_with(Vec::new);
            e.push(event);
            // Trim to capacity if needed
            let cap = self.cfg.replay_capacity.max(16);
            if e.len() > cap {
                let excess = e.len() - cap;
                e.drain(0..excess);
            }
        }
        Ok(())
    }
}

/* ─────────────────────── HTTP handlers ─────────────────────── */

async fn handler_index_html() -> impl IntoResponse {
    Html(include_str!("./static/index.html"))
}

async fn handler_app_js() -> impl IntoResponse {
    (
        [("Content-Type", "application/javascript")],
        include_str!("./static/app.js"),
    )
        .into_response()
}

async fn handler_styles_css() -> impl IntoResponse {
    (
        [("Content-Type", "text/css")],
        include_str!("./static/styles.css"),
    )
        .into_response()
}

async fn handler_health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn handler_stats(State(state): State<Arc<InnerState>>) -> impl IntoResponse {
    let Some(db) = state.db.read().unwrap().clone() else {
        // file mode: dataset overview
        let d = state.datasets.read().unwrap();
        let mut first = true;
        let mut body = String::from("{\"mode\":\"file\",\"datasets\":[");
        for (name, v) in d.iter() {
            if !first {
                body.push(',');
            }
            first = false;
            body.push_str(&format!("{{\"name\":\"{}\",\"events\":{}}}", name, v.len()));
        }
        body.push_str("]}");
        return ([("Content-Type", "application/json")], body).into_response();
    };
    // Collect simple stats: number of tables and total row counts
    let (data_tables, index_tables) = db.list_registered_tables();
    let mut total_rows_data: u64 = 0;
    let mut total_rows_index: u64 = 0;
    for t in &data_tables {
        if let Ok(c) = db.count_rows_in_table(*t) {
            total_rows_data += c;
        }
    }
    for t in &index_tables {
        if let Ok(c) = db.count_rows_in_table(*t) {
            total_rows_index += c;
        }
    }
    let body = format!(
        "{{\"data_table_count\":{},\"index_table_count\":{},\"total_rows_data\":{},\"total_rows_index\":{}}}",
        data_tables.len(),
        index_tables.len(),
        total_rows_data,
        total_rows_index
    );
    ([("Content-Type", "application/json")], body).into_response()
}

async fn handler_replay(
    State(state): State<Arc<InnerState>>,
    RawQuery(q): RawQuery,
) -> impl IntoResponse {
    // parse ?source=, ?limit=, ?offset=
    let mut source = None;
    let mut limit: usize = 1000;
    let mut offset: usize = 0;
    if let Some(qs) = q {
        for pair in qs.split('&') {
            let mut it = pair.splitn(2, '=');
            if let (Some(k), Some(v)) = (it.next(), it.next()) {
                let v = urlencoding::decode(v)
                    .unwrap_or_else(|_| v.into())
                    .into_owned();
                if k == "source" {
                    source = Some(v);
                } else if k == "limit" {
                    if let Ok(n) = v.parse() {
                        limit = n;
                    }
                } else if k == "offset" {
                    if let Ok(n) = v.parse() {
                        offset = n;
                    }
                }
            }
        }
    }
    let src = if let Some(s) = source {
        s
    } else if state.db.read().unwrap().is_some() {
        "attached".into()
    } else {
        String::new()
    };
    if src.is_empty() {
        return ([("Content-Type", "application/json")], String::from("[]")).into_response();
    }
    // lazily load from disk if a replay file with this name exists
    let need_load = {
        let d = state.datasets.read().unwrap();
        !d.contains_key(&src)
    };
    if need_load && src != "attached" {
        let path = std::path::Path::new("exports").join(&src);
        if let Ok(bytes) = std::fs::read(path) {
            if let Ok((events, _)) = bincode::decode_from_slice::<Vec<crate::plugins::replay::ReplayEvent>, _>(&bytes, bincode::config::standard()) {
                {
                    let mut ds = state.datasets.write().unwrap();
                    ds.insert(src.clone(), events.clone());
                }
                // build table name cache
                let mut m = HashMap::new();
                for e in events.iter() {
                    for it in e.batch.iter() {
                        if let crate::plugins::replay::LogWriteRequest::TableNames { mappings } = it {
                            for (h, name) in mappings.iter() { m.insert(*h, name.clone()); }
                        }
                    }
                }
                if !m.is_empty() {
                    state.table_names.write().unwrap().insert(src.clone(), m);
                }
            }
        }
    }
    let d = state.datasets.read().unwrap();
    let mut events = d.get(&src).cloned().unwrap_or_default();
    let n = events.len();
    let end = n.saturating_sub(offset);
    let start = end.saturating_sub(limit);
    let part: Vec<_> = if start < end { events[start..end].to_vec() } else { Vec::new() };
    let mut first = true;
    let mut body = String::new();
    body.push_str(&format!("{{\"total\":{},\"offset\":{},\"limit\":{},\"events\":[", n, offset, limit));
    for e in part.iter() {
        if !first {
            body.push(',');
        }
        first = false;
        let peer_hex = hex::encode(e.peer_id);
        // derive simple summary: puts, deletes, tables
        let mut puts = 0usize;
        let mut dels = 0usize;
        let mut tables: Vec<String> = Vec::new();
        for it in e.batch.iter() {
            match it {
                crate::plugins::replay::LogWriteRequest::Put { data_table_hash, .. } => {
                    puts += 1;
                    tables.push(table_label(&state, Some(&src), *data_table_hash));
                }
                crate::plugins::replay::LogWriteRequest::Delete { data_table_hash, .. } => {
                    dels += 1;
                    tables.push(table_label(&state, Some(&src), *data_table_hash));
                }
                crate::plugins::replay::LogWriteRequest::StatsSnapshot { .. } => {}
                crate::plugins::replay::LogWriteRequest::TableNames { .. } => {}
            }
        }
        tables.sort();
        tables.dedup();
        let origin_lbl = origin_label(e.origin);
        body.push_str(&format!(
            "{{\"id\":{},\"ts\":{},\"origin_code\":{},\"origin\":\"{}\",\"peer_id\":\"{}\",\"puts\":{},\"deletes\":{},\"tables\":[{}]}}",
            e.id, e.ts, e.origin, origin_lbl, peer_hex, puts, dels,
            tables.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",")
        ));
    }
    body.push_str("]}");
    ([("Content-Type", "application/json")], body).into_response()
}

fn origin_label(code: u16) -> &'static str {
    if code == 1 {
        return "LocalCommit";
    }
    if code == 2 {
        return "Completer";
    }
    if code == 3 {
        return "RemoteIngest";
    }
    if code == crate::plugins::fnv1a_16("replay") {
        return "Plugin:replay";
    }
    if code == crate::plugins::fnv1a_16("self_identity") {
        return "Plugin:self_identity";
    }
    "Plugin"
}

fn table_label(state: &InnerState, dataset: Option<&str>, hash: u64) -> String {
    if let Some(db) = state.db.read().unwrap().as_ref() {
        if let Some(def) = db.resolve_data_table_by_hash(hash) {
            return def.name().to_string();
        }
    }
    if let Some(ds) = dataset {
        if let Some(map) = state.table_names.read().unwrap().get(ds) {
            if let Some(name) = map.get(&hash) {
                return name.clone();
            }
        }
    }
    format!("0x{:016x}", hash)
}

async fn handler_mode(State(state): State<Arc<InnerState>>) -> impl IntoResponse {
    let m = if state.db.read().unwrap().is_some() {
        "attached"
    } else {
        "replay"
    };
    let s = format!("{{\"mode\":\"{}\"}}", m);
    ([("Content-Type", "application/json")], s)
}

// Return database stats either from attached DB (if source=attached)
// or from the latest StatsSnapshot in the given replay dataset.
async fn handler_replay_stats(
    State(state): State<Arc<InnerState>>,
    RawQuery(q): RawQuery,
) -> impl IntoResponse {
    let mut source = String::new();
    let mut at: Option<u64> = None;
    if let Some(qs) = q {
        for pair in qs.split('&') {
            let mut it = pair.splitn(2, '=');
            if let (Some(k), Some(v)) = (it.next(), it.next()) {
                if k == "source" {
                    source = urlencoding::decode(v).unwrap_or_else(|_| v.into()).into_owned();
                } else if k == "at" {
                    if let Ok(n) = urlencoding::decode(v).unwrap_or_else(|_| v.into()).into_owned().parse::<u64>() {
                        at = Some(n);
                    }
                }
            }
        }
    }
    if source.is_empty() {
        return ([("Content-Type", "application/json")], String::from("{}")).into_response();
    }
    if source == "attached" {
        if let Some(db) = state.db.read().unwrap().as_ref() {
            let (data_tables, index_tables) = db.list_registered_tables();
            let mut total_rows_data: u64 = 0;
            let mut total_rows_index: u64 = 0;
            for t in &data_tables { if let Ok(c) = db.count_rows_in_table(*t) { total_rows_data += c; } }
            for t in &index_tables { if let Ok(c) = db.count_rows_in_table(*t) { total_rows_index += c; } }
            let body = format!(
                "{{\"data_table_count\":{},\"index_table_count\":{},\"total_rows_data\":{},\"total_rows_index\":{}}}",
                data_tables.len(), index_tables.len(), total_rows_data, total_rows_index
            );
            return ([("Content-Type", "application/json")], body).into_response();
        } else {
            return ([("Content-Type", "application/json")], String::from("{}")).into_response();
        }
    }

    // ensure dataset is loaded
    let need_load = { let d = state.datasets.read().unwrap(); !d.contains_key(&source) };
    if need_load {
        let path = std::path::Path::new("exports").join(&source);
        if let Ok(bytes) = std::fs::read(path) {
            if let Ok((events, _)) = bincode::decode_from_slice::<Vec<crate::plugins::replay::ReplayEvent>, _>(&bytes, bincode::config::standard()) {
                let mut ds = state.datasets.write().unwrap();
                ds.insert(source.clone(), events);
            }
        }
    }
    let d = state.datasets.read().unwrap();
    if let Some(ev) = d.get(&source) {
        let mut best: Option<(&crate::plugins::replay::DatabaseStats, u64)> = None;
        for e in ev.iter().rev() {
            for it in e.batch.iter().rev() {
                if let crate::plugins::replay::LogWriteRequest::StatsSnapshot { stats } = it {
                    let ets = e.ts;
                    match at {
                        Some(target) => {
                            if ets <= target {
                                best = Some((stats, ets));
                                // Found the closest at or before target
                                break;
                            }
                        }
                        None => {
                            best = Some((stats, ets));
                            break;
                        }
                    }
                }
            }
            if best.is_some() { break; }
        }
        if let Some((stats, ts)) = best {
            let body = format!(
                "{{\"at\":{},\"data_table_count\":{},\"index_table_count\":{},\"total_rows_data\":{},\"total_rows_index\":{}}}",
                ts, stats.data_table_count, stats.index_table_count, stats.total_rows_data, stats.total_rows_index
            );
            return ([("Content-Type", "application/json")], body).into_response();
        }
    }
    ([("Content-Type", "application/json")], String::from("{}")).into_response()
}

async fn handler_replay_files() -> impl IntoResponse {
    let mut out_v = Vec::new();
    if let Ok(rd) = std::fs::read_dir("exports") {
        for ent in rd.flatten() {
            if let Ok(ft) = ent.file_type() {
                if ft.is_file() {
                    let name = ent.file_name().to_string_lossy().to_string();
                    if name.starts_with("replay_") && name.ends_with(".bin") {
                        out_v.push(name);
                    }
                }
            }
        }
    }
    let mut first = true;
    let mut body = String::from("{\"files\":[");
    for f in out_v.iter() {
        if !first {
            body.push(',');
        }
        first = false;
        body.push_str(&format!("\"{}\"", f));
    }
    body.push_str("]}");
    ([("Content-Type", "application/json")], body)
}

async fn handler_replay_load(
    State(state): State<Arc<InnerState>>,
    RawQuery(q): RawQuery,
) -> impl IntoResponse {
    // basic sanitization: disallow path separators
    let mut file = String::new();
    if let Some(qs) = q {
        for pair in qs.split('&') {
            let mut it = pair.splitn(2, '=');
            if let (Some(k), Some(v)) = (it.next(), it.next()) {
                if k == "file" {
                    file = urlencoding::decode(v)
                        .unwrap_or_else(|_| v.into())
                        .into_owned();
                }
            }
        }
    }
    if file.contains('/') || file.contains('\\') {
        return (StatusCode::BAD_REQUEST, "invalid file").into_response();
    }
    let path = std::path::Path::new("exports").join(&file);
    match std::fs::read(path) {
        Ok(bytes) => {
            match bincode::decode_from_slice::<Vec<crate::plugins::replay::ReplayEvent>, _>(
                &bytes,
                bincode::config::standard(),
            ) {
                Ok((events, _)) => {
                    // update dataset and table name cache
                    {
                        let mut ds = state.datasets.write().unwrap();
                        ds.insert(file.clone(), events.clone());
                    }
                    let mut m = HashMap::new();
                    for e in events.iter() {
                        for it in e.batch.iter() {
                            if let crate::plugins::replay::LogWriteRequest::TableNames { mappings } = it {
                                for (h, name) in mappings.iter() { m.insert(*h, name.clone()); }
                            }
                        }
                    }
                    if !m.is_empty() {
                        state.table_names.write().unwrap().insert(file.clone(), m);
                    }
                    (StatusCode::OK, "loaded").into_response()
                }
                Err(e) => (StatusCode::BAD_REQUEST, format!("decode error: {}", e)).into_response(),
            }
        }
        Err(e) => (StatusCode::NOT_FOUND, format!("read error: {}", e)).into_response(),
    }
}

async fn handler_replay_upload(
    State(state): State<Arc<InnerState>>,
    RawQuery(q): RawQuery,
    body: Bytes,
) -> impl IntoResponse {
    match bincode::decode_from_slice::<Vec<crate::plugins::replay::ReplayEvent>, _>(
        &body,
        bincode::config::standard(),
    ) {
        Ok((events, _)) => {
            let mut ds = state.datasets.write().unwrap();
            let mut name = None;
            if let Some(qs) = q {
                for pair in qs.split('&') {
                    let mut it = pair.splitn(2, '=');
                    if let (Some(k), Some(v)) = (it.next(), it.next()) {
                        if k == "name" {
                            name = Some(
                                urlencoding::decode(v)
                                    .unwrap_or_else(|_| v.into())
                                    .into_owned(),
                            );
                        }
                    }
                }
            }
            let name = name
                .unwrap_or_else(|| format!("upload-{}", state.seq.fetch_add(1, Ordering::Relaxed)));
            ds.insert(name, events);
            (StatusCode::OK, "uploaded").into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, format!("decode error: {}", e)).into_response(),
    }
}

/// Run the web admin server without a DB attached.
/// Useful for serving the static UI or future disk-backed replay browsing.
pub async fn run_standalone(
    bind: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = Arc::new(InnerState {
        db: RwLock::new(None),
        datasets: RwLock::new(HashMap::new()),
        file_bytes: RwLock::new(HashMap::new()),
        file_index: RwLock::new(HashMap::new()),
        table_names: RwLock::new(HashMap::new()),
        seq: AtomicU64::new(0),
        server_started: RwLock::new(true),
    });
    let app = Router::new()
        .route("/", get(handler_index_html))
        .route("/static/app.js", get(handler_app_js))
        .route("/static/styles.css", get(handler_styles_css))
        .route("/api/health", get(handler_health))
        .route("/api/mode", get(handler_mode))
        .route("/api/stats", get(handler_stats))
        .route("/api/replay_stats", get(handler_replay_stats))
        .route("/api/replay", get(handler_replay))
        .route("/api/replay/files", get(handler_replay_files))
        .route("/api/replay/load", get(handler_replay_load))
        .route(
            "/api/replay/upload",
            axum::routing::post(handler_replay_upload),
        )
        .with_state(state);

    let listener = TcpListener::bind(bind).await?;
    println!("web_admin listening on http://{}", bind);
    axum::serve(listener, app).await?;
    Ok(())
}
