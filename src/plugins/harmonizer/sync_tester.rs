use std::collections::HashSet;
use std::sync::Arc;

use tempfile::tempdir;
use std::path::{Path, PathBuf};
use std::fs;

use crate::{
    KuramotoDb,
    clock::MockClock,
    plugins::{
        communication::router::{Router, RouterConfig},
        harmonizer::{
            BasicOptimizer,
            harmonizer::{Harmonizer, PeerContext},
            scorers::server_scorer::{ServerScorer, ServerScorerParams},
        },
    },
    uuid_bytes::UuidBytes,
};

#[derive(Default, Clone, Debug)]
pub struct NodeCounters {
    pub db_puts: usize,
}

pub struct Node {
    pub peer_id: UuidBytes,
    pub router: Arc<Router>,
    pub db: Arc<KuramotoDb>,
    pub harmonizer: Arc<Harmonizer>,
    pub counters: NodeCounters,
}

pub struct SyncTester {
    clock: Arc<MockClock>,
    peers: Vec<Node>,
    watched: HashSet<&'static str>,
    record_replay: bool,
    export_dir: Option<PathBuf>,
    export_basename: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PerPeerStats {
    pub peer: UuidBytes,
    pub db_puts: usize,
}

#[derive(Clone, Debug, Default)]
pub struct SyncTestReport {
    pub ticks_run: u64,
    pub per_peer: Vec<PerPeerStats>,
}

const DEFAULT_TIMEOUT_SECS: u64 = 30;

impl SyncTester {
    pub async fn new(peers: &[UuidBytes], watched_tables: &[&'static str], params: ServerScorerParams) -> Self {
        Self::new_with_options(peers, watched_tables, params, SyncTesterOptions::default()).await
    }

    pub async fn new_with_options(
        peers: &[UuidBytes],
        watched_tables: &[&'static str],
        params: ServerScorerParams,
        opts: SyncTesterOptions,
    ) -> Self {
        let clock = Arc::new(MockClock::new(0));
        let mut nodes = Vec::with_capacity(peers.len());
        let watched: HashSet<&'static str> = watched_tables.iter().copied().collect();

        for &pid in peers.iter() {
            let router = Router::new(RouterConfig::default(), clock.clone());
            let scorer = Box::new(ServerScorer::new(params.clone()));
            let opt = Arc::new(BasicOptimizer::new(scorer, PeerContext { peer_id: pid }));
            let hz = Harmonizer::new(
                router.clone(),
                opt,
                watched.clone(),
                PeerContext { peer_id: pid },
            );

            let dir = tempdir().unwrap();
            let mut plugins: Vec<Arc<dyn crate::plugins::Plugin>> = vec![hz.clone()];
            if opts.record_replay {
                plugins.push(Arc::new(crate::plugins::replay::ReplayPlugin::new()));
            }

            let db = KuramotoDb::new(
                dir.path()
                    .join(format!("peer_{}.redb", pid))
                    .to_str()
                    .unwrap(),
                clock.clone(),
                plugins,
            )
            .await;

            nodes.push(Node {
                peer_id: pid,
                router,
                db,
                harmonizer: hz,
                counters: NodeCounters::default(),
            });
        }

        // Connect full mesh (in-mem)
        use crate::plugins::communication::transports::inmem::{InMemConnector, InMemResolver};
        use crate::plugins::communication::transports::{Connector, PeerResolver};
        let ns: u64 = 0x53_59_4e_43; // SYNC
        let resolver = InMemResolver;
        for i in 0..nodes.len() {
            for j in 0..nodes.len() {
                if i == j {
                    continue;
                }
                let a = nodes[i].peer_id;
                let b = nodes[j].peer_id;
                let conn = InMemConnector::with_namespace(a, ns)
                    .dial(&resolver.resolve(b).await.unwrap())
                    .await
                    .unwrap();
                nodes[i].router.connect_peer(b, conn);
            }
        }

        Self {
            clock,
            peers: nodes,
            watched,
            record_replay: opts.record_replay,
            export_dir: opts.export_dir,
            export_basename: opts.export_basename,
        }
    }

    pub fn peers(&self) -> &Vec<Node> {
        &self.peers
    }
    pub fn peers_mut(&mut self) -> &mut Vec<Node> {
        &mut self.peers
    }

    pub async fn step(&self, ticks: u64) {
        use tokio::task::yield_now;
        use tokio::time::{Duration, advance};
        for _ in 0..ticks {
            yield_now().await;
            advance(Duration::from_millis(1)).await;
            yield_now().await;
        }
    }

    /// Internal insert (bytes) by table name on a specific peer. Counts are updated.
    pub async fn insert_bytes(&mut self, peer: UuidBytes, table: &str, bytes: Vec<u8>) {
        if let Some(n) = self.peers.iter().position(|n| n.peer_id == peer) {
            let db = self.peers[n].db.clone();
            db.put_by_table_bytes(table, &bytes).await.unwrap();
            self.peers[n].counters.db_puts += 1;
        }
    }

    /// Run stepping until `stop` returns true, or until timeout elapses, or max_ticks reached.
    /// Panics on timeout.
    pub async fn run_until<F>(
        &mut self,
        max_ticks: u64,
        timeout: Option<std::time::Duration>,
        mut stop: F,
    ) -> SyncTestReport
    where
        F: FnMut(&Self) -> bool,
    {
        self
            .run_until_async(max_ticks, timeout, move |s| {
                std::pin::Pin::from(Box::new(std::future::ready(stop(s))) as Box<dyn std::future::Future<Output = bool> + Send + '_>)
            })
            .await
    }

    pub async fn run_until_async<F>(
        &mut self,
        max_ticks: u64,
        timeout: Option<std::time::Duration>,
        mut stop: F,
    ) -> SyncTestReport
    where
        F: for<'a> FnMut(
                &'a Self,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>>,
    {
        use tokio::select;
        let to = timeout.unwrap_or_else(|| std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS));

        // Wall-clock watchdog that fires regardless of Tokio's virtual time.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        std::thread::spawn(move || {
            std::thread::sleep(to);
            let _ = tx.send(());
        });

        let mut ticks = 0u64;

        // Drive the system until condition or max_ticks.
        let drive = async {
            if stop(self).await {
                return ticks;
            }
            while ticks < max_ticks {
                self.step(1).await;
                ticks += 1;
                if stop(self).await {
                    break;
                }
            }
            ticks
        };

        select! {
            _ = rx => panic!("SyncTester timeout after {:?}", to),
            t = drive => self.build_report(t),
        }
    }

    fn build_report(&self, ticks: u64) -> SyncTestReport {
        let per_peer = self
            .peers
            .iter()
            .map(|n| PerPeerStats {
                peer: n.peer_id,
                db_puts: n.counters.db_puts,
            })
            .collect();
        SyncTestReport {
            ticks_run: ticks,
            per_peer,
        }
    }

    /// Export replay logs for each peer (if recording enabled) into `out_dir`.
    pub async fn export_replay_files<P: AsRef<Path>>(&self, out_dir: P) -> std::io::Result<()> {
        if !self.record_replay {
            return Ok(());
        }
        let dir = out_dir.as_ref();
        fs::create_dir_all(dir)?;
        for (i, n) in self.peers.iter().enumerate() {
            let events: Vec<crate::plugins::replay::ReplayEvent> = n
                .db
                .range_by_pk::<crate::plugins::replay::ReplayEvent>(&0u64.to_be_bytes(), &u64::MAX.to_be_bytes(), None)
                .await
                .unwrap_or_default();

            // Serialize all events to a single file per DB
            let bytes = bincode::encode_to_vec(&events, bincode::config::standard())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

            let fname = if let Some(base) = &self.export_basename {
                format!("{}_peer_{}.bin", base, i + 1)
            } else {
                let uuid: uuid::Uuid = n.peer_id.into();
                format!("replay_{}.bin", uuid.as_hyphenated())
            };
            let path = dir.join(fname);
            fs::write(path, bytes)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct SyncTesterOptions {
    pub record_replay: bool,
    pub export_dir: Option<PathBuf>,
    pub export_basename: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        plugins::harmonizer::integrity_run_all,
        storage_entity::StorageEntity,
    };
    use redb::{TableDefinition, TableHandle};

    // Local tiny entity to use with SyncTester smoke
    #[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    struct TestEnt { id: u32 }
    impl StorageEntity for TestEnt {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> { self.id.to_le_bytes().to_vec() }
        fn table_def() -> crate::StaticTableDef { static T: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("test_ent"); &T }
        fn meta_table_def() -> crate::StaticTableDef { static M: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("test_ent_meta"); &M }
        fn load_and_migrate(data: &[u8]) -> Result<Self, crate::storage_error::StorageError> {
            match data.first().copied() {
                Some(0) => bincode::decode_from_slice::<Self, _>(&data[1..], bincode::config::standard())
                    .map(|(v, _)| v)
                    .map_err(|e| crate::storage_error::StorageError::Bincode(e.to_string())),
                _ => Err(crate::storage_error::StorageError::Bincode("bad version".into())),
            }
        }
        fn indexes() -> &'static [crate::storage_entity::IndexSpec<Self>] { &[] }
    }

    #[tokio::test(start_paused = true)]
    async fn sync_tester_basic_smoke() {
        let peers = vec![UuidBytes::new(), UuidBytes::new()];
        let mut t = SyncTester::new(&peers, &[TestEnt::table_def().name()], ServerScorerParams::default()).await;
        for n in t.peers_mut() { n.db.create_table_and_indexes::<TestEnt>().unwrap(); }
        // Insert a couple of rows on first peer via tester
        let enc = |e: &TestEnt| {
            let mut out = Vec::new(); out.push(0); out.extend(bincode::encode_to_vec(e, bincode::config::standard()).unwrap()); out
        };
        t.insert_bytes(peers[0], TestEnt::table_def().name(), enc(&TestEnt { id: 1 })).await;
        t.insert_bytes(peers[0], TestEnt::table_def().name(), enc(&TestEnt { id: 2 })).await;
        t.step(10).await;
        // Integrity with expect_complete=false (no availability digests/children in this smoke test)
        for n in t.peers().iter() {
            let errs = integrity_run_all(&n.db, None, n.peer_id, false).await.unwrap();
            assert!(errs.is_empty(), "integrity errors: {errs:?}");
        }
        // Counters moved
        assert!(t.peers().iter().map(|n| n.counters.db_puts).sum::<usize>() >= 2);
    }

    #[tokio::test(start_paused = true)]
    async fn sync_tester_run_until_immediate_ok() {
        let peers = vec![UuidBytes::new()];
        let mut t = SyncTester::new(&peers, &[], ServerScorerParams::default()).await;
        let rep = t.run_until(10, Some(std::time::Duration::from_millis(50)), |_t| true).await;
        assert_eq!(rep.ticks_run, 0);
    }

    #[tokio::test(start_paused = true)]
    #[should_panic]
    async fn sync_tester_run_until_times_out() {
        let peers = vec![UuidBytes::new()];
        let mut t = SyncTester::new(&peers, &[], ServerScorerParams::default()).await;
        // Impossible predicate with tiny timeout should panic
        let _ = t.run_until(1_000_000, Some(std::time::Duration::from_secs(1)), |_t| false).await;
    }
}
