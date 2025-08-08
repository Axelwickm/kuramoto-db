use async_recursion::async_recursion;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use redb::{TableDefinition, TableHandle, WriteTransaction};
use std::{
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    sync::Arc,
};

use smallvec::smallvec;

use crate::middlewares::harmonizer::availability::{
    AVAILABILITIES_META_TABLE, AVAILABILITIES_TABLE, Availability,
};
use crate::middlewares::harmonizer::child_set::{ChildSet, Sym};
use crate::middlewares::harmonizer::range_cube::RangeCube;
use crate::{
    KuramotoDb, StaticTableDef, WriteBatch, database::WriteRequest, middlewares::Middleware,
    storage_entity::StorageEntity, storage_error::StorageError, uuid_bytes::UuidBytes,
};

/*────────────────────────────────────────────────────────────────────────────*/

pub struct PeerContext {
    pub peer_id: UuidBytes,
    // future: region, query_interests, free_space, etc
}

pub trait Scorer: Send + Sync {
    fn score(&self, ctx: &PeerContext, avail: &Availability) -> f32;
}

/*────────────────────────────────────────────────────────────────────────────*/

pub struct Harmonizer {
    scorer: Box<dyn Scorer>,
    watched: HashSet<&'static str>, // table names, not TableDefinition
}

impl Harmonizer {
    pub fn new(scorer: Box<dyn Scorer>) -> Self {
        Self {
            scorer,
            watched: HashSet::new(),
        }
    }

    pub fn watch<E: StorageEntity>(&mut self) {
        self.watched.insert(E::table_def().name());
    }

    fn is_watched(&self, tbl: StaticTableDef) -> bool {
        self.watched.contains(tbl.name())
    }

    // ── helper: hash(table_name || pk) → child symbol
    fn child_sym(&self, data_table: StaticTableDef, key: &[u8]) -> u64 {
        let mut h = DefaultHasher::new();
        data_table.name().hash(&mut h);
        key.hash(&mut h);
        h.finish()
    }

    /*──────────────────── range cover (async DFS) ───────────────────────────*/

    /// Deepest locally-known Availabilities intersecting `target`.
    ///
    /// * `db` – handle to KuramotoDb
    /// * `root_ids` – Availability keys to start from (refs/slices)
    /// * `peer_filter` – closure to filter by peer
    /// * `depth_limit` – maximum depth, or None for unlimited
    ///
    /// Returns `(Vec<Availability>, uncovered_flag)`.
    /// `uncovered_flag == true`  ⇢ none of the roots overlapped the cube.
    pub async fn range_cover<F>(
        &self,
        db: &KuramotoDb,
        target: &RangeCube,
        root_ids: &[UuidBytes],
        peer_filter: F,
        depth_limit: Option<usize>,
    ) -> Result<(Vec<Availability>, bool), StorageError>
    where
        F: Fn(&Availability) -> bool + Sync,
    {
        // ---- resolve helper -------------------------------------------------
        async fn load_avail(
            db: &KuramotoDb,
            id: &UuidBytes,
        ) -> Result<Option<Availability>, StorageError> {
            match db.get_data::<Availability>(id.as_bytes()).await {
                Ok(a) => Ok(Some(a)),
                Err(StorageError::NotFound) => Ok(None),
                Err(e) => Err(e),
            }
        }

        // For now, treat all child Syms as data (not nested availabilities).
        async fn resolve_child_avail(
            _db: &KuramotoDb,
            _sym: Sym,
        ) -> Result<Option<Availability>, StorageError> {
            Ok(None)
        }

        // ---- DFS ------------------------------------------------------------
        #[async_recursion]
        async fn dfs<F>(
            db: &KuramotoDb,
            acc: &mut Vec<Availability>,
            cube: &RangeCube,
            id: UuidBytes,
            peer_filter: &F,
            depth_left: Option<usize>,
            any_overlap: &mut bool,
        ) -> Result<(), StorageError>
        where
            F: Fn(&Availability) -> bool + Sync,
        {
            if depth_left == Some(0) {
                return Ok(());
            }
            let depth_next = depth_left.map(|d| d - 1);

            let Some(av) = load_avail(db, &id).await? else {
                return Ok(());
            };
            if !peer_filter(&av) {
                return Ok(());
            }
            if av.range.intersect(cube).is_none() {
                return Ok(());
            }
            *any_overlap = true;

            // stop if incomplete OR leaf
            let is_leaf = av.children.count() == 0;
            if !av.complete || is_leaf {
                acc.push(av);
                return Ok(());
            }

            // recurse into children that decode into a UUID
            for sym in &av.children.children {
                if let Some(child_av) = resolve_child_avail(db, *sym).await? {
                    dfs(
                        db,
                        acc,
                        cube,
                        child_av.key,
                        peer_filter,
                        depth_next,
                        any_overlap,
                    )
                    .await?;
                } else {
                    // unresolvable child => av is effectively a leaf
                    acc.push(av.clone());
                    return Ok(());
                }
            }
            Ok(())
        }

        // ---- drive the DFS from roots ----
        let mut results = Vec::<Availability>::new();
        let mut any_overlap = false;
        for rid in root_ids {
            dfs(
                db,
                &mut results,
                target,
                *rid,
                &peer_filter,
                depth_limit,
                &mut any_overlap,
            )
            .await?;
        }

        // Dedup by key (roots may share descendants)
        let mut seen = std::collections::HashSet::<UuidBytes>::new();
        results.retain(|a| seen.insert(a.key));

        Ok((results, !any_overlap))
    }
}

/*──────────────────────── middleware impl (batch-based) ─────────────────────*/

#[async_trait]
impl Middleware for Harmonizer {
    /// Called once per top-level write after the DB opened a write transaction.
    /// We *append* a secondary Availability write into the same batch/txn.
    async fn before_write(
        &self,
        db: &KuramotoDb,
        _txn: &WriteTransaction,
        batch: &mut WriteBatch,
    ) -> Result<(), StorageError> {
        // Only the primary request should drive middleware planning
        let Some(first) = batch.get(0) else {
            return Ok(());
        };

        // Only react to PUTs into watched data tables
        let (data_table, key) = match first {
            WriteRequest::Put {
                data_table, key, ..
            } if self.is_watched(*data_table) => (*data_table, key.as_slice()),
            _ => return Ok(()),
        };

        // Build the Availability row deterministically for this data write
        let now = db.get_clock().now();
        let sym = self.child_sym(data_table, key);

        let avail_id = UuidBytes::new();
        // TODO: supply a real/stable peer_id from context later
        let peer_id = UuidBytes::new();

        let avail = Availability {
            key: avail_id,
            peer_id,
            range: RangeCube {
                dims: smallvec![],
                mins: smallvec![],
                maxs: smallvec![],
            },
            children: ChildSet {
                parent: avail_id,
                children: vec![sym],
            },
            schema_hash: 0,
            version: 0,
            updated_at: now,
            complete: true,
        };

        // Serialize the Availability row
        let value = avail.to_bytes();

        // Minimal meta bytes (no serde). This matches your handle_write
        // which only *decodes* new meta when a previous meta exists.
        // For first insert there is no previous meta, so any well-formed
        // bincode payload is fine. Define a tiny local V0.
        #[derive(Encode, Decode)]
        struct AvailMetaV0 {
            version: u32,
            updated_at: u64,
        }
        let meta = bincode::encode_to_vec(
            AvailMetaV0 {
                version: 0,
                updated_at: now,
            },
            bincode::config::standard(),
        )
        .map_err(|e| StorageError::Bincode(e.to_string()))?;

        // Append a *secondary* request: respond_to = None (only primary replies)
        batch.push(WriteRequest::Put {
            data_table: AVAILABILITIES_TABLE,
            meta_table: AVAILABILITIES_META_TABLE,
            key: avail.primary_key(),
            value,
            meta,
            index_removes: Vec::new(),
            index_puts: Vec::new(),
        });

        // IMPORTANT: make sure your Harmonizer never "watches" the availability
        // tables themselves, or you'll recursively generate availabilities for
        // availabilities. (Leaving the responsibility check in is_watched.)

        Ok(())
    }
}
/*───────────────────────────────────────────────────────────────*/
/* tests                                                         */
/*───────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{clock::Clock, storage_entity::*};
    use tempfile::tempdir;

    /* -- stub clock so `updated_at` is deterministic -- */
    struct StubClock;
    impl Clock for StubClock {
        fn now(&self) -> u64 {
            123456
        }
    }

    /* -- dummy scoreable -- */
    struct NullScore;
    impl Scorer for NullScore {
        fn score(&self, _: &PeerContext, _: &Availability) -> f32 {
            0.0
        }
    }

    /*────────────────────────────── tests (unchanged) ───────────────────────────*/

    #[tokio::test]
    async fn inserts_one_availability_per_put() {
        /* -- dummy entity we’ll watch -- */
        #[derive(Clone, Debug, Encode, Decode)]
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

            fn meta_table_def() -> StaticTableDef {
                static TBL: TableDefinition<'static, &'static [u8], Vec<u8>> =
                    TableDefinition::new("foo_meta");
                &TBL
            }

            fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
                if data.is_empty() {
                    return Err(StorageError::Bincode("empty input".into()));
                }
                match data[0] {
                    0 => bincode::decode_from_slice::<Self, _>(
                        &data[1..],
                        bincode::config::standard(),
                    )
                    .map(|(v, _)| v)
                    .map_err(|e| StorageError::Bincode(e.to_string())),
                    n => Err(StorageError::Bincode(format!("unknown version {n}"))),
                }
            }

            fn indexes() -> &'static [IndexSpec<Self>] {
                &[]
            }
        }

        /* -- build middleware first (DB handle comes later) -- */
        let h = Arc::new({
            let mut h = Harmonizer::new(Box::new(NullScore));
            h.watch::<Foo>();
            h
        });

        /* -- build DB, then inject real Arc<KuramotoDb> -- */
        let dir = tempdir().unwrap();
        let db = KuramotoDb::new(
            dir.path().join("t.redb").to_str().unwrap(),
            Arc::new(StubClock),
            vec![h.clone()],
        )
        .await;

        db.create_table_and_indexes::<Foo>().unwrap();
        db.put(Foo { id: 1 }).await.unwrap();

        // With batch-based hook, no detached tasks are needed.
        // The Availability write is appended and committed in the same txn.

        let all: Vec<Availability> = db
            .range_by_pk::<Availability>(&[], &[0xFF], None)
            .await
            .unwrap();
        assert_eq!(all.len(), 1);
    }
}
