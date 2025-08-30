use bincode::{Decode, Encode};
use redb::TableDefinition;

use crate::{
    KuramotoDb, StaticTableDef,
    plugins::harmonizer::{
        child_set::{AVAIL_CHILDREN_BY_CHILD_TBL, Child, ChildSet},
        range_cube::RangeCube,
    },
    storage_entity::{IndexCardinality, IndexSpec, StorageEntity},
    storage_error::StorageError,
    uuid_bytes::UuidBytes,
};

/*──────────────────────── Tables ───────────────────────*/

/// Main storage & meta tables
pub static AVAILABILITIES_TABLE: StaticTableDef = &TableDefinition::new("availabilities");
pub static AVAILABILITIES_META_TABLE: StaticTableDef = &TableDefinition::new("availabilities_meta");

/// Secondary index: `peer_id` → availability rows
pub static AVAILABILITY_BY_PEER: StaticTableDef = &TableDefinition::new("availability_by_peer");

/*──────────────────────── Model ───────────────────────*/

/// V0 Availability: relationships are not embedded; parent/children live in `availability_children`.
#[derive(Clone, Debug, Encode, Decode)]
pub struct AvailabilityV0 {
    /* identification */
    pub key: UuidBytes,
    pub peer_id: UuidBytes,

    /* geometry & hierarchy */
    pub range: RangeCube,
    pub level: u16,

    /* schema */
    pub schema_hash: u64, // hash(dataset + struct_version + index_layout)

    /* bookkeeping */
    pub version: u32,
    pub updated_at: u64,
    pub complete: bool,
}

pub type Availability = AvailabilityV0;

/*──────────────────────── Indexes ─────────────────────*/

pub static AVAILABILITY_INDEXES: &[IndexSpec<AvailabilityV0>] = &[
    // Fast lookup of rows by peer
    IndexSpec::<AvailabilityV0> {
        name: "by_peer",
        key_fn: |a| a.peer_id.as_bytes().to_vec(),
        table_def: &AVAILABILITY_BY_PEER,
        cardinality: IndexCardinality::NonUnique,
    },
];

/*──────────────────────── StorageEntity ───────────────*/

impl StorageEntity for AvailabilityV0 {
    /// Binary payload tag for this struct version (not to be confused with `version` field).
    const STRUCT_VERSION: u8 = 0;

    fn primary_key(&self) -> Vec<u8> {
        self.key.into_vec()
    }

    fn table_def() -> StaticTableDef {
        AVAILABILITIES_TABLE
    }
    fn meta_table_def() -> StaticTableDef {
        AVAILABILITIES_META_TABLE
    }

    fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
        if data.is_empty() {
            return Err(StorageError::Bincode("empty input".into()));
        }
        match data[0] {
            0 => {
                // v0 payload starts after the tag byte
                bincode::decode_from_slice::<Self, _>(&data[1..], bincode::config::standard())
                    .map(|(v, _)| v)
                    .map_err(|e| StorageError::Bincode(e.to_string()))
            }
            n => Err(StorageError::Bincode(format!("unknown version {n}"))),
        }
    }

    fn indexes() -> &'static [IndexSpec<Self>] {
        AVAILABILITY_INDEXES
    }
}

/*──────────────────────── Query helpers ───────────────*/

use redb::ReadTransaction;

/// Fetch all **root** availabilities for a given `peer_id` using the `(peer_id, is_root)` index.
/// This is O(log N + k) and avoids scanning the full table.
///
/// Pass `txn` when you need a snapshot (e.g., from inside `before_update`).
pub async fn roots_for_peer(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    peer_id: &UuidBytes,
) -> Result<Vec<Availability>, StorageError> {
    // 1) Fetch all availabilities for the peer via index
    let peer_key = peer_id.as_bytes().to_vec();
    let all: Vec<Availability> = db
        .get_by_index_all_tx::<Availability>(txn, AVAILABILITY_BY_PEER, &peer_key)
        .await?;

    // 2) Filter roots = nodes with zero incoming edges in Child rows (by_child index)
    let mut roots = Vec::new();
    for a in all {
        let child_key = a.key.as_bytes().to_vec();
        let parents: Vec<Child> = db
            .get_by_index_all_tx::<Child>(txn, AVAIL_CHILDREN_BY_CHILD_TBL, &child_key)
            .await?;
        // Ignore self-edges when determining roots
        let has_external_parent = parents.iter().any(|p| p.parent != a.key);
        if !has_external_parent {
            roots.push(a);
        }
    }
    Ok(roots)
}

/*──────────────────────── Tests ───────────────────────*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{clock::MockClock, tables::TableHash};
    use crate::plugins::harmonizer::child_set::{Child, DigestChunk, ChildSet};
    use smallvec::smallvec;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn cube(dim: TableHash, min: &[u8], max: &[u8]) -> RangeCube {
        RangeCube::new(
            smallvec![dim],
            smallvec![min.to_vec()],
            smallvec![max.to_vec()],
        )
        .unwrap()
    }

    async fn fresh_db() -> Arc<KuramotoDb> {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("avail_v0.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        // Ensure table + indexes exist
        db.create_table_and_indexes::<Availability>().unwrap();
        db.create_table_and_indexes::<Child>().unwrap();
        db.create_table_and_indexes::<crate::plugins::harmonizer::child_set::DigestChunk>()
            .unwrap();
        db
    }

    #[tokio::test]
    async fn roots_detected_by_missing_parents() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 1 };

        let p = UuidBytes::new();

        let r = Availability {
            key: UuidBytes::new(),
            peer_id: p,
            range: cube(dim, b"a", b"b"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let c = Availability {
            key: UuidBytes::new(),
            peer_id: p,
            range: cube(dim, b"a", b"c"),
            level: 2,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(r.clone()).await.unwrap();
        db.put(c.clone()).await.unwrap();

        // Initially, both have no parents → both are roots
        let init_roots = roots_for_peer(&db, None, &p).await.unwrap();
        let ids: std::collections::HashSet<_> = init_roots.iter().map(|a| a.key).collect();
        assert!(ids.contains(&r.key) && ids.contains(&c.key));

        // Add edge r -> c → c loses root status
        let mut cs = ChildSet::open(&db, r.key).await.unwrap();
        cs.add_child(&db, c.key).await.unwrap();

        let roots_after = roots_for_peer(&db, None, &p).await.unwrap();
        let ids2: std::collections::HashSet<_> = roots_after.iter().map(|a| a.key).collect();
        assert!(ids2.contains(&r.key));
        assert!(!ids2.contains(&c.key));
    }

    #[tokio::test]
    async fn roots_for_peer_returns_only_roots_for_that_peer() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 2 };

        let p1 = UuidBytes::new();
        let p2 = UuidBytes::new();

        // p1: 2 roots, 1 non-root
        let r1 = UuidBytes::new();
        db.put(Availability {
            key: r1,
            peer_id: p1,
            range: cube(dim, b"a", b"b"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 1,
            complete: true,
        })
        .await
        .unwrap();

        let r2 = UuidBytes::new();
        db.put(Availability {
            key: r2,
            peer_id: p1,
            range: cube(dim, b"c", b"d"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 2,
            complete: true,
        })
        .await
        .unwrap();

        // non-root under r1
        let c1 = UuidBytes::new();
        db.put(Availability {
            key: c1,
            peer_id: p1,
            range: cube(dim, b"a", b"c"),
            level: 2,
            schema_hash: 0,
            version: 0,
            updated_at: 3,
            complete: true,
        })
        .await
        .unwrap();
        let mut cs = ChildSet::open(&db, r1).await.unwrap();
        cs.add_child(&db, c1).await.unwrap();

        // p2: a root (should not be returned for p1)
        let r3 = UuidBytes::new();
        db.put(Availability {
            key: r3,
            peer_id: p2,
            range: cube(dim, b"x", b"y"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 4,
            complete: true,
        })
        .await
        .unwrap();

        // Query roots for p1 via the index
        let got = roots_for_peer(&db, None, &p1).await.unwrap();
        let ids: std::collections::HashSet<_> = got.into_iter().map(|a| a.key).collect();

        assert!(
            ids.contains(&r1) && ids.contains(&r2),
            "should return p1 roots"
        );
        assert!(!ids.contains(&c1), "non-root must not appear in root query");
        assert!(!ids.contains(&r3), "roots for other peers must not appear");
    }

    #[tokio::test]
    async fn roots_update_when_parent_edge_added() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 3 };
        let p = UuidBytes::new();

        // Start as a root
        let a_id = UuidBytes::new();
        let a = Availability {
            key: a_id,
            peer_id: p,
            range: cube(dim, b"m", b"n"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(a.clone()).await.unwrap();

        // Verify it shows as a root
        let r_before = roots_for_peer(&db, None, &p).await.unwrap();
        assert!(r_before.iter().any(|x| x.key == a_id));

        // Add a parent edge (some parent -> a) to make it non-root
        let parent_id = UuidBytes::new();
        let parent = Availability {
            key: parent_id,
            peer_id: p,
            range: cube(dim, b"l", b"o"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(parent).await.unwrap();
        let mut cs = ChildSet::open(&db, parent_id).await.unwrap();
        cs.add_child(&db, a_id).await.unwrap();

        // Now it should disappear from the root index
        let r_after = roots_for_peer(&db, None, &p).await.unwrap();
        assert!(
            !r_after.iter().any(|x| x.key == a_id),
            "root index must reflect parent flip"
        );
    }
}
