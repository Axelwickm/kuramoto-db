use bincode::{Decode, Encode};
use redb::TableDefinition;

use crate::{
    KuramotoDb, StaticTableDef,
    plugins::harmonizer::{child_set::ChildSet, range_cube::RangeCube},
    storage_entity::{IndexCardinality, IndexSpec, StorageEntity},
    storage_error::StorageError,
    uuid_bytes::UuidBytes,
};

/*──────────────────────── Tables ───────────────────────*/

/// Main storage & meta tables
pub static AVAILABILITIES_TABLE: StaticTableDef = &TableDefinition::new("availabilities");
pub static AVAILABILITIES_META_TABLE: StaticTableDef = &TableDefinition::new("availabilities_meta");

/// Secondary index: `(peer_id, is_root_bit)` → availability rows
/// - `is_root_bit = 1` when `parent.is_none()`
/// - `is_root_bit = 0` otherwise
pub static AVAILABILITY_BY_PEER_ROOT: StaticTableDef =
    &TableDefinition::new("availability_by_peer_root");

/*──────────────────────── Model ───────────────────────*/

/// V0 Availability: includes an optional `parent`. A node is a **root** iff `parent.is_none()`.
#[derive(Clone, Debug, Encode, Decode)]
pub struct AvailabilityV0 {
    /* identification */
    pub key: UuidBytes,
    pub peer_id: UuidBytes,

    /* geometry & hierarchy */
    pub range: RangeCube,
    pub level: u16,
    pub parent: Option<UuidBytes>, // ← None ⇒ root
    pub children: ChildSet,        // inline child list (UUIDs)

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
    // Fast lookup of roots/non-roots per peer without scanning the table.
    IndexSpec::<AvailabilityV0> {
        name: "by_peer_root",
        key_fn: |a| {
            let mut k = a.peer_id.as_bytes().to_vec();
            // parent == None → root bit = 1; else 0
            k.push(if a.parent.is_none() { 1 } else { 0 });
            k
        },
        table_def: &AVAILABILITY_BY_PEER_ROOT,
        cardinality: IndexCardinality::NonUnique,
    },
];

impl AvailabilityV0 {
    #[inline]
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }
}

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
    // Build the composite index key
    let mut idx_key = peer_id.as_bytes().to_vec();
    idx_key.push(1u8); // 1 ⇒ root (parent == None)

    db.get_by_index_all_tx::<Availability>(txn, AVAILABILITY_BY_PEER_ROOT, &idx_key)
        .await
}

/*──────────────────────── Tests ───────────────────────*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{clock::MockClock, tables::TableHash};
    use smallvec::smallvec;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn cube(dim: TableHash, min: &[u8], max: &[u8]) -> RangeCube {
        RangeCube {
            dims: smallvec![dim],
            mins: smallvec![min.to_vec()],
            maxs: smallvec![max.to_vec()],
        }
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
        db
    }

    #[tokio::test]
    async fn is_root_derived_from_parent_none() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 1 };

        let root_id = UuidBytes::new();
        let root = Availability {
            key: root_id,
            peer_id: UuidBytes::new(),
            range: cube(dim, b"a", b"b"),
            level: 1,
            parent: None,
            children: ChildSet {
                parent: root_id,
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(root.clone()).await.unwrap();

        let non_root_id = UuidBytes::new();
        let non_root = Availability {
            key: non_root_id,
            peer_id: root.peer_id, // same peer
            range: cube(dim, b"a", b"c"),
            level: 2,
            parent: Some(root_id),
            children: ChildSet {
                parent: non_root_id,
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(non_root.clone()).await.unwrap();

        // Sanity on the flag
        let got_root: Availability = db.get_data(root_id.as_bytes()).await.unwrap();
        let got_non_root: Availability = db.get_data(non_root_id.as_bytes()).await.unwrap();
        assert!(got_root.is_root());
        assert!(!got_non_root.is_root());
    }

    #[tokio::test]
    async fn index_by_peer_root_returns_only_roots_for_that_peer() {
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
            parent: None,
            children: ChildSet {
                parent: r1,
                children: vec![],
            },
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
            parent: None,
            children: ChildSet {
                parent: r2,
                children: vec![],
            },
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
            parent: Some(r1),
            children: ChildSet {
                parent: c1,
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 3,
            complete: true,
        })
        .await
        .unwrap();

        // p2: a root (should not be returned for p1)
        let r3 = UuidBytes::new();
        db.put(Availability {
            key: r3,
            peer_id: p2,
            range: cube(dim, b"x", b"y"),
            level: 1,
            parent: None,
            children: ChildSet {
                parent: r3,
                children: vec![],
            },
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
    async fn index_updates_when_parent_flips() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 3 };
        let p = UuidBytes::new();

        // Start as a root
        let a_id = UuidBytes::new();
        let mut a = Availability {
            key: a_id,
            peer_id: p,
            range: cube(dim, b"m", b"n"),
            level: 1,
            parent: None,
            children: ChildSet {
                parent: a_id,
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(a.clone()).await.unwrap();

        // Verify it shows as a root
        let r_before = roots_for_peer(&db, None, &p).await.unwrap();
        assert!(r_before.iter().any(|x| x.key == a_id));

        // Flip to non-root
        let parent_id = UuidBytes::new();
        a.parent = Some(parent_id);
        db.put(a.clone()).await.unwrap();

        // Now it should disappear from the root index
        let r_after = roots_for_peer(&db, None, &p).await.unwrap();
        assert!(
            !r_after.iter().any(|x| x.key == a_id),
            "root index must reflect parent flip"
        );
    }
}
