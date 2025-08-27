use redb::ReadTransaction;
use std::collections::{BTreeMap, HashSet};

use crate::tables::TableHash;
use crate::{
    KuramotoDb, StaticTableDef,
    plugins::harmonizer::{
        availability::{Availability, roots_for_peer},
        child_set::{Child, ChildSet, DigestChunk},
        range_cube::RangeCube,
    },
    storage_error::StorageError,
    uuid_bytes::UuidBytes,
};

/// Txn-aware loader for a child **availability** by its *availability key*.
/// Returns `Ok(None)` when the child is missing (broken link).
pub async fn resolve_child_avail(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    child_id: &UuidBytes,
) -> Result<Option<Availability>, StorageError> {
    match db
        .get_data_tx::<Availability>(txn, child_id.as_bytes())
        .await
    {
        Ok(av) => Ok(Some(av)),
        Err(StorageError::NotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Descend from the given `root_ids` and return a **frontier** that covers `target`.
///
/// Traversal:
/// - Only descend into a node if it **intersects** `target`.
/// - If the node is **incomplete**, a **leaf** (no children), or any child fails to resolve,
///   we **return that node** as part of the frontier.
/// - Else we recurse into all resolvable children that intersect `target`.
///
/// Returns `(nodes, no_overlap)` where `no_overlap == true` iff no visited node
/// intersected `target`.
pub async fn range_cover(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    target: &RangeCube,
    root_ids: &[UuidBytes],
    level_limit: Option<usize>,
) -> Result<(Vec<Availability>, bool), StorageError> {
    #[async_recursion::async_recursion]
    async fn dfs(
        db: &KuramotoDb,
        txn: Option<&ReadTransaction>,
        out: &mut Vec<Availability>,
        cube: &RangeCube,
        id: UuidBytes,
        budget: Option<usize>,
        touched: &mut bool,
    ) -> Result<(), StorageError> {
        if budget == Some(0) {
            return Ok(());
        }
        let next_budget = budget.map(|d| d.saturating_sub(1));

        // Load node from the same snapshot (if any)
        let Some(av) = resolve_child_avail(db, txn, &id).await? else {
            return Ok(());
        };

        // Only consider nodes that *touch* target
        if av.range.intersect(cube).is_none() {
            return Ok(());
        }
        *touched = true;

        // Leaf or incomplete → frontier (children resolved via child table)
        let cs = if let Some(t) = txn {
            ChildSet::open_tx(db, t, av.key).await?
        } else {
            ChildSet::open(db, av.key).await?
        };
        let is_leaf = cs.count() == 0;
        if !av.complete || is_leaf {
            out.push(av);
            return Ok(());
        }

        // Resolve all children first; if any is missing → frontier
        let mut child_ids = Vec::with_capacity(cs.count());
        for cid in &cs.children {
            match resolve_child_avail(db, txn, cid).await? {
                Some(_) => child_ids.push(*cid),
                None => {
                    out.push(av);
                    return Ok(());
                }
            }
        }

        // Recurse into resolvable children
        for cid in child_ids {
            dfs(db, txn, out, cube, cid, next_budget, touched).await?;
        }
        Ok(())
    }

    let mut results = Vec::<Availability>::new();
    let mut touched = false;

    for rid in root_ids {
        dfs(
            db,
            txn,
            &mut results,
            target,
            *rid,
            level_limit,
            &mut touched,
        )
        .await?;
    }

    // Dedup (roots may share subtrees)
    let mut seen = HashSet::<UuidBytes>::new();
    results.retain(|a| seen.insert(a.key));

    Ok((results, !touched))
}

/// Try to count storage atoms in `cube` **without deserializing entities**.
/// Returns `Ok(Some(count))` if the cube could be resolved to registered tables; `Ok(None)`
/// if the dims are unknown (caller should fall back to availability counting).
async fn storage_atom_count_in_cube_tx(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    cube: &RangeCube,
) -> Result<Option<usize>, StorageError> {
    // Group dims by **parent data table**:
    // - PK axis: TableHash points directly to a data table
    // - Index axis: TableHash resolves to (index_table, parent_data_table)
    struct Group {
        data: Option<(StaticTableDef, Vec<u8>, Vec<u8>)>, // (data_table, lo, hi)
        idxs: Vec<(StaticTableDef, Vec<u8>, Vec<u8>)>,    // (index_table, lo, hi)
    }
    let mut by_parent: BTreeMap<u64, Group> = BTreeMap::new();

    let n = cube.dims.len().min(cube.mins.len()).min(cube.maxs.len());
    for i in 0..n {
        let h = cube.dims[i].hash;
        let lo = cube.mins[i].clone();
        let hi = cube.maxs[i].clone();

        if let Some(data_tbl) = db.resolve_data_table_by_hash(h) {
            by_parent
                .entry(h)
                .or_insert(Group {
                    data: None,
                    idxs: Vec::new(),
                })
                .data = Some((data_tbl, lo, hi));
            continue;
        }

        if let Some((idx_tbl, parent_tbl)) = db.resolve_index_table_by_hash(h) {
            let ph = TableHash::from(parent_tbl).hash();
            by_parent
                .entry(ph)
                .or_insert(Group {
                    data: None,
                    idxs: Vec::new(),
                })
                .idxs
                .push((idx_tbl, lo, hi));
        }
    }

    if by_parent.is_empty() {
        return Ok(None); // unknown dims → let caller fall back
    }

    // Intersect PKs per parent group across data + all its index axes.
    let mut total = 0usize;
    for (_ph, g) in by_parent {
        let mut acc: Option<HashSet<Vec<u8>>> = if let Some((tbl, lo, hi)) = g.data {
            let pks = db
                .collect_pks_in_data_range_tx(txn, tbl, &lo, &hi, None)
                .await?;
            Some(pks.into_iter().collect())
        } else {
            None
        };

        for (idx_tbl, lo, hi) in g.idxs {
            let pks = db
                .collect_pks_in_index_range_tx(txn, idx_tbl, &lo, &hi, None)
                .await?;
            let set: HashSet<Vec<u8>> = pks.into_iter().collect();

            acc = match acc.take() {
                Some(cur) => {
                    // early-exit when intersection becomes empty
                    let inter: HashSet<_> = cur.intersection(&set).cloned().collect();
                    if inter.is_empty() {
                        Some(inter)
                    } else {
                        Some(inter)
                    }
                }
                None => Some(set),
            };
            if let Some(ref s) = acc {
                if s.is_empty() {
                    break;
                }
            }
        }

        if let Some(s) = acc {
            total += s.len();
        }
    }

    Ok(Some(total))
}

/// Count distinct peers that **fully contain** `target` with at least one **complete** availability.
///
/// This is intentionally global/scan-based because replication is a cross-peer property.
/// If you want a root-scoped view, call `range_cover` first and aggregate there.
pub async fn count_replications<F>(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    target: &RangeCube,
    peer_filter: F,
    level_limit: Option<usize>,
) -> Result<usize, StorageError>
where
    F: Fn(&Availability) -> bool + Sync,
{
    let mut peers = HashSet::<UuidBytes>::new();
    let all: Vec<Availability> = db
        .range_by_pk_tx::<Availability>(txn, &[], &[0xFF], None)
        .await?;

    for a in all {
        if !a.complete {
            continue;
        }
        if let Some(max_lvl) = level_limit {
            if a.level as usize > max_lvl {
                continue;
            }
        }
        if !peer_filter(&a) {
            continue;
        }
        if a.range.contains(target) {
            peers.insert(a.peer_id);
        }
    }

    Ok(peers.len())
}

/// Transaction-aware child count for a single availability (just the embedded list length).
pub async fn child_count_by_availability(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    availability_id: &UuidBytes,
) -> Result<usize, StorageError> {
    let cs = match txn {
        Some(t) => ChildSet::open_tx(db, t, *availability_id).await?,
        None => ChildSet::open(db, *availability_id).await?,
    };
    Ok(cs.count())
}

/// Transaction-aware read of raw child IDs embedded in an availability.
pub async fn children_by_availability(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    availability_id: &UuidBytes,
) -> Result<Vec<UuidBytes>, StorageError> {
    let cs = match txn {
        Some(t) => ChildSet::open_tx(db, t, *availability_id).await?,
        None => ChildSet::open(db, *availability_id).await?,
    };
    Ok(cs.children.clone())
}

/// Level-agnostic **local** child counter under a peer’s roots:
/// 1) If cube dims resolve to known storage tables, count **storage atoms** (fast, no decode).
/// 2) Otherwise, fall back to availability-frontier counting via `range_cover`.
pub async fn local_child_count_under_peer(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    peer: &UuidBytes,
    cube: &RangeCube,
) -> Result<usize, StorageError> {
    if let Some(n) = storage_atom_count_in_cube_tx(db, txn, cube).await? {
        return Ok(n);
    }

    // ---- fallback: walk local availability trees (frontier only) ----
    let roots: Vec<Availability> = roots_for_peer(db, txn, peer).await?;
    if roots.is_empty() {
        return Ok(0);
    }
    let root_ids: Vec<UuidBytes> = roots.iter().map(|r| r.key).collect();

    let (frontier, no_overlap) = range_cover(db, txn, cube, &root_ids, None).await?;
    if no_overlap {
        return Ok(0);
    }

    let mut seen = HashSet::<UuidBytes>::new();
    let mut count = 0usize;
    for a in frontier {
        if a.peer_id == *peer && a.complete && cube.contains(&a.range) && seen.insert(a.key) {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        clock::MockClock,
        plugins::harmonizer::{
            availability::Availability, child_set::ChildSet, range_cube::RangeCube,
        },
        storage_entity::{IndexCardinality, IndexSpec, StorageEntity},
    };
    use redb::TableDefinition;
    use smallvec::smallvec;
    use std::sync::Arc;
    use tempfile::tempdir;

    // ---------- Helpers ----------

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
            dir.path()
                .join("availability_queries.redb")
                .to_str()
                .unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();
        db.create_table_and_indexes::<Child>().unwrap();
        db.create_table_and_indexes::<DigestChunk>().unwrap();
        db
    }

    // ---------- range_cover core behavior ----------

    #[tokio::test]
    async fn range_cover_descends_and_collects_frontier() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 1 };

        let leaf_hit = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"g", b"n"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let leaf_miss = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"x", b"z"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let root = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"a", b"z"),
            level: 2,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(root.clone()).await.unwrap();
        db.put(leaf_hit.clone()).await.unwrap();
        db.put(leaf_miss.clone()).await.unwrap();

        // Link root → leaf_hit, leaf_miss
        let mut cs = ChildSet::open(&db, root.key).await.unwrap();
        cs.add_child(&db, leaf_hit.key).await.unwrap();
        cs.add_child(&db, leaf_miss.key).await.unwrap();

        let target = cube(dim, b"h", b"m");
        let (nodes, no_overlap) = range_cover(&db, None, &target, &[root.key], None)
            .await
            .unwrap();

        assert!(!no_overlap);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].key, leaf_hit.key);
    }

    #[tokio::test]
    async fn range_cover_returns_parent_if_any_child_missing() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 2 };

        let missing = UuidBytes::new();
        let present = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"l", b"o"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let parent = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"a", b"z"),
            level: 2,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(parent.clone()).await.unwrap();
        db.put(present.clone()).await.unwrap();

        // Link parent → {missing, present}
        let mut cs = ChildSet::open(&db, parent.key).await.unwrap();
        cs.add_child(&db, missing).await.unwrap();
        cs.add_child(&db, present.key).await.unwrap();

        let target = cube(dim, b"m", b"n");
        let (nodes, no_overlap) = range_cover(&db, None, &target, &[parent.key], None)
            .await
            .unwrap();

        assert!(!no_overlap);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].key, parent.key);
    }

    #[tokio::test]
    async fn range_cover_respects_level_budget() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 3 };

        let c2 = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"h", b"i"),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let c1 = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"f", b"k"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let root = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"a", b"z"),
            level: 2,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(root.clone()).await.unwrap();
        db.put(c1.clone()).await.unwrap();
        db.put(c2.clone()).await.unwrap();

        // Link root → c1 → c2
        let mut cs = ChildSet::open(&db, root.key).await.unwrap();
        cs.add_child(&db, c1.key).await.unwrap();
        let mut cs1 = ChildSet::open(&db, c1.key).await.unwrap();
        cs1.add_child(&db, c2.key).await.unwrap();

        let target = cube(dim, b"h", b"i");

        let (nodes, _) = range_cover(&db, None, &target, &[root.key], Some(1))
            .await
            .unwrap();
        assert!(
            nodes.is_empty(),
            "budget exhausted before reaching a frontier"
        );

        let (nodes2, _) = range_cover(&db, None, &target, &[root.key], None)
            .await
            .unwrap();
        assert_eq!(nodes2.len(), 1);
        assert_eq!(nodes2[0].key, c2.key);
    }

    #[tokio::test]
    async fn child_helpers_snapshot_semantics() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 4 };

        let id = UuidBytes::new();
        let a = Availability {
            key: id,
            peer_id: UuidBytes::new(),
            range: cube(dim, b"a", b"b"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(a.clone()).await.unwrap();

        let txn = db.begin_read_txn().unwrap();

        // Add a child after snapshot creation
        let mut cs = ChildSet::open(&db, id).await.unwrap();
        cs.add_child(&db, UuidBytes::new()).await.unwrap();

        let cnt_snap = child_count_by_availability(&db, Some(&txn), &id)
            .await
            .unwrap();
        assert_eq!(cnt_snap, 0);
        let cnt_live = child_count_by_availability(&db, None, &id).await.unwrap();
        assert_eq!(cnt_live, 1);

        let kids_live = children_by_availability(&db, None, &id).await.unwrap();
        assert_eq!(kids_live.len(), 1);
    }

    // ---------- Storage-atom counting (no deserialization) ----------

    // A tiny test entity with a UNIQUE and a NON-UNIQUE index so we can exercise
    // the PK and index axes in storage_atom_count_in_cube_tx()
    #[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    struct TEnt {
        id: u64,
        tag: u8,
        uniq: u8,
    }

    static TBL: TableDefinition<'static, &'static [u8], Vec<u8>> = TableDefinition::new("t_ent");
    static META: TableDefinition<'static, &'static [u8], Vec<u8>> =
        TableDefinition::new("t_ent_meta");
    static IDX_TAG: TableDefinition<'static, &'static [u8], Vec<u8>> =
        TableDefinition::new("t_ent_tag_idx"); // NON-UNIQUE
    static IDX_UNIQ: TableDefinition<'static, &'static [u8], Vec<u8>> =
        TableDefinition::new("t_ent_uniq_idx"); // UNIQUE

    static INDEXES: &[IndexSpec<TEnt>] = &[
        IndexSpec {
            name: "tag",
            key_fn: |e: &TEnt| vec![e.tag],
            table_def: &IDX_TAG,
            cardinality: IndexCardinality::NonUnique,
        },
        IndexSpec {
            name: "uniq",
            key_fn: |e: &TEnt| vec![e.uniq],
            table_def: &IDX_UNIQ,
            cardinality: IndexCardinality::Unique,
        },
    ];

    impl StorageEntity for TEnt {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> {
            self.id.to_be_bytes().to_vec()
        }
        fn table_def() -> crate::StaticTableDef {
            &TBL
        }
        fn meta_table_def() -> crate::StaticTableDef {
            &META
        }
        fn load_and_migrate(data: &[u8]) -> Result<Self, crate::storage_error::StorageError> {
            match data.first().copied() {
                Some(0) => bincode::decode_from_slice(&data[1..], bincode::config::standard())
                    .map(|(v, _)| v)
                    .map_err(|e| crate::storage_error::StorageError::Bincode(e.to_string())),
                _ => Err(crate::storage_error::StorageError::Bincode(
                    "bad version".into(),
                )),
            }
        }
        fn indexes() -> &'static [IndexSpec<Self>] {
            INDEXES
        }
    }

    async fn db_with_tent() -> Arc<KuramotoDb> {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("atoms.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<TEnt>().unwrap();

        // Insert 5 rows:
        // ids 1..=5, tags = [1,1,2,2,2], uniq = [10,11,12,13,14]
        for (i, tag, uniq) in [(1, 1, 10), (2, 1, 11), (3, 2, 12), (4, 2, 13), (5, 2, 14)] {
            let e = TEnt { id: i, tag, uniq };
            db.put(e).await.unwrap();
        }
        db
    }

    #[tokio::test]
    async fn atoms_count_pk_only() {
        let db = db_with_tent().await;
        let pk_dim = TableHash::from(TEnt::table_def());
        // PK range: [2,5) → ids 2,3,4
        let r = cube(pk_dim, &2u64.to_be_bytes(), &5u64.to_be_bytes());
        let n = super::storage_atom_count_in_cube_tx(&db, None, &r)
            .await
            .unwrap();
        assert_eq!(n, Some(3));
    }

    #[tokio::test]
    async fn atoms_count_index_only_nonunique() {
        let db = db_with_tent().await;
        let tag_dim = TableHash::from(&IDX_TAG);
        // tag == 2 → ids 3,4,5
        let r = cube(tag_dim, &[2u8], &[3u8]);
        let n = super::storage_atom_count_in_cube_tx(&db, None, &r)
            .await
            .unwrap();
        assert_eq!(n, Some(3));
    }

    #[tokio::test]
    async fn atoms_intersect_pk_and_index() {
        let db = db_with_tent().await;
        let pk_dim = TableHash::from(TEnt::table_def());
        let tag_dim = TableHash::from(&IDX_TAG);

        // PK [2,5) → {2,3,4}, tag==2   → {3,4,5}; intersection = {3,4} → 2 items
        let r = RangeCube {
            dims: smallvec![pk_dim, tag_dim],
            mins: smallvec![2u64.to_be_bytes().to_vec(), vec![2u8]],
            maxs: smallvec![5u64.to_be_bytes().to_vec(), vec![3u8]],
        };

        let n = super::storage_atom_count_in_cube_tx(&db, None, &r)
            .await
            .unwrap();
        assert_eq!(n, Some(2));
    }

    #[tokio::test]
    async fn atoms_intersect_two_indexes() {
        let db = db_with_tent().await;
        let uniq_dim = TableHash::from(&IDX_UNIQ);
        let tag_dim = TableHash::from(&IDX_TAG);

        // uniq in [12,14) → uniq = 12,13 → ids {3,4}
        // tag == 2        → ids {3,4,5}
        // intersection    → {3,4} → 2
        let r = RangeCube {
            dims: smallvec![uniq_dim, tag_dim],
            mins: smallvec![vec![12u8], vec![2u8]],
            maxs: smallvec![vec![14u8], vec![3u8]],
        };

        let n = super::storage_atom_count_in_cube_tx(&db, None, &r)
            .await
            .unwrap();
        assert_eq!(n, Some(2));
    }

    #[tokio::test]
    async fn atoms_unknown_dim_returns_none() {
        let db = db_with_tent().await;
        let fake = TableHash { hash: 0xDEADBEEF };
        let r = cube(fake, b"a", b"z");
        let n = super::storage_atom_count_in_cube_tx(&db, None, &r)
            .await
            .unwrap();
        assert_eq!(n, None);
    }
}
