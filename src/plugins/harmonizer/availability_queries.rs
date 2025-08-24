use redb::ReadTransaction;
use std::collections::HashSet;

use crate::{
    KuramotoDb,
    plugins::harmonizer::{availability::Availability, range_cube::RangeCube},
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

/// Find a small set of nodes **reachable from the provided roots** that “cover” `target`
/// by descending the child links. This avoids full-table scans.
///
/// Traversal behavior:
/// - We only descend into a node if it **intersects** `target`.
/// - If the node is **incomplete**, or a **leaf** (no children), or we **fail to resolve**
///   any of its children, we **return that node** as part of the cover (frontier).
/// - Otherwise, we recurse into all resolvable children that intersect `target`.
///
/// Returns `(nodes, no_overlap)` where `no_overlap == true` iff **none** of the visited
/// nodes intersected `target` (i.e., traversal never “touched” the target).
///
/// NOTE: This does **not** compute a global containment-minimal antichain like the old
/// scan-based version; instead it returns the *frontier* you should ask for next (or
/// consider for adoption), strictly under the given roots.
pub async fn range_cover(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    target: &RangeCube,
    root_ids: &[UuidBytes],
    level_limit: Option<usize>,
) -> Result<(Vec<Availability>, bool), StorageError> {
    // Local, txn-aware loader to keep the dfs body clean.
    async fn load_avail_tx(
        db: &KuramotoDb,
        txn: Option<&ReadTransaction>,
        id: &UuidBytes,
    ) -> Result<Option<Availability>, StorageError> {
        match db.get_data_tx::<Availability>(txn, id.as_bytes()).await {
            Ok(a) => Ok(Some(a)),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    #[async_recursion::async_recursion]
    async fn dfs(
        db: &KuramotoDb,
        txn: Option<&ReadTransaction>,
        acc: &mut Vec<Availability>,
        cube: &RangeCube,
        id: UuidBytes,
        level_left: Option<usize>,
        any_overlap: &mut bool,
    ) -> Result<(), StorageError> {
        // Level budget exhausted → stop (do not emit this node)
        if level_left == Some(0) {
            return Ok(());
        }
        let next_budget = level_left.map(|d| d.saturating_sub(1));

        // Load node (from the same snapshot if txn is provided)
        let Some(av) = load_avail_tx(db, txn, &id).await? else {
            return Ok(());
        };

        // Only consider nodes that *touch* target
        if av.range.intersect(cube).is_none() {
            return Ok(());
        }
        *any_overlap = true;

        let is_leaf = av.children.count() == 0;

        // If we can't (or shouldn't) go deeper, return this node as frontier.
        if !av.complete || is_leaf {
            acc.push(av);
            return Ok(());
        }

        // Try to descend. If **any** child cannot be resolved, we conservatively
        // return the current node (frontier) rather than risk a “hole”.
        let mut all_ok = true;
        let mut child_ids: Vec<UuidBytes> = Vec::with_capacity(av.children.count());
        for cid in &av.children.children {
            match resolve_child_avail(db, txn, cid).await? {
                Some(_) => child_ids.push(*cid),
                None => {
                    all_ok = false;
                    break;
                }
            }
        }

        if !all_ok {
            acc.push(av);
            return Ok(());
        }

        // Recurse into all *resolvable* children that may contribute.
        for cid in child_ids {
            dfs(db, txn, acc, cube, cid, next_budget, any_overlap).await?;
        }
        Ok(())
    }

    let mut results = Vec::<Availability>::new();
    let mut any_overlap = false;

    for rid in root_ids {
        dfs(
            db,
            txn,
            &mut results,
            target,
            *rid,
            level_limit,
            &mut any_overlap,
        )
        .await?;
    }

    // Dedup (roots may share subtrees)
    let mut seen = HashSet::<UuidBytes>::new();
    results.retain(|a| seen.insert(a.key));

    Ok((results, !any_overlap))
}

/// Count distinct peers that **fully contain** `target` with at least one **complete** availability.
/// This remains scan-based (global view), because replication is a *global* property, not tied to
/// a particular root. If you want a root-scoped replication view, call `range_cover` first and
/// aggregate over the returned nodes.
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

/// Transaction-aware child count for an availability.
pub async fn child_count_by_availability(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    availability_id: &UuidBytes,
) -> Result<usize, StorageError> {
    let a: Availability = db
        .get_data_tx::<Availability>(txn, availability_id.as_bytes())
        .await?;
    Ok(a.children.count())
}

/// Transaction-aware read of the raw child IDs (availability keys) embedded in an availability.
pub async fn children_by_availability(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    availability_id: &UuidBytes,
) -> Result<Vec<UuidBytes>, StorageError> {
    let a: Availability = db
        .get_data_tx::<Availability>(txn, availability_id.as_bytes())
        .await?;
    Ok(a.children.children.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        clock::MockClock,
        plugins::harmonizer::{
            availability::Availability, child_set::ChildSet, range_cube::RangeCube,
        },
        tables::TableHash,
        uuid_bytes::UuidBytes,
    };
    use smallvec::smallvec;
    use std::sync::Arc;
    use tempfile::tempdir;

    // One-axis cube builder to keep test fixtures tiny.
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
                .join("availability_queries_roots.redb")
                .to_str()
                .unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();
        db
    }

    #[tokio::test]
    async fn range_cover_descends_from_roots_and_collects_frontier() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 1 };

        // Build a small tree: root -> {c1, c2}
        let c1 = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"g", b"n"), // intersects target
            level: 1,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![], // leaf
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let c2 = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"x", b"z"), // does NOT intersect target
            level: 1,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let root = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"a", b"z"), // intersects target
            level: 2,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![c1.key, c2.key],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(root.clone()).await.unwrap();
        db.put(c1.clone()).await.unwrap();
        db.put(c2.clone()).await.unwrap();

        let target = cube(dim, b"h", b"m");

        let (nodes, no_overlap) = range_cover(&db, None, &target, &[root.key], None)
            .await
            .unwrap();

        assert!(!no_overlap, "root and c1 overlap target");
        // We should get c1 (leaf that overlaps). c2 is filtered by intersection. Root is not returned
        // because it had resolvable children and is complete.
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].key, c1.key);
    }

    #[tokio::test]
    async fn range_cover_returns_parent_when_a_child_is_missing() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 2 };

        let missing = UuidBytes::new();
        let present = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"l", b"o"),
            level: 1,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
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
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![missing, present.key],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };

        db.put(parent.clone()).await.unwrap();
        db.put(present.clone()).await.unwrap();

        let target = cube(dim, b"m", b"n");

        let (nodes, no_overlap) = range_cover(&db, None, &target, &[parent.key], None)
            .await
            .unwrap();

        assert!(!no_overlap);
        // Because one child is missing, we conservatively return the parent.
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].key, parent.key);
    }

    #[tokio::test]
    async fn range_cover_stops_at_incomplete_or_leaf() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 3 };

        // Incomplete parent → should be returned, no descent
        let inc = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"a", b"z"),
            level: 1,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![UuidBytes::new()],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: false, // key point
        };
        db.put(inc.clone()).await.unwrap();

        let target = cube(dim, b"h", b"m");
        let (nodes, _) = range_cover(&db, None, &target, &[inc.key], None)
            .await
            .unwrap();

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].key, inc.key);
    }

    #[tokio::test]
    async fn range_cover_respects_level_limit_budget() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 4 };

        // root -> c1 -> c2 (c2 is leaf overlapping the target)
        let c2 = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"h", b"i"),
            level: 0,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
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
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![c2.key],
            },
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
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![c1.key],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(root.clone()).await.unwrap();
        db.put(c1.clone()).await.unwrap();
        db.put(c2.clone()).await.unwrap();

        let target = cube(dim, b"h", b"i");

        // With budget = 1, we can visit root (depth 0) and c1 (depth 1), but not c2.
        let (nodes, _) = range_cover(&db, None, &target, &[root.key], Some(1))
            .await
            .unwrap();

        // At budget boundary we don't emit a frontier node automatically; result may be empty.
        assert!(
            nodes.is_empty(),
            "budget exhausted before reaching a frontier"
        );

        // With no limit, we reach c2 and return it (leaf frontier).
        let (nodes2, _) = range_cover(&db, None, &target, &[root.key], None)
            .await
            .unwrap();
        assert_eq!(nodes2.len(), 1);
        assert_eq!(nodes2[0].key, c2.key);
    }

    #[tokio::test]
    async fn child_helpers_and_snapshot_semantics() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 5 };

        let id = UuidBytes::new();
        let mut a = Availability {
            key: id,
            peer_id: UuidBytes::new(),
            range: cube(dim, b"a", b"b"),
            level: 1,
            children: ChildSet {
                parent: id,
                children: vec![UuidBytes::new(), UuidBytes::new()],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(a.clone()).await.unwrap();

        // Snapshot txn
        let txn = db.begin_read_txn().unwrap();

        // Mutate live state
        a.children.children.push(UuidBytes::new());
        db.put(a.clone()).await.unwrap();

        // Count with snapshot → still 2
        let cnt_snap = child_count_by_availability(&db, Some(&txn), &id)
            .await
            .unwrap();
        assert_eq!(cnt_snap, 2);

        // Live count → 3
        let cnt_live = child_count_by_availability(&db, None, &id).await.unwrap();
        assert_eq!(cnt_live, 3);

        // Children readback matches
        let kids_live = children_by_availability(&db, None, &id).await.unwrap();
        assert_eq!(kids_live.len(), 3);
    }

    #[tokio::test]
    async fn resolve_child_avail_present_and_missing() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 6 };

        let child = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"m", b"n"),
            level: 0,
            children: ChildSet {
                parent: UuidBytes::new(),
                children: vec![],
            },
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(child.clone()).await.unwrap();

        let ok = resolve_child_avail(&db, None, &child.key).await.unwrap();
        assert!(ok.is_some() && ok.unwrap().key == child.key);

        let missing = UuidBytes::new();
        let none = resolve_child_avail(&db, None, &missing).await.unwrap();
        assert!(none.is_none());
    }
}
