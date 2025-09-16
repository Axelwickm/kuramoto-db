use std::collections::VecDeque;

use redb::ReadTransaction;

use crate::{
    storage_error::StorageError,
    uuid_bytes::UuidBytes,
    KuramotoDb,
};

use crate::plugins::harmonizer::{
    availability::{Availability, roots_for_peer},
    availability_queries::storage_atom_pks_in_cube_tx,
    child_set::ChildSet,
};

#[derive(Clone, Debug, PartialEq)]
pub enum IntegrityErrorKind {
    IncompleteNode,
    MissingNode,
    MissingChild { child: UuidBytes },
    RangeNotContained { child: UuidBytes },
    EmptyLeaf,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IntegrityError {
    pub node: UuidBytes,
    pub kind: IntegrityErrorKind,
}

#[derive(Clone, Debug)]
pub struct IntegrityCursor {
    pub peer: UuidBytes,
    roots: Vec<UuidBytes>,
    root_idx: usize,
    stack: VecDeque<UuidBytes>,
}

#[derive(Clone, Debug, Default)]
pub struct IntegrityReport {
    pub errors: Vec<IntegrityError>,
    pub processed: usize,
    pub done: bool,
    pub cursor: Option<IntegrityCursor>,
}

/// Initialize an integrity check over all local availability trees for `peer`.
pub async fn integrity_start(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    peer: UuidBytes,
) -> Result<IntegrityCursor, StorageError> {
    let roots: Vec<Availability> = roots_for_peer(db, txn, &peer).await?;
    let mut stack = VecDeque::new();
    if let Some(first) = roots.first() { stack.push_back(first.key); }
    Ok(IntegrityCursor {
        peer,
        roots: roots.into_iter().map(|r| r.key).collect(),
        root_idx: 0,
        stack,
    })
}

/// Run up to `max_steps` nodes (or all when None). Returns accumulated errors and a resumable cursor.
pub async fn integrity_step(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    mut cur: IntegrityCursor,
    expect_complete: bool,
    max_steps: Option<usize>,
) -> Result<IntegrityReport, StorageError> {
    let mut rep = IntegrityReport::default();
    let mut steps_left = max_steps.unwrap_or(usize::MAX);

    // Helper to advance to next root
    let mut advance_root = |cur: &mut IntegrityCursor| {
        cur.root_idx += 1;
        if cur.root_idx < cur.roots.len() {
            cur.stack.push_back(cur.roots[cur.root_idx]);
            true
        } else {
            false
        }
    };

    while steps_left > 0 {
        if let Some(id) = cur.stack.pop_front() {
            steps_left -= 1;
            rep.processed += 1;

            let av = match db.get_data_tx::<Availability>(txn, id.as_bytes()).await {
                Ok(v) => v,
                Err(StorageError::NotFound) => {
                    rep.errors.push(IntegrityError { node: id, kind: IntegrityErrorKind::MissingNode });
                    continue;
                }
                Err(e) => return Err(e),
            };

            // Peer filter: only check local nodes
            if av.peer_id != cur.peer {
                continue;
            }

            if expect_complete && !av.complete {
                rep.errors.push(IntegrityError { node: av.key, kind: IntegrityErrorKind::IncompleteNode });
            }

            // Child relations
            let cs = match txn {
                Some(t) => ChildSet::open_tx(db, t, av.key).await?,
                None => ChildSet::open(db, av.key).await?,
            };

            if cs.children.is_empty() {
                // Leaf: sanity-check storage atoms exist in this cube (best effort)
                if let Some(groups) = storage_atom_pks_in_cube_tx(db, txn, &av.range, Some(1)).await? {
                    let total: usize = groups.into_iter().map(|(_, v)| v.len()).sum();
                    if total == 0 {
                        rep.errors.push(IntegrityError { node: av.key, kind: IntegrityErrorKind::EmptyLeaf });
                    }
                }
            } else {
                // Check children exist and are contained; enqueue them
                for cid in cs.children.iter() {
                    match db.get_data_tx::<Availability>(txn, cid.as_bytes()).await {
                        Ok(child) => {
                            if !av.range.contains(&child.range) {
                                rep.errors.push(IntegrityError { node: av.key, kind: IntegrityErrorKind::RangeNotContained { child: *cid } });
                            }
                            cur.stack.push_back(*cid);
                        }
                        Err(StorageError::NotFound) => {
                            rep.errors.push(IntegrityError { node: av.key, kind: IntegrityErrorKind::MissingChild { child: *cid } });
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        } else {
            // Move to next root or finish
            if !advance_root(&mut cur) {
                rep.done = true;
                rep.cursor = None;
                return Ok(rep);
            }
        }
    }

    rep.done = false;
    rep.cursor = Some(cur);
    Ok(rep)
}

/// Convenience: run to completion and return errors.
pub async fn integrity_run_all(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    peer: UuidBytes,
    expect_complete: bool,
) -> Result<Vec<IntegrityError>, StorageError> {
    let mut cur = integrity_start(db, txn, peer).await?;
    let mut errs = Vec::new();
    loop {
        let rep = integrity_step(db, txn, cur, expect_complete, None).await?;
        errs.extend(rep.errors);
        if rep.done { break; }
        cur = rep.cursor.expect("cursor expected when not done");
    }
    Ok(errs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::harmonizer::{availability::Availability, range_cube::RangeCube};
    use crate::clock::MockClock;
    use crate::tables::TableHash;
    use crate::storage_entity::{AuxTableSpec, StorageEntity};
    use smallvec::smallvec;
    use tempfile::tempdir;
    use std::sync::Arc;

    // A tiny test entity to exercise storage-atom checks
    #[derive(Clone, Debug, PartialEq, bincode::Encode, bincode::Decode)]
    struct Foo { id: u64 }
    impl crate::storage_entity::StorageEntity for Foo {
        const STRUCT_VERSION: u8 = 0;
        fn primary_key(&self) -> Vec<u8> { self.id.to_be_bytes().to_vec() }
        fn table_def() -> crate::StaticTableDef {
            static T: redb::TableDefinition<'static, &'static [u8], Vec<u8>> = redb::TableDefinition::new("t_foo");
            &T
        }
        fn aux_tables() -> &'static [AuxTableSpec] {
            static M: redb::TableDefinition<'static, &'static [u8], Vec<u8>> = redb::TableDefinition::new("t_foo_meta");
            static AUX: &[AuxTableSpec] = &[AuxTableSpec {
                role: crate::plugins::versioning::VERSIONING_AUX_ROLE,
                table: &M,
            }];
            AUX
        }
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

    async fn fresh_db() -> Arc<KuramotoDb> {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("integrity.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();
        db.create_table_and_indexes::<crate::plugins::harmonizer::child_set::Child>().unwrap();
        db.create_table_and_indexes::<crate::plugins::harmonizer::child_set::DigestChunk>().unwrap();
        db
    }

    fn cube(dim: TableHash, min: &[u8], max: &[u8]) -> RangeCube {
        RangeCube::new(
            smallvec![dim],
            smallvec![min.to_vec()],
            smallvec![max.to_vec()],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn detects_missing_child_and_range_violation() {
        let db = fresh_db().await;
        let peer = UuidBytes::new();
        let d = TableHash { hash: 1 };
        let parent = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, b"a", b"z"), level: 1, schema_hash: 0, version: 0, updated_at: 0, complete: true };
        let child_bad = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, b"z", b"\xFF"), level: 0, schema_hash: 0, version: 0, updated_at: 0, complete: true };
        db.put(parent.clone()).await.unwrap();
        db.put(child_bad.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, parent.key).await.unwrap();
        // Attach missing child id first, then a child with range outside parent
        let missing = UuidBytes::new();
        cs.add_child(&db, missing).await.unwrap();
        cs.add_child(&db, child_bad.key).await.unwrap();

        let errs = integrity_run_all(&db, None, peer, true).await.unwrap();
        assert!(errs.iter().any(|e| matches!(e.kind, IntegrityErrorKind::MissingChild { .. })), "should detect missing child");
        assert!(errs.iter().any(|e| matches!(e.kind, IntegrityErrorKind::RangeNotContained { .. })), "should detect range violation");
    }

    #[tokio::test]
    async fn happy_valid_chain_no_errors() {
        let db = fresh_db().await;
        let peer = UuidBytes::new();
        let d = TableHash { hash: 5 };
        let root = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, b"a", b"z"), level: 2, schema_hash: 0, version: 0, updated_at: 0, complete: true };
        let child = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, b"m", b"n"), level: 1, schema_hash: 0, version: 0, updated_at: 0, complete: true };
        db.put(root.clone()).await.unwrap();
        db.put(child.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, root.key).await.unwrap();
        cs.add_child(&db, child.key).await.unwrap();

        let errs = integrity_run_all(&db, None, peer, true).await.unwrap();
        assert!(errs.is_empty(), "no errors expected, got {errs:?}");
    }

    #[tokio::test]
    async fn missing_node_detected_after_deletion() {
        let db = fresh_db().await;
        let peer = UuidBytes::new();
        let d = TableHash { hash: 6 };
        let root = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, b"a", b"z"), level: 0, schema_hash: 0, version: 0, updated_at: 0, complete: true };
        db.put(root.clone()).await.unwrap();

        // Start cursor, then delete the node and step
        let cur = integrity_start(&db, None, peer).await.unwrap();
        db.delete::<Availability>(&root.primary_key()).await.unwrap();
        let rep = integrity_step(&db, None, cur, true, Some(1)).await.unwrap();
        assert!(rep.errors.iter().any(|e| matches!(e.kind, IntegrityErrorKind::MissingNode)), "should report missing node");
    }

    #[tokio::test]
    async fn incomplete_node_reported_when_expected_complete() {
        let db = fresh_db().await;
        let peer = UuidBytes::new();
        let d = TableHash { hash: 7 };
        let root = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, b"a", b"b"), level: 0, schema_hash: 0, version: 0, updated_at: 0, complete: false };
        db.put(root.clone()).await.unwrap();
        let errs = integrity_run_all(&db, None, peer, true).await.unwrap();
        assert!(errs.iter().any(|e| matches!(e.kind, IntegrityErrorKind::IncompleteNode)), "should report incomplete node");
    }

    #[tokio::test]
    async fn empty_leaf_detected_with_known_table() {
        let db = fresh_db().await;
        db.create_table_and_indexes::<Foo>().unwrap();
        let peer = UuidBytes::new();
        let d = TableHash::from(Foo::table_def());
        // Leaf range over [10,11), no Foo rows inserted
        let leaf = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, &10u64.to_be_bytes(), &11u64.to_be_bytes()), level: 0, schema_hash: 0, version: 0, updated_at: 0, complete: true };
        db.put(leaf.clone()).await.unwrap();
        let errs = integrity_run_all(&db, None, peer, true).await.unwrap();
        assert!(errs.iter().any(|e| matches!(e.kind, IntegrityErrorKind::EmptyLeaf)), "expected EmptyLeaf error");
    }

    #[tokio::test]
    async fn non_empty_leaf_ok_with_known_table() {
        let db = fresh_db().await;
        db.create_table_and_indexes::<Foo>().unwrap();
        db.put(Foo { id: 42 }).await.unwrap();
        let peer = UuidBytes::new();
        let d = TableHash::from(Foo::table_def());
        let leaf = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, &42u64.to_be_bytes(), &43u64.to_be_bytes()), level: 0, schema_hash: 0, version: 0, updated_at: 0, complete: true };
        db.put(leaf.clone()).await.unwrap();
        let errs = integrity_run_all(&db, None, peer, true).await.unwrap();
        assert!(errs.is_empty(), "no errors expected, got {errs:?}");
    }

    #[tokio::test]
    async fn stepwise_resume_works() {
        let db = fresh_db().await;
        let peer = UuidBytes::new();
        let d = TableHash { hash: 8 };
        let r = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, b"a", b"z"), level: 2, schema_hash: 0, version: 0, updated_at: 0, complete: true };
        let c1 = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, b"b", b"c"), level: 1, schema_hash: 0, version: 0, updated_at: 0, complete: true };
        let c2 = Availability { key: UuidBytes::new(), peer_id: peer, range: cube(d, b"d", b"e"), level: 1, schema_hash: 0, version: 0, updated_at: 0, complete: true };
        db.put(r.clone()).await.unwrap();
        db.put(c1.clone()).await.unwrap();
        db.put(c2.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, r.key).await.unwrap();
        cs.add_child(&db, c1.key).await.unwrap();
        cs.add_child(&db, c2.key).await.unwrap();

        let cur = integrity_start(&db, None, peer).await.unwrap();
        // Step once
        let rep1 = integrity_step(&db, None, cur, true, Some(1)).await.unwrap();
        assert_eq!(rep1.processed, 1);
        assert!(!rep1.done);
        let cur2 = rep1.cursor.unwrap();
        // Finish
        let rep2 = integrity_step(&db, None, cur2, true, None).await.unwrap();
        assert!(rep2.done);
        assert!(rep2.errors.is_empty());
        assert!(rep2.cursor.is_none());
    }
}
