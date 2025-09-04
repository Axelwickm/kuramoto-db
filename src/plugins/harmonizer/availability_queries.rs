use redb::ReadTransaction;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

use crate::plugins::harmonizer::optimizer::Action::Delete;
use crate::plugins::harmonizer::optimizer::Action::Insert;
use crate::tables::TableHash;
use crate::{
    KuramotoDb, StaticTableDef,
    plugins::harmonizer::{
        availability::{Availability, roots_for_peer, AVAILABILITY_BY_RANGE_MIN},
        child_set::{Child, ChildSet, DigestChunk},
        optimizer::{Action, ActionSet, AvailabilityDraft},
        range_cube::RangeCube,
    },
    storage_error::StorageError,
    uuid_bytes::UuidBytes,
};

/*──────────────────────── Query cache (snapshot-aware) ─────────────────────*/

#[derive(Default)]
pub struct AvailabilityQueryCache {
    txn_tag: usize,
    // DB snapshot memoization (overlay applied on top per call)
    roots_by_peer: HashMap<UuidBytes, Vec<UuidBytes>>, // peer -> root ids
    childset_by_parent: HashMap<UuidBytes, ChildSet>,  // parent id -> child set
    avail_by_id: HashMap<UuidBytes, Availability>,     // availability rows by id
}
impl AvailabilityQueryCache {
    pub fn new(txn: &ReadTransaction) -> Self {
        Self {
            txn_tag: (txn as *const ReadTransaction) as usize,
            ..Default::default()
        }
    }
    pub fn compatible_txn(&self, txn: Option<&ReadTransaction>) -> bool {
        match txn {
            Some(t) => (t as *const ReadTransaction) as usize == self.txn_tag,
            None => false,
        }
    }
}

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
    overlay: &ActionSet,
    cache: &mut Option<&mut AvailabilityQueryCache>,
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
        cache: &mut Option<&mut AvailabilityQueryCache>,
    ) -> Result<(), StorageError> {
        if budget == Some(0) {
            return Ok(());
        }
        let next_budget = budget.map(|d| d.saturating_sub(1));

        // Load node from the same snapshot (if any)
        let Some(av) = resolve_child_avail_cached(db, txn, &id, cache).await? else {
            return Ok(());
        };

        // Only consider nodes that *touch* target
        if av.range.intersect(cube).is_none() {
            return Ok(());
        }
        *touched = true;

        // Leaf or incomplete → frontier (children resolved via child table)
        let cs = open_childset_cached(db, txn, av.key, cache).await?;
        let is_leaf = cs.count() == 0;
        if !av.complete || is_leaf {
            out.push(av);
            return Ok(());
        }

        // Resolve all children first; if any is missing → frontier
        let mut child_ids = Vec::with_capacity(cs.count());
        for cid in &cs.children {
            match resolve_child_avail_cached(db, txn, cid, cache).await? {
                Some(_) => child_ids.push(*cid),
                None => {
                    out.push(av);
                    return Ok(());
                }
            }
        }

        // Recurse into resolvable children
        for cid in child_ids {
            dfs(db, txn, out, cube, cid, next_budget, touched, cache).await?;
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
            cache,
        )
        .await?;
    }

    // Dedup by key (roots may share subtrees)
    let mut seen = HashSet::<UuidBytes>::new();
    results.retain(|a| seen.insert(a.key));

    // Overlay-aware: mask out deletes (deletes hide nodes; inserts ignored here)
    if !overlay.is_empty() {
        let mut deleted = HashSet::<UuidBytes>::new();
        for act in overlay {
            if let Delete(id) = act {
                deleted.insert(*id);
            }
        }
        if !deleted.is_empty() {
            results.retain(|a| !deleted.contains(&a.key));
        }
    }

    Ok((results, !touched))
}

/// Internal: cached resolve by id (DB snapshot only)
async fn resolve_child_avail_cached(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    child_id: &UuidBytes,
    cache: &mut Option<&mut AvailabilityQueryCache>,
) -> Result<Option<Availability>, StorageError> {
    if let Some(ref mut c) = cache.as_deref_mut() {
        if c.compatible_txn(txn) {
            if let Some(v) = c.avail_by_id.get(child_id) {
                return Ok(Some(v.clone()));
            }
        }
    }
    match db
        .get_data_tx::<Availability>(txn, child_id.as_bytes())
        .await
    {
        Ok(av) => {
            if let Some(ref mut c) = cache.as_deref_mut() {
                if c.compatible_txn(txn) {
                    c.avail_by_id.insert(*child_id, av.clone());
                }
            }
            Ok(Some(av))
        }
        Err(StorageError::NotFound) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Deterministically synthesize a pseudo-key from an availability draft.
/// This is only used to make overlay inserts act like regular rows during planning-time queries.
// No overlay insert synthesis here — range_cover deals only with availability rows in DB.

/// Try to count storage atoms in `cube` **without deserializing entities**.
/// Returns `Ok(Some(count))` if the cube could be resolved to registered tables; `Ok(None)`
/// if the dims are unknown (caller should fall back to availability counting).
pub(crate) async fn storage_atom_count_in_cube_tx(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    cube: &RangeCube,
) -> Result<Option<usize>, StorageError> {
    // Group dims by **parent data table**:
    // - PK axis: TableHash points directly to a data table
    // - Index axis: TableHash resolves to (index_table, parent_data_table)
    struct Group {
        parent_data: StaticTableDef,
        data: Option<(StaticTableDef, Vec<u8>, Vec<u8>)>, // (data_table, lo, hi)
        idxs: Vec<(StaticTableDef, Vec<u8>, Vec<u8>)>,    // (index_table, lo, hi)
    }
    let mut by_parent: BTreeMap<u64, Group> = BTreeMap::new();

    let n = cube
        .dims()
        .len()
        .min(cube.mins().len())
        .min(cube.maxs().len());
    for i in 0..n {
        let h = cube.dims()[i].hash;
        let lo = cube.mins()[i].clone();
        let hi = cube.maxs()[i].clone();

        if let Some(data_tbl) = db.resolve_data_table_by_hash(h) {
            by_parent
                .entry(h)
                .or_insert(Group {
                    parent_data: data_tbl,
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
                    parent_data: parent_tbl,
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

/// Fast lookup of availabilities fully contained in `cube` using the axis-min index.
/// Picks the narrowest axis in `cube` and range-scans by `(axis_hash_be • min)`.
pub async fn availabilities_contained_in_cube_tx(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    cube: &RangeCube,
    limit: Option<usize>,
) -> Result<Vec<Availability>, StorageError> {
    // Choose the narrowest axis by (len(max) - len(min)) as a cheap heuristic.
    if cube.dims().is_empty() {
        // No constraints → nothing is "contained" in a strict sense; return empty.
        return Ok(Vec::new());
    }
    let mut best_i = 0usize;
    let mut best_span = usize::MAX;
    for i in 0..cube.dims().len() {
        let span = cube.maxs()[i].len().saturating_sub(cube.mins()[i].len());
        if span < best_span {
            best_span = span;
            best_i = i;
        }
    }

    let axis = cube.dims()[best_i];
    let mut lo = axis.hash.to_be_bytes().to_vec();
    lo.extend_from_slice(&cube.mins()[best_i]);
    let mut hi = axis.hash.to_be_bytes().to_vec();
    hi.extend_from_slice(&cube.maxs()[best_i]);
    hi.push(0x01); // half-open upper bound

    // Range-scan candidates via the index and filter by full containment.
    let candidates: Vec<Availability> = db
        .range_by_index_tx::<Availability>(txn, AVAILABILITY_BY_RANGE_MIN, &lo, &hi, limit)
        .await?;

    let mut out = Vec::with_capacity(candidates.len());
    for a in candidates.into_iter() {
        if cube.contains(&a.range) {
            out.push(a);
        }
    }
    Ok(out)
}

/// Collect storage PKs grouped by parent data table whose rows fall inside `cube`.
/// Returns `Ok(Some(map))` if cube dims resolve to known tables; `Ok(None)` if unknown dims.
pub async fn storage_atom_pks_in_cube_tx(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    cube: &RangeCube,
    limit_per_table: Option<usize>,
) -> Result<Option<Vec<(StaticTableDef, Vec<Vec<u8>>)>>, StorageError> {
    // Group dims by parent tables similarly to the counting variant, but carry parent_data.
    struct Group {
        parent_data: StaticTableDef,
        data: Option<(StaticTableDef, Vec<u8>, Vec<u8>)>,
        idxs: Vec<(StaticTableDef, Vec<u8>, Vec<u8>)>,
    }
    let mut by_parent: BTreeMap<u64, Group> = BTreeMap::new();

    let n = cube
        .dims()
        .len()
        .min(cube.mins().len())
        .min(cube.maxs().len());
    for i in 0..n {
        let h = cube.dims()[i].hash;
        let lo = cube.mins()[i].clone();
        let hi = cube.maxs()[i].clone();

        if let Some(data_tbl) = db.resolve_data_table_by_hash(h) {
            by_parent
                .entry(h)
                .or_insert(Group {
                    parent_data: data_tbl,
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
                    parent_data: parent_tbl,
                    data: None,
                    idxs: Vec::new(),
                })
                .idxs
                .push((idx_tbl, lo, hi));
        }
    }

    if by_parent.is_empty() {
        return Ok(None);
    }

    let mut out: Vec<(StaticTableDef, Vec<Vec<u8>>)> = Vec::new();
    for (_ph, g) in by_parent {
        let mut acc: Option<HashSet<Vec<u8>>> = if let Some((tbl, lo, hi)) = g.data {
            let pks = db
                .collect_pks_in_data_range_tx(txn, tbl, &lo, &hi, limit_per_table)
                .await?;
            Some(pks.into_iter().collect())
        } else {
            None
        };

        for (idx_tbl, lo, hi) in g.idxs {
            let pks = db
                .collect_pks_in_index_range_tx(txn, idx_tbl, &lo, &hi, limit_per_table)
                .await?;
            let set: HashSet<Vec<u8>> = pks.into_iter().collect();
            acc = match acc.take() {
                Some(cur) => {
                    let inter: HashSet<_> = cur.intersection(&set).cloned().collect();
                    Some(inter)
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
            let mut v: Vec<Vec<u8>> = s.into_iter().collect();
            if let Some(lim) = limit_per_table {
                if v.len() > lim {
                    v.truncate(lim);
                }
            }
            out.push((g.parent_data, v));
        }
    }

    Ok(Some(out))
}

/*──────────────────────── Level‑aware child counting ─────────────────────*/

// No per-cube memo for atoms; compute directly for clarity.

/// Count children under `cube` for a given candidate `level`.
///
/// - If `level == 0`: children are storage atoms only. If cube doesn't resolve to
///   known storage tables, returns 0.
/// - If `level > 0`: children are availability nodes reachable from local roots; overlay
///   deletes/inserts are respected similar to frontier counting.
pub async fn child_count(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    peer: &UuidBytes,
    cube: &RangeCube,
    level: u16,
    overlay: &ActionSet,
    cache: &mut Option<&mut AvailabilityQueryCache>,
) -> Result<usize, StorageError> {
    if level == 0 {
        // Storage atoms only. Cache by cube geometry per-txn.
        // Level 0: count storage atoms in the cube (direct, no extra memoization here)
        let t0 = Instant::now();
        let out = storage_atom_count_in_cube_tx(db, txn, cube)
            .await?
            .unwrap_or(0);
        // println!(
        //     "aq.child_count: level=0 atoms={} took={}ms",
        //     out,
        //     t0.elapsed().as_millis()
        //);
        return Ok(out);
    }

    // Fast path: count local complete availabilities fully contained in cube using axis-min index.
    // Then apply overlay semantics: deletes mask; inserts add and dedupe by (level, mins, maxs).
    let mut candidates = availabilities_contained_in_cube_tx(db, txn, cube, None).await?;
    // Only count children strictly below the candidate level
    candidates.retain(|a| a.peer_id == *peer && a.complete && (a.level as u16) < level);

    // Deduplicate by geometry (level + mins/maxs) to avoid double-counting across roots.
    let mut geom: std::collections::HashSet<(u16, Vec<Vec<u8>>, Vec<Vec<u8>>)> =
        std::collections::HashSet::new();
    for a in candidates.iter() {
        geom.insert((a.level, a.range.mins().to_vec(), a.range.maxs().to_vec()));
    }

    // Overlay: deletes mask existing; inserts are counted if contained and not already present.
    use crate::plugins::harmonizer::optimizer::Action::{Delete, Insert};
    let mut deleted: std::collections::HashSet<UuidBytes> = std::collections::HashSet::new();
    for act in overlay.iter() {
        if let Delete(id) = act {
            deleted.insert(*id);
        }
    }
    // Filter out any candidate whose id is deleted by overlay.
    if !deleted.is_empty() {
        candidates.retain(|a| !deleted.contains(&a.key));
    }

    for act in overlay.iter() {
        if let Insert(d) = act {
            if d.complete && (d.level as u16) < level && cube.contains(&d.range) {
                let key = (d.level, d.range.mins().to_vec(), d.range.maxs().to_vec());
                if !geom.contains(&key) {
                    geom.insert(key);
                }
            }
        }
    }

    Ok(geom.len())
}

/// Return true if the given peer has a local complete availability that fully contains `target`.
/// Snapshot-aware and root-scoped via `roots_for_peer` + `range_cover`.
pub async fn peer_contains_range_local(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    peer: &UuidBytes,
    target: &RangeCube,
    overlay: &ActionSet,
    cache: &mut Option<&mut AvailabilityQueryCache>,
) -> Result<bool, StorageError> {
    // First honor overlay inserts: if any complete draft contains target, return true.
    for act in overlay {
        if let Insert(d) = act {
            if d.complete && d.range.contains(target) {
                return Ok(true);
            }
        }
    }

    let roots = roots_for_peer_cached(db, txn, peer, cache).await?;
    let root_ids: Vec<UuidBytes> = roots.into_iter().map(|r| r.key).collect();
    let (frontier, no_overlap) =
        range_cover(db, txn, target, &root_ids, None, overlay, cache).await?;
    // Do not early-return on no_overlap; overlay may cover target even if roots don't.
    for a in frontier {
        if a.peer_id == *peer && a.complete && a.range.contains(target) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn roots_for_peer_cached(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    peer: &UuidBytes,
    cache: &mut Option<&mut AvailabilityQueryCache>,
) -> Result<Vec<Availability>, StorageError> {
    if let Some(ref mut c) = cache.as_deref_mut() {
        if c.compatible_txn(txn) {
            if let Some(ids_ref) = c.roots_by_peer.get(peer) {
                let ids = ids_ref.clone();
                drop(c); // release borrow before reusing cache
                let mut out = Vec::with_capacity(ids.len());
                for id in ids {
                    // try cache first again
                    if let Some(c2) = cache.as_deref_mut() {
                        if let Some(av) = c2.avail_by_id.get(&id) {
                            out.push(av.clone());
                            continue;
                        }
                    }
                    if let Some(av) = resolve_child_avail_cached(db, txn, &id, cache).await? {
                        out.push(av);
                    }
                }
                return Ok(out);
            }
        }
    }
    let roots = roots_for_peer(db, txn, peer).await?;
    if let Some(ref mut c) = cache.as_deref_mut() {
        if c.compatible_txn(txn) {
            c.roots_by_peer
                .insert(*peer, roots.iter().map(|r| r.key).collect());
            for r in &roots {
                c.avail_by_id.insert(r.key, r.clone());
            }
        }
    }
    Ok(roots)
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
    let t0 = Instant::now();
    let all: Vec<Availability> = db
        .range_by_pk_tx::<Availability>(txn, &[], &[0xFF], None)
        .await?;
    let all_len = all.len();

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

    let out = peers.len();
    // println!(
    //     "aq.count_replications: scanned={} peers={} took={}ms",
    //     all_len,
    //     out,
    //     t0.elapsed().as_millis()
    // );
    Ok(out)
}

/// Transaction-aware child count for a single availability (just the embedded list length).
pub async fn child_count_by_availability(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    availability_id: &UuidBytes,
    overlay: &ActionSet,
    cache: &mut Option<&mut AvailabilityQueryCache>,
) -> Result<usize, StorageError> {
    let cs = open_childset_cached(db, txn, *availability_id, cache).await?;
    if overlay.is_empty() {
        return Ok(cs.count());
    }
    let mut deleted = std::collections::HashSet::new();
    for a in overlay {
        if let Delete(id) = a {
            deleted.insert(*id);
        }
    }
    if deleted.is_empty() {
        return Ok(cs.count());
    }
    let c = cs
        .children
        .iter()
        .filter(|id| !deleted.contains(id))
        .count();
    Ok(c)
}

/// Transaction-aware read of raw child IDs embedded in an availability.
pub async fn children_by_availability(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    availability_id: &UuidBytes,
    overlay: &ActionSet,
    cache: &mut Option<&mut AvailabilityQueryCache>,
) -> Result<Vec<UuidBytes>, StorageError> {
    let cs = open_childset_cached(db, txn, *availability_id, cache).await?;
    if overlay.is_empty() {
        return Ok(cs.children.clone());
    }
    let mut deleted = std::collections::HashSet::new();
    for a in overlay {
        if let Delete(id) = a {
            deleted.insert(*id);
        }
    }
    if deleted.is_empty() {
        return Ok(cs.children.clone());
    }
    let v: Vec<_> = cs
        .children
        .into_iter()
        .filter(|id| !deleted.contains(id))
        .collect();
    Ok(v)
}

async fn open_childset_cached(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    parent: UuidBytes,
    cache: &mut Option<&mut AvailabilityQueryCache>,
) -> Result<ChildSet, StorageError> {
    if let Some(ref mut c) = cache.as_deref_mut() {
        if c.compatible_txn(txn) {
            if let Some(cs) = c.childset_by_parent.get(&parent) {
                return Ok(cs.clone());
            }
        }
    }
    let cs = match txn {
        Some(t) => ChildSet::open_tx(db, t, parent).await?,
        None => ChildSet::open(db, parent).await?,
    };
    if let Some(ref mut c) = cache.as_deref_mut() {
        if c.compatible_txn(txn) {
            c.childset_by_parent.insert(parent, cs.clone());
        }
    }
    Ok(cs)
}

/// Level-agnostic **local** child counter under a peer’s roots:
/// 1) If cube dims resolve to known storage tables, count **storage atoms** (fast, no decode).
/// 2) Otherwise, fall back to availability-frontier counting via `range_cover`.
pub async fn local_child_count_under_peer(
    db: &KuramotoDb,
    txn: Option<&ReadTransaction>,
    peer: &UuidBytes,
    cube: &RangeCube,
    overlay: &ActionSet,
    cache: &mut Option<&mut AvailabilityQueryCache>,
) -> Result<usize, StorageError> {
    if let Some(n) = storage_atom_count_in_cube_tx(db, txn, cube).await? {
        return Ok(n);
    }

    // ---- fallback: walk local availability trees (frontier only) ----
    let roots: Vec<Availability> = roots_for_peer_cached(db, txn, peer, cache).await?;
    let root_ids: Vec<UuidBytes> = roots.iter().map(|r| r.key).collect();

    let (frontier, _no_overlap) =
        range_cover(db, txn, cube, &root_ids, None, overlay, cache).await?;

    let mut seen = HashSet::<UuidBytes>::new();
    let mut count = 0usize;
    for a in frontier.iter() {
        if a.peer_id == *peer && a.complete && cube.contains(&a.range) && seen.insert(a.key) {
            count += 1;
        }
    }
    // Count overlay-only inserts as present for availability queries.
    // Dedup by (level, geometry) against already-counted nodes.
    let mut geom_seen: HashSet<(u16, Vec<Vec<u8>>, Vec<Vec<u8>>)> = HashSet::new();
    for a in frontier.iter() {
        if a.peer_id == *peer && a.complete && cube.contains(&a.range) {
            geom_seen.insert((a.level, a.range.mins().to_vec(), a.range.maxs().to_vec()));
        }
    }
    for act in overlay {
        if let Insert(d) = act {
            if d.complete && cube.contains(&d.range) {
                let key = (d.level, d.range.mins().to_vec(), d.range.maxs().to_vec());
                if !geom_seen.contains(&key) {
                    geom_seen.insert(key);
                    count += 1;
                }
            }
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

    #[tokio::test]
    async fn contained_lookup_by_narrow_axis() {
        let db = fresh_db().await;
        let d1 = TableHash { hash: 10 };
        let d2 = TableHash { hash: 20 };

        // Peer
        let p = UuidBytes::new();

        // A: inside small box on both axes
        let a = Availability {
            key: UuidBytes::new(),
            peer_id: p,
            range: RangeCube::new(
                smallvec![d1, d2],
                smallvec![b"b".to_vec(), b"m".to_vec()],
                smallvec![b"c".to_vec(), b"n".to_vec()],
            )
            .unwrap(),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(a.clone()).await.unwrap();

        // B: overlaps but NOT contained on d2 (max extends beyond query)
        let b = Availability {
            key: UuidBytes::new(),
            peer_id: p,
            range: RangeCube::new(
                smallvec![d1, d2],
                smallvec![b"b".to_vec(), b"k".to_vec()],
                smallvec![b"c".to_vec(), b"z\xFF".to_vec()],
            )
            .unwrap(),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(b.clone()).await.unwrap();

        // Query cube: narrow on d1, wide on d2 → should choose d1
        let q = RangeCube::new(
            smallvec![d1, d2],
            smallvec![b"b".to_vec(), b"a".to_vec()],
            smallvec![b"c".to_vec(), b"z".to_vec()],
        )
        .unwrap();

        let got = availabilities_contained_in_cube_tx(&db, None, &q, None)
            .await
            .unwrap();
        let ids: std::collections::HashSet<_> = got.into_iter().map(|x| x.key).collect();
        assert!(ids.contains(&a.key));
        assert!(!ids.contains(&b.key));
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
        let (nodes, no_overlap) =
            range_cover(&db, None, &target, &[root.key], None, &vec![], &mut None)
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
        let (nodes, no_overlap) =
            range_cover(&db, None, &target, &[parent.key], None, &vec![], &mut None)
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

        let (nodes, _) = range_cover(&db, None, &target, &[root.key], Some(1), &vec![], &mut None)
            .await
            .unwrap();
        assert!(
            nodes.is_empty(),
            "budget exhausted before reaching a frontier"
        );

        let (nodes2, _) = range_cover(&db, None, &target, &[root.key], None, &vec![], &mut None)
            .await
            .unwrap();
        assert_eq!(nodes2.len(), 1);
        assert_eq!(nodes2[0].key, c2.key);
    }

    #[tokio::test]
    async fn range_cover_masks_deletes_from_overlay() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 42 };

        // Build root -> leaf
        let leaf = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"m", b"n"),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let root = Availability {
            key: UuidBytes::new(),
            peer_id: leaf.peer_id,
            range: cube(dim, b"a", b"z"),
            level: 2,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(root.clone()).await.unwrap();
        db.put(leaf.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, root.key).await.unwrap();
        cs.add_child(&db, leaf.key).await.unwrap();

        let target = cube(dim, b"m", b"n");

        // Sanity: without overlay, leaf is returned
        let (nodes, no_overlap) =
            range_cover(&db, None, &target, &[root.key], None, &vec![], &mut None)
                .await
                .unwrap();
        assert!(!no_overlap);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].key, leaf.key);

        // With overlay delete, node should be masked out
        use crate::plugins::harmonizer::optimizer::Action;
        let overlay = vec![Action::Delete(leaf.key)];
        let (nodes2, no_overlap2) =
            range_cover(&db, None, &target, &[root.key], None, &overlay, &mut None)
                .await
                .unwrap();
        assert!(!no_overlap2);
        assert!(nodes2.is_empty(), "deleted leaf should be masked");
    }

    #[tokio::test]
    // Overlay inserts should not modify range_cover (DB-only),
    // but must be visible via helpers (containment/child count).
    async fn overlay_inserts_affect_helpers_not_range_cover() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 43 };

        let target = cube(dim, b"m", b"n");
        use crate::plugins::harmonizer::optimizer::{Action, AvailabilityDraft};
        let draft = AvailabilityDraft {
            level: 0,
            range: cube(dim, b"l", b"o"),
            complete: true,
        };
        let overlay = vec![Action::Insert(draft)];

        // Range-cover with no roots remains empty
        let (nodes, no_overlap) = range_cover(&db, None, &target, &[], None, &overlay, &mut None)
            .await
            .unwrap();
        assert!(no_overlap);
        assert!(nodes.is_empty());

        // Containment helper should see overlay
        let peer = UuidBytes::new();
        let ok = super::peer_contains_range_local(&db, None, &peer, &target, &overlay, &mut None)
            .await
            .unwrap();
        assert!(ok);

        // Level-0 child_count uses storage atoms only → 0 here (no data rows)
        let cnt0 = super::child_count(&db, None, &peer, &target, 0, &overlay, &mut None)
            .await
            .unwrap();
        assert_eq!(cnt0, 0);
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

        let cnt_snap = child_count_by_availability(&db, Some(&txn), &id, &vec![], &mut None)
            .await
            .unwrap();
        assert_eq!(cnt_snap, 0);
        let cnt_live = child_count_by_availability(&db, None, &id, &vec![], &mut None)
            .await
            .unwrap();
        assert_eq!(cnt_live, 1);

        let kids_live = children_by_availability(&db, None, &id, &vec![], &mut None)
            .await
            .unwrap();
        assert_eq!(kids_live.len(), 1);
    }

    #[tokio::test]
    async fn child_set_overlay_masks_deletes() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 5 };

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
        let c1 = UuidBytes::new();
        let c2 = UuidBytes::new();
        db.put(parent.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, parent.key).await.unwrap();
        cs.add_child(&db, c1).await.unwrap();
        cs.add_child(&db, c2).await.unwrap();

        // Overlay deletes c2
        use crate::plugins::harmonizer::optimizer::Action;
        let overlay = vec![Action::Delete(c2)];

        let cnt = child_count_by_availability(&db, None, &parent.key, &overlay, &mut None)
            .await
            .unwrap();
        assert_eq!(cnt, 1);

        let kids = children_by_availability(&db, None, &parent.key, &overlay, &mut None)
            .await
            .unwrap();
        assert_eq!(kids.len(), 1);
        assert_eq!(kids[0], c1);
    }

    #[tokio::test]
    async fn cache_snapshot_semantics_with_childset() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 77 };

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

        // Start a snapshot and a cache
        let txn = db.begin_read_txn().unwrap();
        let mut cache_owner = Some(super::AvailabilityQueryCache::new(&txn));

        // Initially no children
        let mut cache_ref = cache_owner.as_mut().map(|c| c as _);
        let n0 = child_count_by_availability(&db, Some(&txn), &parent.key, &vec![], &mut cache_ref)
            .await
            .unwrap();
        assert_eq!(n0, 0);

        // Add a child AFTER the snapshot was taken
        let child = UuidBytes::new();
        let mut cs = ChildSet::open(&db, parent.key).await.unwrap();
        cs.add_child(&db, child).await.unwrap();

        // With the same snapshot + cache, count is still 0
        let mut cache_ref2 = cache_owner.as_mut().map(|c| c as _);
        let n1 =
            child_count_by_availability(&db, Some(&txn), &parent.key, &vec![], &mut cache_ref2)
                .await
                .unwrap();
        assert_eq!(n1, 0);
    }

    #[tokio::test]
    async fn overlay_precedence_in_helpers_over_db_and_cache() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 78 };

        // DB has a leaf, which we will delete via overlay, and overlay adds another leaf
        let leaf_db = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"m", b"n"),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let root = Availability {
            key: UuidBytes::new(),
            peer_id: leaf_db.peer_id,
            range: cube(dim, b"a", b"z"),
            level: 2,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(root.clone()).await.unwrap();
        db.put(leaf_db.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, root.key).await.unwrap();
        cs.add_child(&db, leaf_db.key).await.unwrap();

        // Snapshot + cache
        let txn = db.begin_read_txn().unwrap();
        let mut cache_owner = Some(super::AvailabilityQueryCache::new(&txn));

        // Build overlay: delete DB leaf and insert a new overlay leaf
        use crate::plugins::harmonizer::optimizer::{Action, AvailabilityDraft};
        let overlay_leaf = AvailabilityDraft {
            level: 0,
            range: cube(dim, b"l", b"o"),
            complete: true,
        };
        let overlay = vec![Action::Delete(leaf_db.key), Action::Insert(overlay_leaf)];

        let target = cube(dim, b"m", b"n");
        let mut cache_ref = cache_owner.as_mut().map(|c| c as _);
        let (_nodes, _no_overlap) = range_cover(
            &db,
            Some(&txn),
            &target,
            &[root.key],
            None,
            &overlay,
            &mut cache_ref,
        )
        .await
        .unwrap();

        // The DB leaf is deleted, but overlay insert intersects the target → helpers see it
        let peer = leaf_db.peer_id;
        let ok =
            super::peer_contains_range_local(&db, Some(&txn), &peer, &target, &overlay, &mut None)
                .await
                .unwrap();
        assert!(ok);
        // Level-0 child_count still counts storage atoms only → 0 here (overlay only)
        let cnt0 = super::child_count(&db, Some(&txn), &peer, &target, 0, &overlay, &mut None)
            .await
            .unwrap();
        assert_eq!(cnt0, 0);
    }

    #[tokio::test]
    async fn db_fallback_no_overlay_uses_cache() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 79 };

        // Build simple root->leaf
        let leaf = Availability {
            key: UuidBytes::new(),
            peer_id: UuidBytes::new(),
            range: cube(dim, b"c", b"d"),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let root = Availability {
            key: UuidBytes::new(),
            peer_id: leaf.peer_id,
            range: cube(dim, b"a", b"z"),
            level: 2,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        db.put(root.clone()).await.unwrap();
        db.put(leaf.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, root.key).await.unwrap();
        cs.add_child(&db, leaf.key).await.unwrap();

        // Snapshot + cache, then query twice: second hit should be served from cache (behaviorally identical)
        let txn = db.begin_read_txn().unwrap();
        let mut cache_owner = Some(super::AvailabilityQueryCache::new(&txn));
        let target = cube(dim, b"c", b"d");

        let mut cache_ref = cache_owner.as_mut().map(|c| c as _);
        let (first, no1) = range_cover(
            &db,
            Some(&txn),
            &target,
            &[root.key],
            None,
            &vec![],
            &mut cache_ref,
        )
        .await
        .unwrap();
        assert!(!no1 && first.len() == 1);

        let mut cache_ref2 = cache_owner.as_mut().map(|c| c as _);
        let (second, no2) = range_cover(
            &db,
            Some(&txn),
            &target,
            &[root.key],
            None,
            &vec![],
            &mut cache_ref2,
        )
        .await
        .unwrap();
        assert!(!no2 && second.len() == 1);
        assert_eq!(first[0].key, second[0].key);
    }

    #[tokio::test]
    async fn peer_contains_range_local_sees_overlay_inserts() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 6 };
        let peer = UuidBytes::new();
        let target = cube(dim, b"b", b"c");

        // No DB rows, but an overlay insert that fully contains target
        use crate::plugins::harmonizer::optimizer::{Action, AvailabilityDraft};
        let draft = AvailabilityDraft {
            level: 0,
            range: cube(dim, b"a", b"z"),
            complete: true,
        };
        let overlay = vec![Action::Insert(draft)];

        let ok = super::peer_contains_range_local(&db, None, &peer, &target, &overlay, &mut None)
            .await
            .unwrap();
        assert!(ok, "overlay insert should satisfy containment");
    }

    #[tokio::test]
    async fn local_child_count_under_peer_counts_overlay_inserts() {
        let db = fresh_db().await;
        let dim = TableHash { hash: 7 };
        let peer = UuidBytes::new();
        let rng = cube(dim, b"a", b"m");

        use crate::plugins::harmonizer::optimizer::{Action, AvailabilityDraft};
        // Two overlay inserts inside cube; count should be 2
        let overlay = vec![
            Action::Insert(AvailabilityDraft {
                level: 0,
                range: cube(dim, b"b", b"c"),
                complete: true,
            }),
            Action::Insert(AvailabilityDraft {
                level: 1,
                range: cube(dim, b"d", b"e"),
                complete: true,
            }),
        ];

        let n = super::local_child_count_under_peer(&db, None, &peer, &rng, &overlay, &mut None)
            .await
            .unwrap();
        assert_eq!(n, 2);
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
    async fn child_count_level0_counts_storage_atoms() {
        let db = db_with_tent().await;
        let pk_dim = TableHash::from(TEnt::table_def());
        // PK range: [2,5) → ids 2,3,4 ⇒ 3 children at level 0
        let r = cube(pk_dim, &2u64.to_be_bytes(), &5u64.to_be_bytes());
        let peer = UuidBytes::new();
        let n = super::child_count(&db, None, &peer, &r, 0, &vec![], &mut None)
            .await
            .unwrap();
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn child_count_level1_counts_availabilities() {
        use crate::plugins::harmonizer::availability::Availability;
        use crate::plugins::harmonizer::child_set::ChildSet;

        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("lvlaware.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<Availability>().unwrap();
        db.create_table_and_indexes::<crate::plugins::harmonizer::child_set::Child>()
            .unwrap();
        db.create_table_and_indexes::<crate::plugins::harmonizer::child_set::DigestChunk>()
            .unwrap();

        let dim = TableHash { hash: 42 };
        let peer = UuidBytes::new();

        // Create one level-0 leaf and a level-1 parent covering it; link parent->leaf
        let leaf = Availability {
            key: UuidBytes::new(),
            peer_id: peer,
            range: cube(dim, b"b", b"c"),
            level: 0,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let parent = Availability {
            key: UuidBytes::new(),
            peer_id: peer,
            range: cube(dim, b"a", b"z"),
            level: 1,
            schema_hash: 0,
            version: 0,
            updated_at: 0,
            complete: true,
        };
        let pid = parent.key;
        db.put(leaf.clone()).await.unwrap();
        db.put(parent.clone()).await.unwrap();
        let mut cs = ChildSet::open(&db, pid).await.unwrap();
        cs.add_child(&db, leaf.key).await.unwrap();

        // Count children under the parent's cube for level>0 must use availability frontier
        let r = cube(dim, b"a", b"z");
        let n = super::child_count(&db, None, &peer, &r, 1, &vec![], &mut None)
            .await
            .unwrap();
        assert_eq!(n, 1);
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
        let r = RangeCube::new(
            smallvec![pk_dim, tag_dim],
            smallvec![2u64.to_be_bytes().to_vec(), vec![2u8]],
            smallvec![5u64.to_be_bytes().to_vec(), vec![3u8]],
        )
        .unwrap();

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
        let r = RangeCube::new(
            smallvec![uniq_dim, tag_dim],
            smallvec![vec![12u8], vec![2u8]],
            smallvec![vec![14u8], vec![3u8]],
        )
        .unwrap();

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
