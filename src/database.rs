use redb::ReadTransaction;
use redb::ReadableTable;
use redb::TableHandle;
use redb::{Database, TableDefinition};

use tokio::sync::{mpsc, oneshot};

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};

use crate::plugins::Plugin;
use crate::storage_entity::IndexCardinality;

use super::{
    clock::Clock, meta::BlobMeta, region_lock::RegionLock, storage_entity::StorageEntity,
    storage_error::StorageError,
};

pub type StaticTableDef = &'static TableDefinition<'static, &'static [u8], Vec<u8>>;

pub trait PluginMarker: Send + Sync {}

pub struct IndexPutRequest {
    pub table: StaticTableDef,
    pub key: Vec<u8>,   // index key (e.g., email as bytes)
    pub value: Vec<u8>, // usually the main key (e.g., user_id)
    pub unique: bool,
}
pub struct IndexRemoveRequest {
    table: StaticTableDef,
    key: Vec<u8>,
}

pub type WriteBatch = Vec<WriteRequest>;

type WriteMsg = (
    WriteBatch,
    tokio::sync::oneshot::Sender<Result<(), StorageError>>,
);

pub enum WriteRequest {
    Put {
        data_table: StaticTableDef,
        meta_table: StaticTableDef,
        key: Vec<u8>,
        value: Vec<u8>,
        meta: Vec<u8>,
        index_removes: Vec<IndexRemoveRequest>,
        index_puts: Vec<IndexPutRequest>,
    },
    Delete {
        data_table: StaticTableDef,
        meta_table: StaticTableDef,
        key: Vec<u8>,
        meta: Vec<u8>,
        index_removes: Vec<IndexRemoveRequest>,
    },
}

type TablePutFn = dyn for<'a> Fn(
        &'a KuramotoDb,
        &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send + 'a>>
    + Send
    + Sync;

fn encode_nonunique_key(idx_key: &[u8], pk: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(idx_key.len() + 1 + pk.len());
    out.extend_from_slice(idx_key);
    out.push(0); // separator
    out.extend_from_slice(pk); // disambiguator
    out
}

fn nonunique_bounds(idx_key: &[u8]) -> (Vec<u8>, Vec<u8>) {
    // [idx_key • 0x00, idx_key • 0x01) – half-open range
    let mut lo = Vec::with_capacity(idx_key.len() + 1);
    lo.extend_from_slice(idx_key);
    lo.push(0);
    let mut hi = Vec::with_capacity(idx_key.len() + 1);
    hi.extend_from_slice(idx_key);
    hi.push(1);
    (lo, hi)
}

pub struct KuramotoDb {
    db: Arc<Database>,
    write_tx: mpsc::Sender<WriteMsg>,
    clock: Arc<dyn Clock>,
    plugins: Vec<Arc<dyn Plugin>>,

    entity_putters: RwLock<HashMap<String, Arc<TablePutFn>>>,
}

impl KuramotoDb {
    pub async fn new(
        path: &str,
        clock: Arc<dyn Clock>,
        plugins: Vec<Arc<dyn Plugin>>,
    ) -> Arc<Self> {
        let db = Database::create(path).unwrap();
        let (write_tx, mut write_rx) = mpsc::channel(100);
        let db_arc = Arc::new(db);

        let sys = Arc::new(KuramotoDb {
            db: db_arc.clone(),
            write_tx,
            clock,
            plugins,
            entity_putters: RwLock::new(HashMap::new()),
        });

        for p in &sys.plugins {
            p.attach_db(sys.clone());
        }

        let sys2 = sys.clone();
        tokio::spawn(async move {
            while let Some((batch, reply)) = write_rx.recv().await {
                let _ = sys2.handle_write(batch, reply).await;
            }
        });
        sys
    }

    /// Manually create a table and its indexes. Returns error if already exists.
    pub fn create_table_and_indexes<E: StorageEntity>(&self) -> Result<(), StorageError> {
        {
            // For decoding and putting bytes we gotta map to a type
            let mut m = self.entity_putters.write().unwrap();
            m.insert(
                E::table_def().name().to_string(),
                Arc::new(|db, bytes| {
                    Box::pin(async move {
                        // decode via E and call the normal typed put()
                        let e = E::load_and_migrate(bytes)?;
                        db.put::<E>(e).await
                    })
                }),
            );
        }
        {
            // Create the table
            let txn = self
                .db
                .begin_write()
                .map_err(|e| StorageError::Other(e.to_string()))?;
            // Create main table
            txn.open_table(E::table_def().clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;
            // Create meta table
            txn.open_table(E::meta_table_def().clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;
            // Create index tables
            for idx in E::indexes() {
                txn.open_table(idx.table_def.clone())
                    .map_err(|e| StorageError::Other(e.to_string()))?;
            }
            txn.commit()
                .map_err(|e| StorageError::Other(e.to_string()))?;
        }
        Ok(())
    }

    // Getters/setters
    pub fn get_clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }

    #[inline]
    fn with_read<T, F>(&self, txn: Option<&redb::ReadTransaction>, f: F) -> Result<T, StorageError>
    where
        F: FnOnce(&redb::ReadTransaction) -> Result<T, StorageError>,
    {
        if let Some(rt) = txn {
            f(rt)
        } else {
            let rt = self
                .db
                .begin_read()
                .map_err(|e| StorageError::Other(e.to_string()))?;
            f(&rt)
        }
    }

    /// Read and decode an entity by primary key, using the provided read
    /// snapshot (`txn`) if present; otherwise a fresh read transaction is used.
    pub async fn get_data_tx<E: StorageEntity>(
        &self,
        txn: Option<&redb::ReadTransaction>,
        key: &[u8],
    ) -> Result<E, StorageError> {
        // If no snapshot provided, create one and reuse it locally.
        let rtxn_guard;
        let rt = if let Some(rt) = txn {
            rt
        } else {
            rtxn_guard = self
                .db
                .begin_read()
                .map_err(|e| StorageError::Other(e.to_string()))?;
            &rtxn_guard
        };

        let table = rt
            .open_table(E::table_def().clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let val = table
            .get(key)
            .map_err(|e| StorageError::Other(e.to_string()))?;
        if let Some(v) = val {
            E::load_and_migrate(&v.value())
        } else {
            Err(StorageError::NotFound)
        }
    }

    /// Convenience wrapper around `get_data_tx(None, key)`.
    pub async fn get_data<E: StorageEntity>(&self, key: &[u8]) -> Result<E, StorageError> {
        self.get_data_tx::<E>(None, key).await
    }

    // Single-key index read (UNIQUE) through a snapshot (or fresh read txn).
    pub async fn get_index_tx(
        &self,
        txn: Option<&redb::ReadTransaction>,
        table: StaticTableDef,
        idx_key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.with_read(txn, |rt| {
            let t = rt
                .open_table(table.clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;
            let got = t
                .get(idx_key)
                .map_err(|e| StorageError::Other(e.to_string()))?;
            Ok(got.map(|v| v.value().to_vec()))
        })
    }

    /// Get the primary key for an entity by an index.
    /// Returns Ok(Some(main_key)) if found, Ok(None) if not found, or Err on error.
    pub async fn get_index(
        &self,
        table: StaticTableDef,
        idx_key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.get_index_tx(None, table, idx_key).await
    }

    // Non-unique index: read all PKs for a given index key via snapshot.
    pub async fn get_index_all_tx(
        &self,
        txn: Option<&redb::ReadTransaction>,
        table: StaticTableDef,
        idx_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        self.with_read(txn, |rt| {
            let t = rt
                .open_table(table.clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;
            let (lo, hi) = nonunique_bounds(idx_key);
            let mut out = Vec::new();
            for item in t
                .range(lo.as_slice()..hi.as_slice())
                .map_err(|e| StorageError::Other(e.to_string()))?
            {
                let (_k, v) = item.map_err(|e| StorageError::Other(e.to_string()))?;
                out.push(v.value().to_vec());
            }
            Ok(out)
        })
    }

    pub async fn get_index_all(
        &self,
        table: StaticTableDef,
        idx_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        self.get_index_all_tx(None, table, idx_key).await
    }

    /// Return up to `limit` entities whose index key is in `[start_idx_key, end_idx_key)`,
    /// reading through `txn` if supplied; otherwise a fresh read transaction is used.
    /// Works for both unique and non-unique indexes (the index table stores PKs as values).
    pub async fn range_by_index_tx<E: StorageEntity>(
        &self,
        txn: Option<&redb::ReadTransaction>,
        index_table: StaticTableDef,
        start_idx_key: &[u8],
        end_idx_key: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<E>, StorageError> {
        let rtxn_guard;
        let rt = if let Some(rt) = txn {
            rt
        } else {
            rtxn_guard = self
                .db
                .begin_read()
                .map_err(|e| StorageError::Other(e.to_string()))?;
            &rtxn_guard
        };

        // (1) collect PKs from the index
        let idx = rt
            .open_table(index_table.clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let mut pks: Vec<Vec<u8>> = Vec::new();
        for r in idx
            .range(start_idx_key..end_idx_key)
            .map_err(|e| StorageError::Other(e.to_string()))?
        {
            let (_k, v) = r.map_err(|e| StorageError::Other(e.to_string()))?;
            pks.push(v.value().to_vec());
            if limit.map_or(false, |n| pks.len() >= n) {
                break;
            }
        }

        // (2) load entities from the same snapshot
        let data_t = rt
            .open_table(E::table_def().clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let mut out = Vec::with_capacity(pks.len());
        for pk in pks {
            if let Some(v) = data_t
                .get(pk.as_slice())
                .map_err(|e| StorageError::Other(e.to_string()))?
            {
                out.push(E::load_and_migrate(&v.value())?);
            }
        }
        Ok(out)
    }

    /// Convenience wrapper for `range_by_index_tx(None, ...)`.
    pub async fn range_by_index<E: StorageEntity>(
        &self,
        index_table: StaticTableDef,
        start_idx_key: &[u8],
        end_idx_key: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<E>, StorageError> {
        self.range_by_index_tx::<E>(None, index_table, start_idx_key, end_idx_key, limit)
            .await
    }

    /// Like `range_by_index_tx`, but returns `(entity, BlobMeta)` pairs resolved
    /// from the same snapshot (`txn` or a fresh one if `None`).
    pub async fn range_with_meta_by_index_tx<E: StorageEntity>(
        &self,
        txn: Option<&redb::ReadTransaction>,
        index_table: StaticTableDef,
        start_idx_key: &[u8],
        end_idx_key: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(E, BlobMeta)>, StorageError> {
        let rtxn_guard;
        let rt = if let Some(rt) = txn {
            rt
        } else {
            rtxn_guard = self
                .db
                .begin_read()
                .map_err(|e| StorageError::Other(e.to_string()))?;
            &rtxn_guard
        };

        // (1) collect PKs from index
        let idx = rt
            .open_table(index_table.clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let mut pks: Vec<Vec<u8>> = Vec::new();
        for r in idx
            .range(start_idx_key..end_idx_key)
            .map_err(|e| StorageError::Other(e.to_string()))?
        {
            let (_k, v) = r.map_err(|e| StorageError::Other(e.to_string()))?;
            pks.push(v.value().to_vec());
            if limit.map_or(false, |n| pks.len() >= n) {
                break;
            }
        }

        // (2) resolve (entity, meta) within the same snapshot
        let data_t = rt
            .open_table(E::table_def().clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let meta_t = rt
            .open_table(E::meta_table_def().clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let mut out = Vec::with_capacity(pks.len());
        for pk in pks {
            if let Some(v) = data_t
                .get(pk.as_slice())
                .map_err(|e| StorageError::Other(e.to_string()))?
            {
                let e = E::load_and_migrate(&v.value())?;
                let meta_raw = meta_t
                    .get(pk.as_slice())
                    .map_err(|e| StorageError::Other(e.to_string()))?
                    .ok_or(StorageError::NotFound)?
                    .value();
                let (m, _) = bincode::decode_from_slice::<BlobMeta, _>(
                    &meta_raw,
                    bincode::config::standard(),
                )
                .map_err(|e| StorageError::Bincode(e.to_string()))?;
                out.push((e, m));
            }
        }
        Ok(out)
    }

    /// Convenience wrapper for `range_with_meta_by_index_tx(None, ...)`.
    pub async fn range_with_meta_by_index<E: StorageEntity>(
        &self,
        index_table: StaticTableDef,
        start_idx_key: &[u8],
        end_idx_key: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(E, BlobMeta)>, StorageError> {
        self.range_with_meta_by_index_tx::<E>(None, index_table, start_idx_key, end_idx_key, limit)
            .await
    }

    pub async fn get_by_index_tx<E: StorageEntity>(
        &self,
        txn: Option<&redb::ReadTransaction>,
        index_table: StaticTableDef,
        idx_key: &[u8],
    ) -> Result<Option<E>, StorageError> {
        self.with_read(txn, |rt| {
            let idx_t = rt
                .open_table(index_table.clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;
            if let Some(pkv) = idx_t
                .get(idx_key)
                .map_err(|e| StorageError::Other(e.to_string()))?
            {
                let data_t = rt
                    .open_table(E::table_def().clone())
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                if let Some(v) = data_t
                    .get(&*pkv.value())
                    .map_err(|e| StorageError::Other(e.to_string()))?
                {
                    return Ok(Some(E::load_and_migrate(&v.value())?));
                }
            }
            Ok(None)
        })
    }

    /// Get an entity by an index key.
    /// Returns Ok(Some(entity)) if found, Ok(None) if not found, or Err on error.
    pub async fn get_by_index<E: StorageEntity>(
        &self,
        t: StaticTableDef,
        k: &[u8],
    ) -> Result<Option<E>, StorageError> {
        self.get_by_index_tx::<E>(None, t, k).await
    }

    pub async fn get_by_index_all_tx<E: StorageEntity>(
        &self,
        txn: Option<&redb::ReadTransaction>,
        index_table: StaticTableDef,
        idx_key: &[u8],
    ) -> Result<Vec<E>, StorageError> {
        self.with_read(txn, |rt| {
            let (lo, hi) = nonunique_bounds(idx_key);
            let idx_t = rt
                .open_table(index_table.clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;

            let mut pks = Vec::new();
            for row in idx_t
                .range(lo.as_slice()..hi.as_slice())
                .map_err(|e| StorageError::Other(e.to_string()))?
            {
                let (_k, v) = row.map_err(|e| StorageError::Other(e.to_string()))?;
                pks.push(v.value().to_vec());
            }

            let data_t = rt
                .open_table(E::table_def().clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;
            let mut out = Vec::with_capacity(pks.len());
            for pk in pks {
                if let Some(v) = data_t
                    .get(pk.as_slice())
                    .map_err(|e| StorageError::Other(e.to_string()))?
                {
                    out.push(E::load_and_migrate(&v.value())?);
                }
            }
            Ok(out)
        })
    }

    pub async fn get_by_index_all<E: StorageEntity>(
        &self,
        t: StaticTableDef,
        k: &[u8],
    ) -> Result<Vec<E>, StorageError> {
        self.get_by_index_all_tx::<E>(None, t, k).await
    }

    pub async fn get_meta_tx<E: StorageEntity>(
        &self,
        txn: Option<&redb::ReadTransaction>,
        key: &[u8],
    ) -> Result<BlobMeta, StorageError> {
        self.with_read(txn, |rt| {
            let t = rt
                .open_table(E::meta_table_def().clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;
            let v = t
                .get(key)
                .map_err(|e| StorageError::Other(e.to_string()))?
                .ok_or(StorageError::NotFound)?;
            bincode::decode_from_slice::<BlobMeta, _>(&v.value(), bincode::config::standard())
                .map(|(m, _)| m)
                .map_err(|e| StorageError::Bincode(e.to_string()))
        })
    }

    pub async fn get_meta<E: StorageEntity>(&self, key: &[u8]) -> Result<BlobMeta, StorageError> {
        self.get_meta_tx::<E>(None, key).await
    }

    pub async fn get_with_meta_tx<E: StorageEntity>(
        &self,
        txn: Option<&redb::ReadTransaction>,
        key: &[u8],
    ) -> Result<(E, BlobMeta), StorageError> {
        self.with_read(txn, |rt| {
            let data_t = rt
                .open_table(E::table_def().clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;
            let meta_t = rt
                .open_table(E::meta_table_def().clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;

            let dv = data_t
                .get(key)
                .map_err(|e| StorageError::Other(e.to_string()))?
                .ok_or(StorageError::NotFound)?;
            let e = E::load_and_migrate(&dv.value())?;

            let mv = meta_t
                .get(key)
                .map_err(|e| StorageError::Other(e.to_string()))?
                .ok_or(StorageError::NotFound)?;
            let (m, _) =
                bincode::decode_from_slice::<BlobMeta, _>(&mv.value(), bincode::config::standard())
                    .map_err(|e| StorageError::Bincode(e.to_string()))?;
            Ok((e, m))
        })
    }

    pub async fn get_with_meta<E: StorageEntity>(
        &self,
        key: &[u8],
    ) -> Result<(E, BlobMeta), StorageError> {
        self.get_with_meta_tx::<E>(None, key).await
    }

    // ------------  RANGE API  ------------
    /// Txn-aware variant: when `txn` is Some, read using that **write** txn's snapshot.
    /// Otherwise falls back to starting a fresh read transaction.
    pub async fn range_by_pk_tx<E: StorageEntity>(
        &self,
        txn: Option<&ReadTransaction>,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<E>, StorageError> {
        self.with_read(txn, |rt| {
            let t = rt
                .open_table(E::table_def().clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;
            let mut rows = Vec::new();
            for r in t
                .range(start..end)
                .map_err(|e| StorageError::Other(e.to_string()))?
            {
                let (_k, v) = r.map_err(|e| StorageError::Other(e.to_string()))?;
                rows.push(E::load_and_migrate(&v.value())?);
                if limit.map_or(false, |n| rows.len() >= n) {
                    break;
                }
            }
            Ok(rows)
        })
    }

    /// Keep the old API; now it just delegates.
    pub async fn range_by_pk<E: StorageEntity>(
        &self,
        s: &[u8],
        e: &[u8],
        l: Option<usize>,
    ) -> Result<Vec<E>, StorageError> {
        self.range_by_pk_tx::<E>(None, s, e, l).await
    }

    pub async fn range_with_meta_by_pk_tx<E: StorageEntity>(
        &self,
        txn: Option<&redb::ReadTransaction>,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(E, BlobMeta)>, StorageError> {
        self.with_read(txn, |rt| {
            let data_t = rt
                .open_table(E::table_def().clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;
            let meta_t = rt
                .open_table(E::meta_table_def().clone())
                .map_err(|e| StorageError::Other(e.to_string()))?;

            let mut out = Vec::new();
            for row in data_t
                .range(start..end)
                .map_err(|e| StorageError::Other(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| StorageError::Other(e.to_string()))?;
                let e = E::load_and_migrate(v.value().as_slice())?;
                let meta_raw = meta_t
                    .get(k.value())
                    .map_err(|e| StorageError::Other(e.to_string()))?
                    .ok_or(StorageError::NotFound)?
                    .value();
                let (m, _) = bincode::decode_from_slice::<BlobMeta, _>(
                    &meta_raw,
                    bincode::config::standard(),
                )
                .map_err(|e| StorageError::Bincode(e.to_string()))?;
                out.push((e, m));
                if limit.map_or(false, |n| out.len() >= n) {
                    break;
                }
            }
            Ok(out)
        })
    }

    pub async fn range_with_meta_by_pk<E: StorageEntity>(
        &self,
        s: &[u8],
        e: &[u8],
        l: Option<usize>,
    ) -> Result<Vec<(E, BlobMeta)>, StorageError> {
        self.range_with_meta_by_pk_tx::<E>(None, s, e, l).await
    }

    pub async fn put_by_table_bytes(
        &self,
        table_name: &str,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        // Take a clone of the Arc<putter> while holding the read lock,
        // then drop the lock before we .await, so the future is Send.
        let putter = {
            let m = self.entity_putters.read().unwrap();
            m.get(table_name).cloned()
        };

        let Some(put) = putter else {
            return Err(StorageError::Other(format!(
                "no entity registered for table '{table_name}'"
            )));
        };

        // Now no lock/guard is live across this await.
        put(self, bytes).await
    }

    pub async fn put<E: StorageEntity>(&self, entity: E) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        let key = entity.primary_key();
        let now = self.clock.now();
        let old_entity = self.get_data::<E>(&key).await.ok(); // To detect key changes
        let old_meta = self.get_meta::<E>(&key).await.ok();

        let meta = if let Some(old) = old_meta {
            BlobMeta {
                version: old.version + 1,
                created_at: old.created_at,
                updated_at: now,
                deleted_at: None,
                region_lock: RegionLock::None,
            }
        } else {
            BlobMeta {
                version: 0,
                created_at: now,
                updated_at: now,
                deleted_at: None,
                region_lock: RegionLock::None,
            }
        };

        let (index_puts, index_removes) = if let Some(old) = old_entity {
            E::indexes()
                .iter()
                .fold((Vec::new(), Vec::new()), |(mut puts, mut removes), idx| {
                    let old_key = (idx.key_fn)(&old);
                    let new_key = (idx.key_fn)(&entity);

                    // stored keys depend on cardinality
                    let pk = entity.primary_key();
                    let old_stored = match idx.cardinality {
                        IndexCardinality::Unique => old_key.clone(),
                        IndexCardinality::NonUnique => encode_nonunique_key(&old_key, &pk),
                    };
                    let new_stored = match idx.cardinality {
                        IndexCardinality::Unique => new_key.clone(),
                        IndexCardinality::NonUnique => encode_nonunique_key(&new_key, &pk),
                    };

                    if old_key != new_key {
                        removes.push(IndexRemoveRequest {
                            table: idx.table_def,
                            key: old_stored,
                        });
                        puts.push(IndexPutRequest {
                            table: idx.table_def,
                            key: new_stored,
                            value: pk,
                            unique: matches!(idx.cardinality, IndexCardinality::Unique),
                        });
                    }
                    (puts, removes)
                })
        } else {
            let pk = entity.primary_key();
            (
                E::indexes()
                    .iter()
                    .map(|idx| {
                        let idx_key = (idx.key_fn)(&entity);
                        let stored = match idx.cardinality {
                            IndexCardinality::Unique => idx_key,
                            IndexCardinality::NonUnique => encode_nonunique_key(&idx_key, &pk),
                        };
                        IndexPutRequest {
                            table: idx.table_def,
                            key: stored,
                            value: pk.clone(),
                            unique: matches!(idx.cardinality, IndexCardinality::Unique),
                        }
                    })
                    .collect(),
                Vec::new(),
            )
        };
        let req = WriteRequest::Put {
            data_table: &E::table_def(),
            meta_table: &E::meta_table_def(),
            key,
            value: entity.to_bytes(),
            meta: bincode::encode_to_vec(meta, bincode::config::standard()).unwrap(),
            index_puts,
            index_removes,
        };

        self.write_tx
            .send((vec![req], tx))
            .await
            .map_err(|e| StorageError::Other(format!("Write queue dropped: {}", e)))?;
        rx.await
            .map_err(|e| StorageError::Other(format!("Write task dropped: {}", e)))?
    }

    pub async fn delete<E: StorageEntity>(&self, key: &[u8]) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        let now = self.clock.now();

        // Fetch the current entity so we can generate all index keys to remove
        let old_entity = self.get_data::<E>(key).await?;

        let meta = {
            let old_meta = self.get_meta::<E>(key).await?;
            BlobMeta {
                deleted_at: Some(now),
                updated_at: now,
                ..old_meta
            }
        };

        // Generate index_removes
        let pk = key.to_vec();
        let index_removes: Vec<IndexRemoveRequest> = E::indexes()
            .iter()
            .map(|idx| {
                let raw = (idx.key_fn)(&old_entity);
                let stored = match idx.cardinality {
                    IndexCardinality::Unique => raw,
                    IndexCardinality::NonUnique => encode_nonunique_key(&raw, &pk),
                };
                IndexRemoveRequest {
                    table: idx.table_def,
                    key: stored,
                }
            })
            .collect();

        let req = WriteRequest::Delete {
            data_table: &E::table_def(),
            meta_table: &E::meta_table_def(),
            key: key.to_vec(),
            meta: bincode::encode_to_vec(meta, bincode::config::standard()).unwrap(),
            index_removes,
        };
        self.write_tx
            .send((vec![req], tx))
            .await
            .map_err(|e| StorageError::Other(format!("Write queue dropped: {}", e)))?;

        rx.await
            .map_err(|e| StorageError::Other(format!("Write task dropped: {}", e)))?
    }

    // ----------- Raw public handlers --------------
    pub fn begin_read_txn(&self) -> Result<redb::ReadTransaction, StorageError> {
        self.db
            .begin_read()
            .map_err(|e| StorageError::Other(e.to_string()))
    }

    pub async fn raw_write(&self, req: WriteRequest) -> Result<(), StorageError> {
        // create a fresh responder
        let (tx, rx) = oneshot::channel();

        // enqueue
        self.write_tx
            .send((vec![req], tx))
            .await
            .map_err(|e| StorageError::Other(format!("Write queue dropped: {}", e)))?;

        // await the background writer’s result
        rx.await
            .map_err(|e| StorageError::Other(format!("Write task dropped: {}", e)))?
    }

    // ----------- Internal handler --------------
    async fn handle_write(
        &self,
        mut batch: WriteBatch,
        reply: tokio::sync::oneshot::Sender<Result<(), StorageError>>,
    ) -> Result<(), StorageError> {
        let wtxn = self
            .db
            .begin_write()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let rtxn = self
            .db
            .begin_read()
            .map_err(|e| StorageError::Other(e.to_string()))?;

        // Plugins can append/modify requests in `batch` via before_update hooks
        for plugin in self.plugins.iter() {
            plugin.before_update(self, &rtxn, &mut batch).await?;
        }

        // Apply all requests
        for req in batch.into_iter() {
            match req {
                WriteRequest::Put {
                    data_table,
                    meta_table,
                    key,
                    value,
                    meta,
                    index_puts,
                    index_removes,
                } => {
                    // ---- Version check ----
                    {
                        let meta_t = wtxn
                            .open_table(*meta_table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        if let Some(existing_meta_raw) = meta_t
                            .get(&*key)
                            .map_err(|e| StorageError::Other(e.to_string()))?
                        {
                            let (existing_meta, _) = bincode::decode_from_slice::<BlobMeta, _>(
                                &existing_meta_raw.value(),
                                bincode::config::standard(),
                            )
                            .map_err(|e| StorageError::Bincode(e.to_string()))?;
                            let new_meta: BlobMeta = bincode::decode_from_slice::<BlobMeta, _>(
                                &meta,
                                bincode::config::standard(),
                            )
                            .map_err(|e| StorageError::Bincode(e.to_string()))?
                            .0;

                            if new_meta.version <= existing_meta.version {
                                let _ = reply.send(Err(StorageError::PutButNoVersionIncrease));
                                return Err(StorageError::PutButNoVersionIncrease);
                            }
                        }
                    }

                    // ---- Insert/overwrite main row ----
                    {
                        let mut t = wtxn
                            .open_table(*data_table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        t.insert(&*key, value)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }

                    // ---- Index removes ----
                    for idx in index_removes {
                        let mut t = wtxn
                            .open_table(*idx.table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        t.remove(idx.key.as_slice())
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }

                    // ---- Index inserts (uniqueness) ----
                    for idx in index_puts {
                        let mut t = wtxn
                            .open_table(*idx.table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;

                        if idx.unique {
                            if let Some(existing) = t
                                .get(idx.key.as_slice())
                                .map_err(|e| StorageError::Other(e.to_string()))?
                            {
                                if existing.value() != idx.value.as_slice() {
                                    let e = StorageError::DuplicateIndexKey {
                                        index_name: idx.table.name(),
                                    };
                                    let _ = reply.send(Err(e.clone()));
                                    return Err(e);
                                }
                            }
                        }

                        t.insert(idx.key.as_slice(), idx.value)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }

                    // ---- Write meta ----
                    {
                        let mut meta_t = wtxn
                            .open_table(*meta_table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        meta_t
                            .insert(&*key, meta)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }
                }

                WriteRequest::Delete {
                    data_table,
                    meta_table,
                    key,
                    meta,
                    index_removes,
                } => {
                    // ---- Delete main row ----
                    {
                        let mut t = wtxn
                            .open_table(*data_table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        t.remove(&*key)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }

                    // ---- Meta update ----
                    {
                        let mut meta_t = wtxn
                            .open_table(*meta_table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        meta_t
                            .insert(&*key, meta)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }

                    // ---- Index removes ----
                    for idx in index_removes {
                        let mut t = wtxn
                            .open_table(*idx.table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        t.remove(idx.key.as_slice())
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }
                }
            }
        }

        // Commit once
        wtxn.commit()
            .map_err(|e| StorageError::Other(e.to_string()))?;

        // Reply once for the whole batch
        let _ = reply.send(Ok(()));
        Ok(())
    }
}

/*───────────────────────────────────────────────────────────────*/
/* tests                                                         */
/*───────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bincode::{Decode, Encode};
    use redb::ReadTransaction;
    use redb::TableDefinition;
    use redb::TableHandle;
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::clock::MockClock;
    use crate::{
        KuramotoDb, StaticTableDef, WriteBatch, WriteRequest,
        meta::BlobMeta,
        plugins::Plugin,
        region_lock::RegionLock,
        storage_entity::{IndexCardinality, IndexSpec, StorageEntity},
        storage_error::StorageError,
    };

    // ============== TestEntity ==============
    #[derive(Clone, Debug, PartialEq, Encode, Decode)]
    struct TestEntity {
        id: u64,
        name: String,
        value: i32,
    }

    static TEST_TABLE: TableDefinition<'static, &'static [u8], Vec<u8>> =
        TableDefinition::new("test");

    static TEST_META: TableDefinition<'static, &'static [u8], Vec<u8>> =
        TableDefinition::new("test_meta");

    static TEST_NAME_INDEX: TableDefinition<'static, &'static [u8], Vec<u8>> =
        TableDefinition::new("test_name_idx"); // UNIQUE

    static TEST_VALUE_INDEX: TableDefinition<'static, &'static [u8], Vec<u8>> =
        TableDefinition::new("test_value_idx"); // NON-UNIQUE

    static TEST_INDEXES: &[IndexSpec<TestEntity>] = &[
        IndexSpec {
            name: "name",
            key_fn: |e: &TestEntity| e.name.as_bytes().to_vec(),
            table_def: &TEST_NAME_INDEX,
            cardinality: IndexCardinality::Unique,
        },
        IndexSpec {
            name: "value",
            key_fn: |e: &TestEntity| e.value.to_be_bytes().to_vec(), // many can share same value
            table_def: &TEST_VALUE_INDEX,
            cardinality: IndexCardinality::NonUnique,
        },
    ];

    impl StorageEntity for TestEntity {
        const STRUCT_VERSION: u8 = 0;

        fn primary_key(&self) -> Vec<u8> {
            self.id.to_be_bytes().to_vec()
        }

        fn table_def() -> StaticTableDef {
            &TEST_TABLE
        }

        fn meta_table_def() -> StaticTableDef {
            &TEST_META
        }

        fn load_and_migrate(data: &[u8]) -> Result<Self, StorageError> {
            match data.first().copied() {
                Some(0) => bincode::decode_from_slice(&data[1..], bincode::config::standard())
                    .map(|(v, _)| v)
                    .map_err(|e| StorageError::Bincode(e.to_string())),
                _ => Err(StorageError::Bincode("bad version".into())),
            }
        }

        fn indexes() -> &'static [IndexSpec<Self>] {
            TEST_INDEXES
        }
    }

    // ---------- ASSERT HELPERS ----------
    async fn assert_row<E: StorageEntity + PartialEq + std::fmt::Debug>(
        sys: &KuramotoDb,
        key: &[u8],
        expect_data: Option<&E>,
        expect_version: u32,
        expect_created: u64,
        expect_updated: u64,
        expect_deleted: bool,
    ) {
        match expect_data {
            Some(d) => assert_eq!(sys.get_data::<E>(key).await.unwrap(), *d),
            None => assert!(sys.get_data::<E>(key).await.is_err()),
        };
        let meta = sys.get_meta::<E>(key).await.unwrap();
        assert_eq!(meta.created_at, expect_created);
        assert_eq!(meta.updated_at, expect_updated);
        assert_eq!(meta.version, expect_version);

        assert_eq!(meta.deleted_at.is_some(), expect_deleted);
        assert_eq!(meta.region_lock, RegionLock::None);
    }

    async fn assert_index_row(
        sys: &KuramotoDb,
        idx_table: StaticTableDef,
        idx_key: &[u8],
        expect_main_key: Option<&[u8]>,
    ) {
        let got = sys.get_index(idx_table, idx_key).await.unwrap();
        match expect_main_key {
            Some(main) => {
                let v = got.expect("index row missing");
                assert_eq!(v, main);
            }
            None => assert!(got.is_none(), "index row should be absent"),
        }
    }

    /// Assert that get_by_index returns the expected entity (or None).
    async fn assert_get_by_index<E: StorageEntity + PartialEq + std::fmt::Debug>(
        sys: &KuramotoDb,
        idx_table: StaticTableDef,
        idx_key: &[u8],
        expect: Option<&E>,
    ) {
        let got = sys.get_by_index::<E>(idx_table, idx_key).await.unwrap();
        match expect {
            Some(e) => assert_eq!(got.as_ref(), Some(e)),
            None => assert!(got.is_none(), "entity should be absent"),
        }
    }

    // ============== CRUD TESTS ==============

    #[tokio::test]
    async fn insert_and_read() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(1_000));
        let sys = KuramotoDb::new(
            dir.path().join("db.redb").to_str().unwrap(),
            clock.clone(),
            vec![], // no plugins
        )
        .await;

        // Should fail to put before table is created
        let e = TestEntity {
            id: 1,
            name: "Alice".into(),
            value: 42,
        };

        // Now create table and indexes
        sys.create_table_and_indexes::<TestEntity>().unwrap();

        // Now put should succeed
        sys.put(e.clone()).await.unwrap();

        assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 0, 1_000, 1_000, false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn overwrite_updates_meta() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(10));
        let sys = KuramotoDb::new(
            dir.path().join("db.redb").to_str().unwrap(),
            clock.clone(),
            vec![],
        )
        .await;

        sys.create_table_and_indexes::<TestEntity>().unwrap();

        let mut e = TestEntity {
            id: 99,
            name: "X".into(),
            value: 1,
        };
        sys.put(e.clone()).await.unwrap(); // initial
        assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 0, 10, 10, false).await;
        clock.advance(5).await;
        e.value = 2;
        sys.put(e.clone()).await.unwrap(); // overwrite

        assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 1, 10, 15, false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn delete_and_undelete() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(500));
        let sys = KuramotoDb::new(
            dir.path().join("db.redb").to_str().unwrap(),
            clock.clone(),
            vec![],
        )
        .await;

        sys.create_table_and_indexes::<TestEntity>().unwrap();

        let e = TestEntity {
            id: 7,
            name: "Y".into(),
            value: 0,
        };
        sys.put(e.clone()).await.unwrap(); // insert
        clock.advance(10).await;
        sys.delete::<TestEntity>(&e.id.to_be_bytes()).await.unwrap(); // delete
        assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), None, 0, 500, 510, true).await;

        clock.advance(5).await;
        sys.put(e.clone()).await.unwrap(); // undelete / re-insert
        assert_row::<TestEntity>(&sys, &e.id.to_be_bytes(), Some(&e), 1, 500, 515, false).await;
    }

    #[tokio::test]
    async fn stale_version_rejected() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(50));
        let sys = KuramotoDb::new(
            dir.path().join("stale.redb").to_str().unwrap(),
            clock.clone(),
            vec![],
        )
        .await;

        sys.create_table_and_indexes::<TestEntity>().unwrap();

        // --- insert normally ---
        let e = TestEntity {
            id: 5,
            name: "S".into(),
            value: 5,
        };
        sys.put(e.clone()).await.unwrap();

        // --- fetch existing meta so we can craft a stale write ---
        let meta = sys
            .get_meta::<TestEntity>(&e.id.to_be_bytes())
            .await
            .unwrap();

        // Build a stale WriteRequest by hand (version not bumped)
        let stale_wr = WriteRequest::Put {
            data_table: TestEntity::table_def(),
            meta_table: TestEntity::meta_table_def(),
            key: e.primary_key(),
            value: e.to_bytes(),
            meta: bincode::encode_to_vec(meta, bincode::config::standard()).unwrap(), // same version
            index_puts: vec![],
            index_removes: vec![],
        };

        // Push manually and expect StaleVersion
        let outcome = sys.raw_write(stale_wr).await;
        assert!(matches!(
            outcome,
            Err(StorageError::PutButNoVersionIncrease)
        ));
    }

    // ============== INDEX TESTS ==============
    #[tokio::test(start_paused = true)]
    async fn index_insert_update_delete() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let sys = KuramotoDb::new(
            dir.path().join("idx.redb").to_str().unwrap(),
            clock.clone(),
            vec![],
        )
        .await;

        sys.create_table_and_indexes::<TestEntity>().unwrap();

        // ---- insert ----
        let mut e = TestEntity {
            id: 1,
            name: "A".into(),
            value: 1,
        };
        sys.put(e.clone()).await.unwrap();
        assert_index_row(&sys, &TEST_NAME_INDEX, b"A", Some(&e.id.to_be_bytes())).await;
        assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"A", Some(&e)).await;

        // ---- update (name changes) ----
        clock.advance(1).await;
        e.name = "B".into();
        sys.put(e.clone()).await.unwrap();
        assert_index_row(&sys, &TEST_NAME_INDEX, b"A", None).await; // old gone
        assert_index_row(&sys, &TEST_NAME_INDEX, b"B", Some(&e.id.to_be_bytes())).await; // new present
        assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"A", None).await;
        assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"B", Some(&e)).await;

        // ---- delete ----
        clock.advance(1).await;
        sys.delete::<TestEntity>(&e.id.to_be_bytes()).await.unwrap();
        assert_index_row(&sys, &TEST_NAME_INDEX, b"B", None).await; // index row removed
        assert_get_by_index::<TestEntity>(&sys, &TEST_NAME_INDEX, b"B", None).await;
    }

    #[tokio::test]
    async fn duplicate_index_rejected() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let sys = KuramotoDb::new(
            dir.path().join("dup.redb").to_str().unwrap(),
            clock.clone(),
            vec![],
        )
        .await;

        sys.create_table_and_indexes::<TestEntity>().unwrap();

        let a = TestEntity {
            id: 1,
            name: "X".into(),
            value: 0,
        };
        let b = TestEntity {
            id: 2,
            name: "X".into(),
            value: 1,
        }; // same name key

        sys.put(a).await.unwrap();
        let err = sys.put(b).await.err().expect("should error");
        assert!(matches!(err, StorageError::DuplicateIndexKey { .. }));
    }

    #[tokio::test]
    async fn duplicate_index_allowed_nonunique() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let sys = KuramotoDb::new(
            dir.path().join("dup_ok.redb").to_str().unwrap(),
            clock.clone(),
            vec![],
        )
        .await;
        sys.create_table_and_indexes::<TestEntity>().unwrap();

        // Two different entities share the same "value"
        let a = TestEntity {
            id: 10,
            name: "A".into(),
            value: 7,
        };
        let b = TestEntity {
            id: 11,
            name: "B".into(),
            value: 7,
        };

        sys.put(a.clone()).await.unwrap();
        sys.put(b.clone()).await.unwrap(); // should NOT error

        // Range-based lookup should yield both ids
        let all_main_keys = sys
            .get_index_all(&TEST_VALUE_INDEX, &7i32.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(all_main_keys.len(), 2);
        assert!(all_main_keys.contains(&a.id.to_be_bytes().to_vec()));
        assert!(all_main_keys.contains(&b.id.to_be_bytes().to_vec()));

        // Fetch entities by non-unique index
        let got = sys
            .get_by_index_all::<TestEntity>(&TEST_VALUE_INDEX, &7i32.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(got.len(), 2);
        assert!(got.contains(&a));
        assert!(got.contains(&b));

        // Deleting one removes only its (idx_key, pk) mapping
        sys.delete::<TestEntity>(&a.id.to_be_bytes()).await.unwrap();
        let rest = sys
            .get_by_index_all::<TestEntity>(&TEST_VALUE_INDEX, &7i32.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0], b);
    }

    // ===== Plugins: order-dependent meta mutation =====
    struct Add100Plugin;
    #[async_trait]
    impl Plugin for Add100Plugin {
        async fn before_update(
            &self,
            _db: &KuramotoDb,
            _txn: &ReadTransaction,
            batch: &mut WriteBatch,
        ) -> Result<(), StorageError> {
            for req in batch {
                if let WriteRequest::Put { meta, .. } = req {
                    let (mut m, _): (BlobMeta, _) =
                        bincode::decode_from_slice(meta, bincode::config::standard())
                            .map_err(|o| StorageError::Bincode(o.to_string()))?;
                    m.updated_at += 100;
                    *meta = bincode::encode_to_vec(m, bincode::config::standard())
                        .map_err(|o| StorageError::Bincode(o.to_string()))?;
                }
            }
            Ok(())
        }

        fn attach_db(&self, _db: Arc<KuramotoDb>) {}
    }

    struct DoublePlugin;

    #[async_trait]
    impl Plugin for DoublePlugin {
        async fn before_update(
            &self,
            _db: &KuramotoDb,
            _txn: &ReadTransaction,
            batch: &mut WriteBatch,
        ) -> Result<(), StorageError> {
            for req in batch {
                if let WriteRequest::Put { meta, .. } = req {
                    let (mut m, _): (BlobMeta, _) =
                        bincode::decode_from_slice(meta, bincode::config::standard())
                            .map_err(|o| StorageError::Bincode(o.to_string()))?;
                    m.updated_at *= 2;
                    *meta = bincode::encode_to_vec(m, bincode::config::standard())
                        .map_err(|o| StorageError::Bincode(o.to_string()))?;
                }
            }
            Ok(())
        }

        fn attach_db(&self, _db: Arc<KuramotoDb>) {}
    }

    #[tokio::test]
    async fn plugins_applied_in_order() {
        // ───── assemble DB with plugins in a specific order ─────
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(100)); // initial now = 100

        let db = KuramotoDb::new(
            dir.path().join("order.redb").to_str().unwrap(),
            clock.clone(),
            vec![Arc::new(Add100Plugin), Arc::new(DoublePlugin)], // <- order!
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        // ───── put one entity ─────
        let e = TestEntity {
            id: 1,
            name: "O".into(),
            value: 9,
        };
        db.put(e.clone()).await.unwrap();

        // ───── meta.updated_at should be (100 + 100) * 2 = 400 ─────
        let meta = db
            .get_meta::<TestEntity>(&1u64.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(meta.updated_at, 400);
    }

    // ───────── RANGE HELPERS: one test per public function ──────────────────────

    #[tokio::test]
    async fn range_by_pk_basic() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(dir.path().join("pk.redb").to_str().unwrap(), clock, vec![]).await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        // ids 1-3
        for i in 1..=3 {
            db.put(TestEntity {
                id: i,
                name: (b'A' + i as u8 - 1).to_string(),
                value: i as i32,
            })
            .await
            .unwrap();
        }

        let got = db
            .range_by_pk::<TestEntity>(&2u64.to_be_bytes(), &4u64.to_be_bytes(), None)
            .await
            .unwrap();
        assert_eq!(got.iter().map(|e| e.id).collect::<Vec<_>>(), vec![2, 3]);
    }

    #[tokio::test]
    async fn range_by_index_basic() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db =
            KuramotoDb::new(dir.path().join("idx.redb").to_str().unwrap(), clock, vec![]).await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        for (id, name) in [(1, "A"), (2, "B"), (3, "C")] {
            db.put(TestEntity {
                id,
                name: name.into(),
                value: 0,
            })
            .await
            .unwrap();
        }

        let got = db
            .range_by_index::<TestEntity>(&TEST_NAME_INDEX, b"B", b"D", None)
            .await
            .unwrap();
        assert_eq!(
            got.iter().map(|e| e.name.clone()).collect::<Vec<_>>(),
            vec!["B", "C"]
        );
    }

    #[tokio::test]
    async fn range_with_meta_by_pk_basic() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(123)); // initial now = 100
        let db = KuramotoDb::new(
            dir.path().join("pk_meta.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        db.put(TestEntity {
            id: 10,
            name: "X".into(),
            value: 1,
        })
        .await
        .unwrap();
        db.put(TestEntity {
            id: 11,
            name: "Y".into(),
            value: 1,
        })
        .await
        .unwrap();

        let got = db
            .range_with_meta_by_pk::<TestEntity>(&10u64.to_be_bytes(), &12u64.to_be_bytes(), None)
            .await
            .unwrap();
        assert_eq!(got.len(), 2);
        assert!(
            got.iter()
                .all(|(_, m)| m.version == 0 && m.created_at == 123)
        );
    }

    #[tokio::test]
    async fn range_with_meta_by_index_basic() {
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(999));
        let db = KuramotoDb::new(
            dir.path().join("idx_meta.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        let rows = [
            TestEntity {
                id: 5,
                name: "K".into(),
                value: 2,
            },
            TestEntity {
                id: 6,
                name: "L".into(),
                value: 2,
            },
        ];
        for r in &rows {
            db.put(r.clone()).await.unwrap();
        }

        let got = db
            .range_with_meta_by_index::<TestEntity>(&TEST_NAME_INDEX, b"K", b"M", None)
            .await
            .unwrap();
        assert_eq!(got.len(), 2);
        for ((e, meta), expected) in got.iter().zip(rows.iter()) {
            assert_eq!(e, expected);
            assert_eq!(meta.created_at, 999);
        }
    }

    #[tokio::test]
    async fn put_by_table_bytes_decodes_and_indexes() {
        // Arrange: fresh DB with TestEntity registered
        let dir = tempdir().unwrap();
        let clock = Arc::new(MockClock::new(1_234));
        let db = KuramotoDb::new(
            dir.path().join("raw_put.redb").to_str().unwrap(),
            clock.clone(),
            vec![],
        )
        .await;

        db.create_table_and_indexes::<TestEntity>().unwrap();

        // Build a TestEntity and encode it using its StorageEntity format (version byte + bincode)
        let e = TestEntity {
            id: 42,
            name: "Alice".into(),
            value: 7,
        };
        let raw = e.to_bytes();

        // Act: write by table name + raw bytes (no Arc self, just &self)
        db.put_by_table_bytes(TEST_TABLE.name(), &raw)
            .await
            .unwrap();

        // Assert: data is present and equals original
        let got = db
            .get_data::<TestEntity>(&e.id.to_be_bytes())
            .await
            .expect("entity should exist");
        assert_eq!(got, e);

        // Assert: UNIQUE index ("name") points to the PK
        let main = db
            .get_index(&TEST_NAME_INDEX, b"Alice")
            .await
            .expect("index read should succeed")
            .expect("unique index row should exist");
        assert_eq!(main, e.id.to_be_bytes());

        // Assert: NON-UNIQUE index ("value") can fetch back this entity
        let by_value = db
            .get_by_index_all::<TestEntity>(&TEST_VALUE_INDEX, &7i32.to_be_bytes())
            .await
            .expect("non-unique lookup should succeed");
        assert!(by_value.iter().any(|x| x == &e));

        // Error case: unknown table name
        let err = db
            .put_by_table_bytes("no_such_table", &raw)
            .await
            .expect_err("unknown table should error");
        match err {
            StorageError::Other(msg) => {
                assert!(
                    msg.contains("no entity registered for table"),
                    "unexpected message: {msg}"
                )
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    // ───────── NEW TESTS FOR SNAPSHOT SEMANTICS + LIMITS ─────────

    #[tokio::test]
    async fn get_data_tx_respects_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db =
            KuramotoDb::new(dir.path().join("gdt.redb").to_str().unwrap(), clock, vec![]).await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        let mut e = TestEntity {
            id: 1,
            name: "A".into(),
            value: 1,
        };
        db.put(e.clone()).await.unwrap();

        // Take snapshot of state with name "A"
        let snap = db.begin_read_txn().unwrap();

        // Update row after snapshot
        e.name = "B".into();
        db.put(e.clone()).await.unwrap();

        // Snapshot still sees "A"
        let old = db
            .get_data_tx::<TestEntity>(Some(&snap), &1u64.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(old.name, "A");

        // Fresh read sees "B"
        let new_ = db
            .get_data::<TestEntity>(&1u64.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(new_.name, "B");
    }

    #[tokio::test]
    async fn get_index_tx_respects_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(dir.path().join("gi.redb").to_str().unwrap(), clock, vec![]).await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        let mut e = TestEntity {
            id: 1,
            name: "Old".into(),
            value: 0,
        };
        db.put(e.clone()).await.unwrap();

        let snap = db.begin_read_txn().unwrap();

        // Change unique index key
        e.name = "New".into();
        db.put(e.clone()).await.unwrap();

        // Snapshot: unique index "Old" still points to id 1
        let snap_hit = db
            .get_by_index_tx::<TestEntity>(Some(&snap), &TEST_NAME_INDEX, b"Old")
            .await
            .unwrap();
        assert!(snap_hit.is_some());
        assert_eq!(snap_hit.unwrap().id, 1);

        // Fresh read: "Old" is gone, "New" exists
        assert!(
            db.get_by_index::<TestEntity>(&TEST_NAME_INDEX, b"Old")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            db.get_by_index::<TestEntity>(&TEST_NAME_INDEX, b"New")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn get_index_all_tx_respects_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("giall.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        // Two rows share same non-unique value = 7
        let a = TestEntity {
            id: 10,
            name: "A".into(),
            value: 7,
        };
        let b = TestEntity {
            id: 11,
            name: "B".into(),
            value: 7,
        };
        db.put(a.clone()).await.unwrap();
        db.put(b.clone()).await.unwrap();

        let snap = db.begin_read_txn().unwrap();

        // Delete one after snapshot
        db.delete::<TestEntity>(&a.id.to_be_bytes()).await.unwrap();

        // Snapshot sees both PKs
        let pks_snap = db
            .get_index_all_tx(Some(&snap), &TEST_VALUE_INDEX, &7i32.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(pks_snap.len(), 2);

        // Fresh read sees only the remaining one
        let pks_now = db
            .get_index_all(&TEST_VALUE_INDEX, &7i32.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(pks_now.len(), 1);
        assert_eq!(pks_now[0], b.id.to_be_bytes());
    }

    #[tokio::test]
    async fn get_by_index_all_tx_respects_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("gbiall.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        let a = TestEntity {
            id: 20,
            name: "A".into(),
            value: 9,
        };
        let b = TestEntity {
            id: 21,
            name: "B".into(),
            value: 9,
        };
        db.put(a.clone()).await.unwrap();
        db.put(b.clone()).await.unwrap();

        let snap = db.begin_read_txn().unwrap();

        db.delete::<TestEntity>(&b.id.to_be_bytes()).await.unwrap();

        // Snapshot returns both entities
        let got_snap = db
            .get_by_index_all_tx::<TestEntity>(Some(&snap), &TEST_VALUE_INDEX, &9i32.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(got_snap.len(), 2);

        // Fresh returns only the survivor
        let got_now = db
            .get_by_index_all::<TestEntity>(&TEST_VALUE_INDEX, &9i32.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(got_now.len(), 1);
        assert_eq!(got_now[0], a);
    }

    #[tokio::test]
    async fn range_by_index_limit_works() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("rbilimit.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        for (id, name) in [(1, "A"), (2, "B"), (3, "C")] {
            db.put(TestEntity {
                id,
                name: name.into(),
                value: 0,
            })
            .await
            .unwrap();
        }

        let got = db
            .range_by_index::<TestEntity>(&TEST_NAME_INDEX, b"A", b"Z", Some(1))
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
    }

    #[tokio::test]
    async fn range_by_pk_limit_works() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("rbplimit.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        for i in 1..=5 {
            db.put(TestEntity {
                id: i,
                name: format!("N{i}"),
                value: i as i32,
            })
            .await
            .unwrap();
        }

        let got = db
            .range_by_pk::<TestEntity>(&1u64.to_be_bytes(), &6u64.to_be_bytes(), Some(2))
            .await
            .unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].id, 1);
        assert_eq!(got[1].id, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn get_with_meta_tx_respects_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(100));
        let db = KuramotoDb::new(
            dir.path().join("gwmtx.redb").to_str().unwrap(),
            clock.clone(),
            vec![],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        let mut e = TestEntity {
            id: 1,
            name: "X".into(),
            value: 0,
        };
        db.put(e.clone()).await.unwrap(); // version 0, created_at = 100

        let snap = db.begin_read_txn().unwrap();

        clock.advance(5).await;
        e.value = 1;
        db.put(e.clone()).await.unwrap(); // version 1, updated_at = 105

        let (se, sm) = db
            .get_with_meta_tx::<TestEntity>(Some(&snap), &1u64.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(se.value, 0);
        assert_eq!(sm.version, 0);
        assert_eq!(sm.created_at, 100);

        let (_ne, nm) = db
            .get_with_meta::<TestEntity>(&1u64.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(nm.version, 1);
        assert_eq!(nm.updated_at, 105);
    }

    #[tokio::test]
    async fn range_with_meta_by_index_tx_respects_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(777));
        let db = KuramotoDb::new(
            dir.path().join("rwmbi.redb").to_str().unwrap(),
            clock.clone(),
            vec![],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        for (id, name) in [(1, "A"), (2, "B")] {
            db.put(TestEntity {
                id,
                name: name.into(),
                value: 0,
            })
            .await
            .unwrap();
        }

        let snap = db.begin_read_txn().unwrap();

        // Add a third row after snapshot
        db.put(TestEntity {
            id: 3,
            name: "C".into(),
            value: 0,
        })
        .await
        .unwrap();

        let got_snap = db
            .range_with_meta_by_index_tx::<TestEntity>(
                Some(&snap),
                &TEST_NAME_INDEX,
                b"A",
                b"Z",
                None,
            )
            .await
            .unwrap();
        assert_eq!(got_snap.len(), 2);
        for (_, m) in &got_snap {
            assert_eq!(m.created_at, 777);
        }

        let got_now = db
            .range_with_meta_by_index::<TestEntity>(&TEST_NAME_INDEX, b"A", b"Z", None)
            .await
            .unwrap();
        assert_eq!(got_now.len(), 3);
    }

    #[tokio::test]
    async fn get_meta_tx_and_wrappers_match() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(1));
        let db = KuramotoDb::new(
            dir.path().join("gmtx.redb").to_str().unwrap(),
            clock,
            vec![],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        let e = TestEntity {
            id: 9,
            name: "Z".into(),
            value: 1,
        };
        db.put(e.clone()).await.unwrap();

        let snap = db.begin_read_txn().unwrap();

        let m1 = db
            .get_meta::<TestEntity>(&9u64.to_be_bytes())
            .await
            .unwrap();
        let m2 = db
            .get_meta_tx::<TestEntity>(Some(&snap), &9u64.to_be_bytes())
            .await
            .unwrap();
        let m3 = db
            .get_meta_tx::<TestEntity>(None, &9u64.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(m1, m2);
        assert_eq!(m1, m3);
    }

    #[tokio::test]
    async fn range_by_pk_tx_reads_from_read_txn_snapshot() {
        use async_trait::async_trait;
        use std::sync::Arc;
        use tokio::sync::oneshot;

        // Reuse TestEntity from existing tests
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));

        // A oneshot so the plugin can report how many rows it saw during before_update
        let (seen_tx, seen_rx) = oneshot::channel::<usize>();

        struct SnapshotProbe {
            // Wrap in Option so we can move it out once; wrap that in a mutex for &self access
            tx: tokio::sync::Mutex<Option<oneshot::Sender<usize>>>,
        }

        #[async_trait]
        impl Plugin for SnapshotProbe {
            async fn before_update(
                &self,
                db: &KuramotoDb,
                txn: &ReadTransaction,
                batch: &mut WriteBatch,
            ) -> Result<(), StorageError> {
                // Only fire when the incoming write is the one with id == 3
                let target_pk = 3u64.to_be_bytes();
                let should_fire = batch.iter().any(|req| {
                    if let WriteRequest::Put {
                        data_table, key, ..
                    } = req
                    {
                        // match the test table and the pk for id == 3
                        data_table.name() == "test" && key.as_slice() == target_pk.as_slice()
                    } else {
                        false
                    }
                });

                if !should_fire {
                    return Ok(());
                }

                // Read through the provided read-txn snapshot (pre-write state)
                let rows = db
                    .range_by_pk_tx::<TestEntity>(Some(txn), &[], &[0xFF], None)
                    .await?;

                // Move the oneshot sender out exactly once
                if let Some(tx) = self.tx.lock().await.take() {
                    let _ = tx.send(rows.len());
                }
                Ok(())
            }

            fn attach_db(&self, _db: Arc<KuramotoDb>) {}
        }

        let db = KuramotoDb::new(
            dir.path().join("snap.redb").to_str().unwrap(),
            clock.clone(),
            vec![Arc::new(SnapshotProbe {
                tx: tokio::sync::Mutex::new(Some(seen_tx)),
            })],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        // Seed two rows (state visible to the next write txn snapshot)
        for (id, name, value) in [(1u64, "A", 10), (2, "B", 20)] {
            db.put(TestEntity {
                id,
                name: name.into(),
                value,
            })
            .await
            .unwrap();
        }

        // Trigger a third put → before_update runs inside a write txn.
        db.put(TestEntity {
            id: 3,
            name: "C".into(),
            value: 30,
        })
        .await
        .unwrap();

        // The plugin reported what it saw during before_update:
        let seen = seen_rx.await.expect("plugin should report a count");
        assert_eq!(seen, 2, "write-txn snapshot should reflect pre-write state");

        // And after commit, DB has all 3 rows
        let all = db
            .range_by_pk::<TestEntity>(&[], &[0xFF], None)
            .await
            .unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn range_by_index_tx_reads_from_read_txn_snapshot() {
        use async_trait::async_trait;
        use tokio::sync::oneshot;

        // oneshot so the probe can report what it saw during before_update
        let (seen_tx, seen_rx) = oneshot::channel::<usize>();

        // Reusable little probe: fires once when the batch contains id == 3 in the "test" table.
        struct IndexSnapshotProbe {
            tx: tokio::sync::Mutex<Option<oneshot::Sender<usize>>>,
        }

        #[async_trait]
        impl Plugin for IndexSnapshotProbe {
            async fn before_update(
                &self,
                db: &KuramotoDb,
                txn: &redb::ReadTransaction,
                batch: &mut WriteBatch,
            ) -> Result<(), StorageError> {
                // Only fire on the write for id == 3 into TEST_TABLE
                let target_pk = 3u64.to_be_bytes();
                let should_fire = batch.iter().any(|req| match req {
                    WriteRequest::Put {
                        data_table, key, ..
                    } => data_table.name() == "test" && key.as_slice() == target_pk,
                    _ => false,
                });
                if !should_fire {
                    return Ok(());
                }

                // Count rows via the UNIQUE name index, A..Z, from the provided snapshot
                let rows = db
                    .range_by_index_tx::<TestEntity>(Some(txn), &TEST_NAME_INDEX, b"A", b"Z", None)
                    .await?;

                if let Some(tx) = self.tx.lock().await.take() {
                    let _ = tx.send(rows.len());
                }
                Ok(())
            }

            fn attach_db(&self, _db: Arc<KuramotoDb>) {}
        }

        // Set up DB with the probe plugin
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(MockClock::new(0));
        let db = KuramotoDb::new(
            dir.path().join("idxsnap.redb").to_str().unwrap(),
            clock.clone(),
            vec![Arc::new(IndexSnapshotProbe {
                tx: tokio::sync::Mutex::new(Some(seen_tx)),
            })],
        )
        .await;
        db.create_table_and_indexes::<TestEntity>().unwrap();

        // Seed two rows
        for (id, name, value) in [(1u64, "A", 10), (2, "B", 20)] {
            db.put(TestEntity {
                id,
                name: name.into(),
                value,
            })
            .await
            .unwrap();
        }

        // Trigger third put → probe runs in before_update, reading via the snapshot
        db.put(TestEntity {
            id: 3,
            name: "C".into(),
            value: 30,
        })
        .await
        .unwrap();

        // Probe should have seen just the first two
        let seen = seen_rx.await.expect("probe should send a count");
        assert_eq!(seen, 2, "snapshot should reflect pre-write index state");

        // After commit, index range should see all three
        let all = db
            .range_by_index::<TestEntity>(&TEST_NAME_INDEX, b"A", b"Z", None)
            .await
            .unwrap();
        assert_eq!(all.len(), 3);
    }
}
