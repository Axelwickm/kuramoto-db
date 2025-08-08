use redb::ReadableTable;
use redb::TableHandle;
use redb::{Database, TableDefinition};

use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use crate::middlewares::Middleware;
use crate::storage_entity::IndexCardinality;

use super::{
    clock::Clock, meta::BlobMeta, region_lock::RegionLock, storage_entity::StorageEntity,
    storage_error::StorageError,
};

pub type StaticTableDef = &'static TableDefinition<'static, &'static [u8], Vec<u8>>;

pub trait Plugin: Send + Sync {}

pub struct IndexPutRequest {
    table: StaticTableDef,
    key: Vec<u8>,   // index key (e.g., email as bytes)
    value: Vec<u8>, // usually the main key (e.g., user_id)
    unique: bool,
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
    middlewares: Vec<Arc<dyn Middleware>>,
}

impl KuramotoDb {
    pub async fn new(
        path: &str,
        clock: Arc<dyn Clock>,
        middlewares: Vec<Arc<dyn Middleware>>,
    ) -> Arc<Self> {
        let db = Database::create(path).unwrap();
        let (write_tx, mut write_rx) = mpsc::channel(100);
        let db_arc = Arc::new(db);

        let sys = Arc::new(KuramotoDb {
            db: db_arc.clone(),
            write_tx,
            clock,
            middlewares,
        });
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
        Ok(())
    }

    // Getters/setters
    pub fn get_clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }

    // Generic CRUD (async)

    /// Get the primary key for an entity by an index.
    /// Returns Ok(Some(main_key)) if found, Ok(None) if not found, or Err on error.
    pub async fn get_index(
        &self,
        table: StaticTableDef,
        idx_key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let table = txn
            .open_table(table.clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let got = table
            .get(idx_key)
            .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(got.map(|v| v.value().to_vec()))
    }

    /// Get an entity by an index key.
    /// Returns Ok(Some(entity)) if found, Ok(None) if not found, or Err on error.
    pub async fn get_by_index<E: StorageEntity>(
        &self,
        index_table: StaticTableDef,
        idx_key: &[u8],
    ) -> Result<Option<E>, StorageError> {
        if let Some(main_key) = self.get_index(index_table, idx_key).await? {
            match self.get_data::<E>(&main_key).await {
                Ok(entity) => Ok(Some(entity)),
                Err(StorageError::NotFound) => Ok(None),
                Err(e) => Err(e),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn get_index_all(
        &self,
        table: StaticTableDef,
        idx_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let table = txn
            .open_table(table.clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let (lo, hi) = nonunique_bounds(idx_key);
        let mut out = Vec::new();
        for item in table
            .range(lo.as_slice()..hi.as_slice())
            .map_err(|e| StorageError::Other(e.to_string()))?
        {
            let (_k, v) = item.map_err(|e| StorageError::Other(e.to_string()))?;
            out.push(v.value().to_vec());
        }
        Ok(out)
    }

    pub async fn get_by_index_all<E: StorageEntity>(
        &self,
        index_table: StaticTableDef,
        idx_key: &[u8],
    ) -> Result<Vec<E>, StorageError> {
        let mut out = Vec::new();
        for main_key in self.get_index_all(index_table, idx_key).await? {
            if let Ok(e) = self.get_data::<E>(&main_key).await {
                out.push(e);
            }
        }
        Ok(out)
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

    pub async fn get_data<E: StorageEntity>(&self, key: &[u8]) -> Result<E, StorageError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let table = txn
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

    pub async fn get_meta<E: StorageEntity>(&self, key: &[u8]) -> Result<BlobMeta, StorageError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let meta_table = txn
            .open_table(E::meta_table_def().clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let val = meta_table
            .get(key)
            .map_err(|e| StorageError::Other(e.to_string()))?;
        if let Some(v) = val {
            bincode::decode_from_slice::<BlobMeta, _>(&v.value(), bincode::config::standard())
                .map(|(meta, _)| meta)
                .map_err(|e| StorageError::Bincode(e.to_string()))
        } else {
            Err(StorageError::NotFound)
        }
    }

    pub async fn get_with_meta<E: StorageEntity>(
        &self,
        key: &[u8],
    ) -> Result<(E, BlobMeta), StorageError> {
        let data = self.get_data::<E>(key).await?;
        let meta = self.get_meta::<E>(key).await?;
        Ok((data, meta))
    }

    // ------------  RANGE API  ------------
    /// Return up to `limit` entities whose **primary key** ∈ `[start, end)`.
    /// Pass `None` to stream the full range.
    pub async fn range_by_pk<E: StorageEntity>(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<E>, StorageError> {
        // 1) collect raw bytes inside one read-txn
        let txn = self
            .db
            .begin_read()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let table = txn
            .open_table(E::table_def().clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let mut rows: Vec<Vec<u8>> = Vec::new();
        for r in table
            .range(start..end)
            .map_err(|e| StorageError::Other(e.to_string()))?
        {
            let (_k, v) = r.map_err(|e| StorageError::Other(e.to_string()))?;
            rows.push(v.value().to_vec());
            if limit.map_or(false, |n| rows.len() >= n) {
                break;
            }
        }
        drop(table);
        drop(txn); // ensure all borrows dead before decoding

        // 2) decode
        rows.into_iter()
            .map(|bytes| E::load_and_migrate(&bytes))
            .collect()
    }

    /// Return up to `limit` entities whose **index key** ∈ `[start, end)`.
    ///
    /// Works for both unique and non-unique indexes. For a *non-unique*
    /// lookup of a single key you’d normally pass the low/high bounds you
    /// already use elsewhere (`nonunique_bounds(&idx_key)`).
    pub async fn range_by_index<E: StorageEntity>(
        &self,
        index_table: StaticTableDef,
        start_idx_key: &[u8],
        end_idx_key: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<E>, StorageError> {
        // (a) gather PKs
        let txn = self
            .db
            .begin_read()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let idx = txn
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
        drop(idx);
        drop(txn);

        // (b) load entities
        let mut out = Vec::with_capacity(pks.len());
        for pk in pks {
            if let Ok(e) = self.get_data::<E>(&pk).await {
                out.push(e);
            }
        }
        Ok(out)
    }

    pub async fn range_with_meta_by_pk<E: StorageEntity>(
        &self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(E, BlobMeta)>, StorageError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let data_t = txn
            .open_table(E::table_def().clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let meta_t = txn
            .open_table(E::meta_table_def().clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let mut out = Vec::new();
        for row in data_t
            .range(start..end)
            .map_err(|e| StorageError::Other(e.to_string()))?
        {
            let (k, v) = row.map_err(|e| StorageError::Other(e.to_string()))?;

            // decode entity
            let entity = E::load_and_migrate(v.value().as_slice())?;

            // fetch and decode meta
            let meta_raw = meta_t
                .get(k.value())
                .map_err(|e| StorageError::Other(e.to_string()))?
                .expect("meta missing")
                .value();
            let (meta, _) =
                bincode::decode_from_slice::<BlobMeta, _>(&meta_raw, bincode::config::standard())
                    .map_err(|e| StorageError::Bincode(e.to_string()))?;

            out.push((entity, meta));
            if limit.map_or(false, |n| out.len() >= n) {
                break;
            }
        }
        Ok(out)
    }

    /// Return up to `limit` **(entity, meta)** pairs whose *index* key ∈ `[start,end)`.
    pub async fn range_with_meta_by_index<E: StorageEntity>(
        &self,
        index_table: StaticTableDef,
        start_idx_key: &[u8],
        end_idx_key: &[u8],
        limit: Option<usize>,
    ) -> Result<Vec<(E, BlobMeta)>, StorageError> {
        // (1) collect PKs first
        let txn = self
            .db
            .begin_read()
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let idx = txn
            .open_table(index_table.clone())
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let mut pks = Vec::new();
        for row in idx
            .range(start_idx_key..end_idx_key)
            .map_err(|e| StorageError::Other(e.to_string()))?
        {
            let (_k, v) = row.map_err(|e| StorageError::Other(e.to_string()))?;
            pks.push(v.value().to_vec());
            if limit.map_or(false, |n| pks.len() >= n) {
                break;
            }
        }
        drop(idx);
        drop(txn);

        // (2) resolve (entity,meta) for each PK
        let mut out = Vec::with_capacity(pks.len());
        for pk in pks {
            let e = self.get_data::<E>(&pk).await?;
            let m = self.get_meta::<E>(&pk).await?;
            out.push((e, m));
        }
        Ok(out)
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
        let txn = self
            .db
            .begin_write()
            .map_err(|e| StorageError::Other(e.to_string()))?;

        // Middleware can append more requests to `batch`
        for m in &self.middlewares {
            m.before_write(self, &txn, &mut batch).await?;
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
                        let meta_t = txn
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
                        let mut t = txn
                            .open_table(*data_table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        t.insert(&*key, value)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }

                    // ---- Index removes ----
                    for idx in index_removes {
                        let mut t = txn
                            .open_table(*idx.table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        t.remove(idx.key.as_slice())
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }

                    // ---- Index inserts (uniqueness) ----
                    for idx in index_puts {
                        let mut t = txn
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
                        let mut meta_t = txn
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
                        let mut t = txn
                            .open_table(*data_table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        t.remove(&*key)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }

                    // ---- Meta update ----
                    {
                        let mut meta_t = txn
                            .open_table(*meta_table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        meta_t
                            .insert(&*key, meta)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }

                    // ---- Index removes ----
                    for idx in index_removes {
                        let mut t = txn
                            .open_table(*idx.table)
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                        t.remove(idx.key.as_slice())
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }
                }
            }
        }

        // Commit once
        txn.commit()
            .map_err(|e| StorageError::Other(e.to_string()))?;

        // Reply once for the whole batch
        let _ = reply.send(Ok(()));
        Ok(())
    }
}
