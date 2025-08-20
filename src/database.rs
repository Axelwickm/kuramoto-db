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

        // Plugins can append/modify requests in `batch` via before_update hooks
        for plugin in self.plugins.iter() {
            plugin.before_update(self, &txn, &mut batch).await?;
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

/*───────────────────────────────────────────────────────────────*/
/* tests                                                         */
/*───────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bincode::{Decode, Encode};
    use redb::TableDefinition;
    use redb::TableHandle;
    use redb::WriteTransaction;
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
            _txn: &WriteTransaction,
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

        fn attach_db(&self, db: Arc<KuramotoDb>) {}
    }

    struct DoublePlugin;

    #[async_trait]
    impl Plugin for DoublePlugin {
        async fn before_update(
            &self,
            _db: &KuramotoDb,
            _txn: &WriteTransaction,
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

        fn attach_db(&self, db: Arc<KuramotoDb>) {}
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
}
