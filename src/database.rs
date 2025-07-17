use redb::ReadableTable;
use redb::TableHandle;
use redb::{Database, TableDefinition};

use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use super::{
    clock::Clock, meta::BlobMeta, region_lock::RegionLock, storage_entity::StorageEntity,
    storage_error::StorageError,
};

pub type StaticTableDef = &'static TableDefinition<'static, &'static [u8], Vec<u8>>;

pub struct IndexPutRequest {
    table: StaticTableDef,
    key: Vec<u8>,   // index key (e.g., email as bytes)
    value: Vec<u8>, // usually the main key (e.g., user_id)
}
pub struct IndexRemoveRequest {
    table: StaticTableDef,
    key: Vec<u8>,
}

pub enum WriteRequest {
    Put {
        data_table: StaticTableDef,
        meta_table: StaticTableDef,
        key: Vec<u8>,
        value: Vec<u8>,
        meta: Vec<u8>,
        index_removes: Vec<IndexRemoveRequest>,
        index_puts: Vec<IndexPutRequest>,
        respond_to: oneshot::Sender<Result<(), StorageError>>,
    },
    Delete {
        data_table: StaticTableDef,
        meta_table: StaticTableDef,
        key: Vec<u8>,
        meta: Vec<u8>,
        index_removes: Vec<IndexRemoveRequest>,
        respond_to: oneshot::Sender<Result<(), StorageError>>,
    },
}

pub struct KuramotoDb {
    db: Arc<Database>,
    write_tx: mpsc::Sender<WriteRequest>,
    clock: Arc<dyn Clock>,
}

impl KuramotoDb {
    pub async fn new(path: &str, clock: Arc<dyn Clock>) -> Arc<Self> {
        let db = Database::create(path).unwrap();
        let (write_tx, mut write_rx) = mpsc::channel(100);
        let db_arc = Arc::new(db);

        let sys = Arc::new(KuramotoDb {
            db: db_arc.clone(),
            write_tx,
            clock,
        });
        let sys2 = sys.clone();
        tokio::spawn(async move {
            while let Some(req) = write_rx.recv().await {
                let _ = sys2.handle_write(req).await;
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

                    if old_key != new_key {
                        removes.push(IndexRemoveRequest {
                            table: idx.table_def,
                            key: old_key,
                        });
                        puts.push(IndexPutRequest {
                            table: idx.table_def,
                            key: new_key,
                            value: entity.primary_key(),
                        });
                    }
                    (puts, removes)
                })
        } else {
            // New insert, insert index
            (
                E::indexes()
                    .iter()
                    .map(|idx| IndexPutRequest {
                        table: idx.table_def,
                        key: (idx.key_fn)(&entity),
                        value: entity.primary_key(),
                    })
                    .collect(),
                Vec::new(),
            )
        };

        self.write_tx
            .send(WriteRequest::Put {
                data_table: &E::table_def(),
                meta_table: &E::meta_table_def(),
                key,
                value: entity.to_bytes(),
                meta: bincode::encode_to_vec(meta, bincode::config::standard()).unwrap(),
                index_puts,
                index_removes,
                respond_to: tx,
            })
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
        let index_removes: Vec<IndexRemoveRequest> = E::indexes()
            .iter()
            .map(|idx| IndexRemoveRequest {
                table: idx.table_def,
                key: (idx.key_fn)(&old_entity),
            })
            .collect();

        self.write_tx
            .send(WriteRequest::Delete {
                data_table: &E::table_def(),
                meta_table: &E::meta_table_def(),
                key: key.to_vec(),
                meta: bincode::encode_to_vec(meta, bincode::config::standard()).unwrap(),
                index_removes,
                respond_to: tx,
            })
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

    pub async fn raw_write(&self, mut req: WriteRequest) -> Result<(), StorageError> {
        // create a fresh responder
        let (tx, rx) = oneshot::channel();

        // patch the request’s respond_to field
        match &mut req {
            WriteRequest::Put { respond_to, .. } => *respond_to = tx,
            WriteRequest::Delete { respond_to, .. } => *respond_to = tx,
        }

        // enqueue
        self.write_tx
            .send(req)
            .await
            .map_err(|e| StorageError::Other(format!("Write queue dropped: {}", e)))?;

        // await the background writer’s result
        rx.await
            .map_err(|e| StorageError::Other(format!("Write task dropped: {}", e)))?
    }

    // ----------- Internal handler --------------
    async fn handle_write(&self, req: WriteRequest) -> Result<(), StorageError> {
        match req {
            WriteRequest::Put {
                data_table,
                meta_table,
                key,
                value,
                meta,
                index_puts,
                index_removes,
                respond_to,
            } => {
                let txn = self
                    .db
                    .begin_write()
                    .map_err(|e| StorageError::Other(e.to_string()))?;

                // ---- Version check (primary key) ----
                {
                    // Fail if meta table does not exist
                    let meta_t = match txn.open_table(*meta_table) {
                        Ok(t) => t,
                        Err(_) => {
                            let error = Err(StorageError::Other(format!(
                                "Meta table does not exist: {}",
                                meta_table.name()
                            )));
                            let _ = respond_to.send(error.clone());
                            return error;
                        }
                    };

                    if let Some(existing_meta_raw) = meta_t
                        .get(&*key)
                        .map_err(|e| StorageError::Other(e.to_string()))?
                    {
                        // decode existing meta to inspect its version
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
                            return Err(StorageError::PutButNoVersionIncrease);
                        }
                    };
                }

                // ---- Insert / overwrite main row ----
                {
                    // Fail if data table does not exist
                    let mut t = match txn.open_table(*data_table) {
                        Ok(t) => t,
                        Err(_) => {
                            let error = Err(StorageError::Other(format!(
                                "Data table does not exist: {}",
                                data_table.name()
                            )));
                            let _ = respond_to.send(error.clone());
                            return error;
                        }
                    };
                    t.insert(&*key, value)
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                }

                // ---- Handle index removals ----
                for idx in index_removes {
                    let mut t = match txn.open_table(*idx.table) {
                        Ok(t) => t,
                        Err(_) => {
                            let _ = respond_to.send(Err(StorageError::Other(format!(
                                "Index table does not exist: {}",
                                idx.table.name()
                            ))));
                            return Err(StorageError::Other(format!(
                                "Index table does not exist: {}",
                                idx.table.name()
                            )));
                        }
                    };
                    t.remove(idx.key.as_slice())
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                }

                // ---- Handle index inserts with uniqueness check ----
                for idx in index_puts {
                    let mut t = match txn.open_table(*idx.table) {
                        Ok(t) => t,
                        Err(_) => {
                            let error = Err(StorageError::Other(format!(
                                "Index table does not exist: {}",
                                idx.table.name()
                            )));
                            let _ = respond_to.send(error.clone());
                            return error;
                        }
                    };

                    if let Some(existing) = t
                        .get(idx.key.as_slice())
                        .map_err(|e| StorageError::Other(e.to_string()))?
                    {
                        if existing.value() != idx.value.as_slice() {
                            let error = Err(StorageError::DuplicateIndexKey {
                                index_name: idx.table.name(),
                            });
                            let _ = respond_to.send(error.clone());
                            return error;
                        }
                    }

                    t.insert(idx.key.as_slice(), idx.value)
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                }

                // ---- Finally write updated meta (includes bumped version) ----
                {
                    let mut meta_t = match txn.open_table(*meta_table) {
                        Ok(t) => t,
                        Err(_) => {
                            let error = Err(StorageError::Other(format!(
                                "Meta table does not exist: {}",
                                meta_table.name()
                            )));
                            let _ = respond_to.send(error.clone());
                            return error;
                        }
                    };
                    meta_t
                        .insert(&*key, meta)
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                }

                txn.commit()
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                let _ = respond_to.send(Ok(()));
            }

            WriteRequest::Delete {
                data_table,
                meta_table,
                key,
                meta,
                index_removes,
                respond_to,
            } => {
                let txn = self
                    .db
                    .begin_write()
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                {
                    let mut t = txn
                        .open_table(*data_table)
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                    t.remove(&*key)
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                }
                {
                    let mut meta_t = txn
                        .open_table(*meta_table)
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                    meta_t
                        .insert(&*key, meta)
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                }
                // Handle index removes
                for idx in index_removes {
                    let mut t = txn
                        .open_table(*idx.table)
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                    t.remove(idx.key.as_slice())
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                }
                txn.commit()
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                let _ = respond_to.send(Ok(()));
            }
        }
        Ok(())
    }
}
