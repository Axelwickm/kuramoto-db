use bincode::{Decode, Encode};
use redb::TableDefinition;

use crate::{
    KuramotoDb, StaticTableDef,
    plugins::harmonizer::riblt::{CodedSymbol, Encoder, Symbol},
    storage_entity::{IndexSpec, StorageEntity},
    storage_error::StorageError,
    uuid_bytes::UuidBytes,
};

/*──────────────────────────────────── glue impls ────────────────*/
use bincode::de::Decoder as BinDecoder;
use bincode::enc::Encoder as BinEncoder;
use bincode::error::{DecodeError, EncodeError};

/*──────────────────────────── constants ─────────────────────────*/

pub type Sym = u64;
pub type Enc = Encoder<8, Sym>;
pub type Cell = CodedSymbol<8, Sym>;

pub const DIGEST_CHUNK_BYTES: usize = 1024;
pub const CELLS_PER_CHUNK: usize = 16;
pub const STORED_CHUNKS: usize = 1; // ← *only* the first chunk lives in the DB

/*─────────────────────────── tables ─────────────────────────────*/

pub static AVAIL_CHILDREN_TBL: StaticTableDef = &TableDefinition::new("availability_children");
pub static AVAIL_CHILDREN_META_TBL: StaticTableDef =
    &TableDefinition::new("availability_children_meta");

pub static AVAIL_DIG_CHUNK_TBL: StaticTableDef =
    &TableDefinition::new("availability_digest_chunks");
pub static AVAIL_DIG_CHUNK_META_TBL: StaticTableDef =
    &TableDefinition::new("availability_digest_chunks_meta");

/*────────────────────── Child row ───────────────────────────────*/

#[derive(Clone, Debug)]
pub struct Child {
    pub parent: UuidBytes,
    pub ordinal: u32,
    pub child_id: Sym,
}

// manual encode/decode (newtype avoids orphan‑rule problems)
impl Encode for Child {
    fn encode<E: BinEncoder>(&self, e: &mut E) -> Result<(), EncodeError> {
        self.parent.encode(e)?;
        self.ordinal.encode(e)?;
        Encode::encode(&self.child_id, e)
    }
}
impl Decode<()> for Child {
    fn decode<D: BinDecoder<Context = ()>>(d: &mut D) -> Result<Self, DecodeError> {
        Ok(Self {
            parent: UuidBytes::decode(d)?,
            ordinal: <u32 as Decode<()>>::decode(d)?,
            child_id: <u64 as Decode<()>>::decode(d)?,
        })
    }
}

impl StorageEntity for Child {
    const STRUCT_VERSION: u8 = 0;

    fn primary_key(&self) -> Vec<u8> {
        let mut k = self.parent.as_bytes().to_vec();
        k.extend_from_slice(&self.ordinal.to_le_bytes());
        k
    }

    fn table_def() -> StaticTableDef {
        AVAIL_CHILDREN_TBL
    }
    fn meta_table_def() -> StaticTableDef {
        AVAIL_CHILDREN_META_TBL
    }

    fn load_and_migrate(src: &[u8]) -> Result<Self, StorageError> {
        bincode::decode_from_slice(
            src.get(1..).unwrap_or_default(),
            bincode::config::standard(),
        )
        .map(|(v, _)| v)
        .map_err(|e| StorageError::Bincode(e.to_string()))
    }

    fn indexes() -> &'static [IndexSpec<Self>] {
        &[]
    }
}

/*────────────────── DigestChunk row ─────────────────────────────*/

#[derive(Clone, Debug)]
pub struct DigestChunk {
    pub parent: UuidBytes,
    pub chunk_no: u32,
    pub bytes: Vec<u8>, // encoded Vec<Cell>
}

impl Encode for DigestChunk {
    fn encode<E: BinEncoder>(&self, e: &mut E) -> Result<(), EncodeError> {
        self.parent.encode(e)?;
        self.chunk_no.encode(e)?;
        self.bytes.encode(e)
    }
}
impl Decode<()> for DigestChunk {
    fn decode<D: BinDecoder<Context = ()>>(d: &mut D) -> Result<Self, DecodeError> {
        Ok(Self {
            parent: UuidBytes::decode(d)?,
            chunk_no: <u32 as Decode<()>>::decode(d)?,
            bytes: Vec::<u8>::decode(d)?,
        })
    }
}

impl StorageEntity for DigestChunk {
    const STRUCT_VERSION: u8 = 0;

    fn primary_key(&self) -> Vec<u8> {
        let mut k = self.parent.as_bytes().to_vec();
        k.extend_from_slice(&self.chunk_no.to_le_bytes());
        k
    }

    fn table_def() -> StaticTableDef {
        AVAIL_DIG_CHUNK_TBL
    }
    fn meta_table_def() -> StaticTableDef {
        AVAIL_DIG_CHUNK_META_TBL
    }

    fn load_and_migrate(src: &[u8]) -> Result<Self, StorageError> {
        bincode::decode_from_slice(
            src.get(1..).unwrap_or_default(),
            bincode::config::standard(),
        )
        .map(|(v, _)| v)
        .map_err(|e| StorageError::Bincode(e.to_string()))
    }

    fn indexes() -> &'static [IndexSpec<Self>] {
        &[]
    }
}

/*──────────────── prefix‑scan helper ───────────────────────────*/

async fn scan_prefix<E: StorageEntity>(
    db: &KuramotoDb,
    prefix: &[u8],
) -> Result<Vec<E>, StorageError> {
    let mut hi = prefix.to_vec();
    hi.push(0xFF);
    db.range_by_pk::<E>(prefix, &hi, None).await
}

/*────────────────────────── ChildSet ───────────────────────────*/

#[derive(Clone, Debug, Encode, Decode)]
pub struct ChildSet {
    pub parent: UuidBytes,
    pub children: Vec<Sym>,
}

impl ChildSet {
    /* open ------------------------------------------------------*/
    pub async fn open(db: &KuramotoDb, parent: UuidBytes) -> Result<Self, StorageError> {
        let rows = scan_prefix::<Child>(db, parent.as_bytes()).await?;
        Ok(Self {
            parent,
            children: rows.into_iter().map(|r| r.child_id).collect(),
        })
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.children.len()
    }

    /* add/remove child -----------------------------------------*/
    pub async fn add_child(&mut self, db: &KuramotoDb, sym: Sym) -> Result<(), StorageError> {
        let ord = self.children.len() as u32;
        db.put(Child {
            parent: self.parent,
            ordinal: ord,
            child_id: sym,
        })
        .await?;
        self.children.push(sym);
        self.rebuild_chunks(db).await
    }

    pub async fn remove_child(&mut self, db: &KuramotoDb, sym: Sym) -> Result<(), StorageError> {
        if let Some(pos) = self.children.iter().position(|&s| s == sym) {
            self.children.remove(pos);
            // delete the row with that ordinal
            let mut pk = self.parent.as_bytes().to_vec();
            pk.extend_from_slice(&(pos as u32).to_le_bytes());
            db.delete::<Child>(&pk).await?;

            self.rebuild_child_rows(db).await?;
            self.rebuild_chunks(db).await
        } else {
            Ok(())
        }
    }

    /* stream any coded cell ------------------------------------*/
    pub async fn cell(&self, db: &KuramotoDb, idx: usize) -> Result<Cell, StorageError> {
        let stored_limit = CELLS_PER_CHUNK * STORED_CHUNKS;

        // ── 1️⃣ fast path: inside the persisted window
        if idx < stored_limit {
            let chunk_no = (idx / CELLS_PER_CHUNK) as u32;
            let offset = idx % CELLS_PER_CHUNK;

            let mut pk = self.parent.as_bytes().to_vec();
            pk.extend_from_slice(&chunk_no.to_le_bytes());

            let chunk = db.get_data::<DigestChunk>(&pk).await?;
            let vec_cells: Vec<Cell> =
                bincode::decode_from_slice(&chunk.bytes, bincode::config::standard())
                    .map_err(|e| StorageError::Bincode(e.to_string()))?
                    .0;

            return Ok(vec_cells[offset].clone());
        }

        // ── 2️⃣ on‑the‑fly stream beyond persisted window
        let mut enc = Enc::new(&self.children);
        enc.seek(idx);
        Ok(enc.next_coded())
    }

    /*──────────── private helpers ───────────────────────────────*/

    async fn rebuild_child_rows(&self, db: &KuramotoDb) -> Result<(), StorageError> {
        for row in scan_prefix::<Child>(db, self.parent.as_bytes()).await? {
            db.delete::<Child>(&row.primary_key()).await?;
        }
        for (i, sym) in self.children.iter().enumerate() {
            db.put(Child {
                parent: self.parent,
                ordinal: i as u32,
                child_id: *sym,
            })
            .await?;
        }
        Ok(())
    }

    async fn rebuild_chunks(&self, db: &KuramotoDb) -> Result<(), StorageError> {
        // purge old
        for row in scan_prefix::<DigestChunk>(db, self.parent.as_bytes()).await? {
            db.delete::<DigestChunk>(&row.primary_key()).await?;
        }

        let mut enc = Enc::new(&self.children);
        for chunk_no in 0..STORED_CHUNKS {
            let mut buf = Vec::with_capacity(CELLS_PER_CHUNK);
            for _ in 0..CELLS_PER_CHUNK {
                buf.push(enc.next_coded());
            }
            Self::store_chunk(db, self.parent, chunk_no as u32, &buf).await?;
        }
        Ok(())
    }

    async fn store_chunk(
        db: &KuramotoDb,
        parent: UuidBytes,
        no: u32,
        cells: &[Cell],
    ) -> Result<(), StorageError> {
        let bytes = bincode::encode_to_vec(cells, bincode::config::standard()).unwrap();
        debug_assert!(bytes.len() <= DIGEST_CHUNK_BYTES, "digest chunk overflow");
        db.put(DigestChunk {
            parent,
            chunk_no: no,
            bytes,
        })
        .await
    }
}

/*───────────────────────────────────────────────────────────────*/
/* tests                                                         */
/*───────────────────────────────────────────────────────────────*/
#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rand::Rng;
    use redb::WriteTransaction;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::runtime::Runtime;

    /*── glue for db bootstrap ─────────────────────────────*/
    use crate::{
        WriteBatch, clock::MockClock, plugins::Plugin, plugins::communication::router::Router,
    };

    struct NoopMw;

    #[async_trait]
    impl Plugin for NoopMw {
        async fn before_update(
            &self,
            _db: &KuramotoDb,
            _txn: &WriteTransaction,
            _batch: &mut WriteBatch,
        ) -> Result<(), StorageError> {
            Ok(())
        }
    }

    fn fresh_db(rt: &Runtime) -> Arc<KuramotoDb> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("redb.ub");
        rt.block_on(async {
            let clock = Arc::new(MockClock::new(0));
            let router = Router::new(Default::default(), clock.clone());
            KuramotoDb::new(
                path.to_str().unwrap(),
                clock,
                vec![
                    router.clone() as Arc<dyn crate::plugins::Plugin>,
                    Arc::new(NoopMw) as Arc<dyn crate::plugins::Plugin>, // NoopMw is a plain value
                ],
            )
            .await
        })
    }

    /*──────── serialisation sanity check ────────────────*/
    #[test]
    fn serial_roundtrip() {
        let parent = UuidBytes::new();
        let child = Child {
            parent,
            ordinal: 7,
            child_id: 42,
        };
        let bytes = child.to_bytes();
        let decoded = Child::load_and_migrate(&bytes).unwrap();
        assert_eq!(decoded.parent, parent);
        assert_eq!(decoded.ordinal, 7);
        assert_eq!(decoded.child_id, 42);
    }

    /*──────── add/remove + reopen round-trip ─────────────*/
    #[test]
    fn childset_add_remove() {
        let rt = Runtime::new().unwrap();
        let db = fresh_db(&rt);
        db.create_table_and_indexes::<Child>().unwrap();
        db.create_table_and_indexes::<DigestChunk>().unwrap();

        rt.block_on(async {
            let parent = UuidBytes::new();
            let mut cs = ChildSet::open(&db, parent).await.unwrap();
            cs.add_child(&db, 10).await.unwrap();
            cs.add_child(&db, 11).await.unwrap();
            assert_eq!(cs.count(), 2);

            cs.remove_child(&db, 10).await.unwrap();
            assert_eq!(cs.count(), 1);

            let cs2 = ChildSet::open(&db, parent).await.unwrap();
            assert_eq!(cs2.count(), 1);
            assert_eq!(cs2.children[0], 11);
        });
    }

    /*──────── persisted chunk matches encoder ───────────*/
    #[test]
    fn chunk_matches_encoder() {
        let rt = Runtime::new().unwrap();
        let db = fresh_db(&rt);
        db.create_table_and_indexes::<Child>().unwrap();
        db.create_table_and_indexes::<DigestChunk>().unwrap();

        rt.block_on(async {
            let mut rng = rand::rng();
            let kids: Vec<u64> = (0..50).map(|_| rng.random()).collect();

            let mut cs = ChildSet::open(&db, UuidBytes::new()).await.unwrap();
            for k in &kids {
                cs.add_child(&db, *k).await.unwrap();
            }

            let mut enc = Enc::new(&kids);
            for i in 0..(CELLS_PER_CHUNK * STORED_CHUNKS + 10) {
                let expected = enc.next_coded();
                let got = cs.cell(&db, i).await.unwrap();
                assert_eq!(expected.count, got.count, "mismatch at {}", i);
            }
        });
    }
}
