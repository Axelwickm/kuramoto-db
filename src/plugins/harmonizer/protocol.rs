use async_trait::async_trait;
use bincode::{Decode, Encode};
use std::sync::Arc;

use crate::{
    plugins::{
        communication::{
            router::{Handler, Router},
            transports::PeerId,
        },
        fnv1a_16,
        harmonizer::{child_set::Cell, range_cube::RangeCube},
    },
    uuid_bytes::UuidBytes,
};

pub const PROTO_HARMONIZER: u16 = fnv1a_16("kuramoto.middleware.harmonizer.v1");

#[derive(Clone, Debug, Encode, Decode)]
pub struct AvailabilityHeader {
    pub availability_id: UuidBytes,
    pub level: u16,
    pub range: RangeCube,
    pub child_count: u32,
    pub small_digest: u64,
    pub complete: bool,
}

/*──────── Requests ────────*/
#[derive(Clone, Debug, Encode, Decode)]
pub struct GetChildrenByRange {
    pub range: RangeCube,
    pub cursor: Option<Vec<u8>>,
    pub max: u32,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct GetChildrenByAvailability {
    pub availability_id: UuidBytes,
    pub cursor: Option<Vec<u8>>,
    pub max: u32,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct GetSymbolsByAvailability {
    pub availability_id: UuidBytes,
    pub max_cells: u32,
    pub cursor: Option<u32>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct UpdateWithAttestation {
    pub table: String,
    pub pk: Vec<u8>,
    pub range: RangeCube,
    pub local_availability_id: UuidBytes,
    pub level: u16,
    pub child_count: u32,
    pub small_digest: u64,
    pub entity_bytes: Vec<u8>,
}

/*──────── Responses ───────*/
#[derive(Clone, Debug, Encode, Decode)]
pub struct ChildrenResponse {
    pub items: Vec<(u64 /*Sym*/, Vec<u8>)>,
    pub next: Option<Vec<u8>>,
    pub headers: Vec<AvailabilityHeader>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct SymbolsResponse {
    pub cells: Vec<Cell>,
    pub next: Option<u32>,
    pub header: Option<AvailabilityHeader>,
}

#[derive(Clone, Debug, Encode, Decode)]
pub enum ReconcileAsk {
    Symbols {
        availability_id: UuidBytes,
        max_cells: u32,
    },
    Children {
        availability_id: Option<UuidBytes>,
        range: Option<RangeCube>,
        hint_count: Option<u32>,
    },
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct UpdateResponse {
    pub accepted: bool,
    pub need: Option<ReconcileAsk>,
    pub headers: Vec<AvailabilityHeader>,
}

/*──────── Envelope payloads ───────*/
#[derive(Clone, Debug, Encode, Decode)]
pub enum HarmonizerMsg {
    GetChildrenByRange(GetChildrenByRange),
    GetChildrenByAvailability(GetChildrenByAvailability),
    GetSymbolsByAvailability(GetSymbolsByAvailability),
    UpdateWithAttestation(UpdateWithAttestation),
}

#[derive(Clone, Debug, Encode, Decode)]
pub enum HarmonizerResp {
    Children(ChildrenResponse),
    Symbols(SymbolsResponse),
    Update(UpdateResponse),
}

/*──────── Handler wiring ───────*/
pub struct HarmonizerProto {
    // add watchlist/LRU here later if you want; keep v1 minimal
}

impl HarmonizerProto {
    // helper: encode a response
    fn ok(resp: HarmonizerResp) -> Result<Vec<u8>, String> {
        bincode::encode_to_vec(resp, bincode::config::standard()).map_err(|e| e.to_string())
    }
}

#[async_trait]
impl Handler for HarmonizerProto {
    async fn on_request(&self, _peer: PeerId, body: &[u8]) -> Result<Vec<u8>, String> {
        let (msg, _) =
            bincode::decode_from_slice::<HarmonizerMsg, _>(body, bincode::config::standard())
                .map_err(|e| e.to_string())?;

        match msg {
            HarmonizerMsg::GetChildrenByRange(req) => {
                // TODO: implement paging + headers; placeholder empty result
                Self::ok(HarmonizerResp::Children(ChildrenResponse {
                    items: vec![],
                    next: None,
                    headers: vec![],
                }))
            }
            HarmonizerMsg::GetChildrenByAvailability(req) => {
                // TODO: stream direct children for availability_id
                Self::ok(HarmonizerResp::Children(ChildrenResponse {
                    items: vec![],
                    next: None,
                    headers: vec![],
                }))
            }
            HarmonizerMsg::GetSymbolsByAvailability(req) => {
                // TODO: return coded cells (RIBLT) for availability_id
                Self::ok(HarmonizerResp::Symbols(SymbolsResponse {
                    cells: vec![],
                    next: None,
                    header: None,
                }))
            }
            HarmonizerMsg::UpdateWithAttestation(req) => {
                // TODO: apply entity; compute targeted reconcile ask + headers
                Self::ok(HarmonizerResp::Update(UpdateResponse {
                    accepted: true,
                    need: None,
                    headers: vec![],
                }))
            }
        }
    }

    async fn on_notify(&self, _peer: PeerId, _body: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

/*──────── Public helper: register with router ───────*/
pub fn register_harmonizer_protocol(router: Arc<Router>) {
    router.set_handler(PROTO_HARMONIZER, Arc::new(HarmonizerProto {}));
}
