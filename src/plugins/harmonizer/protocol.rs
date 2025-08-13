use async_trait::async_trait;
use bincode::{Decode, Encode};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

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

/*────────────────────────── Types on the wire ─────────────────────────*/

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

/*────────────────────────── Inbox interface ──────────────────────────*/
/* The protocol handler forwards into this channel owned by Harmonizer */

pub enum ProtoCommand {
    /// Router made a request. We forward the decoded message and expect a response.
    HandleRequest {
        peer: PeerId,
        msg: HarmonizerMsg,
        respond: oneshot::Sender<Result<HarmonizerResp, String>>,
    },
    /// Router delivered a notify. No response path.
    HandleNotify { peer: PeerId, msg: HarmonizerMsg },
}

/*──────────────────────── Protocol handler ───────────────────────────*/

pub struct HarmonizerProto {
    inbox: mpsc::Sender<ProtoCommand>,
}

impl HarmonizerProto {
    pub fn new(inbox: mpsc::Sender<ProtoCommand>) -> Self {
        Self { inbox }
    }

    fn ok_bytes(resp: HarmonizerResp) -> Result<Vec<u8>, String> {
        bincode::encode_to_vec(resp, bincode::config::standard()).map_err(|e| e.to_string())
    }
}

#[async_trait]
impl Handler for HarmonizerProto {
    async fn on_request(&self, peer: PeerId, body: &[u8]) -> Result<Vec<u8>, String> {
        let (msg, _) =
            bincode::decode_from_slice::<HarmonizerMsg, _>(body, bincode::config::standard())
                .map_err(|e| e.to_string())?;

        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(ProtoCommand::HandleRequest {
                peer,
                msg,
                respond: tx,
            })
            .await
            .map_err(|_| "inbox closed".to_string())?;

        let resp = rx.await.map_err(|_| "inbox dropped".to_string())??;
        Self::ok_bytes(resp)
    }

    async fn on_notify(&self, peer: PeerId, body: &[u8]) -> Result<(), String> {
        let (msg, _) =
            bincode::decode_from_slice::<HarmonizerMsg, _>(body, bincode::config::standard())
                .map_err(|e| e.to_string())?;

        self.inbox
            .send(ProtoCommand::HandleNotify { peer, msg })
            .await
            .map_err(|_| "inbox closed".to_string())
    }
}

/*──────── Public helper: register with router ───────*/
pub fn register_harmonizer_protocol(router: Arc<Router>, inbox: mpsc::Sender<ProtoCommand>) {
    router.set_handler(PROTO_HARMONIZER, Arc::new(HarmonizerProto::new(inbox)));
}
