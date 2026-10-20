//! Durable references and generation-fenced physical reclamation.
use super::*;
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcReference {
    pub node_id: String,
    pub session_id: String,
    pub reference_id: String,
    pub inode_id: Option<InodeId>,
    pub publication: bool,
    pub chunks: Vec<ChunkObject>,
    pub retired_at_unix_ms: Option<u64>,
}
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcState {
    pub generation: u64,
    pub chunks: Vec<ChunkId>,
    pub waiting_nodes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct UpdateGcReference {
    pub node_id: String,
    pub session_id: String,
    pub reference_id: String,
    pub inode_id: Option<InodeId>,
    pub version_id: Option<FileVersionId>,
    pub publication: bool,
    pub chunks: Vec<ChunkObject>,
    pub release: bool,
    pub admission_deadline_unix_ms: u64,
}
