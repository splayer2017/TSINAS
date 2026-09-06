use p2p_nube_core::Policy;
use serde::{Deserialize, Serialize};

/// Manifiesto de un módulo conectable (tu "construye tu entorno").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub id: String,
    pub name: String,
    pub version: String,
    pub policies: Vec<Policy>,
}

pub trait Module {
    fn manifest() -> Manifest;
}

/// MVP: baúl + streaming video.
pub struct VaultModule;

impl Module for VaultModule {
    fn manifest() -> Manifest {
        Manifest {
            id: "vault".into(),
            name: "Baúl + streaming".into(),
            version: "0.1.0".into(),
            policies: vec![Policy::HostOnly, Policy::StreamOnly, Policy::Mirror],
        }
    }
}

/// Metadato que se publica en iroh-docs por fichero compartido.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedFile {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub mime: String,
    pub policy: Policy,
    pub host_id: String,
}

impl SharedFile {
    pub fn to_doc_value(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    pub fn from_doc_value(v: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(v)?)
    }
}

pub fn guess_mime(path: &str) -> String {
    if path.ends_with(".mkv") {
        "video/x-matroska"
    } else if path.ends_with(".mp4") {
        "video/mp4"
    } else if path.ends_with(".webm") {
        "video/webm"
    } else if path.ends_with(".avi") {
        "video/x-msvideo"
    } else {
        "application/octet-stream"
    }
    .to_string()
}
