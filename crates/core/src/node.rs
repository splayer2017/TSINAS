use std::path::PathBuf;

use iroh::{endpoint::presets, protocol::Router, Endpoint};
use iroh_blobs::{api::Store as BlobsStore, BlobsProtocol};
use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use iroh_docs::protocol::Docs;
use iroh_gossip::{net::Gossip, proto::TopicId};
use sha2::{Digest, Sha256};

use crate::db::Db;

/// Nodo P2P: endpoint iroh + blobs + docs + gossip + índice SQLite local.
#[derive(Debug, Clone)]
pub struct Node {
    pub endpoint: Endpoint,
    pub blobs_store: BlobsStore,
    pub docs: Docs,
    pub gossip: Gossip,
    pub db: Db,
    pub data_dir: PathBuf,
    // Router debe vivir mientras el nodo vive; lo guardamos en Arc para Clone.
    _router: std::sync::Arc<Router>,
}

impl Node {
    /// Arranca (o reabre) un nodo persistente en `data_dir`.
    pub async fn spawn(data_dir: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
        // redb (docs) y FsStore exigen que el directorio padre exista.
        std::fs::create_dir_all(data_dir.join("blobs"))?;
        std::fs::create_dir_all(data_dir.join("docs"))?;
        let endpoint = Endpoint::bind(presets::N0).await?;

        // Store de blobs en disco (PC host) — en móvil se usará igual
        // pero sin hacer `add_path` de lo marcado StreamOnly.
        let fs_store = iroh_blobs::store::fs::FsStore::load(data_dir.join("blobs")).await?;
        let blobs_store: BlobsStore = fs_store.into();
        let blobs = BlobsProtocol::new(&blobs_store, None);

        let gossip = Gossip::builder().spawn(endpoint.clone());

        let docs = Docs::persistent(data_dir.join("docs"))
            .spawn(endpoint.clone(), blobs_store.clone(), gossip.clone())
            .await?;

        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs)
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(iroh_docs::ALPN, docs.clone())
            .spawn();

        let db = Db::open(&data_dir.join("index.sqlite3"))?;

        Ok(Self {
            endpoint,
            blobs_store,
            docs,
            gossip,
            db,
            data_dir,
            _router: std::sync::Arc::new(router),
        })
    }

    pub fn endpoint_id(&self) -> String {
        self.endpoint.id().to_string()
    }

    /// Crea un Doc nuevo (una "biblioteca", p.ej. el baúl) y devuelve su ticket.
    pub async fn create_library(&self) -> anyhow::Result<String> {
        let doc = self.docs.create().await?;
        let ticket = doc
            .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
            .await?;
        Ok(ticket.to_string())
    }

    /// Topic de gossip determinista por biblioteca (para presencia/live-sync).
    pub fn topic_for(library: &str) -> TopicId {
        let mut h = Sha256::new();
        h.update(b"p2p-nube/v1/");
        h.update(library.as_bytes());
        let digest = h.finalize();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&digest[..32]);
        TopicId::from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn arranca_nodo_efimero() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let node = Node::spawn(dir.path().to_path_buf()).await?;
        assert!(!node.endpoint_id().is_empty());
        node.endpoint.close().await;
        Ok(())
    }
}
