//! Prueba P2P de 2 nodos en el mismo proceso (usa relays N0 reales).
//! A crea biblioteca + escribe entrada; B la importa vía ticket y debe verla.
//! Valida el flujo "marcar para transmitir" a nivel metadatos + contenido.

use std::str::FromStr;
use std::time::Duration;

use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use iroh_docs::store::Query;
use p2p_nube_core::Node;

#[tokio::test]
async fn dos_nodos_sincronizan_biblioteca() -> anyhow::Result<()> {
    let dir_a = tempfile::tempdir()?;
    let dir_b = tempfile::tempdir()?;
    let a = Node::spawn(dir_a.path().to_path_buf()).await?;
    let b = Node::spawn(dir_b.path().to_path_buf()).await?;

    // A crea biblioteca y comparte ticket (simula "marcar carpeta para transmitir").
    let author_a = a.docs.author_default().await?;
    let doc_a = a.docs.create().await?;
    let ticket_s = doc_a
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?
        .to_string();
    assert!(!ticket_s.is_empty());

    // B importa el ticket (como hará el móvil).
    let ticket = iroh_docs::DocTicket::from_str(&ticket_s)?;
    let doc_b = b.docs.import(ticket).await?;

    // A publica una entrada StreamOnly (hash ficticio pequeño vía set_bytes,
    // que además guarda el contenido como blob y se sincroniza por defecto).
    let key = "anime/cap01.mkv";
    let value = br#"{"policy":"stream_only","note":"solo metadato+contenido sync"}"#;
    let hash_a = doc_a
        .set_bytes(author_a, key.as_bytes().to_vec(), value.to_vec())
        .await?;

    // B espera hasta ver la entrada (poll con timeout: relays pueden tardar).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let entry = loop {
        let found = doc_b
            .get_one(Query::single_latest_per_key().key_exact(key.as_bytes()))
            .await?;
        if let Some(e) = found {
            break e;
        }
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!("timeout: B no vio la entrada de A en 60s (¿relays bloqueados?)");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(entry.content_hash(), hash_a);
    assert_eq!(entry.content_len() as usize, value.len());

    // El contenido también debe haberse sincronizado (DownloadPolicy por defecto = todo).
    let bytes = b
        .blobs_store
        .blobs()
        .get_bytes(entry.content_hash())
        .await?;
    assert_eq!(&bytes[..], &value[..]);

    a.endpoint.close().await;
    b.endpoint.close().await;
    Ok(())
}
