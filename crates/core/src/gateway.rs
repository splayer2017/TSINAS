use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use iroh_blobs::{api::Store as BlobsStore, Hash};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use crate::db::Db;

/// Estado compartido del gateway: streaming + UI web + API (solo loopback/tailnet).
#[derive(Clone)]
pub struct Gateway {
    pub store: BlobsStore,
    pub db: Db,
    pub endpoint_id: String,
}

const INDEX_HTML: &str = include_str!("../web/index.html");
const MANIFEST_JSON: &str = include_str!("../web/manifest.json");
const SW_JS: &str = include_str!("../web/sw.js");

impl Gateway {
    pub fn router(store: BlobsStore, db: Db, endpoint_id: String) -> Router {
        let state = Arc::new(Gateway {
            store,
            db,
            endpoint_id,
        });
        Router::new()
            .route("/", get(index))
            .route("/manifest.json", get(manifest))
            .route("/sw.js", get(sw))
            .route("/health", get(health))
            .route("/api/files", get(api_files))
            .route("/api/info", get(api_info))
            .route("/stream/:hash", get(stream).head(stream_head))
            .with_state(state)
    }

    /// Sirve en 127.0.0.1:puerto efímero; devuelve la base URL y el handle.
    /// Para exponer al celular: `tailscale serve --bg PORT` (ver README).
    pub async fn serve_loopback(
        store: BlobsStore,
        db: Db,
        endpoint_id: String,
    ) -> anyhow::Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr: SocketAddr = listener.local_addr()?;
        let app = Self::router(store, db, endpoint_id);
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok((format!("http://{addr}"), handle))
    }

    /// Sirve HTTPS con el cert de Tailscale en una dirección fija
    /// (modo Dev: bindear la IP tailnet + puerto 37491).
    /// Devuelve el handle del servidor; el bind falla con mensaje claro si el
    /// puerto está ocupado.
    pub async fn serve_tls(
        store: BlobsStore,
        db: Db,
        endpoint_id: String,
        addr: SocketAddr,
        cert: &std::path::Path,
        key: &std::path::Path,
    ) -> anyhow::Result<(tokio::task::JoinHandle<()>, SocketAddr)> {
        // rustls no elige proveedor solo si conviven ring (iroh) y aws-lc-rs
        // (axum-server): fijamos ring explícitamente (idempotente).
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "no se pudo cargar el cert TLS {cert:?}: {e} (ejecuta `tailscale cert` primero)"
                )
            })?;
        let listener = std::net::TcpListener::bind(addr).map_err(|e| {
            anyhow::anyhow!("no se pudo bindear {addr}: {e} (¿puerto ocupado por otra instancia?)")
        })?;
        let bound: SocketAddr = listener.local_addr()?;
        let app = Self::router(store, db, endpoint_id);
        let handle = tokio::spawn(async move {
            if let Err(e) = axum_server::from_tcp_rustls(listener, config)
                .serve(app.into_make_service())
                .await
            {
                tracing::error!("servidor TLS terminado: {e}");
            }
        });
        Ok((handle, bound))
    }
}

async fn health() -> &'static str {
    "ok"
}

async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        INDEX_HTML,
    )
}

async fn manifest() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/manifest+json")],
        MANIFEST_JSON,
    )
}

async fn sw() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/javascript")], SW_JS)
}

/// Lista de archivos marcados para transmitir (proyección SQLite local).
async fn api_files(State(state): State<Arc<Gateway>>) -> impl IntoResponse {
    match state.db.list_files() {
        Ok(rows) => {
            let arr: Vec<_> = rows
                .iter()
                .map(|r| {
                    json!({
                        "path": r.path,
                        "hash": r.hash,
                        "size": r.size,
                        "mime": r.mime,
                        "policy": r.policy.as_str(),
                        "host_id": r.host_id,
                    })
                })
                .collect();
            (StatusCode::OK, Json(json!(arr))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("db: {e}")).into_response(),
    }
}

async fn api_info(State(state): State<Arc<Gateway>>) -> impl IntoResponse {
    let files = state.db.list_files().map(|v| v.len()).unwrap_or(0);
    (
        StatusCode::OK,
        Json(json!({
            "endpoint_id": state.endpoint_id,
            "files": files,
            "version": env!("CARGO_PKG_VERSION"),
        })),
    )
        .into_response()
}

/// MIME registrado en la DB para un hash, o `application/octet-stream`.
fn mime_for(state: &Gateway, hash_s: &str) -> String {
    match state.db.get_by_hash(hash_s) {
        Ok(Some(row)) if !row.mime.is_empty() => row.mime,
        _ => "application/octet-stream".to_string(),
    }
}

/// HEAD /stream/<hash>: mismos headers que GET pero sin body.
/// Lo piden VLC/mpv para tamaño y tipo antes de reproducir.
async fn stream_head(
    State(state): State<Arc<Gateway>>,
    Path(hash_s): Path<String>,
) -> impl IntoResponse {
    let hash = match Hash::from_str(&hash_s) {
        Ok(h) => h,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "hash inválido").into_response();
        }
    };
    if state.store.blobs().has(hash).await.unwrap_or(false) == false {
        return (StatusCode::NOT_FOUND, "blob no disponible en este nodo").into_response();
    }
    let total = match state.store.blobs().status(hash).await {
        Ok(iroh_blobs::api::blobs::BlobStatus::Complete { size }) => size,
        Ok(_) => {
            return (StatusCode::NOT_FOUND, "blob parcial en este nodo").into_response();
        }
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("status: {e}")).into_response();
        }
    };
    let mime = mime_for(&state, &hash_s);
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
    resp_headers.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    resp_headers.insert(header::CONTENT_LENGTH, total.to_string().parse().unwrap());
    if let Ok(v) = mime.parse() {
        resp_headers.insert(header::CONTENT_TYPE, v);
    }
    (StatusCode::OK, resp_headers).into_response()
}

/// GET /stream/<hash-blake3-hex> con soporte `Range: bytes=start-end`.
/// Lee solo del store local (lo que el host tiene pineado). El visor
/// StreamOnly no persiste a disco: el player consume esta URL y mantiene
/// los chunks en RAM. `Cache-Control: no-store`.
/// También sirve como URL directa para VLC/mpv/ExoPlayer (MKV sin transcode).
async fn stream(
    State(state): State<Arc<Gateway>>,
    Path(hash_s): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let hash = match Hash::from_str(&hash_s) {
        Ok(h) => h,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "hash inválido").into_response();
        }
    };
    if state.store.blobs().has(hash).await.unwrap_or(false) == false {
        return (StatusCode::NOT_FOUND, "blob no disponible en este nodo").into_response();
    }

    // Tamaño total vía status (BlobReader no soporta SeekFrom::End).
    let total = match state.store.blobs().status(hash).await {
        Ok(iroh_blobs::api::blobs::BlobStatus::Complete { size }) => size,
        Ok(_) => {
            return (StatusCode::NOT_FOUND, "blob parcial en este nodo").into_response();
        }
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("status: {e}")).into_response();
        }
    };
    let mut reader = state.store.blobs().reader(hash);

    let (start, end) = match parse_range(headers.get(header::RANGE), total) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let len = end - start + 1;

    if let Err(e) = reader.seek(std::io::SeekFrom::Start(start)).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("seek: {e}")).into_response();
    }
    let limited = reader.take(len);
    let stream = ReaderStream::new(limited);
    let body = Body::from_stream(stream);

    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
    resp_headers.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    resp_headers.insert(header::CONTENT_LENGTH, len.to_string().parse().unwrap());
    let mime = mime_for(&state, &hash_s);
    resp_headers.insert(
        header::CONTENT_TYPE,
        mime.parse()
            .unwrap_or_else(|_| "application/octet-stream".parse().unwrap()),
    );

    if headers.contains_key(header::RANGE) {
        resp_headers.insert(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{total}").parse().unwrap(),
        );
        (StatusCode::PARTIAL_CONTENT, resp_headers, body).into_response()
    } else {
        (StatusCode::OK, resp_headers, body).into_response()
    }
}

/// Devuelve (start, end_inclusive). Sin Range => (0, total-1).
fn parse_range(
    header_val: Option<&axum::http::HeaderValue>,
    total: u64,
) -> Result<(u64, u64), axum::response::Response> {
    if total == 0 {
        return Err((StatusCode::NO_CONTENT, "vacío").into_response());
    }
    let Some(v) = header_val else {
        return Ok((0, total - 1));
    };
    let s = v.to_str().unwrap_or("");
    // Solo soportamos un rango simple: bytes=start-end / bytes=start- / bytes=-suffix
    let s = s.strip_prefix("bytes=").unwrap_or("");
    let (a, b) = s.split_once('-').unwrap_or(("", ""));
    let start: u64 = if a.is_empty() {
        // suffix: últimos N bytes
        let suffix: u64 = b.parse().unwrap_or(0);
        if suffix == 0 {
            return Ok((0, total - 1));
        }
        total.saturating_sub(suffix)
    } else {
        a.parse()
            .map_err(|_| (StatusCode::RANGE_NOT_SATISFIABLE, "rango inválido").into_response())?
    };
    let end: u64 = if b.is_empty() {
        total - 1
    } else {
        b.parse()
            .map_err(|_| (StatusCode::RANGE_NOT_SATISFIABLE, "rango inválido").into_response())?
    };
    if start >= total || end >= total || start > end {
        return Err((
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(
                header::CONTENT_RANGE.to_string(),
                format!("bytes */{total}"),
            )],
            "rango fuera de límites",
        )
            .into_response());
    }
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn gateway_sirve_rangos() -> anyhow::Result<()> {
        use crate::db::FileRow;
        use crate::policy::Policy;
        use iroh_blobs::store::mem::MemStore;
        let mem = MemStore::new();
        let data = b"hola-mundo-streaming-1234567890".to_vec();
        let tag = mem.add_slice(data.clone()).await?;
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        db.upsert_file(&FileRow {
            path: "hola.bin".into(),
            hash: tag.hash.to_string(),
            size: data.len() as u64,
            mime: "application/octet-stream".into(),
            policy: Policy::StreamOnly,
            host_id: "test".into(),
        })?;

        let (base, _h) = Gateway::serve_loopback(store, db, "test-endpoint".into()).await?;
        let client = reqwest::Client::new();
        // Completo
        let r = client
            .get(format!("{base}/stream/{}", tag.hash))
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        let body = r.bytes().await?;
        assert_eq!(&body[..], &data[..]);
        // Rango
        let r = client
            .get(format!("{base}/stream/{}", tag.hash))
            .header("Range", "bytes=0-3")
            .send()
            .await?;
        assert_eq!(r.status(), 206);
        assert_eq!(r.headers()["content-range"], "bytes 0-3/31");
        let body = r.bytes().await?;
        assert_eq!(&body[..], b"hola");
        Ok(())
    }

    #[tokio::test]
    async fn gateway_content_type_y_head() -> anyhow::Result<()> {
        use crate::db::FileRow;
        use crate::policy::Policy;
        use iroh_blobs::store::mem::MemStore;
        let mem = MemStore::new();
        let data = b"fake-mkv-bytes-1234".to_vec();
        let tag = mem.add_slice(data.clone()).await?;
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        db.upsert_file(&FileRow {
            path: "cap07.mkv".into(),
            hash: tag.hash.to_string(),
            size: data.len() as u64,
            mime: "video/x-matroska".into(),
            policy: Policy::StreamOnly,
            host_id: "test".into(),
        })?;

        let (base, _h) = Gateway::serve_loopback(store, db, "test-endpoint".into()).await?;
        let client = reqwest::Client::new();
        // GET devuelve el MIME real, no octet-stream fijo.
        let r = client
            .get(format!("{base}/stream/{}", tag.hash))
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        assert_eq!(r.headers()["content-type"], "video/x-matroska");
        assert_eq!(r.headers()["accept-ranges"], "bytes");
        // HEAD: mismos headers, tamaño total, sin body.
        let r = client
            .head(format!("{base}/stream/{}", tag.hash))
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        assert_eq!(r.headers()["content-type"], "video/x-matroska");
        assert_eq!(
            r.headers()["content-length"],
            data.len().to_string().as_str()
        );
        assert_eq!(r.bytes().await?.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn gateway_sirve_ui_y_api() -> anyhow::Result<()> {
        use crate::db::FileRow;
        use crate::policy::Policy;
        use iroh_blobs::store::mem::MemStore;
        let mem = MemStore::new();
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        db.upsert_file(&FileRow {
            path: "video.mp4".into(),
            hash: "00".repeat(32),
            size: 10,
            mime: "video/mp4".into(),
            policy: Policy::StreamOnly,
            host_id: "h1".into(),
        })?;

        let (base, _h) = Gateway::serve_loopback(store, db, "abc123".into()).await?;
        let client = reqwest::Client::new();
        // UI
        let r = client.get(format!("{base}/")).send().await?;
        assert_eq!(r.status(), 200);
        let html = r.text().await?;
        assert!(html.contains("<video"), "la UI debe incluir <video>");
        assert!(html.contains("/api/files"));
        assert!(html.contains("VLC"), "la UI debe ofrecer abrir en VLC");
        assert!(
            html.contains("type=video"),
            "el intent VLC debe llevar type=video/*"
        );
        assert!(
            html.contains("S.title"),
            "el intent VLC debe llevar S.title"
        );
        // API lista
        let body = client
            .get(format!("{base}/api/files"))
            .send()
            .await?
            .text()
            .await?;
        let files: serde_json::Value = serde_json::from_str(&body)?;
        assert_eq!(files[0]["path"], "video.mp4");
        assert_eq!(files[0]["policy"], "stream_only");
        // API info
        let body = client
            .get(format!("{base}/api/info"))
            .send()
            .await?
            .text()
            .await?;
        let info: serde_json::Value = serde_json::from_str(&body)?;
        assert_eq!(info["endpoint_id"], "abc123");
        assert_eq!(info["files"], 1);
        // Manifest + SW
        let r = client.get(format!("{base}/manifest.json")).send().await?;
        assert_eq!(r.status(), 200);
        let r = client.get(format!("{base}/sw.js")).send().await?;
        assert_eq!(r.status(), 200);
        Ok(())
    }

    #[tokio::test]
    async fn gateway_sirve_tls() -> anyhow::Result<()> {
        use iroh_blobs::store::mem::MemStore;
        // Cert autofirmado efímero solo para el test (en prod: `tailscale cert`).
        let dir = tempfile::tempdir()?;
        let cert_pem;
        let key_pem;
        {
            let key = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()])?;
            cert_pem = key.cert.pem();
            key_pem = key.signing_key.serialize_pem();
        }
        let cert_path = dir.path().join("t.crt");
        let key_path = dir.path().join("t.key");
        std::fs::write(&cert_path, cert_pem)?;
        std::fs::write(&key_path, key_pem)?;

        let mem = MemStore::new();
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse()?;
        let (_h, bound) =
            Gateway::serve_tls(store, db, "tls-test".into(), addr, &cert_path, &key_path).await?;
        // Pequeña espera a que el acceptor esté listo.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()?;
        let r = client.get(format!("https://{bound}/health")).send().await?;
        assert_eq!(r.status(), 200);
        assert_eq!(r.text().await?, "ok");
        let body = client
            .get(format!("https://{bound}/api/info"))
            .send()
            .await?
            .text()
            .await?;
        let info: serde_json::Value = serde_json::from_str(&body)?;
        assert_eq!(info["endpoint_id"], "tls-test");
        Ok(())
    }
}
