use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, Router,
};
use iroh_blobs::{api::Store as BlobsStore, Hash};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use crate::db::Db;
use crate::library::{display_base_for, Jobs, Library};

/// Estado compartido del gateway: streaming + UI web + API (tailnet privada).
#[derive(Clone)]
pub struct Gateway {
    pub store: BlobsStore,
    pub db: Db,
    pub endpoint_id: String,
    pub library: Arc<Library>,
    pub jobs: Jobs,
    pub mime_cache: Arc<std::sync::RwLock<std::collections::HashMap<String, String>>>,
    pub poster_dir: PathBuf,
}

/// Directorio de portadas: si el staging es `<data>/staging`, usar
/// `<data>/posters` (persistente); si no, temporal del sistema.
fn poster_dir_for(library: &Library) -> PathBuf {
    let staging = library.staging_dir();
    if staging.file_name().map(|s| s == "staging").unwrap_or(false) {
        if let Some(parent) = staging.parent() {
            return parent.join("posters");
        }
    }
    std::env::temp_dir().join(format!("p2p-nube-posters-{}", std::process::id()))
}

const INDEX_HTML: &str = include_str!("../web/index.html");
const MANIFEST_JSON: &str = include_str!("../web/manifest.json");
const SW_JS: &str = include_str!("../web/sw.js");

impl Gateway {
    pub fn router(store: BlobsStore, db: Db, endpoint_id: String) -> Router {
        Self::router_with_media_roots(store, db, endpoint_id, Vec::new())
    }

    /// Router con raíces permitidas para los endpoints de escritura.
    /// Vacío = sin restricción (tests, red local de confianza).
    pub fn router_with_media_roots(
        store: BlobsStore,
        db: Db,
        endpoint_id: String,
        media_roots: Vec<PathBuf>,
    ) -> Router {
        let library = Arc::new(Library::new(store, db, endpoint_id).with_media_roots(media_roots));
        Self::router_with_library(library)
    }

    /// Router a partir de una instancia de Library configurada.
    pub fn router_with_library(library: Arc<Library>) -> Router {
        let poster_dir = poster_dir_for(&library);
        let state = Arc::new(Gateway {
            store: library.store().clone(),
            db: library.db().clone(),
            endpoint_id: library.endpoint_id().to_string(),
            library,
            jobs: Jobs::default(),
            mime_cache: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            poster_dir,
        });

        // Capa de timeout de 30s en endpoints de API para protección slowloris
        let api_routes = Router::new()
            .route("/files", get(api_files).patch(api_rename_file))
            .route("/info", get(api_info))
            .route("/library/add-file", post(api_add_file))
            .route("/library/scan-folder", post(api_scan_folder))
            .route("/library/jobs/:id", get(api_job))
            .route("/files/:hash", delete(api_remove_file))
            .route(
                "/collections",
                get(api_collections).post(api_create_collection),
            )
            .route(
                "/collections/:id",
                get(api_collection_detail)
                    .patch(api_update_collection)
                    .delete(api_delete_collection),
            )
            .route(
                "/collections/:id/items",
                post(api_add_item).delete(api_remove_item),
            )
            .route("/collections/:id/items/order", put(api_reorder_items))
            .route("/collections/:id/children", post(api_create_child))
            .route("/collections/:id/children/order", put(api_reorder_children))
            .route("/collections/:id/poster", post(api_upload_poster))
            .layer(tower_http::timeout::TimeoutLayer::new(
                std::time::Duration::from_secs(30),
            ));

        Router::new()
            .route("/", get(index))
            .route("/manifest.json", get(manifest))
            .route("/sw.js", get(sw))
            .route("/health", get(health))
            .nest("/api", api_routes)
            .route("/api/library/upload", post(api_upload))
            .route("/posters/:file", get(serve_poster))
            .route("/stream/:hash", get(stream).head(stream_head))
            // Sin límite global de 2 MB: la subida de vídeos lo necesita.
            // `api_upload` impone su propio tope (8 GiB) mientras escribe.
            .layer(DefaultBodyLimit::disable())
            .with_state(state)
    }

    /// Sirve en 127.0.0.1:puerto efímero; devuelve la base URL y el handle.
    /// Para exponer al celular: `tailscale serve --bg PORT` (ver README).
    pub async fn serve_loopback(
        store: BlobsStore,
        db: Db,
        endpoint_id: String,
    ) -> anyhow::Result<(String, tokio::task::JoinHandle<()>)> {
        Self::serve_loopback_with_roots(store, db, endpoint_id, Vec::new()).await
    }

    pub async fn serve_loopback_with_roots(
        store: BlobsStore,
        db: Db,
        endpoint_id: String,
        media_roots: Vec<PathBuf>,
    ) -> anyhow::Result<(String, tokio::task::JoinHandle<()>)> {
        let library = Arc::new(Library::new(store, db, endpoint_id).with_media_roots(media_roots));
        Self::serve_loopback_with_library(library).await
    }

    pub async fn serve_loopback_with_library(
        library: Arc<Library>,
    ) -> anyhow::Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr: SocketAddr = listener.local_addr()?;
        let app = Self::router_with_library(library);
        let handle = tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await;
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
        Self::serve_tls_with_roots(store, db, endpoint_id, addr, cert, key, Vec::new()).await
    }

    pub async fn serve_tls_with_roots(
        store: BlobsStore,
        db: Db,
        endpoint_id: String,
        addr: SocketAddr,
        cert: &std::path::Path,
        key: &std::path::Path,
        media_roots: Vec<PathBuf>,
    ) -> anyhow::Result<(tokio::task::JoinHandle<()>, SocketAddr)> {
        let library = Arc::new(Library::new(store, db, endpoint_id).with_media_roots(media_roots));
        Self::serve_tls_with_library(library, addr, cert, key).await
    }

    pub async fn serve_tls_with_library(
        library: Arc<Library>,
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
        let app = Self::router_with_library(library);
        let handle = tokio::spawn(async move {
            if let Err(e) = axum_server::from_tcp_rustls(listener, config)
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
            {
                tracing::error!("servidor TLS terminado: {e}");
            }
        });
        Ok((handle, bound))
    }
}

/// Sin roles de usuario: la red tailnet ya es privada (solo tus
/// dispositivos) y cifrada (WireGuard + QUIC/TLS de iroh). Todos los
/// endpoints de escritura (`POST /api/library/*`, `DELETE /api/files/:hash`)
/// están abiertos a cualquier cliente conectado, sea el PC o el móvil.

#[derive(Debug, Deserialize)]
struct AddFileReq {
    path: String,
}

#[derive(Debug, Deserialize)]
struct ScanFolderReq {
    path: String,
    #[serde(default)]
    recursive: bool,
    /// Agrupación semiautomática: si viene título, se crea/busca la
    /// colección y se adjuntan los añadidos.
    #[serde(default)]
    collection_title: String,
    #[serde(default)]
    collection_kind: String,
    #[serde(default)]
    season: String,
    /// Destino ya existente (hoja: temporada/película o serie sin hijas).
    /// Si viene, tiene prioridad sobre `collection_title`/`season`.
    #[serde(default)]
    target_collection_id: String,
}

#[derive(Debug, Deserialize)]
struct CreateCollectionReq {
    #[serde(default)]
    kind: String,
    title: String,
    #[serde(default)]
    parent_id: String,
}

#[derive(Debug, Deserialize)]
struct CreateChildReq {
    #[serde(default)]
    kind: String,
    title: String,
}

#[derive(Debug, Deserialize)]
struct UpdateCollectionReq {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    poster_url: Option<String>,
    #[serde(default)]
    kind: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AddItemReq {
    file_path: String,
    #[serde(default)]
    season: String,
    #[serde(default)]
    label: String,
    #[serde(default)]
    pos: i64,
}

#[derive(Debug, Deserialize)]
struct RemoveItemQuery {
    path: String,
}

#[derive(Debug, Deserialize)]
struct ReorderReq {
    #[serde(default)]
    order: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RenameFileReq {
    path: String,
    #[serde(default)]
    title: String,
}

#[derive(Debug, Serialize)]
struct JobView {
    id: String,
    status: String,
    total: usize,
    done: usize,
    added: Vec<String>,
    errors: Vec<String>,
}

/// POST /api/library/add-file {path} — importa una ruta del servidor.
async fn api_add_file(
    State(state): State<Arc<Gateway>>,
    Json(req): Json<AddFileReq>,
) -> impl IntoResponse {
    match state.library.add_file(&req.path, None).await {
        Ok(a) => (
            StatusCode::OK,
            Json(json!({"path": a.path, "hash": a.hash, "size": a.size, "mime": a.mime})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /api/library/scan-folder {path, recursive} — escanea una carpeta
/// del servidor en segundo plano fuera de los hilos de Tokio (HIGH-03).
async fn api_scan_folder(
    State(state): State<Arc<Gateway>>,
    Json(req): Json<ScanFolderReq>,
) -> impl IntoResponse {
    let lib = state.library.clone();
    let path = req.path.clone();
    let recursive = req.recursive;

    // HIGH-03: Despachar el recorrido síncrono del filesystem a un hilo bloqueante
    let res = tokio::task::spawn_blocking(move || {
        let candidates = lib.scan_candidates(&path, recursive)?;
        let base = std::fs::canonicalize(&path)
            .ok()
            .map(|p| display_base_for(&p))
            .unwrap_or_default();
        let paths: Vec<String> = candidates
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        Ok::<_, anyhow::Error>((base, paths))
    })
    .await;

    let (base, paths) = match res {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("error en tarea de escaneo: {e}")})),
            )
                .into_response();
        }
    };

    let job_id = state.jobs.create(paths.len());
    if !paths.is_empty() {
        let lib = state.library.clone();
        let jobs = state.jobs.clone();
        let db = state.db.clone();
        let jid = job_id.clone();
        // Agrupación semiautomática: crear la raíz y, si hay temporada,
        // la hija (los episodios viven en las hojas). Si el cliente manda
        // `target_collection_id`, se usa una hoja YA CREADA (verificada).
        let target_id = req.target_collection_id.trim().to_string();
        let auto_col: Option<(String, String)> = if !target_id.is_empty() {
            match db.get_collection(&target_id) {
                Ok(Some(col)) => {
                    let kids = db.list_children(&col.id).unwrap_or_default();
                    if !kids.is_empty() {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "elige una temporada o película dentro de la serie"})),
                        )
                            .into_response();
                    }
                    let root = if col.parent_id.is_empty() {
                        col.id.clone()
                    } else {
                        col.parent_id.clone()
                    };
                    Some((
                        col.id.clone(),
                        if root.is_empty() {
                            col.id.clone()
                        } else {
                            root
                        },
                    ))
                }
                Ok(None) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": "la colección destino no existe"})),
                    )
                        .into_response();
                }
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": e.to_string()})),
                    )
                        .into_response();
                }
            }
        } else if !req.collection_title.trim().is_empty() {
            match db.create_collection(&req.collection_kind, &req.collection_title, "") {
                Ok(root) => {
                    let target = if !req.season.trim().is_empty() {
                        let child_kind = if Db::normalize_kind(&req.collection_kind) == "movie" {
                            "movie"
                        } else {
                            "season"
                        };
                        match db.create_collection(child_kind, &req.season, &root.id) {
                            Ok(kid) => kid.id,
                            Err(_) => root.id.clone(),
                        }
                    } else {
                        root.id.clone()
                    };
                    Some((target, root.id))
                }
                Err(_) => None,
            }
        } else {
            None
        };
        // Devolver el id al cliente vía job? Se devuelve abajo en la respuesta
        // extendida; el worker solo adjunta.
        let auto_col_worker = auto_col.clone();
        tokio::spawn(async move {
            let mut pos: i64 = 0;
            for p in paths {
                pos += 1;
                match lib.add_file(&p, Some(&base)).await {
                    Ok(a) => {
                        if let Some((cid, _)) = &auto_col_worker {
                            let _ = db.add_item(cid, &a.path, "", "", pos);
                        }
                        jobs.progress(&jid, Some(a.path), None)
                    }
                    Err(e) => jobs.progress(
                        &jid,
                        None,
                        Some(format!(
                            "{}: {e}",
                            PathBuf::from(&p)
                                .file_name()
                                .unwrap_or_default()
                                .to_string_lossy()
                        )),
                    ),
                }
            }
        });
        let collection_id = auto_col.map(|(_, root)| root).unwrap_or_default();
        return (
            StatusCode::ACCEPTED,
            Json(json!({"job_id": job_id, "found": state.jobs.get(&job_id).map(|j| j.total).unwrap_or(0), "collection_id": collection_id})),
        )
            .into_response();
    }
    (
        StatusCode::ACCEPTED,
        Json(json!({"job_id": job_id, "found": state.jobs.get(&job_id).map(|j| j.total).unwrap_or(0)})),
    )
        .into_response()
}

/// PATCH /api/files {path, title} — renombra solo el título visible.
async fn api_rename_file(
    State(state): State<Arc<Gateway>>,
    Json(req): Json<RenameFileReq>,
) -> impl IntoResponse {
    match state.db.set_title(&req.path, &req.title) {
        Ok(true) => (StatusCode::OK, Json(json!({"renamed": true}))).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "archivo no listado"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// GET /api/collections — lista plana con nº de items propios + hijas.
/// El frontend agrupa por `parent_id` para el árbol.
async fn api_collections(State(state): State<Arc<Gateway>>) -> impl IntoResponse {
    match state.db.list_collections() {
        Ok(cols) => {
            let out: Vec<serde_json::Value> = cols
                .iter()
                .map(|c| {
                    let (n, kids) = state
                        .db
                        .collection_detail(&c.id)
                        .map(|d| {
                            d.map(|v| {
                                (
                                    v.items.len()
                                        + v.children.iter().map(|k| k.items.len()).sum::<usize>(),
                                    v.children.len(),
                                )
                            })
                            .unwrap_or((0, 0))
                        })
                        .unwrap_or((0, 0));
                    json!({"id": c.id, "kind": c.kind, "title": c.title,
                           "poster_url": c.poster_url, "poster_file": c.poster_file,
                           "created_at": c.created_at, "parent_id": c.parent_id,
                           "pos": c.pos, "items": n, "children": kids})
                })
                .collect();
            (StatusCode::OK, Json(out)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /api/collections {kind, title, parent_id?}
async fn api_create_collection(
    State(state): State<Arc<Gateway>>,
    Json(req): Json<CreateCollectionReq>,
) -> impl IntoResponse {
    match state
        .db
        .create_collection(&req.kind, &req.title, &req.parent_id)
    {
        Ok(c) => (StatusCode::CREATED, Json(c)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /api/collections/:id/children {kind, title} — crea una
/// sub-colección (temporada o película) dentro de una serie.
async fn api_create_child(
    State(state): State<Arc<Gateway>>,
    Path(id): Path<String>,
    Json(req): Json<CreateChildReq>,
) -> impl IntoResponse {
    match state.db.create_collection(&req.kind, &req.title, &id) {
        Ok(c) => (StatusCode::CREATED, Json(c)).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// GET /api/collections/:id — detalle con items.
async fn api_collection_detail(
    State(state): State<Arc<Gateway>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.collection_detail(&id) {
        Ok(Some(d)) => (StatusCode::OK, Json(d)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "colección no existe"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// PATCH /api/collections/:id {title?, poster_url?, kind?}
async fn api_update_collection(
    State(state): State<Arc<Gateway>>,
    Path(id): Path<String>,
    Json(req): Json<UpdateCollectionReq>,
) -> impl IntoResponse {
    // Cada campo se actualiza por separado (dos llamadas si hacen falta).
    if let Some(t) = req.title.as_deref() {
        match state.db.update_collection(&id, Some(t), None, None) {
            Ok(true) => {}
            Ok(false) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "colección no existe"})),
                )
                    .into_response();
            }
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    }
    if let Some(u) = req.poster_url.as_deref() {
        match state.db.update_collection(&id, None, Some(u), None) {
            Ok(true) => {}
            Ok(false) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "colección no existe"})),
                )
                    .into_response();
            }
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    }
    if let Some(k) = req.kind.as_deref() {
        match state.db.update_collection(&id, None, None, Some(k)) {
            Ok(true) => {}
            Ok(false) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": "colección no existe"})),
                )
                    .into_response();
            }
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    }
    match state.db.get_collection(&id) {
        Ok(Some(c)) => (StatusCode::OK, Json(c)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "colección no existe"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// DELETE /api/collections/:id — borra la agrupación, nunca los archivos.
async fn api_delete_collection(
    State(state): State<Arc<Gateway>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.db.delete_collection(&id) {
        Ok(true) => (StatusCode::OK, Json(json!({"removed": true}))).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "colección no existe"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /api/collections/:id/items {file_path, season?, label?, pos?}
/// Mueve el archivo a esta colección (sale de cualquier otra).
async fn api_add_item(
    State(state): State<Arc<Gateway>>,
    Path(id): Path<String>,
    Json(req): Json<AddItemReq>,
) -> impl IntoResponse {
    match state
        .db
        .add_item(&id, &req.file_path, &req.season, &req.label, req.pos)
    {
        Ok(()) => (StatusCode::OK, Json(json!({"added": true}))).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// DELETE /api/collections/:id/items?path=... — saca el archivo de la colección.
async fn api_remove_item(
    State(state): State<Arc<Gateway>>,
    Path(id): Path<String>,
    Query(q): Query<RemoveItemQuery>,
) -> impl IntoResponse {
    match state.db.remove_item(&id, &q.path) {
        Ok(true) => (StatusCode::OK, Json(json!({"removed": true}))).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "item no existe"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// PUT /api/collections/:id/items/order {order:[file_path...]} — orden visual
/// persistente de los episodios dentro de una hoja.
async fn api_reorder_items(
    State(state): State<Arc<Gateway>>,
    Path(id): Path<String>,
    Json(req): Json<ReorderReq>,
) -> impl IntoResponse {
    match state.db.reorder_items(&id, &req.order) {
        Ok(()) => (StatusCode::OK, Json(json!({"reordered": true}))).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// PUT /api/collections/:id/children/order {order:[child_id...]} — orden visual
/// persistente de temporadas/películas dentro de una serie raíz.
async fn api_reorder_children(
    State(state): State<Arc<Gateway>>,
    Path(id): Path<String>,
    Json(req): Json<ReorderReq>,
) -> impl IntoResponse {
    match state.db.reorder_children(&id, &req.order) {
        Ok(()) => (StatusCode::OK, Json(json!({"reordered": true}))).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /api/collections/:id/poster (multipart campo `poster`) — sube la
/// portada al servidor (JPG/PNG/WebP ≤10MB). También se admite URL externa
/// vía PATCH poster_url (modo "Ambas").
async fn api_upload_poster(
    State(state): State<Arc<Gateway>>,
    Path(id): Path<String>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    if state.db.get_collection(&id).ok().flatten().is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "colección no existe"})),
        )
            .into_response();
    }
    const MAX_POSTER: u64 = 10 * 1024 * 1024;
    if let Err(e) = tokio::fs::create_dir_all(&state.poster_dir).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("no se pudo preparar portadas: {e}")})),
        )
            .into_response();
    }
    while let Ok(Some(mut field)) = multipart.next_field().await {
        if field.name() != Some("poster") {
            continue;
        }
        let fname = field
            .file_name()
            .map(|s| s.to_string())
            .unwrap_or_default()
            .to_lowercase();
        let ext = if fname.ends_with(".png") {
            "png"
        } else if fname.ends_with(".webp") {
            "webp"
        } else {
            "jpg"
        };
        let ctype = field
            .content_type()
            .map(|s| s.to_string())
            .unwrap_or_default();
        if !(ctype.starts_with("image/") || fname.is_empty()) && !fname.is_empty() {
            // Si el navegador no mandó content-type imagen y la extensión es
            // rara, lo rechazamos.
            if !["jpg", "png", "webp"].contains(&ext) {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "la portada debe ser JPG/PNG/WebP"})),
                )
                    .into_response();
            }
        }
        let mut bytes: Vec<u8> = Vec::new();
        loop {
            match field.chunk().await {
                Ok(Some(chunk)) => {
                    bytes.extend_from_slice(&chunk);
                    if bytes.len() as u64 > MAX_POSTER {
                        return (
                            StatusCode::PAYLOAD_TOO_LARGE,
                            Json(json!({"error": "portada demasiado grande (tope 10 MB)"})),
                        )
                            .into_response();
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": format!("subida interrumpida: {e}")})),
                    )
                        .into_response();
                }
            }
        }
        if bytes.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "portada vacía"})),
            )
                .into_response();
        }
        // Validación mínima por magic bytes.
        let ok_magic = bytes.starts_with(&[0xFF, 0xD8, 0xFF])
            || bytes.starts_with(&[0x89, b'P', b'N', b'G'])
            || bytes.starts_with(b"RIFF") && bytes.len() > 12 && &bytes[8..12] == b"WEBP";
        if !ok_magic {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "la portada debe ser JPG/PNG/WebP"})),
            )
                .into_response();
        }
        // Nombre seguro: solo el id + extensión (sin path traversal).
        let safe_id: String = id
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
            .take(64)
            .collect();
        if safe_id.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "id inválido"})),
            )
                .into_response();
        }
        let file_name = format!("{safe_id}.{ext}");
        let dest = state.poster_dir.join(&file_name);
        if let Err(e) = tokio::fs::write(&dest, &bytes).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("no se pudo guardar: {e}")})),
            )
                .into_response();
        }
        let public = format!("/posters/{file_name}");
        let _ = state.db.set_poster_file(&id, &public);
        // La subida local tiene prioridad: limpia la URL externa.
        let _ = state.db.update_collection(&id, None, Some(""), None);
        return (StatusCode::OK, Json(json!({"poster": public}))).into_response();
    }
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "falta el campo `poster` (multipart/form-data)"})),
    )
        .into_response()
}

/// GET /posters/:file — sirve portadas subidas (cache largo, tailnet privada).
async fn serve_poster(
    State(state): State<Arc<Gateway>>,
    Path(file): Path<String>,
) -> impl IntoResponse {
    if file.contains('/') || file.contains('\\') || file.contains("..") {
        return (StatusCode::BAD_REQUEST, "nombre inválido").into_response();
    }
    let lower = file.to_lowercase();
    if !(lower.ends_with(".jpg")
        || lower.ends_with(".jpeg")
        || lower.ends_with(".png")
        || lower.ends_with(".webp"))
    {
        return (StatusCode::BAD_REQUEST, "solo portadas").into_response();
    }
    let path = state.poster_dir.join(&file);
    match tokio::fs::read(&path).await {
        Ok(bytes) => {
            let mime = if lower.ends_with(".png") {
                "image/png"
            } else if lower.ends_with(".webp") {
                "image/webp"
            } else {
                "image/jpeg"
            };
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, mime),
                    (header::CACHE_CONTROL, "public, max-age=86400"),
                ],
                bytes,
            )
                .into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "sin portada").into_response(),
    }
}

/// POST /api/library/upload (multipart, campo `file`) — subida directa
/// desde el navegador o el móvil.
/// BLOCK-03: Utiliza staging en directorio persistente con guardia RAII TempStagedFile.
async fn api_upload(
    State(state): State<Arc<Gateway>>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    const MAX_UPLOAD: u64 = 8 * 1024 * 1024 * 1024;
    let staging_dir = state.library.staging_dir();
    if let Err(e) = tokio::fs::create_dir_all(staging_dir).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("no se pudo preparar la subida: {e}")})),
        )
            .into_response();
    }
    while let Ok(Some(mut field)) = multipart.next_field().await {
        let name = field
            .file_name()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "archivo".into());
        if field.name() != Some("file") {
            continue;
        }

        // Nombre único con timestamp + contador atómico para evitar colisiones
        static UPLOAD_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let count = UPLOAD_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let safe_name = PathBuf::from(&name)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "archivo".into());
        let staged_path = staging_dir.join(format!("{}-{}-{}", now, count, safe_name));
        let staged = crate::library::TempStagedFile::new(staged_path);

        let mut out = match tokio::fs::File::create(staged.path()).await {
            Ok(f) => f,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("no se pudo recibir la subida: {e}")})),
                )
                    .into_response();
            }
        };
        let mut written: u64 = 0;
        loop {
            match field.chunk().await {
                Ok(Some(chunk)) => {
                    written += chunk.len() as u64;
                    if written > MAX_UPLOAD {
                        // staged se elimina automáticamente al salir del scope por Drop
                        return (
                            StatusCode::PAYLOAD_TOO_LARGE,
                            Json(json!({"error": "archivo demasiado grande (tope 8 GiB)"})),
                        )
                            .into_response();
                    }
                    use tokio::io::AsyncWriteExt;
                    if let Err(e) = out.write_all(&chunk).await {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": format!("error al guardar la subida: {e}")})),
                        )
                            .into_response();
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": format!("subida interrumpida: {e}")})),
                    )
                        .into_response();
                }
            }
        }
        let _ = out.flush().await;
        drop(out);
        match state.library.add_upload(&name, staged.path()).await {
            Ok(a) => {
                staged.commit();
                return (
                    StatusCode::OK,
                    Json(json!({"path": a.path, "hash": a.hash, "size": a.size, "mime": a.mime})),
                )
                    .into_response();
            }
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    }
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "falta el campo `file` (multipart/form-data)"})),
    )
        .into_response()
}

/// GET /api/library/jobs/:id — progreso del escaneo (lectura, abierto).
async fn api_job(State(state): State<Arc<Gateway>>, Path(id): Path<String>) -> impl IntoResponse {
    match state.jobs.get(&id) {
        Some(j) => (
            StatusCode::OK,
            Json(json!(JobView {
                id: j.id,
                status: j.status,
                total: j.total,
                done: j.done,
                added: j.added,
                errors: j.errors,
            })),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "job desconocido"})),
        )
            .into_response(),
    }
}

/// DELETE /api/files/:hash — quita de la lista y des-pinea.
/// Nunca borra el archivo original del usuario, solo la copia del store.
async fn api_remove_file(
    State(state): State<Arc<Gateway>>,
    Path(hash_s): Path<String>,
) -> impl IntoResponse {
    if Hash::from_str(&hash_s).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "hash inválido"})),
        )
            .into_response();
    }
    match state.library.remove_by_hash(&hash_s).await {
        Ok(true) => (StatusCode::OK, Json(json!({"removed": true}))).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "hash no listado"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
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
        Ok(rows) => (StatusCode::OK, Json(rows)).into_response(),
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

/// MIME registrado en la DB para un hash, con caché en RAM (HIGH-02).
fn mime_for(state: &Gateway, hash_s: &str) -> String {
    if let Ok(cache) = state.mime_cache.read() {
        if let Some(m) = cache.get(hash_s) {
            return m.clone();
        }
    }
    let mime = match state.db.get_by_hash(hash_s) {
        Ok(Some(row)) if !row.mime.is_empty() => row.mime,
        _ => "application/octet-stream".to_string(),
    };
    if let Ok(mut cache) = state.mime_cache.write() {
        cache.insert(hash_s.to_string(), mime.clone());
    }
    mime
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
    if !state.store.blobs().has(hash).await.unwrap_or(false) {
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
    if !state.store.blobs().has(hash).await.unwrap_or(false) {
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
        Err(err) => return err.into_response(),
    };
    let len = end - start + 1;

    if let Err(e) = reader.seek(std::io::SeekFrom::Start(start)).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("seek: {e}")).into_response();
    }
    let limited = reader.take(len);
    let stream = ReaderStream::with_capacity(limited, 64 * 1024);
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

#[derive(Debug, PartialEq, Eq)]
pub enum RangeError {
    NoContent,
    Invalid,
    OutOfBounds(u64),
}

impl IntoResponse for RangeError {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::NoContent => (StatusCode::NO_CONTENT, "vacío").into_response(),
            Self::Invalid => (StatusCode::RANGE_NOT_SATISFIABLE, "rango inválido").into_response(),
            Self::OutOfBounds(total) => (
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(
                    header::CONTENT_RANGE.to_string(),
                    format!("bytes */{total}"),
                )],
                "rango fuera de límites",
            )
                .into_response(),
        }
    }
}

/// Devuelve (start, end_inclusive). Sin Range => (0, total-1).
pub fn parse_range(
    header_val: Option<&axum::http::HeaderValue>,
    total: u64,
) -> Result<(u64, u64), RangeError> {
    if total == 0 {
        return Err(RangeError::NoContent);
    }
    let Some(v) = header_val else {
        return Ok((0, total - 1));
    };
    let s = v.to_str().map_err(|_| RangeError::Invalid)?;
    // Solo soportamos un rango simple: bytes=start-end / bytes=start- / bytes=-suffix
    let s = s.strip_prefix("bytes=").ok_or(RangeError::Invalid)?;
    let (a, b) = s.split_once('-').ok_or(RangeError::Invalid)?;
    let (start, end) = if a.is_empty() {
        // suffix: últimos N bytes (bytes=-N)
        let suffix: u64 = b.parse().map_err(|_| RangeError::Invalid)?;
        if suffix == 0 {
            return Ok((0, total - 1));
        }
        (total.saturating_sub(suffix), total - 1)
    } else {
        let start: u64 = a.parse().map_err(|_| RangeError::Invalid)?;
        let end: u64 = if b.is_empty() {
            total - 1
        } else {
            b.parse().map_err(|_| RangeError::Invalid)?
        };
        (start, end)
    };
    if start >= total || end >= total || start > end {
        return Err(RangeError::OutOfBounds(total));
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
            tag: String::new(),
            title: String::new(),
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
            tag: String::new(),
            title: String::new(),
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
            tag: String::new(),
            title: String::new(),
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
        assert!(
            html.contains("Colecciones"),
            "la UI debe tener vista de colecciones"
        );
        assert!(
            html.contains("Copiar URL"),
            "la UI debe permitir copiar URL para VLC"
        );
        assert!(
            html.contains("Ver aquí") || html.contains("Reproducir"),
            "la UI debe ofrecer ver en la web"
        );
        assert!(
            html.contains("scan-mode") && html.contains("scan-root"),
            "Añadir/Escanear debe ofrecer serie ya creada"
        );
        assert!(
            html.contains("file-root") && html.contains("up-root"),
            "Añadir por ruta y Subir deben ofrecer destino existente"
        );
        assert!(
            html.contains(r#"scan-exist-row').style.display=ex?'':'none'"#),
            "el conmutador debe mostrar la fila existente solo en modo existente"
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
    async fn api_colecciones_rename_poster_y_scan_auto() -> anyhow::Result<()> {
        use iroh_blobs::store::mem::MemStore;
        let mem = MemStore::new();
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        let (base, _h) = Gateway::serve_loopback(store, db, "cols-test".into()).await?;
        let client = reqwest::Client::new();

        // Carpeta con 2 mkv.
        let dir = tempfile::tempdir()?;
        for n in ["cap01.mkv", "cap02.mkv"] {
            std::fs::write(dir.path().join(n), format!("contenido-{n}"))?;
        }
        let dir_s = dir.path().to_string_lossy().to_string();

        // Scan con agrupación semiautomática.
        let body = client
            .post(format!("{base}/api/library/scan-folder"))
            .body(
                serde_json::json!({"path": dir_s, "recursive": false,
                    "collection_title": "Frieren", "collection_kind": "series", "season": "T1"})
                .to_string(),
            )
            .header("Content-Type", "application/json")
            .send()
            .await?
            .text()
            .await?;
        let v: serde_json::Value = serde_json::from_str(&body)?;
        assert!(!v["collection_id"].as_str().unwrap_or("").is_empty());
        let job_id = v["job_id"].as_str().unwrap().to_string();
        let cid = v["collection_id"].as_str().unwrap().to_string();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let t = client
                .get(format!("{base}/api/library/jobs/{job_id}"))
                .send()
                .await?
                .text()
                .await?;
            let j: serde_json::Value = serde_json::from_str(&t)?;
            if j["status"] == "done" || j["status"] == "done_with_errors" {
                assert_eq!(j["done"], 2);
                break;
            }
            if tokio::time::Instant::now() > deadline {
                anyhow::bail!("timeout job: {j}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        // Detalle: la raíz trae 1 hija "T1" con 2 episodios.
        let det: serde_json::Value = serde_json::from_str(
            &client
                .get(format!("{base}/api/collections/{cid}"))
                .send()
                .await?
                .text()
                .await?,
        )?;
        assert_eq!(det["children"].as_array().unwrap().len(), 1);
        assert_eq!(det["children"][0]["title"], "T1");
        assert_eq!(det["children"][0]["items"].as_array().unwrap().len(), 2);

        // Crear sub-colección por API y renombrarla.
        let kid = det["children"][0]["id"].as_str().unwrap().to_string();
        let r = client
            .patch(format!("{base}/api/collections/{kid}"))
            .body(r#"{"title":"Temporada 1"}"#)
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        // Añadir directo a la raíz con hijas debe fallar (solo hojas).
        let probe = client
            .post(format!("{base}/api/collections/{cid}/items"))
            .body(serde_json::json!({"file_path": "x", "season": "", "label": ""}).to_string())
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(probe.status(), 400);

        // Rename solo título visible.
        let first_path = det["children"][0]["items"][0]["file_path"]
            .as_str()
            .unwrap()
            .to_string();
        let r = client
            .patch(format!("{base}/api/files"))
            .body(serde_json::json!({"path": first_path, "title": "Capítulo uno"}).to_string())
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        let files: serde_json::Value = serde_json::from_str(
            &client
                .get(format!("{base}/api/files"))
                .send()
                .await?
                .text()
                .await?,
        )?;
        let row = files
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["path"] == first_path)
            .unwrap();
        assert_eq!(row["title"], "Capítulo uno");

        // URL de portada externa.
        let r = client
            .patch(format!("{base}/api/collections/{cid}"))
            .body(r#"{"poster_url":"https://example.com/p.jpg"}"#)
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 200);

        // Subida de portada local (PNG mínimo 1x1).
        let png: Vec<u8> = vec![
            0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00,
            0x00, 0x90, 0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x08,
            0xD7, 0x63, 0xF8, 0xFF, 0xFF, 0x3F, 0x00, 0x05, 0xFE, 0x02, 0xFE, 0xDC, 0xCC, 0x59,
            0xE7, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
        ];
        let b = "limite-portada-1";
        let mut body_bytes: Vec<u8> = Vec::new();
        body_bytes.extend_from_slice(format!("--{b}\r\nContent-Disposition: form-data; name=\"poster\"; filename=\"p.png\"\r\nContent-Type: image/png\r\n\r\n").as_bytes());
        body_bytes.extend_from_slice(&png);
        body_bytes.extend_from_slice(format!("\r\n--{b}--\r\n").as_bytes());
        let r = client
            .post(format!("{base}/api/collections/{cid}/poster"))
            .header("Content-Type", format!("multipart/form-data; boundary={b}"))
            .body(body_bytes)
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        let pj: serde_json::Value = serde_json::from_str(&r.text().await?)?;
        let poster_url = pj["poster"].as_str().unwrap().to_string();
        assert!(poster_url.starts_with("/posters/"));
        // Sirve la portada.
        let r = client.get(format!("{base}{poster_url}")).send().await?;
        assert_eq!(r.status(), 200);
        assert!(r.headers()["content-type"].to_str()?.starts_with("image/"));

        // Mover el item a otra colección lo saca de la primera.
        let body = client
            .post(format!("{base}/api/collections"))
            .body(r#"{"kind":"movie","title":"Akira"}"#)
            .header("Content-Type", "application/json")
            .send()
            .await?
            .text()
            .await?;
        let c2: serde_json::Value = serde_json::from_str(&body)?;
        let c2id = c2["id"].as_str().unwrap().to_string();
        let r = client
            .post(format!("{base}/api/collections/{c2id}/items"))
            .body(
                serde_json::json!({"file_path": first_path, "season": "", "label": "Film"})
                    .to_string(),
            )
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        let det1: serde_json::Value = serde_json::from_str(
            &client
                .get(format!("{base}/api/collections/{cid}"))
                .send()
                .await?
                .text()
                .await?,
        )?;
        assert_eq!(det1["children"][0]["items"].as_array().unwrap().len(), 1);
        // Borrado recursivo: la hija desaparece pero el archivo sigue en Biblioteca.
        let r = client
            .delete(format!("{base}/api/collections/{cid}"))
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        let files: serde_json::Value = serde_json::from_str(
            &client
                .get(format!("{base}/api/files"))
                .send()
                .await?
                .text()
                .await?,
        )?;
        assert!(files
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["path"] == first_path));
        Ok(())
    }

    #[tokio::test]
    async fn api_scan_en_coleccion_existente() -> anyhow::Result<()> {
        use iroh_blobs::store::mem::MemStore;
        let mem = MemStore::new();
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        // Serie ya creada con Temporada 1.
        let root = db.create_collection("series", "Frieren", "")?;
        let t1 = db.create_collection("season", "Temporada 1", &root.id)?;
        let (base, _h) = Gateway::serve_loopback(store, db, "scan-exist".into()).await?;
        let client = reqwest::Client::new();
        async fn wait_done(client: &reqwest::Client, base: &str, job: &str) -> anyhow::Result<()> {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                let t = client
                    .get(format!("{base}/api/library/jobs/{job}"))
                    .send()
                    .await?
                    .text()
                    .await?;
                let j: serde_json::Value = serde_json::from_str(&t)?;
                if j["status"] == "done" || j["status"] == "done_with_errors" {
                    return Ok(());
                }
                if tokio::time::Instant::now() > deadline {
                    anyhow::bail!("timeout job: {j}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
        // Carpeta con 2 episodios nuevos.
        let dir = tempfile::tempdir()?;
        for n in ["cap03.mkv", "cap04.mkv"] {
            std::fs::write(dir.path().join(n), format!("contenido-{n}"))?;
        }
        let dir_s = dir.path().to_string_lossy().to_string();
        // Escaneo directo a la temporada YA CREADA.
        let body = client
            .post(format!("{base}/api/library/scan-folder"))
            .body(
                serde_json::json!({"path": dir_s, "recursive": false,
                    "target_collection_id": t1.id})
                .to_string(),
            )
            .header("Content-Type", "application/json")
            .send()
            .await?
            .text()
            .await?;
        let v: serde_json::Value = serde_json::from_str(&body)?;
        assert_eq!(v["collection_id"].as_str().unwrap_or(""), root.id);
        wait_done(&client, &base, v["job_id"].as_str().unwrap()).await?;
        // No se creó ninguna colección nueva: sigue habiendo 1 hija con 2 items.
        let cols: serde_json::Value = serde_json::from_str(
            &client
                .get(format!("{base}/api/collections"))
                .send()
                .await?
                .text()
                .await?,
        )?;
        assert_eq!(cols.as_array().unwrap().len(), 2);
        let det: serde_json::Value = serde_json::from_str(
            &client
                .get(format!("{base}/api/collections/{}", t1.id))
                .send()
                .await?
                .text()
                .await?,
        )?;
        assert_eq!(det["items"].as_array().unwrap().len(), 2);
        // La raíz con hijas como destino se rechaza (hay que elegir temporada).
        let r = client
            .post(format!("{base}/api/library/scan-folder"))
            .body(serde_json::json!({"path": dir_s, "target_collection_id": root.id}).to_string())
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 400);
        // Destino inexistente también se rechaza.
        let r = client
            .post(format!("{base}/api/library/scan-folder"))
            .body(serde_json::json!({"path": dir_s, "target_collection_id": "c_nope"}).to_string())
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 400);
        Ok(())
    }

    #[tokio::test]
    async fn api_reorden_items_y_children() -> anyhow::Result<()> {
        use crate::db::FileRow;
        use crate::policy::Policy;
        use iroh_blobs::store::mem::MemStore;
        let mem = MemStore::new();
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        for (p, h) in [("e01.mkv", "a1"), ("e02.mkv", "a2")] {
            db.upsert_file(&FileRow {
                path: p.into(),
                hash: h.into(),
                size: 5,
                mime: "video/x-matroska".into(),
                policy: Policy::StreamOnly,
                host_id: "h".into(),
                tag: format!("t{h}"),
                title: String::new(),
            })?;
        }
        let (base, _h) = Gateway::serve_loopback(store, db, "reorder-test".into()).await?;
        let client = reqwest::Client::new();
        async fn post_json(
            client: &reqwest::Client,
            url: String,
            v: serde_json::Value,
        ) -> anyhow::Result<serde_json::Value> {
            let t = client
                .post(url)
                .body(v.to_string())
                .header("Content-Type", "application/json")
                .send()
                .await?
                .text()
                .await?;
            Ok(serde_json::from_str(&t)?)
        }
        let serie = post_json(
            &client,
            format!("{base}/api/collections"),
            serde_json::json!({"kind":"series","title":"Re:Zero"}),
        )
        .await?;
        let sid = serie["id"].as_str().unwrap().to_string();
        let t1 = post_json(
            &client,
            format!("{base}/api/collections/{sid}/children"),
            serde_json::json!({"kind":"season","title":"T1"}),
        )
        .await?;
        let t2 = post_json(
            &client,
            format!("{base}/api/collections/{sid}/children"),
            serde_json::json!({"kind":"movie","title":"Peli"}),
        )
        .await?;
        let t1id = t1["id"].as_str().unwrap().to_string();
        let t2id = t2["id"].as_str().unwrap().to_string();
        for p in ["e01.mkv", "e02.mkv"] {
            let r = client
                .post(format!("{base}/api/collections/{t1id}/items"))
                .body(serde_json::json!({"file_path": p}).to_string())
                .header("Content-Type", "application/json")
                .send()
                .await?;
            assert_eq!(r.status(), 200);
        }
        // Reordena episodios.
        let r = client
            .put(format!("{base}/api/collections/{t1id}/items/order"))
            .body(serde_json::json!({"order": ["e02.mkv", "e01.mkv"]}).to_string())
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        // Reordena hijas.
        let r = client
            .put(format!("{base}/api/collections/{sid}/children/order"))
            .body(serde_json::json!({"order": [t2id, t1id]}).to_string())
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        let det: serde_json::Value = serde_json::from_str(
            &client
                .get(format!("{base}/api/collections/{sid}"))
                .send()
                .await?
                .text()
                .await?,
        )?;
        assert_eq!(det["children"][0]["id"], t2id);
        let kid: serde_json::Value = serde_json::from_str(
            &client
                .get(format!("{base}/api/collections/{t1id}"))
                .send()
                .await?
                .text()
                .await?,
        )?;
        assert_eq!(kid["items"][0]["file_path"], "e02.mkv");
        // Película hija única: el segundo archivo se rechaza.
        let r = client
            .post(format!("{base}/api/collections/{t2id}/items"))
            .body(serde_json::json!({"file_path": "e01.mkv"}).to_string())
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        let r = client
            .post(format!("{base}/api/collections/{t2id}/items"))
            .body(serde_json::json!({"file_path": "e02.mkv"}).to_string())
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 400);
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

    #[tokio::test]
    async fn api_biblioteca_vacia_arranque_sin_archivo() -> anyhow::Result<()> {
        use iroh_blobs::store::mem::MemStore;
        // Estado del arranque `p2p-serve` sin ARCHIVO: DB vacía, la UI debe
        // ofrecer la pestaña de añadir y la API lista vacía.
        let mem = MemStore::new();
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        let (base, _h) = Gateway::serve_loopback(store, db, "empty-test".into()).await?;
        let client = reqwest::Client::new();
        let body = client
            .get(format!("{base}/api/files"))
            .send()
            .await?
            .text()
            .await?;
        let files: serde_json::Value = serde_json::from_str(&body)?;
        assert_eq!(files.as_array().unwrap().len(), 0);
        let html = client.get(format!("{base}/")).send().await?.text().await?;
        assert!(html.contains(">Añadir<"));
        assert!(html.contains("Subir desde este dispositivo"));
        Ok(())
    }

    #[tokio::test]
    async fn api_add_scan_y_remove_desde_loopback() -> anyhow::Result<()> {
        use iroh_blobs::store::mem::MemStore;
        let mem = MemStore::new();
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        let (base, _h) = Gateway::serve_loopback(store, db, "escritura-test".into()).await?;
        let client = reqwest::Client::new();

        // Carpeta con 3 mkv + 1 txt (se ignora).
        let dir = tempfile::tempdir()?;
        for n in ["cap01.mkv", "cap02.mkv", "cap10.mkv", "leeme.txt"] {
            std::fs::write(dir.path().join(n), format!("contenido-{n}"))?;
        }
        let dir_s = dir.path().to_string_lossy().to_string();

        // POST add-file individual.
        let body = client
            .post(format!("{base}/api/library/add-file"))
            .body(format!(
                "{{\"path\":{}}}",
                serde_json::to_string(&dir.path().join("cap01.mkv").to_string_lossy())?
            ))
            .header("Content-Type", "application/json")
            .send()
            .await?
            .text()
            .await?;
        let v: serde_json::Value = serde_json::from_str(&body)?;
        assert_eq!(v["mime"], "video/x-matroska");

        // POST scan-folder → job con 3 encontrados.
        let body = client
            .post(format!("{base}/api/library/scan-folder"))
            .body(format!(
                "{{\"path\":{},\"recursive\":false}}",
                serde_json::to_string(&dir_s)?
            ))
            .header("Content-Type", "application/json")
            .send()
            .await?
            .text()
            .await?;
        let v: serde_json::Value = serde_json::from_str(&body)?;
        assert_eq!(v["found"], 3);
        let job_id = v["job_id"].as_str().unwrap().to_string();

        // Poll del job hasta done (timeout 30s).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let job: serde_json::Value = loop {
            let body = client
                .get(format!("{base}/api/library/jobs/{job_id}"))
                .send()
                .await?
                .text()
                .await?;
            let j: serde_json::Value = serde_json::from_str(&body)?;
            if j["status"] == "done" || j["status"] == "done_with_errors" {
                break j;
            }
            if tokio::time::Instant::now() > deadline {
                anyhow::bail!("timeout esperando job {job_id}: {j}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };
        assert_eq!(job["done"], 3);
        assert!(job["errors"].as_array().unwrap().is_empty());

        // La lista contiene cap01 (individual) + T1/cap* (scan con prefijo).
        let body = client
            .get(format!("{base}/api/files"))
            .send()
            .await?
            .text()
            .await?;
        let files: serde_json::Value = serde_json::from_str(&body)?;
        assert!(files.as_array().unwrap().len() >= 4);

        // DELETE por hash → 200 y desaparece de la lista.
        let hash = files[0]["hash"].as_str().unwrap().to_string();
        let r = client
            .delete(format!("{base}/api/files/{hash}"))
            .send()
            .await?;
        assert_eq!(r.status(), 200);
        let body = client
            .get(format!("{base}/api/files"))
            .send()
            .await?
            .text()
            .await?;
        let files2: serde_json::Value = serde_json::from_str(&body)?;
        assert!(!files2.as_array().unwrap().iter().any(|f| f["hash"] == hash));
        // Repetir el borrado → 404.
        let r = client
            .delete(format!("{base}/api/files/{hash}"))
            .send()
            .await?;
        assert_eq!(r.status(), 404);
        // Path inexistente → 400 con error.
        let r = client
            .post(format!("{base}/api/library/add-file"))
            .body(r#"{"path":"/no/existe.mkv"}"#)
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 400);
        Ok(())
    }

    #[tokio::test]
    async fn api_escritura_respeta_media_roots() -> anyhow::Result<()> {
        use iroh_blobs::store::mem::MemStore;
        let mem = MemStore::new();
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        let allowed = tempfile::tempdir()?;
        let (base, _h) = Gateway::serve_loopback_with_roots(
            store,
            db,
            "roots-test".into(),
            vec![allowed.path().to_path_buf()],
        )
        .await?;
        let client = reqwest::Client::new();
        let fuera = tempfile::tempdir()?;
        let f = fuera.path().join("a.mkv");
        std::fs::write(&f, b"x")?;
        let r = client
            .post(format!("{base}/api/library/add-file"))
            .body(format!(
                "{{\"path\":{}}}",
                serde_json::to_string(&f.to_string_lossy())?
            ))
            .header("Content-Type", "application/json")
            .send()
            .await?;
        assert_eq!(r.status(), 400);
        let body = r.text().await?;
        assert!(body.contains("permitidas"));
        Ok(())
    }

    #[tokio::test]
    async fn api_upload_recibe_archivo_del_navegador() -> anyhow::Result<()> {
        use iroh_blobs::store::mem::MemStore;
        let mem = MemStore::new();
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory()?;
        let (base, _h) = Gateway::serve_loopback(store, db, "upload-test".into()).await?;
        let client = reqwest::Client::new();

        // Multipart construido a mano (reqwest dev no trae feature `multipart`).
        let b = "limite-prueba-123";
        let body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"subido.mp4\"\r\n\
             Content-Type: video/mp4\r\n\r\ncontenido-falso-de-video\r\n--{b}--\r\n"
        );
        let r = client
            .post(format!("{base}/api/library/upload"))
            .header("Content-Type", format!("multipart/form-data; boundary={b}"))
            .body(body)
            .send()
            .await?;
        let status = r.status();
        let text = r.text().await?;
        assert_eq!(status, 200, "Upload failed: {text}");
        let v: serde_json::Value = serde_json::from_str(&text)?;
        assert_eq!(v["path"], "subido.mp4");
        assert_eq!(v["mime"], "video/mp4");

        // Aparece en la lista.
        let body = client
            .get(format!("{base}/api/files"))
            .send()
            .await?
            .text()
            .await?;
        let files: serde_json::Value = serde_json::from_str(&body)?;
        assert!(files
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["path"] == "subido.mp4"));

        // Sin campo `file` → 400.
        let r = client
            .post(format!("{base}/api/library/upload"))
            .header("Content-Type", format!("multipart/form-data; boundary={b}"))
            .body(format!("--{b}--\r\n"))
            .send()
            .await?;
        assert_eq!(r.status(), 400);
        Ok(())
    }

    #[test]
    fn parse_range_casos() {
        use axum::http::HeaderValue;

        let total = 10_000u64;

        // Sin header: todo el rango
        assert_eq!(parse_range(None, total), Ok((0, 9999)));

        // Rango normal
        let h = HeaderValue::from_static("bytes=0-499");
        assert_eq!(parse_range(Some(&h), total), Ok((0, 499)));

        // Rango abierto al final
        let h = HeaderValue::from_static("bytes=500-");
        assert_eq!(parse_range(Some(&h), total), Ok((500, 9999)));

        // Rango sufijo (HIGH-01): últimos 500 bytes -> (9500, 9999)
        let h = HeaderValue::from_static("bytes=-500");
        assert_eq!(parse_range(Some(&h), total), Ok((9500, 9999)));

        // Sufijo mayor al total: clamp a 0 -> (0, 9999)
        let h = HeaderValue::from_static("bytes=-20000");
        assert_eq!(parse_range(Some(&h), total), Ok((0, 9999)));

        // Sufijo 0
        let h = HeaderValue::from_static("bytes=-0");
        assert_eq!(parse_range(Some(&h), total), Ok((0, 9999)));

        // Rango inválido
        let h = HeaderValue::from_static("bytes=abc-def");
        assert_eq!(parse_range(Some(&h), total), Err(RangeError::Invalid));

        // Start > End
        let h = HeaderValue::from_static("bytes=500-200");
        assert_eq!(
            parse_range(Some(&h), total),
            Err(RangeError::OutOfBounds(total))
        );

        // Fuera de límites
        let h = HeaderValue::from_static("bytes=15000-16000");
        assert_eq!(
            parse_range(Some(&h), total),
            Err(RangeError::OutOfBounds(total))
        );

        // Archivo vacío
        assert_eq!(parse_range(None, 0), Err(RangeError::NoContent));
    }
}
