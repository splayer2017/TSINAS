//! Biblioteca del host: añadir archivos/carpetas por path y quitar con des-pineo.
//!
//! El CLI (`p2p-serve`) y el gateway web comparten este servicio para no
//! duplicar la lógica de `add_path + upsert`. Solo trabaja con **paths del
//! servidor** (nunca subidas): el navegador envía strings, el host los valida.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use iroh_blobs::api::blobs::{AddPathOptions, ImportMode};
use iroh_blobs::api::Store as BlobsStore;
use iroh_blobs::BlobFormat;

use crate::db::{Db, FileRow, FILE_KIND_FILE, FILE_KIND_MEDIA};
use crate::policy::Policy;

/// Extensiones de vídeo aceptadas en el escaneo de carpetas.
pub const VIDEO_EXTS: &[&str] = &["mkv", "mp4", "webm", "avi"];

/// Resultado de añadir un archivo.
#[derive(Debug, Clone)]
pub struct AddedFile {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub mime: String,
}

/// Servicio de biblioteca: importa paths al blob store + proyección SQLite.
#[derive(Debug, Clone)]
pub struct Library {
    store: BlobsStore,
    db: Db,
    endpoint_id: String,
    /// Raíces permitidas (canonicalizadas). Vacío = sin restricción.
    media_roots: Vec<PathBuf>,
    staging_dir: PathBuf,
    doc: Option<iroh_docs::api::Doc>,
    author: Option<iroh_docs::AuthorId>,
}

/// Guardia RAII para archivos temporales de staging.
/// Si la tarea asíncrona se interrumpe (p.ej. cliente cierra conexión a mitad de subida),
/// `drop` asegura que el archivo temporal sea eliminado del disco inmediatamente.
pub struct TempStagedFile {
    path: PathBuf,
    committed: bool,
}

impl TempStagedFile {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for TempStagedFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// HIGH-01/MED-01: corre un cierre síncrono de filesystem en el pool
/// bloqueante en vez de estacionar un worker Tokio.
async fn blocking_io<T>(f: impl FnOnce() -> anyhow::Result<T> + Send + 'static) -> anyhow::Result<T>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| anyhow::anyhow!("tarea bloqueante cancelada: {e}"))?
}

impl Library {
    pub fn new(store: BlobsStore, db: Db, endpoint_id: String) -> Self {
        let staging_dir =
            std::env::temp_dir().join(format!("p2p-nube-staging-{}", std::process::id()));
        Self {
            store,
            db,
            endpoint_id,
            media_roots: Vec::new(),
            staging_dir,
            doc: None,
            author: None,
        }
    }

    pub fn with_media_roots(mut self, roots: Vec<PathBuf>) -> Self {
        self.media_roots = roots;
        self
    }

    pub fn with_staging_dir(mut self, path: PathBuf) -> Self {
        self.staging_dir = path;
        self
    }

    pub fn with_doc(mut self, doc: iroh_docs::api::Doc, author: iroh_docs::AuthorId) -> Self {
        self.doc = Some(doc);
        self.author = Some(author);
        self
    }

    pub fn store(&self) -> &BlobsStore {
        &self.store
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn endpoint_id(&self) -> &str {
        &self.endpoint_id
    }

    pub fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    pub fn doc(&self) -> Option<&iroh_docs::api::Doc> {
        self.doc.as_ref()
    }

    /// Valida un path contra las raíces permitidas y lo canonicaliza.
    fn resolve(&self, raw: &str) -> anyhow::Result<PathBuf> {
        let raw = raw.trim();
        if raw.is_empty() {
            anyhow::bail!("path vacío");
        }
        let abs =
            std::fs::canonicalize(raw).map_err(|_| anyhow::anyhow!("no existe la ruta: {raw}"))?;
        if !self.media_roots.is_empty() && !self.media_roots.iter().any(|r| abs.starts_with(r)) {
            anyhow::bail!("ruta fuera de las carpetas permitidas: {}", abs.display());
        }
        Ok(abs)
    }

    /// MED-01: `resolve + is_file + metadata` fuera de workers Tokio.
    /// `reject_empty` preserva la semántica histórica por rama (`add_file`
    /// admite vacíos, el resto no).
    async fn fs_meta(&self, raw: &str, reject_empty: bool) -> anyhow::Result<(PathBuf, u64)> {
        let roots = self.media_roots.clone();
        let raw = raw.trim().to_string();
        blocking_io(move || {
            if raw.is_empty() {
                anyhow::bail!("path vacío");
            }
            let abs = std::fs::canonicalize(&raw)
                .map_err(|_| anyhow::anyhow!("no existe la ruta: {raw}"))?;
            if !roots.is_empty() && !roots.iter().any(|r| abs.starts_with(r)) {
                anyhow::bail!("ruta fuera de las carpetas permitidas: {}", abs.display());
            }
            if !abs.is_file() {
                anyhow::bail!("no es un archivo: {}", abs.display());
            }
            let size = std::fs::metadata(&abs)?.len();
            if reject_empty && size == 0 {
                anyhow::bail!("el archivo está vacío: {}", abs.display());
            }
            Ok((abs, size))
        })
        .await
    }

    /// MED-01: validación del temporal de subida en pool bloqueante.
    async fn staged_meta(staged: &Path) -> anyhow::Result<u64> {
        let staged = staged.to_path_buf();
        blocking_io(move || {
            if !staged.is_file() {
                anyhow::bail!("subida incompleta: temporal no encontrado");
            }
            let size = std::fs::metadata(&staged)?.len();
            if size == 0 {
                anyhow::bail!("el archivo subido está vacío");
            }
            Ok(size)
        })
        .await
    }

    /// HIGH-02: núcleo común de importación (tag/hash/mime + des-pineo
    /// MED-01 + `upsert` + `set_hash` en docs). `display` ya viene resuelto
    /// por el llamante; solo `policy`/`kind` varían entre ramas.
    async fn import_tagged(
        &self,
        display: String,
        size: u64,
        hash: iroh_blobs::Hash,
        tag_s: String,
        policy: Policy,
        kind: &str,
    ) -> anyhow::Result<AddedFile> {
        let mime = guess_mime(&display);
        let hash_s = hash.to_string();
        // Des-pineo del tag previo si cambió (HIGH-01: DB en bloqueante).
        let dp = display.clone();
        let prev: Option<String> = crate::gateway::db_blocking(&self.db, move |db| {
            Ok(db.get_by_path(&dp)?.map(|r| r.tag))
        })
        .await?;
        if let Some(pt) = prev {
            if !pt.is_empty() && pt != tag_s {
                let _ = self.store.tags().delete(pt.as_bytes()).await;
            }
        }
        let row = FileRow {
            path: display.clone(),
            hash: hash_s.clone(),
            size,
            mime: mime.clone(),
            policy,
            host_id: self.endpoint_id.clone(),
            tag: tag_s,
            title: String::new(),
            watched: false,
            kind: kind.to_string(),
        };
        crate::gateway::db_blocking(&self.db, move |db| db.upsert_file(&row)).await?;
        // BLOCK-01: Publicar entrada en iroh-docs si hay un doc activo
        if let (Some(doc), Some(author)) = (&self.doc, &self.author) {
            let _ = doc
                .set_hash(*author, display.as_bytes().to_vec(), hash, size)
                .await;
        }
        Ok(AddedFile {
            path: display,
            hash: hash_s,
            size,
            mime,
        })
    }

    /// Añade un archivo al store y a la lista. `display_base` es el prefijo
    /// de presentación (p.ej. nombre de la carpeta escaneada); `None` usa
    /// solo el nombre del archivo.
    pub async fn add_file(
        &self,
        raw_path: &str,
        display_base: Option<&str>,
    ) -> anyhow::Result<AddedFile> {
        self.add_file_with_policy(raw_path, display_base, Policy::StreamOnly)
            .await
    }

    /// MIN-03: variante con política explícita (el CLI la usa directo y
    /// evita el doble `upsert` + lectura intermedia).
    pub async fn add_file_with_policy(
        &self,
        raw_path: &str,
        display_base: Option<&str>,
        policy: Policy,
    ) -> anyhow::Result<AddedFile> {
        let (abs, size) = self.fs_meta(raw_path, false).await?;
        let name = abs
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "archivo".into());
        let path = match display_base {
            Some(b) if !b.is_empty() => format!("{b}/{name}"),
            _ => name.clone(),
        };

        // Zero-Copy: referenciar en su ubicación original (disco secundario o local)
        // sin copiar los bytes al almacenamiento interno de blobs (DataLocation::External).
        let tag = self
            .store
            .blobs()
            .add_path_with_opts(AddPathOptions {
                path: abs.clone(),
                mode: ImportMode::TryReference,
                format: BlobFormat::Raw,
            })
            .await?;
        let tag_s = String::from_utf8_lossy(tag.name.as_ref()).to_string();
        self.import_tagged(path, size, tag.hash, tag_s, policy, FILE_KIND_MEDIA)
            .await
    }

    /// Añade un archivo genérico al apartado Almacenamiento (cualquier
    /// extensión, sin filtro de vídeo). Zero-copy por path igual que
    /// `add_file`, pero con `policy=Mirror` (descarga permitida) y
    /// `kind=file` para que no aparezca en Biblioteca/Streaming.
    pub async fn add_storage_file(&self, raw_path: &str) -> anyhow::Result<AddedFile> {
        let (abs, size) = self.fs_meta(raw_path, true).await?;
        let name = abs
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "archivo".into());
        let path = sanitize_filename(&name);

        let tag = self
            .store
            .blobs()
            .add_path_with_opts(AddPathOptions {
                path: abs.clone(),
                mode: ImportMode::TryReference,
                format: BlobFormat::Raw,
            })
            .await?;
        let tag_s = String::from_utf8_lossy(tag.name.as_ref()).to_string();
        self.import_tagged(path, size, tag.hash, tag_s, Policy::Mirror, FILE_KIND_FILE)
            .await
    }

    /// Lista candidatos de vídeo en una carpeta (no lee contenido).
    /// Devuelve paths absolutos ordenados (natural: cap2 < cap10).
    pub fn scan_candidates(&self, raw_dir: &str, recursive: bool) -> anyhow::Result<Vec<PathBuf>> {
        let dir = self.resolve(raw_dir)?;
        if !dir.is_dir() {
            anyhow::bail!("no es una carpeta: {}", dir.display());
        }
        let mut out = Vec::new();
        self.collect(&dir, recursive, &mut out)?;
        out.sort_by(|a, b| {
            natural_key(&a.file_name().unwrap_or_default().to_string_lossy()).cmp(&natural_key(
                &b.file_name().unwrap_or_default().to_string_lossy(),
            ))
        });
        Ok(out)
    }

    fn collect(&self, dir: &Path, recursive: bool, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
        if out.len() > 2000 {
            anyhow::bail!("demasiados archivos (límite 2000)");
        }
        let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<Result<_, _>>()?;
        entries.sort_by_key(|e| e.file_name());
        for e in entries {
            let p = e.path();
            if p.is_dir() {
                if recursive {
                    self.collect(&p, true, out)?;
                }
            } else if p.is_file() && is_video(&p) {
                out.push(p);
            }
        }
        Ok(())
    }

    /// Importa un archivo subido desde el navegador/móvil (`multipart`).
    /// `display_name` es el nombre original del archivo (se sanea: solo el
    /// nombre base, sin directorios); `staged` es el temporal que el propio
    /// servidor escribió al recibir la subida. No aplica `media_roots`
    /// (esas raíces solo limitan leer rutas arbitrarias del servidor; aquí
    /// los bytes los aportó el usuario). Tras importar al store se intenta
    /// borrar el temporal (best-effort).
    pub async fn add_upload(&self, display_name: &str, staged: &Path) -> anyhow::Result<AddedFile> {
        let name = sanitize_filename(display_name);
        let size = Self::staged_meta(staged).await?;
        let staged_owned = staged.to_path_buf();
        let tag = self.store.blobs().add_path(&staged_owned).await?;
        let tag_s = String::from_utf8_lossy(tag.name.as_ref()).to_string();
        let added = self
            .import_tagged(
                name,
                size,
                tag.hash,
                tag_s,
                Policy::StreamOnly,
                FILE_KIND_MEDIA,
            )
            .await?;

        // El contenido ya quedó en el blob store; liberar el temporal.
        let _ = tokio::fs::remove_file(&staged_owned).await;
        Ok(added)
    }

    /// Importa una subida del navegador/móvil al apartado Almacenamiento
    /// (`kind=file`, `policy=Mirror`). El tope de 8 GiB se aplica en el
    /// gateway mientras escribe el staging, aquí solo se valida no-vacío.
    pub async fn add_storage_upload(
        &self,
        display_name: &str,
        staged: &Path,
    ) -> anyhow::Result<AddedFile> {
        let name = sanitize_filename(display_name);
        let size = Self::staged_meta(staged).await?;
        let staged_owned = staged.to_path_buf();
        let tag = self.store.blobs().add_path(&staged_owned).await?;
        let tag_s = String::from_utf8_lossy(tag.name.as_ref()).to_string();
        let added = self
            .import_tagged(name, size, tag.hash, tag_s, Policy::Mirror, FILE_KIND_FILE)
            .await?;

        let _ = tokio::fs::remove_file(&staged_owned).await;
        Ok(added)
    }

    /// Quita un archivo de la lista y des-pinea su tag para que el GC del
    /// store libere el espacio. Nunca borra el archivo original del usuario.
    /// Devuelve `true` si existía.
    pub async fn remove_by_hash(&self, hash: &str) -> anyhow::Result<bool> {
        let h = hash.to_string();
        let rows = crate::gateway::db_blocking(&self.db, move |db| db.get_rows_by_hash(&h)).await?;
        if rows.is_empty() {
            return Ok(false);
        }
        for row in &rows {
            // Des-pineo del tag
            if !row.tag.is_empty() {
                let _ = self.store.tags().delete(row.tag.as_bytes()).await;
            }
            // Borrar de iroh-docs si está configurado
            if let (Some(doc), Some(author)) = (&self.doc, &self.author) {
                let _ = doc.del(*author, row.path.as_bytes().to_vec()).await;
            }
        }
        let h = hash.to_string();
        crate::gateway::db_blocking(&self.db, move |db| db.delete_by_hash(&h)).await
    }

    /// Quita un archivo de la lista por path específico. Si otros archivos
    /// comparten el mismo hash/tag, preserva el tag en el blob store.
    pub async fn remove_by_path(&self, path: &str) -> anyhow::Result<bool> {
        let p = path.to_string();
        let row = match crate::gateway::db_blocking(&self.db, move |db| db.get_by_path(&p)).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        // Verificar si algún otro archivo comparte el mismo tag antes de des-pinear
        let h = row.hash.clone();
        let all_rows =
            crate::gateway::db_blocking(&self.db, move |db| db.get_rows_by_hash(&h)).await?;
        if all_rows.len() <= 1 && !row.tag.is_empty() {
            let _ = self.store.tags().delete(row.tag.as_bytes()).await;
        }
        if let (Some(doc), Some(author)) = (&self.doc, &self.author) {
            let _ = doc.del(*author, row.path.as_bytes().to_vec()).await;
        }
        let p = path.to_string();
        crate::gateway::db_blocking(&self.db, move |db| db.delete_by_path(&p)).await
    }
}

/// Nombre de la carpeta para prefijar las entradas de un escaneo.
pub fn display_base_for(dir: &Path) -> String {
    dir.file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "carpeta".into())
}

fn is_video(p: &Path) -> bool {
    let ext = p
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    VIDEO_EXTS.iter().any(|v| *v == ext)
}

/// Clave de orden natural: divide en tramos texto/número.
fn natural_key(s: &str) -> Vec<KeyPart> {
    let mut parts = Vec::new();
    let mut buf = String::new();
    let mut in_digit: Option<bool> = None;
    for c in s.chars() {
        let d = c.is_ascii_digit();
        match in_digit {
            Some(cur) if cur == d => buf.push(c),
            _ => {
                if !buf.is_empty() {
                    parts.push(KeyPart::new(&buf, in_digit.unwrap_or(false)));
                    buf.clear();
                }
                buf.push(c);
                in_digit = Some(d);
            }
        }
    }
    if !buf.is_empty() {
        parts.push(KeyPart::new(&buf, in_digit.unwrap_or(false)));
    }
    parts
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum KeyPart {
    Num(u64, String),
    Str(String),
}

impl KeyPart {
    fn new(s: &str, is_digit: bool) -> Self {
        if is_digit {
            KeyPart::Num(s.parse().unwrap_or(u64::MAX), s.to_lowercase())
        } else {
            KeyPart::Str(s.to_lowercase())
        }
    }
}

/// Nombre seguro para mostrar una subida: solo el nombre base, sin
/// directorios ni `..`, recortado a 128 caracteres. Nunca vacío.
fn sanitize_filename(raw: &str) -> String {
    let base = Path::new(raw)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    let clean: String = base
        .chars()
        .filter(|c| !matches!(c, '/' | '\\' | '\0'))
        .collect();
    let clean = clean.trim();
    if clean.is_empty() || clean == "." || clean == ".." {
        return "archivo".into();
    }
    clean.chars().take(128).collect()
}

/// Determina el tipo MIME según la extensión del archivo (insensible a mayúsculas).
/// Cubre vídeo (streaming) + tipos habituales de almacenamiento.
/// Desconocido → `application/octet-stream`.
pub fn guess_mime(name: &str) -> String {
    let n = name.to_lowercase();
    if n.ends_with(".mkv") {
        "video/x-matroska"
    } else if n.ends_with(".mp4") {
        "video/mp4"
    } else if n.ends_with(".webm") {
        "video/webm"
    } else if n.ends_with(".avi") {
        "video/x-msvideo"
    } else if n.ends_with(".mov") {
        "video/quicktime"
    } else if n.ends_with(".mp3") {
        "audio/mpeg"
    } else if n.ends_with(".ogg") || n.ends_with(".oga") {
        "audio/ogg"
    } else if n.ends_with(".opus") {
        "audio/opus"
    } else if n.ends_with(".flac") {
        "audio/flac"
    } else if n.ends_with(".wav") {
        "audio/wav"
    } else if n.ends_with(".m4a") {
        "audio/mp4"
    } else if n.ends_with(".jpg") || n.ends_with(".jpeg") {
        "image/jpeg"
    } else if n.ends_with(".png") {
        "image/png"
    } else if n.ends_with(".gif") {
        "image/gif"
    } else if n.ends_with(".webp") {
        "image/webp"
    } else if n.ends_with(".svg") {
        "image/svg+xml"
    } else if n.ends_with(".pdf") {
        "application/pdf"
    } else if n.ends_with(".zip") {
        "application/zip"
    } else if n.ends_with(".tar") {
        "application/x-tar"
    } else if n.ends_with(".gz") || n.ends_with(".tgz") {
        "application/gzip"
    } else if n.ends_with(".7z") {
        "application/x-7z-compressed"
    } else if n.ends_with(".rar") {
        "application/vnd.rar"
    } else if n.ends_with(".epub") {
        "application/epub+zip"
    } else if n.ends_with(".cbz") {
        "application/vnd.comicbook+zip"
    } else if n.ends_with(".cbr") {
        "application/vnd.comicbook-rar"
    } else if n.ends_with(".iso") {
        "application/x-iso9660-image"
    } else if n.ends_with(".txt") || n.ends_with(".md") || n.ends_with(".log") {
        "text/plain; charset=utf-8"
    } else if n.ends_with(".json") {
        "application/json"
    } else if n.ends_with(".csv") {
        "text/csv"
    } else if n.ends_with(".html") || n.ends_with(".htm") {
        "text/html"
    } else {
        "application/octet-stream"
    }
    .to_string()
}

/// ¿El navegador puede previsualizar este MIME sin descargar?
/// Imágenes, PDF, texto plano y audio van inline; resto → attachment.
pub fn is_previewable_mime(mime: &str) -> bool {
    let m = mime.to_lowercase();
    m.starts_with("image/")
        || m.starts_with("audio/")
        || m.starts_with("text/")
        || m == "application/pdf"
        || m == "application/json"
}

// ---------------------------------------------------------------------------
// Jobs: progreso del escaneo en background (hashear 24 MKV tarda minutos).
// ---------------------------------------------------------------------------

/// Capacidad máxima de tareas retenidas en memoria para evitar fugas (HIGH-05).
pub const MAX_JOBS: usize = 50;

/// Estado público de un job de escaneo.
#[derive(Debug, Clone)]
pub struct JobState {
    pub id: String,
    pub status: String,
    pub total: usize,
    pub done: usize,
    pub added: Vec<String>,
    pub errors: Vec<String>,
}

/// MED-03: vista resumida sin clonar `added/errors` (el poll solo necesita
/// conteos; el detalle completo va con `?full=1`).
#[derive(Debug, Clone)]
pub struct JobSummary {
    pub id: String,
    pub status: String,
    pub total: usize,
    pub done: usize,
    pub added_count: usize,
    pub errors_count: usize,
}

/// Registro de jobs en memoria con límite acotado (FIFO).
#[derive(Debug, Clone, Default)]
pub struct Jobs {
    inner: Arc<Mutex<HashMap<String, JobState>>>,
    next: Arc<AtomicU64>,
}

impl Jobs {
    pub fn create(&self, total: usize) -> String {
        let id = self.next.fetch_add(1, Ordering::SeqCst).to_string();
        let mut m = self.inner.lock().unwrap();

        // Si superamos la capacidad máxima, purgamos las tareas más antiguas.
        // MED-03: nunca evictar trabajos `running` (su `progress()` posterior
        // sería no-op y el cliente vería 404); si todo está en curso, se
        // acepta un leve desborde acotado por el semáforo de escaneos.
        if m.len() >= MAX_JOBS {
            let mut keys: Vec<u64> = m.keys().filter_map(|k| k.parse().ok()).collect();
            keys.sort_unstable();
            let evictable: Vec<u64> = keys
                .into_iter()
                .filter(|k| {
                    m.get(&k.to_string())
                        .map(|j| j.status != "running")
                        .unwrap_or(true)
                })
                .collect();
            let to_remove = (m.len() - MAX_JOBS + 1).min(evictable.len());
            for k in evictable.into_iter().take(to_remove) {
                m.remove(&k.to_string());
            }
        }

        m.insert(
            id.clone(),
            JobState {
                id: id.clone(),
                status: if total == 0 {
                    "done".into()
                } else {
                    "running".into()
                },
                total,
                done: 0,
                added: Vec::new(),
                errors: Vec::new(),
            },
        );
        id
    }

    pub fn get(&self, id: &str) -> Option<JobState> {
        self.inner.lock().unwrap().get(id).cloned()
    }

    /// Resumen barato para el poll (sin clonar vectores).
    pub fn summary(&self, id: &str) -> Option<JobSummary> {
        self.inner.lock().unwrap().get(id).map(|j| JobSummary {
            id: j.id.clone(),
            status: j.status.clone(),
            total: j.total,
            done: j.done,
            added_count: j.added.len(),
            errors_count: j.errors.len(),
        })
    }

    pub fn progress(&self, id: &str, added: Option<String>, error: Option<String>) {
        let mut m = self.inner.lock().unwrap();
        if let Some(j) = m.get_mut(id) {
            j.done += 1;
            if let Some(a) = added {
                j.added.push(a);
            }
            if let Some(e) = error {
                j.errors.push(e);
            }
            if j.done >= j.total {
                j.status = if j.errors.is_empty() {
                    "done".into()
                } else {
                    "done_with_errors".into()
                };
            }
        }
    }

    pub fn fail(&self, id: &str, msg: String) {
        let mut m = self.inner.lock().unwrap();
        if let Some(j) = m.get_mut(id) {
            j.status = "error".into();
            j.errors.push(msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_blobs::store::mem::MemStore;

    fn mem_lib() -> (Library, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mem = MemStore::new();
        let store: BlobsStore = mem.into();
        let db = Db::open_in_memory().unwrap();
        let lib = Library::new(store, db, "test-host".into());
        (lib, dir)
    }

    #[tokio::test]
    async fn add_y_remove_des_pinea() -> anyhow::Result<()> {
        let (lib, dir) = mem_lib();
        let f = dir.path().join("cap07.mkv");
        std::fs::write(&f, b"fake-mkv-bytes")?;
        let added = lib.add_file(&f.to_string_lossy(), None).await?;
        assert_eq!(added.path, "cap07.mkv");
        assert_eq!(added.mime, "video/x-matroska");
        assert_eq!(lib.db.list_files()?.len(), 1);
        assert!(lib.store.blobs().has(parse_hash(&added.hash)?).await?);
        // El tag quedó registrado para el des-pineo.
        assert!(!lib.db.get_by_hash(&added.hash)?.unwrap().tag.is_empty());
        assert!(lib.remove_by_hash(&added.hash).await?);
        assert!(lib.db.list_files()?.is_empty());
        assert!(!lib.remove_by_hash(&added.hash).await?);
        Ok(())
    }

    #[test]
    fn jobs_resumen_y_no_evicta_running() {
        // MED-03: resumen barato + la purga FIFO respeta trabajos en curso.
        let jobs = Jobs::default();
        let id = jobs.create(2);
        let s = jobs.summary(&id).expect("debe existir");
        assert_eq!(
            (s.done, s.total, s.added_count, s.errors_count),
            (0, 2, 0, 0)
        );
        jobs.progress(&id, Some("a.mkv".into()), None);
        let s = jobs.summary(&id).expect("debe existir");
        assert_eq!((s.done, s.added_count), (1, 1));
        // Llenar por encima de MAX_JOBS con terminados: el running sobrevive.
        for _ in 0..MAX_JOBS {
            jobs.create(0);
        }
        assert_eq!(jobs.summary(&id).expect("running no evictado").done, 1);
    }

    #[tokio::test]
    async fn scan_ordena_natural_y_filtra() -> anyhow::Result<()> {
        let (lib, dir) = mem_lib();
        for n in ["cap10.mkv", "cap2.mkv", "cap1.mkv", "nota.txt", "clip.mp4"] {
            std::fs::write(dir.path().join(n), b"x")?;
        }
        let cands = lib.scan_candidates(&dir.path().to_string_lossy(), false)?;
        let names: Vec<_> = cands
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["cap1.mkv", "cap2.mkv", "cap10.mkv", "clip.mp4"]);
        // Recursivo opcional.
        let sub = dir.path().join("CD1");
        std::fs::create_dir(&sub)?;
        std::fs::write(sub.join("extra.mkv"), b"x")?;
        assert_eq!(
            lib.scan_candidates(&dir.path().to_string_lossy(), false)?
                .len(),
            4
        );
        assert_eq!(
            lib.scan_candidates(&dir.path().to_string_lossy(), true)?
                .len(),
            5
        );
        Ok(())
    }

    #[tokio::test]
    async fn respeta_media_roots() -> anyhow::Result<()> {
        let (lib, dir) = mem_lib();
        let lib = lib.with_media_roots(vec![dir.path().to_path_buf()]);
        let fuera = tempfile::tempdir().unwrap();
        let f = fuera.path().join("a.mkv");
        std::fs::write(&f, b"x")?;
        assert!(lib.add_file(&f.to_string_lossy(), None).await.is_err());
        assert!(lib.add_storage_file(&f.to_string_lossy()).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn storage_acepta_cualquier_archivo() -> anyhow::Result<()> {
        let (lib, dir) = mem_lib();
        for (name, mime) in [
            ("manual.pdf", "application/pdf"),
            ("fotos.zip", "application/zip"),
            ("notas.txt", "text/plain; charset=utf-8"),
            ("imagen.jpg", "image/jpeg"),
            ("binario.raro", "application/octet-stream"),
        ] {
            let f = dir.path().join(name);
            std::fs::write(&f, format!("contenido-{name}"))?;
            let added = lib.add_storage_file(&f.to_string_lossy()).await?;
            assert_eq!(added.path, name);
            assert_eq!(added.mime, mime);
            let row = lib.db.get_by_path(name)?.expect("debe estar en db");
            assert_eq!(row.kind, FILE_KIND_FILE);
            assert!(row.policy.allows_download());
        }
        // Separación por kind: streaming vacío, storage con 5.
        assert!(lib.db.list_files_by_kind(FILE_KIND_MEDIA)?.is_empty());
        assert_eq!(lib.db.list_files_by_kind(FILE_KIND_FILE)?.len(), 5);
        // guess_mime + preview.
        assert_eq!(guess_mime("a.PDF"), "application/pdf");
        assert!(is_previewable_mime("application/pdf"));
        assert!(is_previewable_mime("image/png"));
        assert!(!is_previewable_mime("application/zip"));
        assert!(!is_previewable_mime("application/octet-stream"));
        Ok(())
    }

    #[tokio::test]
    async fn add_file_zero_copy_con_fs_store() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let blobs_dir = dir.path().join("blobs");
        std::fs::create_dir_all(&blobs_dir)?;
        let fs_opts = iroh_blobs::store::fs::options::Options::new(&blobs_dir);
        let fs_store =
            iroh_blobs::store::fs::FsStore::load_with_opts(blobs_dir.join("blobs.db"), fs_opts)
                .await?;
        let store: BlobsStore = fs_store.into();
        let db = Db::open_in_memory()?;
        let lib = Library::new(store, db, "test-host".into());

        // Archivo de ~5 MB para superar el umbral de inlining de outboard (16 KB de outboard)
        let media_dir = tempfile::tempdir()?;
        let source_file = media_dir.path().join("test_video.mkv");
        let content = vec![0x42; 5 * 1024 * 1024 + 128];
        std::fs::write(&source_file, &content)?;

        let added = lib.add_file(&source_file.to_string_lossy(), None).await?;
        assert_eq!(added.path, "test_video.mkv");
        let hash = parse_hash(&added.hash)?;
        assert!(lib.store.blobs().has(hash).await?);

        // Zero-copy: el archivo de datos NO debe crearse en blobs/data
        let data_file = blobs_dir.join("data").join(format!("{}.data", added.hash));
        assert!(
            !data_file.exists(),
            "El archivo de datos NO debe copiarse a blobs/data (Zero-Copy)"
        );

        // El archivo outboard (.obao4) sí se crea en disco para archivos > 4 MB
        let obao_file = blobs_dir.join("data").join(format!("{}.obao4", added.hash));
        assert!(
            obao_file.exists(),
            "Debe existir el árbol outboard (.obao4) en disco para verificación P2P"
        );

        // Verificamos que se puede leer el contenido referenciado a través del reader de iroh
        use tokio::io::AsyncReadExt;
        let mut reader = lib.store.blobs().reader(hash);
        let mut read_buf = Vec::new();
        reader.read_to_end(&mut read_buf).await?;
        assert_eq!(
            read_buf, content,
            "Los bytes leídos del store deben coincidir exactamente con el archivo original"
        );

        Ok(())
    }

    fn parse_hash(s: &str) -> anyhow::Result<iroh_blobs::Hash> {
        use std::str::FromStr;
        Ok(iroh_blobs::Hash::from_str(s)?)
    }
}
