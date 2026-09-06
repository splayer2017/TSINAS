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

use iroh_blobs::api::Store as BlobsStore;

use crate::db::{Db, FileRow};
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
}

impl Library {
    pub fn new(store: BlobsStore, db: Db, endpoint_id: String) -> Self {
        Self {
            store,
            db,
            endpoint_id,
            media_roots: Vec::new(),
        }
    }

    pub fn with_media_roots(mut self, roots: Vec<PathBuf>) -> Self {
        self.media_roots = roots;
        self
    }

    pub fn store(&self) -> &BlobsStore {
        &self.store
    }

    pub fn db(&self) -> &Db {
        &self.db
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

    /// Añade un archivo al store y a la lista. `display_base` es el prefijo
    /// de presentación (p.ej. nombre de la carpeta escaneada); `None` usa
    /// solo el nombre del archivo.
    pub async fn add_file(
        &self,
        raw_path: &str,
        display_base: Option<&str>,
    ) -> anyhow::Result<AddedFile> {
        let abs = self.resolve(raw_path)?;
        if !abs.is_file() {
            anyhow::bail!("no es un archivo: {}", abs.display());
        }
        let size = std::fs::metadata(&abs)?.len();
        let name = abs
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "archivo".into());
        let path = match display_base {
            Some(b) if !b.is_empty() => format!("{b}/{name}"),
            _ => name.clone(),
        };

        // iroh-blobs exige ruta absoluta; `add_path` la importa al store.
        let tag = self.store.blobs().add_path(&abs).await?;
        let hash_s = tag.hash.to_string();
        let tag_s = String::from_utf8_lossy(tag.name.as_ref()).to_string();
        let mime = p2p_nube_vault_mime(&name);

        self.db.upsert_file(&FileRow {
            path: path.clone(),
            hash: hash_s.clone(),
            size,
            mime: mime.clone(),
            policy: Policy::StreamOnly,
            host_id: self.endpoint_id.clone(),
            tag: tag_s,
        })?;
        Ok(AddedFile {
            path,
            hash: hash_s,
            size,
            mime,
        })
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
        if !staged.is_file() {
            anyhow::bail!("subida incompleta: temporal no encontrado");
        }
        let size = std::fs::metadata(staged)?.len();
        if size == 0 {
            anyhow::bail!("el archivo subido está vacío");
        }
        let tag = self.store.blobs().add_path(staged).await?;
        let hash_s = tag.hash.to_string();
        let tag_s = String::from_utf8_lossy(tag.name.as_ref()).to_string();
        let mime = p2p_nube_vault_mime(&name);
        self.db.upsert_file(&FileRow {
            path: name.clone(),
            hash: hash_s.clone(),
            size,
            mime: mime.clone(),
            policy: Policy::StreamOnly,
            host_id: self.endpoint_id.clone(),
            tag: tag_s,
        })?;
        // El contenido ya quedó en el blob store; liberar el temporal.
        let _ = std::fs::remove_file(staged);
        Ok(AddedFile {
            path: name,
            hash: hash_s,
            size,
            mime,
        })
    }

    /// Quita un archivo de la lista y des-pinea su tag para que el GC del
    /// store libere el espacio. Nunca borra el archivo original del usuario.
    /// Devuelve `true` si existía.
    pub async fn remove_by_hash(&self, hash: &str) -> anyhow::Result<bool> {
        let row = match self.db.get_by_hash(hash)? {
            Some(r) => r,
            None => return Ok(false),
        };
        // Des-pineo best-effort: si el tag ya no existe, `delete` devuelve 0
        // sin fallar; si el store es efímero no pasa nada.
        if !row.tag.is_empty() {
            let _ = self.store.tags().delete(row.tag.as_bytes()).await;
        }
        self.db.delete_by_hash(hash)
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

/// MIME mínimo sin depender del crate vault (evita ciclo core→vault).
fn p2p_nube_vault_mime(name: &str) -> String {
    let n = name.to_lowercase();
    if n.ends_with(".mkv") {
        "video/x-matroska"
    } else if n.ends_with(".mp4") {
        "video/mp4"
    } else if n.ends_with(".webm") {
        "video/webm"
    } else if n.ends_with(".avi") {
        "video/x-msvideo"
    } else {
        "application/octet-stream"
    }
    .to_string()
}

// ---------------------------------------------------------------------------
// Jobs: progreso del escaneo en background (hashear 24 MKV tarda minutos).
// ---------------------------------------------------------------------------

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

/// Registro de jobs en memoria.
#[derive(Debug, Clone, Default)]
pub struct Jobs {
    inner: Arc<Mutex<HashMap<String, JobState>>>,
    next: Arc<AtomicU64>,
}

impl Jobs {
    pub fn create(&self, total: usize) -> String {
        let id = self.next.fetch_add(1, Ordering::SeqCst).to_string();
        self.inner.lock().unwrap().insert(
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
        Ok(())
    }

    fn parse_hash(s: &str) -> anyhow::Result<iroh_blobs::Hash> {
        use std::str::FromStr;
        Ok(iroh_blobs::Hash::from_str(s)?)
    }
}
