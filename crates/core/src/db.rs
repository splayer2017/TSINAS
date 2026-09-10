use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::policy::Policy;

/// Kind de fila: `media` (streaming, vídeos) o `file` (almacenamiento genérico).
pub const FILE_KIND_MEDIA: &str = "media";
pub const FILE_KIND_FILE: &str = "file";

fn default_file_kind() -> String {
    FILE_KIND_MEDIA.to_string()
}

/// Normaliza kind de archivo: `file` para almacenamiento, resto → `media`.
pub fn normalize_file_kind(raw: &str) -> String {
    match raw.trim().to_lowercase().as_str() {
        "file" | "storage" | "almacenamiento" | "archivo" => FILE_KIND_FILE.to_string(),
        _ => FILE_KIND_MEDIA.to_string(),
    }
}

/// Fila de la proyección local (derivada de iroh-docs, no es fuente de verdad).
/// `tag` es el nombre del tag persistente en el blob store que protege el
/// contenido del GC; se borra al quitar el archivo (des-pinear).
/// `title` es el título visible editable desde la web (sin tocar disco);
/// vacío = mostrar el nombre base del `path`.
/// `watched` marca el episodio como visto (switch en la UI, compartido).
/// `kind` separa `media` (streaming) de `file` (almacenamiento genérico).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRow {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub mime: String,
    pub policy: Policy,
    pub host_id: String,
    pub tag: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub watched: bool,
    #[serde(default = "default_file_kind")]
    pub kind: String,
}

impl FileRow {
    /// Título a mostrar: `title` si hay, si no el basename del path.
    pub fn display_title(&self) -> String {
        if !self.title.trim().is_empty() {
            return self.title.clone();
        }
        std::path::Path::new(&self.path)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| self.path.clone())
    }
}

/// Colección virtual (Serie / Película). No es una carpeta real en disco:
/// solo agrupa `files` por `path` para la UI.
/// `parent_id` vacío = colección raíz (serie/anime); en otro caso es una
/// sub-colección (temporada o película/partes) cuyo padre es la raíz.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collection {
    pub id: String,
    pub kind: String,
    pub title: String,
    #[serde(default)]
    pub poster_url: String,
    #[serde(default)]
    pub poster_file: String,
    pub created_at: i64,
    #[serde(default)]
    pub parent_id: String,
    #[serde(default)]
    pub pos: i64,
}

/// Entrada de un archivo dentro de una colección.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionItem {
    pub collection_id: String,
    pub file_path: String,
    #[serde(default)]
    pub season: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub pos: i64,
}

/// Detalle de colección con sus items + info del archivo para la UI.
/// `children` solo se rellena en colecciones raíz: cada hija trae sus items.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionDetail {
    #[serde(flatten)]
    pub collection: Collection,
    #[serde(default)]
    pub items: Vec<CollectionItemView>,
    #[serde(default)]
    pub children: Vec<CollectionDetail>,
}

/// Item enriquecido con metadatos del archivo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionItemView {
    pub file_path: String,
    pub title: String,
    pub hash: String,
    pub size: u64,
    pub mime: String,
    #[serde(default)]
    pub season: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub pos: i64,
    #[serde(default)]
    pub watched: bool,
}

/// Metadato compartido de un archivo para sincronización P2P.
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

#[derive(Debug, Clone)]
pub struct Db {
    inner: Arc<Mutex<Connection>>,
}

/// Crea el esquema, activa WAL mode y migra DBs antiguas.
fn ensure_schema(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         CREATE TABLE IF NOT EXISTS files(
            path TEXT PRIMARY KEY,
            hash TEXT NOT NULL,
            size INTEGER NOT NULL,
            mime TEXT NOT NULL DEFAULT '',
            policy TEXT NOT NULL DEFAULT 'stream_only',
            host_id TEXT NOT NULL DEFAULT '',
            tag TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            watched INTEGER NOT NULL DEFAULT 0,
            kind TEXT NOT NULL DEFAULT 'media'
        );
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            val TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS collections(
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL DEFAULT 'series',
            title TEXT NOT NULL DEFAULT '',
            poster_url TEXT NOT NULL DEFAULT '',
            poster_file TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL DEFAULT 0,
            parent_id TEXT NOT NULL DEFAULT '',
            pos INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS collection_items(
            collection_id TEXT NOT NULL,
            file_path TEXT NOT NULL,
            season TEXT NOT NULL DEFAULT '',
            label TEXT NOT NULL DEFAULT '',
            pos INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(collection_id, file_path)
        );
        CREATE INDEX IF NOT EXISTS idx_items_path ON collection_items(file_path);",
    )?;
    // Migración idempotente para DBs creadas antes de `tag` / `title` /
    // `parent_id`. Los ALTER van ANTES de crear índices sobre esas columnas:
    // en una DB vieja la columna aún no existe y el índice fallaría,
    // abortando el arranque (mordió en `tsinas` con data-nodo real).
    let _ = conn.execute(
        "ALTER TABLE files ADD COLUMN tag TEXT NOT NULL DEFAULT ''",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE files ADD COLUMN title TEXT NOT NULL DEFAULT ''",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE files ADD COLUMN watched INTEGER NOT NULL DEFAULT 0",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE files ADD COLUMN kind TEXT NOT NULL DEFAULT 'media'",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE collections ADD COLUMN poster_url TEXT NOT NULL DEFAULT ''",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE collections ADD COLUMN poster_file TEXT NOT NULL DEFAULT ''",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE collections ADD COLUMN parent_id TEXT NOT NULL DEFAULT ''",
        [],
    );
    let _ = conn.execute(
        "ALTER TABLE collections ADD COLUMN pos INTEGER NOT NULL DEFAULT 0",
        [],
    );
    // Índices sobre columnas migradas: solo ahora existen con seguridad.
    conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_cols_parent ON collections(parent_id);")?;
    Ok(())
}

fn row_from(r: &rusqlite::Row) -> rusqlite::Result<FileRow> {
    let policy_s: String = r.get(4)?;
    Ok(FileRow {
        path: r.get(0)?,
        hash: r.get(1)?,
        size: r.get(2)?,
        mime: r.get(3)?,
        policy: Policy::parse(&policy_s).unwrap_or_default(),
        host_id: r.get(5)?,
        tag: r.get(6)?,
        title: r.get(7).unwrap_or_default(),
        watched: r.get::<_, Option<i64>>(8)?.unwrap_or(0) != 0,
        kind: r
            .get::<_, Option<String>>(9)
            .unwrap_or(None)
            .unwrap_or_else(|| FILE_KIND_MEDIA.to_string()),
    })
}

fn collection_from(r: &rusqlite::Row) -> rusqlite::Result<Collection> {
    Ok(Collection {
        id: r.get(0)?,
        kind: r.get(1)?,
        title: r.get(2)?,
        poster_url: r.get(3)?,
        poster_file: r.get(4)?,
        created_at: r.get(5)?,
        parent_id: r.get(6).unwrap_or_default(),
        pos: r.get(7).unwrap_or_default(),
    })
}

impl Db {
    /// Acceso a la conexión con error en vez de pánico (HIGH-01): convierte
    /// un mutex envenenado en `Err` recuperable en lugar de tumbar el handler.
    fn conn(&self) -> anyhow::Result<std::sync::MutexGuard<'_, Connection>> {
        self.inner
            .lock()
            .map_err(|_| anyhow::anyhow!("db bloqueada (mutex envenenado)"))
    }

    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        ensure_schema(&conn)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        ensure_schema(&conn)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn upsert_file(&self, row: &FileRow) -> anyhow::Result<()> {
        let kind = normalize_file_kind(&row.kind);
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO files(path,hash,size,mime,policy,host_id,tag,title,watched,kind)
             VALUES(?,?,?,?,?,?,?,?,?,?)
             ON CONFLICT(path) DO UPDATE SET hash=excluded.hash,size=excluded.size,
               mime=excluded.mime,policy=excluded.policy,host_id=excluded.host_id,
               tag=excluded.tag,kind=excluded.kind",
            params![
                row.path,
                row.hash,
                row.size as i64,
                row.mime,
                row.policy.as_str(),
                row.host_id,
                row.tag,
                row.title,
                i64::from(row.watched),
                kind,
            ],
        )?;
        Ok(())
    }

    /// Cambia solo el título visible (sin tocar disco ni re-hashear).
    /// Recorta a 128 caracteres. Vacío = volver al basename.
    pub fn set_title(&self, path: &str, title: &str) -> anyhow::Result<bool> {
        let t: String = title.trim().chars().take(128).collect();
        let conn = self.conn()?;
        let n = conn.execute("UPDATE files SET title=? WHERE path=?", params![t, path])?;
        Ok(n > 0)
    }

    /// Marca/desmarca un archivo como visto. Devuelve `true` si existía.
    pub fn set_watched(&self, path: &str, watched: bool) -> anyhow::Result<bool> {
        let conn = self.conn()?;
        let n = conn.execute(
            "UPDATE files SET watched=? WHERE path=?",
            params![i64::from(watched), path],
        )?;
        Ok(n > 0)
    }

    pub fn list_files(&self) -> anyhow::Result<Vec<FileRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT path,hash,size,mime,policy,host_id,tag,title,watched,kind FROM files ORDER BY path",
        )?;
        let rows = stmt
            .query_map([], row_from)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Lista solo un kind (`media` = streaming, `file` = almacenamiento).
    pub fn list_files_by_kind(&self, kind: &str) -> anyhow::Result<Vec<FileRow>> {
        self.list_files_by_kind_paged(kind, 10_000, 0)
    }

    /// MED-02: listado con paginación (`?limit=&offset=`). Límites acotados
    /// para no serializar MBs con catálogos grandes (FUT-03).
    pub fn list_files_by_kind_paged(
        &self,
        kind: &str,
        limit: i64,
        offset: i64,
    ) -> anyhow::Result<Vec<FileRow>> {
        let kind = normalize_file_kind(kind);
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT path,hash,size,mime,policy,host_id,tag,title,watched,kind FROM files WHERE kind=? ORDER BY path LIMIT ? OFFSET ?",
        )?;
        let rows = stmt
            .query_map(
                params![kind, limit.clamp(1, 10_000), offset.max(0)],
                row_from,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// MED-02: conteos baratos para `GET /api/info` sin cargar todas las filas.
    /// Devuelve `(total, media, storage)`.
    pub fn count_files(&self) -> anyhow::Result<(i64, i64, i64)> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare("SELECT kind, COUNT(*) FROM files GROUP BY kind")?;
        let mut total = 0;
        let mut media = 0;
        let mut storage = 0;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        for r in rows {
            let (kind, n) = r?;
            total += n;
            if normalize_file_kind(&kind) == FILE_KIND_FILE {
                storage += n;
            } else {
                media += n;
            }
        }
        Ok((total, media, storage))
    }

    pub fn get_by_hash(&self, hash: &str) -> anyhow::Result<Option<FileRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT path,hash,size,mime,policy,host_id,tag,title,watched,kind FROM files WHERE hash=?",
        )?;
        let mut rows = stmt.query_map([hash], row_from)?;
        Ok(rows.next().transpose()?)
    }

    pub fn get_by_path(&self, path: &str) -> anyhow::Result<Option<FileRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT path,hash,size,mime,policy,host_id,tag,title,watched,kind FROM files WHERE path=?",
        )?;
        let mut rows = stmt.query_map([path], row_from)?;
        Ok(rows.next().transpose()?)
    }

    pub fn get_rows_by_hash(&self, hash: &str) -> anyhow::Result<Vec<FileRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT path,hash,size,mime,policy,host_id,tag,title,watched,kind FROM files WHERE hash=?",
        )?;
        let rows = stmt
            .query_map([hash], row_from)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Borra la fila por hash. Devuelve `true` si existía.
    /// Limpia también sus items de colección (huérfanos).
    pub fn delete_by_hash(&self, hash: &str) -> anyhow::Result<bool> {
        let conn = self.conn()?;
        let paths: Vec<String> = conn
            .prepare("SELECT path FROM files WHERE hash=?")?
            .query_map([hash], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        for p in &paths {
            let _ = conn.execute("DELETE FROM collection_items WHERE file_path=?", [p]);
        }
        let n = conn.execute("DELETE FROM files WHERE hash=?", [hash])?;
        Ok(n > 0)
    }

    /// Borra la fila por path. Devuelve `true` si existía.
    pub fn delete_by_path(&self, path: &str) -> anyhow::Result<bool> {
        let conn = self.conn()?;
        let _ = conn.execute("DELETE FROM collection_items WHERE file_path=?", [path]);
        let n = conn.execute("DELETE FROM files WHERE path=?", [path])?;
        Ok(n > 0)
    }

    pub fn get_setting(&self, key: &str) -> anyhow::Result<Option<String>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare("SELECT val FROM settings WHERE key=?")?;
        let mut rows = stmt.query_map([key], |r| r.get(0))?;
        Ok(rows.next().transpose()?)
    }

    pub fn set_setting(&self, key: &str, val: &str) -> anyhow::Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO settings(key, val) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET val=excluded.val",
            params![key, val],
        )?;
        Ok(())
    }

    // ---------------- Colecciones virtuales ----------------

    /// Normaliza kind: `series` (raíz), `season` (temporada hija) o `movie`
    /// (raíz película o hija de serie).
    pub fn normalize_kind(raw: &str) -> String {
        match raw.trim().to_lowercase().as_str() {
            "season" | "temporada" | "t" => "season".to_string(),
            "movie" | "pelicula" | "película" | "film" => "movie".to_string(),
            _ => "series".to_string(),
        }
    }

    /// Crea una colección raíz (`parent_id` vacío) o hija (temporada/película
    /// dentro de una serie). Solo 2 niveles: la hija no puede tener hijas.
    pub fn create_collection(
        &self,
        kind: &str,
        title: &str,
        parent_id: &str,
    ) -> anyhow::Result<Collection> {
        let title: String = title.trim().chars().take(128).collect();
        if title.is_empty() {
            anyhow::bail!("título vacío");
        }
        let parent = parent_id.trim().to_string();
        let kind = if parent.is_empty() {
            // Raíz: serie o película suelta.
            match Self::normalize_kind(kind).as_str() {
                "movie" => "movie".to_string(),
                _ => "series".to_string(),
            }
        } else {
            // Hija: temporada o película/parte.
            match Self::normalize_kind(kind).as_str() {
                "movie" => "movie".to_string(),
                _ => "season".to_string(),
            }
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        // id único sin deps externas: nanos completos + pid + contador.
        // `Relaxed` basta (solo unicidad local); sin máscaras que recorten
        // el espacio a 40 bits (MIN-05).
        static CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let c = CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = format!("c{:x}-{:x}-{}", now as u64, c, std::process::id());
        let col = Collection {
            id,
            kind,
            title,
            poster_url: String::new(),
            poster_file: String::new(),
            created_at: now,
            parent_id: parent.clone(),
            pos: 0,
        };
        let conn = self.conn()?;
        if !parent.is_empty() {
            let prow: Option<(String, String)> = conn
                .query_row(
                    "SELECT id, parent_id FROM collections WHERE id=?",
                    [&parent],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .ok();
            let Some((_, grand)) = prow else {
                anyhow::bail!("la colección padre no existe");
            };
            if !grand.is_empty() {
                anyhow::bail!("solo hay 2 niveles: la temporada no puede tener hijas");
            }
        }
        // Orden visual: anexar al final dentro de su nivel.
        let max_pos: Option<i64> = if parent.is_empty() {
            conn.query_row(
                "SELECT MAX(pos) FROM collections WHERE parent_id=''",
                [],
                |r| r.get(0),
            )
            .ok()
            .flatten()
        } else {
            conn.query_row(
                "SELECT MAX(pos) FROM collections WHERE parent_id=?",
                [&parent],
                |r| r.get(0),
            )
            .ok()
            .flatten()
        };
        let pos = max_pos.unwrap_or(0) + 1;
        conn.execute(
            "INSERT INTO collections(id,kind,title,poster_url,poster_file,created_at,parent_id,pos) VALUES(?,?,?,?,?,?,?,?)",
            params![
                col.id,
                col.kind,
                col.title,
                col.poster_url,
                col.poster_file,
                col.created_at,
                col.parent_id,
                pos
            ],
        )?;
        Ok(Collection { pos, ..col })
    }

    pub fn list_collections(&self) -> anyhow::Result<Vec<Collection>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id,kind,title,poster_url,poster_file,created_at,parent_id,pos FROM collections ORDER BY pos, created_at",
        )?;
        let rows: Vec<Collection> = stmt
            .query_map([], collection_from)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Solo hijas directas de una colección raíz, ordenadas por posición visual.
    pub fn list_children(&self, parent: &str) -> anyhow::Result<Vec<Collection>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id,kind,title,poster_url,poster_file,created_at,parent_id,pos FROM collections WHERE parent_id=? ORDER BY pos, created_at",
        )?;
        let rows: Vec<Collection> = stmt
            .query_map([parent], collection_from)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_collection(&self, id: &str) -> anyhow::Result<Option<Collection>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id,kind,title,poster_url,poster_file,created_at,parent_id,pos FROM collections WHERE id=?",
        )?;
        let mut rows = stmt.query_map([id], collection_from)?;
        Ok(rows.next().transpose()?)
    }

    pub fn update_collection(
        &self,
        id: &str,
        title: Option<&str>,
        poster_url: Option<&str>,
        kind: Option<&str>,
    ) -> anyhow::Result<bool> {
        let conn = self.conn()?;
        if let Some(t) = title {
            let t: String = t.trim().chars().take(128).collect();
            if t.is_empty() {
                anyhow::bail!("título vacío");
            }
            let n = conn.execute("UPDATE collections SET title=? WHERE id=?", params![t, id])?;
            return Ok(n > 0);
        }
        if let Some(u) = poster_url {
            let u: String = u.trim().chars().take(512).collect();
            if !u.is_empty()
                && !(u.starts_with("http://")
                    || u.starts_with("https://")
                    || u.starts_with("/posters/"))
            {
                anyhow::bail!("URL de portada inválida (http(s):// o /posters/…)");
            }
            let n = conn.execute(
                "UPDATE collections SET poster_url=? WHERE id=?",
                params![u, id],
            )?;
            return Ok(n > 0);
        }
        if let Some(k) = kind {
            let cur: Option<(String, String)> = conn
                .query_row(
                    "SELECT kind, parent_id FROM collections WHERE id=?",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .ok();
            let Some((_, parent)) = cur else {
                return Ok(false);
            };
            // En raíz solo series/movie; en hija solo season/movie.
            let nk = Self::normalize_kind(k);
            let ok = if parent.is_empty() {
                nk == "series" || nk == "movie"
            } else {
                nk == "season" || nk == "movie"
            };
            if !ok {
                anyhow::bail!("tipo inválido para este nivel");
            }
            let n = conn.execute("UPDATE collections SET kind=? WHERE id=?", params![nk, id])?;
            return Ok(n > 0);
        }
        Ok(false)
    }

    pub fn set_poster_file(&self, id: &str, file: &str) -> anyhow::Result<bool> {
        let conn = self.conn()?;
        let n = conn.execute(
            "UPDATE collections SET poster_file=? WHERE id=?",
            params![file, id],
        )?;
        Ok(n > 0)
    }

    /// Borra la agrupación (y sus hijas en cascada), nunca los archivos:
    /// los episodios vuelven a quedar solo en Biblioteca.
    pub fn delete_collection(&self, id: &str) -> anyhow::Result<bool> {
        let conn = self.conn()?;
        // Hijas primero (solo 2 niveles).
        let kids: Vec<String> = conn
            .prepare("SELECT id FROM collections WHERE parent_id=?")?
            .query_map([id], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        for k in &kids {
            let _ = conn.execute("DELETE FROM collection_items WHERE collection_id=?", [k]);
            let _ = conn.execute("DELETE FROM collections WHERE id=?", [k]);
        }
        let _ = conn.execute("DELETE FROM collection_items WHERE collection_id=?", [id]);
        let n = conn.execute("DELETE FROM collections WHERE id=?", [id])?;
        Ok(n > 0)
    }

    /// Añade/mueve un archivo a una colección hoja (virtual, no toca disco).
    /// `season` p.ej. "T1"; `label` p.ej. "E03". `pos` ordena dentro
    /// (`pos <= 0` = anexar al final).
    /// Un archivo solo vive en un lugar: moverlo lo saca de cualquier otra.
    /// Las colecciones `movie` (suelta o hija) solo admiten 1 archivo.
    pub fn add_item(
        &self,
        collection_id: &str,
        file_path: &str,
        season: &str,
        label: &str,
        pos: i64,
    ) -> anyhow::Result<()> {
        let conn = self.conn()?;
        let target: Option<(String, String)> = conn
            .query_row(
                "SELECT kind, parent_id FROM collections WHERE id=?",
                [collection_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok();
        let Some((kind, _)) = target else {
            anyhow::bail!("colección no existe");
        };
        // Solo las hojas contienen episodios: una raíz con hijas no acepta
        // items directos (hay que elegir la temporada/película).
        let kids: i64 = conn.query_row(
            "SELECT COUNT(*) FROM collections WHERE parent_id=?",
            [collection_id],
            |r| r.get(0),
        )?;
        if kids > 0 {
            anyhow::bail!("elige una temporada o película dentro de la serie");
        }
        let f: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files WHERE path=?",
            [file_path],
            |r| r.get(0),
        )?;
        if f == 0 {
            anyhow::bail!("archivo no listado");
        }
        // Película (suelta o dentro de serie): un solo archivo.
        if kind == "movie" {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM collection_items WHERE collection_id=? AND file_path<>?",
                rusqlite::params![collection_id, file_path],
                |r| r.get(0),
            )?;
            if n > 0 {
                anyhow::bail!("la película ya tiene un archivo: sácalo antes de mover otro");
            }
        }
        let season: String = season.trim().chars().take(32).collect();
        let label: String = label.trim().chars().take(64).collect();
        let pos = if pos <= 0 {
            let max_pos: Option<i64> = conn
                .query_row(
                    "SELECT MAX(pos) FROM collection_items WHERE collection_id=?",
                    [collection_id],
                    |r| r.get(0),
                )
                .ok()
                .flatten();
            max_pos.unwrap_or(0) + 1
        } else {
            pos
        };
        // Mover = un archivo solo vive en una colección: borra otras entradas.
        let _ = conn.execute(
            "DELETE FROM collection_items WHERE file_path=? AND collection_id<>?",
            rusqlite::params![file_path, collection_id],
        );
        conn.execute(
            "INSERT INTO collection_items(collection_id,file_path,season,label,pos) VALUES(?,?,?,?,?)
             ON CONFLICT(collection_id,file_path) DO UPDATE SET season=excluded.season,label=excluded.label,pos=excluded.pos",
            params![collection_id, file_path, season, label, pos],
        )?;
        Ok(())
    }

    /// Reordena los episodios de una hoja según el orden dado de paths.
    /// `order[i]` queda con `pos=i`. Ignora paths que no estén en la colección.
    pub fn reorder_items(&self, collection_id: &str, order: &[String]) -> anyhow::Result<()> {
        let conn = self.conn()?;
        let exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM collections WHERE id=?",
            [collection_id],
            |r| r.get(0),
        )?;
        if exists == 0 {
            anyhow::bail!("colección no existe");
        }
        for (i, p) in order.iter().enumerate() {
            let _ = conn.execute(
                "UPDATE collection_items SET pos=? WHERE collection_id=? AND file_path=?",
                rusqlite::params![i as i64, collection_id, p],
            );
        }
        Ok(())
    }

    /// Reordena las hijas (temporadas/películas) de una serie raíz.
    /// `order[i]` queda con `pos=i`. Ignora ids que no sean hijas de `root_id`.
    pub fn reorder_children(&self, root_id: &str, order: &[String]) -> anyhow::Result<()> {
        let conn = self.conn()?;
        let exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM collections WHERE id=?",
            [root_id],
            |r| r.get(0),
        )?;
        if exists == 0 {
            anyhow::bail!("colección no existe");
        }
        for (i, cid) in order.iter().enumerate() {
            let _ = conn.execute(
                "UPDATE collections SET pos=? WHERE id=? AND parent_id=?",
                rusqlite::params![i as i64, cid, root_id],
            );
        }
        Ok(())
    }

    pub fn remove_item(&self, collection_id: &str, file_path: &str) -> anyhow::Result<bool> {
        let conn = self.conn()?;
        let n = conn.execute(
            "DELETE FROM collection_items WHERE collection_id=? AND file_path=?",
            params![collection_id, file_path],
        )?;
        Ok(n > 0)
    }

    fn items_of(conn: &rusqlite::Connection, id: &str) -> anyhow::Result<Vec<CollectionItemView>> {
        let mut stmt = conn.prepare(
            "SELECT ci.file_path, ci.season, ci.label, ci.pos,
                    f.hash, f.size, f.mime, COALESCE(f.title,''), COALESCE(f.watched,0)
             FROM collection_items ci JOIN files f ON f.path=ci.file_path
             WHERE ci.collection_id=? ORDER BY ci.pos, f.title, f.path",
        )?;
        let items = stmt
            .query_map([id], |r| {
                let file_path: String = r.get(0)?;
                let season: String = r.get(1)?;
                let label: String = r.get(2)?;
                let pos: i64 = r.get(3)?;
                let hash: String = r.get(4)?;
                let size: i64 = r.get(5)?;
                let mime: String = r.get(6)?;
                let title_raw: String = r.get(7)?;
                let watched: i64 = r.get(8)?;
                let title = if title_raw.trim().is_empty() {
                    std::path::Path::new(&file_path)
                        .file_name()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_else(|| file_path.clone())
                } else {
                    title_raw
                };
                Ok(CollectionItemView {
                    file_path,
                    title,
                    hash,
                    size: size as u64,
                    mime,
                    season,
                    label,
                    pos,
                    watched: watched != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(items)
    }

    /// Detalle con items propios + hijas (cada hija con sus items).
    /// Los episodios viven en las hojas; la raíz con hijas no tiene items
    /// directos (se rechazan en `add_item`).
    pub fn collection_detail(&self, id: &str) -> anyhow::Result<Option<CollectionDetail>> {
        let conn = self.conn()?;
        let col: Option<Collection> = conn
            .query_row(
                "SELECT id,kind,title,poster_url,poster_file,created_at,parent_id,pos FROM collections WHERE id=?",
                [id],
                collection_from,
            )
            .ok();
        let Some(col) = col else {
            return Ok(None);
        };
        let items = Self::items_of(&conn, id)?;
        let mut children = Vec::new();
        if col.parent_id.is_empty() {
            let mut stmt = conn.prepare(
                "SELECT id,kind,title,poster_url,poster_file,created_at,parent_id,pos FROM collections WHERE parent_id=? ORDER BY pos, created_at",
            )?;
            let kids: Vec<Collection> = stmt
                .query_map([id], collection_from)?
                .collect::<Result<Vec<_>, _>>()?;
            for k in kids {
                let kitems = Self::items_of(&conn, &k.id)?;
                children.push(CollectionDetail {
                    collection: k,
                    items: kitems,
                    children: Vec::new(),
                });
            }
        }
        Ok(Some(CollectionDetail {
            collection: col,
            items,
            children,
        }))
    }

    /// ¿En qué colección está un archivo? (máx 1 por regla de mover).
    pub fn collection_of(&self, file_path: &str) -> anyhow::Result<Option<String>> {
        let conn = self.conn()?;
        let mut stmt =
            conn.prepare("SELECT collection_id FROM collection_items WHERE file_path=?")?;
        let mut rows = stmt.query_map([file_path], |r| r.get(0))?;
        Ok(rows.next().transpose()?)
    }

    /// MED-02: resumen agregado para `GET /api/collections` sin N+1.
    /// Por colección: (nº total de items propios + de hijas, nº de hijas).
    /// 3 queries fijas en vez de 1 + 2×N.
    pub fn collections_summary(&self) -> anyhow::Result<HashMap<String, (i64, i64)>> {
        let conn = self.conn()?;
        let mut own: HashMap<String, i64> = HashMap::new();
        {
            let mut stmt = conn.prepare(
                "SELECT collection_id, COUNT(*) FROM collection_items GROUP BY collection_id",
            )?;
            let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
            for r in rows {
                let (k, v) = r?;
                own.insert(k, v);
            }
        }
        let mut kids_of: HashMap<String, Vec<String>> = HashMap::new();
        let mut all_ids = Vec::new();
        {
            let mut stmt = conn.prepare("SELECT id, parent_id FROM collections")?;
            let rows =
                stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            for r in rows {
                let (id, parent) = r?;
                if !parent.is_empty() {
                    kids_of.entry(parent.clone()).or_default().push(id.clone());
                }
                all_ids.push(id);
            }
        }
        let mut out = HashMap::new();
        for id in all_ids {
            let kids: &[String] = kids_of.get(&id).map(Vec::as_slice).unwrap_or(&[]);
            let n = own.get(&id).copied().unwrap_or(0)
                + kids
                    .iter()
                    .map(|k| own.get(k).copied().unwrap_or(0))
                    .sum::<i64>();
            out.insert(id, (n, kids.len() as i64));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_y_lista() -> anyhow::Result<()> {
        let db = Db::open_in_memory()?;
        db.upsert_file(&FileRow {
            path: "anime/cap01.mkv".into(),
            hash: "abc".into(),
            size: 42,
            mime: "video/x-matroska".into(),
            policy: Policy::StreamOnly,
            host_id: "host1".into(),
            tag: "t1".into(),
            title: String::new(),
            watched: false,
            kind: FILE_KIND_MEDIA.into(),
        })?;
        let all = db.list_files()?;
        assert_eq!(all.len(), 1);
        assert!(!all[0].policy.allows_download());
        assert!(all[0].policy.allows_stream());
        assert_eq!(all[0].display_title(), "cap01.mkv");
        assert_eq!(all[0].kind, FILE_KIND_MEDIA);
        assert_eq!(db.list_files_by_kind(FILE_KIND_MEDIA)?.len(), 1);
        assert!(db.list_files_by_kind(FILE_KIND_FILE)?.is_empty());
        assert!(db.set_title("anime/cap01.mkv", "Capítulo 1")?);
        assert_eq!(
            db.get_by_path("anime/cap01.mkv")?.unwrap().display_title(),
            "Capítulo 1"
        );
        // Visto: switch compartido, se conserva al re-hashear (upsert).
        assert!(!db.get_by_path("anime/cap01.mkv")?.unwrap().watched);
        assert!(db.set_watched("anime/cap01.mkv", true)?);
        assert!(db.get_by_path("anime/cap01.mkv")?.unwrap().watched);
        db.upsert_file(&FileRow {
            path: "anime/cap01.mkv".into(),
            hash: "abc2".into(),
            size: 43,
            mime: "video/x-matroska".into(),
            policy: Policy::StreamOnly,
            host_id: "host1".into(),
            tag: "t2".into(),
            title: String::new(),
            watched: false,
            kind: FILE_KIND_MEDIA.into(),
        })?;
        assert!(db.get_by_path("anime/cap01.mkv")?.unwrap().watched);
        assert!(db.set_watched("anime/cap01.mkv", false)?);
        // Colecciones: raíz + hija, mover entre hijas, borrado en cascada.
        let col = db.create_collection("series", "Naruto", "")?;
        assert_eq!(col.kind, "series");
        let t1 = db.create_collection("season", "Temporada 1", &col.id)?;
        assert_eq!(t1.kind, "season");
        assert_eq!(t1.parent_id, col.id);
        db.add_item(&t1.id, "anime/cap01.mkv", "", "E01", 1)?;
        let det = db.collection_detail(&col.id)?.unwrap();
        assert_eq!(det.children.len(), 1);
        assert_eq!(det.children[0].items.len(), 1);
        assert_eq!(det.children[0].items[0].label, "E01");
        // La raíz con hijas no acepta episodios directos.
        assert!(db.add_item(&col.id, "anime/cap01.mkv", "", "", 0).is_err());
        let col2 = db.create_collection("movie", "Akira", "")?;
        db.add_item(&col2.id, "anime/cap01.mkv", "", "Film", 0)?;
        // Mover: ya no está en la primera.
        assert!(db.collection_detail(&t1.id)?.unwrap().items.is_empty());
        assert_eq!(db.collection_detail(&col2.id)?.unwrap().items.len(), 1);
        // Borrado recursivo: borra hijas pero conserva el archivo.
        assert!(db.delete_collection(&col.id)?);
        assert!(db.get_collection(&t1.id)?.is_none());
        assert!(!db.delete_by_hash("no-existe")?);
        assert!(db.delete_by_hash("abc2")?);
        assert!(db.list_files()?.is_empty());
        // Al borrar el archivo se limpian sus items.
        assert!(db.collection_detail(&col2.id)?.unwrap().items.is_empty());

        // Settings test
        db.set_setting("active_doc", "doc-123")?;
        assert_eq!(db.get_setting("active_doc")?, Some("doc-123".into()));
        Ok(())
    }

    #[test]
    fn collections_summary_paginacion_y_conteos() -> anyhow::Result<()> {
        // MED-02: agregado sin N+1 + listados paginados + conteos.
        let db = Db::open_in_memory()?;
        for (p, h) in [
            ("a/cap1.mkv", "h1"),
            ("a/cap2.mkv", "h2"),
            ("a/cap3.mkv", "h3"),
        ] {
            db.upsert_file(&FileRow {
                path: p.into(),
                hash: h.into(),
                size: 10,
                mime: "video/x-matroska".into(),
                policy: Policy::StreamOnly,
                host_id: "host1".into(),
                tag: format!("t{h}"),
                title: String::new(),
                watched: false,
                kind: FILE_KIND_MEDIA.into(),
            })?;
        }
        let root = db.create_collection("series", "Serie", "")?;
        let t1 = db.create_collection("season", "T1", &root.id)?;
        db.add_item(&t1.id, "a/cap1.mkv", "", "", 0)?;
        db.add_item(&t1.id, "a/cap2.mkv", "", "", 0)?;
        let sum = db.collections_summary()?;
        assert_eq!(sum.get(&t1.id), Some(&(2, 0)));
        // Raíz: 2 propios-de-hija + 1 hija.
        assert_eq!(sum.get(&root.id), Some(&(2, 1)));
        // Paginación: 3 archivos, páginas de 2.
        assert_eq!(db.list_files_by_kind_paged(FILE_KIND_MEDIA, 2, 0)?.len(), 2);
        assert_eq!(db.list_files_by_kind_paged(FILE_KIND_MEDIA, 2, 2)?.len(), 1);
        assert_eq!(
            db.list_files_by_kind_paged(FILE_KIND_MEDIA, 10, 0)?.len(),
            3
        );
        let (total, media, storage) = db.count_files()?;
        assert_eq!((total, media, storage), (3, 3, 0));
        Ok(())
    }

    #[test]
    fn reorden_y_pelicula_unica() -> anyhow::Result<()> {
        let db = Db::open_in_memory()?;
        for (p, h) in [
            ("s/e01.mkv", "h1"),
            ("s/e02.mkv", "h2"),
            ("s/film.mkv", "h3"),
        ] {
            db.upsert_file(&FileRow {
                path: p.into(),
                hash: h.into(),
                size: 10,
                mime: "video/x-matroska".into(),
                policy: Policy::StreamOnly,
                host_id: "host1".into(),
                tag: format!("t{h}"),
                title: String::new(),
                watched: false,
                kind: FILE_KIND_MEDIA.into(),
            })?;
        }
        let serie = db.create_collection("series", "Re:Zero", "")?;
        let t1 = db.create_collection("season", "Temporada 1", &serie.id)?;
        let peli = db.create_collection("movie", "Película: arco final", &serie.id)?;
        // Hijas en orden de creación; reordenar invierte.
        let det = db.collection_detail(&serie.id)?.unwrap();
        assert_eq!(det.children.len(), 2);
        db.reorder_children(&serie.id, &[peli.id.clone(), t1.id.clone()])?;
        let det = db.collection_detail(&serie.id)?.unwrap();
        assert_eq!(det.children[0].collection.id, peli.id);
        assert_eq!(det.children[1].collection.id, t1.id);
        // Episodios: anexar con pos=0 asigna al final; reordenar persiste.
        db.add_item(&t1.id, "s/e01.mkv", "", "", 0)?;
        db.add_item(&t1.id, "s/e02.mkv", "", "", 0)?;
        let items = db.collection_detail(&t1.id)?.unwrap().items;
        assert_eq!(items[0].file_path, "s/e01.mkv");
        db.reorder_items(&t1.id, &["s/e02.mkv".to_string(), "s/e01.mkv".to_string()])?;
        let items = db.collection_detail(&t1.id)?.unwrap().items;
        assert_eq!(items[0].file_path, "s/e02.mkv");
        assert_eq!(items[1].file_path, "s/e01.mkv");
        // Película hija: un solo archivo.
        db.add_item(&peli.id, "s/film.mkv", "", "", 0)?;
        assert!(db.add_item(&peli.id, "s/e01.mkv", "", "", 0).is_err());
        // Película suelta raíz: también única.
        let suelta = db.create_collection("movie", "Akira", "")?;
        db.add_item(&suelta.id, "s/film.mkv", "", "", 0)?;
        assert!(db.collection_detail(&peli.id)?.unwrap().items.is_empty());
        assert!(db.add_item(&suelta.id, "s/e02.mkv", "", "", 0).is_err());
        Ok(())
    }

    #[test]
    fn separa_media_y_storage_por_kind() -> anyhow::Result<()> {
        let db = Db::open_in_memory()?;
        db.upsert_file(&FileRow {
            path: "cap01.mkv".into(),
            hash: "h-media".into(),
            size: 10,
            mime: "video/x-matroska".into(),
            policy: Policy::StreamOnly,
            host_id: "h".into(),
            tag: "t1".into(),
            title: String::new(),
            watched: false,
            kind: FILE_KIND_MEDIA.into(),
        })?;
        db.upsert_file(&FileRow {
            path: "manual.pdf".into(),
            hash: "h-file".into(),
            size: 20,
            mime: "application/pdf".into(),
            policy: Policy::Mirror,
            host_id: "h".into(),
            tag: "t2".into(),
            title: String::new(),
            watched: false,
            kind: FILE_KIND_FILE.into(),
        })?;
        assert_eq!(db.list_files()?.len(), 2);
        let media = db.list_files_by_kind(FILE_KIND_MEDIA)?;
        assert_eq!(media.len(), 1);
        assert_eq!(media[0].path, "cap01.mkv");
        let files = db.list_files_by_kind(FILE_KIND_FILE)?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "manual.pdf");
        assert!(files[0].policy.allows_download());
        Ok(())
    }

    #[test]
    fn migra_db_vieja_sin_parent_id() -> anyhow::Result<()> {
        // Regresión: `tsinas` caía al arrancar con una DB creada antes de las
        // sub-colecciones ("no such column: parent_id" en el índice).
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("vieja.db");
        {
            let conn = rusqlite::Connection::open(&path)?;
            conn.execute_batch(
                "CREATE TABLE files(path TEXT PRIMARY KEY, hash TEXT NOT NULL,
                    size INTEGER NOT NULL, mime TEXT NOT NULL DEFAULT '',
                    policy TEXT NOT NULL DEFAULT 'stream_only',
                    host_id TEXT NOT NULL DEFAULT '', tag TEXT NOT NULL DEFAULT '',
                    title TEXT NOT NULL DEFAULT '');
                 CREATE TABLE settings(key TEXT PRIMARY KEY, val TEXT NOT NULL);
                 CREATE TABLE collections(id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL DEFAULT 'series', title TEXT NOT NULL DEFAULT '',
                    poster_url TEXT NOT NULL DEFAULT '', poster_file TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL DEFAULT 0);
                 CREATE TABLE collection_items(collection_id TEXT NOT NULL,
                    file_path TEXT NOT NULL, season TEXT NOT NULL DEFAULT '',
                    label TEXT NOT NULL DEFAULT '', pos INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(collection_id, file_path));
                 INSERT INTO collections(id,kind,title,created_at)
                    VALUES('c1','series','Frieren',1);",
            )?;
        }
        // Abrir con el esquema actual debe migrar sin error y conservar datos.
        let db = Db::open(&path)?;
        let cols = db.list_collections()?;
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].title, "Frieren");
        assert!(cols[0].parent_id.is_empty());
        // Y ya se pueden crear hijas sobre la colección migrada.
        let kid = db.create_collection("season", "Temporada 1", "c1")?;
        assert_eq!(kid.parent_id, "c1");
        Ok(())
    }
}
