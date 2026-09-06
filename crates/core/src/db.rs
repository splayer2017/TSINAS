use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::policy::Policy;

/// Fila de la proyección local (derivada de iroh-docs, no es fuente de verdad).
/// `tag` es el nombre del tag persistente en el blob store que protege el
/// contenido del GC; se borra al quitar el archivo (des-pinear).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRow {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub mime: String,
    pub policy: Policy,
    pub host_id: String,
    pub tag: String,
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
            tag TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            val TEXT NOT NULL
        );",
    )?;
    // Migración idempotente para DBs creadas antes de `tag`.
    let _ = conn.execute(
        "ALTER TABLE files ADD COLUMN tag TEXT NOT NULL DEFAULT ''",
        [],
    );
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
    })
}

impl Db {
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
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "INSERT INTO files(path,hash,size,mime,policy,host_id,tag)
             VALUES(?,?,?,?,?,?,?)
             ON CONFLICT(path) DO UPDATE SET hash=excluded.hash,size=excluded.size,
               mime=excluded.mime,policy=excluded.policy,host_id=excluded.host_id,
               tag=excluded.tag",
            params![
                row.path,
                row.hash,
                row.size as i64,
                row.mime,
                row.policy.as_str(),
                row.host_id,
                row.tag
            ],
        )?;
        Ok(())
    }

    pub fn list_files(&self) -> anyhow::Result<Vec<FileRow>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT path,hash,size,mime,policy,host_id,tag FROM files ORDER BY path")?;
        let rows = stmt
            .query_map([], row_from)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_by_hash(&self, hash: &str) -> anyhow::Result<Option<FileRow>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT path,hash,size,mime,policy,host_id,tag FROM files WHERE hash=?")?;
        let mut rows = stmt.query_map([hash], row_from)?;
        Ok(rows.next().transpose()?)
    }

    pub fn get_by_path(&self, path: &str) -> anyhow::Result<Option<FileRow>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT path,hash,size,mime,policy,host_id,tag FROM files WHERE path=?")?;
        let mut rows = stmt.query_map([path], row_from)?;
        Ok(rows.next().transpose()?)
    }

    pub fn get_rows_by_hash(&self, hash: &str) -> anyhow::Result<Vec<FileRow>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT path,hash,size,mime,policy,host_id,tag FROM files WHERE hash=?")?;
        let rows = stmt
            .query_map([hash], row_from)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Borra la fila por hash. Devuelve `true` si existía.
    pub fn delete_by_hash(&self, hash: &str) -> anyhow::Result<bool> {
        let conn = self.inner.lock().unwrap();
        let n = conn.execute("DELETE FROM files WHERE hash=?", [hash])?;
        Ok(n > 0)
    }

    /// Borra la fila por path. Devuelve `true` si existía.
    pub fn delete_by_path(&self, path: &str) -> anyhow::Result<bool> {
        let conn = self.inner.lock().unwrap();
        let n = conn.execute("DELETE FROM files WHERE path=?", [path])?;
        Ok(n > 0)
    }

    pub fn get_setting(&self, key: &str) -> anyhow::Result<Option<String>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare("SELECT val FROM settings WHERE key=?")?;
        let mut rows = stmt.query_map([key], |r| r.get(0))?;
        Ok(rows.next().transpose()?)
    }

    pub fn set_setting(&self, key: &str, val: &str) -> anyhow::Result<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "INSERT INTO settings(key, val) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET val=excluded.val",
            params![key, val],
        )?;
        Ok(())
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
        })?;
        let all = db.list_files()?;
        assert_eq!(all.len(), 1);
        assert!(!all[0].policy.allows_download());
        assert!(all[0].policy.allows_stream());
        assert!(!db.delete_by_hash("no-existe")?);
        assert!(db.delete_by_hash("abc")?);
        assert!(db.list_files()?.is_empty());

        // Settings test
        db.set_setting("active_doc", "doc-123")?;
        assert_eq!(db.get_setting("active_doc")?, Some("doc-123".into()));
        Ok(())
    }
}
