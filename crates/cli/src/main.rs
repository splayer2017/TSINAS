//! TSINAS: expone un archivo como StreamOnly y lo sirve por gateway loopback.
//!
//! Uso:
//!   TSINAS <ARCHIVO> [--data-dir ./data-nodo] [--policy stream_only|mirror|host_only]
//!
//! Flujo: Node::spawn -> add_path -> doc nuevo -> set_hash -> imprime ticket+hash+URL.
//! El visor (otro nodo con el ticket) hace sync de metadatos y reproduce vía
//! GET /stream/<hash> con Range, sin persistir a disco.

use std::path::PathBuf;
use std::sync::Arc;

use p2p_nube_core::{gateway::Gateway, tailnet, Library, Node, Policy};

fn usage() -> ! {
    eprintln!(
        "Uso: TSINAS [ARCHIVO] [--data-dir DIR] [--policy stream_only|mirror|host_only]\n\
         [--port 37491] [--bind IP] [--domain DOM] [--http-local]\n\
         [--media-roots DIR1,DIR2]  (rutas permitidas para añadir desde la web; vacío = sin restricción)\n\
         Sin ARCHIVO arranca con la biblioteca vacía y los vídeos se añaden\n\
         desde la web (pestaña Añadir (host)).\n\
         Ejemplo: TSINAS ./video.mp4 --data-dir ./data-nodo --policy stream_only\n\
         Por defecto sirve HTTPS en https://<domain>:<port> bindeado a tu IP tailnet (modo Dev)."
    );
    std::process::exit(2);
}

/// URL fija del modo Dev.
const DEV_DOMAIN_DEFAULT: &str = "uriel-1.tail7345d6.ts.net";
const DEV_PORT_DEFAULT: u16 = 37491;

/// Garantiza el cert TLS del tailnet.
/// Si ya existen y tienen menos de 60 dias, se reutilizan sin llamar a tailscale.
/// Si no, intenta `tailscale cert`; como suele requerir sudo, ante el fallo
/// imprime el comando exacto a ejecutar una sola vez y aborta.
fn ensure_tailnet_cert(
    data_dir: &std::path::Path,
    domain: &str,
) -> anyhow::Result<(PathBuf, PathBuf)> {
    let cert = data_dir.join("tls.crt");
    let key = data_dir.join("tls.key");
    if cert.is_file() && key.is_file() {
        let fresh = std::fs::metadata(&cert)
            .and_then(|m| m.modified())
            .map(|t| {
                std::time::SystemTime::now()
                    .duration_since(t)
                    .map(|d| d.as_secs() < 60 * 86400)
                    .unwrap_or(false)
            })
            .unwrap_or(false);
        if fresh {
            return Ok((cert, key));
        }
    }
    let out = std::process::Command::new("tailscale")
        .args([
            "cert",
            "--cert-file",
            &cert.to_string_lossy(),
            "--key-file",
            &key.to_string_lossy(),
            "--min-validity",
            "720h",
            domain,
        ])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "`tailscale cert` falló para {domain}: {}\n\
             Ejecuta UNA vez (pide contraseña) y reintenta:\n\
               sudo tailscale cert --cert-file {} --key-file {} {domain}\n\
               sudo chown $USER:$USER {} {}",
            String::from_utf8_lossy(&out.stderr).trim(),
            cert.display(),
            key.display(),
            cert.display(),
            key.display(),
        );
    }
    Ok((cert, key))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let raw: Vec<String> = std::env::args().skip(1).collect();
    // Archivo inicial opcional: primer argumento si no es un flag.
    // Sin él se arranca con la biblioteca vacía y todo se añade desde la web.
    let mut rest: Vec<String> = raw;
    let file: Option<PathBuf> = match rest.first() {
        Some(a) if !a.starts_with("--") => Some(PathBuf::from(rest.remove(0))),
        _ => None,
    };
    let mut args = rest.into_iter();
    let mut data_dir = PathBuf::from("./data-nodo");
    let mut policy = Policy::StreamOnly;
    let mut port: u16 = DEV_PORT_DEFAULT;
    let mut bind: Option<String> = None;
    let mut domain: Option<String> = None;
    let mut http_local = false;
    let mut media_roots_raw: Option<String> = None;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--data-dir" => data_dir = PathBuf::from(args.next().unwrap_or_else(|| usage())),
            "--policy" => {
                policy = Policy::parse(&args.next().unwrap_or_else(|| usage()))?;
            }
            "--port" => {
                port = args
                    .next()
                    .unwrap_or_else(|| usage())
                    .parse()
                    .unwrap_or_else(|_| usage());
            }
            "--bind" => bind = Some(args.next().unwrap_or_else(|| usage())),
            "--domain" => domain = Some(args.next().unwrap_or_else(|| usage())),
            "--http-local" => http_local = true,
            "--media-roots" => media_roots_raw = Some(args.next().unwrap_or_else(|| usage())),
            _ => usage(),
        }
    }

    // Raíces permitidas para los endpoints de escritura de la web.
    let mut media_roots: Vec<PathBuf> = Vec::new();
    if let Some(raw) = media_roots_raw {
        for r in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            match std::fs::canonicalize(r) {
                Ok(p) => media_roots.push(p),
                Err(_) => anyhow::bail!("media-root no existe: {r}"),
            }
        }
    }

    // iroh-blobs exige rutas absolutas: resolvemos antes de crear el nodo.
    // `canonicalize` además valida que el archivo existe (resuelve symlinks).
    let file: Option<PathBuf> = match file {
        Some(f) => match std::fs::canonicalize(&f) {
            Ok(p) => Some(p),
            Err(_) => anyhow::bail!("no existe el archivo: {}", f.display()),
        },
        None => None,
    };
    let data_dir: PathBuf = std::path::absolute(&data_dir)?;

    let node = Node::spawn(data_dir.clone()).await?;
    println!("endpoint id : {}", node.endpoint_id());
    match tailnet::tailnet_ipv4_async().await {
        Ok(ip) => println!("tailnet ip  : {ip}  (verifica con `tailscale status`)"),
        Err(e) => println!("tailnet ip  : no detectada ({e}) — ¿tailscaled activo?"),
    }
    if let Some(dns) = tailnet::magic_dns_async().await {
        println!("magic dns   : {dns}");
    }

    // BLOCK-01: Obtiene o crea la biblioteca activa persistida en iroh-docs/SQLite.
    // HIGH-07: Emite ticket con ShareMode::Read por defecto.
    let (doc, author, ticket_s) = node.get_or_create_library().await?;

    // BLOCK-03: Directorio de staging en almacenamiento persistente dentro de data_dir
    let staging_dir = data_dir.join("staging");
    let _ = std::fs::create_dir_all(&staging_dir);

    // Biblioteca compartida CLI↔web: conectada al doc P2P para sincronización continua.
    let library = Arc::new(
        Library::new(
            node.blobs_store.clone(),
            node.db.clone(),
            node.endpoint_id(),
        )
        .with_media_roots(media_roots.clone())
        .with_staging_dir(staging_dir)
        .with_doc(doc.clone(), author),
    );

    // Hash del archivo inicial, si lo hubo (para las URLs de ejemplo).
    let mut hash_s = String::new();
    if let Some(f) = file.as_ref() {
        // MIN-03: política directa en la importación (un solo upsert).
        let added = library
            .add_file_with_policy(&f.to_string_lossy(), None, policy)
            .await?;
        println!("blob hash   : {}", added.hash);
        println!("tamaño      : {} bytes", added.size);
        println!(
            "política    : {} (allows_download={})",
            policy.as_str(),
            policy.allows_download()
        );
        hash_s = added.hash.clone();
    } else {
        println!("biblioteca  : vacía — añade vídeos desde la pestaña Añadir (host) de la web");
    }

    // Servidor web + UI + API conectado a la biblioteca P2P.
    let endpoint_id = node.endpoint_id();
    let _ = endpoint_id;
    if http_local {
        // Fallback: HTTP loopback efímero (sin TLS, solo este PC).
        let (base, _handle) = Gateway::serve_loopback_with_library(library.clone()).await?;
        println!("\n== UI WEB LOCAL (sin TLS) ==");
        println!("  {base}/   (lista + reproductor)");
        println!("  curl -s {base}/api/files");
        if !hash_s.is_empty() {
            println!("  mpv {base}/stream/{hash_s}");
        }
    } else {
        // Modo Dev: HTTPS fijo en https://<domain>:<port> bindeado a la IP tailnet.
        // MIN-04: versión async (sin bloquear el runtime en arranque).
        let domain = match domain {
            Some(d) => d,
            None => tailnet::magic_dns_async()
                .await
                .unwrap_or_else(|| DEV_DOMAIN_DEFAULT.into()),
        };
        let bind_ip: std::net::IpAddr = match bind {
            Some(b) => b.parse().unwrap_or_else(|_| usage()),
            None => tailnet::tailnet_ipv4_async().await.unwrap_or_else(|e| {
                    eprintln!("sin IP tailnet ({e}); usa --bind 127.0.0.1 --http-local o revisa tailscaled");
                    std::process::exit(1);
                })
                .parse()
                .unwrap_or_else(|_| usage()),
        };
        let (cert, key) = ensure_tailnet_cert(&data_dir, &domain)?;
        let addr = std::net::SocketAddr::new(bind_ip, port);
        let (_handle, bound) =
            Gateway::serve_tls_with_library(library.clone(), addr, &cert, &key).await?;
        let _ = bound;
        let url = format!("https://{domain}:{port}");
        println!("\n== MODO DEV (URL fija) ==");
        println!("  UI      : {url}/");
        println!("  API     : {url}/api/files  {url}/api/info");
        println!("  curl    : curl {url}/health   (cert tailnet válido, sin -k)");
        println!("  Celular : abre {url}/ en Chrome Android (misma tailnet) → ▶ Reproducir + seek");
        println!("  Local   : también responde en https://{bind_ip}:{port}/ (cert del dominio)");
    }
    println!("\n== TICKET P2P (para el visor nativo / 2º nodo) ==");
    println!("  {ticket_s}");

    tokio::signal::ctrl_c().await?;
    println!("\ncerrando…");
    node.endpoint.close().await;
    Ok(())
}
