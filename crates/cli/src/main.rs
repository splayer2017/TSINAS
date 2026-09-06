//! p2p-serve: expone un archivo como StreamOnly y lo sirve por gateway loopback.
//!
//! Uso:
//!   p2p-serve <ARCHIVO> [--data-dir ./data-nodo] [--policy stream_only|mirror|host_only]
//!
//! Flujo: Node::spawn -> add_path -> doc nuevo -> set_hash -> imprime ticket+hash+URL.
//! El visor (otro nodo con el ticket) hace sync de metadatos y reproduce vía
//! GET /stream/<hash> con Range, sin persistir a disco.

use std::path::PathBuf;

use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use p2p_nube_core::{db::FileRow, gateway::Gateway, tailnet, Node, Policy};
use p2p_nube_vault::{guess_mime, SharedFile};

fn usage() -> ! {
    eprintln!(
        "Uso: p2p-serve <ARCHIVO> [--data-dir DIR] [--policy stream_only|mirror|host_only]\n\
         [--port 37491] [--bind IP] [--domain DOM] [--http-local]\n\
         Ejemplo: p2p-serve ./video.mp4 --data-dir ./data-nodo --policy stream_only\n\
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

    let mut args = std::env::args().skip(1);
    let file: PathBuf = match args.next() {
        Some(a) if !a.starts_with("--") => PathBuf::from(a),
        _ => usage(),
    };
    let mut data_dir = PathBuf::from("./data-nodo");
    let mut policy = Policy::StreamOnly;
    let mut port: u16 = DEV_PORT_DEFAULT;
    let mut bind: Option<String> = None;
    let mut domain: Option<String> = None;
    let mut http_local = false;
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
            _ => usage(),
        }
    }

    // iroh-blobs exige rutas absolutas: resolvemos antes de crear el nodo.
    // `canonicalize` además valida que el archivo existe (resuelve symlinks).
    let file: PathBuf = match std::fs::canonicalize(&file) {
        Ok(p) => p,
        Err(_) => anyhow::bail!("no existe el archivo: {}", file.display()),
    };
    let data_dir: PathBuf = std::path::absolute(&data_dir)?;

    let size = std::fs::metadata(&file)?.len();
    let name = file
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "archivo".into());

    let node = Node::spawn(data_dir.clone()).await?;
    println!("endpoint id : {}", node.endpoint_id());
    match tailnet::tailnet_ipv4() {
        Ok(ip) => println!("tailnet ip  : {ip}  (verifica con `tailscale status`)"),
        Err(e) => println!("tailnet ip  : no detectada ({e}) — ¿tailscaled activo?"),
    }
    if let Some(dns) = tailnet::magic_dns() {
        println!("magic dns   : {dns}");
    }

    // 1. Pineamos el archivo en el store local (solo el host lo tiene).
    let tag = node.blobs_store.blobs().add_path(&file).await?;
    println!("blob hash   : {}", tag.hash);
    println!("tamaño      : {size} bytes");

    // 2. Biblioteca nueva (doc) + entrada que referencia el blob.
    let author = node.docs.author_default().await?;
    let doc = node.docs.create().await?;
    doc.set_hash(author, name.as_bytes().to_vec(), tag.hash, size)
        .await?;
    let ticket = doc
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?;
    println!("doc ticket  : {ticket}");

    // 3. Proyección local + metadato JSON (lo que el móvil verá).
    let shared = SharedFile {
        path: name.clone(),
        hash: tag.hash.to_string(),
        size,
        mime: guess_mime(&name),
        policy,
        host_id: node.endpoint_id(),
    };
    node.db.upsert_file(&FileRow {
        path: shared.path.clone(),
        hash: shared.hash.clone(),
        size,
        mime: shared.mime.clone(),
        policy,
        host_id: shared.host_id.clone(),
    })?;
    println!(
        "política    : {} (allows_download={})",
        policy.as_str(),
        policy.allows_download()
    );

    // 4. Servidor web + UI + API. El tag se mantiene vivo anti-GC.
    let _tag_guard = tag;
    let endpoint_id = node.endpoint_id();
    let hash_s = shared.hash.clone();
    if http_local {
        // Fallback: HTTP loopback efímero (sin TLS, solo este PC).
        let (base, _handle) = Gateway::serve_loopback(
            node.blobs_store.clone(),
            node.db.clone(),
            endpoint_id.clone(),
        )
        .await?;
        println!("\n== UI WEB LOCAL (sin TLS) ==");
        println!("  {base}/   (lista + reproductor)");
        println!("  curl -s {base}/api/files");
        println!("  mpv {base}/stream/{hash_s}");
    } else {
        // Modo Dev: HTTPS fijo en https://<domain>:<port> bindeado a la IP tailnet.
        let domain = domain
            .or_else(tailnet::magic_dns)
            .unwrap_or_else(|| DEV_DOMAIN_DEFAULT.into());
        let bind_ip: std::net::IpAddr = match bind {
            Some(b) => b.parse().unwrap_or_else(|_| usage()),
            None => tailnet::tailnet_ipv4()
                .unwrap_or_else(|e| {
                    eprintln!("sin IP tailnet ({e}); usa --bind 127.0.0.1 --http-local o revisa tailscaled");
                    std::process::exit(1);
                })
                .parse()
                .unwrap_or_else(|_| usage()),
        };
        let (cert, key) = ensure_tailnet_cert(&data_dir, &domain)?;
        let addr = std::net::SocketAddr::new(bind_ip, port);
        let (_handle, bound) = Gateway::serve_tls(
            node.blobs_store.clone(),
            node.db.clone(),
            endpoint_id.clone(),
            addr,
            &cert,
            &key,
        )
        .await?;
        let _ = bound;
        let url = format!("https://{domain}:{port}");
        println!("\n== MODO DEV (URL fija) ==");
        println!("  UI      : {url}/");
        println!("  API     : {url}/api/files  {url}/api/info");
        println!("  curl    : curl {url}/health   (cert tailnet válido, sin -k)");
        println!("  Celular : abre {url}/ en Chrome Android (misma tailnet) → ▶ Reproducir + seek");
        println!("  Local   : también responde en https://{bind_ip}:{port}/ (cert del dominio)");
    }
    println!("\n== TICKET P2P (para el futuro visor nativo / 2º nodo) ==");
    println!("  {ticket}");

    tokio::signal::ctrl_c().await?;
    println!("\ncerrando…");
    node.endpoint.close().await;
    Ok(())
}
