/// IP tailnet (100.x) de este dispositivo, vía CLI `tailscale ip -4`.
/// En MVP exigimos `tailscaled` instalado y logueado; no embebemos tsnet.
/// Variante síncrona: solo para el arranque del CLI (10–50 ms puntuales).
pub fn tailnet_ipv4() -> anyhow::Result<String> {
    let out = std::process::Command::new("tailscale")
        .args(["ip", "-4"])
        .output()?;
    parse_ipv4_out(&out.stdout, &out.stderr, out.status.success())
}

/// Variante asíncrona (no bloquea workers Tokio).
pub async fn tailnet_ipv4_async() -> anyhow::Result<String> {
    let out = tokio::process::Command::new("tailscale")
        .args(["ip", "-4"])
        .output()
        .await?;
    parse_ipv4_out(&out.stdout, &out.stderr, out.status.success())
}

fn parse_ipv4_out(stdout: &[u8], stderr: &[u8], success: bool) -> anyhow::Result<String> {
    if !success {
        anyhow::bail!(
            "`tailscale ip -4` falló: {}",
            String::from_utf8_lossy(stderr)
        );
    }
    let ip = String::from_utf8(stdout.to_vec())?.trim().to_string();
    if ip.is_empty() {
        anyhow::bail!("sin IP tailnet (¿tailscale down?)");
    }
    Ok(ip)
}

/// Nombre MagicDNS (p.ej. `pc-fesb.tailxxx.ts.net`), si está disponible.
/// Variante síncrona: solo para el arranque del CLI.
pub fn magic_dns() -> Option<String> {
    let out = std::process::Command::new("tailscale")
        .args(["status", "--json"])
        .output()
        .ok()?;
    parse_dns_json(&out.stdout)
}

/// Variante asíncrona (no bloquea workers Tokio).
pub async fn magic_dns_async() -> Option<String> {
    let out = tokio::process::Command::new("tailscale")
        .args(["status", "--json"])
        .output()
        .await
        .ok()?;
    parse_dns_json(&out.stdout)
}

fn parse_dns_json(stdout: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(stdout).ok()?;
    v.get("Self")?
        .get("DNSName")?
        .as_str()
        .map(|s| s.trim_end_matches('.').to_string())
}

/// Imprime el comando sugerido para exponer el gateway vía HTTPS tailnet.
/// No lo ejecuta: el usuario decide (requiere HTTPS habilitado en el tailnet).
pub fn serve_hint(local_port: u16) -> String {
    format!(
        "tailscale serve --bg {local_port}   # expone https://<magicdns>/ -> 127.0.0.1:{local_port}\n\
         tailscale cert <magicdns>           # cert local si prefieres tu propio reverse-proxy"
    )
}
