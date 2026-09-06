use std::process::Command;

/// IP tailnet (100.x) de este dispositivo, vía CLI `tailscale ip -4` asíncrono (MED-02).
pub async fn tailnet_ipv4_async() -> anyhow::Result<String> {
    let out = tokio::process::Command::new("tailscale")
        .args(["ip", "-4"])
        .output()
        .await?;
    if !out.status.success() {
        anyhow::bail!(
            "`tailscale ip -4` falló: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let ip = String::from_utf8(out.stdout)?.trim().to_string();
    if ip.is_empty() {
        anyhow::bail!("sin IP tailnet (¿tailscale down?)");
    }
    Ok(ip)
}

/// Nombre MagicDNS asíncrono (p.ej. `pc-fesb.tailxxx.ts.net`), si está disponible (MED-02).
pub async fn magic_dns_async() -> Option<String> {
    let out = tokio::process::Command::new("tailscale")
        .args(["status", "--json"])
        .output()
        .await
        .ok()?;
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    v.get("Self")?
        .get("DNSName")?
        .as_str()
        .map(|s| s.trim_end_matches('.').to_string())
}

/// IP tailnet (100.x) de este dispositivo, vía CLI `tailscale ip -4`.
/// En MVP exigimos `tailscaled` instalado y logueado; no embebemos tsnet.
pub fn tailnet_ipv4() -> anyhow::Result<String> {
    let out = Command::new("tailscale").args(["ip", "-4"]).output()?;
    if !out.status.success() {
        anyhow::bail!(
            "`tailscale ip -4` falló: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let ip = String::from_utf8(out.stdout)?.trim().to_string();
    if ip.is_empty() {
        anyhow::bail!("sin IP tailnet (¿tailscale down?)");
    }
    Ok(ip)
}

/// Nombre MagicDNS (p.ej. `pc-fesb.tailxxx.ts.net`), si está disponible.
pub fn magic_dns() -> Option<String> {
    let out = Command::new("tailscale")
        .args(["status", "--json"])
        .output()
        .ok()?;
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
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
