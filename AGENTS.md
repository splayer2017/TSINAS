# AGENTS.md — p2p-nube

Nube P2P privada (Tailscale + iroh). MVP: PC Linux aloja, Android reproduce por streaming sin descargar.

## Comandos

```bash
cargo test -p p2p-nube-core --lib        # rápido: db + nodo + gateway
cargo test -p p2p-nube-core --test two_nodes  # P2P real vía relays N0 (~5s, requiere internet)
cargo test --workspace
cargo fmt                                  # siempre antes de terminar
cargo check --workspace
```

No hay CI, lint ni codegen. Detalle de flujos en `README.md` (modo Dev, celular, PWA).

## Layout

- `crates/core`: `Node` (endpoint+router), `Db` (SQLite), `Gateway` (UI+API+stream), `tailnet`. Fuente de verdad = `iroh-docs`; SQLite es solo proyección.
- `crates/core/web`: UI embebida por `include_str!` (`index.html`, `manifest.json`, `sw.js`). Sin build npm: editar directo.
- `crates/cli`: binario `p2p-serve` (único entrypoint ejecutable).

## Versiones pinnadas (no subir por separado)

- Set compatible verificado: `iroh 1.1.0 + iroh-blobs 0.103 + iroh-docs 0.101 + iroh-gossip 0.101` (docs exige `iroh ^1` y `blobs ^0.103`).
- `axum 0.7 ↔ axum-server 0.7` (el 0.8 rompe). `rustls 0.23`.

## Gotchas iroh (ya mordieron una vez)

- `BlobsProtocol` está en la raíz (`iroh_blobs::BlobsProtocol`); `net_protocol` es módulo privado.
- `Doc::share` toma 2 args: `(ShareMode, AddrInfoOptions)` en `iroh_docs::api::protocol`.
- `BlobReader` NO soporta `SeekFrom::End` → tamaño vía `blobs.status()` → `BlobStatus::Complete { size }` (`gateway.rs`).
- `blobs.add_path` exige ruta **absoluta** → el CLI hace `canonicalize`/`absolute` (`cli/src/main.rs`).
- `Node::spawn` debe crear `blobs/` y `docs/` antes (`redb`/FsStore fallan si no existen).
- Conflicto de providers rustls (ring←iroh vs aws-lc-rs←axum-server): hay que llamar `rustls::crypto::ring::default_provider().install_default()` antes de `RustlsConfig::from_pem_file`.

## Convenciones y entorno

- `reqwest` dev no tiene feature `json` → usar `.text()` + `serde_json::from_str` en tests.
- Test TLS usa cert autofirmado (`rcgen`) + `danger_accept_invalid_certs`.
- `StreamOnly` = sin persistencia en el visor, NO es DRM.
- Dev fijo: `https://uriel-1.tail7345d6.ts.net:37491`, bind a IP tailnet (nunca `0.0.0.0`). `tailscale cert` requiere sudo (sin passwordless aquí): emitir una vez a `./data-nodo/tls.{crt,key}` + `chown`; el binario los reutiliza si tienen <60 días.
- Sin Flutter/Android SDK en esta máquina (APK diferida). `cmake/cc/perl` necesarios: compilan `aws-lc-rs`.
- Sesión bash: un `&` en background cuelga la herramienta — probar servidor+cliente en un solo comando y hacer `kill` al final.
