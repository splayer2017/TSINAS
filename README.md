# p2p-nube — nube P2P privada (Tailscale + iroh)

MVP: **Baúl + streaming video**. PC Linux aloja, Android reproduce sin descargar.

## Stack

- Core Rust: `tokio + iroh(1.1) + iroh-blobs(0.103) + iroh-docs(0.101) + iroh-gossip(0.101)`, `SQLite(rusqlite)` como índice proyectado.
- UI futura: Flutter + `flutter_rust_bridge` (este repo expone el core listo; Flutter SDK no instalado en esta máquina).
- Red: se exige `tailscaled` instalado. Cifrado en tránsito = WireGuard (tailnet) + QUIC/TLS de iroh. Sin E2E extra en MVP (decisión).

## Layout

- `crates/core`: `Node` (endpoint+router 3 protocolos), `Policy`, `Db`, `Gateway` HTTP Range en loopback, `tailnet` helpers.
- `crates/core/web`: UI web embebida (`index.html` + `manifest.json` + `sw.js`, sin build de npm).
- `crates/vault`: módulo MVP (`Manifest`, `SharedFile`).
- `crates/cli`: binario `p2p-serve` para pruebas manuales con archivos reales.

## Cómo probar correctamente

```bash
# 1. Unitarios (db + nodo efímero + gateway con rangos)
cargo test -p p2p-nube-core --lib

# 2. P2P 2 nodos (A crea biblioteca, B la importa vía ticket y sincroniza;
#    usa relays N0 reales, tarda ~5s)
cargo test -p p2p-nube-core --test two_nodes

# 3. Todo
cargo test --workspace
```

Prueba manual con archivo real (verificado: UI 200 + API + 200 completo + 206 parcial):

```bash
cargo build -p p2p-nube-cli
./target/debug/p2p-serve /tmp/video-prueba.bin --data-dir /tmp/data-nodo-a --policy stream_only
# En otra terminal (usa la URL que imprime):
xdg-open http://127.0.0.1:PORT/   # UI: lista + reproductor
curl -s http://127.0.0.1:PORT/api/files   # JSON de archivos marcados
curl -i http://127.0.0.1:PORT/stream/<hash> -o /tmp/full.bin   # 200, no-store
curl -i -H "Range: bytes=0-1023" http://127.0.0.1:PORT/stream/<hash>  # 206 + Content-Range
mpv http://127.0.0.1:PORT/stream/<hash>   # reproduce sin descargar
```

## Modo Dev: URL fija https://uriel-1.tail7345d6.ts.net:37491

El servidor sirve HTTPS directamente (TLS nativo con cert de Tailscale) en el puerto
fijo `37491`, bindeado a tu IP tailnet. Siempre la misma URL, en PC y celular.

Requisito único (una sola vez, pide contraseña): emitir el cert del tailnet:

```bash
sudo tailscale cert --cert-file ./data-nodo/tls.crt --key-file ./data-nodo/tls.key uriel-1.tail7345d6.ts.net
sudo chown $USER:$USER ./data-nodo/tls.crt ./data-nodo/tls.key
```

(El binario reutiliza esos archivos mientras tengan menos de 60 días; al arrancar los
refresca solo con `tailscale cert --min-validity 720h`, sin sudo.)

## Probar en el celular (web, sin instalar nada)

Requisitos en el celular: app **Tailscale** instalada, logueada en la **misma tailnet** y en estado activo
(`tailscale status` en el PC debe mostrarlo como online). Nada más que instalar: es una web.

1. En el PC, arranca el servidor (el video inicial es opcional):
    ```bash
    tsinas on     # enciende la web en segundo plano (equivale al p2p-serve largo)
    tsinas off    # la apaga
    ```
    Detalle manual (si prefieres el comando largo):
    ```bash
    ./target/debug/p2p-serve ./mi-video.mp4 --data-dir ./data-nodo --policy stream_only
    # o sin archivo: todo se añade después desde la web
    ./target/debug/p2p-serve --data-dir ./data-nodo
    ```
2. En el celular (Chrome Android), abre `https://uriel-1.tail7345d6.ts.net:37491/`.
3. Verás la lista del baúl: toca **▶ Reproducir**, prueba adelantar/retroceder (seek usa rangos).
4. Opcional: ⋮ > **Agregar a pantalla principal** (PWA; el service worker solo cachea la
   shell, nunca los videos ni la API).

Flags útiles: `--port`, `--bind IP`, `--domain` (por defecto los de Dev),
`--http-local` (HTTP loopback efímero sin TLS, solo este PC),
`--media-roots DIR1,DIR2` (restringe qué rutas aceptan los endpoints de escritura).

## Añadir videos desde la web (sin consola)

Abre la UI (desde el PC o desde el móvil) y ve a la pestaña **Añadir**:

- **Subir desde este dispositivo**: selector de archivos del navegador/móvil →
  el archivo se envía al servidor y aparece en la lista (tope 8 GiB por archivo).
- **Archivo**: pega la ruta del servidor (`/home/uriel/Videos/cap07.mkv`) → aparece en la lista.
- **Carpeta**: pega la carpeta (`/home/uriel/Anime/Temporada1`) → **Escanear** lista
  solo los vídeos (ignora `.txt`, etc.), en orden natural (`cap2 < cap10`), con
  barra de progreso (el hasheo de 24 MKV tarda minutos y corre en background).
- **Quitar**: cada fila tiene botón que la saca de la lista y des-pinea el blob;
  el espacio se libera en ~1 min (GC cada 60 s). Nunca borra tu archivo original.

Sin roles: no hay usuarios ni admin. La red tailnet ya es privada (solo tus
dispositivos) y cifrada (WireGuard + QUIC/TLS de iroh), así que cualquier
dispositivo conectado puede gestionar la biblioteca.

Si algo falla: puerto ocupado → el arranque avisa y debes cerrar la otra instancia;
cert ausente/expirado → el arranque imprime el comando `sudo tailscale cert` a repetir;
host offline → lista vacía o 404 en play; rango inválido → 416.

## Flujo P2P (resumen)

1. Ambos dispositivos en el mismo tailnet (`tailscale status`).
2. PC: `Node::spawn(data_dir)` → `create_library()` → comparte ticket al móvil.
3. PC marca carpeta como `StreamOnly`: `add_path` + publica `SharedFile` en docs + gossip.
4. Móvil: recibe metadato (docs sync), NO descarga; reproduce vía `Gateway /stream/<hash>` con `Range`, `Cache-Control: no-store`.

> Nota: "sin descargar" = sin persistencia + sin botón descargar. No es DRM: un usuario avanzado podría capturar RAM/red.

## APK Flutter (fase 2, diferida — se conserva)

La vía web es para desarrollo y pruebas rápidas. La app nativa queda planificada sin cambios:
instalar Flutter SDK + Android SDK/NDK, `flutter_rust_bridge_codegen integrate`, superficie FRB
prevista (`spawn_node`, `import_library`, `list_files_stream`, `stream_url`, `node_info`),
pantallas Conexión/Lista/Player/Ajustes y `flutter build apk --debug` para `arm64-v8a`.
