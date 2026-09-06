# Registro Continuo de Auditoría de Arquitectura — TSINAS (p2p-nube)

Última actualización: 2026-09-06
ID de sesión activa: TSINAS-ARCH-20260906-01

## Estado de Hallazgos

| ID | Título | Severidad | Estado | Primera Detección |
|---|---|---|---|---|
| BLOCK-01 | Desconexión Arquitectónica Total entre Gateway/Library y Swarm P2P (iroh-docs/gossip) | BLOQUEADOR ESTRUCTURAL | Abierto | TSINAS-ARCH-20260906-01 |
| BLOCK-02 | Política de Descarga por Defecto (`DownloadPolicy::Everything`) en `iroh-docs` anula el Modo `StreamOnly` | BLOQUEADOR ESTRUCTURAL | Abierto | TSINAS-ARCH-20260906-01 |
| BLOCK-03 | Staging No Acotado de Subidas en `/tmp` (tmpfs) Sin Limpieza RAII (Riesgo Crítico de OOM y Fuga de Disco) | BLOQUEADOR ESTRUCTURAL | Abierto | TSINAS-ARCH-20260906-01 |
| HIGH-01 | Bug Funcional en Parsing de Rangos HTTP Suffix (`bytes=-N`) en Streaming de Video | DEUDA ALTA | Abierto | TSINAS-ARCH-20260906-01 |
| HIGH-02 | Bloqueo Síncrono de Hilos Tokio por Mutex de SQLite (`rusqlite::Connection`) en el Pipeline Crítico de Streaming (`mime_for`) | DEUDA ALTA | Abierto | TSINAS-ARCH-20260906-01 |
| HIGH-03 | Bloqueo de Tokio por Recorrido Recursivo Síncrono del Filesystem (`std::fs::read_dir`) en Escaneo de Carpetas | DEUDA ALTA | Abierto | TSINAS-ARCH-20260906-01 |
| HIGH-04 | Tamaño de Buffer Subóptimo (4 KiB) en `ReaderStream` Degradando el Rendimiento en Streaming de Alto Bitrate | DEUDA ALTA | Abierto | TSINAS-ARCH-20260906-01 |
| HIGH-05 | Fuga de Memoria por Crecimiento No Acotado del Registro de Tareas (`Jobs`) | DEUDA ALTA | Abierto | TSINAS-ARCH-20260906-01 |
| HIGH-06 | Ausencia de Capas de Timeout, Límites de Concurrencia y Protección Slowloris en Servidor Axum/TLS | DEUDA ALTA | Abierto | TSINAS-ARCH-20260906-01 |
| HIGH-07 | Compartición Insegura de Tickets P2P con Permisos de Escritura Plena (`ShareMode::Write`) por Defecto | DEUDA ALTA | Abierto | TSINAS-ARCH-20260906-01 |
| MED-01 | Inconsistencia en Eliminación de Archivos con Mismo Hash (`remove_by_hash`) y Fuga de Tags al Sobrescribir | DEUDA MODERADA | Abierto | TSINAS-ARCH-20260906-01 |
| MED-02 | Invocación Síncrona Bloqueante del CLI de Tailscale (`Command::new("tailscale")`) | DEUDA MODERADA | Abierto | TSINAS-ARCH-20260906-01 |
| LOW-01 | Código Muerto, Duplicación de Lógica MIME y Crate Redundante `crates/vault` | MENOR / HIGIENE / DRY | Abierto | TSINAS-ARCH-20260906-01 |
| LOW-02 | Advertencias de Clippy, Enums sin Traits Estándar y Doble Serialización JSON | MENOR / HIGIENE | Abierto | TSINAS-ARCH-20260906-01 |
| FUT-01 | Sobrecarga de Gossip y Tráfico QUIC sin Límite al Superar >500 Peers Concurrentes | RIESGO FUTURO | Pendiente Gatillo | TSINAS-ARCH-20260906-01 |
| FUT-02 | Falta de Transcodificación Dinámica para Streaming 4K HEVC/AV1 hacia Clientes Móviles Web Heterogéneos | RIESGO FUTURO | Pendiente Gatillo | TSINAS-ARCH-20260906-01 |
| FUT-03 | Agotamiento de Memoria y Latencia en API `/api/files` con Bibliotecas de >10,000 Archivos (Falta de Paginación) | RIESGO FUTURO | Pendiente Gatillo | TSINAS-ARCH-20260906-01 |
| UNVERIF-01 | Rendimiento de I/O de red QUIC en escenarios de pérdida masiva de paquetes (>15% packet loss) a través de DERP relays públicos de N0 | NO VERIFICADO | Requiere Benchmarks | TSINAS-ARCH-20260906-01 |
