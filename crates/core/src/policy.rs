use serde::{Deserialize, Serialize};

/// Política de alojamiento/transmisión por fichero o carpeta.
///
/// - `HostOnly`: solo el host lo guarda, no se anuncia para descarga.
/// - `StreamOnly`: se anuncia metadato + se sirve por rangos, el visor
///   no persiste a disco (solo RAM). Sin botón descargar en UI.
/// - `Mirror`: sincronización completa (descarga permitida).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    HostOnly,
    StreamOnly,
    Mirror,
}

impl Default for Policy {
    fn default() -> Self {
        Self::StreamOnly
    }
}

impl Policy {
    pub fn allows_download(self) -> bool {
        matches!(self, Self::Mirror)
    }

    pub fn allows_stream(self) -> bool {
        matches!(self, Self::StreamOnly | Self::Mirror)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::HostOnly => "host_only",
            Self::StreamOnly => "stream_only",
            Self::Mirror => "mirror",
        }
    }

    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "host_only" => Ok(Self::HostOnly),
            "stream_only" => Ok(Self::StreamOnly),
            "mirror" => Ok(Self::Mirror),
            _ => anyhow::bail!("policy desconocida: {s}"),
        }
    }
}
