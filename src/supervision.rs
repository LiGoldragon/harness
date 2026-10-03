//! The `signal-persona` engine-management lifecycle on its own socket.
//!
//! The supervision socket carries only the `signal-persona` engine-management
//! `Query` and `Response`, one Signal frame each. It answers announce,
//! readiness, health and stop for the harness component; it is bound by the
//! engine beside the ordinary and meta listeners the daemon shell owns.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use signal_persona::{
    ComponentHealth, ComponentIdentity, ComponentKind, ComponentName, LifecycleQuery,
    Query as SupervisionQuery, Response as SupervisionResponse,
};
use tokio::net::{UnixListener, UnixStream};

use crate::Result;
use crate::wire::SignalWire;

const ENGINE_MANAGEMENT_PROTOCOL_VERSION: i64 = 1;

/// The identity and health this component announces on its supervision
/// socket.
#[derive(Debug, Clone, PartialEq)]
pub struct SupervisionProfile {
    name: ComponentName,
    kind: ComponentKind,
    health: ComponentHealth,
}

impl SupervisionProfile {
    pub fn harness() -> Self {
        Self {
            name: "harness".to_string(),
            kind: ComponentKind::Harness,
            health: ComponentHealth::Running,
        }
    }

    /// The engine-management reply to one lifecycle request.
    pub fn reply(&self, request: SupervisionQuery) -> SupervisionResponse {
        match request {
            SupervisionQuery::Announce(_) => SupervisionResponse::Identified(ComponentIdentity {
                component_name: self.name.clone(),
                component_kind: self.kind.clone(),
                engine_management_protocol_version: ENGINE_MANAGEMENT_PROTOCOL_VERSION,
                component_startup_error_option: None,
            }),
            SupervisionQuery::Query(LifecycleQuery::ReadinessStatus(_)) => {
                SupervisionResponse::Ready(None)
            }
            SupervisionQuery::Query(LifecycleQuery::HealthStatus(_)) => {
                SupervisionResponse::HealthReport(self.health.clone())
            }
            SupervisionQuery::Stop(_) => SupervisionResponse::StopAcknowledged(None),
        }
    }
}

/// The bound supervision socket: one listener, serving each connection's
/// lifecycle requests in order until the peer closes it.
#[derive(Debug)]
pub struct SupervisionListener {
    profile: SupervisionProfile,
    listener: UnixListener,
    wire: SignalWire,
}

impl SupervisionListener {
    /// Bind the supervision socket with its configured mode, replacing a stale
    /// socket file left by an earlier process.
    pub fn bind(profile: SupervisionProfile, socket: &Path, mode: u32) -> Result<Self> {
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(socket);
        let listener = UnixListener::bind(socket)?;
        std::fs::set_permissions(socket, std::fs::Permissions::from_mode(mode))?;
        Ok(Self {
            profile,
            listener,
            wire: SignalWire::default(),
        })
    }

    /// Serve every supervision connection until the runtime stops.
    pub async fn serve(self) {
        loop {
            let Ok((stream, _address)) = self.listener.accept().await else {
                continue;
            };
            let connection = SupervisionConnection {
                profile: self.profile.clone(),
                wire: self.wire,
            };
            let _connection = tokio::spawn(connection.serve(stream));
        }
    }
}

struct SupervisionConnection {
    profile: SupervisionProfile,
    wire: SignalWire,
}

impl SupervisionConnection {
    async fn serve(self, mut stream: UnixStream) {
        while let Ok(request) = self.wire.read_async::<SupervisionQuery>(&mut stream).await {
            let reply = self.profile.reply(request);
            if self.wire.write_async(&mut stream, &reply).await.is_err() {
                return;
            }
        }
    }
}

/// Where the supervision socket is bound, as the daemon configuration names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionSocket {
    path: PathBuf,
    mode: u32,
}

impl SupervisionSocket {
    pub fn new(path: impl Into<PathBuf>, mode: u32) -> Self {
        Self {
            path: path.into(),
            mode,
        }
    }

    pub fn bind(&self, profile: SupervisionProfile) -> Result<SupervisionListener> {
        SupervisionListener::bind(profile, &self.path, self.mode)
    }
}
