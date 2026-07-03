use std::{env, path::PathBuf};

use schema_rust::{
    MetaListenerTier, NexusDaemonShape, SocketModeBits, WorkingListenerTier,
    build::{GenerationDriver, GenerationPlan, ModuleEmission},
};

const SUPERVISION_SOCKET_MODE: u32 = 0o600;

fn main() {
    SchemaBuild::from_environment().run();
}

struct SchemaBuild {
    crate_root: PathBuf,
}

impl SchemaBuild {
    fn from_environment() -> Self {
        Self {
            crate_root: PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir set")),
        }
    }

    fn run(&self) {
        println!("cargo:rerun-if-changed=schema/nexus.schema");
        println!("cargo:rerun-if-changed=src/schema/daemon.rs");

        let plan = GenerationPlan::new(&self.crate_root, "harness", "0.1.0")
            .with_module(ModuleEmission::daemon_module("nexus", Self::daemon_shape()));
        GenerationDriver::new(plan)
            .generate()
            .expect("generate harness schema artifacts")
            .write_or_check("HARNESS_UPDATE_SCHEMA_ARTIFACTS")
            .expect("checked-in harness schema artifacts are fresh");
    }

    /// Harness's working tier is component-decoded: the ordinary socket speaks
    /// the hand-written `signal-harness` `HarnessFrame` contract (still a
    /// `signal_channel!` wire, not a schema-derived root), so the emitted daemon
    /// owns argv/socket/accept/lifecycle/exit while the component owns the
    /// per-connection `HarnessFrame` decode/encode and drives the existing
    /// `Harness` / `TranscriptSubscriptionManager` kameo actors. The owner-only
    /// meta tier carries the engine-management (supervision) protocol from
    /// `signal-persona`.
    fn daemon_shape() -> NexusDaemonShape {
        NexusDaemonShape::new("harness-daemon", WorkingListenerTier::component_decoded())
            .with_meta_tier(MetaListenerTier::new(SocketModeBits::new(
                SUPERVISION_SOCKET_MODE,
            )))
    }
}
