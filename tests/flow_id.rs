use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::Command,
    sync::Barrier,
    thread,
};

use tempfile::TempDir;

const CODEX_SESSION: &str = "01a05e95-1234-5678-9abc-000715d46abc";
const CLAUDE_SESSION: &str = "02b06f96-4321-8765-cba9-000715d46abc";
const FIRST_ALIAS: &str = "715d46";

fn flows_root() -> TempDir {
    tempfile::tempdir().expect("flows root")
}

fn flow_id() -> Command {
    Command::new(env!("CARGO_BIN_EXE_flow-id"))
}

fn codex(root: &Path, session: &str) -> std::process::Output {
    flow_id()
        .arg("codex")
        .arg("--flows-root")
        .arg(root)
        .env("CODEX_SESSION_ID", session)
        .output()
        .expect("run flow-id codex")
}

fn claude(root: &Path, session: &str) -> std::process::Output {
    flow_id()
        .arg("claude")
        .arg("--flows-root")
        .arg(root)
        .arg("--parent-session")
        .arg(session)
        .output()
        .expect("run flow-id claude")
}

fn success_alias(output: std::process::Output) -> String {
    assert!(
        output.status.success(),
        "flow-id failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty(), "successful stderr must be empty");
    String::from_utf8(output.stdout).expect("stdout utf8")
}

fn marker(root: &Path, alias: &str) -> PathBuf {
    root.join(format!(".{alias}.flow-id"))
}

#[test]
fn codex_extracts_the_normalized_hex_23_to_29_candidate_and_prints_only_the_alias() {
    let root = flows_root();
    let alias = success_alias(codex(root.path(), CODEX_SESSION));
    assert_eq!(alias, "715d46\n");
    assert!(alias.trim().bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert!(root.path().join(FIRST_ALIAS).is_dir());
}

#[test]
fn uppercase_uuid_normalizes_to_the_same_literal_hex_alias() {
    let root = flows_root();
    assert_eq!(
        success_alias(codex(root.path(), &CODEX_SESSION.to_ascii_uppercase())),
        "715d46\n"
    );
}

#[test]
fn same_codex_session_is_idempotent_across_cold_processes() {
    let root = flows_root();
    assert_eq!(success_alias(codex(root.path(), CODEX_SESSION)), "715d46\n");
    assert_eq!(success_alias(codex(root.path(), CODEX_SESSION)), "715d46\n");
    assert_eq!(
        fs::read_dir(root.path()).expect("read claims").count(),
        2,
        "one marker and one lane remain"
    );
}

#[test]
fn unmarked_legacy_lane_is_a_collision_that_extends_the_candidate() {
    let root = flows_root();
    fs::create_dir(root.path().join(FIRST_ALIAS)).expect("legacy lane");
    assert_eq!(
        success_alias(codex(root.path(), CODEX_SESSION)),
        "715d46a\n"
    );
    assert!(root.path().join(FIRST_ALIAS).is_dir());
    assert!(root.path().join("715d46a").is_dir());
}

#[test]
fn malformed_claim_marker_fails_closed() {
    let root = flows_root();
    fs::write(marker(root.path(), FIRST_ALIAS), "not flow-id metadata\n")
        .expect("malformed marker");
    fs::set_permissions(
        marker(root.path(), FIRST_ALIAS),
        fs::Permissions::from_mode(0o600),
    )
    .expect("private malformed marker");
    let output = codex(root.path(), CODEX_SESSION);
    assert!(!output.status.success());
    assert!(!root.path().join(FIRST_ALIAS).exists());
}

#[test]
fn unsafe_roots_and_permissions_are_rejected() {
    let root = flows_root();
    let file = root.path().join("not-a-directory");
    fs::write(&file, "x").expect("non-directory root");
    assert!(!codex(&file, CODEX_SESSION).status.success());

    let target = root.path().join("target");
    fs::create_dir(&target).expect("target root");
    let linked = root.path().join("linked");
    symlink(&target, &linked).expect("symlink root");
    assert!(!codex(&linked, CODEX_SESSION).status.success());

    let writable = root.path().join("writable");
    fs::create_dir(&writable).expect("writable root");
    fs::set_permissions(&writable, fs::Permissions::from_mode(0o777)).expect("unsafe mode");
    assert!(!codex(&writable, CODEX_SESSION).status.success());

    let unsafe_path = root.path().join("target/../target");
    assert!(!codex(&unsafe_path, CODEX_SESSION).status.success());
}

#[test]
fn claimed_lanes_and_markers_reject_unsafe_replacement() {
    let root = flows_root();
    assert_eq!(success_alias(codex(root.path(), CODEX_SESSION)), "715d46\n");
    let lane = root.path().join(FIRST_ALIAS);
    fs::set_permissions(&lane, fs::Permissions::from_mode(0o755)).expect("unsafe lane mode");
    assert!(!codex(root.path(), CODEX_SESSION).status.success());

    fs::set_permissions(&lane, fs::Permissions::from_mode(0o700)).expect("restore lane mode");
    let claim = marker(root.path(), FIRST_ALIAS);
    fs::remove_file(&claim).expect("remove claim marker");
    symlink(root.path().join("missing-target"), &claim).expect("unsafe marker symlink");
    assert!(!codex(root.path(), CODEX_SESSION).status.success());
}

#[test]
fn missing_invalid_and_ambiguous_identities_are_rejected_without_a_lane() {
    let root = flows_root();
    let missing = flow_id()
        .arg("codex")
        .arg("--flows-root")
        .arg(root.path())
        .output()
        .expect("run missing codex identity");
    assert!(!missing.status.success());
    assert!(!codex(root.path(), "not-a-uuid").status.success());
    let ambiguous = flow_id()
        .arg("claude")
        .arg("--flows-root")
        .arg(root.path())
        .arg("--parent-session")
        .arg(CLAUDE_SESSION)
        .arg("--parent-session")
        .arg(CODEX_SESSION)
        .output()
        .expect("run ambiguous parent identity");
    assert!(!ambiguous.status.success());
    assert!(
        fs::read_dir(root.path())
            .expect("empty root")
            .next()
            .is_none()
    );
}

#[test]
fn concurrent_same_and_different_sessions_claim_without_overwriting() {
    let root = flows_root();
    let root_path = root.path().to_owned();
    let start = std::sync::Arc::new(Barrier::new(4));
    let sessions = [
        "10a05e95-1234-5678-9abc-abc000000a12",
        "10a05e95-1234-5678-9abc-abc000000a12",
        "20b06f96-4321-8765-cba9-def000000b12",
    ];
    let workers = sessions.map(|session| {
        let root = root_path.clone();
        let start = start.clone();
        thread::spawn(move || {
            start.wait();
            success_alias(codex(&root, session))
        })
    });
    start.wait();
    let aliases = workers.map(|worker| worker.join().expect("claim worker"));
    assert_eq!(aliases[0], aliases[1], "same identity shares its claim");
    assert_ne!(
        aliases[0], aliases[2],
        "different identities never overwrite"
    );
    assert!(
        matches!(aliases[0].as_str(), "000000\n" | "000000a\n"),
        "same identity received an unexpected extension: {}",
        aliases[0]
    );
    assert!(
        matches!(aliases[2].as_str(), "000000\n" | "000000b\n"),
        "different identity received an unexpected extension: {}",
        aliases[2]
    );
    assert!(root.path().join("000000").is_dir());
    assert!(root.path().join("000000a").is_dir() || root.path().join("000000b").is_dir());
}

#[test]
fn claude_uses_only_the_explicit_parent_session() {
    let root = flows_root();
    let output = flow_id()
        .arg("claude")
        .arg("--flows-root")
        .arg(root.path())
        .arg("--parent-session")
        .arg(CLAUDE_SESSION)
        .env("CODEX_SESSION_ID", "not-a-uuid")
        .output()
        .expect("run flow-id claude");
    assert_eq!(success_alias(output), "715d46\n");
}

#[test]
fn child_is_not_a_helper_mode_and_cannot_create_a_child_lane() {
    let root = flows_root();
    let output = flow_id()
        .arg("child")
        .arg("--flows-root")
        .arg(root.path())
        .output()
        .expect("run unsupported helper mode");
    assert!(!output.status.success());
    assert!(
        fs::read_dir(root.path())
            .expect("empty root")
            .next()
            .is_none()
    );
}
