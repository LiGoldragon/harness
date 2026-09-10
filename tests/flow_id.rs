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
const CLAUDE_SESSION: &str = "a1b2c3d4-e5f6-4a78-9abc-def012345678";
const CLAUDE_V5_SESSION: &str = "a1b2c3e4-e5f6-5a78-9abc-def012345678";
const FIRST_ALIAS: &str = "715d46";
const CLAUDE_FIRST_ALIAS: &str = "a1b2c3";

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

fn claude(root: &Path, parent_session: &str) -> std::process::Output {
    flow_id()
        .arg("claude")
        .arg("--flows-root")
        .arg(root)
        .arg("--parent-session")
        .arg(parent_session)
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

fn claim_lock(root: &Path, alias: &str) -> PathBuf {
    root.join(format!(".{alias}.flow-id.lock"))
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
        3,
        "one stable private lock, one marker, and one lane remain"
    );
    assert_eq!(
        fs::metadata(claim_lock(root.path(), FIRST_ALIAS))
            .expect("stable claim lock")
            .permissions()
            .mode()
            & 0o777,
        0o600,
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

    fs::remove_file(&claim).expect("remove unsafe marker");
    fs::remove_dir(&lane).expect("remove formerly claimed lane");
    let lock = claim_lock(root.path(), FIRST_ALIAS);
    fs::remove_file(&lock).expect("remove stable claim lock");
    symlink(root.path().join("missing-target"), &lock).expect("unsafe claim lock symlink");
    assert!(!codex(root.path(), CODEX_SESSION).status.success());
}

#[test]
fn missing_invalid_and_ambiguous_identities_are_rejected_without_a_lane() {
    let root = flows_root();
    let missing = flow_id()
        .arg("codex")
        .arg("--flows-root")
        .arg(root.path())
        .env_remove("CODEX_SESSION_ID")
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
    for alias in ["000000", "000000a", "000000b"] {
        let marker_path = marker(root.path(), alias);
        if marker_path.exists() {
            let metadata = fs::metadata(&marker_path).expect("complete concurrent marker");
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            assert!(
                fs::read_to_string(marker_path)
                    .expect("complete concurrent marker content")
                    .starts_with("version=1\n"),
            );
        }
    }
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
    assert_eq!(success_alias(output), "a1b2c3\n");
}

#[test]
fn claude_uses_the_first_six_literal_characters_of_its_canonical_v4_or_v5_parent_session() {
    let root = flows_root();
    let output = claude(root.path(), CLAUDE_SESSION);
    assert_eq!(success_alias(output), "a1b2c3\n");
    assert!(root.path().join(CLAUDE_FIRST_ALIAS).is_dir());
}

#[test]
fn claude_rejects_noncanonical_unsupported_version_and_invalid_variant_parent_sessions_without_claiming_a_lane()
 {
    let root = flows_root();
    for parent_session in [
        "a1b2c3d4-e5f6-1a78-9abc-def012345678",
        "a1b2c3d4-e5f6-3a78-9abc-def012345678",
        "a1b2c3d4-e5f6-4a78-7abc-def012345678",
        "a1b2c3d4-e5f6-4a78-cabc-def012345678",
        "A1B2C3D4-E5F6-4A78-9ABC-DEF012345678",
        "a1b2c3d4e5f64a789abcdef012345678",
    ] {
        assert!(
            !claude(root.path(), parent_session).status.success(),
            "invalid Claude parent session unexpectedly succeeded: {parent_session}"
        );
    }
    assert!(
        fs::read_dir(root.path())
            .expect("empty root")
            .next()
            .is_none()
    );
}

#[test]
fn claude_v5_claims_are_idempotent_private_and_separate_from_same_prefix_v4_claims() {
    let root = flows_root();
    assert_eq!(
        success_alias(claude(root.path(), CLAUDE_SESSION)),
        "a1b2c3\n"
    );
    assert_eq!(
        success_alias(claude(root.path(), CLAUDE_V5_SESSION)),
        "a1b2c3e\n",
        "a v5 root collides with, rather than adopts, the v4 first-six lane"
    );
    assert_eq!(
        success_alias(claude(root.path(), CLAUDE_V5_SESSION)),
        "a1b2c3e\n",
        "the v5 root keeps its extended claim"
    );
    assert_eq!(
        fs::read_to_string(marker(root.path(), CLAUDE_FIRST_ALIAS)).expect("v4 marker"),
        "version=1\nharness=claude\nidentity=a1b2c3d4e5f64a789abcdef012345678\nalias=a1b2c3\nuuid-version=uuid-v4\n"
    );
    assert_eq!(
        fs::read_to_string(marker(root.path(), "a1b2c3e")).expect("v5 marker"),
        "version=1\nharness=claude\nidentity=a1b2c3e4e5f65a789abcdef012345678\nalias=a1b2c3e\nuuid-version=uuid-v5\n"
    );
    for alias in [CLAUDE_FIRST_ALIAS, "a1b2c3e"] {
        assert_eq!(
            fs::metadata(root.path().join(alias))
                .expect("Claude lane")
                .permissions()
                .mode()
                & 0o777,
            0o700,
        );
        assert_eq!(
            fs::metadata(claim_lock(root.path(), alias))
                .expect("Claude claim lock")
                .permissions()
                .mode()
                & 0o777,
            0o600,
        );
    }
}

#[test]
fn deployed_untyped_v4_claude_marker_remains_idempotent_but_untyped_v5_fails_closed() {
    let root = flows_root();
    fs::create_dir(root.path().join(CLAUDE_FIRST_ALIAS)).expect("legacy v4 lane");
    fs::set_permissions(
        root.path().join(CLAUDE_FIRST_ALIAS),
        fs::Permissions::from_mode(0o700),
    )
    .expect("private v4 lane");
    fs::write(
        marker(root.path(), CLAUDE_FIRST_ALIAS),
        "version=1\nharness=claude\nidentity=a1b2c3d4e5f64a789abcdef012345678\nalias=a1b2c3\n",
    )
    .expect("deployed v4 marker");
    fs::set_permissions(
        marker(root.path(), CLAUDE_FIRST_ALIAS),
        fs::Permissions::from_mode(0o600),
    )
    .expect("private deployed marker");
    fs::write(claim_lock(root.path(), CLAUDE_FIRST_ALIAS), "").expect("legacy v4 lock");
    fs::set_permissions(
        claim_lock(root.path(), CLAUDE_FIRST_ALIAS),
        fs::Permissions::from_mode(0o600),
    )
    .expect("private deployed lock");

    assert_eq!(
        success_alias(claude(root.path(), CLAUDE_SESSION)),
        "a1b2c3\n",
        "a deployed v4 marker remains its original stable claim"
    );
    assert!(
        !fs::read_to_string(marker(root.path(), CLAUDE_FIRST_ALIAS))
            .expect("deployed marker")
            .contains("uuid-version="),
        "idempotence does not rewrite private deployed metadata"
    );

    let untyped_v5 = "b1c2d3e4-e5f6-5a78-9abc-def012345678";
    let untyped_v5_alias = "b1c2d3";
    fs::create_dir(root.path().join(untyped_v5_alias)).expect("untyped v5 lane");
    fs::set_permissions(
        root.path().join(untyped_v5_alias),
        fs::Permissions::from_mode(0o700),
    )
    .expect("private untyped v5 lane");
    fs::write(
        marker(root.path(), untyped_v5_alias),
        "version=1\nharness=claude\nidentity=b1c2d3e4e5f65a789abcdef012345678\nalias=b1c2d3\n",
    )
    .expect("untyped v5 marker");
    fs::set_permissions(
        marker(root.path(), untyped_v5_alias),
        fs::Permissions::from_mode(0o600),
    )
    .expect("private untyped v5 marker");
    fs::write(claim_lock(root.path(), untyped_v5_alias), "").expect("untyped v5 lock");
    fs::set_permissions(
        claim_lock(root.path(), untyped_v5_alias),
        fs::Permissions::from_mode(0o600),
    )
    .expect("private untyped v5 lock");
    assert!(
        !claude(root.path(), untyped_v5).status.success(),
        "untyped v5 marker must not be trusted as old v4 metadata"
    );
}

#[test]
fn claude_fails_closed_when_every_eligible_literal_hex_candidate_is_occupied() {
    let root = flows_root();
    let identity = "a1b2c3d4e5f64a789abcdef012345678";
    for end in 6..=identity.len() {
        fs::create_dir(root.path().join(&identity[..end])).expect("occupy Claude candidate");
    }
    let output = claude(root.path(), CLAUDE_SESSION);
    assert!(!output.status.success(), "exhausted Claude claim succeeded");
    assert_eq!(
        fs::read_dir(root.path())
            .expect("occupied candidates")
            .count(),
        identity.len() - 5,
        "a rejected exhaustion claim must not overwrite or add a lane"
    );
}

#[test]
fn claude_claim_is_idempotent_and_keeps_private_marker_and_lock_permissions() {
    let root = flows_root();
    assert_eq!(
        success_alias(claude(root.path(), CLAUDE_SESSION)),
        "a1b2c3\n"
    );
    assert_eq!(
        success_alias(claude(root.path(), CLAUDE_SESSION)),
        "a1b2c3\n"
    );
    assert_eq!(
        fs::read_to_string(marker(root.path(), CLAUDE_FIRST_ALIAS)).expect("Claude marker"),
        "version=1\nharness=claude\nidentity=a1b2c3d4e5f64a789abcdef012345678\nalias=a1b2c3\nuuid-version=uuid-v4\n"
    );
    assert_eq!(
        fs::metadata(claim_lock(root.path(), CLAUDE_FIRST_ALIAS))
            .expect("stable Claude claim lock")
            .permissions()
            .mode()
            & 0o777,
        0o600,
    );
    assert_eq!(
        fs::metadata(root.path().join(CLAUDE_FIRST_ALIAS))
            .expect("Claude lane")
            .permissions()
            .mode()
            & 0o777,
        0o700,
    );
}

#[test]
fn claude_extends_the_next_literal_hex_character_when_its_first_six_are_taken() {
    let root = flows_root();
    fs::create_dir(root.path().join(CLAUDE_FIRST_ALIAS)).expect("legacy Claude collision");
    assert_eq!(
        success_alias(claude(root.path(), CLAUDE_SESSION)),
        "a1b2c3d\n"
    );
    assert!(root.path().join(CLAUDE_FIRST_ALIAS).is_dir());
    assert!(root.path().join("a1b2c3d").is_dir());
}

#[test]
fn claude_does_not_adopt_a_codex_marker_with_the_same_alias() {
    let root = flows_root();
    let codex_session = "01a05e95-1234-5678-9abc-000a1b2c3def";
    assert_eq!(success_alias(codex(root.path(), codex_session)), "a1b2c3\n");
    assert_eq!(
        success_alias(claude(root.path(), CLAUDE_SESSION)),
        "a1b2c3d\n"
    );
    assert!(
        fs::read_to_string(marker(root.path(), CLAUDE_FIRST_ALIAS))
            .expect("Codex marker")
            .contains("harness=codex\n")
    );
    assert!(
        fs::read_to_string(marker(root.path(), "a1b2c3d"))
            .expect("Claude marker")
            .contains("harness=claude\n")
    );
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
