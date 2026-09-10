use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const MARKER_VERSION: &str = "1";
const FIRST_CANDIDATE_LENGTH: usize = 6;
const CODEX_CANDIDATE_START: usize = 23;
const FLOW_DIRECTORY_MODE: u32 = 0o700;
const MARKER_MODE: u32 = 0o600;
static TEMP_MARKER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessKind {
    Codex,
    Claude,
}

impl HarnessKind {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "codex" => Ok(Self::Codex),
            "claude" => Ok(Self::Claude),
            _ => Err(Error::Argument(
                "harness must be `codex` or `claude`".into(),
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Argument(String),
    Filesystem { path: PathBuf, source: io::Error },
    UnsafePath(PathBuf),
    UnsafeRoot(PathBuf),
    UnsafeLane(PathBuf),
    UnsafeMarker(PathBuf),
    MalformedMarker(PathBuf),
    CandidateExhausted,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Argument(message) => formatter.write_str(message),
            Self::Filesystem { path, source } => write!(formatter, "{}: {source}", path.display()),
            Self::UnsafePath(path) => {
                write!(formatter, "unsafe flows root path: {}", path.display())
            }
            Self::UnsafeRoot(path) => write!(formatter, "unsafe flows root: {}", path.display()),
            Self::UnsafeLane(path) => write!(formatter, "unsafe flow lane: {}", path.display()),
            Self::UnsafeMarker(path) => write!(formatter, "unsafe flow marker: {}", path.display()),
            Self::MalformedMarker(path) => {
                write!(formatter, "malformed flow marker: {}", path.display())
            }
            Self::CandidateExhausted => {
                formatter.write_str("no unambiguous flow-id candidate remains")
            }
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Marker {
    harness: HarnessKind,
    identity: String,
    alias: String,
    claude_uuid_version: Option<ClaudeUuidVersion>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaudeUuidVersion {
    V4,
    V5,
}

impl ClaudeUuidVersion {
    fn marker_value(self) -> &'static str {
        match self {
            Self::V4 => "uuid-v4",
            Self::V5 => "uuid-v5",
        }
    }

    fn from_normalized_identity(identity: &str) -> Option<Self> {
        if identity.len() != 32
            || !matches!(identity.as_bytes().get(16), Some(b'8' | b'9' | b'a' | b'b'))
        {
            return None;
        }
        match identity.as_bytes().get(12) {
            Some(b'4') => Some(Self::V4),
            Some(b'5') => Some(Self::V5),
            _ => None,
        }
    }
}

impl Marker {
    fn new(
        harness: HarnessKind,
        identity: &str,
        alias: &str,
        claude_uuid_version: Option<ClaudeUuidVersion>,
    ) -> Self {
        Self {
            harness,
            identity: identity.into(),
            alias: alias.into(),
            claude_uuid_version,
        }
    }

    fn encode(&self) -> String {
        let uuid_version = self
            .claude_uuid_version
            .map(|version| format!("uuid-version={}\n", version.marker_value()))
            .unwrap_or_default();
        format!(
            "version={MARKER_VERSION}\nharness={}\nidentity={}\nalias={}\n",
            self.harness.name(),
            self.identity,
            self.alias,
        ) + &uuid_version
    }

    fn decode(path: &Path, text: &str) -> Result<Self> {
        let mut lines = text.lines();
        let version = lines.next();
        let harness = lines.next();
        let identity = lines.next();
        let alias = lines.next();
        let uuid_version = lines.next();
        if lines.next().is_some()
            || version != Some("version=1")
            || !identity.is_some_and(|line| line.starts_with("identity="))
            || !alias.is_some_and(|line| line.starts_with("alias="))
        {
            return Err(Error::MalformedMarker(path.into()));
        }
        let harness = match harness {
            Some("harness=codex") => HarnessKind::Codex,
            Some("harness=claude") => HarnessKind::Claude,
            _ => return Err(Error::MalformedMarker(path.into())),
        };
        let identity = identity.expect("validated").trim_start_matches("identity=");
        if identity.len() != 32
            || identity.bytes().any(|byte| !byte.is_ascii_hexdigit())
            || identity.bytes().any(|byte| byte.is_ascii_uppercase())
        {
            return Err(Error::MalformedMarker(path.into()));
        }
        let alias = alias.expect("validated").trim_start_matches("alias=");
        if alias.is_empty() || alias.bytes().any(|byte| !byte.is_ascii_hexdigit()) {
            return Err(Error::MalformedMarker(path.into()));
        }
        let claude_uuid_version = match (harness, uuid_version) {
            (HarnessKind::Codex, None) => None,
            (HarnessKind::Codex, Some(_)) => return Err(Error::MalformedMarker(path.into())),
            // Deployed Claude markers predate this field and could only have
            // been minted for v4 roots. Preserve those claims; untyped v5
            // metadata is not trusted.
            (HarnessKind::Claude, None) => {
                match ClaudeUuidVersion::from_normalized_identity(identity) {
                    Some(ClaudeUuidVersion::V4) => Some(ClaudeUuidVersion::V4),
                    _ => return Err(Error::MalformedMarker(path.into())),
                }
            }
            (HarnessKind::Claude, Some("uuid-version=uuid-v4")) => Some(ClaudeUuidVersion::V4),
            (HarnessKind::Claude, Some("uuid-version=uuid-v5")) => Some(ClaudeUuidVersion::V5),
            (HarnessKind::Claude, Some(_)) => return Err(Error::MalformedMarker(path.into())),
        };
        if let Some(uuid_version) = claude_uuid_version
            && ClaudeUuidVersion::from_normalized_identity(identity) != Some(uuid_version)
        {
            return Err(Error::MalformedMarker(path.into()));
        }
        Ok(Self::new(harness, identity, alias, claude_uuid_version))
    }
}

pub fn claim(harness: HarnessKind, flows_root: &Path, identity: &str) -> Result<String> {
    let (identity, candidate_start, claude_uuid_version) = match harness {
        HarnessKind::Codex => (normalize_uuid(identity)?, CODEX_CANDIDATE_START, None),
        HarnessKind::Claude => {
            let (identity, version) = normalize_claude_parent_uuid(identity)?;
            (identity, 0, Some(version))
        }
    };
    validate_root(flows_root)?;

    for end in candidate_start + FIRST_CANDIDATE_LENGTH..=identity.len() {
        let alias = &identity[candidate_start..end];
        match inspect_lane(flows_root, alias)? {
            Lane::Legacy => continue,
            Lane::Missing | Lane::Owned => {
                match claim_candidate(harness, flows_root, &identity, alias, claude_uuid_version)? {
                    Candidate::Claimed => return Ok(alias.into()),
                    Candidate::Collision => continue,
                }
            }
        }
    }
    Err(Error::CandidateExhausted)
}

pub fn normalize_uuid(value: &str) -> Result<String> {
    if value.len() != 36
        || value.bytes().enumerate().any(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte != b'-'
            } else {
                !byte.is_ascii_hexdigit()
            }
        })
    {
        return Err(Error::Argument(
            "identity must be one canonical UUID".into(),
        ));
    }
    Ok(value
        .bytes()
        .filter(|byte| *byte != b'-')
        .map(|byte| byte.to_ascii_lowercase() as char)
        .collect())
}

fn normalize_claude_parent_uuid(value: &str) -> Result<(String, ClaudeUuidVersion)> {
    let canonical_uuid = value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
            }
        });
    let uuid_version = match value.as_bytes().get(14) {
        Some(b'4') => Some(ClaudeUuidVersion::V4),
        Some(b'5') => Some(ClaudeUuidVersion::V5),
        _ => None,
    };
    let rfc4122_variant = matches!(value.as_bytes().get(19), Some(b'8' | b'9' | b'a' | b'b'));
    if !canonical_uuid || uuid_version.is_none() || !rfc4122_variant {
        return Err(Error::Argument(
            "Claude parent session must be one canonical RFC 4122 UUIDv4 or UUIDv5".into(),
        ));
    }
    Ok((
        value
            .bytes()
            .filter(|byte| *byte != b'-')
            .map(char::from)
            .collect(),
        uuid_version.expect("validated"),
    ))
}

enum Lane {
    Missing,
    Legacy,
    Owned,
}

fn inspect_lane(root: &Path, alias: &str) -> Result<Lane> {
    let lane = root.join(alias);
    match fs::symlink_metadata(&lane) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(Error::UnsafeLane(lane))
        }
        Ok(_) => {
            let marker = marker_path(root, alias);
            match fs::symlink_metadata(&marker) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                    Err(Error::UnsafeMarker(marker))
                }
                Ok(_) => Ok(Lane::Owned),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Lane::Legacy),
                Err(source) => Err(Error::Filesystem {
                    path: marker,
                    source,
                }),
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Lane::Missing),
        Err(source) => Err(Error::Filesystem { path: lane, source }),
    }
}

enum Candidate {
    Claimed,
    Collision,
}

fn claim_candidate(
    harness: HarnessKind,
    root: &Path,
    identity: &str,
    alias: &str,
    claude_uuid_version: Option<ClaudeUuidVersion>,
) -> Result<Candidate> {
    let marker_path = marker_path(root, alias);
    let expected = Marker::new(harness, identity, alias, claude_uuid_version);
    let lock_path = claim_lock_path(root, alias);
    let lock_file = open_claim_lock(&lock_path)?;
    lock_file.lock().map_err(|source| Error::Filesystem {
        path: lock_path.clone(),
        source,
    })?;

    #[cfg(test)]
    test_hook::after_claim_lock();

    let lane = root.join(alias);
    let marker = match read_marker(&marker_path)? {
        Some(marker) => marker,
        None => match fs::symlink_metadata(&lane) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(Error::UnsafeLane(lane));
            }
            Ok(_) => return Ok(Candidate::Collision),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if publish_marker(root, alias, &expected)? {
                    expected.clone()
                } else {
                    // Another writer occupied the marker name while the stable
                    // lock was held.  Inspect it rather than treating it as a
                    // benign collision, so malformed metadata remains closed.
                    read_marker(&marker_path)?.ok_or_else(|| Error::Filesystem {
                        path: marker_path.clone(),
                        source: io::Error::new(
                            io::ErrorKind::NotFound,
                            "flow marker disappeared during claim",
                        ),
                    })?
                }
            }
            Err(source) => {
                return Err(Error::Filesystem { path: lane, source });
            }
        },
    };

    if marker.alias != alias {
        return Err(Error::MalformedMarker(marker_path));
    }
    if marker != expected {
        return Ok(Candidate::Collision);
    }

    match fs::symlink_metadata(&lane) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(Error::UnsafeLane(lane))
        }
        Ok(metadata) => {
            validate_lane(&lane, &metadata)?;
            Ok(Candidate::Claimed)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(&lane).map_err(|source| Error::Filesystem {
                path: lane.clone(),
                source,
            })?;
            fs::set_permissions(&lane, fs::Permissions::from_mode(FLOW_DIRECTORY_MODE)).map_err(
                |source| Error::Filesystem {
                    path: lane.clone(),
                    source,
                },
            )?;
            let metadata = fs::symlink_metadata(&lane).map_err(|source| Error::Filesystem {
                path: lane.clone(),
                source,
            })?;
            validate_lane(&lane, &metadata)?;
            Ok(Candidate::Claimed)
        }
        Err(source) => Err(Error::Filesystem { path: lane, source }),
    }
}

fn open_claim_lock(path: &Path) -> Result<File> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(MARKER_MODE)
        .open(path)
    {
        Ok(file) => {
            fs::set_permissions(path, fs::Permissions::from_mode(MARKER_MODE)).map_err(
                |source| Error::Filesystem {
                    path: path.into(),
                    source,
                },
            )?;
            Ok(file)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            validate_marker_path(path)?;
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .map_err(|source| Error::Filesystem {
                    path: path.into(),
                    source,
                })
        }
        Err(source) => Err(Error::Filesystem {
            path: path.into(),
            source,
        }),
    }
}

fn publish_marker(root: &Path, alias: &str, marker: &Marker) -> Result<bool> {
    let marker_path = marker_path(root, alias);
    let (temporary_path, mut temporary_file) = create_temporary_marker(root, alias)?;
    write_marker(&temporary_path, &mut temporary_file, marker)?;
    drop(temporary_file);

    match fs::hard_link(&temporary_path, &marker_path) {
        Ok(()) => {
            fs::remove_file(&temporary_path).map_err(|source| Error::Filesystem {
                path: temporary_path,
                source,
            })?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            fs::remove_file(&temporary_path).map_err(|source| Error::Filesystem {
                path: temporary_path,
                source,
            })?;
            Ok(false)
        }
        Err(source) => Err(Error::Filesystem {
            path: marker_path,
            source,
        }),
    }
}

fn create_temporary_marker(root: &Path, alias: &str) -> Result<(PathBuf, File)> {
    for _ in 0..64 {
        let sequence = TEMP_MARKER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = root.join(format!(
            ".{alias}.flow-id.tmp.{}.{}",
            std::process::id(),
            sequence
        ));
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(MARKER_MODE)
            .open(&path)
        {
            Ok(file) => {
                fs::set_permissions(&path, fs::Permissions::from_mode(MARKER_MODE)).map_err(
                    |source| Error::Filesystem {
                        path: path.clone(),
                        source,
                    },
                )?;
                return Ok((path, file));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(Error::Filesystem { path, source });
            }
        }
    }
    Err(Error::CandidateExhausted)
}

fn write_marker(path: &Path, file: &mut File, marker: &Marker) -> Result<()> {
    file.write_all(marker.encode().as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|source| Error::Filesystem {
            path: path.into(),
            source,
        })
}

fn read_marker(path: &Path) -> Result<Option<Marker>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(Error::UnsafeMarker(path.into()))
        }
        Ok(_) => {
            validate_marker_path(path)?;
            let mut file = File::open(path).map_err(|source| Error::Filesystem {
                path: path.into(),
                source,
            })?;
            let mut text = String::new();
            file.read_to_string(&mut text)
                .map_err(|source| Error::Filesystem {
                    path: path.into(),
                    source,
                })?;
            Marker::decode(path, &text).map(Some)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(Error::Filesystem {
            path: path.into(),
            source,
        }),
    }
}

fn marker_path(root: &Path, alias: &str) -> PathBuf {
    root.join(format!(".{alias}.flow-id"))
}

fn claim_lock_path(root: &Path, alias: &str) -> PathBuf {
    root.join(format!(".{alias}.flow-id.lock"))
}

fn validate_root(path: &Path) -> Result<()> {
    let text = path
        .to_str()
        .ok_or_else(|| Error::UnsafePath(path.into()))?;
    if !text.starts_with('/')
        || text
            .split('/')
            .skip(1)
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(Error::UnsafePath(path.into()));
    }
    let metadata = fs::symlink_metadata(path).map_err(|source| Error::Filesystem {
        path: path.into(),
        source,
    })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.permissions().mode() & 0o022 != 0
    {
        return Err(Error::UnsafeRoot(path.into()));
    }
    Ok(())
}

fn validate_marker_path(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|source| Error::Filesystem {
        path: path.into(),
        source,
    })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.permissions().mode() & 0o777 != MARKER_MODE
    {
        return Err(Error::UnsafeMarker(path.into()));
    }
    Ok(())
}

fn validate_lane(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.permissions().mode() & 0o777 != FLOW_DIRECTORY_MODE
    {
        return Err(Error::UnsafeLane(path.into()));
    }
    Ok(())
}

#[cfg(test)]
mod test_hook {
    use std::sync::{
        Mutex, OnceLock,
        mpsc::{Receiver, SyncSender},
    };

    struct ClaimLockHook {
        entered: SyncSender<()>,
        resume: Receiver<()>,
    }

    fn hook() -> &'static Mutex<Option<ClaimLockHook>> {
        static HOOK: OnceLock<Mutex<Option<ClaimLockHook>>> = OnceLock::new();
        HOOK.get_or_init(|| Mutex::new(None))
    }

    pub(super) fn install(entered: SyncSender<()>, resume: Receiver<()>) {
        *hook().lock().expect("claim lock hook mutex") = Some(ClaimLockHook { entered, resume });
    }

    pub(super) fn after_claim_lock() {
        let hook = hook().lock().expect("claim lock hook mutex").take();
        if let Some(hook) = hook {
            hook.entered.send(()).expect("claim lock hook entered");
            hook.resume.recv().expect("claim lock hook resume");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::mpsc, thread};

    use tempfile::tempdir;

    use super::{HarnessKind, Marker, claim, marker_path, test_hook};

    #[test]
    fn claude_first_creator_publishes_complete_marker_only_after_the_stable_claim_lock() {
        let root = tempdir().expect("flows root");
        let (entered_sender, entered_receiver) = mpsc::sync_channel(0);
        let (resume_sender, resume_receiver) = mpsc::sync_channel(0);
        test_hook::install(entered_sender, resume_receiver);

        let root_path = root.path().to_owned();
        let claimant = thread::spawn(move || {
            claim(
                HarnessKind::Claude,
                &root_path,
                "a1b2c3d4-e5f6-4a78-9abc-def012345678",
            )
        });

        entered_receiver
            .recv()
            .expect("first claimant holds stable lock");
        let marker_path = marker_path(root.path(), "a1b2c3");
        assert!(
            !marker_path.exists(),
            "no empty or partial marker is visible while publication is paused"
        );
        let follower_root = root.path().to_owned();
        let follower = thread::spawn(move || {
            claim(
                HarnessKind::Claude,
                &follower_root,
                "a1b2c3d4-e5f6-4a78-9abc-def012345678",
            )
        });
        resume_sender.send(()).expect("resume publication");

        assert_eq!(
            claimant
                .join()
                .expect("claimant thread")
                .expect("first claim succeeds"),
            "a1b2c3"
        );
        assert_eq!(
            follower
                .join()
                .expect("follower thread")
                .expect("follower claim succeeds"),
            "a1b2c3"
        );
        let marker_text = fs::read_to_string(&marker_path).expect("published marker");
        assert!(
            Marker::decode(&marker_path, &marker_text).is_ok(),
            "the first visible marker is complete metadata"
        );
    }
}
