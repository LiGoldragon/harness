use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

const MARKER_VERSION: &str = "1";
const FIRST_CANDIDATE_END: usize = 29;
const FLOW_DIRECTORY_MODE: u32 = 0o700;
const MARKER_MODE: u32 = 0o600;

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
}

impl Marker {
    fn new(harness: HarnessKind, identity: &str, alias: &str) -> Self {
        Self {
            harness,
            identity: identity.into(),
            alias: alias.into(),
        }
    }

    fn encode(&self) -> String {
        format!(
            "version={MARKER_VERSION}\nharness={}\nidentity={}\nalias={}\n",
            self.harness.name(),
            self.identity,
            self.alias,
        )
    }

    fn decode(path: &Path, text: &str) -> Result<Self> {
        let mut lines = text.lines();
        let version = lines.next();
        let harness = lines.next();
        let identity = lines.next();
        let alias = lines.next();
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
        Ok(Self::new(harness, &identity, alias))
    }
}

pub fn claim(harness: HarnessKind, flows_root: &Path, identity: &str) -> Result<String> {
    let identity = normalize_uuid(identity)?;
    validate_root(flows_root)?;

    for end in FIRST_CANDIDATE_END..=identity.len() {
        let alias = &identity[23..end];
        match inspect_lane(flows_root, alias)? {
            Lane::Legacy => continue,
            Lane::Missing | Lane::Owned => {
                match claim_candidate(harness, flows_root, &identity, alias)? {
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
) -> Result<Candidate> {
    let marker_path = marker_path(root, alias);
    let expected = Marker::new(harness, identity, alias);
    let (mut marker_file, created) = open_marker(&marker_path)?;
    marker_file.lock().map_err(|source| Error::Filesystem {
        path: marker_path.clone(),
        source,
    })?;

    if !marker_path.exists() {
        return Ok(Candidate::Collision);
    }

    let marker = if created {
        write_marker(&marker_path, &mut marker_file, &expected)?;
        expected.clone()
    } else {
        read_marker(&marker_path, &mut marker_file)?
    };

    if marker.alias != alias {
        return Err(Error::MalformedMarker(marker_path));
    }
    if marker != expected {
        return Ok(Candidate::Collision);
    }

    let lane = root.join(alias);
    match fs::symlink_metadata(&lane) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(Error::UnsafeLane(lane))
        }
        Ok(metadata) => {
            if created {
                fs::remove_file(&marker_path).map_err(|source| Error::Filesystem {
                    path: marker_path,
                    source,
                })?;
                return Ok(Candidate::Collision);
            }
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

fn open_marker(path: &Path) -> Result<(File, bool)> {
    if path.exists() {
        validate_marker_path(path)?;
        return OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map(|file| (file, false))
            .map_err(|source| Error::Filesystem {
                path: path.into(),
                source,
            });
    }

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
            Ok((file, true))
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => open_marker(path),
        Err(source) => Err(Error::Filesystem {
            path: path.into(),
            source,
        }),
    }
}

fn write_marker(path: &Path, file: &mut File, marker: &Marker) -> Result<()> {
    file.write_all(marker.encode().as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|source| Error::Filesystem {
            path: path.into(),
            source,
        })
}

fn read_marker(path: &Path, file: &mut File) -> Result<Marker> {
    validate_marker_path(path)?;
    let mut text = String::new();
    file.read_to_string(&mut text)
        .map_err(|source| Error::Filesystem {
            path: path.into(),
            source,
        })?;
    Marker::decode(path, &text)
}

fn marker_path(root: &Path, alias: &str) -> PathBuf {
    root.join(format!(".{alias}.flow-id"))
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
