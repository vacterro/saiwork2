//! Read-only workspace file browsing (Phase C, FB-C).
//!
//! Security model: the frontend NEVER passes raw paths. Every operation
//! resolves the workspace root from a WorkspaceId and treats the rel path
//! as untrusted input: normalized, component-checked, then canonicalized
//! and containment-verified against the canonical root (SECURITY.md
//! "Workspace boundary" — never a naive string prefix).
//!
//! Symlinks are never followed for traversal: a directory listing classifies
//! symlinks without following them (kind = Symlink), and any attempt to open
//! a path whose canonical target lands outside the workspace is rejected.

use std::io::Read;
use std::path::{Component, Path, PathBuf};

use std::collections::BinaryHeap;

use serde::Serialize;

/// Hard cap on entries returned per directory listing (law 13). Sorted
/// listing is cut at this bound and `truncated` is reported honestly.
pub const MAX_ENTRIES_PER_DIR: usize = 512;

/// Bounded text preview: a file longer than this is reported via
/// `FilePreview::truncated` and only the head is returned.
pub const FILE_PREVIEW_MAX_BYTES: usize = 32 * 1024;

/// NUL-sniff window for binary detection. A NUL inside this window means
/// "not text"; the preview text is then empty.
pub const BINARY_SNIFF_BYTES: usize = 4096;

#[derive(Debug, thiserror::Error)]
pub enum FilesError {
    #[error("workspace-relative path required: {0}")]
    InvalidRelative(String),
    #[error("path escapes the workspace: {0}")]
    Escape(String),
    #[error("not a directory: {0}")]
    NotADirectory(String),
    #[error("not a file: {0}")]
    NotAFile(String),
    /// A path component (including the final target) is a symbolic link.
    /// The published contract is "symlinks are never followed for traversal"
    /// (CORE-008): the entry is still listed as `kind = Symlink`, but no read
    /// or listing may traverse through it.
    #[error("symlink traversal rejected (never followed): {0}")]
    Symlink(String),
    #[error("path not found: {0}")]
    NotFound(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    File,
    Dir,
    /// Symbolic link — listed, never followed.
    Symlink,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileEntry {
    /// Display name (last path component). Non-UTF-8 names are lossy-mapped.
    pub name: String,
    /// Workspace-relative path with forward slashes ("sub/name.txt").
    pub rel_path: String,
    pub kind: FileKind,
    /// Byte size; files only.
    pub size: Option<u64>,
    /// Last-modified ms since epoch; files only.
    pub modified_ms: Option<i64>,
    /// W2-007: whether the UI may OPEN this entry. A non-UTF-8 filename cannot
    /// be represented losslessly as a rel-path token (canonicalizing a lossy
    /// copy would resolve to the WRONG file or none), so it is listed with its
    /// lossy display name but marked `false` — `rel_path` is then empty so the
    /// UI can never hand back an unopenable token.
    pub navigable: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DirListing {
    /// The rel path that was listed ("." for the workspace root).
    pub dir: String,
    /// Sorted entries (dirs first, then case-insensitive name order).
    /// Bounded at MAX_ENTRIES_PER_DIR; oversized dirs report `truncated`.
    pub entries: Vec<FileEntry>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilePreview {
    pub rel_path: String,
    /// Bounded UTF-8 head of the file; char-boundary trimmed. Empty for
    /// binary files and empty files.
    pub text: String,
    /// True when the file is longer than FILE_PREVIEW_MAX_BYTES.
    pub truncated: bool,
    /// True when a NUL byte was found in the sniff window.
    pub binary: bool,
    pub total_bytes: u64,
}

// ---- Path boundary (pattern from saiwork-saipen::paths, SECURITY.md) ----

/// Normalize for comparison: strip the extended-length prefix and (on
/// Windows) fold case. Comparison form only — never used for fs access.
fn normalize_for_compare(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_string_lossy().into_owned();
    if cfg!(windows) {
        for prefix in [r"\\?\", r"\\.\"] {
            if let Some(rest) = s.strip_prefix(prefix) {
                s = rest.to_string();
                break;
            }
        }
        s = s.to_lowercase();
    }
    PathBuf::from(s)
}

fn same_component(a: Component<'_>, b: Component<'_>) -> bool {
    match (a, b) {
        (Component::Normal(a), Component::Normal(b)) => {
            let sa = a.to_string_lossy();
            let sb = b.to_string_lossy();
            if cfg!(windows) {
                sa.to_lowercase() == sb.to_lowercase()
            } else {
                sa == sb
            }
        }
        (Component::RootDir, Component::RootDir) => true,
        (Component::CurDir, Component::CurDir) => true,
        (Component::Prefix(a), Component::Prefix(b)) => a.as_os_str() == b.as_os_str(),
        _ => false,
    }
}

/// Component-aware containment: `child` is inside `parent` when every
/// component of `parent` prefixes `child`. Both already normalized.
fn is_contained(child: &Path, parent: &Path) -> bool {
    let mut child_iter = child.components();
    for pc in parent.components() {
        match child_iter.next() {
            Some(cc) if same_component(pc, cc) => {}
            _ => return false,
        }
    }
    true
}

/// Untrusted rel path → safe relative PathBuf. Rejects absolute forms,
/// device/UNC roots, and any `..` component. `.` components are dropped.
fn validate_rel(rel: &str) -> Result<PathBuf, FilesError> {
    // W2-006: do NOT trim. Rel-path tokens are untrusted input that may carry
    // meaningful whitespace (a space-padded or whitespace-only component is a
    // legitimate, if unusual, filename). Trimming here would corrupt the token
    // before containment checks and break the list/preview round-trip.
    let raw = rel;
    if raw.is_empty() {
        return Err(FilesError::InvalidRelative("empty".into()));
    }
    // "." is the workspace root itself.
    if raw == "." {
        return Ok(PathBuf::new());
    }
    // Device namespaces are never valid as relative paths, on any host
    // (the string check keeps tests portable).
    for prefix in [r"\\.\", r"\\?\"] {
        if raw.starts_with(prefix) {
            return Err(FilesError::InvalidRelative(raw.to_string()));
        }
    }
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(FilesError::InvalidRelative(raw.to_string()));
    }
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir => return Err(FilesError::Escape(raw.to_string())),
            Component::RootDir | Component::Prefix(_) => {
                return Err(FilesError::InvalidRelative(raw.to_string()))
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(FilesError::InvalidRelative(raw.to_string()));
    }
    Ok(out)
}

/// True when `symlink_metadata` reports a path-redirection reparse point on
/// Windows (junction, mount point, or symlink). On Unix this always returns
/// false — the caller checks `is_symlink()` separately.
fn is_windows_reparse_point(md: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        md.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        let _ = md;
        false
    }
}

/// Final containment-proving canonicalization of the resolved target.
///
/// INVARIANT (CORE-003): this step is the proof that the target lands inside
/// the workspace. It must NEVER silently fall back to the unchecked lexical
/// path — a canonicalization failure means the proof does not exist, so the
/// error is propagated and the caller must refuse to operate (fail-closed).
///
/// Test seam: under `#[cfg(test)]` a thread-local failpoint lets a test force
/// the canonicalization to fail deterministically, WITHOUT affecting other
/// tests (the default harness runs tests in parallel on separate threads, so a
/// process-global flag would spuriously break them). The seam compiles out
/// entirely in non-test builds (zero cost).
fn canonicalize_final(p: &Path) -> std::io::Result<PathBuf> {
    #[cfg(test)]
    {
        if FORCE_CANON_FAIL.with(|f| f.get()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "forced canonicalization failure (test failpoint)",
            ));
        }
    }
    std::fs::canonicalize(p)
}

#[cfg(test)]
thread_local! {
    /// Test failpoint: when set, `canonicalize_final` returns `Err` instead of
    /// calling `std::fs::canonicalize`. Thread-local so it cannot perturb
    /// other tests running in parallel under the default test harness.
    static FORCE_CANON_FAIL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Resolve `rel` inside `root` WITHOUT following symbolic links, junctions,
/// or any other path-redirection reparse point (CORE-008, CORE-019).
///
/// The published contract is "symlinks are never followed for traversal":
/// a directory listing classifies symlinks, but no path component —
/// including the final target — may be a symlink or Windows junction.
/// Every component is checked with `symlink_metadata` under the canonical
/// root; any link-like component (symlink on any platform, reparse point
/// on Windows) is rejected.
///
/// After the walk the final resolved target is canonicalized and its
/// containment is verified against the canonical root — this catches any
/// path redirection that the per-component check might miss (e.g. a
/// Windows junction whose `symlink_metadata` does not expose the redirect
/// through `FileType::is_symlink()`). The canonicalized path is returned
/// so callers operate on the verified target, never on the unchecked
/// lexical path.
pub fn resolve_within(root: &Path, rel: &str) -> Result<PathBuf, FilesError> {
    let rel = validate_rel(rel)?;
    let root_access = std::fs::canonicalize(root)?;
    // Walk each component WITHOUT dereferencing symlinks or junctions.
    // Pushing a component onto the canonical root and inspecting it with
    // `symlink_metadata` proves the path stays inside the workspace and is
    // never a link-like redirection.
    let mut current = root_access.clone();
    for comp in rel.components() {
        current.push(comp);
        let md = match std::fs::symlink_metadata(&current) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(FilesError::NotFound(rel.to_string_lossy().into_owned()))
            }
            Err(e) => return Err(FilesError::Io(e)),
        };
        // Reject symlinks (all platforms) and Windows reparse points
        // (junctions, mount points). On Windows, `FileType::is_symlink()`
        // returns false for junctions — the reparse-point attribute check
        // catches them.
        if md.file_type().is_symlink() || is_windows_reparse_point(&md) {
            // No-follow contract: a link-like component is never traversed.
            // The entry itself remains visible in its parent listing as
            // kind=Symlink.
            return Err(FilesError::Symlink(current.to_string_lossy().into_owned()));
        }
    }
    // Defense in depth: canonicalize the final path (which follows any
    // remaining redirection the per-component check could not see) and
    // verify the resolved target is inside the workspace. This catches
    // junctions whose `symlink_metadata` does not expose the redirect via
    // `FileType::is_symlink()` — the canonicalized path will resolve to
    // the junction's target, which may differ from the lexical path.
    // CORE-003: a canonicalization failure means the containment proof does
    // not exist — fail closed (propagate) rather than fall back to the
    // unchecked lexical `current`, which would silently bypass the check.
    let resolved = canonicalize_final(&current).map_err(FilesError::Io)?;
    let root_norm = normalize_for_compare(&root_access);
    let resolved_norm = normalize_for_compare(&resolved);
    if !is_contained(&resolved_norm, &root_norm) {
        return Err(FilesError::Escape(rel.to_string_lossy().into_owned()));
    }
    Ok(resolved)
}

fn join_rel(parent: &str, name: &str) -> String {
    if parent == "." {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

/// List one directory (never recursive — the UI expands lazily).
pub fn list_dir(root: &Path, rel: &str) -> Result<DirListing, FilesError> {
    let dir = resolve_within(root, rel)?;
    let md = std::fs::metadata(&dir)?;
    if !md.is_dir() {
        return Err(FilesError::NotADirectory(rel.to_string()));
    }
    // W2-006: keep the rel token verbatim (no trimming) so whitespace-bearing
    // components round-trip exactly through list/preview (validate_rel no
    // longer trims either).
    let parent_rel = if rel.is_empty() || rel == "." {
        "."
    } else {
        rel
    };

    // CORE-007: classify EVERY eligible entry before any truncation, then
    // order with the exact canonical comparator the returned list promises.
    // Membership of an oversized result is therefore the canonical first
    // MAX_ENTRIES_PER_DIR entries — not the raw read_dir name order. The
    // truncation bound and the sort comparator share ONE canonical rule.
    // CORE-007 / PERF-005: keep the canonical top-K, never every entry. A
    // directory with millions of files must not materialize them all into
    // memory just to show the first `MAX_ENTRIES_PER_DIR`. Each classified
    // entry is pushed into a bounded max-heap (cap = MAX_ENTRIES_PER_DIR); on
    // overflow the canonical-last entry is evicted, so the retained set is
    // exactly the K entries that sort earliest by `canonical_entry_cmp` —
    // identical membership and display order to the old collect-then-truncate,
    // at bounded memory. `truncated` is true iff we ever evicted.
    let mut truncated = false;
    let mut heap: BinaryHeap<EntryKey> = BinaryHeap::with_capacity(MAX_ENTRIES_PER_DIR + 1);
    for ent in std::fs::read_dir(&dir)?
        .filter_map(|e| e.ok())
        .filter_map(|ent| {
            // W2-007: a non-UTF-8 filename cannot be represented losslessly as
            // a workspace-relative path token — canonicalizing a lossy copy
            // would resolve to the WRONG file (or none). Keep its (lossy)
            // display name but mark it non-navigable: the UI must never open
            // it. Valid UTF-8 names stay navigable.
            let (name, navigable) = match ent.file_name().into_string() {
                Ok(n) => (n, true),
                Err(os) => (os.to_string_lossy().into_owned(), false),
            };
            let md = match std::fs::symlink_metadata(ent.path()) {
                Ok(m) => m,
                Err(_) => return None,
            };
            let ft = md.file_type();
            // CORE-019: classify symlinks AND Windows reparse points
            // (junctions, mount points) as Symlink — they redirect access
            // and must never be traversed.
            let kind = if ft.is_symlink() || is_windows_reparse_point(&md) {
                FileKind::Symlink
            } else if ft.is_dir() {
                FileKind::Dir
            } else if ft.is_file() {
                FileKind::File
            } else {
                return None;
            };
            let (size, modified_ms) = if kind == FileKind::File {
                let modified_ms = md
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as i64);
                (Some(md.len()), modified_ms)
            } else {
                (None, None)
            };
            // W2-007: a non-navigable (non-UTF-8) entry exposes no openable
            // token; navigable entries carry their exact rel-path.
            let rel_path = if navigable {
                join_rel(parent_rel, &name)
            } else {
                String::new()
            };
            Some(FileEntry {
                name,
                rel_path,
                kind,
                size,
                modified_ms,
                navigable,
            })
        })
    {
        heap.push(EntryKey(ent));
        if heap.len() > MAX_ENTRIES_PER_DIR {
            heap.pop();
            truncated = true;
        }
    }
    let mut all: Vec<FileEntry> = heap.into_iter().map(|k| k.0).collect();
    // Canonical ordering (dirs first, then case-insensitive name, then exact
    // name) decides BOTH membership and the final display order.
    all.sort_by(canonical_entry_cmp);
    Ok(DirListing {
        dir: parent_rel.to_string(),
        entries: all,
        truncated,
    })
}

/// Canonical entry ordering promised by `DirListing`: dirs first, then
/// case-insensitive name, then exact name (stable tiebreak on Windows).
/// Shared by the membership selection and the final display sort (CORE-007)
/// so an oversized result keeps exactly the canonical first
/// MAX_ENTRIES_PER_DIR entries.
fn canonical_entry_cmp(a: &FileEntry, b: &FileEntry) -> std::cmp::Ordering {
    kind_rank(a.kind)
        .cmp(&kind_rank(b.kind))
        .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        .then_with(|| a.name.cmp(&b.name))
}

fn kind_rank(kind: FileKind) -> u8 {
    match kind {
        FileKind::Dir => 0,
        FileKind::File => 1,
        FileKind::Symlink => 2,
    }
}

/// Wrapper that orders `FileEntry` by `canonical_entry_cmp` so a `BinaryHeap`
/// (a max-heap) pops the canonical-last entry first. Used by `list_dir` to
/// retain the top-K smallest entries at bounded memory (PERF-005).
struct EntryKey(FileEntry);

impl PartialEq for EntryKey {
    fn eq(&self, other: &Self) -> bool {
        canonical_entry_cmp(&self.0, &other.0) == std::cmp::Ordering::Equal
    }
}
impl Eq for EntryKey {}
impl PartialOrd for EntryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for EntryKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        canonical_entry_cmp(&self.0, &other.0)
    }
}

/// Char-boundary-safe UTF-8 decode: on a partial trailing char, drop the
/// incomplete tail instead of emitting a replacement char.
fn utf8_trimmed(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(e) => {
            let valid = e.valid_up_to();
            // SAFETY: `valid` is a UTF-8 boundary by from_utf8's contract.
            unsafe { String::from_utf8_unchecked(bytes[..valid].to_vec()) }
        }
    }
}

/// Bounded head preview of a file (read-only).
pub fn read_preview(root: &Path, rel: &str) -> Result<FilePreview, FilesError> {
    let path = resolve_within(root, rel)?;
    let md = std::fs::metadata(&path)?;
    if !md.is_file() {
        return Err(FilesError::NotAFile(rel.to_string()));
    }
    let total_bytes = md.len();
    let truncated = total_bytes > FILE_PREVIEW_MAX_BYTES as u64;
    let want = (FILE_PREVIEW_MAX_BYTES as u64).min(total_bytes) as usize;
    let mut buf = vec![0u8; want];
    let mut file = std::fs::File::open(&path)?;
    let mut filled = 0usize;
    while filled < buf.len() {
        let n = file.read(&mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    let sniff = buf.len().min(BINARY_SNIFF_BYTES);
    let binary = buf[..sniff].contains(&0);
    let text = if binary { String::new() } else { utf8_trimmed(&buf) };
    Ok(FilePreview {
        // W2-006: preserve the rel token verbatim (no trimming) so the preview
        // token matches exactly what list_dir handed the UI.
        rel_path: rel.to_string(),
        text,
        truncated,
        binary,
        total_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_root(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "saiwork_files_test_{}_{}",
            label,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::create_dir_all(&dir);
        dir
    }

    fn rmrf(p: &std::path::Path) {
        let _ = fs::remove_dir_all(p);
    }

    #[test]
    fn validate_rel_preserves_whitespace_tokens() {
        // W2-006: surrounding whitespace in a rel token must survive validation
        // — the backend must not trim it. A space-padded component is a
        // legitimate filename; trimming would corrupt the token.
        let p = validate_rel(" a/b ").expect("valid rel");
        let parts: Vec<String> = p
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect();
        assert_eq!(parts, vec![" a".to_string(), "b ".to_string()]);
    }

    #[test]
    fn list_dir_and_preview_round_trip_preserve_names() {
        // W2-006: a listing + preview must carry the exact rel_path the caller
        // passed (no trimming), so a whitespace-bearing token round-trips.
        let root = temp_root("roundtrip");
        fs::write(root.join("hello world.txt"), b"content").unwrap();
        let listing = list_dir(&root, ".").expect("list root");
        let entry = listing
            .entries
            .iter()
            .find(|e| e.name == "hello world.txt")
            .expect("entry present");
        assert_eq!(entry.rel_path, "hello world.txt");
        assert!(entry.navigable);
        let preview = read_preview(&root, &entry.rel_path).expect("preview");
        assert_eq!(preview.rel_path, "hello world.txt");
        rmrf(&root);
    }

    #[test]
    fn final_canonicalize_failure_fails_closed() {
        // CORE-003: when the final containment-proving canonicalization fails,
        // resolve_within must NOT fall back to the unchecked lexical path. The
        // resolver must return Err (fail-closed), and callers that trust it
        // (list_dir / read_preview, which call resolve_within first) must also
        // refuse — NO filesystem operation happens on the unverified lexical
        // target. The target is a real, in-workspace file so that a fail-OPEN
        // bug (returning the lexical path) would have succeeded and been
        // detected as a test failure.
        FORCE_CANON_FAIL.with(|f| f.set(true));
        let root = temp_root("canonfail");
        fs::write(root.join("real.txt"), b"secret").unwrap();

        // Component walk succeeds (no symlink); the only failing step is the
        // final canonicalize, which the failpoint forces to fail.
        let res = resolve_within(&root, "real.txt");
        assert!(
            res.is_err(),
            "resolve_within must fail closed when final canonicalize fails"
        );
        // Callers must refuse to operate on the unverified path.
        assert!(
            list_dir(&root, "real.txt").is_err(),
            "list_dir must fail when resolve_within fails closed"
        );
        assert!(
            read_preview(&root, "real.txt").is_err(),
            "read_preview must fail when resolve_within fails closed"
        );

        FORCE_CANON_FAIL.with(|f| f.set(false));
        // Sanity: with the failpoint cleared, resolution still works.
        assert!(
            resolve_within(&root, "real.txt").is_ok(),
            "resolution must succeed once the failpoint is cleared"
        );
        rmrf(&root);
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_filename_is_marked_non_navigable() {
        // W2-007: a filename that is not valid UTF-8 is still listed (lossy
        // display name) but marked non-navigable with an empty rel_path, so
        // the UI can never open a token it cannot canonicalize back to the
        // real file.
        use std::os::unix::ffi::OsStringExt;
        let root = temp_root("nonutf8");
        let bad = std::path::Path::new(&root)
            .join(std::ffi::OsString::from_vec(b"valid\xfe.bin".to_vec()));
        fs::write(&bad, b"x").unwrap();
        let listing = list_dir(&root, ".").expect("list root");
        let entry = listing
            .entries
            .iter()
            .find(|e| e.name == "valid\u{fffd}.bin")
            .expect("non-UTF-8 entry listed");
        assert!(!entry.navigable, "non-UTF-8 entry must be non-navigable");
        assert_eq!(
            entry.rel_path, "",
            "non-navigable entry must not expose an openable token"
        );
        rmrf(&root);
    }
}
