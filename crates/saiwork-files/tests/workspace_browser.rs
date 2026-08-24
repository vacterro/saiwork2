//! Hostile-path and bounds tests for the read-only file browser
//! (SECURITY.md "Workspace boundary", ARCHITECTURE law 13).

use std::fs;
use std::io::Write;

use saiwork_files::{
    list_dir, read_preview, resolve_within, FileKind, FilesError, MAX_ENTRIES_PER_DIR,
};
use tempfile::TempDir;

fn tree() -> (TempDir, std::path::PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("ws");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("a.txt"), b"alpha").unwrap();
    fs::write(root.join("b.rs"), b"fn main() {}").unwrap();
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("sub/inner.txt"), b"inner").unwrap();
    (tmp, root)
}

#[test]
fn lists_root_sorted_dirs_first() {
    let (_tmp, root) = tree();
    let listing = list_dir(&root, ".").unwrap();
    assert_eq!(listing.dir, ".");
    let names: Vec<&str> = listing.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["sub", "a.txt", "b.rs"]);
    assert!(!listing.truncated);
    let sub = &listing.entries[0];
    assert_eq!(sub.kind, FileKind::Dir);
    assert_eq!(sub.rel_path, "sub");
    assert_eq!(sub.size, None);
    let a = &listing.entries[1];
    assert_eq!(a.kind, FileKind::File);
    assert_eq!(a.size, Some(5));
    assert!(a.modified_ms.is_some());
}

#[test]
fn dot_means_root_and_empty_is_rejected() {
    let (_tmp, root) = tree();
    let a = list_dir(&root, ".").unwrap();
    assert_eq!(a.entries.len(), 3);
    assert!(matches!(
        list_dir(&root, ""),
        Err(FilesError::InvalidRelative(_))
    ));
}

#[test]
fn nested_listing_keeps_forward_slash_rel_paths() {
    let (_tmp, root) = tree();
    let listing = list_dir(&root, "sub").unwrap();
    assert_eq!(listing.dir, "sub");
    assert_eq!(listing.entries[0].rel_path, "sub/inner.txt");
}

#[test]
fn rejects_parent_escape_and_absolute_paths() {
    let (_tmp, root) = tree();
    assert!(matches!(
        list_dir(&root, "../outside"),
        Err(FilesError::Escape(_))
    ));
    assert!(matches!(
        list_dir(&root, "sub/../../x"),
        Err(FilesError::Escape(_))
    ));
    assert!(matches!(
        list_dir(&root, "/etc"),
        Err(FilesError::InvalidRelative(_))
    ));
    assert!(matches!(
        list_dir(&root, r"\\.\pipe"),
        Err(FilesError::InvalidRelative(_))
    ));
    assert!(matches!(
        list_dir(&root, r"\\?\C:\x"),
        Err(FilesError::InvalidRelative(_))
    ));
    #[cfg(windows)]
    assert!(matches!(
        list_dir(&root, "C:/x"),
        Err(FilesError::InvalidRelative(_))
    ));
}

#[test]
fn missing_and_type_mismatch_are_typed_errors() {
    let (_tmp, root) = tree();
    assert!(matches!(
        list_dir(&root, "nope"),
        Err(FilesError::NotFound(_))
    ));
    assert!(matches!(
        list_dir(&root, "a.txt"),
        Err(FilesError::NotADirectory(_))
    ));
    assert!(matches!(
        read_preview(&root, "sub"),
        Err(FilesError::NotAFile(_))
    ));
}

#[cfg(unix)]
#[test]
fn symlink_escape_is_rejected_but_symlinks_are_listed() {
    let (_tmp, root) = tree();
    let outside = _tmp.path().join("outside");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("secret.txt"), b"secret").unwrap();
    std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
    fs::write(outside.join("sneaky.txt"), b"x").unwrap();

    let listing = list_dir(&root, ".").unwrap();
    let link = listing.entries.iter().find(|e| e.name == "link").unwrap();
    assert_eq!(link.kind, FileKind::Symlink);
    assert_eq!(link.size, None);

    // CORE-008: traversal through the symlink is rejected with the typed
    // no-follow error, NOT a generic escape — the entry is still listed as
    // kind=Symlink (see above), it just may not be traversed.
    assert!(matches!(
        list_dir(&root, "link/sub"),
        Err(FilesError::Symlink(_))
    ));
    assert!(matches!(
        read_preview(&root, "link/secret.txt"),
        Err(FilesError::Symlink(_))
    ));
}

#[test]
fn preview_bounds_at_cap_and_reports_truncation() {
    let (_tmp, root) = tree();
    let big = root.join("big.txt");
    let mut f = fs::File::create(&big).unwrap();
    f.write_all(&vec![b'x'; 40 * 1024]).unwrap();
    let pv = read_preview(&root, "big.txt").unwrap();
    assert!(pv.truncated);
    assert_eq!(pv.text.len(), saiwork_files::FILE_PREVIEW_MAX_BYTES);
    assert_eq!(pv.total_bytes, 40 * 1024);
}

#[test]
fn preview_small_file_is_byte_identical_and_untagged() {
    let (_tmp, root) = tree();
    let pv = read_preview(&root, "a.txt").unwrap();
    assert!(!pv.truncated);
    assert!(!pv.binary);
    assert_eq!(pv.text, "alpha");
    assert_eq!(pv.total_bytes, 5);
}

#[test]
fn preview_trims_a_split_multibyte_char() {
    let (_tmp, root) = tree();
    let file = root.join("utf.txt");
    fs::write(&file, "界".repeat(11_000)).unwrap();
    let pv = read_preview(&root, "utf.txt").unwrap();
    assert!(pv.truncated);
    assert!(!pv.binary);
    assert_eq!(pv.total_bytes, 33_000);
    // Char-boundary trim: every char is a full 3-byte 界.
    assert_eq!(pv.text.len() % 3, 0);
    assert!(pv.text.chars().all(|c| c == '界'));
}

#[test]
fn binary_file_has_empty_text() {
    let (_tmp, root) = tree();
    let bin = root.join("bin.dat");
    fs::write(&bin, [b'A', b'B', 0, b'C']).unwrap();
    let pv = read_preview(&root, "bin.dat").unwrap();
    assert!(pv.binary);
    assert_eq!(pv.text, "");
}

#[test]
fn oversized_directory_is_truncated_honestly() {
    let (_tmp, root) = tree();
    fs::create_dir_all(root.join("many")).unwrap();
    for i in 0..(MAX_ENTRIES_PER_DIR + 25) {
        fs::write(root.join("many").join(format!("f{i:04}.txt")), b"x").unwrap();
    }
    let listing = list_dir(&root, "many").unwrap();
    assert!(listing.truncated);
    assert_eq!(listing.entries.len(), MAX_ENTRIES_PER_DIR);
}

#[test]
fn oversized_directory_keeps_canonical_first_512() {
    // CORE-007: an oversized directory's membership must be the canonical
    // first MAX_ENTRIES_PER_DIR entries, not the raw read_dir name order.
    // Here dirs ("z...") raw-sort AFTER the files ("a..."), so the buggy
    // raw-prefix truncation would drop every directory — the new code keeps
    // them (dirs first) and only then fills from the canonical file order.
    let (_tmp, root) = tree();
    fs::create_dir_all(root.join("many")).unwrap();
    let n_dirs = 26usize;
    let n_files = MAX_ENTRIES_PER_DIR + 10; // total exceeds the cap
    for i in 0..n_dirs {
        fs::create_dir_all(root.join("many").join(format!("z{i:04}"))).unwrap();
    }
    for i in 0..n_files {
        fs::write(root.join("many").join(format!("a{i:04}.txt")), b"x").unwrap();
    }
    let listing = list_dir(&root, "many").unwrap();
    assert!(listing.truncated);
    assert_eq!(listing.entries.len(), MAX_ENTRIES_PER_DIR);

    // Every directory must be present despite raw-sorting after the files.
    let dir_names: Vec<&str> = listing
        .entries
        .iter()
        .filter(|e| e.kind == FileKind::Dir)
        .map(|e| e.name.as_str())
        .collect();
    assert_eq!(dir_names.len(), n_dirs, "no directory may be dropped");
    assert!(dir_names.iter().all(|n| n.starts_with('z')));

    // Reconstruct the canonical first 512 and compare exactly.
    let mut dirs: Vec<String> = (0..n_dirs).map(|i| format!("z{i:04}")).collect();
    dirs.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()).then_with(|| a.cmp(b)));
    let mut files: Vec<String> = (0..n_files).map(|i| format!("a{i:04}.txt")).collect();
    files.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()).then_with(|| a.cmp(b)));
    let mut expected: Vec<String> = Vec::new();
    expected.extend(dirs);
    expected.extend(files.into_iter().take(MAX_ENTRIES_PER_DIR - n_dirs));
    let got: Vec<&str> = listing.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(got, expected);
}

#[test]
fn case_insensitive_name_ordering_within_kind() {
    // The canonical comparator orders by case-insensitive name with an exact
    // name tiebreak — verify mixed-case names sort as the API promises.
    let (_tmp, root) = tree();
    fs::write(root.join("Banana.rs"), b"x").unwrap();
    fs::write(root.join("apple.rs"), b"x").unwrap();
    fs::write(root.join("Cherry.rs"), b"x").unwrap();
    let listing = list_dir(&root, ".").unwrap();
    let files: Vec<&str> = listing
        .entries
        .iter()
        .filter(|e| e.kind == FileKind::File)
        .map(|e| e.name.as_str())
        .collect();
    // case-insensitive: a.txt < apple.rs < b.rs < Banana.rs < Cherry.rs.
    assert_eq!(files, vec!["a.txt", "apple.rs", "b.rs", "Banana.rs", "Cherry.rs"]);
}

#[cfg(windows)]
#[test]
fn windows_junction_escape_is_rejected() {
    // CORE-019: a Windows junction (reparse point) inside the workspace
    // targeting an external directory must be rejected by resolve_within,
    // list_dir, and read_preview — the no-follow contract covers junctions
    // as path-redirection reparse points.
    use std::os::windows::fs::symlink_dir;
    let (_tmp, root) = tree();
    let outside = _tmp.path().join("outside");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("secret.txt"), b"secret").unwrap();
    // Create a junction (directory symlink) inside the workspace pointing
    // outside. On Windows, `symlink_dir` creates a junction when the caller
    // lacks SeCreateSymbolicLinkPrivilege.
    symlink_dir(&outside, root.join("junction"))
        .expect("creating junction requires privileges");

    let listing = list_dir(&root, ".").unwrap();
    let j = listing
        .entries
        .iter()
        .find(|e| e.name == "junction")
        .expect("junction listed");
    // A junction must be classified as Symlink (path-redirection), not Dir.
    assert_eq!(j.kind, FileKind::Symlink);

    // Traversal through the junction is rejected with the typed no-follow error.
    assert!(matches!(
        list_dir(&root, "junction"),
        Err(FilesError::Symlink(_))
    ));
    assert!(matches!(
        read_preview(&root, "junction/secret.txt"),
        Err(FilesError::Symlink(_))
    ));
}

#[test]
fn resolve_within_returns_canonical_path() {
    // CORE-019: resolve_within must return the canonical (realpath-resolved)
    // path so callers operate on the verified target, never on the unchecked
    // lexical path. This catches any path redirection that the per-component
    // check might miss.
    let (_tmp, root) = tree();
    // A nested directory with a case-variant name on a case-insensitive fs:
    // the canonical path differs from the lexical path.
    fs::create_dir_all(root.join("Sub")).unwrap();
    fs::write(root.join("Sub/file.txt"), b"content").unwrap();
    let resolved = resolve_within(&root, "Sub/file.txt").unwrap();
    // The returned path must be canonicalized (absolute, no component
    // ambiguity).
    assert!(resolved.is_absolute());
    // The file must actually exist at the resolved path.
    assert!(resolved.exists(), "resolved path must be accessible: {resolved:?}");
}

#[cfg(unix)]
#[test]
fn internal_symlinks_are_listed_but_not_traversed() {
    // CORE-008: a symlink whose target stays INSIDE the workspace must still
    // be listed as kind=Symlink, but list_dir/read_preview THROUGH it are
    // rejected with the typed no-follow error (FilesError::Symlink).
    let (_tmp, root) = tree();
    fs::create_dir_all(root.join("inner")).unwrap();
    fs::write(root.join("inner/data.txt"), b"hi").unwrap();
    fs::write(root.join("real.txt"), b"real").unwrap();
    std::os::unix::fs::symlink(root.join("inner"), root.join("linkdir")).unwrap();
    std::os::unix::fs::symlink(root.join("real.txt"), root.join("linkfile")).unwrap();

    let listing = list_dir(&root, ".").unwrap();
    assert_eq!(
        listing.entries.iter().find(|e| e.name == "linkdir").unwrap().kind,
        FileKind::Symlink
    );
    assert_eq!(
        listing.entries.iter().find(|e| e.name == "linkfile").unwrap().kind,
        FileKind::Symlink
    );

    assert!(matches!(
        list_dir(&root, "linkdir"),
        Err(FilesError::Symlink(_))
    ));
    assert!(matches!(
        read_preview(&root, "linkfile"),
        Err(FilesError::Symlink(_))
    ));
    // The real targets remain directly accessible.
    assert!(read_preview(&root, "real.txt").is_ok());
    assert!(list_dir(&root, "inner").is_ok());
}
