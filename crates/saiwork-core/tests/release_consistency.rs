//! CORE-008: release-identity consistency gate.
//!
//! A release must use one identity in release-facing source, runtime
//! diagnostics, Rust and JS package metadata, and the native Tauri bundle.
//! A split makes bug reports and packaged artifacts impossible to attribute
//! to a source release.
//!
//! This test fails closed: every project-owned version source must resolve to
//! the SAME VERSION. Bump them all together (see AGENTS.md reporting rule).

use std::fs;
use std::path::Path;

#[test]
fn release_identity_consistent() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR"); // crates/saiwork-core
    // Walk up to the workspace root (crates/saiwork-core -> crates -> root).
    let workspace_root = Path::new(manifest_dir)
        .parent()
        .expect("crates dir")
        .parent()
        .expect("workspace root");

    let expected = "0.1.7";

    // 1. VERSION file (release-facing source of truth).
    let version = fs::read_to_string(workspace_root.join("VERSION"))
        .expect("VERSION must exist")
        .trim()
        .to_string();
    assert_eq!(version, expected, "VERSION must be {expected}");

    // 2. Cargo workspace package version.
    let cargo = fs::read_to_string(workspace_root.join("Cargo.toml")).expect("Cargo.toml");
    assert!(
        cargo.contains(&format!("version = \"{expected}\"")),
        "workspace Cargo [workspace.package] version must be {expected}"
    );

    // 3. Tauri native bundle identity.
    let tauri = fs::read_to_string(workspace_root.join("apps/desktop/src-tauri/tauri.conf.json"))
        .expect("tauri.conf.json");
    assert!(
        tauri.contains(&format!("\"version\": \"{expected}\"")),
        "Tauri bundle version must be {expected}"
    );

    // 4. Root + desktop npm package metadata.
    let pkg = fs::read_to_string(workspace_root.join("package.json")).expect("package.json");
    assert!(
        pkg.contains(&format!("\"version\": \"{expected}\"")),
        "root package.json version must be {expected}"
    );
    let desktop_pkg =
        fs::read_to_string(workspace_root.join("apps/desktop/package.json")).expect("desktop package.json");
    assert!(
        desktop_pkg.contains(&format!("\"version\": \"{expected}\"")),
        "apps/desktop package.json version must be {expected}"
    );

    // 5. Runtime identity (APP_VERSION == CARGO_PKG_VERSION, inherited from the
    //    workspace package version).
    assert_eq!(
        env!("CARGO_PKG_VERSION"),
        expected,
        "runtime APP_VERSION must be {expected}"
    );
}
