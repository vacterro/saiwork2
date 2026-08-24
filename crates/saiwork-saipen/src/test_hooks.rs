//! Test-only failpoints for deterministic perf/correctness tests (TASK 24
//! perf pass). Compiled only with the `failpoints` feature — never reachable
//! in production builds. Keyed by root path so parallel tests cannot
//! interfere with each other.
//!
//! - `set_read_slow`: make every snapshot read for one root sleep (blocks
//!   the read so a test can prove reads never hold the service lock and
//!   never occupy a Tokio async worker).
//! - `read_count`: total authoritative snapshot reads performed for one
//!   root since the last `clear` (attach + refreshes) — used to prove
//!   storm coalescing.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use crate::model::SaipenRoot;

static READ_SLOWS: Mutex<Option<HashMap<String, Duration>>> = Mutex::new(None);
static READ_COUNTS: std::sync::LazyLock<Mutex<HashMap<String, u64>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

fn key(root: &Path) -> String {
    // Same canonicalization the reader uses for SaipenRoot.dir (symlink
    // resolution, `\\?\` strip, Windows case folding) so test paths and
    // canonical roots always match. Tests pass the WORKSPACE root while the
    // reader records the canonical `<workspace>/.saipen` dir — normalize
    // both sides to the same key by appending SAIPEN_DIR when missing.
    let resolved = crate::paths::resolve(root).unwrap_or_else(|_| root.to_path_buf());
    let is_saipen_dir = resolved
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .is_some_and(|n| n == crate::model::SAIPEN_DIR);
    if is_saipen_dir {
        resolved.to_string_lossy().into_owned()
    } else {
        resolved.join(crate::model::SAIPEN_DIR).to_string_lossy().into_owned()
    }
}

/// Make every authoritative snapshot read for `root` sleep `d` — the read
/// is genuinely blocked, so a test can prove the service lock is free and
/// other workspaces stay responsive while it is in flight.
pub fn set_read_slow(root: &Path, d: Duration) {
    let mut guard = READ_SLOWS.lock().expect("read-slows mutex poisoned");
    guard
        .get_or_insert_with(HashMap::new)
        .insert(key(root), d);
}

/// Reset all failpoint state (slow tables + counters).
pub fn clear() {
    *READ_SLOWS.lock().expect("read-slows mutex poisoned") = None;
    READ_COUNTS.lock().expect("read-counts mutex poisoned").clear();
}

/// Total authoritative snapshot reads performed for `root` since `clear`
/// (attach + refreshes). A 100-event storm coalescing into ~1–2 refreshes
/// proves the service-level single-reader rule.
pub fn read_count(root: &Path) -> u64 {
    READ_COUNTS
        .lock()
        .expect("read-counts mutex poisoned")
        .get(&key(root))
        .copied()
        .unwrap_or(0)
}

/// Called at the top of every authoritative `read_snapshot`: counts the
/// read, then blocks it if this root is configured slow.
pub fn maybe_slow_down(root: &SaipenRoot) {
    {
        let mut counts = READ_COUNTS.lock().expect("read-counts mutex poisoned");
        *counts.entry(key(&root.dir)).or_insert(0) += 1;
    }
    let slow = READ_SLOWS
        .lock()
        .expect("read-slows mutex poisoned")
        .as_ref()
        .and_then(|m| m.get(&key(&root.dir)).copied());
    if let Some(d) = slow {
        std::thread::sleep(d);
    }
}
