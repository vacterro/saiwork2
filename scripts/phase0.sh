#!/usr/bin/env bash
# SAIWORK2 — canonical Phase 0 validation gate (TASK 09 §100).
# Orchestrates the full static + automated runtime gate. Every step must pass;
# the script stops at the first failure. Run from the repository root:
#
#   bash scripts/phase0.sh            # static gates only
#   bash scripts/phase0.sh --runtime  # also desktop runtime torture (slow)
#
# What stays manual/platform-smoke (never hidden inside this command):
#   - packaged release build + install (cargo tauri build) — release build
#   - real Windows smoke: paths with spaces, Unicode path, read-only portable
#     location, second-instance races at the OS level, process-tree evidence
set -u
cd "$(dirname "$0")/.."
FAIL=0

step() { printf '\n=== %s ===\n' "$1"; }

step "cargo fmt --check"
cargo fmt --check || FAIL=1

step "cargo clippy --workspace --all-targets"
cargo clippy --workspace --all-targets || FAIL=1

step "cargo test --workspace"
cargo test --workspace || FAIL=1

step "frontend typecheck"
npm run typecheck || FAIL=1

step "frontend build"
npm run build || FAIL=1

if [ "$FAIL" -ne 0 ]; then
  echo "PHASE 0 GATE: FAIL (see failing step above)"
  exit 1
fi

echo "PHASE 0 GATE (static): PASS"

if [ "${1:-}" = "--runtime" ]; then
  step "desktop runtime torture"
  EXE=target/debug/saiwork2.exe
  if [ ! -f "$EXE" ]; then
    echo "no $EXE — skipping runtime torture (build it with: cargo build -p saiwork2)"
    exit 0
  fi
  bash scripts/torture.sh "$EXE" || exit 1
  echo "PHASE 0 GATE (runtime): PASS"
fi
