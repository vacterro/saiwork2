# OUTBOX

## W-001: SAIWORK2 maintained wiki — FORCE-FRESH prepare, 25 laws + 20-doc index mirrored by ID
- **status:** ready
- **summary:** FORCE-FRESH `saipen prepare saiwiki` over SAIWORK2 at baseline `7c74d05`. Mirrors the canonical project truth into a maintained wiki package: the 25 non-negotiable laws (LAW-01..25, verbatim by ID), the 10 confirmed donor landmines, a 20-doc KNOWLEDGE index, the producer status table (saiwiki ready / saitranslate blocked), a shipped audit baseline (3 waves, 16 fixes → commit 7c74d05, v0.1.0), post-release shipped items (T-075..T-081: subscription hygiene, shell layout, launch panic, preset import, auto-session/engine, SAIPEN trust, dialog.confirm), and an IN-PROGRESS 23-ticket audit note (3 waves, T-084..T-106, uncommitted `cargo test` / `vitest` verification running). Pages carry source-digest mirror markers. No main-project file is touched; integration is Core's job via `qqq`.
- **main_project_refs:** [KNOWLEDGE/ARCHITECTURE.md, CHANGELOG.md, RELEASE_NOTES.md, KNOWLEDGE/INDEX.md, KNOWLEDGE/*.md]
- **critical:** false
- **producer:** saiwiki
- **source_head:** 7c74d055074602dbf87fe6972fde2fa231042767
- **source_tree_fingerprint:** git-delta-v1:bd2eff1301796b2873555e89e72675ceb99f362b4c9ee042c0269e9669906c1f
- **role_revision:** sha256:54a42475a124ab0f27e83d600a284a9cc54d9668029c4828cfc48512b031df13
- **coverage:**
  - Architecture.md: 25 laws reproduced verbatim from KNOWLEDGE/ARCHITECTURE.md §"The 25 non-negotiable laws", anchored LAW-01..LAW-25; + 10 confirmed donor landmines. IDs 1-25 present, no duplicates. Byte-identical to source — mirror digest `abffe4dd` matches current ARCHITECTURE.md.
  - Home.md: project identity, release baseline (v0.1.0 / 7c74d05), shipped 3-wave audit summary (16 fixes: CORE-001..007, W2-001..006, PERF-001/004/005/006), post-release shipped (T-075..T-081), IN-PROGRESS 23-ticket audit (T-084..T-106; uncommitted, verification running), engines, page nav.
  - Knowledge.md: index of all 20 KNOWLEDGE/*.md docs with one-line purpose each (count verified = 20 via `ls KNOWLEDGE/*.md`).
  - SubSaipen.md: producer status table — saiwiki W-001 ready, saitranslate SAIT-001 blocked (no real translation surface); + note that the in-progress 23-ticket audit is Core's work, not a producer's.
  - _Footer.md: source-digest rollup + freshness triple + canonical-source list; fingerprint updated to `bd2eff13...`.
- **payload:** 5 wiki pages in `kitchen/wiki/` (local, uncommitted):
  - `Home.md`, `Architecture.md`, `Knowledge.md`, `SubSaipen.md`, `_Footer.md`
  - To integrate: review the pages, then `qqq` collects this `status: ready` handoff and routes through VERIFY/REVIEW/SHIP.
- **verified:**
  - Law count = 25, IDs LAW-01..LAW-25 present, no duplicate IDs; each law text matches KNOWLEDGE/ARCHITECTURE.md §"The 25 non-negotiable laws" verbatim. Mirror digest `abffe4dd` matches current ARCHITECTURE.md (re-verified live).
  - KNOWLEDGE doc count = 20 (ARCHITECTURE, DECISIONS, DEEPSEEK_HARNESS, ENGINE_CONTRACT, EVENTS, INDEX, MIGRATION_SAIWORK, PERFORMANCE, PORTABILITY, PROCESS_LIFECYCLE, PRODUCT, QUEUE, REGRESSION_BACKLOG, ROADMAP, SAIPEN, SECURITY, STORAGE, TESTING, THIRD_PARTY, UI_UX) — verified by `ls KNOWLEDGE/*.md | wc -l` = 20.
  - Freshness triple recomputed live at prepare-time: source_head `7c74d05`, fingerprint `git-delta-v1:bd2eff13…`, role_revision `sha256:54a42475…`; charter role_revision matches declared value.
  - source_head is the documented baseline `7c74d05` (git is not addressable in this checkout; the working tree carries uncommitted post-release edits including 23-ticket audit, captured by the fingerprint). The fingerprint is a content hash over the documented surface (everything except `.saipen/`, `target/`, `node_modules/`, `_vtmp/`, `donors/`), so it moves whenever any documented file changes.
  - CHANGELOG.md digest `7f52db0b` (release log advanced); mirrored reference in Home.md / _Footer.md carries the matching digest. Mirrored content subset (25 laws + 20 KNOWLEDGE docs) is byte-identical (verified by source digests `abffe4dd` / `d5fc4f98`).
  - No main-project file is read-written by this package; writes confined to `.saipen/extensions/subs/saiwiki/`.
- **instructions:**
  1. Review the uncommitted wiki pages: `cat .saipen/extensions/subs/saiwiki/kitchen/wiki/*.md`.
  2. Integrate via `qqq` (collect saiwiki) — Core claims the ticket and routes through VERIFY/REVIEW/SHIP; this producer never writes the main tree.
  3. Verify live after integration: Architecture.md shows LAW-01..LAW-25, Knowledge.md lists 20 docs, _Footer carries the matching fingerprint.
  4. If the tree moves (source_head/fingerprint/role_revision mismatch), this package is stale — re-run `qq`, do not silently reuse.
  5. Cross-reference: saitranslate SAIT-001 is `blocked`; do not expect a translation bundle until the i18n gap is closed.
