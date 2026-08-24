# Board

<!-- Ticket shape is RFC § 1.2's, exactly: a checkbox, the T-### id, a
     description, then only the fields that apply, space-pipe separated.
     Shown here WITHOUT its leading "- " on purpose (see below):

       [ ] T-001 short description | verify: pytest -q

     Other legal fields (RFC § 1.2): the dependency one, taking a
     comma-separated list of T-### this ticket waits on; owner and
     claim_time for claims (§ 1.4); blocker for facts + dead ends; verify as
     shown above. Named rather than shown here on purpose -- see below.

     A real line starts with "- ". Checkbox: [ ] open, [/] in progress
     (## DOING), [x] done (## DONE). A status change MOVES the line between
     sections -- cut and paste, never copy, or the same id ends up under two
     headings. All four headings below are required, even while empty.

     Why the example is de-fanged: neither validator skips HTML comments, so
     anything ticket-shaped in here is read as a real ticket on a brand-new,
     untouched board. Two separate traps, both hit for real while writing
     this very file: a full checkbox line parses as a live ticket, and the
     dependency field followed by an id is flagged as a dangling reference
     even without a leading dash -- tests/validate.sh scans for that field
     across the whole file, not only ticket lines, making it stricter here
     than tools/validate.py. So: no leading dash on any example, and never
     write that field name next to a concrete id anywhere in this file. -->

## DOING

## TODO

<!-- AUDIT RUN acb-mt632nqg 23.08.26: three waves (CORE / SECOND WAVE / PERFORMANCE), 23 tickets, T-084..T-106, handoff IMPLEMENTATION_AGENT. Priority order below = pick order. -->
- [ ] T-095 [AUDIT-PERF-002] P1 bounded active queue snapshot page + aggregate counts, Dock badge from backend counts | verify: cargo test -p saiwork-queue + vitest
- [ ] T-096 [AUDIT-PERF-003] P1 single monotonic queue-projection epoch replacing generation+patchGeneration | verify: vitest
- [ ] T-097 [AUDIT-PERF-004] P1 SSE parser: byte-slice field match, single owned data buffer, mem::take dispatch | verify: cargo test -p engine-opencode
- [ ] T-098 [AUDIT-PERF-005] P1 tool-state normalization from borrowed Value, cap before owning Strings | verify: cargo test -p engine-opencode
- [ ] T-099 [AUDIT-PERF-006] P1 append-aware StreamingText renderer (DOM writes proportional to new suffix) | verify: vitest
- [ ] T-109 [P2] HUNT S5: generic get_setting Tauri IPC reads arbitrary app_settings keys while set_setting is restricted to ui.layout.v1/ui.engine.v1; apply the same explicit read allowlist so typed-owner keys such as core.active_workspace and saipen.trusted_home are not exposed by default | verify: cargo test -p saiwork2 plus npm run typecheck
- [ ] T-100 [AUDIT-CORE-006] P2 preset import atomic: validate-all then single storage transaction | verify: cargo test -p saiwork-core
- [ ] T-101 [AUDIT-CORE-007] P2 bounded preset read at Tauri boundary (limit+1 sentinel) | verify: cargo test -p saiwork2
- [ ] T-102 [AUDIT-CORE-008] P2 preset picker JSON-only advertisement + copy | verify: vitest
- [ ] T-103 [AUDIT-W2-006] P2 transport RAII pending-registration guard across write+await cancellation | verify: cargo test -p engine-deepseek-harness
- [ ] T-104 [AUDIT-W2-007] P2 session name timestamps honor local wall clock (tz-aware conversion or explicit UTC rename) | verify: cargo test -p saiwork-core
- [ ] T-105 [AUDIT-PERF-007] P2 canonical adjacent-move reorder op (bounded neighbor lookup) | verify: cargo test -p saiwork-queue
- [ ] T-106 [AUDIT-PERF-008] P2 dock drag width local until pointer-up commit | verify: vitest
- [ ] T-110 [P3] HUNT S6: remove proven production dead code and redundant state — filesSliceOf, activitySliceOf, ALL_DOCK_TABS, ProcessSupervisor::release_starting have defining-file-only references; harness truncate is production-dead/test-only; accepted=true assignments immediately before loop break are never read | verify: cargo check --workspace --exclude saiwork2 plus npm run typecheck and focused rg reference proof
- [ ] T-111 [P3] HUNT S6 kitchen: CLEAN the stale ready saihunt/saiwiki OUTBOX packages only after preserving their accepted findings; both fingerprints predate source mutations E-200..E-223, so PROTOCOL section 6 forbids collect/reuse — reprepare on future demand, never delete in HUNT | verify: canonical SubSaipen freshness check classifies old packages stale and saipen validate passes after CLEAN
- [ ] T-036 Changes panel (git.rs, porcelain=v2 -z, read-only status/diff, Unicode/spaces/rename/conflict/linked-worktree/missing-git) | verify: vitest + cargo
- [ ] T-037 Queue Freebuff UX (reorder via canonical backend, close-tab-when-empty, badges derived) | verify: vitest
- [ ] T-038 RunProfile + Skills snapshot + Mission + structured To-dos | verify: vitest + cargo
- [ ] T-039 Preview (explicit managed targets, no port scan) | verify: vitest + cargo
- [ ] T-040 Reasoning effort (capability-driven, real adapter mapping) | verify: vitest + cargo
- [ ] T-041 Isolation / worktrees (ExecutionWorkspace, lifecycle, restart discovery) | verify: vitest + cargo
- [ ] T-042 Terminal (PTY via ProcessSupervisor, no frontend spawn) | verify: vitest + cargo
- [ ] T-007 favorites + provider attribution user smoke: star the models you actually use, filter to favorites, restart app — favorites survive | verify: manual (user side)
- [ ] T-005 desktop smoke: user launches Tauri app, Code view model list loads (6637 models) with no modelsError | verify: manual (user side)

## DONE
- [x] T-094 [AUDIT-PERF-001] P1 keyset-paged queue candidate scan (kill quadratic drain materialization) | verify: cargo test -p saiwork-queue | owner: freebuff | claim_time: 2026-08-24T18:44:18Z
- [x] T-092 [AUDIT-W2-004] P1 Harness start cancellation-safe ownership (abort during handshake must not orphan process/transport) | verify: cargo test -p engine-deepseek-harness | owner: freebuff | claim_time: 2026-08-24T17:44:41Z
- [x] T-108 [P1] HUNT S4: OpenCode failed-start cleanup ignores cleanup_attempt Result at lib.rs:855; a force-stop failure is hidden behind the readiness error while process termination stays unproven — surface the cleanup failure and preserve runtime/process authority until exit is proven | verify: cargo test -p engine-opencode --test hostile with injected failed-start teardown failure | owner: freebuff | claim_time: 2026-08-24T17:35:25Z
- [x] T-107 [P1] HUNT S1: restore stale-terminal regression registration — #[tokio::test] is swallowed by the preceding doc comment, so cargo -- --list omits stale_terminal_must_not_release_newer_run_gate; remove the adjacent unused handle warning | verify: cargo test -p saiwork-core -- --list plus cargo test -p saiwork-core | owner: freebuff | claim_time: 2026-08-24T17:12:26Z


## BLOCKED
- [ ] T-093 [AUDIT-W2-005] P1 supervisor final force sweep: prove exit, keep survivor authority, propagate force failures | verify: cargo test -p saiwork-process | owner: freebuff | claim_time: 2026-08-24T17:58:56Z | blocker: global SHIP gate blocked: canonical core validator rejects legacy main/sub SAIPEN ledger and no-git freshness scan exceeds 60s; requires canonical migration/repair of .saipen history before any ticket can pass SHIP

- [ ] T-071 (SAIT-001) saitranslate blocked: SAIWORK2 has no real translation surface — no i18n framework in apps/crates/packages source (only transitive icu_locale_core in Cargo.lock), docs English-only with zero per-language siblings; close the i18n gap + author per-language doc siblings, then re-run ee to reach ready, then eee | blocker: no i18n framework in workspace
