//! Repo-level durable queue tests (TASK 13 §190–§205): atomicity, CAS,
//! ordering persistence, pause persistence, and crash-window recovery.

use rusqlite::named_params;
use saiwork_queue::model::{EnqueueRequest, QueueState, SessionMode, DISPATCH_CANDIDATE_PAGE_SIZE};
use saiwork_queue::QueueRepo;
use saiwork_storage::{Db, StorageError};

fn req(payload: &str) -> EnqueueRequest {
    EnqueueRequest {
        workspace_id: "w1".into(),
        engine_id: "fake".into(),
        session_id: None,
        session_mode: SessionMode::New,
        model: None,
        payload: payload.into(),
    }
}

/// Seed the workspace row with the EXACT id the test enqueue requests use
/// (`w1`): AUDIT-W2-003 makes enqueue verify workspace existence in-tx.
fn seed_w1(db: &Db) {
    db.with_conn(|conn| {
        conn.execute(
            "INSERT OR IGNORE INTO workspaces (id, path, name, last_opened_at, created_at, updated_at)
             VALUES ('w1', 'file:///w1', 'w1', 0, 0, 0)",
            [],
        )
        .map(|_| ())
        .map_err(saiwork_storage::StorageError::Query)
    })
    .unwrap();
}

fn seeded_in_memory() -> Db {
    let db = Db::open_in_memory().unwrap();
    seed_w1(&db);
    db
}

fn seeded_at(path: &std::path::Path) -> Db {
    let db = Db::open(path).unwrap();
    seed_w1(&db);
    db
}

fn repo(db: &Db) -> QueueRepo {
    QueueRepo::new(db.clone())
}

#[test]
fn enqueue_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saiwork2.db");
    let id;
    {
        let db = seeded_at(&path);
        let item = repo(&db).enqueue(&req("hello")).unwrap();
        id = item.id.clone();
    }
    {
        let db = seeded_at(&path);
        let item = repo(&db).get(&id).unwrap().expect("durable after reopen");
        assert_eq!(item.payload, "hello");
        assert_eq!(item.state, QueueState::Queued);
        assert_eq!(item.revision, 1);
        assert_eq!(item.attempt_count, 0);
    }
}

#[test]
fn claim_exactly_one_wins_concurrent() {
    // Two connections to the same file (as two processes would be): the
    // atomic claim must give exactly one winner.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saiwork2.db");
    let item = repo(&seeded_at(&path))
        .enqueue(&req("one"))
        .unwrap();
    let mut handles = Vec::new();
    for _ in 0..16 {
        let path = path.clone();
        let id = item.id.clone();
        handles.push(std::thread::spawn(move || {
            let db = seeded_at(&path);
            QueueRepo::new(db).claim(&id).unwrap()
        }));
    }
    let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(
        results.iter().filter(|ok| **ok).count(),
        1,
        "exactly one concurrent claim wins"
    );
    let db = seeded_at(&path);
    let after = repo(&db).get(&item.id).unwrap().unwrap();
    assert_eq!(after.state, QueueState::Leased);
}

#[test]
fn edit_uses_revision_cas() {
    let db = seeded_in_memory();
    let item = repo(&db).enqueue(&req("v1")).unwrap();
    let edited = repo(&db).edit(&item.id, 1, "v2", None).unwrap();
    assert_eq!(edited.payload, "v2");
    assert_eq!(edited.revision, 2);
    // Stale revision → Conflict.
    let err = repo(&db).edit(&item.id, 1, "stale", None).unwrap_err();
    assert!(
        matches!(err, saiwork_queue::QueueError::Conflict { .. }),
        "stale edit must conflict, got {err:?}"
    );
}

#[test]
fn edit_after_claim_is_rejected() {
    let db = seeded_in_memory();
    let item = repo(&db).enqueue(&req("v1")).unwrap();
    assert!(repo(&db).claim(&item.id).unwrap());
    let err = repo(&db).edit(&item.id, 1, "sneaky", None).unwrap_err();
    assert!(
        matches!(err, saiwork_queue::QueueError::InvalidState { .. }),
        "payload is immutable after claim, got {err:?}"
    );
}

#[test]
fn reorder_is_transactional_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saiwork2.db");
    let ids: Vec<String>;
    {
        let db = seeded_at(&path);
        let r = repo(&db);
        let a = r.enqueue(&req("A")).unwrap();
        let b = r.enqueue(&req("B")).unwrap();
        let c = r.enqueue(&req("C")).unwrap();
        ids = vec![a.id.clone(), b.id.clone(), c.id.clone()];
        // Move C before A.
        r.reorder(&c.id, 1, 0).unwrap();
        let order = r.list_queued().unwrap();
        let names: Vec<&str> = order.iter().map(|i| i.payload.as_str()).collect();
        assert_eq!(names, vec!["C", "A", "B"]);
    }
    {
        let db = seeded_at(&path);
        let order = repo(&db).list_queued().unwrap();
        let names: Vec<&str> = order.iter().map(|i| i.payload.as_str()).collect();
        assert_eq!(names, vec!["C", "A", "B"], "order survives restart");
        assert_eq!(order[0].id, ids[2]);
    }
}

#[test]
fn candidate_keyset_pages_are_bounded_complete_and_ordered() {
    let db = seeded_in_memory();
    let r = repo(&db);
    for index in 0..(DISPATCH_CANDIDATE_PAGE_SIZE * 2 + 7) {
        r.enqueue(&req(&format!("candidate-{index:03}"))).unwrap();
    }
    let expected: Vec<String> = r
        .list_queued()
        .unwrap()
        .into_iter()
        .map(|item| item.id)
        .collect();

    let mut after = None;
    let mut actual = Vec::new();
    loop {
        let page = r.list_candidate_page(after.as_ref()).unwrap();
        assert!(
            page.len() <= DISPATCH_CANDIDATE_PAGE_SIZE,
            "candidate materialization exceeded its fixed page bound"
        );
        if page.is_empty() {
            break;
        }
        after = page.last().cloned();
        actual.extend(page.into_iter().map(|candidate| candidate.id));
    }

    assert_eq!(
        actual, expected,
        "keyset walk must neither skip nor duplicate"
    );
}

#[test]
fn candidate_keyset_page_uses_dispatch_index_without_temp_sort() {
    let db = seeded_in_memory();
    let plan = db
        .with_conn(|conn| -> Result<String, StorageError> {
            let mut stmt = conn
                .prepare(
                    "EXPLAIN QUERY PLAN \
                     SELECT id, revision, engine_id, workspace_id, session_mode, session_id, model, \
                            order_key, created_at \
                     FROM queue_items INDEXED BY idx_queue_items_dispatch_keyset \
                     WHERE state = 'queued' AND (order_key, created_at, id) > (0, 0, '') \
                     ORDER BY order_key, created_at, id LIMIT 128",
                )
                .map_err(StorageError::from)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(3))
                .map_err(StorageError::from)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(StorageError::from)?;
            Ok(rows.join("\n"))
        })
        .unwrap();

    assert!(
        plan.contains("idx_queue_items_dispatch_keyset"),
        "candidate page must seek through the dispatch index; plan was:\n{plan}"
    );
    assert!(
        !plan.to_uppercase().contains("USE TEMP B-TREE"),
        "candidate ordering must not allocate a temp sort; plan was:\n{plan}"
    );
}

#[test]
fn reorder_rejects_stale_revision() {
    let db = seeded_in_memory();
    let r = repo(&db);
    let a = r.enqueue(&req("A")).unwrap();
    let _b = r.enqueue(&req("B")).unwrap();
    let err = r.reorder(&a.id, 99, 0).unwrap_err();
    assert!(matches!(err, saiwork_queue::QueueError::Conflict { .. }));
    r.reorder(&a.id, 1, 0).unwrap(); // current revision works
}

#[test]
fn reorder_rejects_leased_and_done_items() {
    let db = seeded_in_memory();
    let r = repo(&db);
    let a = r.enqueue(&req("A")).unwrap();
    let _b = r.enqueue(&req("B")).unwrap();
    assert!(r.claim(&a.id).unwrap());
    let err = r.reorder(&a.id, 1, 0).unwrap_err();
    assert!(matches!(
        err,
        saiwork_queue::QueueError::InvalidState { .. }
    ));
}

#[test]
fn cancel_queued_uses_revision() {
    let db = seeded_in_memory();
    let r = repo(&db);
    let a = r.enqueue(&req("A")).unwrap();
    assert!(r.cancel_queued(&a.id, 1).unwrap());
    assert_eq!(r.get(&a.id).unwrap().unwrap().state, QueueState::Cancelled);
    // Cancelling a cancelled item with a stale revision → conflict.
    let err = r.cancel_queued(&a.id, 1).unwrap_err();
    assert!(matches!(err, saiwork_queue::QueueError::Conflict { .. }));
}

#[test]
fn pause_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saiwork2.db");
    {
        let db = seeded_at(&path);
        repo(&db).set_paused(true).unwrap();
    }
    {
        let db = seeded_at(&path);
        assert!(repo(&db).is_paused().unwrap());
        repo(&db).set_paused(false).unwrap();
    }
    {
        let db = seeded_at(&path);
        assert!(!repo(&db).is_paused().unwrap());
    }
}

#[test]
fn recovery_prepare_lease_returns_to_queued() {
    let db = seeded_in_memory();
    let r = repo(&db);
    let a = r.enqueue(&req("A")).unwrap();
    assert!(r.claim(&a.id).unwrap());
    let report = r.recover().unwrap();
    assert_eq!(report.recovered_to_queued, 1);
    assert_eq!(r.get(&a.id).unwrap().unwrap().state, QueueState::Queued);
}

#[test]
fn recovery_sending_lease_is_unknown_not_resent() {
    let db = seeded_in_memory();
    let r = repo(&db);
    let a = r.enqueue(&req("A")).unwrap();
    assert!(r.claim(&a.id).unwrap());
    assert!(r.begin_send(&a.id, "sess-1").unwrap());
    let report = r.recover().unwrap();
    assert_eq!(report.marked_unknown, 1);
    let item = r.get(&a.id).unwrap().unwrap();
    assert_eq!(item.state, QueueState::Unknown);
    assert_eq!(item.last_error_code.as_deref(), Some("dispatch_unknown"));
}

#[test]
fn recovery_cancel_intent_cancels() {
    let db = seeded_in_memory();
    let r = repo(&db);
    let a = r.enqueue(&req("A")).unwrap();
    assert!(r.claim(&a.id).unwrap());
    assert!(r.request_cancel_leased(&a.id).unwrap());
    let report = r.recover().unwrap();
    assert_eq!(report.cancelled_from_intent, 1);
    assert_eq!(r.get(&a.id).unwrap().unwrap().state, QueueState::Cancelled);
}

#[test]
fn recovery_marks_dispatched_unknown_after_restart() {
    // TASK 23 §28–§29: a DISPATCHED item at restart has no reconcilable
    // engine authority in this process → UNKNOWN (never resend, never shown
    // as a live run). The old run correlation stays visible for the user.
    let db = seeded_in_memory();
    let r = repo(&db);
    let a = r.enqueue(&req("A")).unwrap();
    assert!(r.claim(&a.id).unwrap());
    assert!(r.begin_send(&a.id, "sess-1").unwrap());
    assert!(r.mark_dispatched(&a.id, "run-1").unwrap());
    let report = r.recover().unwrap();
    assert_eq!(report.marked_unknown_dispatched, 1);
    let item = r.get(&a.id).unwrap().unwrap();
    assert_eq!(item.state, QueueState::Unknown);
    assert_eq!(item.last_error_code.as_deref(), Some("dispatch_unknown"));
    assert_eq!(
        item.run_id.as_deref(),
        Some("run-1"),
        "old correlation retained"
    );
    assert_eq!(item.attempt_count, 1);
}

#[test]
fn terminal_guard_rejects_stale_run() {
    let db = seeded_in_memory();
    let r = repo(&db);
    let a = r.enqueue(&req("A")).unwrap();
    assert!(r.claim(&a.id).unwrap());
    assert!(r.begin_send(&a.id, "sess-1").unwrap());
    assert!(r.mark_dispatched(&a.id, "run-2").unwrap());
    // A late terminal for an old attempt must not mutate the row.
    assert!(!r
        .mark_terminal(&a.id, "run-1", QueueState::Done, None, None)
        .unwrap());
    assert_eq!(r.get(&a.id).unwrap().unwrap().state, QueueState::Dispatched);
    // The current run's terminal wins.
    assert!(r
        .mark_terminal(&a.id, "run-2", QueueState::Done, None, None)
        .unwrap());
    assert_eq!(r.get(&a.id).unwrap().unwrap().state, QueueState::Done);
}

#[test]
fn retry_requeues_failed() {
    let db = seeded_in_memory();
    let r = repo(&db);
    let a = r.enqueue(&req("A")).unwrap();
    assert!(r.claim(&a.id).unwrap());
    assert!(r.begin_send(&a.id, "sess-1").unwrap());
    assert!(r
        .mark_failed_leased(&a.id, "rate_limited", "too many")
        .unwrap());
    assert_eq!(r.get(&a.id).unwrap().unwrap().state, QueueState::Failed);
    assert!(r.retry(&a.id, 1).unwrap());
    let item = r.get(&a.id).unwrap().unwrap();
    assert_eq!(item.state, QueueState::Queued);
    assert_eq!(item.revision, 2);
}

#[test]
fn payload_bounds_are_enforced() {
    let db = seeded_in_memory();
    let r = repo(&db);
    let err = r.enqueue(&req("   ")).unwrap_err();
    assert!(matches!(err, saiwork_queue::QueueError::EmptyPayload));
    let big = "x".repeat(65 * 1024);
    let err = r.enqueue(&req(&big)).unwrap_err();
    assert!(matches!(
        err,
        saiwork_queue::QueueError::PayloadTooLarge { .. }
    ));
}

#[test]
fn failed_detail_is_bounded() {
    let db = seeded_in_memory();
    let r = repo(&db);
    let a = r.enqueue(&req("A")).unwrap();
    assert!(r.claim(&a.id).unwrap());
    let huge = "boom".repeat(5000);
    assert!(r.mark_failed_leased(&a.id, "provider", &huge).unwrap());
    let item = r.get(&a.id).unwrap().unwrap();
    assert!(item.last_error.as_ref().unwrap().len() <= 500);
}

#[test]
fn snapshot_payload_is_bounded_preview_full_payload_via_get() {
    // TASK 24 perf + §13: a queue of thousands of near-max prompts must not
    // serialize/mount tens of MiB through the UI snapshot. The snapshot
    // carries at most PAYLOAD_PREVIEW_BYTES per item (SQL-projected in the
    // database — the full body never enters Rust memory) and flags it with
    // `payload_truncated`; the durable row is untouched and `get` returns
    // the exact full payload for editing.
    let db = seeded_in_memory();
    let r = repo(&db);
    let max = saiwork_queue::model::PAYLOAD_MAX_BYTES;
    let mut ids = Vec::new();
    for i in 0..200 {
        let payload = format!("{i}:{}", "x".repeat(max - 4));
        let item = r.enqueue(&req(&payload)).unwrap();
        ids.push(item.id);
    }
    let snap = r.list_snapshot(10).unwrap();
    // Every active row present (ordering/counts unchanged)…
    assert_eq!(snap.len(), 200);
    // …but payloads are bounded previews, never the 64 KiB bodies, and the
    // truncation flag tells the UI to render the ellipsis.
    for item in &snap {
        assert!(
            item.payload.len() <= saiwork_queue::model::PAYLOAD_PREVIEW_BYTES,
            "snapshot payload must stay bounded, got {}",
            item.payload.len()
        );
        assert!(item.payload_truncated, "projection flag must be set");
    }
    // The exact full payload is retrievable per item.
    for (i, id) in ids.iter().enumerate() {
        let full = r.get(id).unwrap().expect("durable row intact");
        assert!(full.payload.ends_with('x'));
        assert_eq!(full.payload.len(), format!("{i}:").len() + (max - 4));
        // A full decode never carries the projection flag.
        assert!(!full.payload_truncated);
    }
}

#[test]
fn snapshot_preview_trims_a_split_multibyte_character() {
    // 3-byte "界": 171 chars = 513 bytes > 512-byte preview → the SQL
    // byte-cut lands MID-CHARACTER (170 chars + 2 bytes). The snapshot
    // decode must trim to a char boundary — never panic, never emit
    // invalid UTF-8, never hand the UI a broken char.
    let db = seeded_in_memory();
    let r = repo(&db);
    let payload = "界".repeat(171);
    let item = r.enqueue(&req(&payload)).unwrap();
    let snap = r.list_snapshot(10).unwrap();
    assert_eq!(snap[0].payload, "界".repeat(170));
    assert!(snap[0].payload_truncated);
    assert!(snap[0].payload.len() <= saiwork_queue::model::PAYLOAD_PREVIEW_BYTES);
    // get() still returns the exact full body.
    let full = r.get(&item.id).unwrap().unwrap();
    assert_eq!(full.payload, payload);
    assert!(!full.payload_truncated);
}

#[test]
fn snapshot_small_payloads_are_byte_identical() {
    let db = seeded_in_memory();
    let r = repo(&db);
    r.enqueue(&req("hello world")).unwrap();
    let snap = r.list_snapshot(10).unwrap();
    assert_eq!(snap[0].payload, "hello world");
    assert!(!snap[0].payload_truncated);
}

#[test]
fn snapshot_preview_boundary_exact_length_is_not_truncated() {
    // A payload of EXACTLY PAYLOAD_PREVIEW_BYTES is byte-identical in the
    // snapshot and NOT flagged — the flag means "a longer body exists".
    let db = seeded_in_memory();
    let r = repo(&db);
    let exact = "a".repeat(saiwork_queue::model::PAYLOAD_PREVIEW_BYTES);
    r.enqueue(&req(&exact)).unwrap();
    let snap = r.list_snapshot(10).unwrap();
    assert_eq!(snap[0].payload, exact);
    assert!(!snap[0].payload_truncated);
}

#[test]
fn list_snapshot_succeeds_on_empty_active_and_mixed_terminal_db() {
    // PERF-001: the history branch previously used a malformed `INDEXED BY`
    // placement (after LIMIT) that failed to prepare. It must now succeed on
    // empty, active-only, and mixed-terminal DBs and return the newest-50
    // terminal rows ordered by updated_at DESC.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saiwork2.db");
    let db = seeded_at(&path);
    let r = repo(&db);

    // Empty DB.
    assert!(r.list_snapshot(50).unwrap().is_empty());

    // Active-only DB: one queued item, history branch untouched.
    let active = r.enqueue(&req("active")).unwrap();
    let snap = r.list_snapshot(50).unwrap();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].id, active.id);

    // Mixed terminal DB: drive many items to DONE so the history branch is
    // exercised with real rows.
    for i in 0..60u32 {
        let item = r.enqueue(&req(&format!("terminal-{i}"))).unwrap();
        assert!(r.claim(&item.id).unwrap());
        assert!(r.begin_send(&item.id, &format!("s{i}")).unwrap());
        assert!(r.mark_dispatched(&item.id, &format!("r{i}")).unwrap());
        assert!(
            r.mark_terminal(&item.id, &format!("r{i}"), QueueState::Done, None, None)
                .unwrap()
        );
    }
    let snap = r.list_snapshot(50).unwrap();
    // 1 active + bounded 50 terminal (history_limit).
    assert_eq!(snap.len(), 51, "1 active + 50 terminal history");
    // Terminal portion ordered updated_at DESC (newest first).
    let terminals = &snap[1..];
    for w in terminals.windows(2) {
        assert!(
            w[0].updated_at >= w[1].updated_at,
            "history must be ordered updated_at DESC"
        );
    }
}

#[test]
fn list_snapshot_history_uses_partial_index_no_temp_btree() {
    // PERF-001 regression: the history query must be served by the partial
    // index `idx_queue_items_terminal_updated` with no temp B-tree sort. The
    // `INDEXED BY` hint forces the index; EXPLAIN must confirm it is used and
    // that ORDER BY does not require a sort.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saiwork2.db");
    let db = seeded_at(&path);
    let r = repo(&db);
    for i in 0..200u32 {
        let item = r.enqueue(&req(&format!("t{i}"))).unwrap();
        assert!(r.claim(&item.id).unwrap());
        assert!(r.begin_send(&item.id, &format!("s{i}")).unwrap());
        assert!(r.mark_dispatched(&item.id, &format!("r{i}")).unwrap());
        assert!(
            r.mark_terminal(&item.id, &format!("r{i}"), QueueState::Done, None, None)
                .unwrap()
        );
    }

    let plan: String = db
        .with_conn(|conn| -> Result<String, StorageError> {
            let mut stmt = conn
                .prepare(
                    "EXPLAIN QUERY PLAN \
                     SELECT id FROM queue_items INDEXED BY idx_queue_items_terminal_updated \
                     WHERE state IN ('done','failed','cancelled') \
                     ORDER BY updated_at DESC LIMIT :L",
                )
                .map_err(StorageError::from)?;
            let mut rows = stmt
                .query(named_params! { ":L": 50i64 })
                .map_err(StorageError::from)?;
            let mut out = String::new();
            while let Some(row) = rows.next().map_err(StorageError::from)? {
                out.push_str(&row.get::<_, String>(3).map_err(StorageError::from)?);
                out.push('\n');
            }
            Ok(out)
        })
        .map_err(|e| format!("{e:?}"))
        .unwrap();
    assert!(
        plan.contains("idx_queue_items_terminal_updated"),
        "planner must use the partial index; plan was:\n{plan}"
    );
    assert!(
        !plan.to_uppercase().contains("USE TEMP B-TREE"),
        "history must avoid a temp B-tree sort; plan was:\n{plan}"
    );
}

#[test]
fn incremental_get_item_matches_snapshot_membership() {
    // PERF-004: the per-item read (`get`) returns the authoritative row a full
    // snapshot would carry — an incremental single-item patch can never drift
    // from the snapshot truth.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("saiwork2.db");
    let db = seeded_at(&path);
    let r = repo(&db);
    let active = r.enqueue(&req("active")).unwrap();
    let term = r.enqueue(&req("term")).unwrap();
    assert!(r.claim(&term.id).unwrap());
    assert!(r.begin_send(&term.id, "run-1").unwrap());
    assert!(r.mark_dispatched(&term.id, "run-1").unwrap());
    assert!(
        r.mark_terminal(&term.id, "run-1", QueueState::Done, None, None)
            .unwrap()
    );

    let snap = r.list_snapshot(50).unwrap();
    let from_snap = snap
        .iter()
        .find(|i| i.id == term.id)
        .expect("terminal present in snapshot");
    let from_get = r.get(&term.id).unwrap().expect("terminal reachable by id");
    assert_eq!(from_snap.id, from_get.id);
    assert_eq!(from_snap.state, from_get.state);
    assert_eq!(from_snap.revision, from_get.revision);
    assert_eq!(from_snap.updated_at, from_get.updated_at);
    // Active item is authoritative by id too.
    let active_get = r.get(&active.id).unwrap().expect("active reachable by id");
    assert_eq!(active_get.state, QueueState::Queued);
}

// AUDIT-W2-003: enqueue verifies the referenced workspace INSIDE the insert
// transaction — an unknown workspace never persists a row.
#[test]
fn enqueue_unknown_workspace_persists_nothing() {
    let db = seeded_in_memory();
    let mut bad = req("orphan");
    bad.workspace_id = "ws-missing".into();
    let err = repo(&db).enqueue(&bad).unwrap_err();
    assert!(
        matches!(err, saiwork_queue::QueueError::InvalidState { .. }),
        "got {err:?}"
    );
    // No queue row was written for the missing workspace.
    let n: i64 = db
        .with_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM queue_items WHERE workspace_id = 'ws-missing'",
                [],
                |r| r.get(0),
            )
            .map_err(StorageError::Query)
        })
        .unwrap();
    assert_eq!(n, 0, "no durable reference to a deleted/missing identity");
}

// AUDIT-W2-003: Forget rejects nonterminal queue references from INSIDE the
// deletion transaction — an enqueue that committed before the delete makes
// the Forget fail with the workspace fully intact.
#[test]
fn forget_rejects_nonterminal_queue_reference_atomically() {
    let db = seeded_at(
        tempfile::tempdir().unwrap().path().join("w.db").as_path(),
    );
    repo(&db).enqueue(&req("active work")).unwrap();
    let err = db.forget_workspace_with_sessions("w1").unwrap_err();
    assert!(
        matches!(err, StorageError::WorkspaceReferenced { .. }),
        "got {err:?}"
    );
    // Workspace + queue row both survive (transactional rollback).
    let ws: i64 = db
        .with_conn(|conn| {
            conn.query_row("SELECT COUNT(*) FROM workspaces WHERE id = 'w1'", [], |r| r.get(0))
                .map_err(StorageError::Query)
        })
        .unwrap();
    assert_eq!(ws, 1, "workspace must survive a rejected forget");
    // Terminal history rows do NOT block forget.
    db.with_conn(|conn| {
        conn.execute(
            "UPDATE queue_items SET state = 'done' WHERE workspace_id = 'w1'",
            [],
        )
        .map(|_| ())
        .map_err(StorageError::Query)
    })
    .unwrap();
    db.forget_workspace_with_sessions("w1").unwrap();
}
