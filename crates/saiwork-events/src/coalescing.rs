//! Shell-only canonical-event forwarding with MessageDelta coalescing
//! (TASK 24 perf).
//!
//! The Rust→WebView transport used to emit one IPC fact per `message.delta`,
//! so N tokens caused N emissions even though the UI consumes frame-sized
//! text. `forward` coalesces consecutive deltas for the same
//! `(session_id, run_id)` in arrival order and emits them as one envelope,
//! flushed at a <=16 ms window and synchronously before any state/terminal
//! event. The canonical EventBus is untouched; this is purely a transport
//! optimization and is never a durable authority.

use std::collections::HashMap;
use std::time::Duration;

use tracing::{error, info};

use crate::bus::Subscription;
use crate::{Envelope, Event, RunId, SessionId, SubscribeError};

/// How long a quiet pending delta may sit before being flushed anyway.
const DELTA_FLUSH_MS: u64 = 16;

/// A pending coalesced delta, keyed by the typed IDs directly (Arc-backed,
/// cheap clone — no per-delta `String` allocation; TASK 24 perf).
type DeltaKey = (SessionId, RunId);

#[derive(Clone)]
struct PendingDelta {
    text: String,
    seq: u64,
    ts: u64,
}

/// Drain one flush: emit each pending (session, run) text in first-arrival
/// order. Returns `Ok(())` when every emission succeeded and `Err(())` on
/// the FIRST fatal emit failure — the caller must terminate the forwarder
/// exactly like the non-delta path (a dead WebView must not keep the task
/// alive generating repeated errors; TASK 24 §9).
fn flush<F>(
    emit: &mut F,
    pending: &mut HashMap<DeltaKey, PendingDelta>,
    order: &mut Vec<DeltaKey>,
) -> Result<(), ()>
where
    F: FnMut(Envelope) -> Result<(), ()>,
{
    for (session_id, run_id) in order.drain(..) {
        if let Some(p) = pending.remove(&(session_id.clone(), run_id.clone())) {
            let envelope = Envelope {
                seq: p.seq,
                ts: p.ts,
                event: Event::MessageDelta {
                    session_id,
                    run_id,
                    delta: p.text,
                },
            };
            emit(envelope)?;
        }
    }
    Ok(())
}

/// Forward canonical events from `subscription` to `emit`, coalescing
/// consecutive `message.delta` facts per `(session_id, run_id)` into single
/// emissions (<=16 ms window, and synchronously before any non-delta event so
/// pending text always precedes the state/terminal fact that follows it).
///
/// - `emit` returns `Err(())` on a fatal forward failure (stops the loop).
/// - `on_lagged(skipped)` is called when the subscriber fell behind; the
///   caller decides how to reconcile (e.g. ask the frontend to re-snapshot).
/// - Only a closed bus ends the loop (the bounded bus must never freeze the
///   stream on lag; law 13).
pub async fn forward<F, L>(mut subscription: Subscription, mut emit: F, mut on_lagged: L)
where
    F: FnMut(Envelope) -> Result<(), ()>,
    L: FnMut(u64),
{
    let mut pending: HashMap<DeltaKey, PendingDelta> = HashMap::new();
    let mut order: Vec<DeltaKey> = Vec::new();
    let mut flush_deadline: Option<tokio::time::Instant> = None;

    // Returns false when the forwarder must terminate (fatal emit failure).
    macro_rules! flush_or_stop {
        () => {{
            let outcome = flush(&mut emit, &mut pending, &mut order);
            flush_deadline = None;
            if outcome.is_err() {
                error!("event forward failed while flushing deltas; stopping forwarder");
                pending.clear();
                order.clear();
                break;
            }
        }};
    }

    loop {
        // If deltas are pending, race the next event against the flush window
        // so a quiet stream still drains promptly.
        let next = if let Some(deadline) = flush_deadline {
            let sleep = tokio::time::sleep_until(deadline);
            tokio::pin!(sleep);
            tokio::select! {
                _ = &mut sleep => {
                    flush_or_stop!();
                    continue;
                }
                env = subscription.recv() => Some(env),
            }
        } else {
            Some(subscription.recv().await)
        };
        match next {
            Some(Ok(envelope)) => {
                if let Event::MessageDelta {
                    session_id,
                    run_id,
                    delta,
                } = &envelope.event
                {
                    // Keyed by the typed IDs directly (no String allocation
                    // per raw delta; TASK 24 perf).
                    let key = (session_id.clone(), run_id.clone());
                    match pending.get_mut(&key) {
                        Some(p) => p.text.push_str(delta),
                        None => {
                            pending.insert(
                                key.clone(),
                                PendingDelta {
                                    text: delta.clone(),
                                    seq: envelope.seq,
                                    ts: envelope.ts,
                                },
                            );
                            order.push(key);
                        }
                    }
                    if flush_deadline.is_none() {
                        flush_deadline = Some(
                            tokio::time::Instant::now() + Duration::from_millis(DELTA_FLUSH_MS),
                        );
                    }
                    continue;
                }
                // Any non-delta event flushes pending text FIRST so the
                // WebView sees it before the state/terminal fact that follows
                // it (ordering preserved).
                if !pending.is_empty() {
                    flush_or_stop!();
                }
                if emit(envelope).is_err() {
                    error!("event forward failed; stopping forwarder");
                    pending.clear();
                    order.clear();
                    break;
                }
            }
            // The bus is bounded (law 13): a slow forwarder that falls behind
            // observes `Lagged` and must keep flowing (the frontend reconciles
            // from a snapshot) — it must NOT kill the stream. Only a closed
            // bus ends.
            Some(Err(SubscribeError::Lagged(skipped))) => {
                if !pending.is_empty() {
                    flush_or_stop!();
                }
                on_lagged(skipped);
                continue;
            }
            Some(Err(SubscribeError::Closed)) | None => {
                info!("event forwarder ended");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventBus, RunId, SessionId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn ten_k_deltas_coalesce_and_flush_before_terminal() {
        let bus = EventBus::new();
        let subscription = bus.subscribe();

        let delta_emissions = Arc::new(AtomicUsize::new(0));
        let completed_seen = Arc::new(AtomicUsize::new(0));
        let emitted: Arc<Mutex<Vec<(String, String, String)>>> = Arc::new(Mutex::new(Vec::new()));

        let counts = delta_emissions.clone();
        let comp = completed_seen.clone();
        let emitted2 = emitted.clone();
        let forwarder = tokio::spawn(forward(
            subscription,
            move |env: Envelope| {
                match &env.event {
                    Event::MessageDelta {
                        session_id,
                        run_id,
                        delta,
                    } => {
                        counts.fetch_add(1, Ordering::SeqCst);
                        emitted2.lock().unwrap().push((
                            session_id.to_string(),
                            run_id.to_string(),
                            delta.clone(),
                        ));
                    }
                    Event::MessageCompleted { .. } => {
                        comp.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => {}
                }
                Ok(())
            },
            |_skipped| {},
        ));

        let s1 = SessionId::new("s1");
        let s2 = SessionId::new("s2");
        let r1 = RunId::new("r1");
        let r2 = RunId::new("r2");
        let mut expected1 = String::new();
        let mut expected2 = String::new();

        for i in 0..10_000 {
            let (session, run, expected, delta) = if i % 2 == 0 {
                (&s1, &r1, &mut expected1, format!("t{i}-"))
            } else {
                (&s2, &r2, &mut expected2, format!("u{i}-"))
            };
            expected.push_str(&delta);
            bus.publish(Event::MessageDelta {
                session_id: session.clone(),
                run_id: run.clone(),
                delta,
            });
            // Yield so the forwarder drains: the bus is bounded (law 13) and
            // drops the OLDEST events under lag — the test must keep the
            // forwarder caught up, or early deltas would be lost BEFORE the
            // coalescer ever sees them (a bus property, not a coalescing bug).
            if i % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
        bus.publish(Event::MessageCompleted {
            session_id: s1.clone(),
            run_id: r1.clone(),
        });
        bus.publish(Event::MessageCompleted {
            session_id: s2.clone(),
            run_id: r2.clone(),
        });

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while completed_seen.load(Ordering::SeqCst) < 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            completed_seen.load(Ordering::SeqCst),
            2,
            "both terminals must reach the consumer"
        );

        let delta_count = delta_emissions.load(Ordering::SeqCst);
        assert!(
            delta_count <= 16,
            "10k deltas must coalesce to O(render frames) emissions, got {delta_count}"
        );
        let text1 = emitted
            .lock()
            .unwrap()
            .iter()
            .filter(|(s, r, _)| s == "s1" && r == "r1")
            .map(|(_, _, d)| d.clone())
            .collect::<String>();
        let text2 = emitted
            .lock()
            .unwrap()
            .iter()
            .filter(|(s, r, _)| s == "s2" && r == "r2")
            .map(|(_, _, d)| d.clone())
            .collect::<String>();
        assert_eq!(text1, expected1, "run 1 final text must be byte-identical");
        assert_eq!(text2, expected2, "run 2 final text must be byte-identical");

        forwarder.abort();
        let _ = forwarder.await;
    }

    #[tokio::test]
    async fn first_fatal_flush_failure_terminates_forwarder_no_zombie() {
        // The WebView/listener is gone (emit fails on the first delta flush):
        // the forwarder must terminate promptly and attempt NO further
        // emits, even though the bus keeps flowing (no zombie consumer, no
        // repeated errors; TASK 24 §9).
        let bus = EventBus::new();
        let subscription = bus.subscribe();
        let emits = Arc::new(AtomicUsize::new(0));
        let emits2 = emits.clone();
        let forwarder = tokio::spawn(forward(
            subscription,
            move |env: Envelope| {
                if matches!(&env.event, Event::MessageDelta { .. }) {
                    emits2.fetch_add(1, Ordering::SeqCst);
                    Err(()) // fatal: WebView gone
                } else {
                    Ok(())
                }
            },
            |_skipped| {},
        ));

        // A burst of deltas AFTER the first flush attempt: a zombie
        // forwarder would keep consuming/batching/emitting failures.
        for i in 0..10_000 {
            bus.publish(Event::MessageDelta {
                session_id: SessionId::new("s"),
                run_id: RunId::new("r"),
                delta: format!("t{i}-"),
            });
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !forwarder.is_finished() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "forwarder must terminate promptly after a fatal flush failure"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = forwarder.await;
        assert_eq!(
            emits.load(Ordering::SeqCst),
            1,
            "exactly one (failing) emit attempt before termination"
        );
    }

    #[tokio::test]
    async fn quiet_stream_flushes_within_the_window_without_a_terminal() {
        let bus = EventBus::new();
        let subscription = bus.subscribe();
        let emitted: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let emitted2 = emitted.clone();
        let forwarder = tokio::spawn(forward(
            subscription,
            move |env: Envelope| {
                if let Event::MessageDelta { delta, .. } = &env.event {
                    emitted2.lock().unwrap().push(delta.clone());
                }
                Ok(())
            },
            |_skipped| {},
        ));

        bus.publish(Event::MessageDelta {
            session_id: SessionId::new("s"),
            run_id: RunId::new("r"),
            delta: "quiet".into(),
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let flushed = emitted.lock().unwrap().clone();
        assert_eq!(
            flushed,
            vec!["quiet".to_string()],
            "window flush must deliver the delta"
        );

        forwarder.abort();
        let _ = forwarder.await;
    }
}
