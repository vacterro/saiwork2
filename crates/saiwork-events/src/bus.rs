//! Bounded application EventBus (EVENTS.md).
//!
//! - One authority: the bus is the only normalized event path (law 8).
//! - Bounded: fixed-capacity `tokio::sync::broadcast` (law 13). Slow
//!   consumers observe `RecvError::Lagged` and must reconcile from
//!   authoritative state instead of buffering.
//! - Subscription cleanup: dropping a `Subscription` removes it from the bus
//!   (law 19). `cancel()` is explicit for owned long-lived subscribers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::broadcast::{
    self,
    error::{RecvError, TryRecvError},
    Receiver, Sender,
};

use crate::{Envelope, Event, Seq, Timestamp};

/// Default capacity. Deliberately small: subscribers that cannot keep up
/// must reconcile, not grow memory. Revisit only with measured need.
pub const DEFAULT_CAPACITY: usize = 1024;

/// A canceled / lagged / closed subscription result.
#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error("event bus closed")]
    Closed,
    #[error("subscriber lagged behind the bounded bus; reconcile state (missed {0} events)")]
    Lagged(u64),
}

#[derive(Clone)]
pub struct EventBus {
    tx: Sender<Envelope>,
    /// State-class channel: receives ONLY `EventClass::State` events (the
    /// same envelopes as `tx` for those events). State-only consumers
    /// (running tracker, queue coordinator) subscribe here so a high-rate
    /// `message.delta`/`tool.output` flood can neither wake them nor lag
    /// their bounded buffer (PERFORMANCE.md). Ordering among State events is
    /// preserved; cross-class ordering is only guaranteed on the full `tx`.
    state_tx: Sender<Envelope>,
    seq: Arc<AtomicU64>,
}

impl EventBus {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity.max(16));
        let (state_tx, _rx) = broadcast::channel(capacity.max(16));
        Self {
            tx,
            state_tx,
            seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Publish an event. Never blocks and never grows memory: if every
    /// receiver is slow the event is dropped for lagging receivers, which is
    /// the bounded-bus contract. State-class events are also forwarded to the
    /// state-only channel; Stream/Diagnostic events are not, so state-only
    /// consumers never pay for high-rate delta traffic.
    pub fn publish(&self, event: Event) -> Seq {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let envelope = Envelope {
            seq,
            ts: now_ms(),
            event,
        };
        // No receivers is fine (e.g. before the UI attaches); lagging
        // receivers observe the missed count via the bounded-bus contract.
        // PERF-002: classify BEFORE touching the envelope so the hot
        // Stream/Diagnostic path moves the sole envelope into `tx` with ZERO
        // clones, and only State (which must also reach `state_tx`) clones
        // once. A high-rate `message.delta`/`tool.output` flood therefore
        // never pays a clone for the state-only channel it does not use.
        match envelope.event.class() {
            crate::EventClass::State => {
                let _ = self.state_tx.send(envelope.clone());
                let _ = self.tx.send(envelope);
            }
            _ => {
                let _ = self.tx.send(envelope);
            }
        }
        seq
    }

    pub fn subscribe(&self) -> Subscription {
        Subscription {
            rx: self.tx.subscribe(),
            canceled: false,
        }
    }

    /// Subscribe to State-class events only (no stream deltas). For
    /// correctness-critical consumers that only track state — running
    /// tracker, queue coordinator — this guarantees they are never woken by
    /// nor lagged on high-rate stream traffic.
    pub fn subscribe_state(&self) -> Subscription {
        Subscription {
            rx: self.state_tx.subscribe(),
            canceled: false,
        }
    }

    /// Number of live receivers; useful in diagnostics (law 13 visibility).
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count() + self.state_tx.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// One subscription with explicit cleanup. Drop ends the subscription
/// automatically (broadcast receivers unsubscribe on drop).
pub struct Subscription {
    rx: Receiver<Envelope>,
    canceled: bool,
}

impl Subscription {
    /// Explicitly end this subscription.
    pub fn cancel(&mut self) {
        self.canceled = true;
    }

    pub fn is_canceled(&self) -> bool {
        self.canceled
    }

    /// Non-blocking poll. Returns `None` when nothing is pending.
    pub fn try_recv(&mut self) -> Result<Option<Envelope>, SubscribeError> {
        if self.canceled {
            return Err(SubscribeError::Closed);
        }
        match self.rx.try_recv() {
            Ok(env) => Ok(Some(env)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Lagged(skipped)) => Err(SubscribeError::Lagged(skipped)),
            Err(TryRecvError::Closed) => Err(SubscribeError::Closed),
        }
    }

    /// Blocking poll (async). Use inside core tasks, not the UI thread.
    pub async fn recv(&mut self) -> Result<Envelope, SubscribeError> {
        if self.canceled {
            return Err(SubscribeError::Closed);
        }
        match self.rx.recv().await {
            Ok(env) => Ok(env),
            Err(RecvError::Lagged(skipped)) => Err(SubscribeError::Lagged(skipped)),
            Err(RecvError::Closed) => Err(SubscribeError::Closed),
        }
    }
}

fn now_ms() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Event;

    #[tokio::test]
    async fn seq_is_monotonic_and_unique() {
        let bus = EventBus::new();
        let a = bus.publish(Event::RuntimeWarning {
            code: "x".into(),
            message: "a".into(),
        });
        let b = bus.publish(Event::RuntimeWarning {
            code: "x".into(),
            message: "b".into(),
        });
        assert!(b > a);
    }

    #[tokio::test]
    async fn subscription_receives_in_order() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        bus.publish(Event::AppStarted {
            version: "0.1.0".into(),
        });
        bus.publish(Event::RuntimeWarning {
            code: "c".into(),
            message: "m".into(),
        });
        let first = sub.recv().await.unwrap();
        let second = sub.recv().await.unwrap();
        assert_eq!(first.event.name(), "app.started");
        assert_eq!(second.event.name(), "runtime.warning");
        assert!(second.seq > first.seq);
    }

    #[tokio::test]
    async fn cancel_stops_delivery() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        sub.cancel();
        bus.publish(Event::RuntimeWarning {
            code: "x".into(),
            message: "m".into(),
        });
        assert!(matches!(sub.recv().await, Err(SubscribeError::Closed)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lagged_reports_missed_count_not_unbounded_buffer() {
        let bus = EventBus::with_capacity(4);
        let mut sub = bus.subscribe();
        // Join ensures every publish is fully applied (buffer advanced past
        // the subscriber's cursor) before the slow consumer polls — no race
        // between the producer loop and the first `try_recv` on a threaded
        // runtime.
        // Note: `with_capacity` floors the channel at 16, so publishing 32
        // messages guarantees the ring wraps (the slow consumer's cursor is
        // then older than the oldest retained message).
        let producer = async {
            for i in 0..32 {
                bus.publish(Event::RuntimeWarning {
                    code: "x".into(),
                    message: format!("m{i}"),
                });
            }
        };
        tokio::join!(producer);
        // The slow consumer must observe lag, and the bus must still work
        // for subscribers attached afterwards.
        match sub.try_recv() {
            Err(SubscribeError::Lagged(_)) => {}
            other => panic!("expected lag, got {other:?}"),
        }
        // tokio broadcast positions a new receiver at the current tail, so it
        // only sees events published after subscribe — publish one to prove
        // the bounded bus keeps working for fresh subscribers.
        let mut fresh = bus.subscribe();
        bus.publish(Event::RuntimeWarning {
            code: "x".into(),
            message: "after".into(),
        });
        let got = fresh
            .try_recv()
            .unwrap()
            .expect("fresh subscriber sees a new event");
        assert_eq!(got.seq, 32);
    }

    /// Event storm (EVENTS.md backpressure contract, TASK 04 §34): thousands
    /// of lightweight events, a consumer that keeps up — must not deadlock,
    /// must not corrupt ordering, and a fresh subscriber afterwards still
    /// works.
    #[tokio::test(flavor = "current_thread")]
    async fn event_storm_does_not_deadlock_and_preserves_order() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        const N: u64 = 10_000;
        let producer = async {
            for i in 0..N {
                bus.publish(Event::MessageDelta {
                    session_id: "s".into(),
                    run_id: "r".into(),
                    delta: format!("d{i}"),
                });
            }
        };
        tokio::join!(producer);
        let mut got = 0u64;
        let mut accounted = 0u64; // received + explicitly skipped via Lagged
        loop {
            match sub.try_recv() {
                Ok(Some(env)) => {
                    // After a `Lagged(n)` the next delivered event resumes at
                    // seq `accounted`: seq stays contiguous with what the
                    // consumer has already seen (bounded-bus contract).
                    assert_eq!(
                        env.seq, accounted,
                        "delivered events must stay contiguous with the bounded-bus contract"
                    );
                    got += 1;
                    accounted += 1;
                }
                Ok(None) => break,
                Err(SubscribeError::Lagged(n)) => {
                    accounted += n;
                    continue;
                }
                Err(e) => panic!("unexpected drain error: {e:?}"),
            }
        }
        assert_eq!(
            accounted, N,
            "every published event must be either delivered or explicitly accounted via lag"
        );
        assert!(
            got > 0,
            "fast consumer must receive events before the ring wraps"
        );
        // Bus still functional after the storm.
        let mut fresh = bus.subscribe();
        bus.publish(Event::AppStarted {
            version: "0.1.0".into(),
        });
        assert!(fresh.try_recv().unwrap().is_some());
    }

    /// Multi-producer (TASK 04 §35): concurrent publishers must not corrupt
    /// the bus or panic. The guarantee is per-producer ordering: each
    /// publisher's own events arrive in its emission order (sequences are
    /// global and monotonic).
    #[tokio::test]
    async fn concurrent_producers_do_not_corrupt_bus() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        let mut handles = Vec::new();
        for p in 0..4u64 {
            let bus = bus.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..500u64 {
                    bus.publish(Event::RuntimeWarning {
                        code: "p".into(),
                        message: format!("{p}:{i}"),
                    });
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // Drain. The bus is bounded, so the subscriber may lag; the contract
        // is: no corruption (strictly increasing global seq) and no lost
        // delivery without an explicit `Lagged` account.
        let mut seqs: Vec<u64> = Vec::new();
        let mut accounted = 0u64;
        loop {
            match sub.try_recv() {
                Ok(Some(env)) => {
                    seqs.push(env.seq);
                    accounted += 1;
                }
                Ok(None) => break,
                Err(SubscribeError::Lagged(n)) => accounted += n,
                Err(e) => panic!("unexpected drain error: {e:?}"),
            }
        }
        assert_eq!(
            accounted, 2000,
            "every published event delivered or explicitly lagged"
        );
        assert!(
            seqs.windows(2).all(|w| w[1] > w[0]),
            "global seq must be strictly increasing (no corruption)"
        );
    }

    /// Reentrancy policy (TASK 04 §44/§45): the bus has no callback API —
    /// subscribers hold polled handles and the bus never holds a lock across
    /// subscriber delivery. A consumer that publishes while draining must not
    /// deadlock and the bus must keep working.
    #[tokio::test(flavor = "current_thread")]
    async fn consumer_can_publish_while_draining_without_deadlock() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        bus.publish(Event::AppStarted {
            version: "0.1.0".into(),
        });
        // Inside the consumer loop, publish another event while draining.
        let first = sub.try_recv().unwrap().unwrap();
        assert_eq!(first.event.name(), "app.started");
        bus.publish(Event::RuntimeWarning {
            code: "x".into(),
            message: "from-consumer".into(),
        });
        let second = sub.try_recv().unwrap().unwrap();
        assert_eq!(second.event.name(), "runtime.warning");
        assert!(second.seq > first.seq);
    }

    /// Listener lifecycle (TASK 04 §37): repeated subscribe/drop must return
    /// the listener registry to baseline — no listener multiplication across
    /// workspace/session switches or UI remounts.
    #[tokio::test]
    async fn repeated_subscribe_drop_does_not_leak_listeners() {
        let bus = EventBus::new();
        let baseline = bus.subscriber_count();
        for _ in 0..200 {
            let sub = bus.subscribe();
            assert_eq!(bus.subscriber_count(), baseline + 1);
            drop(sub);
        }
        assert_eq!(
            bus.subscriber_count(),
            baseline,
            "no listener leak after 200 cycles"
        );
    }

    /// Consumer failure isolation (TASK 09 §21): one subscriber that stops
    /// consuming (drops out / is slow) must not affect other subscribers or
    /// the bus. The bus is a push-based channel with no callback registry, so
    /// a failed consumer can never corrupt delivery to the rest.
    #[tokio::test]
    async fn failing_consumer_does_not_affect_other_subscribers() {
        let bus = EventBus::new();
        let mut healthy = bus.subscribe();
        let mut stalled = bus.subscribe();

        // The stalled consumer stops polling after its first event.
        bus.publish(Event::AppStarted {
            version: "0.1.0".into(),
        });
        assert!(stalled.try_recv().unwrap().is_some());
        drop(stalled); // "consumer failure": gone without cleanup
                       // The healthy consumer already consumed its copy of the same event;
                       // drain it so the burst below is measured exactly.
        assert!(healthy.try_recv().unwrap().is_some());

        // A burst arrives; the healthy consumer must see it all.
        const N: u64 = 200;
        let producer = async {
            for i in 0..N {
                bus.publish(Event::MessageDelta {
                    session_id: "s".into(),
                    run_id: "r".into(),
                    delta: format!("d{i}"),
                });
            }
        };
        tokio::join!(producer);
        let mut received = 0u64;
        let mut accounted = 0u64;
        loop {
            match healthy.try_recv() {
                Ok(Some(_)) => {
                    received += 1;
                    accounted += 1;
                }
                Ok(None) => break,
                Err(SubscribeError::Lagged(n)) => accounted += n,
                Err(e) => panic!("unexpected drain error: {e:?}"),
            }
        }
        assert_eq!(
            accounted, N,
            "stalled consumer must not cost the healthy one any events"
        );
        assert!(received > 0, "healthy consumer receives a fair share");

        // The bus stays fully usable for a fresh subscriber.
        let mut fresh = bus.subscribe();
        bus.publish(Event::EngineReady {
            engine_id: "e".into(),
        });
        assert!(fresh.try_recv().unwrap().is_some());
    }

    /// A panicking consumer task cannot corrupt the bus or affect others
    /// (TASK 09 §21): subscribers are polled handles, not callbacks, so a
    /// panic in one task has no shared state to poison.
    #[tokio::test]
    async fn panicking_consumer_does_not_poison_the_bus() {
        let bus = EventBus::new();
        let mut healthy = bus.subscribe();
        let panicky_bus = bus.clone();
        // The task signals once it has subscribed, so the event is published
        // only after the receiver is attached (current_thread scheduling:
        // spawned tasks do not run until the next await point).
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut sub = panicky_bus.subscribe();
            let _ = ready_tx.send(());
            // Panic on the very first event, mid-consumer.
            let _ = sub.recv().await;
            panic!("consumer bug");
        });
        ready_rx.await.expect("consumer subscribed");
        bus.publish(Event::AppStarted {
            version: "0.1.0".into(),
        });
        // Healthy consumer drains its copy of the same event before the
        // panic so the post-panic probe below is unambiguous.
        assert!(healthy.try_recv().unwrap().is_some());
        assert!(
            handle.await.is_err(),
            "consumer panic must propagate to its owner"
        );

        // Other consumers are unaffected and the bus still delivers.
        bus.publish(Event::RuntimeWarning {
            code: "x".into(),
            message: "after panic".into(),
        });
        let got = healthy.try_recv().unwrap().unwrap();
        assert_eq!(got.event.name(), "runtime.warning");
    }

    /// Diagnostic events never recurse (TASK 09 §22, EVENTS.md §18): the bus
    /// is a plain channel — publishing a `runtime.error` cannot trigger any
    /// further publish. Exactly the published events arrive; there is no
    /// feedback path that could storm.
    #[tokio::test]
    async fn diagnostic_publish_never_recurses() {
        let bus = EventBus::new();
        let mut sub = bus.subscribe();
        // Baseline AFTER subscribing: the test's own subscription is the only
        // one; publishing diagnostics must never add more.
        let baseline = bus.subscriber_count();
        const N: usize = 50;
        for i in 0..N {
            bus.publish(Event::RuntimeError {
                code: format!("E{i}"),
                message: format!("boom {i}"),
            });
        }
        // Exactly N events arrive — publishing a diagnostic never spawns
        // another one (no recursion, no error-storm amplification).
        let mut got = 0usize;
        loop {
            match sub.try_recv() {
                Ok(Some(env)) => {
                    assert!(matches!(env.event, Event::RuntimeError { .. }));
                    got += 1;
                }
                Ok(None) => break,
                Err(SubscribeError::Lagged(_)) => panic!("50 events cannot lag a 1024-cap bus"),
                Err(e) => panic!("unexpected drain error: {e:?}"),
            }
        }
        assert_eq!(got, N, "no auto-generated events, no loss");
        assert_eq!(
            bus.subscriber_count(),
            baseline,
            "diagnostics do not multiply subscriptions"
        );
    }

    #[test]
    fn event_classes_are_classified() {
        use crate::EventClass;
        assert_eq!(
            Event::EngineReady {
                engine_id: "e".into()
            }
            .class(),
            EventClass::State
        );
        assert_eq!(
            Event::MessageDelta {
                session_id: "s".into(),
                run_id: "r".into(),
                delta: "d".into()
            }
            .class(),
            EventClass::Stream
        );
        assert_eq!(
            Event::RuntimeWarning {
                code: "x".into(),
                message: "m".into()
            }
            .class(),
            EventClass::Diagnostic
        );
    }
}
