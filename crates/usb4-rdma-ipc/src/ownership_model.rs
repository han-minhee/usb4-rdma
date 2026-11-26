#[test]
fn slot_reuse_preserves_exclusive_payload_ownership() {
    use loom::{
        cell::UnsafeCell,
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        thread,
    };

    struct Slot {
        state: AtomicU32,
        payload: UnsafeCell<u32>,
    }
    // SAFETY: the model checks that every payload access is protected by
    // the same exclusive release/acquire ownership transitions as IPC.
    unsafe impl Sync for Slot {}

    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.max_permutations = Some(1000);
    model.check(|| {
        let slot = Arc::new(Slot {
            state: AtomicU32::new(crate::FREE),
            payload: UnsafeCell::new(0),
        });
        let client = slot.clone();
        let server = slot.clone();
        let a = thread::spawn(move || {
            for round in 1..=2 {
                while client.state.load(Ordering::Acquire) != crate::FREE {
                    thread::yield_now();
                }
                client.payload.with_mut(|p| {
                    // SAFETY: client owns FREE; server awaits POSTED.
                    unsafe {
                        *p = round;
                    }
                });
                client.state.store(crate::POSTED, Ordering::Release);
                while client.state.load(Ordering::Acquire) != crate::READY {
                    thread::yield_now();
                }
                client.payload.with(|p| {
                    // SAFETY: READY belongs to client until CONSUMED is published.
                    unsafe {
                        assert_eq!(*p, round * 2);
                    }
                });
                client.state.store(crate::CONSUMED, Ordering::Release);
                while client.state.load(Ordering::Acquire) != crate::COMPLETE {
                    thread::yield_now();
                }
                client.state.store(crate::FREE, Ordering::Release);
            }
        });
        let b = thread::spawn(move || {
            for round in 1..=2 {
                while server
                    .state
                    .compare_exchange(
                        crate::POSTED,
                        crate::BUSY,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_err()
                {
                    thread::yield_now();
                }
                server.payload.with_mut(|p| {
                    // SAFETY: successful CAS exclusively claimed POSTED payload.
                    unsafe {
                        assert_eq!(*p, round);
                        *p *= 2;
                    }
                });
                server.state.store(crate::READY, Ordering::Release);
                while server.state.load(Ordering::Acquire) != crate::CONSUMED {
                    thread::yield_now();
                }
                server.state.store(crate::COMPLETE, Ordering::Release);
            }
        });
        a.join().unwrap();
        b.join().unwrap();
    });
}
