use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};
use usb4_proto::protocol::{CHUNK, FrameHeader, Kind, MAX_FRAME, PacketPool};

struct CountingAlloc;

thread_local! {
    // Ignore allocations made by the test harness on other threads.
    static ALLOCS: Cell<Option<usize>> = const { Cell::new(None) };
}

// SAFETY: delegates directly to System allocator while counting calls on the measured thread.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|count| count.set(count.get().map(|n| n + 1)));
        // SAFETY: forwards valid layout to System.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwards pointer and layout allocated by System.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

#[test]
fn steady_state_packet_pool_makes_zero_allocations() {
    let mut tx_pool = PacketPool::new(4);
    let mut rx_pool = PacketPool::new(4);
    let mut wire = vec![0u8; MAX_FRAME];
    let payload = vec![0xA5u8; CHUNK];
    let mut header = FrameHeader::control(Kind::RdmaWrite, 0, 1, 7);
    header.total = CHUNK as u64;
    header.rkey = 0x1234_5678_9ABC_DEF0;

    ALLOCS.set(Some(0));
    for i in 0..1000u64 {
        header.request = i + 1;
        let pkt = tx_pool.packet(header, &payload).unwrap();
        let cloned = pkt.clone();
        let n = cloned.encode(&mut wire).unwrap();
        drop(cloned);
        drop(pkt);
        let decoded = rx_pool.decode(&wire[..n]).unwrap();
        assert_eq!(decoded.header.rkey, header.rkey);
        assert_eq!(decoded.data().len(), CHUNK);
        drop(decoded);
    }
    assert_eq!(
        ALLOCS.replace(None),
        Some(0),
        "steady-state packet path allocated memory"
    );
}
