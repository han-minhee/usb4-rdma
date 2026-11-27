#![cfg(target_os = "linux")]

use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use usb4_proto::{
    Error,
    protocol::{FrameHeader, Kind, Packet},
};
use usb4_rdma_core::{
    AccessFlags, CompletionQueue, Context, Fabric, OwnedMr, QueuePair, Service, WcStatus,
    config::{Cluster, Link, Node},
    ipc::{Server, Welcome},
};
use usb4_sim::{Cable, CableConfig, Unplug};
use usb4_stream_hal::{Event, FastPoll, LinkCapabilities, StreamTransport, stream::StreamLink};

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);
const LIMIT: Duration = Duration::from_secs(25);
const RW: [AccessFlags; 2] = [AccessFlags::LOCAL_READ, AccessFlags::LOCAL_WRITE];

#[derive(Default)]
struct TailGate {
    hold: AtomicBool,
    observed: AtomicBool,
}

struct DelayedTail {
    inner: StreamLink,
    gate: Arc<TailGate>,
    written: Arc<AtomicUsize>,
}

impl DelayedTail {
    fn blocked(&self, header: FrameHeader) -> bool {
        if self.gate.hold.load(Ordering::Acquire)
            && header.kind == Kind::RdmaWrite
            && header.offset + u64::from(header.length) == header.total
        {
            self.gate.observed.store(true, Ordering::Release);
            true
        } else {
            false
        }
    }

    fn record(&self, header: &FrameHeader, submission: Option<u64>) {
        if submission.is_some() && header.kind == Kind::RdmaWrite {
            self.written
                .fetch_add(header.length as usize, Ordering::Relaxed);
        }
    }
}

impl StreamTransport for DelayedTail {
    fn capabilities(&self) -> LinkCapabilities {
        self.inner.capabilities()
    }
    fn try_submit(&mut self, packet: &Packet) -> usb4_proto::Result<Option<u64>> {
        if self.blocked(packet.header) {
            Ok(None)
        } else {
            let submission = self.inner.try_submit(packet)?;
            self.record(&packet.header, submission);
            Ok(submission)
        }
    }
    fn try_submit_with(
        &mut self,
        header: &FrameHeader,
        fill: &mut dyn FnMut(&mut [u8]) -> usb4_proto::Result<()>,
    ) -> usb4_proto::Result<Option<u64>> {
        if self.blocked(*header) {
            Ok(None)
        } else {
            let submission = self.inner.try_submit_with(header, fill)?;
            self.record(header, submission);
            Ok(submission)
        }
    }
    fn can_submit_data(&self) -> bool {
        self.inner.can_submit_data()
    }
    fn can_submit_direct(&self) -> bool {
        self.inner.can_submit_direct()
    }
    fn poll(&mut self) -> usb4_proto::Result<Option<Event>> {
        self.inner.poll()
    }
    fn poll_fast(
        &mut self,
        callback: &mut dyn FnMut(FrameHeader, &[u8]) -> usb4_proto::Result<bool>,
    ) -> usb4_proto::Result<FastPoll> {
        self.inner.poll_fast(callback)
    }
    fn flush(&mut self) -> usb4_proto::Result<()> {
        self.inner.flush()
    }
    fn shutdown(&mut self) -> usb4_proto::Result<()> {
        self.inner.shutdown()
    }
}

struct Pair {
    nodes: [Context; 2],
    rails: Vec<Cable>,
    rail_write_bytes: Vec<Arc<AtomicUsize>>,
    gate: Arc<TailGate>,
    stopped: Arc<AtomicBool>,
    workers: Vec<JoinHandle<usb4_proto::Result<()>>>,
    dir: PathBuf,
}

impl Pair {
    fn new(count: usize) -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("usb4-sim-{}-{id}", std::process::id()));
        fs::create_dir(&dir).unwrap();
        let nodes = [0, 1]
            .map(|i| Node {
                id: i,
                name: format!("node-{i}"),
            })
            .into();
        let mut cfg = Cluster {
            epoch: 300 + id as u64,
            nodes,
            links: Vec::new(),
            queue_depth: 16,
            timeout_ms: 20_000,
        };
        let mut transports: [Vec<Box<dyn StreamTransport>>; 2] = [Vec::new(), Vec::new()];
        let mut rails = Vec::new();
        let mut rail_write_bytes = Vec::new();
        let gate = Arc::new(TailGate::default());
        for n in 0..count {
            let (rail, [left, right]) = Cable::pair(CableConfig::default());
            rails.push(rail);
            let written = Arc::new(AtomicUsize::new(0));
            rail_write_bytes.push(written.clone());
            transports[0].push(Box::new(DelayedTail {
                inner: left.into_link(1, 40_000),
                gate: gate.clone(),
                written,
            }));
            transports[1].push(Box::new(right.into_link(0, 40_000)));
            cfg.links.push(Link {
                id: format!("rail-{n}"),
                a: 0,
                b: 1,
                endpoint_a: format!("a{n}"),
                endpoint_b: format!("b{n}"),
                controller_a: format!("controller-{n}"),
                controller_b: format!("controller-{n}"),
                speed_mbps: 40_000,
            });
        }
        let stopped = Arc::new(AtomicBool::new(false));
        let workers = transports
            .into_iter()
            .enumerate()
            .map(|(i, rails)| {
                let node = i as u32;
                let fabric = Fabric::new(&cfg, node, rails).unwrap();
                let server = Server::bind(
                    &dir.join(i.to_string()),
                    Welcome {
                        node,
                        epoch: cfg.epoch,
                        digest: cfg.digest(),
                        client: 0,
                    },
                )
                .unwrap();
                let service =
                    Service::with_max_mr_bytes(fabric, server, cfg.timeout_ms, 128 * 1024 * 1024);
                let closing = stopped.clone();
                thread::spawn(move || service.run(0, || closing.load(Ordering::Acquire)))
            })
            .collect();
        let nodes = [0, 1].map(|i| Context::open(dir.join(i.to_string())).unwrap());
        let deadline = Instant::now() + LIMIT;
        for ctx in &nodes {
            while !ctx.stats().unwrap().ready {
                assert!(Instant::now() < deadline);
                thread::yield_now();
            }
        }
        Self {
            nodes,
            rails,
            rail_write_bytes,
            gate,
            stopped,
            workers,
            dir,
        }
    }

    fn qps(&self) -> [(QueuePair, CompletionQueue, CompletionQueue); 2] {
        let [a, b] = &self.nodes;
        let listener = b.alloc_pd().listen().unwrap();
        let [a_tx, a_rx, b_tx, b_rx] = [a, a, b, b].map(|ctx| ctx.create_cq().unwrap());
        let mut accept = listener.begin_accept(&b_tx, &b_rx).unwrap();
        let a_qp = a
            .alloc_pd()
            .connect(&listener.handle(), &a_tx, &a_rx)
            .unwrap();
        let b_qp = accept.poll().unwrap().unwrap();
        [(a_qp, a_tx, a_rx), (b_qp, b_tx, b_rx)]
    }

    fn memory(&self, len: usize, remote_write: bool) -> [OwnedMr; 2] {
        let flags = RW[0]
            | RW[1]
            | if remote_write {
                AccessFlags::REMOTE_WRITE
            } else {
                AccessFlags::NONE
            };
        self.nodes
            .each_ref()
            .map(|ctx| ctx.alloc_pd().alloc_mr(len, flags).unwrap())
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
        fs::remove_dir_all(&self.dir).unwrap();
    }
}

#[test]
fn write_visibility_precedes_send_and_failed_predecessor_cancels_send() {
    let pair = Pair::new(2);
    let [(left, tx, _), (right, _, rx)] = pair.qps();
    let [(ctl, ctl_tx, _), (peer_ctl, _, ctl_rx)] = pair.qps();
    let len = 256 * 1024 + 17;
    let [src, target] = pair.memory(len, true);
    let [signal, observed] = pair.memory(1, false);
    let pattern = vec![0xab; len];
    src.write_bytes(0, &pattern).unwrap();
    pair.gate.hold.store(true, Ordering::Release);
    right.post_recv(2, &observed, 0..1, u64::MAX).unwrap();
    left.post_rdma_write(1, &src, 0..len, target.rkey(), 0)
        .unwrap();
    left.post_send(2, &signal, 0..1, u64::MAX).unwrap();
    let deadline = Instant::now() + LIMIT;
    while !pair.gate.observed.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline);
        assert!(tx.poll_one().unwrap().is_none());
        thread::yield_now();
    }
    assert!(rx.poll_one().unwrap().is_none());
    let [probe, response] = pair.memory(1, false);
    peer_ctl.post_recv(1, &response, 0..1, 55).unwrap();
    ctl.post_send(1, &probe, 0..1, 55).unwrap();
    let (mut probe_tx, mut probe_rx) = (false, false);
    while !probe_tx || !probe_rx {
        assert!(
            Instant::now() < deadline,
            "control traffic stalled behind an unrelated bulk frame"
        );
        if let Some(wc) = ctl_tx.poll_one().unwrap() {
            assert_eq!(wc.status, WcStatus::Success);
            probe_tx = true;
        }
        if let Some(wc) = ctl_rx.poll_one().unwrap() {
            assert_eq!(wc.status, WcStatus::Success);
            probe_rx = true;
        }
        assert!(tx.poll_one().unwrap().is_none());
        thread::yield_now();
    }
    pair.gate.hold.store(false, Ordering::Release);
    let mut sent = 0;
    let mut received = false;
    while sent != 2 || !received {
        assert!(Instant::now() < deadline);
        if let Some(wc) = tx.poll_one().unwrap() {
            sent += 1;
            assert_eq!((wc.wr_id, wc.status), (sent, WcStatus::Success));
        }
        if let Some(wc) = rx.poll_one().unwrap() {
            assert_eq!((wc.tag_or_imm, wc.status), (u64::MAX, WcStatus::Success));
            let mut actual = vec![0; len];
            target.read_bytes(0, &mut actual).unwrap();
            assert_eq!(actual, pattern);
            received = true;
        }
        thread::yield_now();
    }
    let mut striped = 0;
    for rail in &pair.rail_write_bytes {
        let bytes = rail.load(Ordering::Relaxed);
        assert!(bytes > 0, "an eligible rail did not carry any write bytes");
        striped += bytes;
    }
    assert_eq!(striped, len);
    pair.rails.iter().for_each(|rail| rail.pause(true));
    left.post_rdma_write(3, &src, 0..len, target.rkey(), 1)
        .unwrap();
    left.post_send(4, &signal, 0..1, 44).unwrap();
    pair.rails.iter().for_each(|rail| rail.pause(false));
    for wr in [3, 4] {
        let wc = tx.poll_blocking(LIMIT).unwrap();
        assert_eq!((wc.wr_id, wc.status), (wr, WcStatus::RemoteAccessErr));
    }
    assert!(!right.peek_recv(44).unwrap());
}

#[test]
fn unplug_completes_active_and_queued_work_once() {
    for mode in [Unplug::BrokenPipe, Unplug::PollError] {
        let pair = Pair::new(1);
        let [(left, tx, _), _right] = pair.qps();
        let [src, target] = pair.memory(4 * 1024 * 1024, true);
        pair.rails[0].pause(true);
        left.post_rdma_write(1, &src, 0..src.len(), target.rkey(), 0)
            .unwrap();
        left.post_send(2, &src, 0..1, 1).unwrap();
        pair.rails[0].unplug(mode);
        for wr in [1, 2] {
            let wc = tx.poll_blocking(LIMIT).unwrap();
            assert_eq!(wc.wr_id, wr);
            assert!(
                matches!(
                    wc.status,
                    WcStatus::TransportErr | WcStatus::ConnectionClosed
                ),
                "unexpected link-loss completion {wc:?}"
            );
        }
        assert!(tx.poll_one().unwrap().is_none());
        match left.post_send(3, &src, 0..1, 1) {
            Err(Error::Disconnected) => {}
            Ok(()) => assert_ne!(tx.poll_blocking(LIMIT).unwrap().status, WcStatus::Success),
            Err(error) => panic!("unexpected post error after link loss: {error}"),
        }
    }
}

#[test]
fn bidirectional_small_messages_and_large_writes_recover_credits() {
    const BULK: usize = 4 * 1024 * 1024;
    const WRITES: usize = 200;
    const MESSAGES: usize = 10_000;
    let pair = Pair::new(2);
    let bulk = pair.qps();
    let small = pair.qps();
    let local = pair.memory(BULK, false);
    let target = pair.memory(BULK * 2, true);
    let [rkey0, rkey1] = target.each_ref().map(OwnedMr::rkey);
    let tx_messages = pair.memory(16, false);
    let rx_messages = pair.memory(16, false);
    thread::scope(|s| {
        for (side, ((qp, tx, rx), (src, dst))) in small
            .into_iter()
            .zip(tx_messages.into_iter().zip(rx_messages))
            .enumerate()
        {
            s.spawn(move || {
                let own = [(side + 1) as u8; 16];
                let peer = [(2 - side) as u8; 16];
                src.write_bytes(0, &own).unwrap();
                for i in 0..MESSAGES as u64 {
                    qp.post_recv(i, &dst, 0..16, i).unwrap();
                    qp.post_send(i, &src, 0..16, i).unwrap();
                    assert_eq!(tx.poll_blocking(LIMIT).unwrap().status, WcStatus::Success);
                    let wc = rx.poll_blocking(LIMIT).unwrap();
                    assert_eq!((wc.wr_id, wc.status), (i, WcStatus::Success));
                    let mut bytes = [0; 16];
                    dst.read_bytes(0, &mut bytes).unwrap();
                    assert_eq!(bytes, peer);
                }
                assert!(tx.poll_one().unwrap().is_none());
            });
        }
        let [(left, left_tx, left_rx), (right, right_tx, right_rx)] = bulk;
        let [left_src, right_src] = local;
        let [left_dst, right_dst] = target;
        for (side, (qp, tx, rx, src, dst, key)) in [
            (left, left_tx, left_rx, left_src, left_dst, rkey1),
            (right, right_tx, right_rx, right_src, right_dst, rkey0),
        ]
        .into_iter()
        .enumerate()
        {
            s.spawn(move || {
                let body = vec![(side + 1) as u8; BULK];
                src.write_bytes(0, &body).unwrap();
                for i in 0..WRITES as u64 {
                    qp.post_recv(i, &dst, BULK..BULK + 1, i).unwrap();
                    qp.post_rdma_write(i * 2, &src, 0..BULK, key, 0).unwrap();
                    qp.post_send(i * 2 + 1, &src, 0..1, i).unwrap();
                    for id in [i * 2, i * 2 + 1] {
                        let wc = tx.poll_blocking(LIMIT).unwrap();
                        assert_eq!((wc.wr_id, wc.status), (id, WcStatus::Success));
                    }
                    let wc = rx.poll_blocking(LIMIT).unwrap();
                    assert_eq!((wc.wr_id, wc.status), (i, WcStatus::Success));
                }
                let mut actual = vec![0; BULK];
                dst.read_bytes(0, &mut actual).unwrap();
                assert_eq!(actual, vec![(2 - side) as u8; BULK]);
                assert!(tx.poll_one().unwrap().is_none());
            });
        }
    });
    for ctx in &pair.nodes {
        let stats = ctx.stats().unwrap();
        let expected = (BULK * WRITES + 16 * MESSAGES + WRITES) as u64;
        assert_eq!(stats.tx_payload_bytes, expected);
        assert_eq!(stats.rx_payload_bytes, expected);
    }
}
