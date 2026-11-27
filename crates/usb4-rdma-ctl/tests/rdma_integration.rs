//! Multiprocess end-to-end RDMA integration tests over emulated USB4 links.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use usb4_rdma_core::{AccessFlags, Context, Error, WcOpcode, WcStatus};

static TEST_SEQ: AtomicU64 = AtomicU64::new(1);

struct TestCluster {
    dir: PathBuf,
    children: Vec<Child>,
    sockets: Vec<PathBuf>,
}

impl TestCluster {
    fn spawn(config_toml: &str, node_ids: &[u32]) -> Self {
        let seq = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("u4rd-it-{}-{seq}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let cfg = dir.join("cluster.toml");
        fs::write(&cfg, config_toml).unwrap();
        let emu_dir = dir.join("emu");
        fs::create_dir_all(&emu_dir).unwrap();

        let bin = env!("CARGO_BIN_EXE_usb4-rdma-ctl");
        let mut children = Vec::new();
        let mut sockets = Vec::new();
        for &node in node_ids {
            let sock = dir.join(format!("n{node}.sock"));
            let child = Command::new(bin)
                .args([
                    "serve",
                    "--config",
                    cfg.to_str().unwrap(),
                    "--node",
                    &node.to_string(),
                    "--socket",
                    sock.to_str().unwrap(),
                    "--backend",
                    "emulated",
                    "--emulated-dir",
                    emu_dir.to_str().unwrap(),
                ])
                .stderr(Stdio::null())
                .stdout(Stdio::null())
                .spawn()
                .expect("spawn usb4-rdma-ctl");
            children.push(child);
            sockets.push(sock);
        }

        // Wait until every daemon socket is up and its fabric handshake is ready.
        let deadline = Instant::now() + Duration::from_secs(10);
        for sock in &sockets {
            loop {
                if let Ok(ctx) = Context::open(sock)
                    && let Ok(stats) = ctx.stats()
                    && stats.ready
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "daemon at {} did not become ready",
                    sock.display()
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        Self {
            dir,
            children,
            sockets,
        }
    }

    fn socket(&self, idx: usize) -> &Path {
        &self.sockets[idx]
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for c in &mut self.children {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = fs::remove_dir_all(&self.dir);
    }
}

const TWO_NODE_CFG: &str = r#"
epoch = 101
queue_depth = 16
timeout_ms = 5000

[[nodes]]
id = 0
name = "node0"

[[nodes]]
id = 1
name = "node1"

[[links]]
id = "l0"
a = 0
b = 1
endpoint_a = "e0"
endpoint_b = "e1"
controller_a = "c0"
controller_b = "c0"
speed_mbps = 40000
"#;

const THREE_NODE_CHAIN_CFG: &str = r#"
epoch = 202
queue_depth = 16
timeout_ms = 5000

[[nodes]]
id = 0
name = "node0"

[[nodes]]
id = 1
name = "node1"

[[nodes]]
id = 2
name = "node2"

[[links]]
id = "l01"
a = 0
b = 1
endpoint_a = "e0"
endpoint_b = "e1"
controller_a = "c0"
controller_b = "c0"
speed_mbps = 40000

[[links]]
id = "l12"
a = 1
b = 2
endpoint_a = "e2"
endpoint_b = "e3"
controller_a = "c1"
controller_b = "c0"
speed_mbps = 40000
"#;

#[test]
fn one_sided_rdma_write_read_and_write_with_imm() {
    let cluster = TestCluster::spawn(TWO_NODE_CFG, &[0, 1]);
    let ctx0 = Context::open(cluster.socket(0)).unwrap();
    let ctx1 = Context::open(cluster.socket(1)).unwrap();

    let attr0 = ctx0.query_device().unwrap();
    assert_eq!(attr0.node_id, 0);
    assert!(attr0.supports_one_sided_rdma);
    assert!(!attr0.zero_copy_dma);

    let pd0 = ctx0.alloc_pd();
    let pd1 = ctx1.alloc_pd();
    let cq0 = ctx0.create_cq().unwrap();
    let cq1 = ctx1.create_cq().unwrap();

    let listener1 = pd1.listen().unwrap();
    let handle1 = listener1.handle();

    let accept_thread = std::thread::spawn(move || {
        let qp1 = listener1
            .accept(&cq1, &cq1, Duration::from_secs(5))
            .unwrap();
        (qp1, cq1)
    });
    let qp0 = pd0.connect(&handle1, &cq0, &cq0).unwrap();
    let (qp1, cq1) = accept_thread.join().unwrap();

    let rw_all = AccessFlags::LOCAL_READ
        | AccessFlags::LOCAL_WRITE
        | AccessFlags::REMOTE_READ
        | AccessFlags::REMOTE_WRITE;

    // Allocate a 3 MiB remotely accessible MR on Node 1 and a local MR on Node 0.
    let size = 2 * 1024 * 1024 + 512 * 1024 + 137; // > 2.5 MiB (multi-window + unaligned tail)
    let mr0 = pd0.alloc_mr(size, rw_all).unwrap();
    let mr1 = pd1.alloc_mr(size, rw_all).unwrap();
    assert_ne!(mr1.rkey(), 0);

    // Fill Node 0 source buffer with a deterministic pattern.
    let pattern: Vec<u8> = (0..size).map(|i| ((i * 31 + 17) & 0xFF) as u8).collect();
    mr0.write_bytes(0, &pattern).unwrap();

    // 1. One-sided RDMA_WRITE (2.5+ MiB) — ZERO recv calls on Node 1!
    qp0.post_rdma_write(1001, &mr0, 0..size, mr1.rkey(), 0)
        .unwrap();
    let wc = cq0.poll_blocking(Duration::from_secs(10)).unwrap();
    assert_eq!(wc.wr_id, 1001);
    assert_eq!(wc.status, WcStatus::Success);
    assert_eq!(wc.opcode, WcOpcode::RdmaWrite);
    assert_eq!(wc.byte_len, size as u64);

    // Verify Node 1's memory received every byte without Node 1 ever polling or calling recv.
    let mut verify = vec![0u8; size];
    mr1.read_bytes(0, &mut verify).unwrap();
    assert_eq!(verify, pattern);

    // 2. Zero-length RDMA_WRITE_WITH_IMM signaling a 32-bit immediate value.
    let dummy_recv_mr = pd1.alloc_mr(64, rw_all).unwrap();
    qp1.post_recv(2001, &dummy_recv_mr, 0..64, 0).unwrap();
    qp0.post_rdma_write_imm(1002, &mr0, 0..0, mr1.rkey(), 16, 0xCAFE_BABE)
        .unwrap();
    let wc_recv = cq1.poll_blocking(Duration::from_secs(5)).unwrap();
    assert_eq!(wc_recv.wr_id, 2001);
    assert_eq!(wc_recv.status, WcStatus::Success);
    assert_eq!(wc_recv.opcode, WcOpcode::RecvRdmaWithImm);
    assert!(wc_recv.has_imm);
    assert_eq!(wc_recv.tag_or_imm, 0xCAFE_BABE);
    assert_eq!(wc_recv.byte_len, 0);

    let wc_send = cq0.poll_blocking(Duration::from_secs(5)).unwrap();
    assert_eq!(wc_send.wr_id, 1002);
    assert_eq!(wc_send.status, WcStatus::Success);
    assert_eq!(wc_send.opcode, WcOpcode::RdmaWrite);

    // 3. One-sided RDMA_READ back into a borrowed local Vec<u8> — ZERO send calls on Node 1!
    let mut read_back = vec![0u8; size];
    // SAFETY: `read_back` stays live and untouched until `rdma_read` completes and `borrowed` drops.
    let borrowed = unsafe {
        pd0.reg_mr_local(
            read_back.as_mut_ptr(),
            read_back.len(),
            AccessFlags::LOCAL_READ | AccessFlags::LOCAL_WRITE,
        )
        .unwrap()
    };
    qp0.post_rdma_read(1003, &borrowed, 0..size, mr1.rkey(), 0)
        .unwrap();
    let wc_read = cq0.poll_blocking(Duration::from_secs(10)).unwrap();
    assert_eq!(wc_read.wr_id, 1003);
    assert_eq!(wc_read.status, WcStatus::Success);
    assert_eq!(wc_read.opcode, WcOpcode::RdmaRead);
    assert_eq!(wc_read.byte_len, size as u64);
    assert_eq!(read_back, pattern);

    // 4. Two-sided full-duplex SEND/RECV on the exact same QP (both directions simultaneously).
    let msg0 = b"hello-from-node0-to-node1";
    let msg1 = b"hello-from-node1-to-node0";
    let mut buf0 = [0u8; 64];
    let mut buf1 = [0u8; 64];
    mr0.write_bytes(0, msg0).unwrap();
    mr1.write_bytes(0, msg1).unwrap();
    // SAFETY: stack arrays remain live until completions return and MRs drop.
    let r0 = unsafe {
        pd0.reg_mr_local(buf0.as_mut_ptr(), buf0.len(), AccessFlags::LOCAL_WRITE)
            .unwrap()
    };
    // SAFETY: stack arrays remain live until completions return and MRs drop.
    let r1 = unsafe {
        pd1.reg_mr_local(buf1.as_mut_ptr(), buf1.len(), AccessFlags::LOCAL_WRITE)
            .unwrap()
    };

    qp0.post_recv(3001, &r0, 0..64, 7).unwrap();
    qp1.post_recv(3002, &r1, 0..64, 9).unwrap();
    qp0.post_send(3003, &mr0, 0..msg0.len(), 9).unwrap();
    qp1.post_send(3004, &mr1, 0..msg1.len(), 7).unwrap();

    let mut done0 = 0;
    let mut done1 = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while done0 < 2 || done1 < 2 {
        assert!(Instant::now() < deadline, "full-duplex send/recv timed out");
        if let Some(w) = cq0.poll_one().unwrap() {
            assert_eq!(w.status, WcStatus::Success);
            done0 += 1;
        }
        if let Some(w) = cq1.poll_one().unwrap() {
            assert_eq!(w.status, WcStatus::Success);
            done1 += 1;
        }
        std::thread::sleep(Duration::from_micros(100));
    }
    assert_eq!(&buf0[..msg1.len()], msg1);
    assert_eq!(&buf1[..msg0.len()], msg0);
}

#[test]
fn access_control_bounds_and_deregistered_rkey_fail_closed() {
    let cluster = TestCluster::spawn(TWO_NODE_CFG, &[0, 1]);
    let ctx0 = Context::open(cluster.socket(0)).unwrap();
    let ctx1 = Context::open(cluster.socket(1)).unwrap();
    let pd0 = ctx0.alloc_pd();
    let pd1 = ctx1.alloc_pd();
    let cq0 = ctx0.create_cq().unwrap();
    let cq1 = ctx1.create_cq().unwrap();

    // 1. Borrowed local MR must reject REMOTE_* access flags fail-closed.
    let mut stack = [0u8; 64];
    // SAFETY: pointer valid during this call; rejected before retaining pointer.
    let err = unsafe {
        pd0.reg_mr_local(
            stack.as_mut_ptr(),
            stack.len(),
            AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE,
        )
    };
    assert!(matches!(err, Err(Error::UnsupportedAccess)));

    let listener1 = pd1.listen().unwrap();
    let handle1 = listener1.handle();
    let t = std::thread::spawn(move || {
        listener1
            .accept(&cq1, &cq1, Duration::from_secs(5))
            .unwrap()
    });
    let qp0 = pd0.connect(&handle1, &cq0, &cq0).unwrap();
    let _qp1 = t.join().unwrap();

    let src = pd0
        .alloc_mr(4096, AccessFlags::LOCAL_READ | AccessFlags::LOCAL_WRITE)
        .unwrap();
    let ro_mr = pd1
        .alloc_mr(4096, AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_READ)
        .unwrap();
    let ro_rkey = ro_mr.rkey();

    // 2. RDMA_WRITE to a REMOTE_READ-only MR must return RemoteAccessErr.
    qp0.post_rdma_write(1, &src, 0..64, ro_rkey, 0).unwrap();
    let wc = cq0.poll_blocking(Duration::from_secs(5)).unwrap();
    assert_eq!(wc.wr_id, 1);
    assert_eq!(wc.status, WcStatus::RemoteAccessErr);

    // 3. Out-of-bounds RDMA_READ must return RemoteAccessErr.
    qp0.post_rdma_read(2, &src, 0..64, ro_rkey, 4080).unwrap();
    let wc = cq0.poll_blocking(Duration::from_secs(5)).unwrap();
    assert_eq!(wc.wr_id, 2);
    assert_eq!(wc.status, WcStatus::RemoteAccessErr);

    // 4. Valid RDMA_READ works, then after deregistering ro_mr the same rkey fails cleanly.
    ro_mr.write_bytes(0, b"readonly-ok").unwrap();
    qp0.post_rdma_read(3, &src, 0..11, ro_rkey, 0).unwrap();
    let wc = cq0.poll_blocking(Duration::from_secs(5)).unwrap();
    assert_eq!(wc.status, WcStatus::Success);
    let mut got = [0u8; 11];
    src.read_bytes(0, &mut got).unwrap();
    assert_eq!(&got, b"readonly-ok");

    drop(ro_mr); // deregisters rkey on daemon
    qp0.post_rdma_read(4, &src, 0..11, ro_rkey, 0).unwrap();
    let wc = cq0.poll_blocking(Duration::from_secs(5)).unwrap();
    assert_eq!(wc.wr_id, 4);
    assert_eq!(wc.status, WcStatus::RemoteAccessErr);
}

#[test]
fn multihop_chain_one_sided_rdma_across_intermediate_node() {
    let cluster = TestCluster::spawn(THREE_NODE_CHAIN_CFG, &[0, 1, 2]);
    let ctx0 = Context::open(cluster.socket(0)).unwrap();
    let ctx1 = Context::open(cluster.socket(1)).unwrap();
    let ctx2 = Context::open(cluster.socket(2)).unwrap();

    let pd0 = ctx0.alloc_pd();
    let pd2 = ctx2.alloc_pd();
    let cq0 = ctx0.create_cq().unwrap();
    let cq2 = ctx2.create_cq().unwrap();

    let listener2 = pd2.listen().unwrap();
    let handle2 = listener2.handle();
    let t = std::thread::spawn(move || {
        listener2
            .accept(&cq2, &cq2, Duration::from_secs(5))
            .unwrap()
    });
    let qp0 = pd0.connect(&handle2, &cq0, &cq0).unwrap();
    let _qp2 = t.join().unwrap();

    let all = AccessFlags::LOCAL_READ
        | AccessFlags::LOCAL_WRITE
        | AccessFlags::REMOTE_READ
        | AccessFlags::REMOTE_WRITE;
    let mr0 = pd0.alloc_mr(128 * 1024, all).unwrap();
    let mr2 = pd2.alloc_mr(128 * 1024, all).unwrap();
    let payload: Vec<u8> = (0..128 * 1024)
        .map(|i| ((i * 7 + 3) & 0xFF) as u8)
        .collect();
    mr0.write_bytes(0, &payload).unwrap();

    // Node 0 writes 128 KiB to Node 2 through Node 1!
    qp0.post_rdma_write(501, &mr0, 0..payload.len(), mr2.rkey(), 0)
        .unwrap();
    let wc = cq0.poll_blocking(Duration::from_secs(5)).unwrap();
    assert_eq!(wc.status, WcStatus::Success);

    let mut check2 = vec![0u8; payload.len()];
    mr2.read_bytes(0, &mut check2).unwrap();
    assert_eq!(check2, payload);

    // Node 0 reads it back from Node 2 through Node 1!
    mr0.write_bytes(0, &vec![0u8; payload.len()]).unwrap();
    qp0.post_rdma_read(502, &mr0, 0..payload.len(), mr2.rkey(), 0)
        .unwrap();
    let wc = cq0.poll_blocking(Duration::from_secs(5)).unwrap();
    assert_eq!(wc.status, WcStatus::Success);

    let mut check0 = vec![0u8; payload.len()];
    mr0.read_bytes(0, &mut check0).unwrap();
    assert_eq!(check0, payload);

    // Confirm intermediate Node 1 actually forwarded frames.
    let stats1 = ctx1.stats().unwrap();
    assert!(
        stats1.forwarded_frames > 0,
        "expected Node 1 to forward multi-hop RDMA traffic"
    );
}
