//! 3-node host-memory Ring AllReduce (SUM) over `usb4-rdma` using one-sided `RDMA_WRITE_WITH_IMM`.

use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use usb4_rdma_core::{AccessFlags, Context, WcOpcode, WcStatus};

fn find_daemon_bin() -> PathBuf {
    let exe = std::env::current_exe().expect("current_exe");
    let bin = exe
        .parent()
        .and_then(|p| p.parent())
        .expect("profile dir")
        .join("usb4-rdma-ctl");
    assert!(
        bin.exists(),
        "build usb4-rdma-ctl first (cargo build -p usb4-rdma-ctl)"
    );
    bin
}

fn main() {
    let dir = std::env::temp_dir().join(format!("u4rd-ex-ring-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("emu")).unwrap();
    let cfg = dir.join("ring.toml");
    fs::write(
        &cfg,
        r#"
epoch = 22
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
[[links]]
id = "l12"
a = 1
b = 2
endpoint_a = "e2"
endpoint_b = "e3"
controller_a = "c1"
controller_b = "c0"
[[links]]
id = "l20"
a = 2
b = 0
endpoint_a = "e4"
endpoint_b = "e5"
controller_a = "c1"
controller_b = "c1"
"#,
    )
    .unwrap();

    let bin = find_daemon_bin();
    let mut procs: Vec<Child> = Vec::new();
    let mut socks = Vec::new();
    for node in 0..3u32 {
        let sock = dir.join(format!("n{node}.sock"));
        let child = Command::new(&bin)
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
                dir.join("emu").to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn daemon");
        procs.push(child);
        socks.push(sock);
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    for sock in &socks {
        while Context::open(sock)
            .and_then(|c| c.stats())
            .map_or(true, |s| !s.ready)
        {
            assert!(Instant::now() < deadline, "daemon startup timed out");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    let n_ranks = 3usize;
    let elems = 1024usize;
    let bytes = elems * std::mem::size_of::<u32>();
    let all = AccessFlags::LOCAL_READ
        | AccessFlags::LOCAL_WRITE
        | AccessFlags::REMOTE_READ
        | AccessFlags::REMOTE_WRITE;

    let ctxs: Vec<_> = socks.iter().map(|s| Context::open(s).unwrap()).collect();
    let pds: Vec<_> = ctxs.iter().map(Context::alloc_pd).collect();
    let cqs: Vec<_> = ctxs.iter().map(|c| c.create_cq().unwrap()).collect();
    let listeners: Vec<_> = pds.iter().map(|p| p.listen().unwrap()).collect();
    let handles: Vec<_> = listeners.iter().map(|l| l.handle()).collect();

    // Connect ring: rank r sends to (r + 1) % 3, receives from (r + 2) % 3.
    let mut send_qps = Vec::new();
    let mut recv_qps = Vec::new();
    for r in 0..n_ranks {
        let next = (r + 1) % n_ranks;
        let (sqp, rqp) = std::thread::scope(|s| {
            let acceptor = s.spawn(|| {
                listeners[next]
                    .accept(&cqs[next], &cqs[next], Duration::from_secs(5))
                    .unwrap()
            });
            let sqp = pds[r].connect(&handles[next], &cqs[r], &cqs[r]).unwrap();
            let rqp = acceptor.join().unwrap();
            (sqp, rqp)
        });
        send_qps.push(sqp);
        recv_qps.push(rqp);
    }
    // Reorder recv_qps so recv_qps[r] is the QP owned by rank r receiving from (r + 2) % 3.
    recv_qps.rotate_right(1);

    let send_mrs: Vec<_> = pds
        .iter()
        .map(|p| p.alloc_mr(bytes, all).unwrap())
        .collect();
    let recv_mrs: Vec<_> = pds
        .iter()
        .map(|p| p.alloc_mr(bytes, all).unwrap())
        .collect();
    let dummy_mrs: Vec<_> = pds.iter().map(|p| p.alloc_mr(16, all).unwrap()).collect();

    // Rank r starts with value (r + 1) in every element; expected SUM = 1 + 2 + 3 = 6.
    let mut accum: Vec<Vec<u32>> = (0..n_ranks).map(|r| vec![(r + 1) as u32; elems]).collect();
    let mut forward = accum.clone();

    for step in 0..(n_ranks - 1) {
        for r in 0..n_ranks {
            let raw: Vec<u8> = forward[r].iter().flat_map(|v| v.to_le_bytes()).collect();
            send_mrs[r].write_bytes(0, &raw).unwrap();
            recv_qps[r]
                .post_recv(step as u64 * 10 + 1, &dummy_mrs[r], 0..16, 0)
                .unwrap();
        }
        for r in 0..n_ranks {
            let next = (r + 1) % n_ranks;
            send_qps[r]
                .post_rdma_write_imm(
                    step as u64 * 10 + 2,
                    &send_mrs[r],
                    0..bytes,
                    recv_mrs[next].rkey(),
                    0,
                    step as u64,
                )
                .unwrap();
        }
        for r in 0..n_ranks {
            let mut got_send = false;
            let mut got_recv = false;
            while !(got_send && got_recv) {
                let wc = cqs[r].poll_blocking(Duration::from_secs(5)).unwrap();
                assert_eq!(wc.status, WcStatus::Success);
                match wc.opcode {
                    WcOpcode::RdmaWrite => got_send = true,
                    WcOpcode::RecvRdmaWithImm => {
                        assert_eq!(wc.tag_or_imm, step as u64);
                        got_recv = true;
                    }
                    other => panic!("unexpected opcode: {other:?}"),
                }
            }
            let mut raw = vec![0u8; bytes];
            recv_mrs[r].read_bytes(0, &mut raw).unwrap();
            let incoming: Vec<u32> = raw
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| u32::from_le_bytes(*c))
                .collect();
            for i in 0..elems {
                accum[r][i] += incoming[i];
            }
            forward[r] = incoming;
        }
    }

    for (r, vals) in accum.iter().enumerate() {
        assert!(
            vals.iter().all(|&v| v == 6),
            "rank {r} expected all 6s, got {:?}",
            &vals[..4]
        );
    }
    println!(
        "ring_allreduce OK: 3 ranks reduced {elems} u32 elements (sum=6) via one-sided RDMA_WRITE_WITH_IMM"
    );

    for mut p in procs {
        let _ = p.kill();
        let _ = p.wait();
    }
    let _ = fs::remove_dir_all(&dir);
}
