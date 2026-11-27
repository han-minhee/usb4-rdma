//! Self-contained example demonstrating one-sided RDMA_WRITE, RDMA_READ, and SEND/RECV
//! across two emulated `usb4-rdma-ctl` nodes.

use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use usb4_rdma_core::{AccessFlags, Context, WcStatus};

fn find_daemon_bin() -> PathBuf {
    let exe = std::env::current_exe().expect("current_exe");
    // Cargo places examples in target/<profile>/examples/<name>
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
    let dir = std::env::temp_dir().join(format!("u4rd-ex-wr-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("emu")).unwrap();
    let cfg = dir.join("cluster.toml");
    fs::write(
        &cfg,
        r#"
epoch = 11
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
"#,
    )
    .unwrap();

    let bin = find_daemon_bin();
    let mut procs: Vec<Child> = Vec::new();
    let mut socks = Vec::new();
    for node in [0u32, 1] {
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

    let ctx0 = Context::open(&socks[0]).unwrap();
    let ctx1 = Context::open(&socks[1]).unwrap();
    let pd0 = ctx0.alloc_pd();
    let pd1 = ctx1.alloc_pd();
    let cq0 = ctx0.create_cq().unwrap();
    let cq1 = ctx1.create_cq().unwrap();

    let listener1 = pd1.listen().unwrap();
    let handle1 = listener1.handle();
    let t = std::thread::spawn(move || {
        listener1
            .accept(&cq1, &cq1, Duration::from_secs(5))
            .unwrap()
    });
    let qp0 = pd0.connect(&handle1, &cq0, &cq0).unwrap();
    let _qp1 = t.join().unwrap();

    let all = AccessFlags::LOCAL_READ
        | AccessFlags::LOCAL_WRITE
        | AccessFlags::REMOTE_READ
        | AccessFlags::REMOTE_WRITE;
    let mr0 = pd0.alloc_mr(65536, all).unwrap();
    let mr1 = pd1.alloc_mr(65536, all).unwrap();

    let msg = b"Generic USB4 RDMA one-sided write & read verified!";
    mr0.write_bytes(0, msg).unwrap();

    qp0.post_rdma_write(1, &mr0, 0..msg.len(), mr1.rkey(), 128)
        .unwrap();
    let wc = cq0.poll_blocking(Duration::from_secs(5)).unwrap();
    assert_eq!(wc.status, WcStatus::Success);

    mr0.write_bytes(0, &vec![0u8; msg.len()]).unwrap();
    qp0.post_rdma_read(2, &mr0, 0..msg.len(), mr1.rkey(), 128)
        .unwrap();
    let wc = cq0.poll_blocking(Duration::from_secs(5)).unwrap();
    assert_eq!(wc.status, WcStatus::Success);

    let mut out = vec![0u8; msg.len()];
    mr0.read_bytes(0, &mut out).unwrap();
    assert_eq!(&out, msg);
    println!("rdma_write_read OK: {}", String::from_utf8_lossy(&out));

    for mut p in procs {
        let _ = p.kill();
        let _ = p.wait();
    }
    let _ = fs::remove_dir_all(&dir);
}
