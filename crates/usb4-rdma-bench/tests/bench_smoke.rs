//! Smoke test verifying `usb4-rdma-bench` end-to-end against two emulated daemons + loopback TCP.

use std::{
    fs,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use usb4_rdma::Context;

fn find_daemon_bin() -> PathBuf {
    let bench = PathBuf::from(env!("CARGO_BIN_EXE_usb4-rdma-bench"));
    let daemon = bench.parent().unwrap().join("usb4-rdma-daemon");
    assert!(
        daemon.exists(),
        "build usb4-rdma-daemon before running bench smoke test"
    );
    daemon
}

#[test]
fn bench_client_server_smoke_emulated_and_tcp() {
    let dir = std::env::temp_dir().join(format!("u4rd-bench-smoke-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("emu")).unwrap();
    let cfg = dir.join("cluster.toml");
    fs::write(
        &cfg,
        r#"
epoch = 303
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

    let daemon = find_daemon_bin();
    let bench = PathBuf::from(env!("CARGO_BIN_EXE_usb4-rdma-bench"));
    let mut procs: Vec<Child> = Vec::new();
    let mut socks = Vec::new();

    for node in [0u32, 1] {
        let sock = dir.join(format!("n{node}.sock"));
        let child = Command::new(&daemon)
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
            assert!(Instant::now() < deadline, "daemon did not become ready");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let addr = format!("127.0.0.1:{port}");

    let mut srv = Command::new(&bench)
        .args([
            "server",
            "--bind",
            &addr,
            "--rdma-socket",
            socks[1].to_str().unwrap(),
            "--mr-bytes",
            "4194304",
            "--once",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn bench server");

    std::thread::sleep(Duration::from_millis(200));

    let json_path = dir.join("out.json");
    let status = Command::new(&bench)
        .args([
            "client",
            "--peer",
            &addr,
            "--rdma-socket",
            socks[0].to_str().unwrap(),
            "--suite",
            "all",
            "--only",
            "both",
            "--lat-sizes",
            "64,4096",
            "--bw-sizes",
            "65536",
            "--allreduce-elems",
            "4096",
            "--lat-iters",
            "8",
            "--warmup",
            "2",
            "--bw-total-bytes",
            "524288",
            "--mr-bytes",
            "4194304",
            "--json",
            json_path.to_str().unwrap(),
        ])
        .status()
        .expect("run bench client");

    let srv_status = srv.wait().unwrap();
    for mut p in procs {
        let _ = p.kill();
        let _ = p.wait();
    }

    assert!(status.success(), "bench client failed");
    assert!(srv_status.success(), "bench server failed");
    let report: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&json_path).unwrap()).unwrap();
    assert_eq!(report["latency"].as_array().unwrap().len(), 8);
    assert_eq!(report["bandwidth"].as_array().unwrap().len(), 5);
    let duplex = report["bandwidth"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["op"] == "rdma_duplex")
        .unwrap();
    assert_eq!(duplex["total_bytes"], 1_048_576);
    assert_eq!(duplex["rdma_tx_bytes_delta"], 524_288);
    assert_eq!(duplex["rdma_rx_bytes_delta"], 524_288);
    assert_eq!(report["allreduce"].as_array().unwrap().len(), 2);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn client_rejects_invalid_sizes_before_connecting() {
    for (flag, value) in [
        ("--bw-sizes", "0"),
        ("--lat-sizes", ""),
        ("--lat-iters", "0"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_usb4-rdma-bench"))
            .args(["client", "--peer", "127.0.0.1:1", flag, value])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("positive"));
    }
}
