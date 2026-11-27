use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DuplexTiming {
    TransferCompletion,
    TransferAndRemoteCrc,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyResult {
    pub transport: String,
    pub op: String,
    pub size_bytes: usize,
    pub iters: usize,
    pub min_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub mean_us: f64,
    pub rdma_tx_bytes_delta: u64,
    pub rdma_rx_bytes_delta: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BandwidthResult {
    pub transport: String,
    pub op: String,
    pub size_bytes: usize,
    pub iters: usize,
    pub total_bytes: u64,
    pub elapsed_secs: f64,
    pub gbytes_per_sec: f64,
    pub gib_per_sec: f64,
    pub gbits_per_sec: f64,
    pub verified_crc32: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duplex_timing: Option<DuplexTiming>,
    pub rdma_tx_bytes_delta: u64,
    pub rdma_rx_bytes_delta: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AllReduceResult {
    pub transport: String,
    pub elems: usize,
    pub bytes: usize,
    pub iters: usize,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub algbw_gbytes_per_sec: f64,
    pub rdma_tx_bytes_delta: u64,
    pub rdma_rx_bytes_delta: u64,
}

#[derive(Debug, Serialize)]
pub struct BenchReport {
    pub peer: String,
    pub rdma_socket: Option<String>,
    pub latency: Vec<LatencyResult>,
    pub bandwidth: Vec<BandwidthResult>,
    pub allreduce: Vec<AllReduceResult>,
}

pub fn summarize_latency(samples_us: &mut [f64]) -> (f64, f64, f64, f64, f64) {
    assert!(!samples_us.is_empty());
    samples_us.sort_by(|a, b| a.total_cmp(b));
    let n = samples_us.len();
    let min = samples_us[0];
    let p50 = samples_us[n / 2];
    let p95 = samples_us[(n * 95 / 100).min(n - 1)];
    let p99 = samples_us[(n * 99 / 100).min(n - 1)];
    let mean = samples_us.iter().sum::<f64>() / n as f64;
    (min, p50, p95, p99, mean)
}

pub fn print_report(report: &BenchReport) {
    if !report.latency.is_empty() {
        println!(
            "\n=== Latency (us, half-RTT for ping-pong; full completion RTT for one-sided) ==="
        );
        println!(
            "{:<10} {:<16} {:>10} {:>9} {:>9} {:>9} {:>10} {:>12}",
            "Transport", "Op", "Size(B)", "p50(us)", "p95(us)", "p99(us)", "Mean(us)", "RDMA_Bytes"
        );
        for r in &report.latency {
            println!(
                "{:<10} {:<16} {:>10} {:>9.1} {:>9.1} {:>9.1} {:>10.1} {:>12}",
                r.transport,
                r.op,
                r.size_bytes,
                r.p50_us,
                r.p95_us,
                r.p99_us,
                r.mean_us,
                r.rdma_tx_bytes_delta + r.rdma_rx_bytes_delta
            );
        }
    }

    if !report.bandwidth.is_empty() {
        println!("\n=== Bandwidth / Goodput ===");
        println!(
            "{:<10} {:<16} {:>10} {:>10} {:>10} {:>10} {:>14}",
            "Transport", "Op", "Size(B)", "GB/s", "GiB/s", "Gb/s", "RDMA_Bytes"
        );
        for r in &report.bandwidth {
            println!(
                "{:<10} {:<16} {:>10} {:>10.3} {:>10.3} {:>10.2} {:>14}",
                r.transport,
                r.op,
                r.size_bytes,
                r.gbytes_per_sec,
                r.gib_per_sec,
                r.gbits_per_sec,
                r.rdma_tx_bytes_delta + r.rdma_rx_bytes_delta
            );
        }
        if report
            .bandwidth
            .iter()
            .any(|row| row.duplex_timing == Some(DuplexTiming::TransferAndRemoteCrc))
        {
            println!("RDMA duplex timing includes remote CRC verification with this peer.");
        }
    }

    if !report.allreduce.is_empty() {
        println!("\n=== 2-Node Host f32 SUM AllReduce ===");
        println!(
            "{:<10} {:>10} {:>10} {:>9} {:>9} {:>11} {:>14}",
            "Transport", "Elems", "Bytes", "p50(us)", "p99(us)", "AlgBW(GB/s)", "RDMA_Bytes"
        );
        for r in &report.allreduce {
            println!(
                "{:<10} {:>10} {:>10} {:>9.1} {:>9.1} {:>11.3} {:>14}",
                r.transport,
                r.elems,
                r.bytes,
                r.p50_us,
                r.p99_us,
                r.algbw_gbytes_per_sec,
                r.rdma_tx_bytes_delta + r.rdma_rx_bytes_delta
            );
        }
    }
}
