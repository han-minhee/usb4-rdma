use anyhow::{Result, ensure};
use std::{
    io::{Read, Write},
    path::Path,
    time::{Duration, Instant},
};
use usb4_stream_hal::{configfs, stream::open_device};

pub fn run(
    endpoint: &Path,
    bytes: u64,
    receive: bool,
    busy_poll: bool,
    timeout: Duration,
) -> Result<()> {
    ensure!(
        bytes > 0 && !timeout.is_zero(),
        "a positive byte count and timeout are required"
    );
    let device = configfs::map_device_with_busy_poll(endpoint, Path::new("/dev"), busy_poll)?;
    let mut io = open_device(&device)?;
    // Use a repeatable pattern whose period divides the I/O slab size.
    let expected: Vec<u8> = (0..256 * 1024)
        .map(|i| (i as u8).wrapping_mul(31))
        .collect();
    let mut received = vec![0; expected.len()];
    let start = Instant::now();
    let mut done = 0;
    while done < bytes {
        ensure!(
            start.elapsed() < timeout,
            "raw benchmark timed out at {done} bytes"
        );
        let offset = (done % expected.len() as u64) as usize;
        let count = (bytes - done).min((expected.len() - offset) as u64) as usize;
        let result = if receive {
            io.read(&mut received[..count])
        } else {
            io.write(&expected[offset..offset + count])
        };
        match result {
            Ok(0) => anyhow::bail!("USB4 stream closed at byte {done}"),
            Ok(n) => {
                ensure!(
                    !receive || received[..n] == expected[offset..offset + n],
                    "payload mismatch at byte {done}"
                );
                done += n as u64;
            }
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                std::thread::yield_now()
            }
            Err(e) => return Err(e.into()),
        }
    }
    println!(
        "{}",
        serde_json::json!({"bytes": done, "receive": receive, "gbps": done as f64 * 8e-9 / start.elapsed().as_secs_f64(), "validated_payload": receive})
    );
    Ok(())
}
