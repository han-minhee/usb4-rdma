use std::sync::atomic::{AtomicBool, Ordering};

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn stop(_: libc::c_int) {
    // Only a lock-free atomic store runs in signal context; no I/O or allocation.
    STOP.store(true, Ordering::Relaxed);
}

pub fn install() -> std::io::Result<()> {
    for signal in [libc::SIGTERM, libc::SIGINT] {
        // SAFETY: static C-ABI handler remains live for the process lifetime and
        // only stores to a lock-free AtomicBool. No borrowed memory is retained.
        if unsafe { libc::signal(signal, stop as *const () as libc::sighandler_t) } == libc::SIG_ERR
        {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

pub fn stopping() -> bool {
    STOP.load(Ordering::Relaxed)
}
