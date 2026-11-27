//! Synchronous ggml-rpc byte stream over one ordered virtual queue pair.

use std::{
    ffi::{CStr, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    ptr,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};
use usb4_rdma_core::{
    AccessFlags, CompletionQueue, Context, EndpointHandle, Error, Listener, OwnedMr, QueuePair,
    Result, WcOpcode, WcStatus,
};

pub const USB4_RPC_HANDLE_BYTES: usize = 20;
const _: () = assert!(USB4_RPC_HANDLE_BYTES == EndpointHandle::SIZE);
const WINDOW: usize = usb4_rdma_core::ipc::WINDOW;
const TIMEOUT: Duration = Duration::from_secs(30);
const IDLE_POLL: Duration = Duration::from_micros(200);
const STREAM_TAG: u64 = 1;
const SEND_WR: u64 = 1;
const RECV_WR: u64 = 2;

pub struct Usb4RpcListener {
    listener: Listener,
    context: Context,
}

pub struct Usb4RpcConnection(Arc<Connection>);

pub struct Usb4RpcTransaction {
    connection: Arc<Connection>,
    committed: bool,
}

struct Connection {
    stream: Mutex<Stream>,
    transaction: Mutex<TransactionState>,
    available: Condvar,
}

#[derive(Default)]
struct TransactionState {
    busy: bool,
    failed: bool,
}

struct Stream {
    qp: QueuePair,
    tx: CompletionQueue,
    rx: CompletionQueue,
    tx_mr: OwnedMr,
    rx_mr: OwnedMr,
    staged: usize,
    received: usize,
    consumed: usize,
    failed: bool,
}

impl Stream {
    fn submit(&mut self) -> Result<()> {
        if self.staged == 0 {
            return Ok(());
        }
        self.qp
            .post_send(SEND_WR, &self.tx_mr, 0..self.staged, STREAM_TAG)?;
        let completion = self.tx.poll_blocking(TIMEOUT)?;
        if completion.status != WcStatus::Success {
            return Err(Error::Disconnected);
        }
        if completion.wr_id != SEND_WR
            || completion.opcode != WcOpcode::Send
            || completion.qp_id != self.qp.id()
            || completion.byte_len != self.staged as u64
        {
            return Err(Error::Protocol("unexpected RPC send completion"));
        }
        self.staged = 0;
        Ok(())
    }

    fn stage(&mut self, mut data: &[u8]) -> Result<()> {
        while !data.is_empty() {
            if self.staged == WINDOW {
                self.submit()?;
            }
            let take = data.len().min(WINDOW - self.staged);
            self.tx_mr.write_bytes(self.staged, &data[..take])?;
            self.staged += take;
            data = &data[take..];
        }
        Ok(())
    }

    fn receive(&mut self, mut data: &mut [u8], mut allow_idle: bool) -> Result<()> {
        while !data.is_empty() {
            if self.received == self.consumed {
                self.qp
                    .post_standing_recv(RECV_WR, &self.rx_mr, 0..WINDOW, STREAM_TAG)?;
                let completion = if allow_idle {
                    // The daemon starts its transfer deadline when this receive matches.
                    loop {
                        if let Some(completion) = self.rx.poll_one()? {
                            break completion;
                        }
                        thread::sleep(IDLE_POLL);
                    }
                } else {
                    self.rx.poll_blocking(TIMEOUT)?
                };
                if completion.status != WcStatus::Success {
                    return Err(Error::Disconnected);
                }
                if completion.wr_id != RECV_WR
                    || completion.opcode != WcOpcode::Recv
                    || completion.qp_id != self.qp.id()
                    || completion.tag_or_imm != STREAM_TAG
                    || completion.byte_len == 0
                    || completion.byte_len > WINDOW as u64
                {
                    return Err(Error::Protocol("unexpected RPC receive completion"));
                }
                self.received = completion.byte_len as usize;
                self.consumed = 0;
            }
            allow_idle = false;
            let take = data.len().min(self.received - self.consumed);
            self.rx_mr.read_bytes(self.consumed, &mut data[..take])?;
            self.consumed += take;
            data = &mut data[take..];
        }
        Ok(())
    }
}

impl Usb4RpcConnection {
    fn new(
        context: &Context,
        qp: QueuePair,
        tx: CompletionQueue,
        rx: CompletionQueue,
    ) -> Result<Self> {
        let pd = context.alloc_pd();
        let tx_mr = pd.alloc_mr(WINDOW, AccessFlags::LOCAL_READ | AccessFlags::LOCAL_WRITE)?;
        let rx_mr = pd.alloc_mr(WINDOW, AccessFlags::LOCAL_READ | AccessFlags::LOCAL_WRITE)?;
        Ok(Self(Arc::new(Connection {
            stream: Mutex::new(Stream {
                qp,
                tx,
                rx,
                tx_mr,
                rx_mr,
                staged: 0,
                received: 0,
                consumed: 0,
                failed: false,
            }),
            transaction: Mutex::default(),
            available: Condvar::new(),
        })))
    }
}

impl Connection {
    fn begin(self: &Arc<Self>) -> Result<Usb4RpcTransaction> {
        let state = self.transaction.lock().map_err(|_| Error::Disconnected)?;
        let (mut state, _) = self
            .available
            .wait_timeout_while(state, TIMEOUT, |state| state.busy && !state.failed)
            .map_err(|_| Error::Disconnected)?;
        if state.failed {
            return Err(Error::Disconnected);
        }
        if state.busy {
            return Err(Error::Timeout);
        }
        state.busy = true;
        Ok(Usb4RpcTransaction {
            connection: self.clone(),
            committed: false,
        })
    }
}

impl Usb4RpcTransaction {
    fn use_stream(&self, f: impl FnOnce(&mut Stream) -> Result<()>) -> Result<()> {
        let mut stream = self
            .connection
            .stream
            .lock()
            .map_err(|_| Error::Disconnected)?;
        if stream.failed {
            return Err(Error::Disconnected);
        }
        let result = f(&mut stream);
        if result.is_err() {
            stream.failed = true;
        }
        result
    }
}

impl Drop for Usb4RpcTransaction {
    fn drop(&mut self) {
        let failed = self.connection.stream.lock().map_or(true, |stream| {
            stream.failed || !self.committed || stream.staged != 0
        });
        if let Ok(mut state) = self.connection.transaction.lock() {
            state.failed = failed;
            state.busy = false;
        }
        self.connection.available.notify_all();
    }
}

fn guard<T>(fallback: T, action: impl FnOnce() -> Result<T>) -> T {
    catch_unwind(AssertUnwindSafe(action))
        .ok()
        .and_then(Result::ok)
        .unwrap_or(fallback)
}

unsafe fn open_context(socket: *const c_char) -> Result<Context> {
    if socket.is_null() {
        return Context::open_default();
    }
    // SAFETY: the ABI requires a live NUL-terminated path for the duration of this call.
    let path = unsafe { CStr::from_ptr(socket) };
    let path =
        std::str::from_utf8(path.to_bytes()).map_err(|_| Error::InvalidArg("socket path"))?;
    Context::open(Path::new(path))
}

/// # Safety
/// The socket path is null or a live NUL-terminated path.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_listen(socket: *const c_char) -> *mut Usb4RpcListener {
    guard(ptr::null_mut(), || {
        // SAFETY: this call has the same socket contract as open_context.
        let context = unsafe { open_context(socket) }?;
        let listener = context.alloc_pd().listen()?;
        Ok(Box::into_raw(Box::new(Usb4RpcListener {
            listener,
            context,
        })))
    })
}

/// # Safety
/// `listener` is live and `out` holds `USB4_RPC_HANDLE_BYTES` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_listener_handle(
    listener: *const Usb4RpcListener,
    out: *mut u8,
) -> bool {
    guard(false, || {
        // SAFETY: a non-null listener is a live shared handle under the ABI contract.
        let listener = unsafe { listener.as_ref() }.ok_or(Error::Stale)?;
        if out.is_null() {
            return Err(Error::InvalidArg("null handle output"));
        }
        let bytes = listener.listener.handle().to_bytes();
        // SAFETY: the caller provides the required output size and the source is local.
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), out, bytes.len()) };
        Ok(true)
    })
}

/// # Safety
/// `listener` is a live handle. Only one caller may accept at a time.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_accept(
    listener: *const Usb4RpcListener,
) -> *mut Usb4RpcConnection {
    guard(ptr::null_mut(), || {
        // SAFETY: the caller keeps the listener live for accept.
        let listener = unsafe { listener.as_ref() }.ok_or(Error::Stale)?;
        let tx = listener.context.create_cq()?;
        let rx = listener.context.create_cq()?;
        let qp = listener.listener.accept(&tx, &rx, TIMEOUT)?;
        let conn = Usb4RpcConnection::new(&listener.context, qp, tx, rx)?;
        Ok(Box::into_raw(Box::new(conn)))
    })
}

/// # Safety
/// `handle` holds `USB4_RPC_HANDLE_BYTES` readable bytes and socket follows `usb4_rpc_listen`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_connect(
    socket: *const c_char,
    handle: *const u8,
) -> *mut Usb4RpcConnection {
    guard(ptr::null_mut(), || {
        if handle.is_null() {
            return Err(Error::InvalidArg("null peer handle"));
        }
        // SAFETY: the ABI supplies the fixed-size readable handle and optional socket path.
        let (bytes, context) = unsafe {
            (
                std::slice::from_raw_parts(handle, USB4_RPC_HANDLE_BYTES),
                open_context(socket),
            )
        };
        let context = context?;
        let handle = EndpointHandle::from_bytes(bytes)?;
        let tx = context.create_cq()?;
        let rx = context.create_cq()?;
        let qp = context.alloc_pd().connect(&handle, &tx, &rx)?;
        Ok(Box::into_raw(Box::new(Usb4RpcConnection::new(
            &context, qp, tx, rx,
        )?)))
    })
}

/// # Safety
/// `conn` is live until begin returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_begin(conn: *const Usb4RpcConnection) -> *mut Usb4RpcTransaction {
    guard(ptr::null_mut(), || {
        // SAFETY: a non-null connection handle is live for this call.
        let conn = unsafe { conn.as_ref() }.ok_or(Error::Stale)?;
        Ok(Box::into_raw(Box::new(conn.0.begin()?)))
    })
}

/// # Safety
/// `txn` is live. `data` points to `len` readable bytes, or can be null when `len` is zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_stage_send(
    txn: *const Usb4RpcTransaction,
    data: *const u8,
    len: usize,
) -> bool {
    guard(false, || {
        // SAFETY: a non-null transaction handle is live for this call.
        let txn = unsafe { txn.as_ref() }.ok_or(Error::Stale)?;
        if len == 0 {
            return Ok(true);
        }
        if data.is_null() || len > isize::MAX as usize {
            return Err(Error::InvalidArg("send range"));
        }
        // SAFETY: the caller provides a live readable range with the checked length.
        txn.use_stream(|stream| stream.stage(unsafe { std::slice::from_raw_parts(data, len) }))?;
        Ok(true)
    })
}

/// # Safety
/// `txn` is live for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_flush(txn: *const Usb4RpcTransaction) -> bool {
    guard(false, || {
        // SAFETY: a non-null transaction handle is live for this call.
        let txn = unsafe { txn.as_ref() }.ok_or(Error::Stale)?;
        txn.use_stream(Stream::submit)?;
        Ok(true)
    })
}

/// # Safety
/// `txn` is live. `data` points to `len` exclusively writable bytes, or can be null at length zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_recv_exact(
    txn: *const Usb4RpcTransaction,
    data: *mut u8,
    len: usize,
) -> bool {
    // SAFETY: this entry point has the same handle and buffer contract as receive_raw.
    unsafe { receive_raw(txn, data, len, false) }
}

/// Wait for the first byte of a new request while the peer remains connected.
/// Subsequent reads still use the regular receive deadline.
///
/// # Safety
/// `txn` is live. `data` points to one exclusively writable byte.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_recv_after_idle(
    txn: *const Usb4RpcTransaction,
    data: *mut u8,
) -> bool {
    // SAFETY: the caller provides one writable byte and a live transaction.
    unsafe { receive_raw(txn, data, 1, true) }
}

unsafe fn receive_raw(
    txn: *const Usb4RpcTransaction,
    data: *mut u8,
    len: usize,
    allow_idle: bool,
) -> bool {
    guard(false, || {
        // SAFETY: a non-null transaction handle is live for this call.
        let txn = unsafe { txn.as_ref() }.ok_or(Error::Stale)?;
        if len == 0 {
            return Ok(true);
        }
        if data.is_null() || len > isize::MAX as usize {
            return Err(Error::InvalidArg("receive range"));
        }
        txn.use_stream(|stream| {
            stream.submit()?;
            // SAFETY: the caller retains exclusive access over this writable range.
            stream.receive(
                unsafe { std::slice::from_raw_parts_mut(data, len) },
                allow_idle,
            )
        })?;
        Ok(true)
    })
}

/// # Safety
/// `txn` is null or a uniquely owned live transaction. This consumes it exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_end(txn: *mut Usb4RpcTransaction, commit: bool) -> bool {
    guard(false, || {
        if txn.is_null() {
            return Err(Error::Stale);
        }
        // SAFETY: ownership is transferred exactly once by the ABI contract.
        let mut txn = unsafe { Box::from_raw(txn) };
        if commit {
            txn.use_stream(Stream::submit)?;
            txn.committed = true;
        }
        Ok(true)
    })
}

/// # Safety
/// `conn` is null or a uniquely owned connection handle, consumed exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_close(conn: *mut Usb4RpcConnection) {
    guard((), || {
        if !conn.is_null() {
            // SAFETY: ownership is transferred exactly once by the ABI contract.
            drop(unsafe { Box::from_raw(conn) });
        }
        Ok(())
    });
}

/// # Safety
/// `listener` is null or a uniquely owned listener handle. No accept may be active.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn usb4_rpc_close_listener(listener: *mut Usb4RpcListener) {
    guard((), || {
        if !listener.is_null() {
            // SAFETY: ownership is transferred exactly once by the ABI contract.
            drop(unsafe { Box::from_raw(listener) });
        }
        Ok(())
    });
}
