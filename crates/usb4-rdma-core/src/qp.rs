//! Listener and full-duplex Reliable Connected QueuePair (`SEND`, `RECV`, `RDMA_WRITE`, `RDMA_READ`).

use crate::ipc::*;
use crate::{
    Error, NodeId, Result,
    rdma::{AccessFlags, EndpointHandle},
};
use crate::{
    context::{CompletionQueue, ContextInner, LocalTarget, OpKind, PendingOp},
    mr::{BorrowedMr, MrRef, OwnedMr},
};
use std::{
    collections::VecDeque,
    ops::Range,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Passive endpoint accepting incoming `QueuePair` connections.
pub struct Listener {
    inner: Arc<Mutex<ContextInner>>,
    listener_id: u64,
    handle: EndpointHandle,
    closed: bool,
}

impl Listener {
    pub(crate) fn new(inner: Arc<Mutex<ContextInner>>) -> Result<Self> {
        let (listener_id, handle) = {
            let mut st = inner.lock().map_err(|_| Error::Disconnected)?;
            let low = st.next_listener_low;
            st.next_listener_low = low.checked_add(1).ok_or(Error::Exhausted)?;
            let listener_id = (st.client.welcome.client << 32) | (low as u64);
            let slot = st
                .client
                .post(
                    Descriptor {
                        op: OP_LISTEN,
                        listener: listener_id,
                        ..Default::default()
                    },
                    &[],
                )?
                .ok_or(Error::Exhausted)?;
            let start = Instant::now();
            while !matches!(st.client.state(slot)?, COMPLETE | FAILED) {
                if start.elapsed() > Duration::from_secs(5) {
                    return Err(Error::Timeout);
                }
                st.client.alive()?;
                std::thread::yield_now();
            }
            let d = st.client.finish(slot)?;
            if d.status != STATUS_OK {
                return Err(Error::RemoteError(d.status));
            }
            let w = st.client.welcome;
            (
                listener_id,
                EndpointHandle {
                    node_id: w.node,
                    listener_id,
                    epoch: w.epoch,
                },
            )
        };
        Ok(Self {
            inner,
            listener_id,
            handle,
            closed: false,
        })
    }

    pub fn handle(&self) -> EndpointHandle {
        self.handle
    }

    /// Post an accept without waiting for the peer.
    pub fn begin_accept(
        &self,
        send_cq: &CompletionQueue,
        recv_cq: &CompletionQueue,
    ) -> Result<ConnectionRequest> {
        ConnectionRequest::post(
            self.inner.clone(),
            Descriptor {
                op: OP_ACCEPT,
                listener: self.listener_id,
                ..Default::default()
            },
            send_cq,
            recv_cq,
        )
    }

    /// Accept one incoming connection within the specified deadline.
    pub fn accept(
        &self,
        send_cq: &CompletionQueue,
        recv_cq: &CompletionQueue,
        timeout: Duration,
    ) -> Result<QueuePair> {
        self.begin_accept(send_cq, recv_cq)?.wait(timeout)
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        if let Ok(mut st) = self.inner.lock()
            && let Ok(Some(slot)) = st.client.post(
                Descriptor {
                    op: OP_CLOSE,
                    listener: self.listener_id,
                    flags: 2,
                    ..Default::default()
                },
                &[],
            )
        {
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(1) {
                if matches!(st.client.state(slot), Ok(COMPLETE | FAILED)) {
                    let _ = st.client.finish(slot);
                    break;
                }
                std::thread::yield_now();
            }
        }
    }
}

/// An asynchronous connection or accept operation.
///
/// Poll this request to completion. If it cannot be completed, close the
/// owning context to retire its daemon-side control slot.
pub struct ConnectionRequest {
    inner: Arc<Mutex<ContextInner>>,
    slot: Option<usize>,
    send_cq: u64,
    recv_cq: u64,
}

impl ConnectionRequest {
    pub(crate) fn post(
        inner: Arc<Mutex<ContextInner>>,
        descriptor: Descriptor,
        send_cq: &CompletionQueue,
        recv_cq: &CompletionQueue,
    ) -> Result<Self> {
        if !Arc::ptr_eq(&inner, &send_cq.inner) || !Arc::ptr_eq(&inner, &recv_cq.inner) {
            return Err(Error::InvalidArg(
                "connection completion queues belong to another context",
            ));
        }
        let slot = {
            let mut state = inner.lock().map_err(|_| Error::Disconnected)?;
            state.client.alive()?;
            state
                .client
                .post(descriptor, &[])?
                .ok_or(Error::Exhausted)?
        };
        Ok(Self {
            inner,
            slot: Some(slot),
            send_cq: send_cq.id,
            recv_cq: recv_cq.id,
        })
    }

    pub fn poll(&mut self) -> Result<Option<QueuePair>> {
        let slot = self.slot.ok_or(Error::Stale)?;
        let mut state = self.inner.lock().map_err(|_| Error::Disconnected)?;
        state.client.alive()?;
        if !matches!(state.client.state(slot)?, COMPLETE | FAILED) {
            return Ok(None);
        }
        let result = state.client.finish(slot)?;
        self.slot = None;
        if result.status != STATUS_OK {
            return Err(Error::RemoteError(result.status));
        }
        Ok(Some(QueuePair {
            inner: self.inner.clone(),
            peer: result.peer,
            connection: result.connection,
            send_cq: self.send_cq,
            recv_cq: self.recv_cq,
            closed: false,
        }))
    }

    pub(crate) fn wait(mut self, timeout: Duration) -> Result<QueuePair> {
        let start = Instant::now();
        loop {
            if let Some(pair) = self.poll()? {
                return Ok(pair);
            }
            if start.elapsed() >= timeout {
                return Err(Error::Timeout);
            }
            std::thread::sleep(Duration::from_micros(100));
        }
    }
}

/// Full-duplex Reliable Connected Queue Pair supporting both two-sided and one-sided RDMA.
pub struct QueuePair {
    inner: Arc<Mutex<ContextInner>>,
    peer: NodeId,
    connection: u64,
    send_cq: u64,
    recv_cq: u64,
    closed: bool,
}

/// Internal trait unifying address/MR-id resolution for `OwnedMr` and `BorrowedMr`.
pub trait LocalMr: MrRef {
    fn raw_addr(&self) -> usize;
}

impl LocalMr for OwnedMr {
    fn raw_addr(&self) -> usize {
        self.as_ptr() as usize
    }
}

impl LocalMr for BorrowedMr {
    fn raw_addr(&self) -> usize {
        self.as_ptr() as usize
    }
}

impl QueuePair {
    pub fn id(&self) -> u64 {
        self.connection
    }

    pub fn peer_node(&self) -> NodeId {
        self.peer
    }

    fn validate_range<M: LocalMr>(mr: &M, range: &Range<usize>, need_write: bool) -> Result<usize> {
        if range.start > range.end || range.end > mr.len() {
            return Err(Error::InvalidArg("MR subrange out of bounds"));
        }
        if need_write && !mr.access().contains(AccessFlags::LOCAL_WRITE) {
            return Err(Error::AccessViolation);
        }
        if !need_write
            && !mr.access().contains(AccessFlags::LOCAL_READ)
            && !mr.access().contains(AccessFlags::LOCAL_WRITE)
        {
            return Err(Error::AccessViolation);
        }
        Ok(range.end - range.start)
    }

    /// Post a two-sided tagged `SEND` from `mr[range]`.
    pub fn post_send<M: LocalMr>(
        &self,
        wr_id: u64,
        mr: &M,
        range: Range<usize>,
        tag: u64,
    ) -> Result<()> {
        let len = Self::validate_range(mr, &range, false)?;
        let target = if mr.daemon_mr_id() != 0 {
            LocalTarget::SharedMr {
                mr_id: mr.daemon_mr_id(),
                base_offset: range.start as u64,
            }
        } else {
            LocalTarget::RawPtr {
                addr: mr.raw_addr() + range.start,
            }
        };
        let mut st = self.inner.lock().map_err(|_| Error::Disconnected)?;
        let id = st.next_op_id;
        st.next_op_id = id.checked_add(1).ok_or(Error::Exhausted)?;
        let seq = {
            let e = st.qp_send_seq.entry(self.connection).or_insert(0);
            *e += 1;
            *e
        };
        let wire_request = st.alloc_wire_request();
        st.enqueue_op(PendingOp {
            id,
            wr_id,
            cq_id: self.send_cq,
            qp_id: self.connection,
            peer: self.peer,
            connection: self.connection,
            kind: OpKind::Send { tag, seq },
            target,
            total_len: len,
            cursor: 0,
            completed: 0,
            current_window: 0,
            wire_request,
            slot: None,
            in_flight: VecDeque::new(),
            posted_zero: false,
            err_status: None,
            started: Instant::now(),
        })
    }

    /// Post a two-sided tagged `RECV` into `mr[range]` (also receives `RDMA_WRITE_WITH_IMM`).
    pub fn post_recv<M: LocalMr>(
        &self,
        wr_id: u64,
        mr: &M,
        range: Range<usize>,
        tag: u64,
    ) -> Result<()> {
        self.post_recv_inner(wr_id, mr, range, tag, false)
    }

    /// Post a receive that can wait indefinitely for a match. The transfer remains
    /// bounded after it begins, and a disconnect completes the receive with an error.
    pub fn post_standing_recv<M: LocalMr>(
        &self,
        wr_id: u64,
        mr: &M,
        range: Range<usize>,
        tag: u64,
    ) -> Result<()> {
        self.post_recv_inner(wr_id, mr, range, tag, true)
    }

    fn post_recv_inner<M: LocalMr>(
        &self,
        wr_id: u64,
        mr: &M,
        range: Range<usize>,
        tag: u64,
        standing: bool,
    ) -> Result<()> {
        let len = Self::validate_range(mr, &range, true)?;
        let target = LocalTarget::RawPtr {
            addr: mr.raw_addr() + range.start,
        };
        let mut st = self.inner.lock().map_err(|_| Error::Disconnected)?;
        let id = st.next_op_id;
        st.next_op_id = id.checked_add(1).ok_or(Error::Exhausted)?;
        st.enqueue_op(PendingOp {
            id,
            wr_id,
            cq_id: self.recv_cq,
            qp_id: self.connection,
            peer: self.peer,
            connection: self.connection,
            kind: OpKind::Recv {
                tag,
                standing,
                mr_id: if standing { mr.daemon_mr_id() } else { 0 },
                mr_offset: range.start as u64,
            },
            target,
            total_len: len,
            cursor: 0,
            completed: 0,
            current_window: 0,
            wire_request: 0,
            slot: None,
            in_flight: VecDeque::new(),
            posted_zero: false,
            err_status: None,
            started: Instant::now(),
        })
    }

    /// Advisory non-blocking readiness probe for a queued incoming `SEND` or `RDMA_WRITE_WITH_IMM`.
    pub fn peek_recv(&self, tag: u64) -> Result<bool> {
        let mut st = self.inner.lock().map_err(|_| Error::Disconnected)?;
        let slot = st
            .client
            .post(
                Descriptor {
                    op: OP_PEEK,
                    peer: self.peer,
                    connection: self.connection,
                    tag_or_imm: tag,
                    ..Default::default()
                },
                &[],
            )?
            .ok_or(Error::Exhausted)?;
        let start = Instant::now();
        while !matches!(st.client.state(slot)?, COMPLETE | FAILED) {
            if start.elapsed() > Duration::from_secs(3) {
                return Err(Error::Timeout);
            }
            st.client.alive()?;
            std::thread::yield_now();
        }
        let d = st.client.finish(slot)?;
        if d.status == STATUS_CONNECTION_CLOSED {
            return Err(Error::Disconnected);
        }
        if d.status != STATUS_OK {
            return Err(Error::RemoteError(d.status));
        }
        Ok(d.flags != 0)
    }

    /// Post a one-sided `RDMA_WRITE` from local `mr[range]` into remote `(rkey, remote_offset)`.
    /// Requires zero target-application involvement on the remote node.
    pub fn post_rdma_write<M: LocalMr>(
        &self,
        wr_id: u64,
        mr: &M,
        range: Range<usize>,
        rkey: u64,
        remote_offset: u64,
    ) -> Result<()> {
        self.post_rdma_write_internal(wr_id, mr, range, rkey, remote_offset, None)
    }

    /// Post a one-sided `RDMA_WRITE_WITH_IMM` writing `mr[range]` to remote `(rkey, remote_offset)`
    /// and delivering `imm_data` to a posted `RECV` on the remote QP's receive CQ.
    pub fn post_rdma_write_imm<M: LocalMr>(
        &self,
        wr_id: u64,
        mr: &M,
        range: Range<usize>,
        rkey: u64,
        remote_offset: u64,
        imm_data: u64,
    ) -> Result<()> {
        self.post_rdma_write_internal(wr_id, mr, range, rkey, remote_offset, Some(imm_data))
    }

    fn post_rdma_write_internal<M: LocalMr>(
        &self,
        wr_id: u64,
        mr: &M,
        range: Range<usize>,
        rkey: u64,
        remote_offset: u64,
        imm: Option<u64>,
    ) -> Result<()> {
        let len = Self::validate_range(mr, &range, false)?;
        let target = if mr.daemon_mr_id() != 0 {
            LocalTarget::SharedMr {
                mr_id: mr.daemon_mr_id(),
                base_offset: range.start as u64,
            }
        } else {
            LocalTarget::RawPtr {
                addr: mr.raw_addr() + range.start,
            }
        };
        let mut st = self.inner.lock().map_err(|_| Error::Disconnected)?;
        let id = st.next_op_id;
        st.next_op_id = id.checked_add(1).ok_or(Error::Exhausted)?;
        st.enqueue_op(PendingOp {
            id,
            wr_id,
            cq_id: self.send_cq,
            qp_id: self.connection,
            peer: self.peer,
            connection: self.connection,
            kind: OpKind::RdmaWrite {
                rkey,
                remote_offset,
                imm,
            },
            target,
            total_len: len,
            cursor: 0,
            completed: 0,
            current_window: 0,
            wire_request: 0,
            slot: None,
            in_flight: VecDeque::new(),
            posted_zero: false,
            err_status: None,
            started: Instant::now(),
        })
    }

    /// Post a one-sided `RDMA_READ` pulling bytes from remote `(rkey, remote_offset)` into local `mr[range]`.
    /// Requires zero target-application involvement on the remote node.
    pub fn post_rdma_read<M: LocalMr>(
        &self,
        wr_id: u64,
        mr: &M,
        range: Range<usize>,
        rkey: u64,
        remote_offset: u64,
    ) -> Result<()> {
        let len = Self::validate_range(mr, &range, true)?;
        let target = if mr.daemon_mr_id() != 0 {
            LocalTarget::SharedMr {
                mr_id: mr.daemon_mr_id(),
                base_offset: range.start as u64,
            }
        } else {
            LocalTarget::RawPtr {
                addr: mr.raw_addr() + range.start,
            }
        };
        let mut st = self.inner.lock().map_err(|_| Error::Disconnected)?;
        let id = st.next_op_id;
        st.next_op_id = id.checked_add(1).ok_or(Error::Exhausted)?;
        st.enqueue_op(PendingOp {
            id,
            wr_id,
            cq_id: self.send_cq,
            qp_id: self.connection,
            peer: self.peer,
            connection: self.connection,
            kind: OpKind::RdmaRead {
                rkey,
                remote_offset,
            },
            target,
            total_len: len,
            cursor: 0,
            completed: 0,
            current_window: 0,
            wire_request: 0,
            slot: None,
            in_flight: VecDeque::new(),
            posted_zero: false,
            err_status: None,
            started: Instant::now(),
        })
    }
}

impl Drop for QueuePair {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        if let Ok(mut st) = self.inner.lock()
            && let Ok(Some(slot)) = st.client.post(
                Descriptor {
                    op: OP_CLOSE,
                    peer: self.peer,
                    connection: self.connection,
                    ..Default::default()
                },
                &[],
            )
        {
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(1) {
                if matches!(st.client.state(slot), Ok(COMPLETE | FAILED)) {
                    let _ = st.client.finish(slot);
                    break;
                }
                std::thread::yield_now();
            }
        }
    }
}
