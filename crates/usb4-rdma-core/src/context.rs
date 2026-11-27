//! Context, ProtectionDomain, CompletionQueue, and multi-window RDMA progress engine.

use crate::ipc::*;
use crate::{
    Error, Result,
    protocol::CHUNK,
    rdma::{AccessFlags, DeviceAttr, EndpointHandle, WcOpcode, WcStatus, WorkCompletion},
};
use crate::{
    mr::{BorrowedMr, OwnedMr},
    qp::{ConnectionRequest, Listener, QueuePair},
};
use std::{
    collections::{BTreeMap, VecDeque},
    os::fd::OwnedFd,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(crate) const MAX_PENDING_OPS: usize = 256;
pub(crate) const DEFAULT_OP_TIMEOUT: Duration = Duration::from_secs(30);
const PIPELINE_WINDOWS: usize = 8;

#[derive(Clone, Debug)]
pub struct FabricStats {
    pub node: u32,
    pub epoch: u64,
    pub ready: bool,
    pub tx_frames: u64,
    pub rx_frames: u64,
    pub forwarded_frames: u64,
    pub tx_payload_bytes: u64,
    pub rx_payload_bytes: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum OpKind {
    Send {
        tag: u64,
        seq: u64,
    },
    Recv {
        tag: u64,
        standing: bool,
        mr_id: u64,
        mr_offset: u64,
    },
    RdmaWrite {
        rkey: u64,
        remote_offset: u64,
        imm: Option<u64>,
    },
    RdmaRead {
        rkey: u64,
        remote_offset: u64,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum LocalTarget {
    SharedMr { mr_id: u64, base_offset: u64 },
    RawPtr { addr: usize },
}

pub(crate) struct PendingOp {
    pub id: u64,
    pub wr_id: u64,
    pub cq_id: u64,
    pub qp_id: u64,
    pub peer: u32,
    pub connection: u64,
    pub kind: OpKind,
    pub target: LocalTarget,
    pub total_len: usize,
    pub cursor: usize,
    pub completed: usize,
    pub current_window: usize,
    pub wire_request: u64,
    pub slot: Option<usize>,
    pub in_flight: VecDeque<(usize, usize, usize)>,
    pub posted_zero: bool,
    pub err_status: Option<i32>,
    pub started: Instant,
}

pub(crate) struct ContextInner {
    pub client: Client,
    pub last_alive: Instant,
    failed: bool,
    pub next_cq_id: u64,
    pub next_listener_low: u32,
    pub next_local_mr_key: u64,
    pub next_wire_request: u64,
    pub next_op_id: u64,
    pub cqs: BTreeMap<u64, VecDeque<WorkCompletion>>,
    pub qp_send_seq: BTreeMap<u64, u64>,
    pub qp_send_active: BTreeMap<u64, u64>,
    pub qp_send_queued: BTreeMap<u64, VecDeque<u64>>,
    pub ops: BTreeMap<u64, PendingOp>,
    finished_ops: Vec<(u64, WorkCompletion)>,
    pub op_timeout: Duration,
}

impl ContextInner {
    pub fn alloc_wire_request(&mut self) -> u64 {
        let low = self.next_wire_request;
        self.next_wire_request = low.wrapping_add(1).max(1);
        ((self.client.welcome.node as u64) << 48)
            | ((self.client.welcome.client & 0xFFFF) << 32)
            | (low & 0xFFFF_FFFF)
    }

    pub fn push_wc(&mut self, cq_id: u64, wc: WorkCompletion) {
        if let Some(q) = self.cqs.get_mut(&cq_id) {
            q.push_back(wc);
        }
    }

    pub fn enqueue_op(&mut self, op: PendingOp) -> Result<()> {
        if self.failed {
            return Err(Error::Disconnected);
        }
        if self.ops.len() >= MAX_PENDING_OPS {
            return Err(Error::Exhausted);
        }
        let id = op.id;
        let qp_id = op.qp_id;
        let ordered_send = matches!(op.kind, OpKind::Send { .. } | OpKind::RdmaWrite { .. });
        self.ops.insert(id, op);
        if ordered_send {
            if let std::collections::btree_map::Entry::Vacant(e) = self.qp_send_active.entry(qp_id)
            {
                e.insert(id);
            } else {
                self.qp_send_queued.entry(qp_id).or_default().push_back(id);
            }
        }
        self.drive_all()
    }

    fn fail_pending(&mut self) {
        self.failed = true;
        self.qp_send_active.clear();
        self.qp_send_queued.clear();
        for (_, op) in std::mem::take(&mut self.ops) {
            self.push_wc(
                op.cq_id,
                WorkCompletion {
                    wr_id: op.wr_id,
                    status: WcStatus::TransportErr,
                    opcode: op_to_wc_opcode(op.kind, false),
                    byte_len: 0,
                    tag_or_imm: 0,
                    has_imm: false,
                    qp_id: op.qp_id,
                },
            );
        }
    }

    /// Drive all active operations across every QP/CQ so out-of-order polling never deadlocks.
    pub fn drive_all(&mut self) -> Result<()> {
        if self.failed {
            return Ok(());
        }
        let now = Instant::now();
        if now.saturating_duration_since(self.last_alive) >= Duration::from_millis(2) {
            if self.client.alive().is_err() {
                self.fail_pending();
                return Ok(());
            }
            self.last_alive = now;
        }
        let mut ops = std::mem::take(&mut self.ops);
        let mut finished = std::mem::take(&mut self.finished_ops);
        let result = (|| {
            for (&id, op) in &mut ops {
                let ordered = matches!(op.kind, OpKind::Send { .. } | OpKind::RdmaWrite { .. });
                if ordered && self.qp_send_active.get(&op.qp_id) != Some(&id) {
                    continue;
                }
                // The daemon bounds the transfer once a standing receive has matched.
                let completion = if !matches!(op.kind, OpKind::Recv { standing: true, .. })
                    && now.saturating_duration_since(op.started) > self.op_timeout
                {
                    Some(WorkCompletion {
                        wr_id: op.wr_id,
                        status: WcStatus::TransportErr,
                        opcode: op_to_wc_opcode(op.kind, false),
                        byte_len: 0,
                        tag_or_imm: 0,
                        has_imm: false,
                        qp_id: op.qp_id,
                    })
                } else {
                    self.step_op(op, now)?
                };
                if let Some(wc) = completion {
                    finished.push((id, wc));
                }
            }
            Ok(())
        })();

        for (id, wc) in finished.drain(..) {
            if let Some(op) = ops.remove(&id) {
                self.push_wc(op.cq_id, wc);
                if self.qp_send_active.get(&op.qp_id) == Some(&id) {
                    if matches!(op.kind, OpKind::RdmaWrite { .. }) && wc.status != WcStatus::Success
                    {
                        self.qp_send_active.remove(&op.qp_id);
                        if let Some(queued) = self.qp_send_queued.remove(&op.qp_id) {
                            let mut canceled_sends = 0;
                            for queued_id in queued {
                                if let Some(dependent) = ops.remove(&queued_id) {
                                    canceled_sends +=
                                        usize::from(matches!(dependent.kind, OpKind::Send { .. }));
                                    self.push_wc(
                                        dependent.cq_id,
                                        WorkCompletion {
                                            wr_id: dependent.wr_id,
                                            status: wc.status,
                                            opcode: op_to_wc_opcode(dependent.kind, false),
                                            byte_len: 0,
                                            tag_or_imm: 0,
                                            has_imm: false,
                                            qp_id: dependent.qp_id,
                                        },
                                    );
                                }
                            }
                            if let Some(seq) = self.qp_send_seq.get_mut(&op.qp_id) {
                                *seq -= canceled_sends as u64;
                            }
                        }
                    } else if let Some(next_id) = self
                        .qp_send_queued
                        .get_mut(&op.qp_id)
                        .and_then(VecDeque::pop_front)
                    {
                        self.qp_send_active.insert(op.qp_id, next_id);
                    } else {
                        self.qp_send_active.remove(&op.qp_id);
                    }
                }
            }
        }
        self.ops = ops;
        self.finished_ops = finished;
        if matches!(result, Err(Error::Io(_) | Error::Disconnected)) {
            self.fail_pending();
            return Ok(());
        }
        result
    }

    fn step_op(&mut self, op: &mut PendingOp, now: Instant) -> Result<Option<WorkCompletion>> {
        match op.kind {
            OpKind::Send { tag, seq } => {
                if let Some(slot) = op.slot {
                    match self.client.state(slot)? {
                        COMPLETE | FAILED => {
                            let d = self.client.finish(slot)?;
                            op.slot = None;
                            if d.status != STATUS_OK {
                                return Ok(Some(WorkCompletion {
                                    wr_id: op.wr_id,
                                    status: WcStatus::from_ipc_status(d.status),
                                    opcode: WcOpcode::Send,
                                    byte_len: 0,
                                    tag_or_imm: tag,
                                    has_imm: false,
                                    qp_id: op.qp_id,
                                }));
                            }
                            if matches!(op.target, LocalTarget::SharedMr { .. })
                                && d.length == 0
                                && d.offset == op.total_len as u64
                            {
                                op.cursor = op.total_len;
                            } else {
                                op.cursor += op.current_window;
                            }
                            op.started = now;
                            if op.cursor == op.total_len {
                                return Ok(Some(WorkCompletion {
                                    wr_id: op.wr_id,
                                    status: WcStatus::Success,
                                    opcode: WcOpcode::Send,
                                    byte_len: op.total_len as u64,
                                    tag_or_imm: tag,
                                    has_imm: false,
                                    qp_id: op.qp_id,
                                }));
                            }
                        }
                        _ => return Ok(None),
                    }
                }
                let win = (op.total_len - op.cursor).min(WINDOW);
                let (local_mr_id, local_offset, inline): (u64, u64, &[u8]) = match op.target {
                    LocalTarget::SharedMr { mr_id, base_offset } => {
                        (mr_id, base_offset + op.cursor as u64, &[])
                    }
                    LocalTarget::RawPtr { addr } => {
                        let slice = if win == 0 {
                            &[]
                        } else {
                            // SAFETY: caller's `BorrowedMr` remains readable until send completion.
                            unsafe {
                                std::slice::from_raw_parts((addr as *const u8).add(op.cursor), win)
                            }
                        };
                        (0, 0, slice)
                    }
                };
                let d = Descriptor {
                    op: OP_SEND,
                    peer: op.peer,
                    connection: op.connection,
                    request: op.wire_request,
                    listener: if op.cursor == 0 { seq } else { 0 },
                    offset: op.cursor as u64,
                    total: op.total_len as u64,
                    tag_or_imm: tag,
                    local_mr_id,
                    local_offset,
                    length: win as u32,
                    flags: if local_mr_id != 0 && op.cursor == 0 && op.total_len > WINDOW {
                        FLAG_IPC_SEND_REGISTERED
                    } else {
                        0
                    },
                    ..Default::default()
                };
                if let Some(slot) = self.client.post(d, inline)? {
                    op.slot = Some(slot);
                    op.current_window = win;
                    op.started = now;
                }
                Ok(None)
            }
            OpKind::Recv {
                tag,
                standing,
                mr_id,
                mr_offset,
            } => {
                if let Some(slot) = op.slot {
                    match self.client.state(slot)? {
                        READY => {
                            let (payload, _) = self.client.ready_payload(slot)?;
                            let n = payload.len();
                            if op.cursor.checked_add(n).is_none_or(|e| e > op.total_len) {
                                return Err(Error::Protocol("recv overflow"));
                            }
                            if n > 0 {
                                match op.target {
                                    LocalTarget::RawPtr { addr } => {
                                        // SAFETY: the application-owned target is writable until
                                        // completion and disjoint from the private IPC arena.
                                        unsafe {
                                            std::ptr::copy_nonoverlapping(
                                                payload.as_ptr(),
                                                (addr as *mut u8).add(op.cursor),
                                                n,
                                            );
                                        }
                                    }
                                    LocalTarget::SharedMr { .. } => {
                                        unreachable!("recv target stores raw mapping address");
                                    }
                                }
                            }
                            op.cursor += n;
                            op.started = now;
                            self.client.consume(slot)?;
                            return Ok(None);
                        }
                        COMPLETE | FAILED => {
                            if self.client.state(slot)? == COMPLETE
                                && mr_id == 0
                                && let LocalTarget::RawPtr { addr } = op.target
                            {
                                let (payload, inline_d) = self.client.completed_payload(slot)?;
                                if inline_d.flags & FLAG_IPC_INLINE_READY != 0
                                    && !payload.is_empty()
                                {
                                    let n = payload.len();
                                    if op.cursor.checked_add(n).is_none_or(|e| e > op.total_len) {
                                        return Err(Error::Protocol("recv overflow"));
                                    }
                                    // SAFETY: application target remains writable until completion
                                    // and is disjoint from the IPC shared-memory slot.
                                    unsafe {
                                        std::ptr::copy_nonoverlapping(
                                            payload.as_ptr(),
                                            (addr as *mut u8).add(op.cursor),
                                            n,
                                        );
                                    }
                                    op.cursor += n;
                                }
                            }
                            let d = self.client.finish(slot)?;
                            op.slot = None;
                            if d.status != STATUS_OK {
                                return Ok(Some(WorkCompletion {
                                    wr_id: op.wr_id,
                                    status: WcStatus::from_ipc_status(d.status),
                                    opcode: WcOpcode::Recv,
                                    byte_len: 0,
                                    tag_or_imm: tag,
                                    has_imm: false,
                                    qp_id: op.qp_id,
                                }));
                            }
                            if d.flags & FLAG_IPC_RECV_IMM != 0 {
                                return Ok(Some(WorkCompletion {
                                    wr_id: op.wr_id,
                                    status: WcStatus::Success,
                                    opcode: WcOpcode::RecvRdmaWithImm,
                                    byte_len: d.total,
                                    tag_or_imm: d.tag_or_imm,
                                    has_imm: true,
                                    qp_id: op.qp_id,
                                }));
                            }
                            return Ok(Some(WorkCompletion {
                                wr_id: op.wr_id,
                                status: WcStatus::Success,
                                opcode: WcOpcode::Recv,
                                byte_len: d.total,
                                tag_or_imm: d.tag_or_imm,
                                has_imm: false,
                                qp_id: op.qp_id,
                            }));
                        }
                        _ => return Ok(None),
                    }
                }
                let inline_flag = if mr_id == 0 && matches!(op.target, LocalTarget::RawPtr { .. }) {
                    FLAG_IPC_INLINE_READY
                } else {
                    0
                };
                let d = Descriptor {
                    op: OP_RECV,
                    peer: op.peer,
                    connection: op.connection,
                    request: op.id,
                    total: op.total_len as u64,
                    tag_or_imm: tag,
                    local_mr_id: mr_id,
                    local_offset: mr_offset,
                    flags: if standing { FLAG_IPC_RECV_STANDING } else { 0 } | inline_flag,
                    ..Default::default()
                };
                if let Some(slot) = self.client.post(d, &[])? {
                    op.slot = Some(slot);
                    op.started = now;
                }
                Ok(None)
            }
            OpKind::RdmaWrite {
                rkey,
                remote_offset,
                imm,
            } => {
                let mut i = 0;
                while i < op.in_flight.len() {
                    let (slot, _off, win) = op.in_flight[i];
                    if matches!(self.client.state(slot)?, COMPLETE | FAILED) {
                        let d = self.client.finish(slot)?;
                        op.in_flight.remove(i);
                        op.started = now;
                        if d.status != STATUS_OK {
                            op.err_status.get_or_insert(d.status);
                        } else {
                            op.completed += win;
                        }
                    } else {
                        i += 1;
                    }
                }
                if op.in_flight.is_empty() {
                    if let Some(status) = op.err_status {
                        return Ok(Some(WorkCompletion {
                            wr_id: op.wr_id,
                            status: WcStatus::from_ipc_status(status),
                            opcode: WcOpcode::RdmaWrite,
                            byte_len: 0,
                            tag_or_imm: imm.unwrap_or(0),
                            has_imm: imm.is_some(),
                            qp_id: op.qp_id,
                        }));
                    }
                    if op.completed == op.total_len && (op.total_len > 0 || op.posted_zero) {
                        return Ok(Some(WorkCompletion {
                            wr_id: op.wr_id,
                            status: WcStatus::Success,
                            opcode: WcOpcode::RdmaWrite,
                            byte_len: op.total_len as u64,
                            tag_or_imm: imm.unwrap_or(0),
                            has_imm: imm.is_some(),
                            qp_id: op.qp_id,
                        }));
                    }
                }
                if op.err_status.is_some() {
                    return Ok(None);
                }
                let max_inflight = if op.total_len > WINDOW {
                    PIPELINE_WINDOWS
                } else {
                    1
                };
                while op.in_flight.len() < max_inflight
                    && (op.cursor < op.total_len || (op.total_len == 0 && !op.posted_zero))
                {
                    let win = (op.total_len - op.cursor).min(WINDOW);
                    let is_last = op.cursor + win == op.total_len;
                    if imm.is_some() && is_last && !op.in_flight.is_empty() {
                        break;
                    }
                    let wire_req = self.alloc_wire_request();
                    let (local_mr_id, local_offset, inline): (u64, u64, &[u8]) = match op.target {
                        LocalTarget::SharedMr { mr_id, base_offset } => {
                            (mr_id, base_offset + op.cursor as u64, &[])
                        }
                        LocalTarget::RawPtr { addr } => {
                            let slice = if win == 0 {
                                &[]
                            } else {
                                // SAFETY: caller's `BorrowedMr` remains valid and readable until completion.
                                unsafe {
                                    std::slice::from_raw_parts(
                                        (addr as *const u8).add(op.cursor),
                                        win,
                                    )
                                }
                            };
                            (0, 0, slice)
                        }
                    };
                    let d = Descriptor {
                        op: OP_RDMA_WRITE,
                        peer: op.peer,
                        connection: op.connection,
                        request: wire_req,
                        listener: op.total_len as u64,
                        offset: 0,
                        total: win as u64,
                        rkey,
                        remote_offset: remote_offset + op.cursor as u64,
                        local_mr_id,
                        local_offset,
                        length: win as u32,
                        tag_or_imm: if is_last { imm.unwrap_or(0) } else { 0 },
                        flags: if is_last && imm.is_some() {
                            FLAG_IPC_WITH_IMM
                        } else {
                            0
                        },
                        ..Default::default()
                    };
                    if let Some(slot) = self.client.post(d, inline)? {
                        op.in_flight.push_back((slot, op.cursor, win));
                        op.cursor += win;
                        op.posted_zero = true;
                        op.started = now;
                    } else {
                        break;
                    }
                }
                Ok(None)
            }
            OpKind::RdmaRead {
                rkey,
                remote_offset,
            } => {
                let mut i = 0;
                while i < op.in_flight.len() {
                    let (slot, off, win) = op.in_flight[i];
                    match self.client.state(slot)? {
                        READY => {
                            let (payload, _) = self.client.ready_payload(slot)?;
                            if payload.len() != win {
                                return Err(Error::Protocol("RDMA read payload length mismatch"));
                            }
                            if win > 0
                                && let LocalTarget::RawPtr { addr } = op.target
                            {
                                // SAFETY: borrowed destination range stays valid and writable until completion.
                                unsafe {
                                    std::ptr::copy_nonoverlapping(
                                        payload.as_ptr(),
                                        (addr as *mut u8).add(off),
                                        win,
                                    );
                                }
                            }
                            op.started = now;
                            self.client.consume(slot)?;
                            i += 1;
                        }
                        COMPLETE | FAILED => {
                            if self.client.state(slot)? == COMPLETE
                                && let LocalTarget::RawPtr { addr } = op.target
                            {
                                let (payload, inline_d) = self.client.completed_payload(slot)?;
                                if inline_d.flags & FLAG_IPC_INLINE_READY != 0
                                    && win > 0
                                    && payload.len() >= win
                                {
                                    // SAFETY: borrowed destination stays valid until completion.
                                    unsafe {
                                        std::ptr::copy_nonoverlapping(
                                            payload.as_ptr(),
                                            (addr as *mut u8).add(off),
                                            win,
                                        );
                                    }
                                }
                            }
                            let d = self.client.finish(slot)?;
                            op.in_flight.remove(i);
                            op.started = now;
                            if d.status != STATUS_OK {
                                op.err_status.get_or_insert(d.status);
                            } else {
                                op.completed += win;
                            }
                        }
                        _ => i += 1,
                    }
                }
                if op.in_flight.is_empty() {
                    if let Some(status) = op.err_status {
                        return Ok(Some(WorkCompletion {
                            wr_id: op.wr_id,
                            status: WcStatus::from_ipc_status(status),
                            opcode: WcOpcode::RdmaRead,
                            byte_len: 0,
                            tag_or_imm: 0,
                            has_imm: false,
                            qp_id: op.qp_id,
                        }));
                    }
                    if op.completed == op.total_len && (op.total_len > 0 || op.posted_zero) {
                        return Ok(Some(WorkCompletion {
                            wr_id: op.wr_id,
                            status: WcStatus::Success,
                            opcode: WcOpcode::RdmaRead,
                            byte_len: op.total_len as u64,
                            tag_or_imm: 0,
                            has_imm: false,
                            qp_id: op.qp_id,
                        }));
                    }
                }
                if op.err_status.is_some() {
                    return Ok(None);
                }
                let max_inflight =
                    if matches!(op.target, LocalTarget::SharedMr { .. }) && op.total_len > WINDOW {
                        PIPELINE_WINDOWS
                    } else {
                        1
                    };
                while op.in_flight.len() < max_inflight
                    && (op.cursor < op.total_len || (op.total_len == 0 && !op.posted_zero))
                {
                    let win = (op.total_len - op.cursor).min(WINDOW);
                    let wire_req = self.alloc_wire_request();
                    let (local_mr_id, local_offset) = match op.target {
                        LocalTarget::SharedMr { mr_id, base_offset } => {
                            (mr_id, base_offset + op.cursor as u64)
                        }
                        LocalTarget::RawPtr { .. } => (0, 0),
                    };
                    let d = Descriptor {
                        op: OP_RDMA_READ,
                        peer: op.peer,
                        connection: op.connection,
                        request: wire_req,
                        offset: 0,
                        total: win as u64,
                        rkey,
                        remote_offset: remote_offset + op.cursor as u64,
                        local_mr_id,
                        local_offset,
                        length: win as u32,
                        flags: if local_mr_id == 0 {
                            FLAG_IPC_INLINE_READY
                        } else {
                            0
                        },
                        ..Default::default()
                    };
                    if let Some(slot) = self.client.post(d, &[])? {
                        op.in_flight.push_back((slot, op.cursor, win));
                        op.cursor += win;
                        op.posted_zero = true;
                        op.started = now;
                    } else {
                        break;
                    }
                }
                Ok(None)
            }
        }
    }
}

fn op_to_wc_opcode(kind: OpKind, recv_imm: bool) -> WcOpcode {
    match kind {
        OpKind::Send { .. } => WcOpcode::Send,
        OpKind::Recv { .. } if recv_imm => WcOpcode::RecvRdmaWithImm,
        OpKind::Recv { .. } => WcOpcode::Recv,
        OpKind::RdmaWrite { .. } => WcOpcode::RdmaWrite,
        OpKind::RdmaRead { .. } => WcOpcode::RdmaRead,
    }
}

/// Connection to a local `usb4-rdma-daemon`.
#[derive(Clone)]
pub struct Context {
    pub(crate) inner: Arc<Mutex<ContextInner>>,
}

impl Context {
    pub fn open(socket: impl AsRef<Path>) -> Result<Self> {
        let client = Client::connect(socket.as_ref())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(ContextInner {
                client,
                last_alive: Instant::now(),
                failed: false,
                next_cq_id: 1,
                next_listener_low: 1,
                next_local_mr_key: 1u64 << 63,
                next_wire_request: 1,
                next_op_id: 1,
                cqs: BTreeMap::new(),
                qp_send_seq: BTreeMap::new(),
                qp_send_active: BTreeMap::new(),
                qp_send_queued: BTreeMap::new(),
                ops: BTreeMap::new(),
                finished_ops: Vec::with_capacity(MAX_PENDING_OPS),
                op_timeout: DEFAULT_OP_TIMEOUT,
            })),
        })
    }

    pub fn open_default() -> Result<Self> {
        let path = std::env::var("USB4_RDMA_SOCKET")
            .map_err(|_| Error::Config("USB4_RDMA_SOCKET is not set".into()))?;
        Self::open(path)
    }

    pub fn query_device(&self) -> Result<DeviceAttr> {
        let inner = self.inner.lock().map_err(|_| Error::Disconnected)?;
        let w = inner.client.welcome;
        Ok(DeviceAttr {
            node_id: w.node,
            epoch: w.epoch,
            topology_digest: w.digest,
            max_window_bytes: WINDOW as u32,
            max_chunk_bytes: CHUNK as u32,
            max_mr_count: MAX_CLIENT_MRS as u32,
            supports_one_sided_rdma: true,
            zero_copy_dma: false,
        })
    }

    pub fn stats(&self) -> Result<FabricStats> {
        let mut inner = self.inner.lock().map_err(|_| Error::Disconnected)?;
        let slot = inner
            .client
            .post(
                Descriptor {
                    op: OP_STATS,
                    ..Default::default()
                },
                &[],
            )?
            .ok_or(Error::Exhausted)?;
        let start = Instant::now();
        while !matches!(inner.client.state(slot)?, COMPLETE | FAILED) {
            if start.elapsed() > Duration::from_secs(5) {
                return Err(Error::Timeout);
            }
            inner.client.alive()?;
            std::thread::yield_now();
        }
        let d = inner.client.finish(slot)?;
        Ok(FabricStats {
            node: inner.client.welcome.node,
            epoch: inner.client.welcome.epoch,
            ready: d.flags == 1,
            tx_frames: d.connection,
            rx_frames: d.request,
            forwarded_frames: d.listener,
            tx_payload_bytes: d.total,
            rx_payload_bytes: d.offset,
        })
    }

    pub fn alloc_pd(&self) -> ProtectionDomain {
        ProtectionDomain {
            inner: self.inner.clone(),
        }
    }

    pub fn create_cq(&self) -> Result<CompletionQueue> {
        let mut inner = self.inner.lock().map_err(|_| Error::Disconnected)?;
        let id = inner.next_cq_id;
        inner.next_cq_id = id.checked_add(1).ok_or(Error::Exhausted)?;
        inner.cqs.insert(id, VecDeque::new());
        Ok(CompletionQueue {
            inner: self.inner.clone(),
            id,
        })
    }
}

/// Protection domain owning memory registrations, listeners, and queue pairs.
#[derive(Clone)]
pub struct ProtectionDomain {
    pub(crate) inner: Arc<Mutex<ContextInner>>,
}

impl ProtectionDomain {
    /// Allocate a shared-memory MR mapped into both the caller and `usb4-rdma-daemon`.
    /// Supports `LOCAL_READ`, `LOCAL_WRITE`, `REMOTE_READ`, and `REMOTE_WRITE`.
    pub fn alloc_mr(&self, len: usize, access: AccessFlags) -> Result<OwnedMr> {
        OwnedMr::allocate(self.inner.clone(), len, access)
    }

    /// Register an existing caller-owned `memfd`/shared-memory FD with the daemon.
    pub fn reg_mr_fd(&self, fd: OwnedFd, len: usize, access: AccessFlags) -> Result<OwnedMr> {
        OwnedMr::from_fd(self.inner.clone(), fd, len, access)
    }

    /// Borrow an arbitrary host buffer for local operations (`LOCAL_READ | LOCAL_WRITE` only).
    ///
    /// # Safety
    /// `ptr..ptr+len` must remain valid and live until the returned `BorrowedMr` and all
    /// operations using it have completed.
    pub unsafe fn reg_mr_local(
        &self,
        ptr: *mut u8,
        len: usize,
        access: AccessFlags,
    ) -> Result<BorrowedMr> {
        let lkey = {
            let mut inner = self.inner.lock().map_err(|_| Error::Disconnected)?;
            let k = inner.next_local_mr_key;
            inner.next_local_mr_key = k.checked_add(1).ok_or(Error::Exhausted)?;
            k
        };
        // SAFETY: caller upholds buffer lifetime and access contract.
        unsafe { BorrowedMr::new(ptr, len, access, lkey) }
    }

    pub fn listen(&self) -> Result<Listener> {
        Listener::new(self.inner.clone())
    }

    pub fn connect(
        &self,
        remote: &EndpointHandle,
        send_cq: &CompletionQueue,
        recv_cq: &CompletionQueue,
    ) -> Result<QueuePair> {
        self.begin_connect(remote, send_cq, recv_cq)?
            .wait(Duration::from_secs(15))
    }

    /// Post a connection request without waiting for the remote accept.
    pub fn begin_connect(
        &self,
        remote: &EndpointHandle,
        send_cq: &CompletionQueue,
        recv_cq: &CompletionQueue,
    ) -> Result<ConnectionRequest> {
        {
            let state = self.inner.lock().map_err(|_| Error::Disconnected)?;
            if remote.epoch != state.client.welcome.epoch {
                return Err(Error::Protocol("remote handle epoch mismatch"));
            }
        }
        ConnectionRequest::post(
            self.inner.clone(),
            Descriptor {
                op: OP_CONNECT,
                peer: remote.node_id,
                total: remote.listener_id,
                ..Default::default()
            },
            send_cq,
            recv_cq,
        )
    }
}

/// Queue receiving completed work requests (`WorkCompletion`).
pub struct CompletionQueue {
    pub(crate) inner: Arc<Mutex<ContextInner>>,
    pub(crate) id: u64,
}

impl CompletionQueue {
    pub fn poll(&self, out: &mut [WorkCompletion]) -> Result<usize> {
        let mut inner = self.inner.lock().map_err(|_| Error::Disconnected)?;
        inner.drive_all()?;
        let Some(q) = inner.cqs.get_mut(&self.id) else {
            return Err(Error::Stale);
        };
        let mut n = 0;
        while n < out.len() {
            let Some(wc) = q.pop_front() else {
                break;
            };
            out[n] = wc;
            n += 1;
        }
        Ok(n)
    }

    pub fn poll_one(&self) -> Result<Option<WorkCompletion>> {
        let mut buf = [WorkCompletion {
            wr_id: 0,
            status: WcStatus::Success,
            opcode: WcOpcode::Send,
            byte_len: 0,
            tag_or_imm: 0,
            has_imm: false,
            qp_id: 0,
        }; 1];
        let n = self.poll(&mut buf)?;
        Ok((n == 1).then_some(buf[0]))
    }

    pub fn poll_blocking(&self, timeout: Duration) -> Result<WorkCompletion> {
        let start = Instant::now();
        let spin_budget = Duration::from_micros(500);
        loop {
            if let Some(wc) = self.poll_one()? {
                return Ok(wc);
            }
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Err(Error::Timeout);
            }
            if elapsed < spin_budget {
                std::hint::spin_loop();
            } else {
                std::thread::sleep(Duration::from_micros(10));
            }
        }
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.cqs.remove(&self.id);
        }
    }
}
