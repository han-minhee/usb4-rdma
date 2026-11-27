mod two_sided;

use crate::ipc::*;
use crate::mr_table::MrTable;
use crate::{
    Error, Result,
    protocol::{CHUNK, FLAG_DIRECT, FLAG_WITH_IMM, FrameHeader, Kind},
    reassembly::mark_window_bitmap,
};
use crate::{Fabric, LinkFailure};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    time::{Duration, Instant},
};

const LIMIT: usize = 2048;
const MAX_INBOUND_RDMA_WRITES: usize = 256;
const MAX_READ_RESPONDERS: usize = 256;

const ERR_OP_SEND: u32 = 1;
const ERR_OP_RDMA_WRITE: u32 = 2;
const ERR_OP_RDMA_READ: u32 = 3;

#[derive(Clone, Copy)]
struct Job {
    d: Descriptor,
    seq: u64,
    phase: u8,
    started: Instant,
    cursor: usize,
    progress: usize,
    limit: usize,
    received: u32,
    mr_pinned: bool,
}

struct LocalClient {
    ipc: ServerClient,
    jobs: [Option<Job>; SLOTS],
    next_seq: u64,
    active_jobs: u64,
    pending_dereg_replies: VecDeque<u64>,
}

impl LocalClient {
    fn set_job(&mut self, si: usize, job: Option<Job>) {
        if job.is_some() {
            self.active_jobs |= 1u64 << si;
        } else {
            self.active_jobs &= !(1u64 << si);
        }
        self.jobs[si] = job;
    }
}

#[derive(Clone, Copy)]
struct Connection {
    client: usize,
    next_send: u64,
}

#[derive(Clone, Copy)]
struct InboundRdmaWrite {
    header: FrameHeader,
    received_bytes: usize,
    bitmap: u32,
    started: Instant,
}

#[derive(Clone, Copy)]
struct ImmNotice {
    peer: u32,
    connection: u64,
    byte_len: u64,
    imm_data: u64,
}

#[derive(Clone, Copy)]
struct ReadResponder {
    peer: u32,
    connection: u64,
    request: u64,
    target_client: usize,
    mr_id: u64,
    rkey: u64,
    remote_offset: u64,
    total: usize,
    sent: usize,
    sent_zero: bool,
}

struct ServiceState {
    node: u32,
    epoch: u64,
    server: Server,
    clients: Vec<Option<LocalClient>>,
    active_clients: Vec<usize>,
    mrs: MrTable,
    next: u64,
    listeners: BTreeMap<u64, usize>,
    connections: HashMap<(u32, u64), Connection>,
    opens: VecDeque<FrameHeader>,
    rts: VecDeque<FrameHeader>,
    grants: HashMap<(u32, u64, u64, u64), FrameHeader>,
    acks: HashMap<(u32, u64, u64, u64), FrameHeader>,
    rdma_write_acks: HashMap<(u32, u64, u64), FrameHeader>,
    rdma_errors: VecDeque<FrameHeader>,
    recv_index: HashMap<(u32, u64, u64), (usize, usize)>,
    read_index: HashMap<(u32, u64, u64), (usize, usize)>,
    inbound_writes: HashMap<(u32, u64, u64), InboundRdmaWrite>,
    failed_inbound_writes: VecDeque<(u32, u64, u64)>,
    imm_notices: VecDeque<ImmNotice>,
    read_responders: VecDeque<ReadResponder>,
    outgoing: VecDeque<FrameHeader>,
    timeout: Duration,
}

pub struct Service {
    fabric: Fabric,
    state: ServiceState,
}

impl Service {
    pub fn with_max_mr_bytes(
        fabric: Fabric,
        server: Server,
        timeout_ms: u64,
        max_mr_bytes: usize,
    ) -> Self {
        let node = fabric.node;
        let epoch = fabric.routes.epoch;
        Self {
            fabric,
            state: ServiceState {
                node,
                epoch,
                server,
                clients: (0..32).map(|_| None).collect(),
                active_clients: Vec::with_capacity(32),
                mrs: MrTable::new(max_mr_bytes),
                next: 1,
                listeners: BTreeMap::new(),
                connections: HashMap::with_capacity(64),
                opens: VecDeque::with_capacity(LIMIT),
                rts: VecDeque::with_capacity(LIMIT),
                grants: HashMap::with_capacity(LIMIT),
                acks: HashMap::with_capacity(LIMIT),
                rdma_write_acks: HashMap::with_capacity(LIMIT),
                rdma_errors: VecDeque::with_capacity(LIMIT),
                recv_index: HashMap::with_capacity(LIMIT),
                read_index: HashMap::with_capacity(LIMIT),
                inbound_writes: HashMap::with_capacity(MAX_INBOUND_RDMA_WRITES),
                failed_inbound_writes: VecDeque::with_capacity(256),
                imm_notices: VecDeque::with_capacity(LIMIT),
                read_responders: VecDeque::with_capacity(MAX_READ_RESPONDERS),
                outgoing: VecDeque::with_capacity(LIMIT),
                timeout: Duration::from_millis(timeout_ms),
            },
        }
    }

    pub fn run(self, spin_us: u64, stopping: impl Fn() -> bool) -> Result<()> {
        self.run_with_link_failure_report(spin_us, stopping, |_| {})
    }

    /// Runs the service and reports link-specific evidence on terminal failure when available.
    pub fn run_with_link_failure_report(
        mut self,
        spin_us: u64,
        stopping: impl Fn() -> bool,
        report_failure: impl FnOnce(LinkFailure),
    ) -> Result<()> {
        // SAFETY: PR_SET_TIMERSLACK takes only scalar integer arguments and no pointers.
        unsafe {
            libc::prctl(libc::PR_SET_TIMERSLACK, 1, 0, 0, 0);
        }
        let budget = Duration::from_micros(spin_us);
        let mut active = Instant::now();
        let link_fds = self.fabric.link_fds();
        let mut pollfds: Vec<libc::pollfd> = Vec::with_capacity(1 + link_fds.len() + 32);
        let mut ctrlfds: Vec<libc::pollfd> = Vec::with_capacity(1 + 32);
        let mut tick_count: u32 = 0;
        let mut force_fd_check = true;
        let mut last_expire = Instant::now();
        let mut last_now = last_expire;
        let mut idle_spins: u32 = 0;

        loop {
            if stopping() {
                for &ci in &self.state.active_clients {
                    if let Some(c) = self.state.clients[ci].as_mut() {
                        c.ipc.fail_all(STATUS_CONNECTION_CLOSED);
                    }
                }
                return self.fabric.shutdown();
            }

            let mut check_accept = false;
            let mut ctrl_mask: u32 = 0;
            if force_fd_check || (tick_count & 0x3F) == 0 {
                force_fd_check = false;
                ctrlfds.clear();
                ctrlfds.push(libc::pollfd {
                    fd: self.state.server.raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                });
                for &ci in &self.state.active_clients {
                    if let Some(c) = self.state.clients[ci].as_ref() {
                        ctrlfds.push(libc::pollfd {
                            fd: c.ipc.raw_fd(),
                            events: libc::POLLIN,
                            revents: 0,
                        });
                    }
                }
                // SAFETY: ctrlfds points to initialized pollfd entries of length ctrlfds.len().
                let ready = unsafe { libc::poll(ctrlfds.as_mut_ptr(), ctrlfds.len() as _, 0) };
                if ready > 0 {
                    check_accept = (ctrlfds[0].revents & libc::POLLIN) != 0;
                    for (idx, &ci) in self.state.active_clients.iter().enumerate() {
                        if let Some(pfd) = ctrlfds.get(1 + idx)
                            && pfd.revents != 0
                        {
                            ctrl_mask |= 1u32 << ci;
                        }
                    }
                }
            }
            tick_count = tick_count.wrapping_add(1);

            let now = if force_fd_check || check_accept || ctrl_mask != 0 || (idle_spins & 15) == 0
            {
                let t = Instant::now();
                last_now = t;
                t
            } else {
                last_now
            };
            let expire_inbound =
                now.saturating_duration_since(last_expire) >= Duration::from_millis(50);
            if expire_inbound {
                last_expire = now;
            }

            match self.tick(now, check_accept, ctrl_mask, expire_inbound) {
                Ok(true) => {
                    idle_spins = 0;
                    active = now;
                }
                Ok(false)
                    if (idle_spins & 15) != 0 || now.saturating_duration_since(active) < budget =>
                {
                    idle_spins = idle_spins.wrapping_add(1);
                    std::hint::spin_loop();
                }
                Ok(false) => {
                    idle_spins = 0;
                    pollfds.clear();
                    pollfds.push(libc::pollfd {
                        fd: self.state.server.raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    });
                    for &fd in &link_fds {
                        pollfds.push(libc::pollfd {
                            fd,
                            events: libc::POLLIN,
                            revents: 0,
                        });
                    }
                    for &ci in &self.state.active_clients {
                        if let Some(c) = self.state.clients[ci].as_ref() {
                            pollfds.push(libc::pollfd {
                                fd: c.ipc.raw_fd(),
                                events: libc::POLLIN,
                                revents: 0,
                            });
                        }
                    }
                    let ts = libc::timespec {
                        tv_sec: 0,
                        tv_nsec: 25_000,
                    };
                    self.fabric.set_consumer_sleeping(true);
                    // SAFETY: pollfds and ts are valid initialized structs; sigmask is null.
                    let ready = unsafe {
                        libc::ppoll(
                            pollfds.as_mut_ptr(),
                            pollfds.len() as _,
                            &ts,
                            std::ptr::null(),
                        )
                    };
                    self.fabric.set_consumer_sleeping(false);
                    if ready > 0 {
                        active = Instant::now();
                        force_fd_check = true;
                    }
                }
                Err(e) => {
                    for &ci in &self.state.active_clients {
                        if let Some(c) = self.state.clients[ci].as_mut() {
                            c.ipc.fail_all(STATUS_TRANSPORT_ERR);
                        }
                    }
                    let _ = self.fabric.shutdown();
                    if let Some(failure) = self.fabric.link_failure() {
                        report_failure(failure);
                    }
                    return Err(e);
                }
            }
        }
    }

    pub fn tick(
        &mut self,
        now: Instant,
        check_accept: bool,
        ctrl_mask: u32,
        expire_inbound: bool,
    ) -> Result<bool> {
        let mut progress = {
            let state = &mut self.state;
            self.fabric
                .poll_and_deliver_at(now, |h, payload| state.incoming_raw(h, payload, now))?
        };

        if check_accept && let Some(ipc) = self.state.server.accept()? {
            if let Some(i) = self.state.clients.iter().position(Option::is_none) {
                self.state.clients[i] = Some(LocalClient {
                    ipc,
                    jobs: [None; SLOTS],
                    next_seq: 1,
                    active_jobs: 0,
                    pending_dereg_replies: VecDeque::new(),
                });
                self.state.active_clients.push(i);
            }
            progress = true;
        }

        let num_active = self.state.active_clients.len();
        let mut idx = 0;
        while idx < num_active && idx < self.state.active_clients.len() {
            let ci = self.state.active_clients[idx];
            idx += 1;
            if self.state.clients[ci].is_none() {
                continue;
            }
            if (ctrl_mask & (1u32 << ci)) != 0 {
                let event = self.state.clients[ci]
                    .as_mut()
                    .unwrap()
                    .ipc
                    .poll_control()?;
                match event {
                    ControlEvent::None => {}
                    ControlEvent::Disconnected => {
                        self.state.drop_client(ci)?;
                        progress = true;
                        continue;
                    }
                    ControlEvent::RegMr { fd, len, access } => {
                        progress = true;
                        let reply = match self.state.mrs.register(ci, fd, len, access) {
                            Ok((mr_id, rkey)) => ControlReply {
                                status: STATUS_OK,
                                mr_id,
                                rkey,
                            },
                            Err(Error::AccessViolation) => ControlReply {
                                status: STATUS_REMOTE_ACCESS_ERR,
                                mr_id: 0,
                                rkey: 0,
                            },
                            Err(_) => ControlReply {
                                status: STATUS_GENERAL_ERR,
                                mr_id: 0,
                                rkey: 0,
                            },
                        };
                        if self.state.clients[ci]
                            .as_mut()
                            .unwrap()
                            .ipc
                            .reply_control(reply)
                            .is_err()
                        {
                            self.state.drop_client(ci)?;
                            continue;
                        }
                    }
                    ControlEvent::DeregMr { mr_id } => {
                        progress = true;
                        match self.state.mrs.deregister(ci, mr_id) {
                            Ok(true) => {
                                let reply = ControlReply {
                                    status: STATUS_OK,
                                    mr_id,
                                    rkey: 0,
                                };
                                if self.state.clients[ci]
                                    .as_mut()
                                    .unwrap()
                                    .ipc
                                    .reply_control(reply)
                                    .is_err()
                                {
                                    self.state.drop_client(ci)?;
                                    continue;
                                }
                            }
                            Ok(false) => {
                                self.state.clients[ci]
                                    .as_mut()
                                    .unwrap()
                                    .pending_dereg_replies
                                    .push_back(mr_id);
                            }
                            Err(_) => {
                                let reply = ControlReply {
                                    status: STATUS_GENERAL_ERR,
                                    mr_id,
                                    rkey: 0,
                                };
                                if self.state.clients[ci]
                                    .as_mut()
                                    .unwrap()
                                    .ipc
                                    .reply_control(reply)
                                    .is_err()
                                {
                                    self.state.drop_client(ci)?;
                                    continue;
                                }
                            }
                        }
                    }
                }
            }

            if self.state.clients[ci].as_mut().unwrap().ipc.poll_doorbell() {
                let free_mask = !self.state.clients[ci].as_ref().unwrap().active_jobs;
                let mut m = free_mask;
                while m != 0 {
                    let si = m.trailing_zeros() as usize;
                    m &= m - 1;
                    if let Some(mut d) = self.state.clients[ci].as_mut().unwrap().ipc.claim(si)? {
                        progress = true;
                        if d.op == OP_STATS {
                            let mut payload = Vec::new();
                            if d.flags == STATS_LINKS {
                                let count = self.fabric.link_payload_bytes().len();
                                if count > WINDOW / LINK_STATS_BYTES {
                                    self.state.complete(ci, si, d, STATUS_GENERAL_ERR)?;
                                    continue;
                                }
                                payload.reserve_exact(count * LINK_STATS_BYTES);
                                for (tx, rx) in self.fabric.link_payload_bytes() {
                                    payload.extend_from_slice(&tx.to_le_bytes());
                                    payload.extend_from_slice(&rx.to_le_bytes());
                                }
                            }
                            d.length = payload.len() as u32;
                            d.peer = self.fabric.node;
                            d.connection = self.fabric.transmitted;
                            d.request = self.fabric.received;
                            d.listener = self.fabric.forwarded;
                            d.flags = u32::from(self.fabric.ready());
                            d.total = self.fabric.data_tx_bytes;
                            d.offset = self.fabric.data_rx_bytes;
                            self.state.clients[ci]
                                .as_mut()
                                .unwrap()
                                .ipc
                                .publish(si, d, &payload, COMPLETE)?;
                        } else if d.op == OP_LISTEN {
                            if self.state.listeners.len() >= LIMIT {
                                return Err(Error::Exhausted);
                            }
                            if d.listener >> 32
                                != self.state.clients[ci].as_ref().unwrap().ipc.namespace
                                || self.state.listeners.contains_key(&d.listener)
                            {
                                return Err(Error::Protocol("listener namespace"));
                            }
                            self.state.listeners.insert(d.listener, ci);
                            self.state.clients[ci].as_mut().unwrap().ipc.publish(
                                si,
                                d,
                                &[],
                                COMPLETE,
                            )?;
                        } else {
                            if d.op == OP_CONNECT {
                                d.connection = self.state.next_id()?;
                                d.listener = d.total;
                            }
                            let client = self.state.clients[ci].as_mut().unwrap();
                            let seq = client.next_seq;
                            client.next_seq += 1;
                            client.set_job(
                                si,
                                Some(Job {
                                    d,
                                    seq,
                                    phase: 0,
                                    started: now,
                                    cursor: 0,
                                    progress: 0,
                                    limit: 0,
                                    received: 0,
                                    mr_pinned: false,
                                }),
                            );
                        }
                    }
                }
            }
        }

        for _ in 0..128 {
            let Some(p) = self.fabric.receive()? else {
                break;
            };
            self.state.incoming_raw(p.header, p.data(), now)?;
            progress = true;
        }

        if expire_inbound && !self.state.inbound_writes.is_empty() {
            let timeout = self.state.timeout;
            self.state
                .inbound_writes
                .retain(|_, w| now.saturating_duration_since(w.started) <= timeout);
        }

        if !self.state.read_responders.is_empty() {
            progress |= self.state.advance_read_responders(&mut self.fabric, now)?;
        }

        let mut i = 0;
        while i < self.state.active_clients.len() {
            let ci = self.state.active_clients[i];
            i += 1;
            let Some(c) = self.state.clients[ci].as_ref() else {
                continue;
            };
            // Process active jobs in deterministic submission order (`op.id` for `OP_RECV`,
            // daemon intake `seq` otherwise) so wrapped IPC slot indices never reorder receives.
            let mut order = [((0u8, 0u64, 0u64), 0usize); SLOTS];
            let mut count = 0;
            let mut m = c.active_jobs;
            while m != 0 {
                let si = m.trailing_zeros() as usize;
                m &= m - 1;
                if let Some(job) = c.jobs[si] {
                    let is_recv = u8::from(job.d.op == OP_RECV);
                    let primary = if job.d.op == OP_RECV && job.d.request != 0 {
                        job.d.request
                    } else {
                        job.seq
                    };
                    order[count] = ((is_recv, primary, job.seq), si);
                    count += 1;
                }
            }
            order[..count].sort_unstable_by_key(|&(key, _)| key);
            for &(_, si) in &order[..count] {
                let Some(job) = self.state.clients[ci].as_ref().and_then(|c| c.jobs[si]) else {
                    continue;
                };
                let idle_wait = job.d.op == OP_ACCEPT || (job.d.op == OP_RECV && job.phase == 0);
                if !idle_wait
                    && expire_inbound
                    && now.saturating_duration_since(job.started) > self.state.timeout
                {
                    eprintln!(
                        "IPC operation timed out (op={}, phase={}, peer={}, connection={})",
                        job.d.op, job.phase, job.d.peer, job.d.connection
                    );
                    self.state.drop_client(ci)?;
                    progress = true;
                    break;
                }
                progress |= self.state.advance(&mut self.fabric, ci, si, job, now)?;
            }
        }

        let mut blocked = [0; 64];
        let mut blocked_count = 0;
        for _ in 0..self.state.outgoing.len() {
            let h = self
                .state
                .outgoing
                .pop_front()
                .expect("bounded queue iteration");
            if blocked[..blocked_count].contains(&h.destination) {
                self.state.outgoing.push_back(h);
            } else if self.fabric.try_send_direct_at(h, 0, now, &mut |_| Ok(()))? {
                progress = true;
            } else {
                blocked[blocked_count] = h.destination;
                blocked_count += 1;
                self.state.outgoing.push_back(h);
            }
        }

        self.fabric.flush_tx()?;

        Ok(progress)
    }
}

impl ServiceState {
    fn header(&self, kind: Kind, d: Descriptor) -> FrameHeader {
        let mut h = FrameHeader::control(kind, self.node, d.peer, self.epoch);
        h.connection = d.connection;
        h.request = d.request;
        h.offset = d.offset;
        h.total = d.total;
        h.tag_or_imm = d.tag_or_imm;
        h.rkey = d.rkey;
        h.remote_offset = d.remote_offset;
        h.aux = d.listener;
        h
    }

    fn push<T>(q: &mut VecDeque<T>, item: T) -> Result<()> {
        if q.len() == LIMIT {
            return Err(Error::Exhausted);
        }
        q.push_back(item);
        Ok(())
    }

    fn advance_read_responders(&mut self, fabric: &mut Fabric, now: Instant) -> Result<bool> {
        let mut progress = false;
        let count = self.read_responders.len();
        for _ in 0..count {
            let Some(mut resp) = self.read_responders.pop_front() else {
                break;
            };
            if !self.connections.contains_key(&(resp.peer, resp.connection)) {
                self.finish_read_responder(resp);
                progress = true;
                continue;
            }
            let done = if resp.total == 0 {
                if !resp.sent_zero {
                    let mut h =
                        FrameHeader::control(Kind::RdmaReadResp, self.node, resp.peer, self.epoch);
                    h.connection = resp.connection;
                    h.request = resp.request;
                    h.rkey = resp.rkey;
                    h.remote_offset = resp.remote_offset;
                    h.offset = 0;
                    h.total = 0;
                    if fabric.can_send_data(&h)?
                        && fabric.try_send_direct_at(h, 0, now, &mut |_| Ok(()))?
                    {
                        resp.sent_zero = true;
                        progress = true;
                    }
                }
                resp.sent_zero
            } else {
                for _ in 0..64 {
                    if resp.sent >= resp.total {
                        break;
                    }
                    let n = (resp.total - resp.sent).min(CHUNK);
                    let mut h =
                        FrameHeader::control(Kind::RdmaReadResp, self.node, resp.peer, self.epoch);
                    h.connection = resp.connection;
                    h.request = resp.request;
                    h.rkey = resp.rkey;
                    h.remote_offset = resp.remote_offset;
                    h.offset = resp.sent as u64;
                    h.total = resp.total as u64;
                    if !fabric.can_send_data(&h)? {
                        break;
                    }
                    let read_off = resp.remote_offset + resp.sent as u64;
                    let mrs = &self.mrs;
                    let mut fill = |out: &mut [u8]| {
                        mrs.read_for_responder(resp.target_client, resp.mr_id, read_off, out)
                    };
                    if !fabric.try_send_direct_at(h, n, now, &mut fill)? {
                        break;
                    }
                    resp.sent += n;
                    progress = true;
                }
                resp.sent >= resp.total
            };
            if done {
                self.finish_read_responder(resp);
            } else {
                self.read_responders.push_back(resp);
            }
        }
        Ok(progress)
    }

    fn finish_read_responder(&mut self, resp: ReadResponder) {
        self.finish_mr_operation(resp.target_client, resp.mr_id);
    }

    fn finish_mr_operation(&mut self, client: usize, mr_id: u64) {
        if let Some(finished_mr_id) = self.mrs.end_operation(client, mr_id)
            && let Some(c) = self.clients[client].as_mut()
            && let Some(pos) = c
                .pending_dereg_replies
                .iter()
                .position(|&id| id == finished_mr_id)
        {
            c.pending_dereg_replies.remove(pos);
            let _ = c.ipc.reply_control(ControlReply {
                status: STATUS_OK,
                mr_id: finished_mr_id,
                rkey: 0,
            });
        }
    }

    fn control(&mut self, kind: Kind, d: Descriptor) -> Result<()> {
        let h = self.header(kind, d);
        Self::push(&mut self.outgoing, h)
    }

    fn send_op_error(
        &mut self,
        peer: u32,
        connection: u64,
        request: u64,
        op_tag: u32,
        status: i32,
    ) -> Result<()> {
        let mut h = FrameHeader::control(Kind::Error, self.node, peer, self.epoch);
        h.connection = connection;
        h.request = request;
        h.flags = op_tag;
        h.aux = status as i64 as u64;
        Self::push(&mut self.outgoing, h)
    }

    fn next_id(&mut self) -> Result<u64> {
        let n = u32::try_from(self.next).map_err(|_| Error::Exhausted)?;
        self.next += 1;
        Ok(((self.node as u64) << 32) | n as u64)
    }

    fn complete(&mut self, ci: usize, si: usize, mut d: Descriptor, status: i32) -> Result<()> {
        if let Some(j) = self.clients[ci].as_ref().unwrap().jobs[si] {
            if j.mr_pinned {
                self.finish_mr_operation(ci, d.local_mr_id);
            }
            if j.d.op == OP_RECV && j.phase == 1 {
                self.recv_index
                    .remove(&(j.d.peer, j.d.connection, j.d.request));
            } else if j.d.op == OP_RDMA_READ && j.phase == 1 {
                self.read_index
                    .remove(&(j.d.peer, j.d.connection, j.d.request));
            }
        }
        d.status = status;
        let c = self.clients[ci].as_mut().unwrap();
        c.ipc
            .publish(si, d, &[], if status == 0 { COMPLETE } else { FAILED })?;
        c.set_job(si, None);
        Ok(())
    }

    fn record_failed_inbound_write(&mut self, key: (u32, u64, u64)) {
        if self.failed_inbound_writes.len() == 256 {
            self.failed_inbound_writes.pop_front();
        }
        self.failed_inbound_writes.push_back(key);
    }

    fn incoming_raw(&mut self, h: FrameHeader, data: &[u8], now: Instant) -> Result<()> {
        match h.kind {
            Kind::Open => Self::push(&mut self.opens, h)?,
            Kind::OpenAck => {
                for &ci in &self.active_clients {
                    if let Some(c) = self.clients[ci].as_mut() {
                        let mut m = c.active_jobs;
                        while m != 0 {
                            let i = m.trailing_zeros() as usize;
                            m &= m - 1;
                            if let Some(j) = c.jobs[i]
                                && j.d.op == OP_CONNECT
                                && j.d.peer == h.source
                                && j.d.connection == h.connection
                                && j.phase == 1
                            {
                                self.connections.insert(
                                    (h.source, h.connection),
                                    Connection {
                                        client: ci,
                                        next_send: 1,
                                    },
                                );
                                c.ipc.publish(i, j.d, &[], COMPLETE)?;
                                c.set_job(i, None);
                            }
                        }
                    }
                }
            }
            Kind::Rts => {
                if self.connections.contains_key(&(h.source, h.connection)) {
                    Self::push(&mut self.rts, h)?;
                }
            }
            Kind::Grant => {
                if self.connections.contains_key(&(h.source, h.connection)) {
                    if self.grants.len() >= LIMIT {
                        return Err(Error::Exhausted);
                    }
                    self.grants
                        .insert((h.source, h.connection, h.request, h.offset), h);
                }
            }
            Kind::Ack => {
                if !self.connections.contains_key(&(h.source, h.connection)) {
                    return Ok(());
                }
                if self.acks.len() >= LIMIT {
                    return Err(Error::Exhausted);
                }
                self.acks
                    .insert((h.source, h.connection, h.request, h.offset), h);
            }
            Kind::Data => {
                if !self.connections.contains_key(&(h.source, h.connection)) {
                    return Ok(());
                }
                let key = (h.source, h.connection, h.request);
                let found = self.recv_index.get(&key).copied().and_then(|(ci, si)| {
                    let j = self.clients[ci].as_ref()?.jobs[si]?;
                    if j.d.op == OP_RECV
                        && j.phase == 1
                        && j.d.peer == h.source
                        && j.d.connection == h.connection
                        && j.d.request == h.request
                        && h.offset >= j.d.offset
                        && h.offset - j.d.offset
                            < (j.limit as u64).min(j.d.total - j.d.offset).max(1)
                    {
                        Some((ci, si, j))
                    } else {
                        None
                    }
                });
                let (ci, si, mut j) = found.ok_or(Error::Protocol("data without receive grant"))?;
                let window = j.limit.min((j.d.total - j.d.offset) as usize);
                let offset = (h.offset - j.d.offset) as usize;
                if h.total != j.d.total {
                    return Err(Error::Protocol("receive total"));
                }
                let received = mark_window_bitmap(window, offset, h.length as usize, j.received)?;
                j.started = now;
                let c = self.clients[ci].as_mut().unwrap();
                if j.d.local_mr_id == 0 {
                    c.ipc.write_range(si, offset, data)?;
                } else {
                    self.mrs
                        .write_local(ci, j.d.local_mr_id, j.d.local_offset + h.offset, data)?;
                }
                j.received = received;
                j.cursor += h.length as usize;
                if j.cursor == window {
                    j.d.length = window as u32;
                    self.recv_index.remove(&key);
                    if j.d.local_mr_id == 0
                        && (j.d.flags & FLAG_IPC_INLINE_READY) != 0
                        && j.d.offset + window as u64 == j.d.total
                    {
                        let mut ack = self.header(Kind::Ack, j.d);
                        ack.aux = j.d.length as u64;
                        Self::push(&mut self.outgoing, ack)?;
                        j.d.offset += j.d.length as u64;
                        self.complete(ci, si, j.d, STATUS_OK)?;
                        return Ok(());
                    }
                    j.phase = 2;
                    if j.d.local_mr_id == 0 {
                        c.ipc.publish(si, j.d, &[], READY)?;
                    }
                }
                c.set_job(si, Some(j));
            }
            Kind::RdmaWrite => {
                let Some(&conn) = self.connections.get(&(h.source, h.connection)) else {
                    return Ok(());
                };
                let key = (h.source, h.connection, h.request);
                if self.failed_inbound_writes.contains(&key) {
                    return Ok(());
                }
                let previous = self.inbound_writes.get(&key).copied();
                let total = h.total as usize;
                let consistent = previous.is_none_or(|entry| {
                    let first = entry.header;
                    first.total == h.total
                        && first.rkey == h.rkey
                        && first.remote_offset == h.remote_offset
                        && first.flags & FLAG_WITH_IMM == h.flags & FLAG_WITH_IMM
                        && first.tag_or_imm == h.tag_or_imm
                        && first.aux == h.aux
                });
                let bitmap = mark_window_bitmap(
                    total,
                    h.offset as usize,
                    data.len(),
                    previous.map_or(0, |entry| entry.bitmap),
                );
                let status = if h.total > WINDOW as u64 || !consistent || bitmap.is_err() {
                    Some(STATUS_REMOTE_INVALID_REQ)
                } else if previous.is_none()
                    && data.len() < total
                    && self.inbound_writes.len() >= MAX_INBOUND_RDMA_WRITES
                {
                    Some(STATUS_GENERAL_ERR)
                } else if self
                    .mrs
                    .validate_and_write_remote(
                        conn.client,
                        h.rkey,
                        h.remote_offset,
                        h.total,
                        h.offset,
                        data,
                    )
                    .is_err()
                {
                    Some(STATUS_REMOTE_ACCESS_ERR)
                } else {
                    None
                };
                if let Some(status) = status {
                    self.inbound_writes.remove(&key);
                    self.record_failed_inbound_write(key);
                    self.send_op_error(
                        h.source,
                        h.connection,
                        h.request,
                        ERR_OP_RDMA_WRITE,
                        status,
                    )?;
                    return Ok(());
                }
                let received_bytes = previous.map_or(0, |entry| entry.received_bytes) + data.len();
                let complete = received_bytes == total;
                if complete {
                    self.inbound_writes.remove(&key);
                } else {
                    self.inbound_writes.insert(
                        key,
                        InboundRdmaWrite {
                            header: h,
                            received_bytes,
                            bitmap: bitmap?,
                            started: now,
                        },
                    );
                }
                if complete {
                    if h.flags & FLAG_WITH_IMM != 0 {
                        Self::push(
                            &mut self.imm_notices,
                            ImmNotice {
                                peer: h.source,
                                connection: h.connection,
                                byte_len: h.aux,
                                imm_data: h.tag_or_imm,
                            },
                        )?;
                    }
                    let mut ack =
                        FrameHeader::control(Kind::RdmaWriteAck, self.node, h.source, self.epoch);
                    ack.connection = h.connection;
                    ack.request = h.request;
                    ack.aux = h.total;
                    Self::push(&mut self.outgoing, ack)?;
                }
            }
            Kind::RdmaWriteAck => {
                if self.connections.contains_key(&(h.source, h.connection)) {
                    if self.rdma_write_acks.len() >= LIMIT {
                        return Err(Error::Exhausted);
                    }
                    self.rdma_write_acks
                        .insert((h.source, h.connection, h.request), h);
                }
            }
            Kind::RdmaReadReq => {
                let Some(&conn) = self.connections.get(&(h.source, h.connection)) else {
                    return Ok(());
                };
                if h.total as usize > WINDOW {
                    self.send_op_error(
                        h.source,
                        h.connection,
                        h.request,
                        ERR_OP_RDMA_READ,
                        STATUS_REMOTE_INVALID_REQ,
                    )?;
                    return Ok(());
                }
                if self.read_responders.len() >= MAX_READ_RESPONDERS {
                    self.send_op_error(
                        h.source,
                        h.connection,
                        h.request,
                        ERR_OP_RDMA_READ,
                        STATUS_GENERAL_ERR,
                    )?;
                    return Ok(());
                }
                match self
                    .mrs
                    .begin_remote_read(conn.client, h.rkey, h.remote_offset, h.total)
                {
                    Ok(mr_id) => {
                        self.read_responders.push_back(ReadResponder {
                            peer: h.source,
                            connection: h.connection,
                            request: h.request,
                            target_client: conn.client,
                            mr_id,
                            rkey: h.rkey,
                            remote_offset: h.remote_offset,
                            total: h.total as usize,
                            sent: 0,
                            sent_zero: false,
                        });
                    }
                    Err(_) => {
                        self.send_op_error(
                            h.source,
                            h.connection,
                            h.request,
                            ERR_OP_RDMA_READ,
                            STATUS_REMOTE_ACCESS_ERR,
                        )?;
                    }
                }
            }
            Kind::RdmaReadResp => {
                if !self.connections.contains_key(&(h.source, h.connection)) {
                    return Ok(());
                }
                let key = (h.source, h.connection, h.request);
                let Some((ci, si, mut j)) =
                    self.read_index.get(&key).copied().and_then(|(ci, si)| {
                        let j = self.clients[ci].as_ref()?.jobs[si]?;
                        if j.d.op == OP_RDMA_READ
                            && j.phase == 1
                            && j.d.peer == h.source
                            && j.d.connection == h.connection
                            && j.d.request == h.request
                        {
                            Some((ci, si, j))
                        } else {
                            None
                        }
                    })
                else {
                    return Ok(());
                };
                let window = j.d.length as usize;
                if h.total as usize != window {
                    return Err(Error::Protocol("RDMA read response total mismatch"));
                }
                let offset = h.offset as usize;
                let received = mark_window_bitmap(window, offset, h.length as usize, j.received)?;
                j.started = now;
                if j.d.local_mr_id != 0 {
                    let write_off = j.d.local_offset + offset as u64;
                    if self
                        .mrs
                        .write_local(ci, j.d.local_mr_id, write_off, data)
                        .is_err()
                    {
                        self.complete(ci, si, j.d, STATUS_LOCAL_LEN_ERR)?;
                        return Ok(());
                    }
                } else {
                    self.clients[ci]
                        .as_mut()
                        .unwrap()
                        .ipc
                        .write_range(si, offset, data)?;
                }
                j.received = received;
                j.cursor += h.length as usize;
                if j.cursor == window {
                    self.read_index.remove(&key);
                    if j.d.local_mr_id != 0 || (j.d.flags & FLAG_IPC_INLINE_READY) != 0 {
                        self.complete(ci, si, j.d, STATUS_OK)?;
                        return Ok(());
                    } else {
                        j.phase = 2;
                        self.clients[ci]
                            .as_mut()
                            .unwrap()
                            .ipc
                            .publish(si, j.d, &[], READY)?;
                    }
                }
                self.clients[ci].as_mut().unwrap().set_job(si, Some(j));
            }
            Kind::Error => {
                let op_tag = h.flags & !FLAG_DIRECT;
                if matches!(op_tag, ERR_OP_RDMA_WRITE | ERR_OP_RDMA_READ | ERR_OP_SEND) {
                    if self.connections.contains_key(&(h.source, h.connection)) {
                        Self::push(&mut self.rdma_errors, h)?;
                    }
                    return Ok(());
                }
                self.handle_connection_close(h.source, h.connection)?;
            }
            Kind::Close => {
                self.handle_connection_close(h.source, h.connection)?;
            }
            _ => return Err(Error::Protocol("unexpected local control")),
        }
        Ok(())
    }

    fn handle_connection_close(&mut self, peer: u32, connection: u64) -> Result<()> {
        let active = self.active_clients.clone();
        for ci in active {
            let Some(c) = &self.clients[ci] else {
                continue;
            };
            let mut m = c.active_jobs;
            while m != 0 {
                let si = m.trailing_zeros() as usize;
                m &= m - 1;
                let Some(c) = &self.clients[ci] else {
                    break;
                };
                if let Some(j) = c.jobs[si]
                    && j.d.op != OP_CLOSE
                    && j.d.peer == peer
                    && j.d.connection == connection
                    && matches!(c.ipc.state(si)?, BUSY | CONSUMED)
                {
                    self.complete(ci, si, j.d, STATUS_CONNECTION_CLOSED)?;
                }
            }
        }
        self.connections.remove(&(peer, connection));
        self.purge_connection(peer, connection);
        Ok(())
    }

    fn take_op_error(
        &mut self,
        peer: u32,
        connection: u64,
        request: u64,
        op_tag: u32,
    ) -> Option<i32> {
        let pos = self.rdma_errors.iter().position(|h| {
            h.source == peer
                && h.connection == connection
                && h.request == request
                && (h.flags & !FLAG_DIRECT) == op_tag
        })?;
        let h = self.rdma_errors.remove(pos).unwrap();
        Some(h.aux as i64 as i32)
    }

    fn advance(
        &mut self,
        fabric: &mut Fabric,
        ci: usize,
        si: usize,
        mut j: Job,
        now: Instant,
    ) -> Result<bool> {
        let d = j.d;
        let mut progressed = false;
        match d.op {
            OP_CONNECT => {
                if j.phase == 0 {
                    if self.connections.len() >= LIMIT {
                        return Err(Error::Exhausted);
                    }
                    self.control(Kind::Open, d)?;
                    self.connections.insert(
                        (d.peer, d.connection),
                        Connection {
                            client: ci,
                            next_send: 1,
                        },
                    );
                    j.phase = 1;
                    progressed = true;
                } else {
                    return Ok(false);
                }
            }
            OP_ACCEPT => {
                if self.listeners.get(&d.listener) != Some(&ci) {
                    self.complete(ci, si, d, STATUS_GENERAL_ERR)?;
                    return Ok(true);
                }
                if let Some(i) = self.opens.iter().position(|h| h.aux == d.listener) {
                    let h = self.opens.remove(i).unwrap();
                    j.d.connection = h.connection;
                    j.d.peer = h.source;
                    self.connections.insert(
                        (h.source, h.connection),
                        Connection {
                            client: ci,
                            next_send: 1,
                        },
                    );
                    self.control(Kind::OpenAck, j.d)?;
                    self.complete(ci, si, j.d, STATUS_OK)?;
                    return Ok(true);
                }
                return Ok(false);
            }
            OP_SEND => return self.advance_send(fabric, ci, si, j, now),
            OP_PEEK => {
                if !self
                    .connections
                    .get(&(d.peer, d.connection))
                    .is_some_and(|c| c.client == ci)
                {
                    self.complete(ci, si, d, STATUS_CONNECTION_CLOSED)?;
                    return Ok(true);
                }
                let mut reply = d;
                let has_rts = self.rts.iter().any(|h| {
                    h.source == d.peer
                        && h.connection == d.connection
                        && h.tag_or_imm == d.tag_or_imm
                });
                let has_imm = self
                    .imm_notices
                    .iter()
                    .any(|n| n.peer == d.peer && n.connection == d.connection);
                reply.flags = u32::from(has_rts || has_imm);
                self.complete(ci, si, reply, STATUS_OK)?;
                return Ok(true);
            }
            OP_RECV => return self.advance_recv(ci, si, j, now),
            OP_RDMA_WRITE => {
                if !self
                    .connections
                    .get(&(d.peer, d.connection))
                    .is_some_and(|c| c.client == ci)
                {
                    self.complete(ci, si, d, STATUS_CONNECTION_CLOSED)?;
                    return Ok(true);
                }
                if let Some(err_status) =
                    self.take_op_error(d.peer, d.connection, d.request, ERR_OP_RDMA_WRITE)
                {
                    self.complete(ci, si, d, err_status)?;
                    return Ok(true);
                }
                let window = d.length as usize;
                if j.phase == 0 {
                    // Stream up to 64 chunks per tick directly to the remote MR (1 RTT, no RTS/Grant).
                    for _ in 0..64 {
                        let n = if window == 0 {
                            0
                        } else {
                            (window - j.progress).min(CHUNK)
                        };
                        let mut h =
                            FrameHeader::control(Kind::RdmaWrite, self.node, d.peer, self.epoch);
                        h.connection = d.connection;
                        h.request = d.request;
                        h.offset = j.progress as u64;
                        h.total = window as u64;
                        h.rkey = d.rkey;
                        h.remote_offset = d.remote_offset;
                        h.tag_or_imm = d.tag_or_imm;
                        h.aux = d.listener; // carries full multi-window op byte length for WITH_IMM
                        if d.flags & FLAG_IPC_WITH_IMM != 0 {
                            h.flags |= FLAG_WITH_IMM;
                        }
                        if !fabric.can_send_data(&h)? {
                            break;
                        }
                        let sent = if d.local_mr_id != 0 {
                            let read_off = d.local_offset + j.progress as u64;
                            let mrs = &self.mrs;
                            let mut fill =
                                |out: &mut [u8]| mrs.read_local(ci, d.local_mr_id, read_off, out);
                            match fabric.try_send_direct_at(h, n, now, &mut fill) {
                                Ok(b) => b,
                                Err(
                                    Error::AccessViolation
                                    | Error::UnsupportedAccess
                                    | Error::Stale,
                                ) => {
                                    self.complete(ci, si, d, STATUS_LOCAL_LEN_ERR)?;
                                    return Ok(true);
                                }
                                Err(error) => return Err(error),
                            }
                        } else {
                            let ipc = &self.clients[ci].as_ref().unwrap().ipc;
                            let off = j.progress;
                            let mut fill = |out: &mut [u8]| ipc.read_range(si, off, out);
                            fabric.try_send_direct_at(h, n, now, &mut fill)?
                        };
                        if !sent {
                            break;
                        }
                        j.progress += n;
                        j.started = now;
                        progressed = true;
                        if j.progress == window {
                            j.phase = 1;
                            break;
                        }
                    }
                } else if j.phase == 1 {
                    if let Some(ack) =
                        self.rdma_write_acks
                            .remove(&(d.peer, d.connection, d.request))
                    {
                        if ack.aux != window as u64 {
                            return Err(Error::Protocol("RDMA write ACK size"));
                        }
                        self.complete(ci, si, d, STATUS_OK)?;
                        return Ok(true);
                    }
                    return Ok(false);
                } else {
                    return Ok(false);
                }
            }
            OP_RDMA_READ => {
                if !self
                    .connections
                    .get(&(d.peer, d.connection))
                    .is_some_and(|c| c.client == ci)
                {
                    // The application still owns a published receive window.
                    if self.clients[ci].as_ref().unwrap().ipc.state(si)? == READY {
                        return Ok(false);
                    }
                    self.complete(ci, si, d, STATUS_CONNECTION_CLOSED)?;
                    return Ok(true);
                }
                if let Some(err_status) =
                    self.take_op_error(d.peer, d.connection, d.request, ERR_OP_RDMA_READ)
                {
                    self.complete(ci, si, d, err_status)?;
                    return Ok(true);
                }
                if j.phase == 0 {
                    let mut req =
                        FrameHeader::control(Kind::RdmaReadReq, self.node, d.peer, self.epoch);
                    req.connection = d.connection;
                    req.request = d.request;
                    req.rkey = d.rkey;
                    req.remote_offset = d.remote_offset;
                    req.total = d.length as u64;
                    req.aux = d.length as u64;
                    Self::push(&mut self.outgoing, req)?;
                    j.phase = 1;
                    j.started = now;
                    self.read_index
                        .insert((d.peer, d.connection, d.request), (ci, si));
                    progressed = true;
                } else if j.phase == 2 {
                    // Borrowed local pointer path: wait until client copied out of IPC slot.
                    if self.clients[ci]
                        .as_ref()
                        .unwrap()
                        .ipc
                        .consumed(si)?
                        .is_none()
                    {
                        return Ok(false);
                    }
                    self.complete(ci, si, d, STATUS_OK)?;
                    return Ok(true);
                } else {
                    return Ok(false);
                }
            }
            OP_CLOSE => {
                if d.flags == 2 {
                    if self.listeners.get(&d.listener) == Some(&ci) {
                        self.listeners.remove(&d.listener);
                        self.opens.retain(|h| h.aux != d.listener);
                    }
                } else {
                    self.connections.remove(&(d.peer, d.connection));
                    self.control(Kind::Close, d)?;
                    self.purge_connection(d.peer, d.connection);
                }
                self.complete(ci, si, d, STATUS_OK)?;
                return Ok(true);
            }
            _ => {
                self.complete(ci, si, d, STATUS_GENERAL_ERR)?;
                return Ok(true);
            }
        }
        if progressed {
            self.clients[ci].as_mut().unwrap().set_job(si, Some(j));
        }
        Ok(progressed)
    }

    fn purge_connection(&mut self, peer: u32, connection: u64) {
        for q in [&mut self.opens, &mut self.rts, &mut self.rdma_errors] {
            q.retain(|h| h.source != peer || h.connection != connection);
        }
        self.grants
            .retain(|&(p, c, _, _), _| p != peer || c != connection);
        self.acks
            .retain(|&(p, c, _, _), _| p != peer || c != connection);
        self.rdma_write_acks
            .retain(|&(p, c, _), _| p != peer || c != connection);
        self.recv_index
            .retain(|&(p, c, _), _| p != peer || c != connection);
        self.read_index
            .retain(|&(p, c, _), _| p != peer || c != connection);
        self.inbound_writes
            .retain(|&(p, c, _), _| p != peer || c != connection);
        self.imm_notices
            .retain(|n| n.peer != peer || n.connection != connection);
    }

    fn drop_client(&mut self, ci: usize) -> Result<()> {
        let mut closing = Vec::new();
        for (&(peer, connection), c) in &self.connections {
            if c.client == ci {
                closing.push((peer, connection));
            }
        }
        for (peer, connection) in closing {
            self.purge_connection(peer, connection);
            self.control(
                Kind::Close,
                Descriptor {
                    peer,
                    connection,
                    ..Default::default()
                },
            )?;
        }
        self.connections.retain(|_, c| c.client != ci);
        self.opens
            .retain(|h| self.listeners.get(&h.aux) != Some(&ci));
        self.listeners.retain(|_, owner| *owner != ci);
        self.read_responders.retain(|r| r.target_client != ci);
        self.recv_index.retain(|_, &mut (c, _)| c != ci);
        self.read_index.retain(|_, &mut (c, _)| c != ci);
        self.mrs.drop_client(ci);
        self.clients[ci] = None;
        self.active_clients.retain(|&c| c != ci);
        Ok(())
    }
}
