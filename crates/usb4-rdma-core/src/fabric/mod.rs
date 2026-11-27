mod routes;

use crate::{
    Error, NodeId, Result,
    config::Cluster,
    protocol::{CHUNK, FLAG_DIRECT, FrameHeader, HEADER, Kind, Packet, PacketPool},
};
pub use routes::Routes;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    os::fd::RawFd,
    time::{Duration, Instant},
};
use usb4_stream_hal::{Event, FastPoll, StreamTransport};

type LocalRxCallback<'a> = &'a mut dyn FnMut(FrameHeader, &[u8]) -> Result<()>;

/// The event observed on a link when a fabric epoch failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LinkFailureCause {
    StartupTimeout,
    HeartbeatTimeout,
    Closed,
    PollError,
}

impl std::fmt::Display for LinkFailureCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::StartupTimeout => "startup-timeout",
            Self::HeartbeatTimeout => "heartbeat-timeout",
            Self::Closed => "closed",
            Self::PollError => "poll-error",
        })
    }
}

/// Local observation of a terminal link failure in a fabric epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct LinkFailure {
    /// Zero-based position in the link list supplied to the fabric.
    pub port: usize,
    pub peer: NodeId,
    pub cause: LinkFailureCause,
}

struct Port {
    peer: NodeId,
    backend: Box<dyn StreamTransport>,
    ready: bool,
    nonce: u64,
    peer_nonce: Option<u64>,
    last_probe: Instant,
    last_pong: Instant,
    last_transfer: Option<Instant>,
    challenge: u64,
    pending_challenge: Option<u64>,
    control: VecDeque<Packet>,
    direct: VecDeque<Packet>,
    transit: VecDeque<Packet>,
    incoming: VecDeque<Packet>,
    incoming_control: VecDeque<Packet>,
    credits: [usize; 2],
    receive_credits: [usize; 2],
    turn: bool,
    inflight_bytes: u64,
    control_burst: usize,
    data_tx_bytes: u64,
    data_rx_bytes: u64,
}

pub struct Fabric {
    pool: PacketPool,
    pub node: NodeId,
    pub routes: Routes,
    ports: Vec<Port>,
    depth: usize,
    delivered: VecDeque<(usize, Packet)>,
    delivered_control: VecDeque<Packet>,
    failed: bool,
    link_failure: Option<LinkFailure>,
    started: Instant,
    startup_timeout: Duration,
    active_probe_interval: Duration,
    last_timers_check: Option<Instant>,
    assignments: HashMap<(u32, u32, u64, u64), usize>,
    pub transmitted: u64,
    pub received: u64,
    pub forwarded: u64,
    pub data_tx_bytes: u64,
    pub data_rx_bytes: u64,
}

impl Fabric {
    pub fn new(c: &Cluster, node: NodeId, links: Vec<Box<dyn StreamTransport>>) -> Result<Self> {
        c.node(node)?;
        let routes = Routes::new(c)?;
        let depth = c.queue_depth;
        // A partially opened bond must never silently shrink the fabric.
        let mut expected = BTreeMap::new();
        for link in &c.links {
            let peer = if link.a == node {
                Some(link.b)
            } else if link.b == node {
                Some(link.a)
            } else {
                None
            };
            if let Some(peer) = peer {
                *expected.entry(peer).or_insert(0usize) += 1;
            }
        }
        let mut actual = BTreeMap::new();
        for link in &links {
            *actual.entry(link.capabilities().peer).or_insert(0usize) += 1;
        }
        if actual != expected {
            return Err(Error::Config(
                "physical links do not match configured peer multiplicity".into(),
            ));
        }
        let mut ports = Vec::new();
        let now = Instant::now();
        for backend in links {
            let peer = backend.capabilities().peer;
            if !c
                .links
                .iter()
                .any(|l| (l.a == node && l.b == peer) || (l.b == node && l.a == peer))
            {
                return Err(Error::Config("unconfigured physical peer".into()));
            }
            let mut control = VecDeque::with_capacity(depth * 4);
            use std::io::Read;
            let mut bytes = [0u8; 8];
            std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
            let nonce = u64::from_le_bytes(bytes).max(1);
            let mut hello = FrameHeader::control(Kind::Hello, node, peer, c.epoch);
            hello.request = nonce;
            control.push_back(Packet::new(hello, &routes.digest)?);
            ports.push(Port {
                peer,
                backend,
                ready: false,
                nonce,
                peer_nonce: None,
                last_probe: now,
                last_pong: now,
                last_transfer: None,
                challenge: 0,
                pending_challenge: None,
                control,
                direct: VecDeque::with_capacity(depth),
                transit: VecDeque::with_capacity(depth),
                incoming: VecDeque::with_capacity(depth * 2),
                incoming_control: VecDeque::with_capacity(depth * 4),
                credits: [0; 2],
                receive_credits: [0; 2],
                turn: false,
                inflight_bytes: 0,
                control_burst: 0,
                data_tx_bytes: 0,
                data_rx_bytes: 0,
            });
        }
        if ports.is_empty() {
            return Err(Error::Config("node has no links".into()));
        }
        let capacity = ports.len() * depth * 2;
        let control_capacity = ports.len() * depth * 4;
        Ok(Self {
            pool: PacketPool::new(ports.len() * depth * 2 + 16),
            node,
            routes,
            ports,
            depth,
            delivered: VecDeque::with_capacity(capacity),
            delivered_control: VecDeque::with_capacity(control_capacity),
            failed: false,
            link_failure: None,
            started: now,
            startup_timeout: Duration::from_millis(c.timeout_ms),
            active_probe_interval: Duration::from_millis(1),
            last_timers_check: None,
            assignments: HashMap::with_capacity(2048),
            transmitted: 0,
            received: 0,
            forwarded: 0,
            data_tx_bytes: 0,
            data_rx_bytes: 0,
        })
    }

    /// Limit the idle gap between progress probes after a recent transfer.
    /// The failure deadline and fully idle heartbeat cadence are independent.
    pub fn with_active_probe_interval(mut self, interval: Duration) -> Self {
        self.active_probe_interval =
            interval.clamp(Duration::from_micros(50), Duration::from_millis(1));
        self
    }

    pub fn packet_with(
        &mut self,
        h: FrameHeader,
        length: usize,
        fill: impl FnOnce(&mut [u8]) -> Result<()>,
    ) -> Result<Packet> {
        self.pool.packet_with(h, length, fill)
    }

    pub fn ready(&self) -> bool {
        !self.failed && self.ports.iter().all(|p| p.ready)
    }

    pub fn failed(&self) -> bool {
        self.failed
    }

    /// Returns link-specific evidence when fabric polling observed a terminal link failure.
    pub fn link_failure(&self) -> Option<LinkFailure> {
        self.link_failure
    }

    fn record_link_failure(&mut self, port: usize, cause: LinkFailureCause) {
        self.link_failure = Some(LinkFailure {
            port,
            peer: self.ports[port].peer,
            cause,
        });
    }

    /// Data counters in the order the caller supplied the link backends.
    pub fn link_payload_bytes(&self) -> impl ExactSizeIterator<Item = (u64, u64)> + '_ {
        self.ports
            .iter()
            .map(|p| (p.data_tx_bytes, p.data_rx_bytes))
    }

    pub fn link_fds(&self) -> Vec<RawFd> {
        self.ports
            .iter()
            .filter_map(|p| p.backend.raw_fd())
            .collect()
    }

    pub fn try_send(&mut self, mut packet: Packet) -> Result<bool> {
        if self.failed {
            return Err(Error::Disconnected);
        }
        if packet.header.source != self.node || packet.header.epoch != self.routes.epoch {
            return Err(Error::Protocol("local identity/epoch"));
        }
        packet.header.hops = 64;
        self.queue(packet)
    }

    /// Encode and submit directly into the backend TX buffer when queues allow,
    /// avoiding an intermediate `PacketPool` allocation and payload copy.
    pub fn try_send_direct(
        &mut self,
        header: FrameHeader,
        length: usize,
        fill: &mut dyn FnMut(&mut [u8]) -> Result<()>,
    ) -> Result<bool> {
        self.try_send_direct_at(header, length, Instant::now(), fill)
    }

    /// Encode and submit directly using a caller-supplied timestamp.
    pub fn try_send_direct_at(
        &mut self,
        mut header: FrameHeader,
        length: usize,
        now: Instant,
        fill: &mut dyn FnMut(&mut [u8]) -> Result<()>,
    ) -> Result<bool> {
        if self.failed {
            return Err(Error::Disconnected);
        }
        if header.source != self.node || header.epoch != self.routes.epoch {
            return Err(Error::Protocol("local identity/epoch"));
        }
        header.hops = 64;
        header.length = length as u32;
        header.validate()?;

        let Some((i, direct, assigned_port)) = self.select_port(&header)? else {
            return Ok(false);
        };
        if direct {
            header.flags |= FLAG_DIRECT;
        } else {
            header.flags &= !FLAG_DIRECT;
        }
        let data = header.kind.is_data_plane();
        let class = usize::from(!direct);
        let p = &mut self.ports[i];
        let direct_ready = if data {
            p.credits[class] > 0
                && (if direct {
                    p.direct.is_empty()
                } else {
                    p.transit.is_empty()
                })
                && (p.control.is_empty() || p.control_burst >= 16)
        } else {
            p.control.is_empty() && p.control_burst < 16
        };

        if direct_ready
            && if data {
                p.backend.can_submit_data()
            } else {
                p.backend.can_submit_direct()
            }
        {
            let key = (
                header.source,
                header.destination,
                header.connection,
                header.request,
            );
            let can_assign = !(data
                && header.total <= CHUNK as u64
                && assigned_port.is_none()
                && header.offset + header.length as u64 != header.total
                && self.assignments.len() >= 2048);
            if can_assign && p.backend.try_submit_with(&header, fill)?.is_some() {
                if !matches!(
                    header.kind,
                    Kind::Hello | Kind::HelloAck | Kind::Ping | Kind::Pong
                ) {
                    p.last_transfer = Some(now);
                }
                p.inflight_bytes += header.length as u64 + HEADER as u64;
                if data {
                    p.credits[class] -= 1;
                    p.turn = !p.turn;
                    p.control_burst = 0;
                    self.data_tx_bytes += header.length as u64;
                    p.data_tx_bytes += header.length as u64;
                } else {
                    p.control_burst += 1;
                }
                self.transmitted += 1;
                if data && header.total <= CHUNK as u64 {
                    let last = header.offset + header.length as u64 == header.total;
                    if last {
                        if assigned_port.is_some() {
                            self.assignments.remove(&key);
                        }
                    } else if assigned_port.is_none() {
                        self.assignments.insert(key, i);
                    }
                }
                return Ok(true);
            }
        }

        let packet = self.pool.packet_with(header, length, fill)?;
        self.queue(packet)
    }

    /// Check admission before copying a payload chunk into a pooled packet.
    pub fn can_send_data(&self, header: &FrameHeader) -> Result<bool> {
        if self.failed {
            return Err(Error::Disconnected);
        }
        Ok(self.select_port(header)?.is_some())
    }

    fn select_port(&self, header: &FrameHeader) -> Result<Option<(usize, bool, Option<usize>)>> {
        let next = self
            .routes
            .next(header.source, header.destination, self.node)?
            .ok_or(Error::Protocol("cannot send to self"))?;
        let direct = self.routes.path(header.source, header.destination)?.len() == 2;
        let data = header.kind.is_data_plane();
        let key = (
            header.source,
            header.destination,
            header.connection,
            header.request,
        );
        let assigned_port = if data && header.total <= CHUNK as u64 {
            self.assignments.get(&key).copied()
        } else {
            None
        };
        let count = self.ports.iter().filter(|p| p.peer == next).count();
        if count == 0 {
            return Err(Error::Disconnected);
        }
        // The configured link ordinal stays fixed even while links are becoming ready.
        // Wait for that link instead of letting a later RTS overtake this one.
        let fixed = (header.kind == Kind::Rts && header.connection != 0)
            .then_some((header.connection % count as u64) as usize);
        let stripe =
            (data && header.total > 64 * 1024).then_some(header.offset as usize / CHUNK % count);
        let mut selected = None;
        let mut score = (true, true, u64::MAX, u64::MAX);
        let mut ordinal = 0;
        for (i, p) in self.ports.iter().enumerate() {
            if p.peer != next {
                continue;
            }
            let current = ordinal;
            ordinal += 1;
            if fixed.is_some_and(|x| x != current) || assigned_port.is_some_and(|port| port != i) {
                continue;
            }
            let q = if direct { &p.direct } else { &p.transit };
            if !p.ready
                || data && q.len() >= self.depth
                || !data && p.control.len() >= self.depth * 4
            {
                continue;
            }
            let queued =
                (p.direct.len() + p.transit.len()) as u64 * CHUNK as u64 + p.inflight_bytes;
            let cost = queued.saturating_mul(1_000_000)
                / p.backend.capabilities().speed_mbps.max(1) as u64;
            let credit_stalled = data && p.credits[usize::from(!direct)] == 0;
            let candidate = (
                credit_stalled,
                stripe.is_some_and(|preferred| current != preferred),
                cost,
                p.data_tx_bytes,
            );
            if candidate < score {
                score = candidate;
                selected = Some(i);
            }
        }
        Ok(selected.map(|i| (i, direct, assigned_port)))
    }

    fn queue(&mut self, mut packet: Packet) -> Result<bool> {
        let Some((i, direct, assigned_port)) = self.select_port(&packet.header)? else {
            return Ok(false);
        };
        if direct {
            packet.header.flags |= FLAG_DIRECT;
        } else {
            packet.header.flags &= !FLAG_DIRECT;
        }
        let data = packet.header.kind.is_data_plane();
        let key = (
            packet.header.source,
            packet.header.destination,
            packet.header.connection,
            packet.header.request,
        );
        if data && packet.header.total <= CHUNK as u64 {
            let last = packet.header.offset + packet.header.length as u64 == packet.header.total;
            if last {
                if assigned_port.is_some() {
                    self.assignments.remove(&key);
                }
            } else if assigned_port.is_none() {
                if self.assignments.len() >= 2048 {
                    return Ok(false);
                }
                self.assignments.insert(key, i);
            }
        }
        let p = &mut self.ports[i];
        if data {
            if direct {
                p.direct.push_back(packet);
            } else {
                p.transit.push_back(packet);
            }
        } else {
            p.control.push_back(packet);
        }
        Ok(true)
    }

    fn credit(&mut self, port: usize, class: usize, count: usize) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        let p = &mut self.ports[port];
        p.receive_credits[class] = p.receive_credits[class]
            .checked_add(count)
            .filter(|&n| n <= self.depth)
            .ok_or(Error::Protocol("receive credit overflow"))?;
        if let Some(last) = p.control.back_mut()
            && last.header.kind == Kind::Credit
            && last.header.flags == class as u32
            && last.header.aux + count as u64 <= self.depth as u64
        {
            last.header.aux += count as u64;
            return Ok(());
        }
        if p.control.len() >= self.depth * 4 {
            return Err(Error::Exhausted);
        }
        let mut h = FrameHeader::control(Kind::Credit, self.node, p.peer, self.routes.epoch);
        h.flags = class as u32;
        h.aux = count as u64;
        p.control.push_back(Packet::new(h, &[])?);
        Ok(())
    }

    pub fn receive(&mut self) -> Result<Option<Packet>> {
        if self.failed {
            return Err(Error::Disconnected);
        }
        if let Some(packet) = self.delivered_control.pop_front() {
            return Ok(Some(packet));
        }
        if let Some((port, p)) = self.delivered.pop_front() {
            if p.header.kind.is_data_plane() {
                self.credit(port, usize::from(p.header.flags & FLAG_DIRECT == 0), 1)?;
            }
            return Ok(Some(p));
        }
        Ok(None)
    }

    pub fn shutdown(&mut self) -> Result<()> {
        self.failed = true;
        let mut error = None;
        for port in &mut self.ports {
            if let Err(e) = port.backend.shutdown() {
                error = Some(e);
            }
        }
        error.map_or(Ok(()), Err)
    }

    pub fn poll(&mut self) -> Result<bool> {
        self.poll_at(Instant::now())
    }

    pub fn poll_and_deliver(
        &mut self,
        mut on_local: impl FnMut(FrameHeader, &[u8]) -> Result<()>,
    ) -> Result<bool> {
        self.poll_with(Instant::now(), Some(&mut on_local))
    }

    pub fn poll_and_deliver_at(
        &mut self,
        now: Instant,
        mut on_local: impl FnMut(FrameHeader, &[u8]) -> Result<()>,
    ) -> Result<bool> {
        self.poll_with(now, Some(&mut on_local))
    }

    pub fn poll_at(&mut self, now: Instant) -> Result<bool> {
        self.poll_with(now, None)
    }

    pub fn flush_tx(&mut self) -> Result<()> {
        for p in &mut self.ports {
            p.backend.flush()?;
        }
        Ok(())
    }

    pub fn set_consumer_sleeping(&self, sleeping: bool) {
        for p in &self.ports {
            p.backend.set_consumer_sleeping(sleeping);
        }
    }

    fn poll_with(&mut self, now: Instant, on_local: Option<LocalRxCallback<'_>>) -> Result<bool> {
        if self.failed {
            return Err(Error::Disconnected);
        }
        let result = if !self.ready()
            && now.saturating_duration_since(self.started) >= self.startup_timeout
        {
            if let Some(index) = self.ports.iter().position(|port| !port.ready) {
                self.record_link_failure(index, LinkFailureCause::StartupTimeout);
            }
            Err(Error::Timeout)
        } else {
            self.progress(now, on_local)
        };
        match result {
            Ok(p) => Ok(p),
            Err(e) => {
                self.failed = true;
                for p in &mut self.ports {
                    let _ = p.backend.shutdown();
                }
                Err(e)
            }
        }
    }

    fn enqueue_control(&mut self, port: usize, h: FrameHeader, payload: &[u8]) -> Result<()> {
        let p = &mut self.ports[port];
        if p.control.iter().any(|q| {
            q.header.kind == h.kind && q.header.request == h.request && q.header.aux == h.aux
        }) {
            return Ok(());
        }
        if p.control.len() >= self.depth * 4 {
            return Err(Error::Exhausted);
        }
        p.control.push_back(Packet::new(h, payload)?);
        Ok(())
    }

    fn handshake(&mut self, i: usize, packet: &Packet, now: Instant) -> Result<()> {
        let h = packet.header;
        let p = &mut self.ports[i];
        if h.source != p.peer
            || h.destination != self.node
            || h.request == 0
            || packet.data() != self.routes.digest
            || (h.kind == Kind::HelloAck && h.aux != p.nonce)
            || p.peer_nonce.is_some_and(|n| n != h.request)
        {
            return Err(Error::Protocol("peer incarnation/identity/topology"));
        }
        if p.peer_nonce.is_none() {
            p.peer_nonce = Some(h.request);
            p.receive_credits = [self.depth; 2];
        }
        if h.kind == Kind::HelloAck && !p.ready {
            p.ready = true;
            p.credits = [self.depth; 2];
            p.last_pong = now;
            p.last_probe = now;
        }
        if h.kind == Kind::Hello {
            let mut ack =
                FrameHeader::control(Kind::HelloAck, self.node, p.peer, self.routes.epoch);
            ack.request = p.nonce;
            ack.aux = h.request;
            let digest = self.routes.digest;
            self.enqueue_control(i, ack, &digest)?;
        }
        Ok(())
    }

    fn heartbeat(&mut self, i: usize, h: FrameHeader, now: Instant) -> Result<()> {
        let p = &mut self.ports[i];
        if h.source != p.peer
            || h.destination != self.node
            || Some(h.request) != p.peer_nonce
            || h.offset != p.nonce
            || h.aux == 0
        {
            return Err(Error::Protocol("heartbeat identity"));
        }
        if h.kind == Kind::Pong {
            if p.pending_challenge == Some(h.aux) {
                p.pending_challenge = None;
                p.last_pong = now;
            } else if h.aux > p.challenge {
                return Err(Error::Protocol("unsolicited heartbeat response"));
            }
        } else {
            let mut reply = FrameHeader::control(Kind::Pong, self.node, p.peer, self.routes.epoch);
            reply.request = p.nonce;
            reply.offset = h.request;
            reply.aux = h.aux;
            self.enqueue_control(i, reply, &[])?;
        }
        Ok(())
    }

    fn timers(&mut self, now: Instant) -> Result<()> {
        if self.last_timers_check == Some(now) {
            return Ok(());
        }
        self.last_timers_check = Some(now);
        let interval = (self.startup_timeout / 4).max(Duration::from_millis(1));
        for i in 0..self.ports.len() {
            let p = &mut self.ports[i];
            if p.ready && now.saturating_duration_since(p.last_pong) >= self.startup_timeout {
                self.record_link_failure(i, LinkFailureCause::HeartbeatTimeout);
                return Err(Error::Timeout);
            }
            let interval = if p.ready
                && p.last_transfer
                    .is_some_and(|t| now.saturating_duration_since(t) < Duration::from_secs(1))
            {
                interval.min(self.active_probe_interval)
            } else {
                interval
            };
            if now.saturating_duration_since(p.last_probe) < interval {
                continue;
            }
            p.last_probe = now;
            let mut h = FrameHeader::control(
                if p.ready { Kind::Ping } else { Kind::Hello },
                self.node,
                p.peer,
                self.routes.epoch,
            );
            h.request = p.nonce;
            if p.ready {
                if p.pending_challenge.is_none() {
                    p.challenge = p.challenge.checked_add(1).ok_or(Error::Exhausted)?;
                    p.pending_challenge = Some(p.challenge);
                }
                h.aux = p.challenge;
                h.offset = p.peer_nonce.unwrap();
                self.enqueue_control(i, h, &[])?;
            } else {
                let digest = self.routes.digest;
                self.enqueue_control(i, h, &digest)?;
            }
        }
        Ok(())
    }

    fn progress(
        &mut self,
        now: Instant,
        mut on_local: Option<LocalRxCallback<'_>>,
    ) -> Result<bool> {
        self.timers(now)?;
        let mut progress = false;
        for i in 0..self.ports.len() {
            let mut credit_delta = [0usize; 2];
            for _ in 0..64 {
                let polled = if let Some(cb) = on_local.as_deref_mut() {
                    let p = &mut self.ports[i];
                    let node = self.node;
                    let epoch = self.routes.epoch;
                    let routes = &self.routes;
                    let can_deliver_data = self.delivered.is_empty() && p.incoming.is_empty();
                    let can_deliver_control =
                        self.delivered_control.is_empty() && p.incoming_control.is_empty();
                    let rx_credits = &mut p.receive_credits;
                    let p_data_rx = &mut p.data_rx_bytes;
                    let p_last_transfer = &mut p.last_transfer;
                    let peer = p.peer;
                    let peer_nonce = p.peer_nonce;
                    let f_received = &mut self.received;
                    let f_data_rx = &mut self.data_rx_bytes;
                    let mut on_rx = |h: FrameHeader, payload: &[u8]| -> Result<bool> {
                        let can_deliver = if h.kind.is_data_plane() {
                            can_deliver_data
                        } else {
                            can_deliver_control
                        };
                        if !can_deliver
                            || h.destination != node
                            || matches!(
                                h.kind,
                                Kind::Hello
                                    | Kind::HelloAck
                                    | Kind::Ping
                                    | Kind::Pong
                                    | Kind::Credit
                            )
                        {
                            return Ok(false);
                        }
                        if h.epoch != epoch {
                            return Err(Error::Protocol("epoch mismatch"));
                        }
                        if peer_nonce.is_none() {
                            return Err(Error::Protocol("traffic before hello"));
                        }
                        routes.verify_previous(h.source, h.destination, node, peer)?;
                        *p_last_transfer = Some(now);
                        *f_received += 1;
                        if h.kind.is_data_plane() {
                            let direct = routes.path(h.source, h.destination)?.len() == 2;
                            if (h.flags & FLAG_DIRECT != 0) != direct {
                                return Err(Error::Protocol("incorrect traffic class"));
                            }
                            let class = usize::from(!direct);
                            rx_credits[class] = rx_credits[class]
                                .checked_sub(1)
                                .ok_or(Error::Protocol("data without hop credit"))?;
                            *f_data_rx += h.length as u64;
                            *p_data_rx += h.length as u64;
                            credit_delta[class] += 1;
                        }
                        cb(h, payload)?;
                        Ok(true)
                    };
                    p.backend.poll_fast(&mut on_rx)
                } else {
                    self.ports[i]
                        .backend
                        .poll()
                        .map(|event| event.map_or(FastPoll::None, FastPoll::Event))
                };
                let fast = polled.inspect_err(|_| {
                    self.record_link_failure(i, LinkFailureCause::PollError);
                })?;
                match fast {
                    FastPoll::Delivered => {
                        progress = true;
                    }
                    FastPoll::Event(event) => {
                        progress = true;
                        match event {
                            Event::Closed => {
                                self.record_link_failure(i, LinkFailureCause::Closed);
                                return Err(Error::Disconnected);
                            }
                            Event::Sent(_, bytes) => {
                                self.ports[i].inflight_bytes = self.ports[i]
                                    .inflight_bytes
                                    .checked_sub(bytes as u64)
                                    .ok_or(Error::Protocol("completion bytes"))?;
                            }
                            Event::Received(packet) => {
                                let h = packet.header;
                                if !matches!(
                                    h.kind,
                                    Kind::Hello | Kind::HelloAck | Kind::Ping | Kind::Pong
                                ) {
                                    self.ports[i].last_transfer = Some(now);
                                }
                                self.received += 1;
                                if h.epoch != self.routes.epoch {
                                    return Err(Error::Protocol("epoch mismatch"));
                                }
                                if matches!(h.kind, Kind::Hello | Kind::HelloAck) {
                                    self.handshake(i, &packet, now)?;
                                } else if matches!(h.kind, Kind::Ping | Kind::Pong) {
                                    self.heartbeat(i, h, now)?;
                                } else if h.kind == Kind::Credit {
                                    if self.ports[i].peer_nonce.is_none() {
                                        return Err(Error::Protocol("credit before hello"));
                                    }
                                    if h.source != self.ports[i].peer
                                        || h.destination != self.node
                                        || h.flags > 1
                                        || h.aux == 0
                                    {
                                        return Err(Error::Protocol("credit identity"));
                                    }
                                    let credit = &mut self.ports[i].credits[h.flags as usize];
                                    *credit = credit
                                        .checked_add(h.aux as usize)
                                        .filter(|&n| n <= self.depth)
                                        .ok_or(Error::Protocol("credit overflow"))?;
                                } else {
                                    if self.ports[i].peer_nonce.is_none() {
                                        return Err(Error::Protocol("traffic before hello"));
                                    }
                                    self.routes.verify_previous(
                                        h.source,
                                        h.destination,
                                        self.node,
                                        self.ports[i].peer,
                                    )?;
                                    if h.kind.is_data_plane() {
                                        let direct =
                                            self.routes.path(h.source, h.destination)?.len() == 2;
                                        if (h.flags & FLAG_DIRECT != 0) != direct {
                                            return Err(Error::Protocol("incorrect traffic class"));
                                        }
                                        let available = &mut self.ports[i].receive_credits
                                            [usize::from(!direct)];
                                        *available = available
                                            .checked_sub(1)
                                            .ok_or(Error::Protocol("data without hop credit"))?;
                                        self.data_rx_bytes += h.length as u64;
                                        self.ports[i].data_rx_bytes += h.length as u64;
                                    }
                                    let port = &mut self.ports[i];
                                    let (queue, limit) = if h.kind.is_data_plane() {
                                        (&mut port.incoming, self.depth * 2)
                                    } else {
                                        (&mut port.incoming_control, self.depth * 4)
                                    };
                                    if queue.len() >= limit {
                                        return Err(Error::Exhausted);
                                    }
                                    queue.push_back(packet);
                                }
                            }
                        }
                    }
                    FastPoll::None => break,
                }
            }
            if credit_delta[0] > 0 {
                self.credit(i, 0, credit_delta[0])?;
            }
            if credit_delta[1] > 0 {
                self.credit(i, 1, credit_delta[1])?;
            }
        }
        for i in 0..self.ports.len() {
            for control in [true, false] {
                for _ in 0..32 {
                    let queue = if control {
                        &self.ports[i].incoming_control
                    } else {
                        &self.ports[i].incoming
                    };
                    let Some(mut packet) = queue.front().cloned() else {
                        break;
                    };
                    let header = packet.header;
                    let local = header.destination == self.node;
                    if local {
                        if let Some(cb) = on_local.as_deref_mut() {
                            cb(header, packet.data())?;
                            if !control {
                                self.credit(i, usize::from(header.flags & FLAG_DIRECT == 0), 1)?;
                            }
                        } else if control
                            && self.delivered_control.len() < self.delivered_control.capacity()
                        {
                            self.delivered_control.push_back(packet);
                        } else if !control && self.delivered.len() < self.delivered.capacity() {
                            self.delivered.push_back((i, packet));
                        } else {
                            break;
                        }
                    } else {
                        packet.header.hops = packet
                            .header
                            .hops
                            .checked_sub(1)
                            .filter(|&hops| hops > 0)
                            .ok_or(Error::Protocol("expired hop limit"))?;
                        if !self.queue(packet)? {
                            break;
                        }
                        self.forwarded += 1;
                        if !control {
                            self.credit(i, 1, 1)?;
                        }
                    }
                    if control {
                        self.ports[i].incoming_control.pop_front();
                    } else {
                        self.ports[i].incoming.pop_front();
                    }
                    progress = true;
                }
            }
        }
        for p in &mut self.ports {
            for _ in 0..32 {
                let mut data_choice = (2, None);
                for class in if p.turn { [1, 0] } else { [0, 1] } {
                    let q = if class == 0 { &p.direct } else { &p.transit };
                    if p.credits[class] > 0
                        && p.backend.can_submit_data()
                        && let Some(packet) = q.front()
                    {
                        data_choice = (class, Some(packet));
                        break;
                    }
                }
                let (class, packet) = if p.control_burst < 16 || data_choice.1.is_none() {
                    p.control
                        .front()
                        .map_or(data_choice, |packet| (2, Some(packet)))
                } else {
                    data_choice
                };
                if let Some(packet) = packet
                    && p.backend.try_submit(packet)?.is_some()
                {
                    let header = packet.header;
                    if !matches!(
                        header.kind,
                        Kind::Hello | Kind::HelloAck | Kind::Ping | Kind::Pong
                    ) {
                        p.last_transfer = Some(now);
                    }
                    p.inflight_bytes += header.length as u64 + HEADER as u64;
                    if class == 2 {
                        p.control.pop_front();
                        p.control_burst += 1;
                    } else {
                        if class == 0 {
                            p.direct.pop_front();
                        } else {
                            p.transit.pop_front();
                        }
                        p.credits[class] -= 1;
                        p.turn = !p.turn;
                        p.control_burst = 0;
                    }
                    self.transmitted += 1;
                    if header.kind.is_data_plane() {
                        self.data_tx_bytes += header.length as u64;
                        p.data_tx_bytes += header.length as u64;
                    }
                    progress = true;
                } else {
                    break;
                }
            }
        }
        Ok(progress)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Link, Node};
    use std::os::unix::net::UnixStream;
    use usb4_stream_hal::{LinkCapabilities, TransportKind, stream::StreamLink};

    fn link_pair() -> (StreamLink, StreamLink) {
        let (a, b) = UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        let caps = |peer| LinkCapabilities {
            peer,
            speed_mbps: 40000,
            controller_group: 0,
            transport_kind: TransportKind::Emulated,
            kernel_copies: false,
            fixed_buffers: false,
            sqpoll: false,
            zero_copy_dma: false,
        };
        (
            StreamLink::new(Box::new(a), caps(1)),
            StreamLink::new(Box::new(b), caps(0)),
        )
    }

    fn pair_cluster(timeout_ms: u64) -> Cluster {
        Cluster {
            epoch: 7,
            nodes: vec![
                Node {
                    id: 0,
                    name: "n0".into(),
                },
                Node {
                    id: 1,
                    name: "n1".into(),
                },
            ],
            links: vec![Link {
                id: "l0".into(),
                a: 0,
                b: 1,
                endpoint_a: "e0".into(),
                endpoint_b: "e1".into(),
                controller_a: "c0".into(),
                controller_b: "c1".into(),
                speed_mbps: 40000,
            }],
            queue_depth: 4,
            timeout_ms,
        }
    }

    fn bonded_pair_cluster(timeout_ms: u64) -> Cluster {
        let mut c = pair_cluster(timeout_ms);
        c.links.push(Link {
            id: "l1".into(),
            a: 0,
            b: 1,
            endpoint_a: "e2".into(),
            endpoint_b: "e3".into(),
            controller_a: "c1".into(),
            controller_b: "c1".into(),
            speed_mbps: 40000,
        });
        c
    }

    #[test]
    fn startup_timeout_reports_unready_link() {
        let (link, _peer) = link_pair();
        let mut fabric = Fabric::new(&pair_cluster(100), 0, vec![Box::new(link)]).unwrap();
        let expired = fabric.started + Duration::from_millis(100);
        assert!(matches!(fabric.poll_at(expired), Err(Error::Timeout)));
        assert_eq!(
            fabric.link_failure(),
            Some(LinkFailure {
                port: 0,
                peer: 1,
                cause: LinkFailureCause::StartupTimeout,
            })
        );
    }

    #[test]
    fn handshake_and_liveness_timeout_with_injected_time() {
        let c = pair_cluster(100);
        let (a, b) = link_pair();
        let mut f0 = Fabric::new(&c, 0, vec![Box::new(a)]).unwrap();
        let mut f1 = Fabric::new(&c, 1, vec![Box::new(b)]).unwrap();
        let base = Instant::now();
        for step in 0..200 {
            let t = base + Duration::from_millis(step);
            f0.poll_at(t).unwrap();
            f1.poll_at(t).unwrap();
            if f0.ready() && f1.ready() {
                break;
            }
        }
        assert!(f0.ready() && f1.ready());
        // Freeze peer f1; f0 must fail after timeout_ms.
        let expired = base + Duration::from_secs(5);
        assert!(matches!(f0.poll_at(expired), Err(Error::Timeout)));
        assert!(f0.failed());
        assert_eq!(
            f0.link_failure(),
            Some(LinkFailure {
                port: 0,
                peer: 1,
                cause: LinkFailureCause::HeartbeatTimeout,
            })
        );
    }

    #[test]
    fn bond_peer_close_reports_the_local_link_and_stays_terminal() {
        let c = bonded_pair_cluster(1000);
        let (a0, b0) = link_pair();
        let (a1, b1) = link_pair();
        let mut f0 = Fabric::new(&c, 0, vec![Box::new(a0), Box::new(a1)]).unwrap();
        let mut f1 = Fabric::new(&c, 1, vec![Box::new(b0), Box::new(b1)]).unwrap();
        let now = Instant::now();
        for _ in 0..10 {
            f0.poll_at(now).unwrap();
            f1.poll_at(now).unwrap();
            if f0.ready() && f1.ready() {
                break;
            }
        }
        assert!(f0.ready() && f1.ready());
        assert!(f0.link_failure().is_none());

        drop(f1.ports.remove(1));
        assert!(matches!(f0.poll_at(now), Err(Error::Disconnected)));
        assert!(f0.failed() && !f0.ready());
        let failure = Some(LinkFailure {
            port: 1,
            peer: 1,
            cause: LinkFailureCause::Closed,
        });
        assert_eq!(f0.link_failure(), failure);
        assert!(matches!(f0.poll_at(now), Err(Error::Disconnected)));
        assert_eq!(f0.link_failure(), failure);
    }

    #[test]
    fn probe_interval_applies_only_after_recent_transfer() {
        let (link, _peer) = link_pair();
        let mut fabric = Fabric::new(&pair_cluster(100), 0, vec![Box::new(link)])
            .unwrap()
            .with_active_probe_interval(Duration::from_micros(100));
        let start = fabric.started;
        let port = &mut fabric.ports[0];
        port.control.clear();
        port.ready = true;
        port.peer_nonce = Some(9);

        fabric.timers(start + Duration::from_micros(100)).unwrap();
        assert!(fabric.ports[0].control.is_empty());

        fabric.ports[0].last_transfer = Some(start);
        fabric.timers(start + Duration::from_micros(99)).unwrap();
        assert!(fabric.ports[0].control.is_empty());
        fabric.timers(start + Duration::from_micros(100)).unwrap();
        assert_eq!(fabric.ports[0].control.len(), 1);
        assert_eq!(fabric.ports[0].control[0].header.kind, Kind::Ping);
    }

    #[test]
    fn small_messages_balance_across_equal_parallel_links() {
        let c = bonded_pair_cluster(1000);
        let (a0, b0) = link_pair();
        let (a1, b1) = link_pair();
        let mut f0 = Fabric::new(&c, 0, vec![Box::new(a0), Box::new(a1)]).unwrap();
        let mut f1 = Fabric::new(&c, 1, vec![Box::new(b0), Box::new(b1)]).unwrap();
        let base = Instant::now();
        for step in 0..200 {
            let t = base + Duration::from_millis(step);
            f0.poll_at(t).unwrap();
            f1.poll_at(t).unwrap();
            if f0.ready() && f1.ready() {
                break;
            }
        }
        assert!(f0.ready() && f1.ready());

        for req in 1..=4u64 {
            let mut h = FrameHeader::control(Kind::RdmaWrite, 0, 1, 7);
            h.connection = 1;
            h.request = req;
            h.total = 4096;
            let pkt = f0
                .packet_with(h, 4096, |b| {
                    b.fill(req as u8);
                    Ok(())
                })
                .unwrap();
            assert!(f0.try_send(pkt).unwrap());
            for step in 0..20 {
                let t = base + Duration::from_millis(200 + req * 20 + step);
                f0.poll_at(t).unwrap();
                f1.poll_at(t).unwrap();
            }
            while f1.receive().unwrap().is_some() {}
        }
        let counts: Vec<(u64, u64)> = f0.link_payload_bytes().collect();
        assert_eq!(counts[0].0, 8192);
        assert_eq!(counts[1].0, 8192);
    }

    #[test]
    fn rts_waits_for_its_configured_link_and_preserves_order() {
        let c = bonded_pair_cluster(1000);
        let (a0, b0) = link_pair();
        let (a1, b1) = link_pair();
        let mut f0 = Fabric::new(&c, 0, vec![Box::new(a0), Box::new(a1)]).unwrap();
        let mut f1 = Fabric::new(&c, 1, vec![Box::new(b0), Box::new(b1)]).unwrap();
        let base = Instant::now();
        for step in 0..200 {
            let t = base + Duration::from_millis(step);
            f0.poll_at(t).unwrap();
            f1.poll_at(t).unwrap();
            if f0.ready() && f1.ready() {
                break;
            }
        }
        assert!(f0.ready() && f1.ready());

        let mut h = FrameHeader::control(Kind::Rts, 0, 1, 7);
        h.connection = 1;
        h.tag_or_imm = 42;
        // Only link 1 (connection % 2) is eligible even when just link 0 is ready.
        f0.ports[1].ready = false;
        let pkt = f0.packet_with(h, 0, |_| Ok(())).unwrap();
        assert!(!f0.try_send(pkt).unwrap());
        assert!(f0.ports.iter().all(|p| p.control.is_empty()));
        f0.ports[1].ready = true;

        for req in 1..=6u64 {
            h.request = req;
            h.remote_offset = req;
            let pkt = f0.packet_with(h, 0, |_| Ok(())).unwrap();
            assert!(f0.try_send(pkt).unwrap());
        }
        assert!(f0.ports[0].control.is_empty());
        assert_eq!(f0.ports[1].control.len(), 6);
        for step in 0..40 {
            let t = base + Duration::from_millis(250 + step);
            f0.poll_at(t).unwrap();
            f1.poll_at(t).unwrap();
        }
        let mut seqs = Vec::new();
        while let Some(pkt) = f1.receive().unwrap() {
            if pkt.header.kind == Kind::Rts {
                seqs.push(pkt.header.remote_offset);
            }
        }
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6]);
    }
}
