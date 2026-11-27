use super::{ERR_OP_SEND, Job, ServiceState};
use crate::Fabric;
use crate::ipc::{
    BUSY, FLAG_IPC_INLINE_READY, FLAG_IPC_RECV_IMM, FLAG_IPC_SEND_REGISTERED, READY,
    STATUS_CONNECTION_CLOSED, STATUS_LOCAL_LEN_ERR, STATUS_OK, WINDOW,
};
use crate::{
    Error, Result,
    protocol::{CHUNK, Kind},
    rdma::AccessFlags,
};
use std::time::Instant;

impl ServiceState {
    pub(super) fn advance_send(
        &mut self,
        fabric: &mut Fabric,
        ci: usize,
        si: usize,
        mut j: Job,
        now: Instant,
    ) -> Result<bool> {
        let d = j.d;
        let mut progressed = false;
        if !self
            .connections
            .get(&(d.peer, d.connection))
            .is_some_and(|c| c.client == ci)
        {
            self.complete(ci, si, d, STATUS_CONNECTION_CLOSED)?;
            return Ok(true);
        }
        if let Some(err_status) = self.take_op_error(d.peer, d.connection, d.request, ERR_OP_SEND) {
            self.complete(ci, si, d, err_status)?;
            return Ok(true);
        }
        let registered =
            d.local_mr_id != 0 && d.offset == 0 && d.flags & FLAG_IPC_SEND_REGISTERED != 0;
        let send_len = if registered {
            usize::try_from(d.total).map_err(|_| Error::Exhausted)?
        } else {
            d.length as usize
        };
        if j.phase == 0 {
            if d.offset == 0 {
                let conn = self.connections.get_mut(&(d.peer, d.connection)).unwrap();
                if d.listener != 0 && d.listener != conn.next_send {
                    return Ok(false);
                }
                if d.listener != 0 {
                    conn.next_send += 1;
                }
                if registered {
                    if self
                        .mrs
                        .begin_local_transfer(
                            ci,
                            d.local_mr_id,
                            d.local_offset,
                            d.total,
                            AccessFlags::LOCAL_READ,
                        )
                        .is_err()
                    {
                        self.complete(ci, si, d, STATUS_LOCAL_LEN_ERR)?;
                        return Ok(true);
                    }
                    j.mr_pinned = true;
                }
                let mut h = self.header(Kind::Rts, d);
                h.remote_offset = d.listener;
                h.aux = if d.flags == CHUNK as u32 {
                    CHUNK as u64
                } else {
                    WINDOW as u64
                };
                Self::push(&mut self.outgoing, h)?;
            }
            j.phase = 1;
            progressed = true;
        } else if j.phase == 1 {
            let key = (d.peer, d.connection, d.request, d.offset + j.cursor as u64);
            if let Some(grant) = self.grants.remove(&key) {
                let remaining = send_len - j.cursor;
                if grant.aux > remaining.min(WINDOW) as u64 || (grant.aux == 0 && remaining != 0) {
                    return Err(Error::Protocol("grant window"));
                }
                j.limit = grant.aux as usize;
                j.progress = 0;
                j.phase = 2;
                progressed = true;
            } else {
                return Ok(false);
            }
        }
        if j.phase == 2 {
            for _ in 0..64 {
                let n = (j.limit - j.progress).min(CHUNK);
                let mut h = self.header(Kind::Data, d);
                h.offset += (j.cursor + j.progress) as u64;
                if !fabric.can_send_data(&h)? {
                    break;
                }
                let sent = if d.local_mr_id != 0 {
                    let read_off = d.local_offset + (j.cursor + j.progress) as u64;
                    let mrs = &self.mrs;
                    let mut fill =
                        |out: &mut [u8]| mrs.read_local(ci, d.local_mr_id, read_off, out);
                    fabric.try_send_direct_at(h, n, now, &mut fill)?
                } else {
                    let ipc = &self.clients[ci].as_ref().unwrap().ipc;
                    let off = j.cursor + j.progress;
                    let mut fill = |out: &mut [u8]| ipc.read_range(si, off, out);
                    fabric.try_send_direct_at(h, n, now, &mut fill)?
                };
                if !sent {
                    break;
                }
                j.progress += n;
                j.started = now;
                progressed = true;
                if j.progress == j.limit {
                    j.phase = 3;
                    break;
                }
            }
        } else if j.phase == 3 {
            let key = (d.peer, d.connection, d.request, d.offset + j.cursor as u64);
            if let Some(ack) = self.acks.remove(&key) {
                if ack.aux != j.limit as u64 {
                    return Err(Error::Protocol("ACK size"));
                }
                j.cursor += j.limit;
                if j.cursor == send_len {
                    let mut reply = d;
                    if registered {
                        reply.offset = d.total;
                        reply.length = 0;
                    }
                    self.complete(ci, si, reply, STATUS_OK)?;
                    return Ok(true);
                }
                j.phase = 1;
                j.started = now;
                progressed = true;
            } else {
                return Ok(false);
            }
        }
        if progressed {
            self.clients[ci].as_mut().unwrap().set_job(si, Some(j));
        }
        Ok(progressed)
    }

    pub(super) fn advance_recv(
        &mut self,
        ci: usize,
        si: usize,
        mut j: Job,
        now: Instant,
    ) -> Result<bool> {
        let d = j.d;
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
        if j.phase == 0 {
            // Check for a pending RDMA_WRITE_WITH_IMM immediate notification first.
            if let Some(pos) = self
                .imm_notices
                .iter()
                .position(|n| n.peer == d.peer && n.connection == d.connection)
            {
                let notice = self.imm_notices.remove(pos).unwrap();
                let mut reply = j.d;
                reply.total = notice.byte_len;
                reply.length = 0;
                reply.tag_or_imm = notice.imm_data;
                reply.flags = FLAG_IPC_RECV_IMM;
                self.complete(ci, si, reply, STATUS_OK)?;
                return Ok(true);
            }
            if let Some((i, _)) = self
                .rts
                .iter()
                .enumerate()
                .filter(|(_, h)| {
                    h.source == d.peer
                        && h.connection == d.connection
                        && h.tag_or_imm == d.tag_or_imm
                })
                .min_by_key(|&(idx, h)| (h.remote_offset, idx))
            {
                let h = self.rts.remove(i).unwrap();
                if h.total > d.total
                    || (d.local_mr_id != 0
                        && self
                            .mrs
                            .begin_local_transfer(
                                ci,
                                d.local_mr_id,
                                d.local_offset,
                                h.total,
                                AccessFlags::LOCAL_WRITE,
                            )
                            .is_err())
                {
                    self.send_op_error(
                        d.peer,
                        d.connection,
                        h.request,
                        ERR_OP_SEND,
                        STATUS_LOCAL_LEN_ERR,
                    )?;
                    self.complete(ci, si, d, STATUS_LOCAL_LEN_ERR)?;
                    return Ok(true);
                }
                let recv_cap = if (d.flags & !FLAG_IPC_INLINE_READY) == CHUNK as u32 {
                    CHUNK
                } else {
                    WINDOW
                };
                let ceiling = (h.aux as usize).clamp(CHUNK, WINDOW).min(recv_cap);
                j.mr_pinned = d.local_mr_id != 0;
                j.limit = ceiling;
                j.d.total = h.total;
                j.d.request = h.request;
                j.d.offset = 0;
                j.d.length = 0;
                j.phase = 1;
                j.started = now;
                self.recv_index
                    .insert((d.peer, d.connection, h.request), (ci, si));
                let mut grant = self.header(Kind::Grant, j.d);
                grant.aux = (j.limit as u64).min(j.d.total - j.d.offset);
                Self::push(&mut self.outgoing, grant)?;
            } else {
                return Ok(false);
            }
        } else if j.phase == 2 {
            if d.local_mr_id == 0
                && self.clients[ci]
                    .as_ref()
                    .unwrap()
                    .ipc
                    .consumed(si)?
                    .is_none()
            {
                return Ok(false);
            }
            let mut ack = self.header(Kind::Ack, d);
            ack.aux = d.length as u64;
            Self::push(&mut self.outgoing, ack)?;
            j.d.offset += d.length as u64;
            if j.d.offset == j.d.total {
                self.complete(ci, si, j.d, STATUS_OK)?;
                return Ok(true);
            }
            j.d.length = 0;
            j.phase = 1;
            self.recv_index
                .insert((j.d.peer, j.d.connection, j.d.request), (ci, si));
            if d.local_mr_id == 0 {
                self.clients[ci]
                    .as_mut()
                    .unwrap()
                    .ipc
                    .publish(si, j.d, &[], BUSY)?;
            }
            j.cursor = 0;
            j.received = 0;
            let mut grant = self.header(Kind::Grant, j.d);
            grant.aux = (j.limit as u64).min(j.d.total - j.d.offset);
            Self::push(&mut self.outgoing, grant)?;
        } else {
            return Ok(false);
        }
        self.clients[ci].as_mut().unwrap().set_job(si, Some(j));
        Ok(true)
    }
}
