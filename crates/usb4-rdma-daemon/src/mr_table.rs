//! Daemon-side bounded registry of client shared-memory MRs for one-sided RDMA.

use std::{collections::HashMap, io::Read, os::fd::OwnedFd};
use usb4_rdma_core::{Error, Result, rdma::AccessFlags};
use usb4_rdma_ipc::{MAX_CLIENT_MRS, mr::SharedRegion};

pub const DEFAULT_MAX_MR_BYTES: usize = 2 * 1024 * 1024 * 1024; // 2 GiB

pub struct MappedMr {
    pub rkey: u64,
    pub access: AccessFlags,
    pub region: SharedRegion,
    pub active_ops: usize,
    pub pending_dereg: bool,
}

pub struct MrTable {
    mrs: HashMap<(usize, u64), MappedMr>,
    rkeys: HashMap<u64, (usize, u64)>,
    client_counts: HashMap<usize, usize>,
    total_mapped_bytes: usize,
    max_total_bytes: usize,
    next_mr_id: u64,
}

impl MrTable {
    pub fn new(max_total_bytes: usize) -> Self {
        Self {
            mrs: HashMap::with_capacity(64),
            rkeys: HashMap::with_capacity(64),
            client_counts: HashMap::with_capacity(16),
            total_mapped_bytes: 0,
            max_total_bytes,
            next_mr_id: 1,
        }
    }

    fn gen_rkey(&self) -> Result<u64> {
        let mut f = std::fs::File::open("/dev/urandom")?;
        for _ in 0..8 {
            let mut b = [0u8; 8];
            f.read_exact(&mut b)?;
            let k = u64::from_le_bytes(b);
            if k != 0 && !self.rkeys.contains_key(&k) {
                return Ok(k);
            }
        }
        Err(Error::Exhausted)
    }

    pub fn register(
        &mut self,
        client: usize,
        fd: OwnedFd,
        len: usize,
        access: AccessFlags,
    ) -> Result<(u64, u64)> {
        let count = self.client_counts.get(&client).copied().unwrap_or(0);
        if count >= MAX_CLIENT_MRS {
            return Err(Error::Exhausted);
        }
        let daemon_writable =
            access.contains(AccessFlags::REMOTE_WRITE) || access.contains(AccessFlags::LOCAL_WRITE);
        let region = SharedRegion::map_fd(fd, len, daemon_writable)?;
        let new_total = self
            .total_mapped_bytes
            .checked_add(region.mapped_len())
            .ok_or(Error::Exhausted)?;
        if new_total > self.max_total_bytes {
            return Err(Error::Exhausted);
        }
        let mr_id = self.next_mr_id;
        self.next_mr_id = mr_id.checked_add(1).ok_or(Error::Exhausted)?;
        let rkey = if access.has_remote() {
            self.gen_rkey()?
        } else {
            0
        };
        if rkey != 0 {
            self.rkeys.insert(rkey, (client, mr_id));
        }
        self.total_mapped_bytes = new_total;
        *self.client_counts.entry(client).or_insert(0) += 1;
        self.mrs.insert(
            (client, mr_id),
            MappedMr {
                rkey,
                access,
                region,
                active_ops: 0,
                pending_dereg: false,
            },
        );
        Ok((mr_id, rkey))
    }

    /// Revokes `rkey` immediately. Returns `true` if unmapped now, `false` if deferred
    /// until in-flight read responders and registered transfers finish.
    pub fn deregister(&mut self, client: usize, mr_id: u64) -> Result<bool> {
        let Some(mr) = self.mrs.get_mut(&(client, mr_id)) else {
            return Err(Error::Stale);
        };
        if mr.rkey != 0 {
            self.rkeys.remove(&mr.rkey);
            mr.rkey = 0;
        }
        if mr.active_ops > 0 {
            mr.pending_dereg = true;
            return Ok(false);
        }
        self.remove_now(client, mr_id);
        Ok(true)
    }

    fn remove_now(&mut self, client: usize, mr_id: u64) {
        if let Some(mr) = self.mrs.remove(&(client, mr_id)) {
            if mr.rkey != 0 {
                self.rkeys.remove(&mr.rkey);
            }
            self.total_mapped_bytes = self
                .total_mapped_bytes
                .saturating_sub(mr.region.mapped_len());
            if let Some(c) = self.client_counts.get_mut(&client) {
                *c = c.saturating_sub(1);
                if *c == 0 {
                    self.client_counts.remove(&client);
                }
            }
        }
    }

    pub fn drop_client(&mut self, client: usize) {
        let ids: Vec<u64> = self
            .mrs
            .keys()
            .filter_map(|&(c, id)| (c == client).then_some(id))
            .collect();
        for id in ids {
            self.remove_now(client, id);
        }
    }

    pub fn read_local(&self, client: usize, mr_id: u64, offset: u64, out: &mut [u8]) -> Result<()> {
        let mr = self.mrs.get(&(client, mr_id)).ok_or(Error::Stale)?;
        if !mr.access.contains(AccessFlags::LOCAL_READ)
            && !mr.access.contains(AccessFlags::LOCAL_WRITE)
        {
            return Err(Error::AccessViolation);
        }
        let off = usize::try_from(offset).map_err(|_| Error::AccessViolation)?;
        mr.region.read_range(off, out)
    }

    pub fn begin_local_transfer(
        &mut self,
        client: usize,
        mr_id: u64,
        offset: u64,
        length: u64,
        access: AccessFlags,
    ) -> Result<()> {
        let mr = self.mrs.get_mut(&(client, mr_id)).ok_or(Error::Stale)?;
        if mr.pending_dereg
            || !mr.access.contains(access)
            || offset
                .checked_add(length)
                .is_none_or(|end| end > mr.region.len() as u64)
        {
            return Err(Error::AccessViolation);
        }
        mr.active_ops += 1;
        Ok(())
    }

    pub fn write_local(&self, client: usize, mr_id: u64, offset: u64, data: &[u8]) -> Result<()> {
        let mr = self.mrs.get(&(client, mr_id)).ok_or(Error::Stale)?;
        if !mr.access.contains(AccessFlags::LOCAL_WRITE) {
            return Err(Error::AccessViolation);
        }
        let off = usize::try_from(offset).map_err(|_| Error::AccessViolation)?;
        mr.region.write_range(off, data)
    }

    pub fn validate_and_write_remote(
        &self,
        target_client: usize,
        rkey: u64,
        remote_offset: u64,
        total: u64,
        chunk_offset: u64,
        data: &[u8],
    ) -> Result<()> {
        if rkey == 0 {
            return Err(Error::AccessViolation);
        }
        let &(owner, mr_id) = self.rkeys.get(&rkey).ok_or(Error::AccessViolation)?;
        if owner != target_client {
            return Err(Error::AccessViolation);
        }
        let mr = self
            .mrs
            .get(&(owner, mr_id))
            .ok_or(Error::AccessViolation)?;
        if !mr.access.contains(AccessFlags::REMOTE_WRITE) {
            return Err(Error::AccessViolation);
        }
        let end_total = remote_offset
            .checked_add(total)
            .ok_or(Error::AccessViolation)?;
        if end_total > mr.region.len() as u64 {
            return Err(Error::AccessViolation);
        }
        let write_off = remote_offset
            .checked_add(chunk_offset)
            .ok_or(Error::AccessViolation)?;
        let off = usize::try_from(write_off).map_err(|_| Error::AccessViolation)?;
        mr.region.write_range(off, data)
    }

    pub fn begin_remote_read(
        &mut self,
        target_client: usize,
        rkey: u64,
        remote_offset: u64,
        length: u64,
    ) -> Result<u64> {
        if rkey == 0 {
            return Err(Error::AccessViolation);
        }
        let &(owner, mr_id) = self.rkeys.get(&rkey).ok_or(Error::AccessViolation)?;
        if owner != target_client {
            return Err(Error::AccessViolation);
        }
        let mr = self
            .mrs
            .get_mut(&(owner, mr_id))
            .ok_or(Error::AccessViolation)?;
        if !mr.access.contains(AccessFlags::REMOTE_READ) {
            return Err(Error::AccessViolation);
        }
        let end = remote_offset
            .checked_add(length)
            .ok_or(Error::AccessViolation)?;
        if end > mr.region.len() as u64 {
            return Err(Error::AccessViolation);
        }
        mr.active_ops += 1;
        Ok(mr_id)
    }

    pub fn read_for_responder(
        &self,
        client: usize,
        mr_id: u64,
        offset: u64,
        out: &mut [u8],
    ) -> Result<()> {
        let mr = self.mrs.get(&(client, mr_id)).ok_or(Error::Stale)?;
        let off = usize::try_from(offset).map_err(|_| Error::AccessViolation)?;
        mr.region.read_range(off, out)
    }

    /// Decrements `active_ops`; returns `Some(mr_id)` if a deferred deregistration just completed.
    pub fn end_operation(&mut self, client: usize, mr_id: u64) -> Option<u64> {
        let should_remove = if let Some(mr) = self.mrs.get_mut(&(client, mr_id)) {
            mr.active_ops = mr.active_ops.saturating_sub(1);
            mr.pending_dereg && mr.active_ops == 0
        } else {
            false
        };
        if should_remove {
            self.remove_now(client, mr_id);
            Some(mr_id)
        } else {
            None
        }
    }
}
