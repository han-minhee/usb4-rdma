//! RKey registry for CPU mappings served by the owning process.

use crate::{AccessFlags, Error, Result};
use std::{collections::HashMap, io::Read, ptr::NonNull};

struct Region {
    address: NonNull<u8>,
    len: usize,
    access: AccessFlags,
}

pub struct HostMemory {
    entries: HashMap<u64, Region>,
    limit: usize,
    random: std::fs::File,
}

// SAFETY: registrations have an explicit retained raw allocation contract. Moving the
// owning table does not copy or access any registered bytes. Operations serialize access.
unsafe impl Send for HostMemory {}

impl HostMemory {
    pub fn new(limit: usize) -> Result<Self> {
        Ok(Self {
            entries: HashMap::with_capacity(limit),
            limit,
            random: std::fs::File::open("/dev/urandom")?,
        })
    }

    /// Retain one region for access by the local progress thread.
    ///
    /// # Safety
    /// The application must keep this CPU mapping live until deregistration, and
    /// synchronize its accesses with remote reads and writes to the same bytes.
    pub unsafe fn register(
        &mut self,
        address: NonNull<u8>,
        len: usize,
        access: AccessFlags,
    ) -> Result<u64> {
        if len == 0
            || len > isize::MAX as usize
            || (address.as_ptr() as usize).checked_add(len).is_none()
        {
            return Err(Error::InvalidArg("host registration range"));
        }
        if self.entries.len() >= self.limit {
            return Err(Error::Exhausted);
        }
        let key = self.new_key()?;
        self.entries.insert(
            key,
            Region {
                address,
                len,
                access,
            },
        );
        Ok(key)
    }

    fn new_key(&mut self) -> Result<u64> {
        loop {
            let mut bytes = [0; 8];
            self.random.read_exact(&mut bytes)?;
            let key = u64::from_le_bytes(bytes);
            if key != 0 && !self.entries.contains_key(&key) {
                return Ok(key);
            }
        }
    }

    pub fn deregister(&mut self, key: u64) -> Result<()> {
        self.entries.remove(&key).map(|_| ()).ok_or(Error::Stale)
    }

    pub fn read(&self, key: u64, offset: u64, out: &mut [u8]) -> Result<()> {
        let src = self.range(key, offset, out.len(), AccessFlags::REMOTE_READ)?;
        // SAFETY: registration retains this readable range. Copy permits overlap.
        unsafe { std::ptr::copy(src, out.as_mut_ptr(), out.len()) };
        Ok(())
    }

    pub fn write(&mut self, key: u64, offset: u64, data: &[u8]) -> Result<()> {
        let dst = self.range(key, offset, data.len(), AccessFlags::REMOTE_WRITE)?;
        // SAFETY: registration retains an exclusively writable range for this operation.
        unsafe { std::ptr::copy(data.as_ptr(), dst, data.len()) };
        Ok(())
    }

    fn range(&self, key: u64, offset: u64, len: usize, access: AccessFlags) -> Result<*mut u8> {
        let region = self
            .entries
            .get(&key)
            .filter(|r| r.access.contains(access))
            .ok_or(Error::AccessViolation)?;
        let offset = usize::try_from(offset).map_err(|_| Error::AccessViolation)?;
        if offset.checked_add(len).is_none_or(|end| end > region.len) {
            return Err(Error::AccessViolation);
        }
        // SAFETY: checked offset is within the retained allocation or one byte past it.
        Ok(unsafe { region.address.as_ptr().add(offset) })
    }
}
