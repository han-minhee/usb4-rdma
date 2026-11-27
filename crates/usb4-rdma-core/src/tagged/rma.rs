use super::{
    endpoint::{ACCESS, Buffer, Completion},
    wire::{EAGER_LIMIT, Header, Kind, invalid},
};
use crate::host_memory::HostMemory;
use std::io;

const CHUNK: usize = EAGER_LIMIT - 8;

#[derive(Clone, Copy)]
pub struct Request {
    pub buffer: Buffer,
    pub offset: u64,
    pub key: u64,
    pub context: usize,
    pub write: bool,
    pub notify: bool,
}

pub(crate) struct Job {
    pub id: u64,
    pub peer: usize,
    request: Request,
    cursor: usize,
    in_flight: bool,
}

impl Job {
    pub fn new(id: u64, peer: usize, request: Request) -> Self {
        Self {
            id,
            peer,
            request,
            cursor: 0,
            in_flight: false,
        }
    }

    fn chunk(&self) -> usize {
        (self.request.buffer.len - self.cursor).min(CHUNK)
    }

    pub fn prepare(&mut self, slab: &mut [u8; EAGER_LIMIT]) -> Option<Header> {
        if self.in_flight {
            return None;
        }
        self.in_flight = true;
        let len = self.chunk();
        let offset = self.request.offset + self.cursor as u64;
        slab[..8].copy_from_slice(&offset.to_le_bytes());
        let (kind, body) = if self.request.write {
            slab[8..8 + len].copy_from_slice(self.request.buffer.read(self.cursor, len));
            (Kind::RmaWrite, 8 + len)
        } else {
            slab[8..16].copy_from_slice(&(len as u64).to_le_bytes());
            (Kind::RmaRead, 16)
        };
        Some(Header {
            body,
            ..Header::control(kind, self.id, self.request.key)
        })
    }

    pub fn complete_chunk(&mut self, header: Header, body: &[u8]) -> io::Result<bool> {
        if !self.in_flight
            || header.id != self.id
            || header.value != 0 && header.value != ACCESS as u64
        {
            return Err(invalid());
        }
        self.in_flight = false;
        let len = self.chunk();
        if header.value != 0 {
            if !body.is_empty() {
                return Err(invalid());
            }
            return Ok(true);
        }
        if self.request.write {
            if !body.is_empty() {
                return Err(invalid());
            }
        } else {
            if body.len() != len {
                return Err(invalid());
            }
            self.request.buffer.copy_from(self.cursor, body);
        }
        self.cursor += len;
        Ok(self.cursor == self.request.buffer.len)
    }

    pub fn completion(&self, status: i32) -> Option<Completion> {
        (self.request.notify || status != 0).then_some(Completion {
            context: self.request.context,
            address: self.peer as u64,
            len: self.cursor,
            kind: if self.request.write { 6 } else { 5 },
            status,
            ..Completion::default()
        })
    }
}

pub(crate) fn target(
    memory: &mut HostMemory,
    header: Header,
    body: &[u8],
    slab: &mut [u8; EAGER_LIMIT],
) -> io::Result<Header> {
    let offset = u64::from_le_bytes(
        body.get(..8)
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
    );
    let (result, len) = match header.kind {
        Kind::RmaRead => {
            let len = u64::from_le_bytes(body[8..].try_into().map_err(|_| invalid())?);
            if len > CHUNK as u64 {
                return Err(invalid());
            }
            let len = len as usize;
            (memory.read(header.value, offset, &mut slab[..len]), len)
        }
        Kind::RmaWrite => (memory.write(header.value, offset, &body[8..]), 0),
        _ => return Err(invalid()),
    };
    Ok(if result.is_ok() {
        Header {
            body: len,
            ..Header::control(Kind::RmaResult, header.id, 0)
        }
    } else {
        Header::control(Kind::RmaResult, header.id, ACCESS as u64)
    })
}
