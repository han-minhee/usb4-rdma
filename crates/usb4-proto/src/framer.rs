//! Cursor state for a caller-owned receive buffer, including registered I/O buffers.

use crate::{
    Error, Result,
    protocol::{FrameHeader, HEADER, decode_header, verify_crc},
};

#[derive(Default)]
pub struct Framer {
    start: usize,
    end: usize,
    header: Option<FrameHeader>,
}

impl Framer {
    pub fn end(&self) -> usize {
        self.end
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    pub fn filled(&mut self, count: usize, capacity: usize) -> Result<()> {
        self.end = self
            .end
            .checked_add(count)
            .filter(|&end| end <= capacity)
            .ok_or(Error::Protocol("receive buffer overrun"))?;
        Ok(())
    }

    /// Borrow the next validated frame. Keep it pending until the consumer accepts it.
    pub fn frame<'a>(&mut self, buffer: &'a [u8]) -> Result<Option<(FrameHeader, &'a [u8])>> {
        let bytes = buffer
            .get(self.start..self.end)
            .ok_or(Error::Protocol("receive buffer range"))?;
        if self.header.is_none() && bytes.len() >= HEADER {
            self.header = Some(decode_header(bytes)?);
        }
        let Some(header) = self.header else {
            return Ok(None);
        };
        let Some(frame) = bytes.get(..HEADER + header.length as usize) else {
            return Ok(None);
        };
        verify_crc(frame)?;
        Ok(Some((header, &frame[HEADER..])))
    }

    pub fn consume(&mut self) {
        let header = self.header.take().expect("consume an accepted frame");
        self.start += HEADER + header.length as usize;
    }

    /// Make room for the next read after parsing all complete buffered frames.
    pub fn compact(&mut self, buffer: &mut [u8]) {
        if self.start != 0 {
            buffer.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
    }
}
