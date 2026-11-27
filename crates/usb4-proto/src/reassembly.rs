use crate::{Error, Result, protocol::CHUNK};

/// Bounded chunk bitmap; allocation occurs at request setup, never on arrival.
pub struct Reassembly {
    total: usize,
    seen: Vec<bool>,
    bytes: usize,
}

impl Reassembly {
    pub fn new(total: usize) -> Self {
        Self {
            total,
            seen: vec![false; total.max(1).div_ceil(CHUNK)],
            bytes: 0,
        }
    }

    pub fn mark(&mut self, offset: usize, length: usize) -> Result<bool> {
        if !offset.is_multiple_of(CHUNK)
            || offset.checked_add(length).is_none_or(|n| n > self.total)
            || length != (self.total - offset).min(CHUNK)
        {
            return Err(Error::Protocol("chunk shape"));
        }
        let i = offset / CHUNK;
        if self.seen.get(i).copied() != Some(false) {
            return Err(Error::Protocol("duplicate chunk"));
        }
        self.seen[i] = true;
        self.bytes += length;
        Ok(self.bytes == self.total)
    }
}

/// Bounded 32-chunk window bitmap helper (covers up to 1 MiB at 32 KiB per chunk).
pub fn mark_window_bitmap(window: usize, offset: usize, length: usize, seen: u32) -> Result<u32> {
    if !offset.is_multiple_of(CHUNK)
        || offset.checked_add(length).is_none_or(|n| n > window)
        || length != (window - offset).min(CHUNK)
    {
        return Err(Error::Protocol("chunk shape"));
    }
    let chunk = offset / CHUNK;
    if chunk >= 32 {
        return Err(Error::Protocol("window chunk index"));
    }
    let bit = 1u32 << chunk;
    if seen & bit != 0 {
        return Err(Error::Protocol("duplicate chunk"));
    }
    Ok(seen | bit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reordered_and_duplicate() {
        let mut r = Reassembly::new(CHUNK + 3);
        assert!(!r.mark(CHUNK, 3).unwrap());
        assert!(r.mark(CHUNK, 3).is_err());
        assert!(r.mark(0, CHUNK).unwrap());
    }

    #[test]
    fn window_bitmap_marks_zero_and_multi_chunk() {
        let b0 = mark_window_bitmap(0, 0, 0, 0).unwrap();
        assert_eq!(b0, 1);
        assert!(mark_window_bitmap(0, 0, 0, b0).is_err());

        let m1 = mark_window_bitmap(CHUNK + 5, CHUNK, 5, 0).unwrap();
        let m2 = mark_window_bitmap(CHUNK + 5, 0, CHUNK, m1).unwrap();
        assert_eq!(m2, 0b11);
    }
}
