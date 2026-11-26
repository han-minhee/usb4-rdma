use crate::{Error, Result};

pub struct Pool<T> {
    slots: Vec<Slot<T>>,
    cursor: usize,
}

struct Slot<T> {
    generation: u32,
    value: Option<T>,
}

impl<T> Pool<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0 && capacity < u32::MAX as usize);
        Self {
            slots: (0..capacity)
                .map(|_| Slot {
                    generation: 0,
                    value: None,
                })
                .collect(),
            cursor: 0,
        }
    }

    pub fn insert(&mut self, value: T) -> Result<u64> {
        let capacity = self.slots.len();
        for d in 0..capacity {
            let i = (self.cursor + d) % self.slots.len();
            let s = &mut self.slots[i];
            if s.value.is_none() && s.generation < u32::MAX {
                s.generation += 1;
                s.value = Some(value);
                self.cursor = (i + 1) % capacity;
                return Ok(((s.generation as u64) << 32) | (i as u64 + 1));
            }
        }
        Err(Error::Exhausted)
    }

    fn slot(&self, id: u64) -> Result<&Slot<T>> {
        let i = (id as u32).checked_sub(1).ok_or(Error::Stale)? as usize;
        let s = self.slots.get(i).ok_or(Error::Stale)?;
        if s.generation != (id >> 32) as u32 || s.value.is_none() {
            return Err(Error::Stale);
        }
        Ok(s)
    }

    pub fn get(&self, id: u64) -> Result<&T> {
        Ok(self.slot(id)?.value.as_ref().expect("occupied"))
    }

    pub fn get_mut(&mut self, id: u64) -> Result<&mut T> {
        self.slot(id)?;
        Ok(self.slots[(id as u32 - 1) as usize]
            .value
            .as_mut()
            .expect("occupied"))
    }

    pub fn remove(&mut self, id: u64) -> Result<T> {
        self.slot(id)?;
        Ok(self.slots[(id as u32 - 1) as usize]
            .value
            .take()
            .expect("occupied"))
    }

    /// Snapshot active generation handles into caller-owned bounded storage.
    pub fn active_ids(&self, out: &mut [u64]) -> usize {
        assert!(out.len() >= self.slots.len());
        let mut n = 0;
        for (i, slot) in self.slots.iter().enumerate() {
            if slot.value.is_some() {
                out[n] = ((slot.generation as u64) << 32) | (i as u64 + 1);
                n += 1;
            }
        }
        n
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub fn contains(&self, id: u64) -> bool {
        self.slot(id).is_ok()
    }

    pub fn available(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| s.value.is_none() && s.generation < u32::MAX)
            .count()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.iter().all(|s| s.value.is_none())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_and_full() {
        let mut p = Pool::new(1);
        let a = p.insert(7).unwrap();
        assert!(p.insert(8).is_err());
        assert_eq!(p.remove(a).unwrap(), 7);
        let b = p.insert(9).unwrap();
        assert_ne!(a, b);
        assert!(p.get(a).is_err());
    }
}
