use usb4_rdma_core::{Error, Result, protocol::CHUNK};

/// Scheduling applies only to parallel cables to a single configured peer.
pub struct Bond {
    queued: Vec<u64>,
    speed: Vec<u32>,
}

impl Bond {
    pub fn new(speeds: &[u32]) -> Result<Self> {
        if speeds.is_empty() || speeds.contains(&0) {
            return Err(Error::Config("bond speeds".into()));
        }
        Ok(Self {
            queued: vec![0; speeds.len()],
            speed: speeds.to_vec(),
        })
    }

    pub fn select(&self, total: u64, offset: u64) -> usize {
        if total > CHUNK as u64 {
            (offset as usize / CHUNK) % self.queued.len()
        } else {
            (0..self.queued.len())
                .min_by_key(|&i| self.queued[i].saturating_mul(1_000_000) / self.speed[i] as u64)
                .unwrap()
        }
    }

    pub fn submitted(&mut self, link: usize, bytes: u64) {
        self.queued[link] = self.queued[link].saturating_add(bytes);
    }

    pub fn completed(&mut self, link: usize, bytes: u64) {
        self.queued[link] = self.queued[link].saturating_sub(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thresholds() {
        let mut b = Bond::new(&[40000, 40000]).unwrap();
        assert_eq!(b.select(CHUNK as u64 + 1, 0), 0);
        assert_eq!(b.select(CHUNK as u64 + 1, CHUNK as u64), 1);
        b.submitted(0, 100);
        assert_eq!(b.select(64, 0), 1);
    }
}
