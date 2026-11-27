//! Test transports using the same framing and backpressure as native streams.

use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    sync::{Arc, Mutex},
};
use usb4_stream_hal::{LinkCapabilities, TransportKind, stream::StreamLink};

pub mod unix;

#[derive(Clone, Copy)]
pub struct CableConfig {
    pub capacity: usize,
    pub read_limit: usize,
    pub write_limit: usize,
    pub latency_ticks: u64,
}

impl Default for CableConfig {
    fn default() -> Self {
        Self {
            capacity: 256 * 1024,
            read_limit: usize::MAX,
            write_limit: usize::MAX,
            latency_ticks: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Unplug {
    BrokenPipe,
    PollError,
}

struct State {
    incoming: [VecDeque<u8>; 2],
    arrivals: [VecDeque<(u64, usize)>; 2],
    paused: bool,
    now: u64,
    config: CableConfig,
    fault: Option<Unplug>,
}

#[derive(Clone)]
pub struct Cable(Arc<Mutex<State>>);

impl Cable {
    pub fn pair(config: CableConfig) -> (Self, [Endpoint; 2]) {
        assert!(config.capacity > 0 && config.read_limit > 0 && config.write_limit > 0);
        let cable = Self(Arc::new(Mutex::new(State {
            incoming: std::array::from_fn(|_| VecDeque::with_capacity(config.capacity)),
            arrivals: std::array::from_fn(|_| VecDeque::new()),
            paused: false,
            now: 0,
            config,
            fault: None,
        })));
        let ends = std::array::from_fn(|side| Endpoint {
            cable: cable.clone(),
            side,
        });
        (cable, ends)
    }

    pub fn advance(&self, ticks: u64) {
        let mut state = self.0.lock().unwrap();
        state.now = state.now.saturating_add(ticks);
    }

    pub fn pause(&self, paused: bool) {
        self.0.lock().unwrap().paused = paused;
    }

    pub fn unplug(&self, mode: Unplug) {
        self.0.lock().unwrap().fault = Some(mode);
    }
}

pub struct Endpoint {
    cable: Cable,
    side: usize,
}

impl Endpoint {
    pub fn into_link(self, peer: u32, speed_mbps: u32) -> StreamLink {
        StreamLink::new(
            Box::new(self),
            LinkCapabilities {
                peer,
                speed_mbps,
                controller_group: 0,
                transport_kind: TransportKind::Emulated,
                kernel_copies: false,
                fixed_buffers: false,
                sqpoll: false,
                zero_copy_dma: false,
            },
        )
    }
}

impl State {
    fn check(&self, writing: bool) -> io::Result<()> {
        match self.fault {
            Some(Unplug::BrokenPipe) if writing => Err(io::Error::from_raw_os_error(libc::EPIPE)),
            Some(Unplug::PollError) => Err(io::Error::from_raw_os_error(libc::EPIPE)),
            _ => Ok(()),
        }
    }
}

impl Read for Endpoint {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let mut state = self.cable.0.lock().unwrap();
        state.check(false)?;
        if bytes.is_empty() || matches!(state.fault, Some(Unplug::BrokenPipe)) {
            return Ok(0);
        }
        if state.paused {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let limit = bytes.len().min(state.config.read_limit);
        let now = state.now;
        let due = &mut state.arrivals[self.side];
        let mut count = 0;
        while count < limit {
            match due.front_mut() {
                Some((tick, remaining)) if *tick <= now => {
                    let n = (*remaining).min(limit - count);
                    *remaining -= n;
                    count += n;
                    if *remaining == 0 {
                        due.pop_front();
                    }
                }
                _ => break,
            }
        }
        if count == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let queue = &mut state.incoming[self.side];
        let (first, second) = queue.as_slices();
        let front = count.min(first.len());
        bytes[..front].copy_from_slice(&first[..front]);
        bytes[front..count].copy_from_slice(&second[..count - front]);
        queue.drain(..count);
        Ok(count)
    }
}

impl Write for Endpoint {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut state = self.cable.0.lock().unwrap();
        state.check(true)?;
        if bytes.is_empty() {
            return Ok(0);
        }
        if state.paused {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let due = state.now.saturating_add(state.config.latency_ticks);
        let config = state.config;
        let queue = &mut state.incoming[1 - self.side];
        let count = bytes
            .len()
            .min(config.write_limit)
            .min(config.capacity - queue.len());
        if count == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        queue.extend(&bytes[..count]);
        let arrivals = &mut state.arrivals[1 - self.side];
        if let Some((_, len)) = arrivals.back_mut().filter(|(tick, _)| *tick == due) {
            *len += count;
        } else {
            arrivals.push_back((due, count));
        }
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.cable.0.lock().unwrap().check(true)
    }
}
