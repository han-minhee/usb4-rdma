use crate::{Error, NodeId, Result};
use std::ops::{BitAnd, BitOr};

/// Memory access permissions for registered memory regions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct AccessFlags(u32);

impl AccessFlags {
    pub const NONE: Self = Self(0);
    pub const LOCAL_READ: Self = Self(1 << 0);
    pub const LOCAL_WRITE: Self = Self(1 << 1);
    pub const REMOTE_READ: Self = Self(1 << 2);
    pub const REMOTE_WRITE: Self = Self(1 << 3);

    pub const ALL_BITS: u32 =
        Self::LOCAL_READ.0 | Self::LOCAL_WRITE.0 | Self::REMOTE_READ.0 | Self::REMOTE_WRITE.0;

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub fn from_bits(bits: u32) -> Result<Self> {
        if bits & !Self::ALL_BITS != 0 {
            return Err(Error::InvalidArg("unknown access flag bits"));
        }
        Ok(Self(bits))
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub const fn has_remote(self) -> bool {
        (self.0 & (Self::REMOTE_READ.0 | Self::REMOTE_WRITE.0)) != 0
    }
}

impl BitOr for AccessFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitAnd for AccessFlags {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum WcOpcode {
    Send = 1,
    Recv = 2,
    RdmaWrite = 3,
    RdmaRead = 4,
    RecvRdmaWithImm = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum WcStatus {
    Success = 0,
    LocalLenErr = 1,
    RemoteAccessErr = 2,
    RemoteInvalidReqErr = 3,
    TransportErr = 4,
    ConnectionClosed = 5,
    GeneralErr = 6,
}

impl WcStatus {
    pub fn from_ipc_status(status: i32) -> Self {
        match status {
            0 => Self::Success,
            -2 => Self::ConnectionClosed,
            -3 => Self::RemoteAccessErr,
            -4 => Self::RemoteInvalidReqErr,
            -5 => Self::LocalLenErr,
            -6 => Self::TransportErr,
            _ => Self::GeneralErr,
        }
    }
}

/// Work completion returned from polling a `CompletionQueue`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkCompletion {
    pub wr_id: u64,
    pub status: WcStatus,
    pub opcode: WcOpcode,
    pub byte_len: u64,
    pub tag_or_imm: u64,
    pub has_imm: bool,
    pub qp_id: u64,
}

/// Capabilities reported by a local `usb4-rdma-daemon` endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceAttr {
    pub node_id: NodeId,
    pub epoch: u64,
    pub topology_digest: [u8; 32],
    pub max_window_bytes: u32,
    pub max_chunk_bytes: u32,
    pub max_mr_count: u32,
    pub supports_one_sided_rdma: bool,
    pub zero_copy_dma: bool,
}

/// Serializable out-of-band handle identifying a listening endpoint on a cluster node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EndpointHandle {
    pub node_id: NodeId,
    pub listener_id: u64,
    pub epoch: u64,
}

impl EndpointHandle {
    pub const SIZE: usize = 20;

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..4].copy_from_slice(&self.node_id.to_le_bytes());
        out[4..12].copy_from_slice(&self.listener_id.to_le_bytes());
        out[12..20].copy_from_slice(&self.epoch.to_le_bytes());
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::SIZE {
            return Err(Error::InvalidArg("endpoint handle must be 20 bytes"));
        }
        let node_id = u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes"));
        let listener_id = u64::from_le_bytes(bytes[4..12].try_into().expect("8 bytes"));
        let epoch = u64::from_le_bytes(bytes[12..20].try_into().expect("8 bytes"));
        if listener_id == 0 || epoch == 0 {
            return Err(Error::InvalidArg(
                "endpoint handle has zero listener or epoch",
            ));
        }
        Ok(Self {
            node_id,
            listener_id,
            epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_flags_validation() {
        let rw = AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE;
        assert!(rw.contains(AccessFlags::REMOTE_READ));
        assert!(rw.has_remote());
        assert_eq!(AccessFlags::from_bits(rw.bits()).unwrap(), rw);
        assert!(AccessFlags::from_bits(1 << 16).is_err());
    }

    #[test]
    fn endpoint_handle_roundtrip() {
        let h = EndpointHandle {
            node_id: 3,
            listener_id: 0x0000_0001_0000_0007,
            epoch: 42,
        };
        let b = h.to_bytes();
        assert_eq!(EndpointHandle::from_bytes(&b).unwrap(), h);
        assert!(EndpointHandle::from_bytes(&[0; 20]).is_err());
    }
}
