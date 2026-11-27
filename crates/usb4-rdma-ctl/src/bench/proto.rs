use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ControlReq {
    Hello {
        want_rdma: bool,
        client_handle: Option<[u8; 20]>,
        client_rkey: Option<u64>,
        client_mr_len: Option<usize>,
    },
    TcpPingPong {
        size: usize,
        warmup: usize,
        iters: usize,
    },
    TcpUniRecv {
        size: usize,
        iters: usize,
    },
    TcpDuplex {
        size: usize,
        iters: usize,
    },
    RdmaSendPingPong {
        size: usize,
        warmup: usize,
        iters: usize,
    },
    RdmaPreparePattern {
        size: usize,
        seed: u8,
    },
    RdmaVerifyCrc {
        offset: usize,
        size: usize,
    },
    RdmaDuplexWrite {
        size: usize,
        iters: usize,
        peer_rkey: u64,
        #[serde(default)]
        report_transfer_completion: bool,
    },
    RdmaDuplexStart,
    RdmaDuplexFinish,
    AllReduceTcp {
        elems: usize,
        warmup: usize,
        iters: usize,
    },
    AllReduceRdma {
        elems: usize,
        warmup: usize,
        iters: usize,
        peer_rkey: u64,
    },
    Quit,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ControlResp {
    HelloAck {
        rdma_available: bool,
        server_handle: Option<[u8; 20]>,
        server_rkey: Option<u64>,
        server_mr_len: Option<usize>,
        data_port: u16,
    },
    Ready,
    RdmaDuplexTransferred {
        bytes: u64,
    },
    Done {
        crc32: u32,
        bytes: u64,
    },
    Error {
        message: String,
    },
}

pub fn send_json<T: Serialize>(stream: &mut TcpStream, msg: &T) -> anyhow::Result<()> {
    let mut line = serde_json::to_string(msg)?;
    line.push('\n');
    stream.write_all(line.as_bytes())?;
    stream.flush()?;
    Ok(())
}

pub fn recv_json<T: for<'de> Deserialize<'de>>(
    reader: &mut BufReader<TcpStream>,
) -> anyhow::Result<T> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    anyhow::ensure!(n > 0, "control connection closed");
    Ok(serde_json::from_str(&line)?)
}

#[cfg(test)]
mod tests {
    use super::ControlReq;

    #[test]
    fn legacy_duplex_request_keeps_the_combined_reply() {
        let request: ControlReq = serde_json::from_str(
            r#"{"type":"RdmaDuplexWrite","size":4096,"iters":8,"peer_rkey":42}"#,
        )
        .unwrap();
        assert!(matches!(
            request,
            ControlReq::RdmaDuplexWrite {
                report_transfer_completion: false,
                ..
            }
        ));
    }
}
