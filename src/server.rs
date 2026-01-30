use crate::{Op, Request, Response};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::thread;

/// A running storage node server.
pub struct StorageServer {
    data: Arc<super::StorageNode>,
}

impl StorageServer {
    /// Start a new storage node server.
    pub fn start<A: ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let data = Arc::new(super::StorageNode::new());
        let data_clone = data.clone();

        thread::spawn(move || {
            for stream in listener.incoming() {
                if let Ok(stream) = stream {
                    let _ = handle_connection(stream, data_clone.clone());
                }
            }
        });

        Ok(Self { data })
    }

    /// Get a handle to the underlying storage node.
    pub fn data(&self) -> &super::StorageNode {
        &self.data
    }
}

/// Handle a single client connection.
pub fn handle_connection(
    mut stream: TcpStream,
    data: Arc<super::StorageNode>,
) -> std::io::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(_) => return Ok(()), // Client disconnected
        }

        let req_len = u32::from_le_bytes(len_buf) as usize;
        let mut req_buf = vec![0u8; req_len];
        stream.read_exact(&mut req_buf)?;

        let request: Request =
            match bincode::decode_from_slice(&req_buf, bincode::config::standard()) {
                Ok((req, _)) => req,
                Err(_) => return Ok(()),
            };

        let response = match request.op {
            Op::Get => {
                if let Some(value) = data.get(&request.key) {
                    Response {
                        found: true,
                        value: Some(value),
                    }
                } else {
                    Response {
                        found: false,
                        value: None,
                    }
                }
            }
            Op::Put => {
                if let Some(value) = request.value {
                    data.put(&request.key, &value);
                }
                Response {
                    found: true,
                    value: None,
                }
            }
            Op::Delete => {
                data.delete(&request.key);
                Response {
                    found: true,
                    value: None,
                }
            }
        };

        let resp_data = bincode::encode_to_vec(&response, bincode::config::standard())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        stream.write_all(&(resp_data.len() as u32).to_le_bytes())?;
        stream.write_all(&resp_data)?;
        stream.flush()?;
    }
}
