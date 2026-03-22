//! TCP server.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::Result;
use super::protocol::{Request, Response, QueryResult, StatusInfo, HEADER_SIZE, encode_message, decode_length, decode_message};

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Listen address.
    pub addr: SocketAddr,
    /// Node ID.
    pub node_id: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:5432".parse().unwrap(),
            node_id: 1,
        }
    }
}

/// Database server.
pub struct Server {
    config: ServerConfig,
    /// Placeholder for actual database state.
    term: Arc<Mutex<u64>>,
}

impl Server {
    /// Create a new server.
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            term: Arc::new(Mutex::new(0)),
        }
    }

    /// Run the server.
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.config.addr).await?;
        println!("Listening on {}", self.config.addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("Connection from {}", addr);

            let node_id = self.config.node_id;
            let term = self.term.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, node_id, term).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    }
}

/// Handle a single client connection.
async fn handle_connection(
    mut socket: TcpStream,
    node_id: u64,
    term: Arc<Mutex<u64>>,
) -> Result<()> {
    let mut header = [0u8; HEADER_SIZE];

    loop {
        // Read message length
        match socket.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let len = decode_length(&header);
        let mut data = vec![0u8; len];
        socket.read_exact(&mut data).await?;

        // Decode request
        let request: Request = match decode_message(&data) {
            Some(r) => r,
            None => {
                let resp = Response::Error("Invalid request".into());
                let encoded = encode_message(&resp);
                socket.write_all(&encoded).await?;
                continue;
            }
        };

        // Handle request
        let response = match request {
            Request::Ping => Response::Pong,
            Request::Status => {
                let t = *term.lock().await;
                Response::Status(StatusInfo {
                    node_id,
                    term: t,
                    state: "follower".into(),
                    leader_id: None,
                    peer_count: 0,
                    commit_index: 0,
                })
            }
            Request::Query(sql) => {
                // Placeholder: just return empty result
                Response::QueryResult(QueryResult {
                    columns: vec!["result".into()],
                    rows: vec![vec![format!("Received: {}", sql)]],
                    rows_affected: 0,
                })
            }
        };

        // Send response
        let encoded = encode_message(&response);
        socket.write_all(&encoded).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.node_id, 1);
    }
}
