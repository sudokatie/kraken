//! TCP client.

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::Result;
use super::protocol::{Request, Response, HEADER_SIZE, encode_message, decode_length, decode_message};

/// Database client.
pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// Connect to a server.
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }

    /// Send a request and receive response.
    async fn send_request(&mut self, request: Request) -> Result<Response> {
        // Send request
        let encoded = encode_message(&request);
        self.stream.write_all(&encoded).await?;

        // Read response
        let mut header = [0u8; HEADER_SIZE];
        self.stream.read_exact(&mut header).await?;

        let len = decode_length(&header);
        let mut data = vec![0u8; len];
        self.stream.read_exact(&mut data).await?;

        decode_message(&data)
            .ok_or_else(|| crate::Error::NetworkError("Invalid response".into()))
    }

    /// Execute a SQL query.
    pub async fn query(&mut self, sql: &str) -> Result<Response> {
        self.send_request(Request::Query(sql.to_string())).await
    }

    /// Ping the server.
    pub async fn ping(&mut self) -> Result<Response> {
        self.send_request(Request::Ping).await
    }

    /// Get server status.
    pub async fn status(&mut self) -> Result<Response> {
        self.send_request(Request::Status).await
    }
}

#[cfg(test)]
mod tests {
    // Integration tests would require running server
}
