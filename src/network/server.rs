//! TCP server with Raft integration.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tokio::time::interval;

use crate::Result;
use crate::executor::ExecutionEngine;
use crate::sql::types::Value;
use crate::raft::{RaftNode, NodeState, LogEntry, RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse};
use super::protocol::{Request, Response, QueryResult, StatusInfo, HEADER_SIZE, encode_message, decode_length, decode_message};

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Listen address.
    pub addr: SocketAddr,
    /// Node ID.
    pub node_id: u64,
    /// Peer addresses.
    pub peers: Vec<SocketAddr>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:5432".parse().unwrap(),
            node_id: 1,
            peers: Vec::new(),
        }
    }
}

/// Command to be replicated via Raft.
#[derive(Debug, Clone)]
enum RaftCommand {
    Sql(String),
}

/// Server state shared across connections.
struct ServerState {
    /// Configuration.
    config: ServerConfig,
    /// Raft node state.
    raft: RaftNode,
    /// Execution engine.
    engine: ExecutionEngine,
    /// Pending client responses (log_index -> response channel).
    pending_responses: HashMap<u64, tokio::sync::oneshot::Sender<Response>>,
    /// Peer connections.
    peer_connections: HashMap<u64, Option<TcpStream>>,
}

impl ServerState {
    fn new(config: &ServerConfig, data_dir: &str) -> Result<Self> {
        let engine = ExecutionEngine::open(data_dir)?;
        let mut raft = RaftNode::new(config.node_id);
        
        // Add peers (assuming peer IDs are sequential from 1)
        for (i, _) in config.peers.iter().enumerate() {
            let peer_id = if i as u64 + 1 >= config.node_id {
                i as u64 + 2
            } else {
                i as u64 + 1
            };
            raft.add_peer(peer_id);
        }
        
        Ok(Self {
            config: config.clone(),
            raft,
            engine,
            pending_responses: HashMap::new(),
            peer_connections: HashMap::new(),
        })
    }
    
    fn is_leader(&self) -> bool {
        self.raft.state == NodeState::Leader
    }
    
    fn leader_addr(&self) -> Option<SocketAddr> {
        self.raft.leader_id.and_then(|id| {
            let idx = if id >= self.config.node_id {
                (id - 2) as usize
            } else {
                (id - 1) as usize
            };
            self.config.peers.get(idx).copied()
        })
    }
}

/// Database server.
pub struct Server {
    config: ServerConfig,
    state: Arc<RwLock<ServerState>>,
    data_dir: String,
}

impl Server {
    /// Create a new server.
    pub fn new(config: ServerConfig) -> Self {
        let data_dir = format!("./data/node_{}", config.node_id);
        Self { 
            config: config.clone(), 
            state: Arc::new(RwLock::new(
                ServerState::new(&config, &data_dir).expect("failed to initialize server state")
            )),
            data_dir,
        }
    }

    /// Run the server.
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.config.addr).await?;
        println!("Listening on {}", self.config.addr);

        // Start Raft background tasks if we have peers
        if !self.config.peers.is_empty() {
            let state = self.state.clone();
            let peers = self.config.peers.clone();
            
            // Election/heartbeat timer task
            tokio::spawn(async move {
                run_raft_timer(state, peers).await;
            });
        } else {
            // Single node mode - become leader immediately
            let mut state = self.state.write().await;
            state.raft.become_leader();
            println!("Single node mode - became leader");
        }

        loop {
            let (socket, addr) = listener.accept().await?;
            tracing::debug!("Connection from {}", addr);

            let state = self.state.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, state).await {
                    tracing::error!("Connection error: {}", e);
                }
            });
        }
    }
}

/// Run Raft election and heartbeat timer.
async fn run_raft_timer(state: Arc<RwLock<ServerState>>, peers: Vec<SocketAddr>) {
    let mut timer = interval(Duration::from_millis(50));
    
    loop {
        timer.tick().await;
        
        let mut state = state.write().await;
        
        match state.raft.state {
            NodeState::Follower | NodeState::Candidate => {
                if state.raft.election_timeout_elapsed() {
                    // Start election
                    state.raft.start_election();
                    let req = state.raft.create_request_vote();
                    
                    // Send RequestVote to all peers
                    for (i, peer_addr) in peers.iter().enumerate() {
                        let peer_id = if i as u64 + 1 >= state.config.node_id {
                            i as u64 + 2
                        } else {
                            i as u64 + 1
                        };
                        
                        if let Ok(resp) = send_request_vote(*peer_addr, &req).await {
                            state.raft.handle_term(resp.term);
                            if resp.vote_granted {
                                state.raft.receive_vote(peer_id, resp.term, true);
                            }
                        }
                    }
                }
            }
            NodeState::Leader => {
                if state.raft.should_send_heartbeat() {
                    // Send heartbeats
                    let heartbeat = state.raft.create_heartbeat();
                    
                    for peer_addr in &peers {
                        if let Ok(resp) = send_append_entries(*peer_addr, &heartbeat).await {
                            state.raft.handle_term(resp.term);
                        }
                    }
                    
                    state.raft.heartbeat_sent();
                }
            }
        }
    }
}

/// Send RequestVote RPC to a peer.
async fn send_request_vote(addr: SocketAddr, req: &RequestVoteRequest) -> Result<RequestVoteResponse> {
    let timeout = Duration::from_millis(100);
    
    let mut stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| crate::Error::NetworkError("connection timeout".into()))?
        .map_err(|e| crate::Error::NetworkError(e.to_string()))?;
    
    // Send as internal Raft message
    let msg = bincode::serialize(&RaftMessage::RequestVote(req.clone()))
        .map_err(|e| crate::Error::Internal(e.to_string()))?;
    
    let len = (msg.len() as u32).to_le_bytes();
    stream.write_all(&[0xFF]).await?; // Raft message marker
    stream.write_all(&len).await?;
    stream.write_all(&msg).await?;
    
    // Read response
    let mut marker = [0u8; 1];
    stream.read_exact(&mut marker).await?;
    
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;
    
    let resp: RaftMessageResponse = bincode::deserialize(&data)
        .map_err(|e| crate::Error::Internal(e.to_string()))?;
    
    match resp {
        RaftMessageResponse::RequestVote(r) => Ok(r),
        _ => Err(crate::Error::Internal("unexpected response".into())),
    }
}

/// Send AppendEntries RPC to a peer.
async fn send_append_entries(addr: SocketAddr, req: &AppendEntriesRequest) -> Result<AppendEntriesResponse> {
    let timeout = Duration::from_millis(100);
    
    let mut stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| crate::Error::NetworkError("connection timeout".into()))?
        .map_err(|e| crate::Error::NetworkError(e.to_string()))?;
    
    let msg = bincode::serialize(&RaftMessage::AppendEntries(req.clone()))
        .map_err(|e| crate::Error::Internal(e.to_string()))?;
    
    let len = (msg.len() as u32).to_le_bytes();
    stream.write_all(&[0xFF]).await?;
    stream.write_all(&len).await?;
    stream.write_all(&msg).await?;
    
    let mut marker = [0u8; 1];
    stream.read_exact(&mut marker).await?;
    
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;
    
    let resp: RaftMessageResponse = bincode::deserialize(&data)
        .map_err(|e| crate::Error::Internal(e.to_string()))?;
    
    match resp {
        RaftMessageResponse::AppendEntries(r) => Ok(r),
        _ => Err(crate::Error::Internal("unexpected response".into())),
    }
}

/// Internal Raft message.
#[derive(serde::Serialize, serde::Deserialize)]
enum RaftMessage {
    RequestVote(RequestVoteRequest),
    AppendEntries(AppendEntriesRequest),
}

/// Internal Raft message response.
#[derive(serde::Serialize, serde::Deserialize)]
enum RaftMessageResponse {
    RequestVote(RequestVoteResponse),
    AppendEntries(AppendEntriesResponse),
}

/// Handle a single client connection.
async fn handle_connection(
    mut socket: TcpStream,
    state: Arc<RwLock<ServerState>>,
) -> Result<()> {
    let mut header = [0u8; 1];

    loop {
        // Read first byte to determine message type
        match socket.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        if header[0] == 0xFF {
            // Raft internal message
            handle_raft_message(&mut socket, &state).await?;
            continue;
        }

        // Client message - first byte is part of length
        let mut len_rest = [0u8; HEADER_SIZE - 1];
        socket.read_exact(&mut len_rest).await?;
        
        let mut len_buf = [0u8; HEADER_SIZE];
        len_buf[0] = header[0];
        len_buf[1..].copy_from_slice(&len_rest);
        
        let len = decode_length(&len_buf);
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
        let response = handle_client_request(request, &state).await;

        // Send response
        let encoded = encode_message(&response);
        socket.write_all(&encoded).await?;
    }

    Ok(())
}

/// Handle internal Raft message.
async fn handle_raft_message(
    socket: &mut TcpStream,
    state: &Arc<RwLock<ServerState>>,
) -> Result<()> {
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    
    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;
    
    let msg: RaftMessage = bincode::deserialize(&data)
        .map_err(|e| crate::Error::Internal(e.to_string()))?;
    
    let resp = {
        let mut state = state.write().await;
        match msg {
            RaftMessage::RequestVote(req) => {
                let resp = state.raft.handle_request_vote(req);
                RaftMessageResponse::RequestVote(resp)
            }
            RaftMessage::AppendEntries(req) => {
                // Apply committed entries to state machine
                let prev_commit = state.raft.commit_index;
                let resp = state.raft.handle_append_entries(req);
                
                // Apply newly committed entries
                if state.raft.commit_index > prev_commit {
                    for idx in (prev_commit + 1)..=state.raft.commit_index {
                        if let Some(entry) = state.raft.log.get(idx) {
                            // Apply to state machine
                            if let Ok(cmd) = String::from_utf8(entry.data.clone()) {
                                let _ = state.engine.execute(&cmd);
                            }
                        }
                    }
                    state.raft.last_applied = state.raft.commit_index;
                }
                
                RaftMessageResponse::AppendEntries(resp)
            }
        }
    };
    
    let resp_data = bincode::serialize(&resp)
        .map_err(|e| crate::Error::Internal(e.to_string()))?;
    
    let len = (resp_data.len() as u32).to_le_bytes();
    socket.write_all(&[0xFF]).await?;
    socket.write_all(&len).await?;
    socket.write_all(&resp_data).await?;
    
    Ok(())
}

/// Handle client request.
async fn handle_client_request(
    request: Request,
    state: &Arc<RwLock<ServerState>>,
) -> Response {
    match request {
        Request::Ping => Response::Pong,
        Request::Status => {
            let s = state.read().await;
            Response::Status(StatusInfo {
                node_id: s.config.node_id,
                term: s.raft.term,
                state: match s.raft.state {
                    NodeState::Follower => "follower".into(),
                    NodeState::Candidate => "candidate".into(),
                    NodeState::Leader => "leader".into(),
                },
                leader_id: s.raft.leader_id,
                peer_count: s.config.peers.len(),
                commit_index: s.raft.commit_index,
            })
        }
        Request::Query(sql) => {
            let mut s = state.write().await;
            
            // Check if we're the leader for write operations
            let is_write = sql.trim().to_uppercase().starts_with("INSERT")
                || sql.trim().to_uppercase().starts_with("UPDATE")
                || sql.trim().to_uppercase().starts_with("DELETE")
                || sql.trim().to_uppercase().starts_with("CREATE");
            
            if is_write && !s.is_leader() {
                // Redirect to leader
                if let Some(leader_addr) = s.leader_addr() {
                    return Response::Redirect(leader_addr.to_string());
                }
                return Response::Error("No leader available".into());
            }
            
            // For writes in cluster mode, replicate via Raft
            if is_write && !s.config.peers.is_empty() {
                // Append to log
                let entry = LogEntry {
                    term: s.raft.term,
                    index: s.raft.log.last_index() + 1,
                    data: sql.as_bytes().to_vec(),
                };
                s.raft.log.append(entry);
                
                // In a full implementation, we'd wait for replication
                // For now, just execute directly
            }
            
            // Execute query
            match s.engine.execute(&sql) {
                Ok(result) => {
                    let rows: Vec<Vec<String>> = result.rows.iter().map(|row| {
                        row.iter().map(value_to_string).collect()
                    }).collect();

                    Response::QueryResult(QueryResult {
                        columns: result.columns,
                        rows,
                        rows_affected: result.rows_affected as u64,
                    })
                }
                Err(e) => Response::Error(e.to_string()),
            }
        }
    }
}

/// Convert a Value to its string representation.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(n) => n.to_string(),
        Value::Real(n) => n.to_string(),
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Blob(b) => format!("<blob:{} bytes>", b.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.node_id, 1);
        assert!(config.peers.is_empty());
    }

    #[test]
    fn test_value_to_string() {
        assert_eq!(value_to_string(&Value::Null), "NULL");
        assert_eq!(value_to_string(&Value::Integer(42)), "42");
        assert_eq!(value_to_string(&Value::Real(3.14)), "3.14");
        assert_eq!(value_to_string(&Value::Text("hello".into())), "hello");
        assert_eq!(value_to_string(&Value::Boolean(true)), "true");
    }
}
