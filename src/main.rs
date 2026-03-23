//! Kraken - A distributed SQL database.

use clap::{Parser, Subcommand};
use std::io::{self, Write};
use std::net::SocketAddr;

use kraken::network::{Server, ServerConfig, Client, Response};

#[derive(Parser)]
#[command(name = "kraken")]
#[command(about = "A distributed SQL database")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the database server
    Server {
        /// Data directory
        #[arg(long, default_value = "./data")]
        data_dir: String,

        /// Port to listen on
        #[arg(long, default_value = "5432")]
        port: u16,

        /// Node ID
        #[arg(long, default_value = "1")]
        node_id: u64,

        /// Peer addresses (comma-separated, e.g., "node2:5432,node3:5432")
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,
    },

    /// Connect to a database server
    Client {
        /// Server host
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Server port
        #[arg(long, default_value = "5432")]
        port: u16,
    },

    /// Show cluster status
    Status {
        /// Server host
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Server port
        #[arg(long, default_value = "5432")]
        port: u16,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server { data_dir, port, node_id, peers } => {
            run_server(data_dir, port, node_id, peers).await;
        }
        Commands::Client { host, port } => {
            run_client(host, port).await;
        }
        Commands::Status { host, port } => {
            run_status(host, port).await;
        }
    }
}

async fn run_server(data_dir: String, port: u16, node_id: u64, peers: Vec<String>) {
    println!("Kraken v0.1.0");
    println!("Node ID: {}", node_id);
    println!("Data directory: {}", data_dir);
    println!("Listening on 0.0.0.0:{}", port);

    if !peers.is_empty() {
        println!("Peers: {}", peers.join(", "));
    }

    // Create data directory (include node_id for multi-node on same machine)
    let node_data_dir = format!("{}/node_{}", data_dir, node_id);
    std::fs::create_dir_all(&node_data_dir).expect("failed to create data directory");

    // Parse peer addresses
    let peer_addrs: Vec<SocketAddr> = peers.iter()
        .filter_map(|p| {
            // Handle both "host:port" and just "host" (use default port)
            if p.contains(':') {
                p.parse().ok()
            } else {
                format!("{}:5432", p).parse().ok()
            }
        })
        .collect();

    let config = ServerConfig {
        addr: format!("0.0.0.0:{}", port).parse().unwrap(),
        node_id,
        peers: peer_addrs,
    };

    let server = Server::new(config);
    if let Err(e) = server.run().await {
        eprintln!("Server error: {}", e);
    }
}

async fn run_client(host: String, port: u16) {
    let addr = format!("{}:{}", host, port).parse().unwrap();

    let mut client = match Client::connect(addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Connection failed: {}", e);
            return;
        }
    };

    println!("Connected to {}:{}", host, port);
    println!("Type SQL commands. Use Ctrl+D to exit.");
    println!();

    let stdin = io::stdin();
    let mut input = String::new();

    loop {
        print!("kraken> ");
        io::stdout().flush().unwrap();

        input.clear();
        match stdin.read_line(&mut input) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("Input error: {}", e);
                break;
            }
        }

        let sql = input.trim();
        if sql.is_empty() {
            continue;
        }

        match client.query(sql).await {
            Ok(Response::QueryResult(result)) => {
                if !result.columns.is_empty() {
                    // Print header
                    println!("{}", result.columns.join(" | "));
                    println!("{}", "-".repeat(result.columns.len() * 10));

                    // Print rows
                    for row in &result.rows {
                        println!("{}", row.join(" | "));
                    }
                    println!();
                    println!("{} row(s)", result.rows.len());
                } else if result.rows_affected > 0 {
                    println!("{} row(s) affected", result.rows_affected);
                } else {
                    println!("OK");
                }
            }
            Ok(Response::Error(msg)) => {
                eprintln!("Error: {}", msg);
            }
            Ok(Response::Redirect(addr)) => {
                println!("Redirected to leader: {}", addr);
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Query failed: {}", e);
            }
        }
        println!();
    }

    println!("Goodbye!");
}

async fn run_status(host: String, port: u16) {
    let addr = format!("{}:{}", host, port).parse().unwrap();

    let mut client = match Client::connect(addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Connection failed: {}", e);
            return;
        }
    };

    match client.status().await {
        Ok(Response::Status(info)) => {
            println!("Kraken Cluster Status");
            println!("=====================");
            println!("Node ID:      {}", info.node_id);
            println!("Term:         {}", info.term);
            println!("State:        {}", info.state);
            println!("Leader:       {}", info.leader_id.map(|id| id.to_string()).unwrap_or_else(|| "unknown".into()));
            println!("Peers:        {}", info.peer_count);
            println!("Commit Index: {}", info.commit_index);
        }
        Ok(Response::Error(msg)) => {
            eprintln!("Error: {}", msg);
        }
        Ok(_) => {
            eprintln!("Unexpected response");
        }
        Err(e) => {
            eprintln!("Status check failed: {}", e);
        }
    }
}
