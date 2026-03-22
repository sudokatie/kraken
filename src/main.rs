//! Kraken - A distributed SQL database.

use clap::{Parser, Subcommand};

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

        /// Peer addresses for cluster
        #[arg(long)]
        peers: Option<String>,
    },

    /// Connect to a database server
    Client {
        /// Server host
        #[arg(long, default_value = "localhost")]
        host: String,

        /// Server port
        #[arg(long, default_value = "5432")]
        port: u16,
    },

    /// Show cluster status
    Status {
        /// Server host
        #[arg(long, default_value = "localhost")]
        host: String,

        /// Server port
        #[arg(long, default_value = "5432")]
        port: u16,
    },
}

fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server { data_dir, port, peers } => {
            println!("Starting kraken server on port {}", port);
            println!("Data directory: {}", data_dir);
            if let Some(peers) = peers {
                println!("Peers: {}", peers);
            }
            // TODO: Start server
        }
        Commands::Client { host, port } => {
            println!("Connecting to {}:{}", host, port);
            // TODO: Start REPL
        }
        Commands::Status { host, port } => {
            println!("Checking status of {}:{}", host, port);
            // TODO: Query status
        }
    }
}
