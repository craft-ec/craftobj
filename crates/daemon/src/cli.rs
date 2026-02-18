//! CraftOBJ CLI — connects to a running daemon via IPC
//!
//! Usage:
//!   craftobj-cli [--socket <PATH>] <COMMAND> [ARGS...]
//!
//! Commands:
//!   status                          Show daemon status
//!   peers                           List connected peers
//!   publish <FILE> [--encrypted]    Publish a file
//!   fetch <CID> <OUTPUT> [--key K]  Fetch content by CID
//!   list                            List stored content
//!   pin <CID>                       Pin content
//!   unpin <CID>                     Unpin content
//!   access grant <CID> ...          Grant access (PRE)
//!   access list <CID>               List authorized DIDs

use craftec_ipc::IpcClient;
use craftobj_daemon::service;

fn usage() -> ! {
    eprintln!("CraftOBJ CLI");
    eprintln!();
    eprintln!("Usage: craftobj-cli [--socket <PATH>] <COMMAND> [ARGS...]");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  status                            Show daemon status");
    eprintln!("  peers                             List connected peers");
    eprintln!("  publish <FILE> [--encrypted]      Publish a file");
    eprintln!("  fetch <CID> <OUTPUT> [--key HEX]  Fetch content by CID");
    eprintln!("  list                              List stored content");
    eprintln!("  pin <CID>                         Pin content");
    eprintln!("  unpin <CID>                       Unpin content");
    eprintln!("  access list <CID>                 List authorized DIDs");
    std::process::exit(1);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut socket_path: Option<String> = None;
    let mut cmd_start = 1;

    // Parse global options
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--socket" | "-s" => {
                i += 1;
                if i < args.len() {
                    socket_path = Some(args[i].clone());
                    cmd_start = i + 1;
                }
            }
            "--help" | "-h" => usage(),
            _ => break,
        }
        i += 1;
    }

    let socket = socket_path.unwrap_or_else(service::default_socket_path);
    let client = IpcClient::new(&socket);

    let cmd_args = &args[cmd_start..];
    if cmd_args.is_empty() {
        usage();
    }

    let result = match cmd_args[0].as_str() {
        "status" => client.send_request("status", None).await,
        "peers" => client.send_request("peers", None).await,
        "list" => client.send_request("list", None).await,

        "publish" => {
            if cmd_args.len() < 2 {
                eprintln!("Usage: craftobj-cli publish <FILE> [--encrypted]");
                std::process::exit(1);
            }
            let path = &cmd_args[1];
            let encrypted = cmd_args.iter().any(|a| a == "--encrypted");
            let params = serde_json::json!({
                "path": path,
                "encrypted": encrypted,
            });
            client.send_request("publish", Some(params)).await
        }

        "fetch" => {
            if cmd_args.len() < 3 {
                eprintln!("Usage: craftobj-cli fetch <CID> <OUTPUT> [--key HEX]");
                std::process::exit(1);
            }
            let cid = &cmd_args[1];
            let output = &cmd_args[2];
            let key = cmd_args.windows(2).find(|w| w[0] == "--key").map(|w| w[1].clone());
            let mut params = serde_json::json!({
                "cid": cid,
                "output": output,
            });
            if let Some(k) = key {
                params["key"] = serde_json::Value::String(k);
            }
            client.send_request("fetch", Some(params)).await
        }

        "pin" => {
            if cmd_args.len() < 2 {
                eprintln!("Usage: craftobj-cli pin <CID>");
                std::process::exit(1);
            }
            let params = serde_json::json!({ "cid": cmd_args[1] });
            client.send_request("pin", Some(params)).await
        }

        "unpin" => {
            if cmd_args.len() < 2 {
                eprintln!("Usage: craftobj-cli unpin <CID>");
                std::process::exit(1);
            }
            let params = serde_json::json!({ "cid": cmd_args[1] });
            client.send_request("unpin", Some(params)).await
        }

        "access" => {
            if cmd_args.len() < 2 {
                eprintln!("Usage: craftobj-cli access <list|grant> <CID> ...");
                std::process::exit(1);
            }
            match cmd_args[1].as_str() {
                "list" => {
                    if cmd_args.len() < 3 {
                        eprintln!("Usage: craftobj-cli access list <CID>");
                        std::process::exit(1);
                    }
                    let params = serde_json::json!({ "cid": cmd_args[2] });
                    client.send_request("access.list", Some(params)).await
                }
                "grant" => {
                    // access grant <CID> <CREATOR_SECRET> <RECIPIENT_PUBKEY> <CONTENT_KEY>
                    if cmd_args.len() < 6 {
                        eprintln!("Usage: craftobj-cli access grant <CID> <CREATOR_SECRET> <RECIPIENT_PUBKEY> <CONTENT_KEY>");
                        std::process::exit(1);
                    }
                    let params = serde_json::json!({
                        "cid": cmd_args[2],
                        "creator_secret": cmd_args[3],
                        "recipient_pubkey": cmd_args[4],
                        "content_key": cmd_args[5],
                    });
                    client.send_request("access.grant", Some(params)).await
                }
                other => {
                    eprintln!("Unknown access subcommand: {}", other);
                    std::process::exit(1);
                }
            }
        }

        other => {
            eprintln!("Unknown command: {}", other);
            usage();
        }
    };

    match result {
        Ok(value) => {
            println!("{}", serde_json::to_string_pretty(&value).unwrap());
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
