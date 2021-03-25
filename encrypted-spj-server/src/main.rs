mod handler;

use std::net::TcpListener;
use std::thread;

/// Entry point for server.
///
/// Waits for user connections and creates a new thread for each connection.
fn main() {
    let host = String::from("127.0.0.1");
    let port = String::from("4000");
    
    let mut bind_addr = host.clone();
    bind_addr.push_str(":");
    bind_addr.push_str(&port);
    let listener = TcpListener::bind(bind_addr).unwrap();

    // Accept connections and process them on independent threads.
    println!("Encrypted SPJ server listening on with host {} on port {}", &host, &port);
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New connection: {}", stream.peer_addr().unwrap());
                // let server_state = Arc::clone(&server_state);
                let _handler = thread::spawn(move || {
                    // Connection succeeded.
                    handler::handle_client_request(stream); // server_state
                });
            }
            Err(e) => {
                // Connection failed.
                println!("Error: {}", e);
            }
        }
    }
    // Close the socket server.
    drop(listener);
}
