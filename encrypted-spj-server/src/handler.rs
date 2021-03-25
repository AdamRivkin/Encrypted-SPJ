use serde_json::Result;
use std::io::{BufRead, BufReader, Write};

use std::net::{Shutdown, TcpStream};
use common::ste::STEServer;
use common::token::ServerCommand;

/// Waits for user commands and dispatches the commands.
///
/// # Arguments
///
/// * `stream` - TCP stream containing user inputs.
pub fn handle_client_request(mut stream: TcpStream) {
    let mut data = String::new();
    let mut buf_stream = BufReader::new(stream.try_clone().expect("Failed to clone stream"));
    let mut edb_server : Option<STEServer> = None;
    
    while match buf_stream.read_line(&mut data) {
        Ok(size) => {
            if size == 0 {
                false
            } else {
                // buf_stream.consume(size);
                let res : Result<Vec<u8>>;
                res = serde_json::from_str(&data);
                match res{
                    Ok(bincode_command) => {
                        if let Ok(ServerCommand::Setup(edb)) = &bincode::deserialize(&bincode_command[..]){
                            edb_server = Some((*edb).clone());
                            println!("Server received an encrypted database");
                        } else if let Ok(ServerCommand::Search(tk)) = &bincode::deserialize(&bincode_command[..]){
                            println!("Server received a search");
                            if let Some(ref mut edb_server_found) = edb_server {
                                let ciphertexts = edb_server_found.eval_ste((*tk).clone());
                                println!("Number ciphertexts: {}", ciphertexts.len());
                                let message = ServerCommand::SearchResponse(ciphertexts);
                                let message_bytes = &bincode::serialize(&message).unwrap()[..];
                                let mut message_json = serde_json::to_string(&message_bytes).unwrap();
                                message_json.push_str("\n");
                                stream.write_all(&message_json.as_bytes()).unwrap();
                            } else {
                                let message = ServerCommand::SearchFailure("No edb has been set up on the server yet".to_string());
                                let message_bytes = &bincode::serialize(&message).unwrap()[..];
                                let mut message_json = serde_json::to_string(&message_bytes).unwrap();
                                message_json.push_str("\n");
                                stream.write_all(&message_json.as_bytes()).unwrap();
                            }
                        } else {
                            println!("Server could not process a message");
                        }
                    },
                    Err(e) => println!("Error getting from json: {}", e)
                }
                data = String::new();
                true
            }
        }
        Err(e) => {
            stream.shutdown(Shutdown::Both).unwrap();
            println!("Shutting down due to write error: {}", e);
            std::process::exit(0);
        }
    } {}
    println!("Ending connection");
}
