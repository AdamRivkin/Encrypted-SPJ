mod parse;

use rustyline;
use serde_json;
use csv::Reader;
use std::net::{Shutdown, TcpStream};
use std::io::{ Write, BufRead, BufReader };
use std::time::Instant;
use common::token::{ ServerCommand, HybQuery };
use common::db_structs::{ DB, Relation};
use common::mm::MM;
use common::emm::EMM;
use common::ste::{ STEClient, STEServer };
use common::leakage_query_planner::Statistics;
use crate::parse::{ parse, parse_hyb, annotate_query, load_db_from_txt, annotate_from_txt };

enum Command {
    Help,
    HelpEMM,
    ConnectServer,
    DisconnectServer,
    AnnotateDB(String),
    LoadCSV(String),
    LoadDB(String),
    SetupEDB,
    SetupStatistics,
    EvalBandwidth(String),
    SearchEDB(String),
    SearchEDBHyb(String),
    ResetMM,
    AddMM(String, String),
    SearchMM(String),
    SetupEMM,
    SearchEMMRR(String),
    SearchEMMRH(String),
    Quit,
    Parse(String),
    ParseHyb(String),
    PrintSchema,
    PrintSchemaEDB,
    ToggleFull,
    CommandError(String)
}

fn check_num_args(first_word : &str, num_words : usize) -> Option<String> {
    let mut option = None;
    if first_word == "help"
        || first_word == "help-emm"
        || first_word == "setup-edb"
        || first_word == "reset-mm"
        || first_word == "setup-emm"
        || first_word == "setup-stats"
        || first_word == "quit"
        || first_word == "print-schema"
        || first_word == "print-schema-edb"
        || first_word == "toggle-full"
        || first_word == "connect-server"
        || first_word == "disconnect-server"{
        if num_words != 1 {
            option = Some(String::from(first_word) + " should have no arguments")
        }
    } else if first_word == "search-emm"
        || first_word == "search-emm-rr"
        || first_word == "search-emm-rh"
        || first_word == "load-db-txt"
        || first_word == "load-csv"
        || first_word == "annotate-db"
        || first_word == "search-mm" {
        if num_words != 2 {
            option = Some(String::from(first_word) + " should take one argument")
        }
    } else if first_word == "add-mm" {
        if num_words != 3 {
            option = Some(String::from(first_word) + " should take two arguments");
        }
    }
    option
}

fn input_to_command(input: &str) -> Command {
    let words = input.split_whitespace();
    let words: Vec<&str> = words.collect();
    if words.len() <= 0 {
        Command::CommandError(String::from("Your command must include at least one word"))
    } else {
        let arg_check = check_num_args(words[0], words.len());
        if let Some(err) = arg_check {
            Command::CommandError(String::from(err))
        } else {
            if words[0] == "help" {
                Command::Help
            } else if words[0] == "help-emm" {
                Command::HelpEMM
            } else if words[0] == "connect-server" {
                Command::ConnectServer
            } else if words[0] == "disconnect-server" {
                Command::DisconnectServer
            } else if words[0] == "load-csv" {
                Command::LoadCSV(String::from(words[1]))
            } else if words[0] == "load-db-txt" {
                Command::LoadDB(String::from(words[1]))
            } else if words[0] == "annotate-db" {
                Command::AnnotateDB(String::from(words[1]))
            } else if words[0] == "setup-edb" {
                Command::SetupEDB
            } else if words[0] == "setup-stats" {
                Command::SetupStatistics
            } else if words[0] == "reset-mm" {
                Command::ResetMM
            } else if words[0] == "add-mm" {
                Command::AddMM(String::from(words[1]), String::from(words[2]))
            }  else if words[0] == "search-mm" {
                Command::SearchMM(String::from(words[1]))
            } else if words[0] == "setup-emm" {
                Command::SetupEMM
            } else if words[0] == "search-emm-rr" {
                Command::SearchEMMRR(String::from(words[1]))
            } else if words[0] == "search-emm-rh" || words[0] == "search-emm" {
                Command::SearchEMMRH(String::from(words[1]))
            } else if words[0] == "print-schema" {
                Command::PrintSchema
            } else if words[0] == "print-schema-edb" {
                Command::PrintSchemaEDB
            } else if words[0] == "toggle-full" {
                Command::ToggleFull
            } else if words[0] == "parse" {
                let mut words_parse = input.split_whitespace();
                words_parse.next().unwrap();
                let query = String::from(words_parse.collect::<Vec<&str>>().join(" "));
                Command::Parse(query)
            } else if words[0] == "parse-hyb" {
                let mut words_parse = input.split_whitespace();
                words_parse.next().unwrap();
                let query = String::from(words_parse.collect::<Vec<&str>>().join(" "));
                Command::ParseHyb(query)
            } else if words[0] == "search-edb" {
                let mut words_search = input.split_whitespace();
                words_search.next().unwrap();
                let query = String::from(words_search.collect::<Vec<&str>>().join(" "));
                Command::SearchEDB(query)
            } else if words[0] == "search-edb-hyb" {
                let mut words_search = input.split_whitespace();
                words_search.next().unwrap();
                let query = String::from(words_search.collect::<Vec<&str>>().join(" "));
                Command::SearchEDBHyb(query)
            } else if words[0] == "eval-stats" {
                let mut words_search = input.split_whitespace();
                words_search.next().unwrap();
                let query = String::from(words_search.collect::<Vec<&str>>().join(" "));
                Command::EvalBandwidth(query)
            } else if words[0] == "quit" {
                Command::Quit
            } else {
                Command::CommandError(String::from("That is not a recognized command"))
            }
        }
    }
}


fn connect_server() -> Option<TcpStream>{
    let host = String::from("127.0.0.1");
    let port = String::from("4000");
    
    let mut bind_addr = host.clone();
    bind_addr.push_str(":");
    bind_addr.push_str(&port);

    match TcpStream::connect(bind_addr) {
        Ok(stream) => {
            Some(stream)
        },
        Err(_) => {
            None
        }
    }
}

fn load_rel_from_csv(filename : &String) -> Result<Relation, String>{
    if let Ok(mut rdr) = Reader::from_path(format!("csvs/{}.csv", filename.to_string())){
        let mut inserted_records = 0;
        
        // let mut cur_attribs = db.get_all_ats();
        
        let headers : &Vec<String> = &rdr.headers()
            .unwrap()
            .iter()
            .map(|s| {
                // let s = String::from(s);
                // if cur_attribs.contains(&s){
                    String::from(format!("{}.{}", filename, s))
                // } else {
                //     s
                // }
            })
            .collect();
        let mut rel = Relation::new_rel(headers.clone());
        
        
        for result in rdr.records() {
            match result {
                Ok(rec) => {
                    let row : Vec<String> = rec.iter().map(|s| String::from(s)).collect();
                    rel.add_row(row);
                    inserted_records = inserted_records + 1;
                }
                _ => {
                    panic!("Could not read row from CSV");
                }
            }
        }
        println!("Loaded {} records from {}", inserted_records, filename);
        Ok(rel)
    } else {
        // Ok(Relation::empty_rel())
        Err("Could not read a file from that path".to_string())
    }
}

fn main() {
    println!("Testing implementation SQL over encrypted data with structured encryption");
    print_help();
    
    let mut mm = MM::new_mm();
    let mut emm = EMM::new_emm();
    let mut db = DB::new_db();
    let mut edb_client = STEClient::new_ste_client();
    let mut edb_server_local = STEServer::new_ste_server();
    let mut stream : Option<TcpStream> = None;
    let mut stats = Statistics::new();
    
    let mut full = true;
    
    let mut rl = rustyline::Editor::<()>::new();
    if let Err(_) = rl.load_history("command-line-history.txt"){
        println!("No command line history");
    }
    
    let repeat_lines = "load-csv city\nload-csv country\nload-csv address\nload-csv customer\nload-csv film\nload-csv film_category\nload-csv category\nload-csv film_actor\nannotate-db annotate_sakila\n";
        
    let mut repeat_lines : Vec<&str> = repeat_lines.lines().collect();
    repeat_lines.reverse();
    
    loop {
        let input_line;
        if let Some(line) = repeat_lines.pop(){
            input_line = Ok(String::from(line));
        } else {
            input_line = rl.readline("> ");
        }
        if let Ok(input) = input_line{
            rl.add_history_entry(input.as_str());
            rl.save_history("command-line-history.txt").unwrap();
            let cmd = input_to_command(&input[..]);
            if let Command::Quit = cmd {
                println!("Quitting");
                break;
            } else {
                match cmd {
                    Command::Help => print_help(),
                    Command::HelpEMM => print_help_emm(),
                    Command::SetupStatistics => stats = Statistics::from_database(&mut db),
                    Command::LoadDB(filename) => db = load_db_from_txt(&filename),
                    Command::LoadCSV(filename) => {
                        match load_rel_from_csv(&filename){
                            Ok(rel) => db.add_rel(&filename, rel),
                            Err(e) => println!("Error loading csv: {}", e)
                        }
                    },
                    Command::AnnotateDB(filename) => annotate_from_txt(&filename, &mut db),
                    Command::SetupEDB => {
                        if let Some(ref mut stream_found) = stream {
                            let start = Instant::now();
                            let edb_server = edb_client.setup_ste(&mut db);
                            let message = ServerCommand::Setup(edb_server);
                            let message_bytes = &bincode::serialize(&message).unwrap()[..];
                            let db_bytes = &bincode::serialize(&db).unwrap()[..];
                            println!("The server must store {} bytes for this database", message_bytes.len());
                            println!("The unencrypted database stores {} bytes", db_bytes.len());
                            let mut message_json = serde_json::to_string(&message_bytes).unwrap();
                            message_json.push_str("\n");
                            match stream_found.write_all(&message_json.as_bytes()) {
                                Ok(_) => {
                                    let duration = start.elapsed();
                                    println!("Setting up the edb took {:?}", duration);
                                },
                                Err(e) => println!("Error writing edb to server: {}", e),
                            }
                        } else {
                            edb_server_local = edb_client.setup_ste(&mut db);
                        }
                    },
                    Command::SearchEDB(query) => {
                        let qry = annotate_query(parse(&query));
                        search_edb(qry, &mut stream, &mut edb_client, &mut edb_server_local, full);
                    },
                    Command::SearchEDBHyb(query) => {
                        let qry = parse_hyb(&query);
                        search_edb(qry, &mut stream, &mut edb_client, &mut edb_server_local, full);
                    },
                    Command::EvalBandwidth(query) => {
                        let qry = parse_hyb(&query);
                        stats.estimate_query(qry);
                    },
                    Command::ResetMM => mm = MM::new_mm(),
                    Command::AddMM(identifier, keyword) => {
                        mm.add_mm(identifier.into_bytes(), keyword.into_bytes())
                    },
                    Command::SearchMM(keyword) => mm.search_mm(&keyword.into_bytes()),
                    Command::Parse(query) => {
                        let ast = parse(&query);
                        println!("{:?}", ast);
                    },
                    Command::ParseHyb(query) => {
                        let ast = parse_hyb(&query);
                        println!("{:?}", ast);
                    },
                    Command::PrintSchema => db.print_schema(),
                    Command::PrintSchemaEDB => edb_client.print_schema(),
                    Command::ToggleFull => {
                        println!("Setting printing full relations to: {}", !full);
                        full = !full;
                    },
                    Command::CommandError(err) => println!("{}", err),
                    Command::SetupEMM => emm.setup_emm(&mut mm),
                    Command::SearchEMMRR(keyword) => emm.search_emm_rr(&keyword.into_bytes()),
                    Command::SearchEMMRH(keyword) => emm.search_emm_rh(&keyword.into_bytes()),
                    Command::ConnectServer => {
                        stream = connect_server();
                        if let None = stream { println!("Could not connect"); }
                    },
                    Command::DisconnectServer => {
                        if let Some(ref stream_found) = stream {
                            match stream_found.shutdown(Shutdown::Both) {
                                Ok(_) => println!("Shutting down connection to server"),
                                Err(e) => println!("Error disconnecting from server: {}", e)
                            }
                        }
                    },
                    _ => println!("TODO"),
                }
            }
        }
    }
    if let Some(stream_found) = stream {
        match stream_found.shutdown(Shutdown::Both) {
            Ok(_) => println!("Shutting down connection to server"),
            Err(e) => println!("Error disconnecting from server: {}", e)
        }
    }
}


fn search_edb(qry : HybQuery,
    stream : &mut Option<TcpStream>,
    edb_client : &mut STEClient,
    edb_server_local : &mut STEServer,
    full : bool){
    let start = Instant::now();
    let tk_wrap = edb_client.tokenize_ste(qry);
    if let Err(s) = &tk_wrap {
        println!("Error: {}", s);
    } else {
        let tk = tk_wrap.unwrap();
        if let Some(ref mut stream_found) = stream {
            let message = ServerCommand::Search(tk);
            let message_bytes = &bincode::serialize(&message).unwrap()[..];
            let mut message_json = serde_json::to_string(&message_bytes).unwrap();
            message_json.push_str("\n");
            match stream_found.write_all(&message_json.as_bytes()) {
                Ok(_) => { },
                Err(e) => println!("Error writing edb to server: {}", e),
            }
            let mut data = String::new();
            let mut buf_stream = BufReader::new(stream_found.try_clone().expect("Failed to clone stream"));
            
            if let Ok(_) = buf_stream.read_line(&mut data) {
                let res : serde_json::Result<Vec<u8>>;
                res = serde_json::from_str(&data);
                match res{
                    Ok(bincode_command) => {
                        println!("The server sent back {} bytes", bincode_command.len());
                        if let Ok(ServerCommand::SearchResponse(ciphertexts)) = &bincode::deserialize(&bincode_command[..]){
                            let rel = edb_client.decrypt_ste(ciphertexts.to_vec());
                            let duration = start.elapsed();
                            println!("The search took: {:?}", duration);
                            rel.print_rel(full);
                        }
                        if let Ok(ServerCommand::SearchFailure(e)) = &bincode::deserialize(&bincode_command[..]){
                            println!("Search failure: {}", e);
                        }
                    },
                    Err(e) => println!("Error getting from json: {}", e)
                }
            }
        } else {
            let ciphertexts = edb_server_local.eval_ste(tk);
            let rel = edb_client.decrypt_ste(ciphertexts);
            rel.print_rel(full);
        }
    }
}


fn print_help() {
    println!("Commands:");
    println!("    help                  - Prints the program\'s commands and instructions");
    println!("    help-emm              - Prints commands specific to testing the emm primitive");
    println!("    connect-server        - Attempts to connect to an encrypted-spj server if one is running");
    println!("    disconnect-server     - Disconnects from the server if a connection has been made");
    println!("    load-db-txt path      - Loads a database from a file");
    println!("    load-csv path         - Loads a relation into the current database from a csv");
    println!("    annotate-db path      - Loads all the annotations from a text file to the current database");
    println!("    setup-stats           - Prepares client-side statistics for bandwidth/leakage estimation");
    println!("    eval-stats            - Evaluates a hybrid query for how many rows/bytes will be returned and how many volumes will be leaked");
    println!("    load-edb filename     - Loads an encrypted database which is saved on the server under that filename");
    println!("    setup-edb             - Encrypts the currently loaded database");
    println!("    search-edb query      - Queries the edb if one has been setup");
    println!("    search-edb-hyb query  - Queries the edb with an annotated query");
    println!("    parse query           - Parses a query into selection, projection, and joins");
    println!("    parse-hyb query       - Parses a query with annotated joins. Same as spj but with JOINF and JOINP for full and partial");
    println!("    print-schema          - Prints the schema of the currently loaded database");
    println!("    print-schema-edb      - Prints the schema of the currently setup edb");
    println!("    toggle-full           - Toggles whether a search should print a full relation or a readable truncation");
    println!("    quit                  - Quits the program");
}

fn print_help_emm() {
    println!("EMM primitive commands:");
    println!("    reset-mm                  - Resets the unencrypted database");
    println!("    add-mm identifier keyword - Adds an identifier keyword pair to the mm");
    println!("    search-mm keyword         - Gets back the identifiers associatd with the keyword of the mm");
    println!("    setup-emm                 - Encrypts the current database");
    println!("    search-emm keyword        - Gets back the identifiers associated with the keyword of the emm");
    println!("    search-emm-rr keyword     - Response revealing query to the emm");
    println!("    search-emm-rh keyword     - Response hiding query to the emm");
}
