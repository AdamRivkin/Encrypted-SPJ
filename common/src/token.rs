type KeyPair = ([u8; 16], [u8; 16]);
use serde::{Deserialize, Serialize};
use crate::ste::STEServer;

#[derive(Debug)]
pub enum BoolQuery {
    Eq(String, String),
    // Not(Box<BoolQuery>),
    // And(Box<BoolQuery>, Box<BoolQuery>),
    // Or(Box<BoolQuery>, Box<BoolQuery>),
    BadBool(String)
}

#[derive(Debug)]
pub enum SPJQuery {
    Select(BoolQuery, Box<SPJQuery>),
    Join(String, String, Box<SPJQuery>, Box<SPJQuery>),
    Project(Vec<String>, Box<SPJQuery>),
    Id(String),
    BadQuery(String)
}

#[derive(Debug)]
// #[derive(Serialize, Deserialize)]
pub enum HybQuery {
    Select(BoolQuery, Box<HybQuery>),
    FPJoin(String, String, Box<HybQuery>, Box<HybQuery>),
    PPJoin(String, String, Box<HybQuery>, Box<HybQuery>),
    Project(Vec<String>, Box<HybQuery>),
    Id(String),
    BadQuery(String)
}

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
pub enum HybToken {
    Id(KeyPair, String, usize),
    Select(KeyPair, Box<HybToken>, String),
    Project(Vec<KeyPair>, Box<HybToken>),
    FPJoin(KeyPair, Box<HybToken>, Box<HybToken>, String, String, bool),
    PPJoin(KeyPair, KeyPair, Box<HybToken>, Box<HybToken>, String, String, usize, usize, bool),
    BadToken,
}

#[derive(Serialize, Deserialize)]
pub enum ServerCommand {
    Setup(STEServer),
    Search(HybToken),
    SearchResponse(Vec<Vec<Vec<u8>>>),
    SearchFailure(String)
}