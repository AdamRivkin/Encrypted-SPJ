use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::crypto_helper::{ prf, fixed_encrypt, fixed_decrypt, gen_key };
use crate::mm::MM;

pub struct EMM {
    client: EMMClient,
    server: EMMServer,
}

impl EMM {
    pub fn new_emm() -> EMM {
        EMM {
            client : EMMClient::new_emm_client(),
            server : EMMServer::new_emm_server()
        }
    }
    
    pub fn setup_emm(&mut self, mm : &mut MM){
        self.server = self.client.setup_emm(mm);
    }
    
    pub fn search_emm_rr(&mut self, keyword : &Vec<u8>){
        self.search_emm(keyword, true);
    }
    
    pub fn search_emm_rh(&mut self, keyword : &Vec<u8>){
        self.search_emm(keyword, false);
    }
    
    fn search_emm(&mut self, keyword : &Vec<u8>, rr : bool){
        let (key1, key2) = self.client.tokenize_emm(keyword);
        let results;
        if rr {
            results = self.server.eval_emm_rr(&key1[..], &key2[..]);
        } else {
            let enc_results = self.server.eval_emm_rh(&key1[..]);
            results = self.client.decrypt_emm(enc_results);
        }
        
        if let Ok(keyword_string) = std::str::from_utf8(&keyword[..]) {
            println!("Identifiers for keyword: {}", keyword_string);
            for value in results {
                match std::str::from_utf8(&value[..]) {
                    Ok(v) => println!("    {}", v),
                    Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                }
            }
        } else {
            panic!("Invalid UTF-8 sequence");
        }
    }
}

pub struct EMMClient {
    key: [u8; 16],
    last_token_key : [u8; 16]
}

impl EMMClient {
    pub fn new_emm_client() -> EMMClient {
        EMMClient {
            key : [0u8; 16],
            last_token_key : [0u8; 16]
        }
    }
    
    pub fn setup_emm(&mut self, mm : &mut MM)->EMMServer{
        let mut emm_data = HashMap::new();
        let key = gen_key();
        
        for (keyword, v) in &mm.data{
            let key_pair = prf(&keyword[..], &key[..]);
            let key1 = &key_pair[0..16];
            let key2 = &key_pair[16..];
            
            let mut c = 0;
            
            for id in v.iter(){
                let counter_bytes = &bincode::serialize(&c).unwrap()[..];
                let enc_key = prf(&counter_bytes, key1);
                let enc_value = fixed_encrypt(&id[..], key2);
                emm_data.insert(enc_key, enc_value);
                c = c + 1;
            }
        }
        self.key = key;
        EMMServer {
            data : emm_data
        }
    }
    
    pub fn tokenize_emm(&mut self, keyword : &Vec<u8>) -> ([u8; 16], [u8; 16]){
        let key_pair = prf(&keyword[..], &self.key[..]);
        let key1_slice = &key_pair[0..16];
        let key2_slice = &key_pair[16..];
        let mut key1 = [0u8; 16];
        key1.copy_from_slice(key1_slice);
        let mut key2 = [0u8; 16];
        key2.copy_from_slice(key2_slice);
        self.last_token_key = key2;
        (key1, key2)
    }
    
    pub fn decrypt_emm(&mut self, enc_results : Vec<Vec<u8>>) -> Vec<Vec<u8>>{
        decrypt_emm_helper(&self.last_token_key[..], enc_results)
    }
}

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
pub struct EMMServer {
    data: HashMap<[u8; 32], Vec<u8>>,
}

impl EMMServer {
    pub fn new_emm_server() -> EMMServer {
        EMMServer {
            data : HashMap::new()
        }
    }
    
    pub fn eval_emm_rh(&mut self, key : &[u8]) -> Vec<Vec<u8>> {
        let mut c = 0;
        let mut enc_results = Vec::new();
        
        loop {
            let counter_bytes = &bincode::serialize(&c).unwrap()[..];
            let enc_key = prf(&counter_bytes, key);
            let poss_val = self.data.get(&enc_key);
            match poss_val {
                Some(enc_value) => {
                    let enc_value_clone = enc_value.clone();
                    enc_results.push(enc_value_clone);
                },
                None  => break,
            }
            c = c + 1;
        }
        
        enc_results
    }
    
    pub fn eval_emm_rr(&mut self, key1 : &[u8], key2 : &[u8]) -> Vec<Vec<u8>> {
        let enc_results = self.eval_emm_rh(key1);
        decrypt_emm_helper(key2, enc_results)
    }
}

fn decrypt_emm_helper(key : &[u8], enc_results : Vec<Vec<u8>>) -> Vec<Vec<u8>>{
    let mut dec_results = Vec::new();
    
    for enc_value in enc_results{
        let dec_value = fixed_decrypt(&enc_value[..], key);
        dec_results.push(dec_value);
    }
    
    dec_results
}