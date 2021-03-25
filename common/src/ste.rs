use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::crypto_helper::{ prf, fixed_encrypt, fixed_decrypt, gen_key };
use crate::token::{ HybToken, HybQuery };
use crate::db_structs::{ DB, Relation };
use crate::sti::{ STIClient, STIServer };

// pub struct STE{
//     client: STEClient,
//     server: STEServer,
// }
// 
// impl STE{
//     pub fn new_ste() -> STE {
//         STE {
//             client : STEClient::new_ste_client(),
//             server : STEServer::new_ste_server()
//         }
//     }
// 
//     pub fn setup_ste(&mut self, db : &mut DB){
//         self.server = self.client.setup_ste(db);
//     }
// 
//     pub fn search_ste(&mut self, qry : HybQuery){
//         let tk = self.client.tokenize_ste(qry);
//         let ciphertexts = self.server.eval_ste(tk);
//         let rel = self.client.decrypt_ste(ciphertexts);
//         rel.print_rel();
//     }
// }

pub struct STEClient {
    enc_key: [u8; 16],
    sti_client : STIClient,
    last_query : Option<HybQuery>,
}

impl STEClient {
    pub fn new_ste_client()->STEClient {
        STEClient {
            enc_key : [0u8; 16],
            sti_client : STIClient::new_sti_client(),
            last_query : None
        }
    }
    
    pub fn setup_ste(&mut self, db : &mut DB) -> STEServer {
        self.enc_key = gen_key();
        let label_key = gen_key();
        let mut data : HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut tk_map : HashMap<Vec<u8>, [u8; 32]> = HashMap::new();
        
        let ids = db.ids();
        let new_ids : Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        for id in new_ids{
            let rel = db.get_rel(&id.to_string()).unwrap();
            for row in &rel.table{
                for (i, at) in rel.get_ats().iter().enumerate(){
                    let label = (&id, &at, &row[0].to_string());
                    let label_bytes = &bincode::serialize(&label).unwrap()[..];
                    let label_vec = label_bytes.iter().cloned().collect();
                    let tk = prf(label_bytes, &label_key[..]);
                    tk_map.insert(label_vec, tk);
                    
                    let row_bytes = &bincode::serialize(&row[i]).unwrap()[..];
                    let enc_row = fixed_encrypt(row_bytes, &self.enc_key[..]);
                    data.insert(tk.to_vec(), enc_row);
                }
            }
        }
        
        let sti_server = self.sti_client.setup_sti(db, &tk_map);
        
        STEServer{
            sti_server: sti_server,
            data: data
        }
    }
    
    pub fn tokenize_ste(&mut self, qry : HybQuery) -> Result<HybToken, String> {
        if let HybQuery::BadQuery(s) = qry{
            Err(s)
        } else {
            let tk_wrap = self.sti_client.tokenize_sti(&qry);
            match tk_wrap {
                Ok(tk) => {
                    self.last_query = Some(qry);
                    Ok(tk)
                },
                Err(s) => Err(s)
            }
        }
    }
    
    pub fn decrypt_ste(&mut self, ciphertexts : Vec<Vec<Vec<u8>>>) -> Relation {
        let mut plaintexts : Vec<Vec<Vec<u8>>> = Vec::new();
        
        for ciphertext in ciphertexts{
            let mut p_vec = Vec::new();
            for c_table in ciphertext{
                p_vec.push(fixed_decrypt(&c_table[..], &self.enc_key[..]));
            }
            plaintexts.push(p_vec);
        }
        
        let rel = self.sti_client.fin_sti(&self.last_query, plaintexts);
        
        rel
    }
    
    pub fn print_schema(&self){
        self.sti_client.print_schema();
    }
}

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
pub struct STEServer  {
    sti_server: STIServer,
    pub data: HashMap<Vec<u8>, Vec<u8>>
}

impl STEServer {
    pub fn new_ste_server() -> STEServer {
        STEServer {
            sti_server : STIServer::new_sti_server(),
            data : HashMap::new()
        }
    }
    
    pub fn eval_ste(&mut self, tk : HybToken) -> Vec<Vec<Vec<u8>>> {
        let mut result : Vec<Vec<Vec<u8>>> = Vec::new();
        let ref_tables = self.sti_server.eval_sti(tk);
        let mut pointers = Vec::new();
        let mut table_pointers = Vec::new();
        for ref_table in ref_tables{
            // println!("Number ref rows in id: {}", ref_table.refs.len());
            for ref_row in ref_table.refs{
                // println!("Number ref cells in row: {}", &ref_row.cells.len());
                table_pointers.extend(ref_row.cells);
            }
            pointers.push(table_pointers);
            table_pointers = Vec::new()
        }
        
        // println!("Number pointers {}", pointers.len());
        for pointer in pointers{
            // println!("Number cell pointers {}", pointer.len());
            let mut res_vec = Vec::new();
            for cell_token in pointer{
                res_vec.push(self.data.get(&cell_token).unwrap().clone());
            }
            result.push(res_vec);
        }
        
        result
    }
}

