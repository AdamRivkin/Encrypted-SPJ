use std::collections::HashMap;
use std::collections::HashSet;
use std::cmp::{ min, max };
use serde::{Deserialize, Serialize};
use crate::mm::MM;
use crate::emm::{ EMMClient, EMMServer };
use crate::token::{ HybToken, HybQuery, BoolQuery };
use crate::db_structs::{DB, Relation};
use crate::eval_references::{ RefTable, RefRow };
// use crate::crypto_helper::{ prf, fixed_encrypt, fixed_decrypt, gen_key };

fn get_tk(tk_map : &HashMap<Vec<u8>, [u8; 32]>, id : &String, at : &String, val : &String) -> [u8; 32]{
    let label = (id, at, val);
    let label_bytes = &bincode::serialize(&label).unwrap()[..];
    let label_vec : Vec<u8> = label_bytes.iter().cloned().collect();
    let tk = tk_map.get(&label_vec).unwrap();
    *tk
}

pub struct STIClient {
    emm_client : EMMClient,
    schema: HashMap<String, Vec<String>>,
    annotations : HashSet<(String, String)>
    // label_key: [u8; 16],
}

impl STIClient {
    pub fn new_sti_client() -> STIClient {
        STIClient{
            emm_client : EMMClient::new_emm_client(),
            schema : HashMap::new(),
            annotations : HashSet::new(),
            // label_key: [0u8; 16],
        }
    }
    
    pub fn setup_sti(&mut self, db : &mut DB, tk_map : &HashMap<Vec<u8>, [u8; 32]>) -> STIServer{
        let mut mm = MM::new_mm();
        let mut emm_client = EMMClient::new_emm_client();
        // self.label_key = gen_key();
        self.schema = db.get_schema();
        self.annotations = db.annotations.clone();
        // let mut set = HashSet::new();
        // let mut data : HashMap<[u8; 32], Vec<u8>> = HashMap::new();
        
    
        let ids = db.ids();
        let new_ids : Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        for id in new_ids{
            let id_qry = ("i", id.to_string());
            let id_qry_bytes = &bincode::serialize(&id_qry).unwrap()[..];
            let rel = db.get_rel(&id.to_string()).unwrap();
            for row in &rel.table{
                for (i, at) in rel.get_ats().iter().enumerate(){
                    // add id to leaf multimap
                    let tk = get_tk(&tk_map, &id, &at, &row[0].to_string());
                    mm.add_mm(tk[..].to_vec(), id_qry_bytes.to_vec());
                    
                    
                    // add to project multimap
                    let proj_qry = ("p", &at);
                    let proj_qry_bytes = &bincode::serialize(&proj_qry).unwrap()[..];
                    mm.add_mm(tk[..].to_vec(), proj_qry_bytes.to_vec());
                    
                    // add to select multimap
                    let sel_qry = ("s", &at, &row[i]);
                    let sel_qry_bytes = &bincode::serialize(&sel_qry).unwrap()[..];
                    // Note: can get rid of this for loop but it means you can't select on an attribute
                    // once it's been projected away. This is logical but my current parser
                    // projects before it selects when it sees a SELECT a FROM b WHERE c = d.
                    // Can remove once I update the parser
                    // for sub_at in rel.get_ats(){
                    let tk = get_tk(&tk_map, &id, &at, &row[0].to_string());
                    mm.add_mm(tk[..].to_vec(), sel_qry_bytes.to_vec());
                        
                        // opx set for inner selects
                        // let set_label = prf(self.label_key, label_bytes);
                        // 
                        // set.insert();
                }
            }
        }
        
        for (attrib1, attrib2) in &db.annotations{
            // prepare join preliminaries
            // println!("Annotated {} {}", attrib1, attrib2);
            let id1 = &db.get_id_from_at(&attrib1);
            let id2 = &db.get_id_from_at(&attrib2);
            if let None = id1{
                println!("Note: Could not annotate joining on ({0}, {1}) because the attribute {0} does not belong to any relations", &attrib1, &attrib2);
                continue;
            }
            if let None = id2{
                println!("Note: Could not annotate joining on ({0}, {1}) because the attribute {1} does not belong to any relations", &attrib1, &attrib2);
                continue;
            }
            let id1 = id1.as_ref().unwrap();
            let id2 = id2.as_ref().unwrap();
            if id1 == id2{
                println!("Note: Could not annotate joining on ({0}, {1}) because they both belong to the relation {2}", &attrib1, &attrib2, &id1);
                continue;
            }
            let rel1 = &db.get_rel(&id1).unwrap();
            let rel2 = &db.get_rel(&id2).unwrap();
            let uk1 = &rel1.get_ats()[0];
            let uk2 = &rel2.get_ats()[0];
            let (h1, h2) = self.prepare_join(&rel1, &rel2, &attrib1, &attrib2);
            let rel1_values: HashSet<String> = h1.keys().cloned().collect();
            let rel2_values: HashSet<String> = h2.keys().cloned().collect();
            let intersect_values : HashSet<&String> = rel1_values.intersection(&rel2_values).collect();
            
            // add to fp join multimap
            let fp_join_qry = ("fpj", &attrib1, &attrib2);
            let fp_join_qry_bytes = &bincode::serialize(&fp_join_qry).unwrap()[..];
            for value in &intersect_values{
                for ref1 in h1.get(*value).unwrap(){
                    for ref2 in h2.get(*value).unwrap(){
                        let tk1 = get_tk(&tk_map, &id1, &uk1, &ref1.to_string());
                        let tk2 = get_tk(&tk_map, &id2, &uk2, &ref2.to_string());
                        
                        let tk_pair = (tk1, tk2);
                        let tk_bytes = &bincode::serialize(&tk_pair).unwrap()[..];
                        mm.add_mm(tk_bytes[..].to_vec(), fp_join_qry_bytes.to_vec());
                    }
                }
            }
            
            // add to pp join multimap
            let pp_join_qry1 = ("ppj", &attrib1, &attrib2, 1);
            let pp_join_qry_bytes1 = &bincode::serialize(&pp_join_qry1).unwrap()[..];
            let pp_join_qry2 = ("ppj", &attrib1, &attrib2, 2);
            let pp_join_qry_bytes2 = &bincode::serialize(&pp_join_qry2).unwrap()[..];
            
            for value in &intersect_values{
                for ref1 in h1.get(*value).unwrap(){
                    let tk1 = get_tk(&tk_map, &id1, &uk1, &ref1.to_string());
                    mm.add_mm(tk1[..].to_vec(), pp_join_qry_bytes1.to_vec());
                }
            
                for ref2 in h2.get(*value).unwrap(){
                    let tk2 = get_tk(&tk_map, &id2, &uk2, &ref2.to_string());
                    mm.add_mm(tk2[..].to_vec(), pp_join_qry_bytes2.to_vec());
                }
            }
        }
        
        let emm_server = emm_client.setup_emm(&mut mm);
        self.emm_client = emm_client;
        
        STIServer{
            emm_server: emm_server,
            // set: set,
            // data: data
        }
    }
    
    fn prepare_join(&self, rel1 : &Relation, rel2 : &Relation, at1 : &String, at2 : &String)
                    -> (HashMap<String, Vec<String>>, HashMap<String, Vec<String>>){        
        let mut i1_opt = None;
        let mut i2_opt = None;
        let mut h1 : HashMap<String, Vec<String>> = HashMap::new();
        let mut h2 : HashMap<String, Vec<String>> = HashMap::new();
        for (i, at) in rel1.get_ats().iter().enumerate(){
            if at == at1{
                i1_opt = Some(i);
            }
        }
        for (i, at) in rel2.get_ats().iter().enumerate(){
            if at == at2{
                i2_opt = Some(i);
            }
        }
        if let Some(i1) = i1_opt {
            if let Some(i2) = i2_opt {
                for row in &rel1.table {
                    h1.entry(row[i1].clone()).or_insert(Vec::new()).push(row[0].clone())
                }
                for row in &rel2.table {
                    h2.entry(row[i2].clone()).or_insert(Vec::new()).push(row[0].clone())
                }
            }
        }
        (h1, h2)
    }
    
    pub fn tokenize_sti(&mut self, qry : &HybQuery) -> Result<HybToken, String> {
        match qry {
            HybQuery::Id(id) => {
                let id_qry = ("i", id.to_string());
                let id_qry_bytes = &bincode::serialize(&id_qry).unwrap()[..];
                match self.schema.get(id) {
                    Some(ats) => {
                        let length = ats.len();
                        Ok(HybToken::Id(self.emm_client.tokenize_emm(&id_qry_bytes.to_vec()),
                            id.to_string(),
                            length)
                        )
                    },
                    None => Err(format!("Could not find a relation named {} in the schema", id))
                }
            },
            HybQuery::Select(BoolQuery::Eq(attrib, val), sub_query) => {
                let sub_tk_wrap = self.tokenize_sti(sub_query);
                match sub_tk_wrap {
                    Ok(sub_tk) => {
                        match &self.get_id_from_at(&attrib) {
                            Some((id, _)) => {
                                let sel_qry = ("s", &attrib, &val);
                                let sel_qry_bytes = &bincode::serialize(&sel_qry).unwrap()[..];
                                Ok(HybToken::Select(
                                    self.emm_client.tokenize_emm(&sel_qry_bytes.to_vec()),
                                    Box::new(sub_tk),
                                    id.to_string()
                                ))
                            },
                            None => Err(format!("Could not find a column named {} in the schema", attrib))
                        }
                    }
                    Err(s) => Err(s)
                }
            },
            HybQuery::Project(cols, sub_query) => {
                let sub_tk_wrap = self.tokenize_sti(sub_query);
                match sub_tk_wrap {
                    Ok(sub_tk) => {
                        let mut tk_vec = Vec::new();
                        for attrib in cols{
                            let proj_qry = ("p", &attrib);
                            let proj_qry_bytes = &bincode::serialize(&proj_qry).unwrap()[..];
                            tk_vec.push(self.emm_client.tokenize_emm(&proj_qry_bytes.to_vec()));
                        }
                        Ok(HybToken::Project(tk_vec, Box::new(sub_tk)))
                    },
                    Err(s) => Err(s)
                }
            },
            HybQuery::FPJoin(attrib1, attrib2, sub_query1, sub_query2) => {
                let sub_tk1_wrap = self.tokenize_sti(sub_query1);
                let sub_tk2_wrap = self.tokenize_sti(sub_query2);
                match (sub_tk1_wrap, sub_tk2_wrap) {
                    (Ok(sub_tk1), Ok(sub_tk2)) => {
                        let mut fp_join_qry = ("fpj", &attrib1, &attrib2);
                        let id1_wrap = &self.get_id_from_at(&attrib1);
                        let id2_wrap = &self.get_id_from_at(&attrib2);
                        
                        match (id1_wrap, id2_wrap) {
                            (Some((id1, _)), Some((id2, _))) => {
                                let mut swapped = false;
                                if !self.annotations.contains(&(attrib1.to_string(), attrib2.to_string()))
                                    && !self.annotations.contains(&(attrib2.to_string(), attrib1.to_string())){
                                    Err(format!("Could not find ({}, {}) in the database annotations", attrib1, attrib2))
                                } else {
                                    if !self.annotations.contains(&(attrib1.to_string(), attrib2.to_string()))
                                        && self.annotations.contains(&(attrib2.to_string(), attrib1.to_string())){
                                        swapped = true;
                                        fp_join_qry = ("fpj", &attrib2, &attrib1);
                                    }
                                    let fp_join_qry_bytes = &bincode::serialize(&fp_join_qry).unwrap()[..];
                                
                                    Ok(HybToken::FPJoin(
                                        self.emm_client.tokenize_emm(&fp_join_qry_bytes.to_vec()),
                                        Box::new(sub_tk1),
                                        Box::new(sub_tk2),
                                        id1.to_string(),
                                        id2.to_string(),
                                        swapped
                                    ))
                                }
                            },
                            (None, _) => Err(format!("Could not find a column named {} in the schema for a join", attrib1)),
                            (_, None) => Err(format!("Could not find a column named {} in the schema for a join", attrib2)),
                        }
                    },
                    (Err(s), _) => Err(s),
                    (_, Err(s)) => Err(s)
                }
            },
            HybQuery::PPJoin(attrib1, attrib2, sub_query1, sub_query2) => {
                let sub_tk1_wrap = self.tokenize_sti(sub_query1);
                let sub_tk2_wrap = self.tokenize_sti(sub_query2);
                match (sub_tk1_wrap, sub_tk2_wrap) {
                    (Ok(sub_tk1), Ok(sub_tk2)) => {
                        let mut pp_join_qry1 = ("ppj", &attrib1, &attrib2, 1);
                        let mut pp_join_qry2 = ("ppj", &attrib1, &attrib2, 2);
                        let id1_wrap = &self.get_id_from_at(&attrib1);
                        let id2_wrap = &self.get_id_from_at(&attrib2);
                        
                        match (id1_wrap, id2_wrap) {
                            (Some((id1, save_col1)), Some((id2, save_col2))) => {
                                let mut swapped = false;
                                if !self.annotations.contains(&(attrib1.to_string(), attrib2.to_string()))
                                    && !self.annotations.contains(&(attrib2.to_string(), attrib1.to_string())){
                                    Err(format!("Could not find ({}, {}) in the database annotations", attrib1, attrib2))
                                } else {
                                    if !self.annotations.contains(&(attrib1.to_string(), attrib2.to_string()))
                                        && self.annotations.contains(&(attrib2.to_string(), attrib1.to_string())){
                                        swapped = true;
                                        pp_join_qry1 = ("ppj", &attrib2, &attrib1, 1);
                                        pp_join_qry2 = ("ppj", &attrib2, &attrib1, 2);
                                    }
                                    let pp_join_qry_bytes1 = &bincode::serialize(&pp_join_qry1).unwrap()[..];
                                    let pp_join_qry_bytes2 = &bincode::serialize(&pp_join_qry2).unwrap()[..];
                                    
                                    Ok(HybToken::PPJoin(
                                        self.emm_client.tokenize_emm(&pp_join_qry_bytes1.to_vec()),
                                        self.emm_client.tokenize_emm(&pp_join_qry_bytes2.to_vec()),
                                        Box::new(sub_tk1),
                                        Box::new(sub_tk2),
                                        id1.to_string(),
                                        id2.to_string(),
                                        *save_col1,
                                        *save_col2,
                                        swapped
                                    ))
                                }
                            },
                            (None, _) => Err(format!("Could not find a column named {} in the schema for a join", attrib1)),
                            (_, None) => Err(format!("Could not find a column named {} in the schema for a join", attrib2)),
                        }
                    },
                    (Err(s), _) => Err(s),
                    (_, Err(s)) => Err(s)
                }
            },
            _ => Ok(HybToken::BadToken)
        }
    }
    
    fn get_id_from_at(&self, at_target : &String) -> Option<(String, usize)>{
        let mut res = None;
        for (id, ats) in self.schema.iter(){
            for (i, at) in ats.iter().enumerate(){
                if at_target == at {
                    res = Some((String::from(id), i))
                }
            }
        }
        res
    }
    
    fn get_ats_from_qry(&self, qry: &HybQuery)-> (Vec<Vec<String>>, Vec<String>){
        let ats_res : (Vec<Vec<String>>, Vec<String>);
        let mut ats_all : Vec<Vec<String>> = Vec::new();
        match qry{
            HybQuery::Id(id)=> {
                let ats = self.schema.get(id).unwrap().clone();
                let primary_key = &ats[0].clone();
                ats_res = (vec!(ats), vec!(primary_key.to_string()));
            },
            HybQuery::Select(_, sub_query)=>{
                ats_res = self.get_ats_from_qry(sub_query);
            },
            HybQuery::FPJoin(attrib1, attrib2, sub_query1, sub_query2)=>{
                let (mut sub_ats1, mut save_ats1) = self.get_ats_from_qry(sub_query1);
                let (mut sub_ats2, mut save_ats2) = self.get_ats_from_qry(sub_query2);
                
                let mut index1 = None;
                let mut index2 = None;
                for (i, sub_at) in sub_ats1.iter().enumerate(){
                    if sub_at.iter().any(|at| at == attrib1){
                        index1 = Some(i);
                        break;
                    }
                }
                for (i, sub_at) in sub_ats2.iter().enumerate(){
                    if sub_at.iter().any(|at| at == attrib2){
                        index2 = Some(i);
                        break;
                    }
                }
                let mut merged = sub_ats1[index1.unwrap()].clone();
                merged.append(&mut sub_ats2[index2.unwrap()]);
                sub_ats1.remove(index1.unwrap());
                sub_ats2.remove(index2.unwrap());
                sub_ats1.append(&mut sub_ats2);
                sub_ats1.push(merged);
                save_ats1.append(&mut save_ats2);
                
                ats_res = (sub_ats1, save_ats1);
            },
            HybQuery::PPJoin(attrib1, attrib2, sub_query1, sub_query2)=>{
                let (mut sub_ats1, mut save_ats1) = self.get_ats_from_qry(sub_query1);
                let (mut sub_ats2, mut save_ats2) = self.get_ats_from_qry(sub_query2);
                sub_ats1.append(&mut sub_ats2);
                save_ats1.append(&mut save_ats2);
                save_ats1.push(attrib1.to_string());
                save_ats1.push(attrib2.to_string());
                ats_res = (sub_ats1, save_ats1);
            },
            HybQuery::Project(cols, sub_query)=>{
                let (sub_ats, save_ats) = self.get_ats_from_qry(sub_query);
                for t_sub_ats in sub_ats{
                    let filter_ats = t_sub_ats.into_iter()
                        .filter(|at| cols.iter().any(|proj_at| proj_at==at)
                        || save_ats.iter().any(|save_at| save_at==at))
                        .collect();
                    ats_all.push(filter_ats);
                }
                ats_res = (ats_all, save_ats);
            },
            _ => { ats_res = (Vec::new(), Vec::new()) }
        }
        ats_res
    }
    
    fn get_remaining_joins(&self, qry: &HybQuery)->Vec<(String, String)>{
        match qry {
            HybQuery::Id(_)=> Vec::new(),
            HybQuery::Select(_, sub_query)=> self.get_remaining_joins(sub_query),
            HybQuery::Project(_, sub_query)=> self.get_remaining_joins(sub_query),
            HybQuery::FPJoin(_, _, sub_query1, sub_query2)=>{
                let mut sub_remaining1 = self.get_remaining_joins(sub_query1);
                let mut sub_remaining2 = self.get_remaining_joins(sub_query2);
                sub_remaining1.append(&mut sub_remaining2);
                sub_remaining1
            },
            HybQuery::PPJoin(attrib1, attrib2, sub_query1, sub_query2)=>{
                let mut sub_remaining1 = self.get_remaining_joins(sub_query1);
                let mut sub_remaining2 = self.get_remaining_joins(sub_query2);
                sub_remaining1.append(&mut sub_remaining2);
                sub_remaining1.push((attrib1.to_string(), attrib2.to_string()));
                sub_remaining1
            },
            _ => Vec::new()
        }
    }
    
    fn get_true_projection(&self, qry: &HybQuery)->HashSet<String>{
        match qry {
            HybQuery::Id(id)=> {
                let ats = self.schema.get(id).unwrap().clone();
                let hash_cols : HashSet<String> = ats.iter().map(|s| s.to_string()).collect();
                hash_cols
            },
            HybQuery::Select(_, sub_query)=> self.get_true_projection(sub_query),
            HybQuery::Project(cols, sub_query)=>{
                let sub_p = self.get_true_projection(sub_query);
                let hash_cols : HashSet<String> = cols.iter().map(|s| s.to_string()).collect();
                let res : HashSet<String>;
                res = sub_p.intersection(&hash_cols).map(|s| s.to_string()).collect();
                res
            },
            HybQuery::FPJoin(_, _, sub_query1, sub_query2)=>{
                let sub_p1 = self.get_true_projection(sub_query1);
                let sub_p2 = self.get_true_projection(sub_query2);
                let res : HashSet<String> = sub_p1.union(&sub_p2).map(|s| s.to_string()).collect();
                res
            },
            HybQuery::PPJoin(_, _, sub_query1, sub_query2)=>{
                let sub_p1 = self.get_true_projection(sub_query1);
                let sub_p2 = self.get_true_projection(sub_query2);
                let res : HashSet<String> = sub_p1.union(&sub_p2).map(|s| s.to_string()).collect();
                res
            },
            _ => HashSet::new()
        }
    }
    
    pub fn fin_sti(&mut self, qry_opt: &Option<HybQuery>, plaintexts : Vec<Vec<Vec<u8>>>) -> Relation{
        if let Some(qry) = qry_opt {
            let mut rels : Vec<Relation> = Vec::new();
            let (ats_all, _) = self.get_ats_from_qry(qry);
            // println!("SAVE ATTRIBUTES");
            // for save_at in save_ats{
            //     println!("{}", save_at);
            // }
            let remaining_joins = self.get_remaining_joins(qry);
            let true_projection = self.get_true_projection(qry);
            // println!("TRUE PROJECTION");
            // for at in &true_projection{
            //     println!("{}", at);
            // }
            for (i, ats) in ats_all.iter().enumerate(){
                let mut rel = Relation::new_rel(ats.to_vec());
                let mut row = Vec::new();
                for (j, cell) in plaintexts[i].iter().enumerate(){
                    if let Ok(cell_str) = bincode::deserialize::<String>(&cell){
                        row.push(cell_str);
                    } 
                    if (j + 1) % ats.len() == 0 {
                        rel.add_row(row.clone());
                        row = Vec::new();
                    }
                }
                rels.push(rel);
            }
            for (attrib1, attrib2) in &remaining_joins{
                let mut rel1 = &Relation::empty_rel();
                let mut rel2 = &Relation::empty_rel();
                let mut index1 = None;
                let mut index2 = None;
                for (i, rel) in rels.iter().enumerate(){
                    // println!("Number attributes: {}", rel.get_ats().len());
                    if rel.get_ats().iter().any(|at| attrib1 == at) {
                        rel1 = rel;
                        index1 = Some(i)
                    }
                    if rel.get_ats().iter().any(|at| attrib2 == at) {
                        rel2 = rel;
                        index2 = Some(i)
                    }
                }
                let merged = rel1.join(rel2, attrib1, attrib2);
                let first_remove = max(index1.unwrap(), index2.unwrap());
                let second_remove = min(index1.unwrap(), index2.unwrap());
                rels.remove(first_remove);
                rels.remove(second_remove);
                rels.push(merged);
            }
            if rels.len() == 1{
                if !true_projection.is_empty(){
                    rels[0].projection(true_projection);
                }
                rels.pop().unwrap()
            } else {
                panic!("Multiple tables in join. Something went wrong");
            }
        } else {
            Relation::empty_rel()
        }
    }
    
    pub fn print_schema(&self){
        println!("Printing edb schema:");
        for (id, ats) in &self.schema{
            println!("Relation: {}", id);
            let ats_str = ats.join(", ");
            println!("    {}", ats_str);
        }
    }
}

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
pub struct STIServer {
    emm_server: EMMServer,
    // set : HashSet<[u8; 32]>,
    // data : HashMap<[u8; 32], Vec<u8>>,
}

impl STIServer {
    pub fn new_sti_server() -> STIServer {
        STIServer{
            emm_server : EMMServer::new_emm_server(),
            // set: HashSet::new(),
            // data: HashMap::new(),
        }
    }
    
    pub fn eval_sti(&mut self, tk : HybToken)-> Vec<RefTable>{
        match tk{
            HybToken::Id((key1, key2), id, length) => {
                let mut new_ref_cells = Vec::new();
                let mut ref_rows = Vec::new();
                let mut i = 0;
                let matches = self.emm_server.eval_emm_rr(&key1, &key2);
                for m in matches {
                    new_ref_cells.push(m);
                    if (i + 1) % length == 0 {
                        let mut uk = [0u8; 32];
                        uk.copy_from_slice(&new_ref_cells[0]);
                        ref_rows.push(RefRow::from_uk(new_ref_cells.clone(), uk));
                        new_ref_cells = Vec::new();
                    }
                    i = i + 1;
                } 
                let ref_table = RefTable::from_id(ref_rows, id);
                vec!(ref_table)
            }
            HybToken::Select((key1, key2), sub_tk, id) => {
                let mut sub_ref_tables = self.eval_sti(*sub_tk);
                let matches = self.emm_server.eval_emm_rr(&key1, &key2);
                for sub_ref_table in &mut sub_ref_tables{
                    if sub_ref_table.has_id(&id){
                        (*sub_ref_table).filter_rows(&matches);
                    }
                }
                sub_ref_tables
            },
            HybToken::Project(tk_vec, sub_tk) => {
                let mut sub_ref_tables = self.eval_sti(*sub_tk);
                let mut matches = Vec::new();
                for (key1, key2) in tk_vec{
                    matches.extend(self.emm_server.eval_emm_rr(&key1, &key2));
                }
                for sub_ref_table in &mut sub_ref_tables{
                    (*sub_ref_table).filter_refs(&matches);
                }
                sub_ref_tables
            },
            HybToken::FPJoin((key1, key2), sub_tk1, sub_tk2, id1, id2, swapped) => {
                let mut sub_ref_tables1 = self.eval_sti(*sub_tk1);
                let mut sub_ref_tables2 = self.eval_sti(*sub_tk2);
                let matches_tuple = self.emm_server.eval_emm_rr(&key1, &key2);
                let mut ref_pairs = HashSet::new();
                
                let mut index1 = None;
                for (i, sub_ref_table) in sub_ref_tables1.iter().enumerate(){
                    if sub_ref_table.has_id(&id1){
                        index1 = Some(i);
                        break;
                    }
                }
                
                let mut index2 = None;
                for (i, sub_ref_table) in sub_ref_tables2.iter().enumerate(){
                    if sub_ref_table.has_id(&id2){
                        index2 = Some(i);
                        break;
                    }
                }
                
                for match_tuple in matches_tuple{
                    if let Ok((tk1, tk2)) = bincode::deserialize::<([u8; 32], [u8; 32])>(&match_tuple){
                        if !swapped{
                            ref_pairs.insert((tk1, tk2));
                        } else {
                            ref_pairs.insert((tk2, tk1));
                        }
                    }
                }
                let merged = sub_ref_tables1[index1.unwrap()].concat_table(&sub_ref_tables2[index2.unwrap()], &ref_pairs);
                sub_ref_tables1.remove(index1.unwrap());
                sub_ref_tables2.remove(index2.unwrap());
                sub_ref_tables1.append(&mut sub_ref_tables2);
                sub_ref_tables1.push(merged);
                sub_ref_tables1
            },
            HybToken::PPJoin((key11, key21), (key12, key22), sub_tk1, sub_tk2, id1, id2, save_col1, save_col2, swapped) => {
                let mut sub_ref_tables1 = self.eval_sti(*sub_tk1);
                let mut sub_ref_tables2 = self.eval_sti(*sub_tk2);
                
                let matches1;
                let matches2;
                if !swapped {
                    matches1 = self.emm_server.eval_emm_rr(&key11, &key21);
                    matches2 = self.emm_server.eval_emm_rr(&key12, &key22);
                } else {
                    matches1 = self.emm_server.eval_emm_rr(&key12, &key22);
                    matches2 = self.emm_server.eval_emm_rr(&key11, &key21);
                }
                for sub_ref_table in &mut sub_ref_tables1{
                    if sub_ref_table.has_id(&id1){
                        (*sub_ref_table).filter_rows(&matches1);
                        (*sub_ref_table).add_save_col(save_col1);
                    }
                }
                for sub_ref_table in &mut sub_ref_tables2{
                    if sub_ref_table.has_id(&id2){
                        (*sub_ref_table).filter_rows(&matches2);
                        (*sub_ref_table).add_save_col(save_col2);
                    }
                }
                sub_ref_tables1.append(&mut sub_ref_tables2);
                sub_ref_tables1
            }
            _ => { Vec::new() }
        }
    }
}

