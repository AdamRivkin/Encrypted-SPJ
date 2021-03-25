use std::collections::HashMap;
use std::collections::HashSet;
// use histogram::Histogram;
use crate::db_structs::DB;
use crate::token::{ HybQuery, BoolQuery };
use std::cmp::max;

pub struct Statistics{
    hists : HashMap<String, HashMap<String, usize>>,
    sizes : HashMap<String, usize>,
    widths : HashMap<String, usize>,
    schema : HashMap<String, Vec<String>>
}

impl Statistics {
    pub fn new()-> Statistics{
        Statistics{
            hists: HashMap::new(),
            sizes: HashMap::new(),
            widths: HashMap::new(),
            schema: HashMap::new(),
        }
    }
    
    pub fn from_database(db : &mut DB) -> Statistics{
        let mut hists = HashMap::new();
        let mut sizes = HashMap::new();
        let mut widths = HashMap::new();
        for (id, rel) in db.get_rels(){
            sizes.insert(id.to_string(), rel.table.len());
            for (i, at) in rel.get_ats().iter().enumerate(){
                let mut width = 0;
                let mut h : HashMap<std::string::String, usize> = HashMap::new();
                for row in &rel.table{
                    // h.increment(row[i]);
                    if h.contains_key(&row[i]){
                        h.insert(row[i].to_string(), &h.get(&row[i]).unwrap().clone() + 1);
                    } else {
                        h.insert(row[i].to_string(), 1);
                    }
                    width = max(width, (row[i].as_bytes().len() + 15) & !15usize);
                }
                widths.insert(at.to_string(), width);
                hists.insert(at.to_string(), h);
            }
        }
        Statistics{
            hists: hists,
            sizes: sizes,
            widths: widths,
            schema: db.get_schema(),
        }
    }
    
    pub fn estimate_query(&self, qry: HybQuery){
        let ats_all = self.get_ats_from_qry(&qry).0;
        let results = self.estimate_query_helper(qry);
        let mut total_rs = 0;
        let mut total_b = 0;
        let mut total_v = 0;
        for (i, (b, v, _)) in results.iter().enumerate(){
            let ats = &ats_all[i];
            let mut row_width = 0;
            for at in ats{
                row_width = row_width + self.widths.get(at).unwrap();
            }
            total_rs = total_rs + b;
            total_b = total_b + (b * row_width);
            total_v = total_v + v;
        }
        println!("Estimated bandwidth: {} rows with a size of {} bytes will be sent from the server to the client", total_rs, total_b);
        println!("Estimated volumes leaked: {} volumes will be leaked to the server", total_v);
    }
    
    fn estimate_query_helper(&self, qry: HybQuery) -> Vec<(usize, usize, HashSet<String>)>{
        match qry {
            HybQuery::Id(id) => {
                let mut ids = HashSet::new();
                match self.sizes.get(&id) {
                    Some(i) => {
                        ids.insert(id);
                        vec!((*i, 1, ids))
                    },
                    None => vec!((0, 0, ids))
                }
            },
            HybQuery::Select(BoolQuery::Eq(attrib, val), sub_query) => {
                let mut sub = self.estimate_query_helper(*sub_query);
                let id = self.get_id_from_at(&attrib).unwrap();
                let mut new_b = 0;
                let mut new_v = 0;
                let mut index = None;
                for (i, (sub_b, sub_v, sub_ids)) in sub.iter().enumerate(){
                    if sub_ids.contains(&id){
                        if let Some(h) = self.hists.get(&attrib){
                            let id_size = self.sizes.get(&id).unwrap();
                            let count = h.get(&val).unwrap();
                            let frac : f64 = (*count as f64) / (*id_size as f64);
                            index = Some(i);
                            new_b = ((*sub_b as f64) * frac) as usize;
                            new_v = sub_v + 1;
                        }
                    }
                }
                let (_, _, ids) = &sub[index.unwrap()];
                sub[index.unwrap()] = (new_b, new_v, ids.clone());
                sub
            },
            HybQuery::Project(_, sub_query) => {
                self.estimate_query_helper(*sub_query)
            },
            HybQuery::PPJoin(attrib1, attrib2, sub_query1, sub_query2) => {
                let mut sub1 = self.estimate_query_helper(*sub_query1);
                let mut sub2 = self.estimate_query_helper(*sub_query2);
                let id1 = self.get_id_from_at(&attrib1).unwrap();
                let id2 = self.get_id_from_at(&attrib2).unwrap();
                let mut new_b1 = 0;
                let mut new_v1 = 0;
                let mut new_b2 = 0;
                let mut new_v2 = 0;
                let mut index1 = None;
                let mut index2 = None;
                let h1 = self.hists.get(&attrib1).unwrap();
                let h2 = self.hists.get(&attrib2).unwrap();
                let rel1_values: HashSet<&String> = h1.keys().collect();
                let rel2_values: HashSet<&String> = h2.keys().collect();
                let intersect_values : HashSet<&&String> = rel1_values.intersection(&rel2_values).collect();
                for (i, (sub_b, sub_v, sub_ids)) in sub1.iter().enumerate(){
                    if sub_ids.contains(&id1){
                        let mut frac : f64 = 0.0;
                        let id_size = self.sizes.get(&id1).unwrap();
                        for val in &intersect_values{
                            let count = h1.get(**val).unwrap();
                            frac = frac + (*count as f64);
                        }
                        frac = frac / (*id_size as f64);
                        index1 = Some(i);
                        new_b1 = ((*sub_b as f64) * frac) as usize;
                        new_v1 = sub_v + 1;
                    }
                }
                for (j, (sub_b, sub_v, sub_ids)) in sub2.iter().enumerate(){
                    if sub_ids.contains(&id2){
                        let mut frac : f64 = 0.0;
                        let id_size = self.sizes.get(&id2).unwrap();
                        for val in &intersect_values{
                            let count = h2.get(**val).unwrap();
                            frac = frac + (*count as f64);
                        }
                        frac = frac / (*id_size as f64);
                        index2 = Some(j);
                        new_b2 = ((*sub_b as f64) * frac) as usize;
                        new_v2 = sub_v + 1;
                    }
                }
                let (_, _, ids1) = &sub1[index1.unwrap()];
                let (_, _, ids2) = &sub2[index2.unwrap()];
                sub1[index1.unwrap()] = (new_b1, new_v1, ids1.clone());
                sub2[index2.unwrap()] = (new_b2, new_v2, ids2.clone());
                sub1.append(&mut sub2);
                sub1
            }
            HybQuery::FPJoin(attrib1, attrib2, sub_query1, sub_query2) => {
                let mut sub1 = self.estimate_query_helper(*sub_query1);
                let mut sub2 = self.estimate_query_helper(*sub_query2);
                let id1 = self.get_id_from_at(&attrib1).unwrap();
                let id2 = self.get_id_from_at(&attrib2).unwrap();
                let mut new_b = 0;
                let mut new_v = 0;
                let mut index1 = None;
                let mut index2 = None;
                let h1 = self.hists.get(&attrib1).unwrap();
                let h2 = self.hists.get(&attrib2).unwrap();
                let rel1_values: HashSet<&String> = h1.keys().collect();
                let rel2_values: HashSet<&String> = h2.keys().collect();
                let intersect_values : HashSet<&&String> = rel1_values.intersection(&rel2_values).collect();
                let mut found = false;
                for (i, (sub_b1, sub_v1, sub_ids1)) in sub1.iter().enumerate(){
                    if sub_ids1.contains(&id1){
                        for (j, (sub_b2, sub_v2, sub_ids2)) in sub2.iter().enumerate(){
                            if sub_ids2.contains(&id2){
                                let mut frac : f64 = 0.0;
                                let id_size1 = self.sizes.get(&id1).unwrap();
                                let id_size2 = self.sizes.get(&id2).unwrap();
                                for val in &intersect_values{
                                    let count1 = h1.get(**val).unwrap();
                                    let count2 = h2.get(**val).unwrap();
                                    frac = frac + ((count1 * count2) as f64);
                                }
                                frac = frac / ((*id_size1 as f64) * (*id_size2 as f64));
                                index1 = Some(i);
                                index2 = Some(j);
                                new_b = ((*sub_b1 as f64) * (*sub_b2 as f64) * frac) as usize;
                                new_v = sub_v1 + sub_v2 + &intersect_values.len();
                                found = true;
                                break
                            }
                        }
                    }
                    if found {
                        break
                    }
                }
                
                let (_, _, ids1) = &sub1[index1.unwrap()];
                let (_, _, ids2) = &sub2[index2.unwrap()];
                ids1.union(ids2);
                let merged = (new_b, new_v, ids1.clone());
                sub1.remove(index1.unwrap());
                sub2.remove(index2.unwrap());
                sub1.append(&mut sub2);
                sub1.push(merged);
                sub1
            }
            _ => vec!((0, 0, HashSet::new()))
        }
    }
    
    fn get_id_from_at(&self, at_target : &String) -> Option<String>{
        let mut res = None;
        for (id, ats) in self.schema.iter(){
            for at in ats{
                if at_target == at {
                    res = Some(String::from(id))
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
}