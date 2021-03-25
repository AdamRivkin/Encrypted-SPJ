use std::collections::HashMap;
use std::collections::HashSet;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct DB {
    rels: HashMap<String, Relation>,
    pub annotations : HashSet<(String, String)>
}

impl DB {
    pub fn new_db() -> DB{
        DB {
            rels: HashMap::new(),
            annotations: HashSet::new()
        }
    }
    
    pub fn get_rels(&self) -> &HashMap<String, Relation>{
        &self.rels
    }
    
    pub fn get_rel(&self, id : &String) -> Option<&Relation> {
        self.rels.get(id)
    }
    
    pub fn get_rel_mut(&mut self, id : &String) -> Option<&mut Relation> {
        self.rels.get_mut(id)
    }
    
    pub fn ids(&mut self) -> Vec<&String> {
        self.rels.keys().collect()
    }
    
    pub fn add_rel(&mut self, id : &String, rel : Relation) {
        self.rels.insert(id.to_string(), rel);
    }
    
    pub fn add_annotation(&mut self, attrib1 : &String, attrib2 :&String) {
        self.annotations.insert((attrib1.to_string(), attrib2.to_string()));
    }
    
    pub fn get_id_from_at(&self, at_target : &String) -> Option<String>{
        let mut res = None;
        for (id, rel) in self.rels.iter(){
            if rel.get_ats().iter().any(|at| at_target == at){
                res = Some(String::from(id));
            }
        }
        res
    }
    
    pub fn get_schema(&mut self) -> HashMap<String, Vec<String>>{
        let mut result = HashMap::new();
        for (id, rel) in &self.rels{
            result.insert(String::from(&id[..]), rel.get_ats());
        }
        result
    }
    
    pub fn get_all_ats(&self) -> HashSet<String>{
        let mut result = HashSet::new();
        for (_, rel) in &self.rels{
            for attrib in rel.get_ats().iter(){
                result.insert(attrib.to_string());
            }
        }
        result
    }
    
    pub fn print_schema(&self){
        println!("Printing schema:");
        for (id, rel) in &self.rels{
            println!("Relation: {}", id);
            let ats = rel.get_ats().join(", ");
            println!("    {}", ats);
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Relation {
    pub table: Vec<Vec<String>>,
    ats: Vec<String>,
}

impl Relation {
    pub fn empty_rel() -> Relation {
        Relation {
            table : Vec::new(),
            ats: Vec::new()
        }
    }
    
    pub fn new_rel(ats : Vec<String>) -> Relation {
        Relation {
            table : Vec::new(),
            ats: ats
        }
    }
    
    pub fn add_row(&mut self, row : Vec<String>) {
        if row.len() == self.ats.len(){
            self.table.push(row);
        } else {
            println!("A row doesn\'t fit into a relation");
        }
    }
    
    pub fn join(&self, rel : &Relation, at1 : &String, at2 : &String) -> Relation{
        let mut result = Relation::empty_rel();
        let mut all_ats = self.get_ats();
        all_ats.extend(rel.get_ats());
        result.set_ats(all_ats);
        
        let mut i1_opt = None;
        let mut i2_opt = None;
        let mut h1 : HashMap<String, Vec<Vec<String>>> = HashMap::new();
        let mut h2 : HashMap<String, Vec<Vec<String>>> = HashMap::new();
        for (i, at) in self.ats.iter().enumerate(){
            if at == at1{
                i1_opt = Some(i);
            }
        }
        for (i, at) in rel.get_ats().iter().enumerate(){
            if at == at2{
                i2_opt = Some(i);
            }
        }
        if let Some(i1) = i1_opt {
            if let Some(i2) = i2_opt {
                for row in &self.table {
                    h1.entry(row[i1].clone()).or_insert(Vec::new()).push(row.clone())
                }
                for row in &rel.table {
                    h2.entry(row[i2].clone()).or_insert(Vec::new()).push(row.clone())
                }
                let set1: HashSet<String> = h1.keys().cloned().collect();
                let set2: HashSet<String> = h2.keys().cloned().collect();
                for value in set1.intersection(&set2){
                    for ref1 in h1.get(value).unwrap(){
                        for ref2 in h2.get(value).unwrap(){
                            let row1 = ref1.clone();
                            let row2 = ref2.clone();
                            result.add_row([&row1[..], &row2[..]].concat());
                        }
                    }
                }
            }
        }
        result
    }
    
    pub fn projection(&mut self, cols: HashSet<String>){
        let indices : HashSet<usize> = self.get_ats()
            .iter()
            .enumerate()
            .filter(|(_, at)| cols.contains(&at.to_string()))
            .map(|(i, _)| i).collect();
        for row in &mut self.table{
            let mut i = 0;
            row.retain(|_| (indices.contains(&i), i += 1).0);
        }
        self.ats.retain(|at| cols.contains(&at.to_string()))
    }
    
    pub fn set_ats(&mut self, ats : Vec<String>){
        self.ats = ats;
    }
    
    pub fn get_ats(&self) -> Vec<String> {
        self.ats.clone()
    }
    
    pub fn print_rel(&self, full : bool){
        println!("Selected {} record(s)", self.table.len());
        for at in &self.ats{
            print!("{}", format!("{:14}|", get_at_most(14, at)));
        }
        println!("");
        for _ in 0..self.ats.len(){
            print!("---------------")
        }
        println!("");
        if full || &self.table.len() <= &10 {
            for row in &self.table{
                for cell in row{
                    print!("{}", format!("{:14}|", get_at_most(14, cell)));
                }
                println!("");
            }
        } else {
            for row in &self.table[0..5]{
                for cell in row{
                    print!("{}", format!("{:14}|", get_at_most(14, cell)));
                }
                println!("");
            }
            println!("...");
            for row in &self.table[(self.table.len() - 6)..self.table.len()]{
                for cell in row{
                    print!("{}", format!("{:14}|", get_at_most(14, cell)));
                }
                println!("");
            }
        }
    }
}

fn get_at_most(length : usize, s : &String) -> &str {
    if s.len() <= length{
        &s[..]
    } else {
        &s[..][..length]
    }
}

