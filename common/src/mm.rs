use std::collections::HashMap;

pub struct MM {
    pub data: HashMap<Vec<u8>, Vec<Vec<u8>>>,
}

impl MM {
    pub fn new_mm() -> MM {
        MM {
            data : HashMap::new()
        }
    }
    
    pub fn add_mm(&mut self, identifier : Vec<u8>, keyword : Vec<u8>){
        self.data.entry(keyword).or_insert(Vec::new()).push(identifier)
    }
    
    pub fn search_mm(&mut self, keyword : &Vec<u8>){
        if let Ok(keyword_string) = std::str::from_utf8(&keyword[..]) {
            if !self.data.contains_key(keyword) {
                println!("{} is not a keyword in the database", keyword_string);
            } else {
                println!("Identifiers for keyword: {}", keyword_string);
                let v = self.data.get_mut(keyword).unwrap();
                for id in v.iter(){
                    match std::str::from_utf8(&id[..]) {
                        Ok(v) => println!("    {}", v),
                        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                    }
                }
            }
        } else {
            println!("a non utf-8");
            // panic!("Invalid UTF-8 sequence");
        }
    }
}