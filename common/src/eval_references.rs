use std::collections::HashSet;

pub struct RefTable {
    pub refs : Vec<RefRow>,
    ids : HashSet<String>,
    save_cols : HashSet<usize>,
}

impl RefTable {
    // fn new() -> RefTable{
    //     RefTable {
    //         refs : Vec::new(),
    //         ids : Vec::new()
    //     }
    // }
    
    pub fn from_id(refs : Vec<RefRow>, id: String) -> RefTable{
        let mut ids = HashSet::new();
        ids.insert(id);
        let mut save_cols = HashSet::new();
        save_cols.insert(0);
        RefTable {
            refs : refs,
            ids : ids,
            save_cols : save_cols
        }
    }
    
    fn from_table(refs : Vec<RefRow>, ids: HashSet<String>, save_cols : HashSet<usize>) -> RefTable{
        RefTable {
            refs : refs,
            ids : ids,
            save_cols : save_cols
        }
    }
    
    pub fn has_id(&self, id : &String)-> bool {
        self.ids.contains(id)
    }
    
    pub fn filter_refs(&mut self, matches : &Vec<Vec<u8>>){
        for ref_row in &mut self.refs{
            (*ref_row).filter_refs(matches, &self.save_cols);
        }
    }
    
    pub fn filter_rows(&mut self, matches : &Vec<Vec<u8>>){
        &self.refs.retain(|t| matches.iter().any(|match_at| t.has_cell(match_at)));
    }
    
    pub fn concat_table(&self, other_ref_table : &RefTable, matches : &HashSet<([u8; 32], [u8; 32])>)
                    -> RefTable{
        let mut new_refs = Vec::new();
        for ref_row1 in &self.refs{
            for ref_row2 in &other_ref_table.refs{
                let mut pairs = HashSet::new();
                for uk1 in &ref_row1.unique_keys{
                    for uk2 in &ref_row2.unique_keys{
                        pairs.insert((uk1.clone(), uk2.clone()));
                    }
                }
                if !pairs.is_disjoint(matches){
                    new_refs.push((*ref_row1).concat_row(ref_row2));
                }
            }
        }
        let new_ids : HashSet<_> = self.ids.union(&other_ref_table.ids).map(|id| id.clone()).collect();
        let mut new_save_cols = self.save_cols.clone();
        if !self.refs.is_empty() {
            let offset = self.refs[0].cells.len();
            for save_col in &other_ref_table.save_cols{
                new_save_cols.insert(save_col + offset);
            }
        }
        RefTable::from_table(new_refs, new_ids, new_save_cols)
    }
    
    pub fn add_save_col(&mut self, save_col : usize){
        &self.save_cols.insert(save_col);
    }
}

pub struct RefRow {
    pub cells : Vec<Vec<u8>>,
    unique_keys : HashSet<[u8; 32]>,
}

impl RefRow {
    // fn new() -> RefRow{
    //     RefRow {
    //         cells : Vec::new(),
    //         unique_keys : HashSet::new()
    //     }
    // }
    
    pub fn from_uk(cells : Vec<Vec<u8>>, unique_key: [u8; 32]) -> RefRow{
        let mut uks = HashSet::new();
        uks.insert(unique_key);
        RefRow {
            cells : cells,
            unique_keys : uks
        }
    }
    
    fn from_row(cells : Vec<Vec<u8>>, unique_keys: HashSet<[u8; 32]>) -> RefRow{
        RefRow {
            cells : cells,
            unique_keys : unique_keys
        }
    }
    
    fn concat_row(&self, other_ref_row : &RefRow) -> RefRow{
        let new_uks : HashSet<_> = self.unique_keys.union(&other_ref_row.unique_keys).map(|uk| *uk).collect();
        let mut new_cells = self.cells.clone();
        for cell in &other_ref_row.cells{
            new_cells.push(cell.clone());
        }
        RefRow::from_row(new_cells, new_uks)
    }
    
    // fn has_unique_key(&self, unique_key : &[u8; 32])-> bool {
    //     self.unique_keys.contains(unique_key)
    // }
    
    fn has_cell(&self, cell : &Vec<u8>)-> bool {
        self.cells.iter().any(|c| c == cell)
    }
    
    fn filter_refs(&mut self, matches : &Vec<Vec<u8>>, save_cols : &HashSet<usize>){
        let mut i = 0;
        &self.cells.retain(|at| (matches.iter().any(|match_at| match_at == at || save_cols.contains(&i)), i += 1).0);
    }
}