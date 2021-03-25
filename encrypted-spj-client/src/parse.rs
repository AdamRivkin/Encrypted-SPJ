use std::fs;
use std::str::SplitWhitespace;
// use std::collections::HashSet;
use common::token::{ SPJQuery, HybQuery, BoolQuery };
use common::db_structs::{ DB, Relation};

pub fn annotate_query(qry : SPJQuery)-> HybQuery{
    match qry {
        SPJQuery::Select(bq, sub_query) => HybQuery::Select(bq, Box::new(annotate_query(*sub_query))),
        SPJQuery::Join(at1, at2, sub_query1, sub_query2) => {
            HybQuery::FPJoin(at1, at2, Box::new(annotate_query(*sub_query1)), Box::new(annotate_query(*sub_query2)))
        },
        SPJQuery::Project(cols, sub_query) => HybQuery::Project(cols, Box::new(annotate_query(*sub_query))),
        SPJQuery::Id(s) => HybQuery::Id(s),
        SPJQuery::BadQuery(s) => HybQuery::BadQuery(s)
    }
}

pub fn parse(query : &String)-> SPJQuery{
    let clean_query = query.replace("=", " = ");
    let clean_query = clean_query.replace(",", ", ");
    let clean_query = clean_query.replace("\"", " \" ");
    let mut query_iter = clean_query.split_whitespace();
    let (ast, spoiler) = parse_helper(&mut query_iter);
    if let None = query_iter.next(){
        if let None = spoiler{
            ast
        } else {
            SPJQuery::BadQuery(String::from("Last word of query left dangling"))
        }
    } else {
        SPJQuery::BadQuery(String::from("Malformed query"))
    }
}

fn parse_helper(query_iter : &mut SplitWhitespace)-> (SPJQuery, Option<String>){
    let mut ast = SPJQuery::BadQuery(String::from("Empty query"));
    let mut whereable = false;
    let mut spoiler : Option<String> = None;
    
    let mut skip_post_loop = false;
    let mut will_apply_project = false;
    let mut projection_cols_save = Vec::new();
    
    if let Some(first_word) = query_iter.next(){
        if first_word.to_uppercase() == "SELECT"{
            let mut projection = Vec::new();
            let mut loop_error = false;
            loop {
                if let Some(project_word) = query_iter.next(){
                    if project_word == "*" && projection.is_empty(){
                        break;
                    } else if project_word.chars().last().unwrap() == ',' {
                        let mut new_project_word = String::from(project_word);
                        new_project_word.pop();
                        projection.push(new_project_word);
                    } else {
                        projection.push(String::from(project_word));
                        break;
                    }
                } else {
                    loop_error = true;
                    break;
                }
            }
            if loop_error {
                ast = SPJQuery::BadQuery(String::from("You must list columns for projection or *"));
            } else {
                if let Some(from_word) = query_iter.next(){
                    if from_word.to_uppercase() == "FROM" {
                        let (sub_query, spoiler_inner) = parse_helper(query_iter);
                        if let SPJQuery::BadQuery(s) = sub_query{
                            ast = SPJQuery::BadQuery(s);
                            spoiler = None;
                            skip_post_loop = true;
                        } else {
                            spoiler = spoiler_inner;
                            if !projection.is_empty(){
                                will_apply_project = true;
                                projection_cols_save = projection.clone();
                            }
                            ast = sub_query;
                            whereable = true;
                        }
                    } else {
                        ast = SPJQuery::BadQuery(String::from("FROM must come after the projected columns of SELECT"));
                    }
                } else {
                    ast = SPJQuery::BadQuery(String::from("Missing \"FROM\" after the projected columns of SELECT"));
                }
            }
        } else {
            ast = SPJQuery::Id(String::from(first_word));
        }
    }
    while !skip_post_loop {
        let mut next_word : Option<String> = None;
        match spoiler{
            Some(ref suffix_word) => next_word = Some(suffix_word.to_string()),
            None => {
                match query_iter.next(){
                    Some(suffix_word) => next_word = Some(String::from(suffix_word)),
                    None => {}
                }
            }
        }
        if let Some(suffix_word) = next_word{
            if suffix_word.to_uppercase() == "WHERE" && whereable{
                whereable = false;
                spoiler = None;
                let b_query = parse_bool(query_iter);
                if let BoolQuery::BadBool(s) = b_query{
                    ast = SPJQuery::BadQuery(s);
                    spoiler = None;
                    break;
                }
                if will_apply_project {
                    ast = SPJQuery::Project(projection_cols_save.clone(), Box::new(
                        SPJQuery::Select(b_query, Box::new(ast))
                    ));
                } else {
                    ast = SPJQuery::Select(b_query, Box::new(ast));
                }
            } else if suffix_word.to_uppercase() == "JOIN" {
                let (sub_query, spoiler_inner) = parse_helper(query_iter);
                if let SPJQuery::BadQuery(s) = sub_query{
                    ast = SPJQuery::BadQuery(s);
                    spoiler = None;
                    break;
                }
                spoiler = spoiler_inner;
                let mut on_word_opt : Option<String> = None;
                match spoiler{
                    Some(ref pos_on_word) => on_word_opt = Some(pos_on_word.to_string()),
                    None => {
                        match query_iter.next(){
                            Some(pos_on_word) => on_word_opt = Some(String::from(pos_on_word)),
                            None => {}
                        }
                    }
                }
                spoiler = None;
                if let Some(on_word) = on_word_opt{
                    if on_word.to_uppercase() == "ON" {
                        let mut error_in_on = true;
                        if let Some(attrib1) = query_iter.next(){
                            if let Some(eq_word) = query_iter.next(){
                                if eq_word == "=" {
                                    if let Some(attrib2) = query_iter.next(){
                                        error_in_on = false;
                                        ast = SPJQuery::Join(
                                            String::from(attrib1),
                                            String::from(attrib2),
                                            Box::new(ast),
                                            Box::new(sub_query)
                                        );
                                    }
                                }
                            }
                        }
                        if error_in_on {
                            ast = SPJQuery::BadQuery(String::from("You must join on attrib1 = attrib2"));
                        }
                    } else {
                        ast = SPJQuery::BadQuery(String::from("A word other than ON followed a JOIN. Remember, no natural joins"));
                    }
                } else {
                    ast = SPJQuery::BadQuery(String::from("You must specify what attributes to join on. Sorry - no natural joins"));
                }
            } else {
                spoiler = Some(String::from(suffix_word));
                break;
            }
        } else {
            break;
        }
    }
    (ast, spoiler)
}

fn parse_bool(query_iter : &mut SplitWhitespace)-> BoolQuery{
    if let Some(attrib) = query_iter.next(){
        if let Some(eq_word) = query_iter.next(){
            if eq_word == "=" {
                if let Some(val) = parse_value(query_iter){
                    BoolQuery::Eq(String::from(attrib), String::from(val))
                } else{
                    BoolQuery::BadBool(String::from(format!("Could not parse the value that {} is supposed to equal", attrib)))
                }
            } else {
                BoolQuery::BadBool(String::from("Error in parsing a select condition. \"=\" must come after an attribute in a selection predicate"))
            }
        } else {
            BoolQuery::BadBool(String::from("Error in parsing a select condition. \"=\" must come after an attribute in a selection predicate"))
        }
    } else {
        BoolQuery::BadBool(String::from("WHERE must be followed by a selection predicate"))
    }
}

fn parse_value(query_iter : &mut SplitWhitespace)-> Option<String>{
    let mut val = None;
    let mut res = "".to_string();
    if let Some(next_token) = query_iter.next(){
        if next_token == "\""{
            let mut good_string = true;
            loop {
                if let Some(next_word) = query_iter.next(){
                    if next_word == "\""{
                        break
                    }
                    res.push_str(next_word);
                    res.push_str(" ");
                } else {
                    good_string = false;
                    break;
                }
            }
            if good_string {
                if res.len() >= 1{
                    res = res[0..(res.len() - 1)].to_string();
                }
                val = Some(res);
            }
        } else {
            res = next_token.to_string();
            val = Some(res);
        }
    }
    val
}


pub fn load_db_from_txt(filename : &String) -> DB{
    let mut db = DB::new_db();
    if let Ok(contents) = fs::read_to_string(format!("txts/{}.txt", filename)){
        let lines = contents.lines();
        for line in lines{
            let mut words = line.split_whitespace();
            if let Some(first_word) = words.next(){
                if first_word == "CREATE"{
                    if let Some(second_word) = words.next(){
                        if second_word == "TABLE"{
                            if let Some(rel_id) = words.next(){
                                let ats : Vec<String> = words.map(|s| String::from(s)).collect();
                                let rel = Relation::new_rel(ats);
                                db.add_rel(&String::from(rel_id), rel);
                            } else {
                                println!("You need to supply a name for the table");
                            }
                        } else {
                            println!("Expected TABLE");
                        }
                    } else {
                        println!("Expected TABLE");
                    }
                } else if first_word == "INSERT" {
                    if let Some(second_word) = words.next(){
                        if second_word == "INTO"{
                            if let Some(rel_id) = words.next(){
                                let row : Vec<String> = words.map(|s| String::from(s)).collect();
                                if let Some(rel) = db.get_rel_mut(&String::from(rel_id)){
                                    rel.add_row(row);
                                } else {
                                    println!("Could not find table with that id in the database");
                                }
                            } else {
                                println!("You need to supply a name for the table");
                            }
                        } else {
                            println!("Expected INTO");
                        }
                    }else {
                        println!("Expected INTO");
                    }
                } else if first_word == "ANNOTATE" {
                    if let Some(attrib1) = words.next(){
                        if let Some(attrib2) = words.next(){
                            &db.add_annotation(&String::from(attrib1), &String::from(attrib2));
                        }
                    }
                } else {
                    println!("Unrecognized command on this line");
                }
            }
        }
        db
    } else {
        println!("Something went wrong while loading the db. Reverting to an empty database");
        db
    }
}

pub fn annotate_from_txt(filename : &String, db : &mut DB){
    if let Ok(contents) = fs::read_to_string(format!("txts/{}.txt", filename)){
        let lines = contents.lines();
        for line in lines{
            let mut words = line.split_whitespace();
            if let Some(first_word) = words.next(){
                if first_word == "ANNOTATE" {
                    if let Some(attrib1) = words.next(){
                        if let Some(attrib2) = words.next(){
                            db.add_annotation(&String::from(attrib1), &String::from(attrib2));
                        }
                    }
                } else {
                    println!("Unrecognized command on this line");
                }
            } else {
                println!("Unrecognized command on this line");
            }
        }
    } else {
        println!("Could not find that file so could not annotate");
    }
}

















// There are way better ways to do this than just copying and pasting, but this is the lazy solution for now

pub fn parse_hyb(query : &String)-> HybQuery{
    let clean_query = query.replace("=", " = ");
    let clean_query = clean_query.replace(",", ", ");
    let mut query_iter = clean_query.split_whitespace();
    let (ast, spoiler) = parse_helper_hyb(&mut query_iter);
    if let None = query_iter.next(){
        if let None = spoiler{
            ast
        } else {
            HybQuery::BadQuery(String::from("Last word of query left dangling"))
        }
    } else {
        HybQuery::BadQuery(String::from("Malformed query"))
    }
}

fn parse_helper_hyb(query_iter : &mut SplitWhitespace)-> (HybQuery, Option<String>){
    let mut ast = HybQuery::BadQuery(String::from("Empty query"));
    let mut whereable = false;
    let mut spoiler : Option<String> = None;
    
    if let Some(first_word) = query_iter.next(){
        if first_word.to_uppercase() == "SELECT"{
            let mut projection = Vec::new();
            let mut loop_error = false;
            loop {
                if let Some(project_word) = query_iter.next(){
                    if project_word == "*" && projection.is_empty(){
                        break;
                    } else if project_word.chars().last().unwrap() == ',' {
                        let mut new_project_word = String::from(project_word);
                        new_project_word.pop();
                        projection.push(new_project_word);
                    } else {
                        projection.push(String::from(project_word));
                        break;
                    }
                } else {
                    loop_error = true;
                    break;
                }
            }
            if loop_error {
                ast = HybQuery::BadQuery(String::from("You must list columns for projection or *"));
            } else {
                if let Some(from_word) = query_iter.next(){
                    if from_word.to_uppercase() == "FROM" {
                        let (sub_query, spoiler_inner) = parse_helper_hyb(query_iter);
                        spoiler = spoiler_inner;
                        if !projection.is_empty(){
                            ast = HybQuery::Project(projection, Box::new(sub_query));
                        } else {
                            ast = sub_query;
                        }
                        whereable = true;
                    } else {
                        ast = HybQuery::BadQuery(String::from("FROM must come after the projected columns of SELECT"));
                    }
                } else {
                    ast = HybQuery::BadQuery(String::from("Missing \"FROM\""));
                }
            }
        } else {
            ast = HybQuery::Id(String::from(first_word));
        }
    }
    loop{
        let join_type_fpj;
        let mut next_word : Option<String> = None;
        match spoiler{
            Some(ref suffix_word) => next_word = Some(suffix_word.to_string()),
            None => {
                match query_iter.next(){
                    Some(suffix_word) => next_word = Some(String::from(suffix_word)),
                    None => {}
                }
            }
        }
        if let Some(suffix_word) = next_word{
            if suffix_word.to_uppercase() == "WHERE" && whereable{
                whereable = false;
                spoiler = None;
                let b_query = parse_bool(query_iter);
                ast = HybQuery::Select(b_query, Box::new(ast));
            } else if suffix_word.to_uppercase() == "JOINF" || suffix_word.to_uppercase() == "JOINP" {
                if suffix_word.to_uppercase() == "JOINF"{
                    join_type_fpj = true;
                } else {
                    join_type_fpj = false
                }
                let (sub_query, spoiler_inner) = parse_helper_hyb(query_iter);
                spoiler = spoiler_inner;
                let mut on_word_opt : Option<String> = None;
                match spoiler{
                    Some(ref pos_on_word) => on_word_opt = Some(pos_on_word.to_string()),
                    None => {
                        match query_iter.next(){
                            Some(pos_on_word) => on_word_opt = Some(String::from(pos_on_word)),
                            None => {}
                        }
                    }
                }
                spoiler = None;
                if let Some(on_word) = on_word_opt{
                    if on_word.to_uppercase() == "ON" {
                        let mut error_in_on = true;
                        if let Some(attrib1) = query_iter.next(){
                            if let Some(eq_word) = query_iter.next(){
                                if eq_word == "=" {
                                    if let Some(attrib2) = query_iter.next(){
                                        error_in_on = false;
                                        if join_type_fpj {
                                            ast = HybQuery::FPJoin(
                                                String::from(attrib1),
                                                String::from(attrib2),
                                                Box::new(ast),
                                                Box::new(sub_query)
                                            );
                                        } else {
                                            ast = HybQuery::PPJoin(
                                                String::from(attrib1),
                                                String::from(attrib2),
                                                Box::new(ast),
                                                Box::new(sub_query)
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        if error_in_on {
                            ast = HybQuery::BadQuery(String::from("You must join on attrib1 = attrib2"));
                        }
                    } else {
                        ast = HybQuery::BadQuery(String::from("A word other than ON followed a JOIN. Remember, no natural joins"));
                    }
                } else {
                    ast = HybQuery::BadQuery(String::from("You must specify what attributes to join on. Sorry - no natural joins"));
                }
            } else {
                spoiler = Some(String::from(suffix_word));
                break;
            }
        } else {
            break;
        }
    }
    (ast, spoiler)
}

