use std::collections::HashMap;
use retl::analysis::Analyzer;
use retl::logical_plan::RelationPlaceholder;
use retl::parser;
use retl::types::{DataType, Field, Schema};

fn main() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int),
        Field::new("cate", DataType::String),
        Field::new("text", DataType::String),
        Field::new("in_bytes", DataType::Long),
        Field::new("out_bytes", DataType::Long),
    ]);
    let mut temp_views = HashMap::new();
    temp_views.insert("tbl".to_string(), RelationPlaceholder::new("tbl".to_string(), schema.to_attributes()));
    let plan = parser::parse_query("select id, cate, text, in_bytes, out_bytes, (in_bytes + out_bytes) bytes, (1 + out_bytes) bytes2 from tbl").unwrap();
    println!("{:?}", plan);
    //println!("{:#?}", plan);
    let analyzer = Analyzer::new(temp_views);
    match analyzer.analyze(plan) {
        Ok(new_plan) => {
            println!("\n{:?}", new_plan);
            println!("\n{:?}", new_plan.output());
        },
        Err(e) => println!("\n{}", e)
    }
}