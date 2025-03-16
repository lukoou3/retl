use std::collections::HashMap;
use mysql::prelude::*;
use mysql::*;

fn test1() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Pool::new("mysql://root:123456@localhost:3306/test")?;
    let mut conn = pool.get_conn()?;
    //let rsts:Vec<Row> = conn.query("SELECT * from test_detail limit 5")?;
    let mut name_idx = HashMap::new();
    let mut rsts:Vec<Row> = conn.query("desc test_detail")?;
    for r in rsts.iter_mut() {
        if name_idx.is_empty() {
            let cols: Vec<_> = r.columns().iter().map(|c| (c.name_str().to_string(), c.column_type())).collect();
            println!("{:?}", cols);
            for (i, c) in r.columns().iter().enumerate() {
                name_idx.insert(c.name_str().to_string(), i);
            }
        }
        //println!("{:?}", r.columns());
        //println!("{:?}", r[0]);
        //println!("{:?}", r);

        println!("Field:{:?},Type:{:?},Default:{:?}",
                 r.take::<Option<String>, _>(name_idx["Field"]).unwrap(),
                 r.take::<Option<String>, _>(name_idx["Type"]).unwrap(),
                 r.take::<Option<String>, _>(name_idx["Default"]).unwrap());
    }
    Ok(())
}

fn test2() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Pool::new("mysql://root:123456@localhost:3306/test")?;
    let mut conn = pool.get_conn()?;
    //let rsts:Vec<Row> = conn.query("SELECT * from test_detail limit 5")?;
    let mut rsts:Vec<Row> = conn.query("SELECT COLUMN_NAME,DATA_TYPE,COLUMN_TYPE, COLUMN_DEFAULT,COLUMN_KEY,EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'test_detail' AND TABLE_SCHEMA = DATABASE() order by ORDINAL_POSITION")?;
    //let mut rsts:Vec<Row> = conn.query("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'test_detail' AND TABLE_SCHEMA = DATABASE() order by ORDINAL_POSITION")?;
    for r in rsts.iter_mut() {
        println!("{:?}", r);
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = OptsBuilder::new().ip_or_hostname(Some("localhost")).tcp_port(3306)
        .user(Some("root")).pass(Some("123456")).db_name(Some("test"));
    let pool = Pool::new(opts)?;
    let mut conn = pool.get_conn()?;
    let mut rsts:Vec<Row> = conn.query("SELECT COLUMN_NAME,DATA_TYPE,COLUMN_TYPE, COLUMN_DEFAULT,COLUMN_KEY,EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'test_detail' AND TABLE_SCHEMA = DATABASE() order by ORDINAL_POSITION")?;
    //let mut rsts:Vec<Row> = conn.query("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'test_detail' AND TABLE_SCHEMA = DATABASE() order by ORDINAL_POSITION")?;
    for r in rsts.iter_mut() {
        println!("{:?}", r);
    }
    conn.query_drop("INSERT INTO test_detail (id,name,age,sex,addr) VALUES (1,'张三',20,'男','北京')")?;
    Ok(())
}