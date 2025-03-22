use chrono::Utc;
use retl::data::{GenericRow, Row, Value};
use retl::expr::Expr;
use retl::parser;
use retl::physical_expr::create_physical_expr;
use retl::types::DataType;

fn main() {
  test();
}
fn test(){
    println!("Hello, world!");
    let result = parser::parse_query("select a + 1 a, func('1') b, func2(a, 3) b, a < b + 1  e, nuallable_int in (1, 2, 3) is_in from tbl");
    println!("{:?}", result);
    let result = parser::parse_data_type("struct<int: int, bigint:bigint, struct:struct<intType: int, longType:bigint>, arrAy:Array<double>, anotherArray:Array<string>>");
    println!("{:?}", result);
}

fn run(){
    let col1 = Expr::col(0, DataType::Int);
    let col2 = Expr::col(1, DataType::Int);
    let literal = Expr::lit(Value::Int(10), DataType::Int);
    let expr = col1 + col2 + literal;
    println!("{:#?}", expr);
    let expr = create_physical_expr(&expr).unwrap();
    println!("{:#?}", expr);
    let mut row:Box<dyn Row> = Box::new(GenericRow::new(vec![Value::Int(101), Value::Int(102) ]));
    let rst = expr.eval(&*row);
    println!("row:{:?}", &*row);
    println!("rst:{:?}", rst);
    row.update(1, Value::Int(1000));
    let rst = expr.eval(&*row);
    println!("row:{:?}", row);
    println!("rst:{:?}", rst);
    row.update(1, Value::Null);
    let rst = expr.eval(&*row);
    println!("row:{:?}", row);
    println!("rst:{:?}", rst);
    row.update(1, Value::Int(100));
    let rst = expr.eval(&*row);
    println!("row:{:?}", row);
    println!("rst:{:?}", rst);
    let start = Utc::now().timestamp_millis();
    println!("start:{}", start);
    let mut sum:i64 = 0;
    for _ in 0..1000000 {
        sum += expr.eval(&*row).get_int() as i64;
    }
    let end = Utc::now().timestamp_millis();
    println!("sum:{}", sum);
    println!("end:{}", end);
    println!("time:{}", end - start);
}
