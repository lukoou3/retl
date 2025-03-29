use std::sync::Arc;
use chrono::Utc;
use crate::expr::{self, Expr};
use crate::physical_expr::*;
use crate::Result;

pub fn create_physical_expr(
    e: &Expr,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr::create_physical_expr(e)
}

#[cfg(test)]
mod tests {
    use crate::data::{GenericRow, Row, Value};
    use crate::types::DataType;
    use super::*;

    #[test]
    fn test_create_physical_expr() {
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
        for _ in 0..100000 {
            sum += expr.eval(&*row).get_int() as i64;
        }
        let end = Utc::now().timestamp_millis();
        println!("sum:{}", sum);
        println!("end:{}", end);
        println!("time:{}", end - start);
    }

    #[test]
    fn test_create_scalar_func_physical_expr() {
        let col1 = Expr::col(0, DataType::String);
        let expr = Expr::ScalarFunction(Box::new(expr::Substring::new(Box::new(col1.clone()), Box::new(Expr::int_lit(2)), Box::new(Expr::int_lit(3)))));
        let expr2 = Expr::ScalarFunction(Box::new(expr::Length::new(Box::new(col1.clone()))));
        println!("{:#?}", expr);
        println!("{:#?}", expr2);
        let expr = create_physical_expr(&expr).unwrap();
        let expr2 = create_physical_expr(&expr2).unwrap();
        println!("{:#?}", expr);
        println!("{:#?}", expr2);

        let mut row:Box<dyn Row> = Box::new(GenericRow::new(vec![Value::string("123456") ]));
        let rst = expr.eval(&*row);
        let rst2 = expr2.eval(&*row);
        println!("row:{}", &*row);
        println!("rst:{}", rst);
        println!("rst2:{}", rst2);
        row.update(0, Value::string("12"));
        let rst = expr.eval(&*row);
        let rst2 = expr2.eval(&*row);
        println!("row:{}", &*row);
        println!("rst:{}", rst);
        println!("rst2:{}", rst2);

        row.update(0, Value::string(""));
        let rst = expr.eval(&*row);
        let rst2 = expr2.eval(&*row);
        println!("row:{}", &*row);
        println!("rst:{}", rst);
        println!("rst2:{}", rst2);

        row.update(0, Value::string("一二三四五六"));
        let rst = expr.eval(&*row);
        let rst2 = expr2.eval(&*row);
        println!("row:{}", &*row);
        println!("rst:{}", rst);
        println!("rst2:{}", rst2);
    }
}