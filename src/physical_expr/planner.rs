use std::sync::Arc;
use chrono::Utc;
use crate::expr::{self, Expr};
use crate::physical_expr::{And, BinaryArithmetic, BinaryComparison, BoundReference, Like, Literal, Or, PhysicalExpr, RLike};
use crate::{Operator, Result};

pub fn create_physical_expr(
    e: &Expr,
) -> Result<Arc<dyn PhysicalExpr>> {
    match e {
        Expr::BoundReference(expr::BoundReference{ordinal, data_type}) =>
            Ok(Arc::new(BoundReference::new(*ordinal, data_type.clone()))),
        Expr::Alias(expr::Alias{child, ..}) =>
            create_physical_expr(child),
        Expr::Literal(expr::Literal{value, data_type}) =>
            Ok(Arc::new(Literal::new(value.clone(), data_type.clone()))),
        Expr::BinaryOperator(expr::BinaryOperator{left, op, right}) => match op {
            Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo => {
                let l = create_physical_expr(left)?;
                let r = create_physical_expr(right)?;
                Ok(Arc::new(BinaryArithmetic::new(l, op.clone(), r)))
            }
            Operator::Eq | Operator::NotEq | Operator::Lt |Operator::LtEq | Operator::Gt | Operator::GtEq =>
                Ok(Arc::new(BinaryComparison::new(create_physical_expr(left)?, op.clone(), create_physical_expr(right)?))),
            Operator::And =>
                Ok(Arc::new(And::new(create_physical_expr(left)?, create_physical_expr(right)?))),
            Operator::Or =>
                Ok(Arc::new(Or::new(create_physical_expr(left)?, create_physical_expr(right)?))),
        },
        Expr::Like(expr::Like{expr, pattern}) =>
            Ok(Arc::new(Like::new(create_physical_expr(expr)?, create_physical_expr(pattern)?))),
        Expr::RLike(expr::Like{expr, pattern}) =>
            Ok(Arc::new(RLike::new(create_physical_expr(expr)?, create_physical_expr(pattern)?))),
        _ => Err(format!("Not implemented:{:?}", e)),
    }

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

}