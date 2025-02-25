use std::sync::Arc;
use chrono::Utc;
use crate::expr::{self, Expr};
use crate::physical_expr::*;
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
        Expr::Cast(expr::Cast{child, data_type}) =>
            Ok(Arc::new(Cast::new(create_physical_expr(child)?, data_type.clone()))),
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
        Expr::ScalarFunction(func) => {
            let any = func.as_any();
            if let Some(expr::Substring{str, pos, len}) = any.downcast_ref::<expr::Substring>() {
                Ok(Arc::new(Substring::new(create_physical_expr(str)?, create_physical_expr(pos)?, create_physical_expr(len)?)))
            } else if let Some(expr::Length{child}) = any.downcast_ref::<expr::Length>() {
                Ok(Arc::new(Length::new(create_physical_expr(child)?)))
            } else if let Some(expr::Concat{children}) = any.downcast_ref::<expr::Concat>() {
                let args = children.into_iter().map(|child| create_physical_expr(child)).collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(Concat::new(args)))
            } else {
                Err(format!("Not implemented:{:?}", func))
            }
        },
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