use std::any::Any;
use std::fmt::{Debug};
use std::hash::{Hash, Hasher};
use crate::data::{Row, Value};
use crate::types::DataType;

pub trait PhysicalExpr: Debug  {
    /// Returns the physical expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
    fn data_type(&self) -> DataType;

    fn eval(&self, input: &dyn Row) -> Value;
}

pub trait UnaryExpr: PhysicalExpr {
    fn child(&self) -> &dyn PhysicalExpr;

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child().eval(input);
        if value.is_null() {
            Value::Null
        } else {
            self.null_safe_eval(value)
        }
    }

    fn null_safe_eval(&self, value: Value) -> Value;
}

pub trait BinaryExpr: PhysicalExpr {
    fn left(&self) -> &dyn PhysicalExpr;
    fn right(&self) -> &dyn PhysicalExpr;

    fn eval(&self, input: &dyn Row) -> Value {
        let value1 = self.left().eval(input);
        if value1.is_null() {
            Value::Null
        } else {
            let value2 = self.right().eval(input);
            if value2.is_null() {
                Value::Null
            } else {
                self.null_safe_eval(value1, value2)
            }
        }
    }

    fn null_safe_eval(&self, value1: Value, value2: Value) -> Value;
}

pub trait TernaryExpr: PhysicalExpr {
    fn child1(&self) -> &dyn PhysicalExpr;
    fn child2(&self) -> &dyn PhysicalExpr;
    fn child3(&self) -> &dyn PhysicalExpr;
    fn eval(&self, input: &dyn Row) -> Value {
        let value1 = self.child1().eval(input);
        if value1.is_null() {
            Value::Null
        } else {
            let value2 = self.child2().eval(input);
            if value2.is_null() {
                Value::Null
            } else {
                let value3 = self.child3().eval(input);
                if value3.is_null() {
                    Value::Null
                } else {
                    self.null_safe_eval(value1, value2, value3)
                }
            }
        }
    }
    fn null_safe_eval(&self, value1: Value, value2: Value, value3: Value) -> Value;
}

#[cfg(test)]
mod tests {
    use crate::data::GenericRow;
    use crate::Operator;
    use crate::types::DataType;
    use super::*;
    use super::super::*;
    fn add_int(left: Value, right: Value) -> Value {
        match (left, right) {
            (Value::Int(x), Value::Int(y)) => Value::Int(x + y),
            _ => Value::Null,
        }
    }

    #[test]
    fn test_add_int() {
        let expr1 = BinaryArithmetic {
            left: Box::new(BoundReference::new(0, DataType::Int)),
            op: Operator::Plus,
            right: Box::new(BoundReference::new(1, DataType::Int)),
            f: Box::new(add_int)
        };
        let expr = BinaryArithmetic {
            left: Box::new(expr1),
            op: Operator::Plus,
            right: Box::new(Literal::new(Value::Int(10), DataType::Int)),
            f: Box::new(add_int)
        };
        let expr: Box<dyn PhysicalExpr> = Box::new(expr);
        println!("{:?}", expr);
        let mut row1 = GenericRow::new(vec![
            Value::Int(101),
            Value::Int(102),
            Value::Int(103),
        ]);
        let row2 = GenericRow::new(vec![
            Value::Int(101),
            Value::Null,
            Value::Int(103),
        ]);
        let rst = expr.eval(&row1);
        println!("{:?}", rst);
        row1.update(1, Value::Int(1000));
        let rst = expr.eval(&row1);
        println!("{:?}", rst);
        let rst = expr.eval(&row2);
        println!("{:?}", rst);
    }

    #[test]
    fn test_like() {
        let expr = Like::new(
            Box::new(BoundReference::new(0, DataType::String)),
            Box::new(Literal::new(Value::string("%ab%"), DataType::String))
        );
        println!("{:?}", expr);
        let mut row = GenericRow::new(vec![Value::string("acb")]);
        let rst = expr.eval(&row);
        println!("{:?},{:?}", row, rst);
        row.update(0, Value::string("cabd"));
        let rst = expr.eval(&row);
        println!("{:?},{:?}", row, rst);
        row.update(0, Value::string("0ab0"));
        let rst = expr.eval(&row);
        println!("{:?},{:?}", row, rst);
        row.update(0, Value::null());
        let rst = expr.eval(&row);
        println!("{:?},{:?}", row, rst);
    }
}