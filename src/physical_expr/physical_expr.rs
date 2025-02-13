use std::any::Any;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use crate::data::{Row, Value};
use crate::types::DataType;

pub trait PhysicalExpr: Send + Sync + Debug + DynEq + DynHash  {
    /// Returns the physical expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
    fn data_type(&self) -> DataType;

    fn eval(&self, input: &dyn Row) -> Value;
}

/// [`PhysicalExpr`] can't be constrained by [`Eq`] directly because it must remain object
/// safe. To ease implementation blanket implementation is provided for [`Eq`] types.
pub trait DynEq {
    fn dyn_eq(&self, other: &dyn Any) -> bool;
}

impl<T: Eq + Any> DynEq for T {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other.downcast_ref::<Self>() == Some(self)
    }
}

impl PartialEq for dyn PhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other.as_any())
    }
}

impl Eq for dyn PhysicalExpr {}

/// [`PhysicalExpr`] can't be constrained by [`Hash`] directly because it must remain
/// object safe. To ease implementation blanket implementation is provided for [`Hash`]
/// types.
pub trait DynHash {
    fn dyn_hash(&self, _state: &mut dyn Hasher);
}

impl<T: Hash + Any> DynHash for T {
    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.type_id().hash(&mut state);
        self.hash(&mut state)
    }
}

impl Hash for dyn PhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
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
            left: Arc::new(BoundReference::new(0, DataType::Int)),
            op: Operator::Plus,
            right: Arc::new(BoundReference::new(1, DataType::Int)),
            f: Arc::new(add_int)
        };
        let expr = BinaryArithmetic {
            left: Arc::new(expr1),
            op: Operator::Plus,
            right: Arc::new(Literal::new(Value::Int(10), DataType::Int)),
            f: Arc::new(add_int)
        };
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
            Arc::new(BoundReference::new(0, DataType::String)),
            Arc::new(Literal::new(Value::string("%ab%"), DataType::String))
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