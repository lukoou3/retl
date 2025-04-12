use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;
use itertools::Itertools;
use crate::Result;
use crate::data::{Row, Value};
use crate::expr::{AttributeReference, Expr};
use crate::types::{AbstractDataType, DataType};

pub trait DeclarativeAggFunction: Debug + Send + Sync + CreateDeclarativeAggFunction + ExtendDeclarativeAggFunction {
    fn name(&self) -> &str;
    fn data_type(&self) -> &DataType;
    fn agg_buffer_attributes(&self) -> Vec<AttributeReference>;
    fn input_agg_buffer_attributes(&self) -> Vec<AttributeReference>;
    fn result_attribute(&self) -> AttributeReference;
    fn initial_values(&self) -> Vec<Expr>;
    fn update_expressions(&self) -> Vec<Expr>;
    fn merge_expressions(&self) -> Vec<Expr>;
    fn evaluate_expression(&self) -> Expr;
    fn args(&self) -> Vec<&Expr>;

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        None
    }
    fn check_input_data_types(&self) ->Result<()> {
        match self.expects_input_types() {
            None => {
                Ok(())
            },
            Some(input_types) => {
                let mut mismatches = Vec::new();
                for (i, (tp, input_type)) in self.args().into_iter().zip(input_types.iter()).enumerate() {
                    if !input_type.accepts_type(tp.data_type()) {
                        mismatches.push(format!("{} argument {} requires {:?}, but get {}", self.name(), i + 1, input_type, tp.data_type()));
                    }
                }
                if mismatches.is_empty() {
                    Ok(())
                } else {
                    Err(mismatches.into_iter().join(" "))
                }
            },
        }
    }
}

pub trait CreateDeclarativeAggFunction {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn DeclarativeAggFunction>> where Self: Sized;
    fn create_function_expr(args: Vec<Expr>) -> Result<Expr> where Self: Sized {
        Ok(Expr::DeclarativeAggFunction(Self::from_args(args)?))
    }
}

pub trait ExtendDeclarativeAggFunction {
    fn clone_box(&self) -> Box<dyn DeclarativeAggFunction>;
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn DeclarativeAggFunction>;
}

impl<T: DeclarativeAggFunction + Clone + 'static> ExtendDeclarativeAggFunction for T {
    fn clone_box(&self) -> Box<dyn DeclarativeAggFunction> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn DeclarativeAggFunction> {
        Self::from_args(args).unwrap()
    }
}

impl Clone for Box<dyn DeclarativeAggFunction> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl PartialEq for Box<dyn DeclarativeAggFunction> {
    fn eq(&self, other: &Self) -> bool {
        if self.as_any().type_id() != other.as_any().type_id() {
            return false;
        }
        let args1 = self.args();
        let args2 = other.args();
        if args1.len() != args2.len() {
            return false;
        };
        args1.iter().zip(args2.iter()).all(|(a, b)| a == b)
    }
}

impl Eq for Box<dyn DeclarativeAggFunction> {}

impl PartialOrd for Box<dyn DeclarativeAggFunction> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let args1 = self.args();
        let args2 = other.args();
        if args1.len() != args2.len() {
            return None;
        };
        for i in 0..args1.len() {
            match args1[i].partial_cmp(args2[i]) {
                None => return None, // 某个元素无法比较
                Some(Ordering::Equal) => continue, // 继续比较下一个元素
                Some(ord) => return Some(ord), // 返回当前元素的比较结果
            }
        }
        // 所有元素都相等
        Some(Ordering::Equal)
    }
}

impl Hash for Box<dyn DeclarativeAggFunction> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for x in self.args() {
            x.hash(state);
        }
    }
}
pub trait TypedAggFunction {
    fn name(&self) -> &str;
    fn data_type(&self) -> &DataType;
    fn args(&self) -> Vec<&Expr>;

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        None
    }
    fn check_input_data_types(&self) -> crate::Result<()> {
        match self.expects_input_types() {
            None => {
                Ok(())
            },
            Some(input_types) => {
                let mut mismatches = Vec::new();
                for (i, (tp, input_type)) in self.args().into_iter().zip(input_types.iter()).enumerate() {
                    if !input_type.accepts_type(tp.data_type()) {
                        mismatches.push(format!("{} argument {} requires {:?}, but get {}", self.name(), i + 1, input_type, tp.data_type()));
                    }
                }
                if mismatches.is_empty() {
                    Ok(())
                } else {
                    Err(mismatches.into_iter().join(" "))
                }
            },
        }
    }

    fn agg_data_type(&self) -> &DataType;
    fn create_agg_buffer(&self) -> Value;

    fn update(&self, buffer: Value, input: &dyn Row) -> Value;

    fn merge(&self, buffer: Value, input: Value) -> Value;

    fn eval(&self, buffer: Value) -> Value;

    // def eval(buffer: T): Any
}