use std::collections::HashSet;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use ahash::AHasher;
use crate::data::{ExtendObject, Object, Row, Value};
use crate::{expr, Result};
use crate::expr::aggregate::{CreateTypedAggFunction, PhysicalTypedAggFunction, TypedAggAttr, TypedAggFunction};
use crate::expr::Expr;
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct CollectSet {
    child: Box<Expr>,
    mutable_agg_buffer_offset: usize,
    input_agg_buffer_offset: usize,
    data_type: DataType,
    agg_attr: TypedAggAttr,
}

impl CollectSet {
    pub fn new(child: Box<Expr>, mutable_agg_buffer_offset: usize, input_agg_buffer_offset: usize) -> Self {
        let data_type = DataType::Array(Box::new(child.data_type().clone()));
        let agg_attr = TypedAggAttr::new(data_type.clone());
        CollectSet { child, mutable_agg_buffer_offset, input_agg_buffer_offset, data_type, agg_attr}
    }
}

impl CreateTypedAggFunction for CollectSet {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn TypedAggFunction>> {
        if args.len() != 1 {
            return Err("requires one argument".into());
        }
        Ok(Box::new(CollectSet::new(Box::new(args[0].clone()), 0, 0)))
    }
}

impl TypedAggFunction for CollectSet {
    fn name(&self) -> &str {
        "collect_set"
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn with_new_mutable_agg_buffer_offset(&self, offset: usize) -> Box<dyn TypedAggFunction> {
        let mut f = self.clone();
        f.mutable_agg_buffer_offset = offset;
        Box::new(f)
    }

    fn agg_attr(&self) -> &TypedAggAttr {
        & self.agg_attr
    }

    fn physical_function(&self) -> Result<Box<dyn PhysicalTypedAggFunction>> {
        Ok(Box::new(PhysicalCollectSet::new(
            expr::create_physical_expr(&self.child)?,
            self.mutable_agg_buffer_offset,
            self.input_agg_buffer_offset,
            self.data_type.clone()
        )))
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }
}

pub struct PhysicalCollectSet {
    child: Box<dyn PhysicalExpr>,
    mutable_agg_buffer_offset: usize,
    input_agg_buffer_offset: usize,
    data_type: DataType
}

impl PhysicalCollectSet {
    pub fn new(child: Box<dyn PhysicalExpr>, mutable_agg_buffer_offset: usize, input_agg_buffer_offset: usize, data_type: DataType) -> Self {
        PhysicalCollectSet { child, mutable_agg_buffer_offset, input_agg_buffer_offset, data_type }
    }
}

impl PhysicalTypedAggFunction for PhysicalCollectSet {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn mutable_agg_buffer_offset(&self) -> usize {
        self.mutable_agg_buffer_offset
    }

    fn input_agg_buffer_offset(&self) -> usize {
        self.input_agg_buffer_offset
    }

    fn create_agg_buffer(&self) -> Value {
        Value::Object(Box::new(Set::new()))
    }

    fn update_value(&self, buffer: &mut Value, input: &dyn Row) {
        let value = self.child.eval(input);
        if value.is_null() {
            return;
        }
        match buffer {
            Value::Object(obj) => {
                let set = obj.as_mut_any().downcast_mut::<Set>().unwrap();
                set.set.insert(value);
            }
            _ => panic!("invalid agg buffer")
        }
    }

    fn merge_value(&self, buffer: &mut Value, input: Value){
        match (buffer, input) {
            (Value::Object(obj), Value::Object(input)) => {
                let any = obj.as_mut_any();
                if let Some(set) = any.downcast_mut::<Set>() {
                    let data = input.into_any().downcast::<Set>().unwrap();
                    set.set.extend(data.set.into_iter());
                } else {
                    panic!("invalid agg buffer")
                }
            },
            _ => panic!("invalid agg buffer")
        }
    }

    fn eval_value(&self, buffer: Value) -> Value {
        match buffer {
            Value::Object(obj) => {
                let set = obj.into_any().downcast::<Set>().unwrap();
                let array = set.set.into_iter().collect();
                Value::Array(Arc::new(array))
            }
            _ => panic!("invalid agg buffer")
        }
    }
}

#[derive(Debug, Clone)]
struct Set {
    set: HashSet<Value, BuildHasherDefault<AHasher>>
}

impl Set {
    fn new() -> Self {
        Set { set: HashSet::with_hasher(BuildHasherDefault::<AHasher>::default()) }
    }
}

impl Object for Set {}