use std::sync::Arc;
use crate::data::{Object, Row, Value};
use crate::{expr, Result};
use crate::expr::aggregate::{CreateTypedAggFunction, PhysicalTypedAggFunction, TypedAggAttr, TypedAggFunction};
use crate::expr::Expr;
use crate::physical_expr::PhysicalExpr;
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct CollectList {
    child: Box<Expr>,
    mutable_agg_buffer_offset: usize,
    input_agg_buffer_offset: usize,
    data_type: DataType,
    agg_attr: TypedAggAttr,
}

impl CollectList {
    pub fn new(child: Box<Expr>, mutable_agg_buffer_offset: usize, input_agg_buffer_offset: usize) -> Self {
        let data_type = DataType::Array(Box::new(child.data_type().clone()));
        let agg_attr = TypedAggAttr::new(data_type.clone());
        CollectList { child, mutable_agg_buffer_offset, input_agg_buffer_offset, data_type, agg_attr}
    }
}

impl CreateTypedAggFunction for CollectList {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn TypedAggFunction>> {
        if args.len() != 1 {
            return Err("requires one argument".into());
        }
        Ok(Box::new(CollectList::new(Box::new(args[0].clone()), 0, 0)))
    }
}

impl TypedAggFunction for CollectList {
    fn name(&self) -> &str {
        "collect_list"
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
        Ok(Box::new(PhysicalCollectList::new(
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

pub struct PhysicalCollectList {
    child: Box<dyn PhysicalExpr>,
    mutable_agg_buffer_offset: usize,
    input_agg_buffer_offset: usize,
    data_type: DataType
}

impl PhysicalCollectList {
    pub fn new(child: Box<dyn PhysicalExpr>, mutable_agg_buffer_offset: usize, input_agg_buffer_offset: usize, data_type: DataType) -> Self {
        PhysicalCollectList { child, mutable_agg_buffer_offset, input_agg_buffer_offset, data_type }
    }
}

impl PhysicalTypedAggFunction for PhysicalCollectList {
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
        Value::Object(Box::new(List::new()))
    }

    fn update_value(&self, buffer: &mut Value, input: &dyn Row) {
        let value = self.child.eval(input);
        if value.is_null() {
            return;
        }
        match buffer {
            Value::Object(obj) => {
                let list = obj.as_mut_any().downcast_mut::<List>().unwrap();
                list.data.push(value);
            }
            _ => panic!("invalid agg buffer")
        }
    }

    fn merge_value(&self, buffer: &mut Value, input: Value){
        match (buffer, input) {
            (Value::Object(obj), Value::Object(input)) => {
                let any = obj.as_mut_any();
                if let Some(list) = any.downcast_mut::<List>() {
                    let data = input.into_any().downcast::<List>().unwrap();
                    list.data.extend(data.data.into_iter());
                } else {
                    panic!("invalid agg buffer");
                }
            },
            _ => panic!("invalid agg buffer")
        }
    }

    fn eval_value(&self, buffer: Value) -> Value {
        match buffer {
            Value::Object(obj) => {
                let list = obj.into_any().downcast::<List>().unwrap();
                let array = list.data.iter().map(|v| v.clone()).collect();
                Value::Array(Arc::new(array))
            }
            _ => panic!("invalid agg buffer")
        }
    }
}

#[derive(Debug, Clone)]
struct List {
    data: Vec<Value>
}

impl List {
    fn new() -> Self {
        List { data: Vec::new() }
    }
}

impl Object for List {}