use std::fmt::{Debug, Formatter};
use std::sync::Mutex;
use crate::data::Value;
use crate::Result;
use crate::expr::{AttributeReference, Expr};
use crate::expr::aggregate::{CreateDeclarativeAggFunction, DeclarativeAggFunction};
use crate::types::DataType;

pub struct Max {
    child: Box<Expr>,
    max: Mutex<Option<AttributeReference>>,
    input_agg_attrs: Mutex<Vec<AttributeReference>>,
    result_attr: Mutex<Option<AttributeReference>>,
}

impl Max {
    pub fn new(child: Box<Expr>) -> Self {
        let max = Mutex::new(None);
        let input_agg_attrs = Mutex::new(vec![]);
        let result_attr = Mutex::new(None);
        Self { child, max, input_agg_attrs, result_attr }
    }

    fn max_attr(&self) -> AttributeReference {
        let mut max_guard = self.max.lock().unwrap();
        if max_guard.is_none() {
            *max_guard = Some(AttributeReference::new("max", self.child.data_type().clone()));
        }
        max_guard.as_ref().unwrap().clone()
    }

    fn input_agg_attrs(&self) -> Vec<AttributeReference> {
        let mut input_agg_attrs_guard = self.input_agg_attrs.lock().unwrap();
        if input_agg_attrs_guard.is_empty() {
            *input_agg_attrs_guard = vec![self.max_attr().new_instance()];
        }
        input_agg_attrs_guard.clone()
    }

    fn result_attr(&self) -> AttributeReference {
        let mut result_attr_attr_guard = self.result_attr.lock().unwrap();
        if result_attr_attr_guard.is_none() {
            *result_attr_attr_guard = Some(AttributeReference::new("max", self.child.data_type().clone()));
        }
        result_attr_attr_guard.as_ref().unwrap().clone()
    }

    fn max_left(&self) -> Expr {
        self.max()
    }

    fn max_right(&self) -> Expr {
        Expr::AttributeReference(self.input_agg_attrs()[0].clone())
    }

    fn max(&self) -> Expr {
        Expr::AttributeReference(self.max_attr())
    }
}

impl CreateDeclarativeAggFunction for Max {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn DeclarativeAggFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl Debug for Max {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Count")
            .field("child", &self.child)
            .field("max", &self.max.lock().unwrap())
            .field("input_agg_attrs", &self.input_agg_attrs.lock().unwrap())
            .field("result_attr", &self.result_attr.lock().unwrap())
            .finish()
    }
}

impl Clone for Max {
    fn clone(&self) -> Self {
        Self {
            child: self.child.clone(),
            max: Mutex::new(self.max.lock().unwrap().clone()),
            input_agg_attrs: Mutex::new(self.input_agg_attrs.lock().unwrap().clone()),
            result_attr: Mutex::new(self.result_attr.lock().unwrap().clone()),
        }
    }
}

impl DeclarativeAggFunction for Max {
    fn name(&self) -> &str {
        "max"
    }

    fn data_type(&self) -> &DataType {
        self.child.data_type()
    }

    fn agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        vec![self.max_attr()]
    }

    fn input_agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        self.input_agg_attrs()
    }

    fn result_attribute(&self) -> AttributeReference {
        self.result_attr()
    }

    fn initial_values(&self) -> Vec<Expr> {
        vec![Expr::lit(Value::Null, self.child.data_type().clone())]
    }

    fn update_expressions(&self) -> Vec<Expr> {
        vec![self.max().greatest(*self.child.clone())]
    }

    fn merge_expressions(&self) -> Vec<Expr> {
        vec![self.max_left().greatest(self.max_right())]
    }

    fn evaluate_expression(&self) -> Expr {
        self.max()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn check_input_data_types(&self) -> Result<()> {
        let tp = self.child.data_type();
        if !tp.is_orderable()  {
            return Err(format!("expressions must be orderable, but found {:?}", tp));
        }
        Ok(())
    }
}

