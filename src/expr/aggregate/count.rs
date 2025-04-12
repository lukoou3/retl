use std::fmt::{Debug, Formatter};
use std::sync::Mutex;
use crate::expr::{AttributeReference, Expr, If, Literal};
use crate::expr::aggregate::{CreateDeclarativeAggFunction, DeclarativeAggFunction};
use crate::types::DataType;

pub struct Count {
    child: Box<Expr>,
    count: Mutex<Option<AttributeReference>>,
    input_agg_attrs: Mutex<Vec<AttributeReference>>,
    result_attr: Mutex<Option<AttributeReference>>,
}

impl Count {
    pub fn new(child: Box<Expr>) -> Self {
        let count = Mutex::new(None);
        let input_agg_attrs = Mutex::new(vec![]);
        let result_attr = Mutex::new(None);
        Self { child, count, input_agg_attrs, result_attr }
    }

    fn count_attr(&self) -> AttributeReference {
        let mut count_guard = self.count.lock().unwrap();
        if count_guard.is_none() {
            *count_guard = Some(AttributeReference::new("count", DataType::Long));
        }
        count_guard.as_ref().unwrap().clone()
    }

    fn input_agg_attrs(&self) -> Vec<AttributeReference> {
        let mut input_agg_attrs_guard = self.input_agg_attrs.lock().unwrap();
        if input_agg_attrs_guard.is_empty() {
            *input_agg_attrs_guard = vec![self.count_attr().new_instance()];
        }
        input_agg_attrs_guard.clone()
    }

    fn result_attr(&self) -> AttributeReference {
        let mut result_attr_attr_guard = self.result_attr.lock().unwrap();
        if result_attr_attr_guard.is_none() {
            *result_attr_attr_guard = Some(AttributeReference::new("count", DataType::Long));
        }
        result_attr_attr_guard.as_ref().unwrap().clone()
    }

    fn count_left(&self) -> Expr {
        self.count()
    }

    fn count_right(&self) -> Expr {
        Expr::AttributeReference(self.input_agg_attrs()[0].clone())
    }

    fn count(&self) -> Expr {
        Expr::AttributeReference(self.count_attr())
    }
}

impl CreateDeclarativeAggFunction for Count {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn DeclarativeAggFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl Debug for Count {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Count")
            .field("child", &self.child)
            .field("count", &self.count.lock().unwrap())
            .field("input_agg_attrs", &self.input_agg_attrs.lock().unwrap())
            .field("result_attr", &self.result_attr.lock().unwrap())
            .finish()
    }
}

impl Clone for Count {
    fn clone(&self) -> Self {
        Self {
            child: self.child.clone(),
            count: Mutex::new(self.count.lock().unwrap().clone()),
            input_agg_attrs: Mutex::new(self.input_agg_attrs.lock().unwrap().clone()),
            result_attr: Mutex::new(self.result_attr.lock().unwrap().clone()),
        }
    }
}

impl DeclarativeAggFunction for Count {
    fn name(&self) -> &str {
        "count"
    }

    fn data_type(&self) -> &DataType {
        DataType::long_type()
    }

    fn agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        vec![self.count_attr()]
    }

    fn input_agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        self.input_agg_attrs()
    }

    fn result_attribute(&self) -> AttributeReference {
        self.result_attr()
    }

    fn initial_values(&self) -> Vec<Expr> {
        vec![Expr::long_lit(0)]
    }

    fn update_expressions(&self) -> Vec<Expr> {
        let expr = match self.child.as_ref() {
            Expr::Literal(Literal{value, ..})  if !value.is_null()=> {
                self.count() + Expr::long_lit(1)
            },
            _ => {
                let f = If::new(
                    Box::new(self.child.clone().is_null()),
                    Box::new(self.count()),
                    Box::new(self.count() + Expr::long_lit(1)),
                );
                Expr::ScalarFunction(Box::new(f))
            }
        };
        vec![expr]
    }

    fn merge_expressions(&self) -> Vec<Expr> {
        vec![self.count_left() + self.count_right()]
    }

    fn evaluate_expression(&self) -> Expr {
        self.count()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }
}


