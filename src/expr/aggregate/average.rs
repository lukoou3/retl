use std::fmt::{Debug, Formatter};
use std::sync::Mutex;
use crate::expr::{coalesce2, AttributeReference, Expr, If};
use crate::expr::aggregate::{CreateDeclarativeAggFunction, DeclarativeAggFunction};
use crate::types::DataType;

pub struct Average {
    child: Box<Expr>,
    sum: Mutex<Option<AttributeReference>>,
    count: Mutex<Option<AttributeReference>>,
    input_agg_attrs: Mutex<Vec<AttributeReference>>,
    result_attr: Mutex<Option<AttributeReference>>,
}

impl Average {
    pub fn new(child: Box<Expr>) -> Self {
        let sum = Mutex::new(None);
        let count = Mutex::new(None);
        let input_agg_attrs = Mutex::new(vec![]);
        let result_attr = Mutex::new(None);
        Self { child, sum, count, input_agg_attrs, result_attr }
    }

    fn sum_attr(&self) -> AttributeReference {
        let mut sum_guard = self.sum.lock().unwrap();
        if sum_guard.is_none() {
            *sum_guard = Some(AttributeReference::new("sum", DataType::Double));
        }
        sum_guard.as_ref().unwrap().clone()
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
            *input_agg_attrs_guard = vec![self.sum_attr().new_instance(), self.count_attr().new_instance()];
        }
        input_agg_attrs_guard.clone()
    }

    fn result_attr(&self) -> AttributeReference {
        let mut result_attr_attr_guard = self.result_attr.lock().unwrap();
        if result_attr_attr_guard.is_none() {
            *result_attr_attr_guard = Some(AttributeReference::new("average", DataType::Double));
        }
        result_attr_attr_guard.as_ref().unwrap().clone()
    }

    fn sum_left(&self) -> Expr {
        self.sum()
    }

    fn sum_right(&self) -> Expr {
        Expr::AttributeReference(self.input_agg_attrs()[0].clone())
    }

    fn sum(&self) -> Expr {
        Expr::AttributeReference(self.sum_attr())
    }

    fn count_left(&self) -> Expr {
        self.count()
    }

    fn count_right(&self) -> Expr {
        Expr::AttributeReference(self.input_agg_attrs()[1].clone())
    }

    fn count(&self) -> Expr {
        Expr::AttributeReference(self.count_attr())
    }

    fn child_cast(&self) -> Expr {
        self.child.clone().cast(DataType::Double)
    }
}

impl CreateDeclarativeAggFunction for Average {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn DeclarativeAggFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let child = args[0].clone();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl Debug for Average {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Average")
            .field("child", &self.child)
            .field("sum", &self.sum.lock().unwrap())
            .field("count", &self.count.lock().unwrap())
            .field("input_agg_attrs", &self.input_agg_attrs.lock().unwrap())
            .field("result_attr", &self.result_attr.lock().unwrap())
            .finish()
    }
}

impl Clone for Average {
    fn clone(&self) -> Self {
        Self {
            child: self.child.clone(),
            sum: Mutex::new(self.sum.lock().unwrap().clone()),
            count: Mutex::new(self.count.lock().unwrap().clone()),
            input_agg_attrs: Mutex::new(self.input_agg_attrs.lock().unwrap().clone()),
            result_attr: Mutex::new(self.result_attr.lock().unwrap().clone()),
        }
    }
}

impl DeclarativeAggFunction for Average {
    fn name(&self) -> &str {
        "average"
    }

    fn data_type(&self) -> &DataType {
        DataType::double_type()
    }

    fn agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        vec![self.sum_attr(), self.count_attr()]
    }

    fn input_agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        self.input_agg_attrs()
    }

    fn result_attribute(&self) -> AttributeReference {
        self.result_attr()
    }

    fn initial_values(&self) -> Vec<Expr> {
        vec![Expr::double_lit(0.0), Expr::long_lit(0)]
    }

    fn update_expressions(&self) -> Vec<Expr> {
        let sum = self.sum() + coalesce2(self.child_cast(), Expr::double_lit(0.0));
        let count = Expr::ScalarFunction(Box::new(If::new(
            Box::new(self.child.clone().is_null()),
            Box::new(self.count()),
            Box::new(self.count() + Expr::long_lit(1)),
        )));
        vec![sum, count]
    }

    fn merge_expressions(&self) -> Vec<Expr> {
        let sum = self.sum_left() + self.sum_right();
        let count = self.count_left() + self.count_right();
        vec![sum, count]
    }

    fn evaluate_expression(&self) -> Expr {
        self.sum() / self.count().cast(DataType::Double)
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }
}




