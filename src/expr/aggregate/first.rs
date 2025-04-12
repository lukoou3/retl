use std::fmt::{Debug, Formatter};
use std::sync::Mutex;
use crate::data::Value;
use crate::expr::{AttributeReference, Expr, If, Literal};
use crate::expr::aggregate::{CreateDeclarativeAggFunction, DeclarativeAggFunction, Min};
use crate::types::DataType;

pub struct First {
    child: Box<Expr>,
    ignore_nulls: bool,
    first: Mutex<Option<AttributeReference>>,
    value_set: Mutex<Option<AttributeReference>>,
    input_agg_attrs: Mutex<Vec<AttributeReference>>,
    result_attr: Mutex<Option<AttributeReference>>,
}

impl First {
    pub fn new(child: Box<Expr>, ignore_nulls: bool) -> Self {
        let first = Mutex::new(None);
        let value_set = Mutex::new(None);
        let input_agg_attrs = Mutex::new(vec![]);
        let result_attr = Mutex::new(None);
        Self { child, ignore_nulls, first, value_set, input_agg_attrs, result_attr }
    }

    fn first_attr(&self) -> AttributeReference {
        let mut first_guard = self.first.lock().unwrap();
        if first_guard.is_none() {
            *first_guard = Some(AttributeReference::new("first", self.child.data_type().clone()));
        }
        first_guard.as_ref().unwrap().clone()
    }

    fn value_set_attr(&self) -> AttributeReference {
        let mut value_set_guard = self.value_set.lock().unwrap();
        if value_set_guard.is_none() {
            *value_set_guard = Some(AttributeReference::new("value_set", DataType::Boolean));
        }
        value_set_guard.as_ref().unwrap().clone()
    }

    fn input_agg_attrs(&self) -> Vec<AttributeReference> {
        let mut input_agg_attrs_guard = self.input_agg_attrs.lock().unwrap();
        if input_agg_attrs_guard.is_empty() {
            *input_agg_attrs_guard = vec![self.first_attr().new_instance(), self.value_set_attr().new_instance()];
        }
        input_agg_attrs_guard.clone()
    }

    fn result_attr(&self) -> AttributeReference {
        let mut result_attr_attr_guard = self.result_attr.lock().unwrap();
        if result_attr_attr_guard.is_none() {
            *result_attr_attr_guard = Some(AttributeReference::new("first", self.child.data_type().clone()));
        }
        result_attr_attr_guard.as_ref().unwrap().clone()
    }

    fn first_left(&self) -> Expr {
        self.first()
    }

    fn first_right(&self) -> Expr {
        Expr::AttributeReference(self.input_agg_attrs()[0].clone())
    }

    fn first(&self) -> Expr {
        Expr::AttributeReference(self.first_attr())
    }

    fn value_set_left(&self) -> Expr {
        self.value_set()
    }

    fn value_set_right(&self) -> Expr {
        Expr::AttributeReference(self.input_agg_attrs()[1].clone())
    }

    fn value_set(&self) -> Expr {
        Expr::AttributeReference(self.value_set_attr())
    }
}

impl CreateDeclarativeAggFunction for First {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn DeclarativeAggFunction>> {
        if args.len() < 1 || args.len() > 2 {
            return Err(format!("requires 1 or 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        let ignore_nulls = iter.next().unwrap_or(Expr::boolean_lit(false));
        match ignore_nulls {
            Expr::Literal(Literal{value, data_type}) if data_type == DataType::Boolean => {
                Ok(Box::new(Self::new(Box::new(child), value.get_boolean())))
            },
            _ => Err("The second argument should be a boolean literal.".to_string())
        }
    }
}

impl Debug for First {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("First")
            .field("child", &self.child)
            .field("ignore_nulls", &self.ignore_nulls)
            .field("first", &self.first.lock().unwrap())
            .field("value_set", &self.value_set.lock().unwrap())
            .field("input_agg_attrs", &self.input_agg_attrs.lock().unwrap())
            .field("result_attr", &self.result_attr.lock().unwrap())
            .finish()
    }
}

impl Clone for First {
    fn clone(&self) -> Self {
        Self {
            child: self.child.clone(),
            ignore_nulls: self.ignore_nulls,
            first: Mutex::new(self.first.lock().unwrap().clone()),
            value_set: Mutex::new(self.value_set.lock().unwrap().clone()),
            input_agg_attrs: Mutex::new(self.input_agg_attrs.lock().unwrap().clone()),
            result_attr: Mutex::new(self.result_attr.lock().unwrap().clone()),
        }
    }
}

impl DeclarativeAggFunction for First {
    fn name(&self) -> &str {
        "first"
    }

    fn data_type(&self) -> &DataType {
        self.child.data_type()
    }

    fn agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        vec![self.first_attr(), self.value_set_attr()]
    }

    fn input_agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        self.input_agg_attrs()
    }

    fn result_attribute(&self) -> AttributeReference {
        self.result_attr()
    }

    fn initial_values(&self) -> Vec<Expr> {
        vec![Expr::lit(Value::Null, self.child.data_type().clone()), Expr::boolean_lit(false)]
    }

    fn update_expressions(&self) -> Vec<Expr> {
        if self.ignore_nulls {
            let first = If::new(
                Box::new(self.value_set().or(self.child.clone().is_null())),
                Box::new(self.first()),
                self.child.clone(),
            );
            let value_set = self.value_set().or(self.child.clone().is_not_null());
            vec![Expr::ScalarFunction(Box::new(first)), value_set]
        } else {
            let first = If::new(
                Box::new(self.value_set()),
                Box::new(self.first()),
                self.child.clone(),
            );
            let value_set = Expr::boolean_lit(true);
            vec![Expr::ScalarFunction(Box::new(first)), value_set]
        }
    }

    fn merge_expressions(&self) -> Vec<Expr> {
        let first = If::new(
            Box::new(self.value_set_left()),
            Box::new(self.first_left()),
            Box::new(self.first_right()),
        );
        let value_set = self.value_set_left().or(self.value_set_right());
        vec![Expr::ScalarFunction(Box::new(first)), value_set]
    }

    fn evaluate_expression(&self) -> Expr {
        self.first()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }
}

