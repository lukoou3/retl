use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Mutex;
use crate::data::Value;
use crate::expr::aggregate::{CreateDeclarativeAggFunction, DeclarativeAggFunction, ExtendDeclarativeAggFunction};
use crate::expr::{coalesce2, AttributeReference, Expr};
use crate::types::{AbstractDataType, DataType};

pub struct Sum {
    child: Box<Expr>,
    result_type: DataType,
    sum: Mutex<Option<AttributeReference>>,
    zero: Expr,
    input_agg_attrs: Mutex<Vec<AttributeReference>>,
    result_attr: Mutex<Option<AttributeReference>>,
}

impl Sum {
    pub fn new(child: Box<Expr>) -> Self {
        let (result_type, zero)  = match child.data_type() {
            DataType::Int | DataType::Long => (DataType::Long, Expr::long_lit(0)),
            _ => (DataType::Double, Expr::double_lit(0.0)),
        };
        let sum = Mutex::new(None);
        let input_agg_attrs = Mutex::new(vec![]);
        let result_attr = Mutex::new(None);
        Self { child, result_type, sum, zero, input_agg_attrs, result_attr }
    }

    fn sum_attr(&self) -> AttributeReference {
        let mut sum_guard = self.sum.lock().unwrap();
        if sum_guard.is_none() {
            *sum_guard = Some(AttributeReference::new("sum", self.result_type.clone()));
        }
        sum_guard.as_ref().unwrap().clone()
    }

    fn input_agg_attrs(&self) -> Vec<AttributeReference> {
        let mut input_agg_attrs_guard = self.input_agg_attrs.lock().unwrap();
        if input_agg_attrs_guard.is_empty() {
            *input_agg_attrs_guard = vec![self.sum_attr().new_instance()];
        }
        input_agg_attrs_guard.clone()
    }

    fn result_attr(&self) -> AttributeReference {
        let mut result_attr_attr_guard = self.result_attr.lock().unwrap();
        if result_attr_attr_guard.is_none() {
            *result_attr_attr_guard = Some(AttributeReference::new("sum", self.result_type.clone()));
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

    fn zero(&self) -> Expr {
        self.zero.clone()
    }

    fn child_cast(&self) -> Expr {
        self.child.clone().cast(self.result_type.clone())
    }
}

impl CreateDeclarativeAggFunction for Sum {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn DeclarativeAggFunction>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl Debug for Sum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sum")
            .field("child", &self.child)
            .field("result_type", &self.result_type)
            .field("sum", &self.sum.lock().unwrap())
            .field("input_agg_attrs", &self.input_agg_attrs.lock().unwrap())
            .field("result_attr", &self.result_attr.lock().unwrap())
            .finish()
    }
}

impl Clone for Sum {
    fn clone(&self) -> Self {
        Self {
            child: self.child.clone(),
            result_type: self.result_type.clone(),
            sum: Mutex::new(self.sum.lock().unwrap().clone()),
            zero: self.zero.clone(),
            input_agg_attrs: Mutex::new(self.input_agg_attrs.lock().unwrap().clone()),
            result_attr: Mutex::new(self.result_attr.lock().unwrap().clone()),
        }
    }
}

impl DeclarativeAggFunction for Sum {
    fn name(&self) -> &str {
        "sum"
    }

    fn data_type(&self) -> &DataType {
        &self.result_type
    }

    fn agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        vec![self.sum_attr()]
    }

    fn input_agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        self.input_agg_attrs()
    }

    fn result_attribute(&self) -> AttributeReference {
        self.result_attr()
    }

    fn initial_values(&self) -> Vec<Expr> {
        vec![Expr::lit(Value::Null, self.result_type.clone())]
    }

    fn update_expressions(&self) -> Vec<Expr> {
        vec![coalesce2(coalesce2(self.sum(), self.zero()) + self.child_cast(), self.sum())]
    }

    fn merge_expressions(&self) -> Vec<Expr> {
        vec![coalesce2(coalesce2(self.sum_left(), self.zero()) + self.sum_right(), self.sum_left())]
    }

    fn evaluate_expression(&self) -> Expr {
        self.sum()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Numeric])
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{empty_row, GenericRow, Row, Value};
    use crate::expr::aggregate::sum::Sum;
    use crate::expr::{AttributeReference, Expr};
    use crate::expr::aggregate::DeclarativeAggFunction;
    use crate::physical_expr::{MutableProjection, MutableProjectionForAgg};
    use crate::types::DataType;

    #[test]
    fn test_sum_one_phase() {
        let input_attr = AttributeReference::new("input", DataType::Int);
        let input = vec![input_attr.clone()];
        let func = Sum::new(Box::new(Expr::AttributeReference(input_attr.clone())));

        let mut initializer = MutableProjection::new(func.initial_values()).unwrap();
        let mut updater = MutableProjectionForAgg::new_with_input_attrs(func.update_expressions(), func.agg_buffer_attributes().into_iter().chain(input.clone().into_iter()).collect()).unwrap();
        let mut evaluator = MutableProjection::new_with_input_attrs(vec![func.evaluate_expression()], func.agg_buffer_attributes()).unwrap();
        println!("initializer:{:?}", initializer);
        println!("updater:{:?}", updater);
        println!("evaluator:{:?}", evaluator);

        updater.targert(initializer.apply(empty_row()).clone()) ;
        println!("init updater_row:{:?}", updater.result());
        assert!(updater.result().get(0).is_null());
        println!("init evaluator_row:{:?}", evaluator.result());

        let rows = vec![
            GenericRow::new(vec![Value::Int(1)]),
            GenericRow::new(vec![Value::Int(9)]),
            GenericRow::new(vec![Value::Null]),
            GenericRow::new(vec![Value::Int(-2)]),
        ];
        for row in &rows {
            updater.apply(row);
            println!("update: {} => {:?}", row, updater.result());
        }
        assert_eq!(updater.result().get(0).clone(), Value::long(8));

        evaluator.apply(updater.result());
        println!("evaluator:{:?}", evaluator.result());
        assert_eq!(evaluator.result().get(0).clone(), Value::long(8));
    }

    #[test]
    fn test_sum_two_phase() {
        let input_attr = AttributeReference::new("input", DataType::Int);
        let input = vec![input_attr.clone()];
        let func = Sum::new(Box::new(Expr::AttributeReference(input_attr.clone())));

        let mut initializer = MutableProjection::new(func.initial_values()).unwrap();
        let mut merger = MutableProjectionForAgg::new_with_input_attrs(func.merge_expressions(), func.agg_buffer_attributes().into_iter().chain(func.input_agg_attrs().into_iter()).collect()).unwrap();
        let mut evaluator = MutableProjection::new_with_input_attrs(vec![func.evaluate_expression()], func.agg_buffer_attributes()).unwrap();
        println!("initializer:{:?}", initializer);
        println!("merger:{:?}", merger);
        println!("evaluator:{:?}", evaluator);

        merger.targert(initializer.apply(empty_row()).clone()) ;
        println!("init merger_row:{:?}", merger.result());
        assert!(merger.result().get(0).is_null());
        println!("init evaluator_row:{:?}", evaluator.result());

        let rows_array = vec![
            Vec::new(),
            vec![GenericRow::new(vec![Value::Int(1)]), GenericRow::new(vec![Value::Int(9)])],
            vec![GenericRow::new(vec![Value::Null])],
            vec![GenericRow::new(vec![Value::Null]), GenericRow::new(vec![Value::Int(-2)])],
        ];

        for rows in &rows_array {
            let mut updater = MutableProjectionForAgg::new_with_input_attrs(func.update_expressions(), func.agg_buffer_attributes().into_iter().chain(input.clone().into_iter()).collect()).unwrap();
            updater.targert(initializer.apply(empty_row()).clone()) ;
            for row in rows {
                updater.apply(row);
                println!("update: {} => {:?}", row, updater.result());
            }
            merger.apply(updater.result());
            println!("merge: {} => {:?}", updater.result(), merger.result());
        }

        evaluator.apply(merger.result());
        println!("evaluator:{:?}", evaluator.result());
        assert_eq!(evaluator.result().get(0).clone(), Value::long(8));
    }
}