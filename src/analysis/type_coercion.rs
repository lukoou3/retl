use crate::analysis::AnalyzerRule;
use crate::data::Value;
use crate::expr::{BinaryOperator, In, Expr, If};
use crate::logical_plan::LogicalPlan;
use crate::Operator;
use crate::tree_node::{Transformed, TreeNode};
use crate::types::DataType;

#[derive(Debug)]
pub struct ImplicitTypeCasts;

impl AnalyzerRule for ImplicitTypeCasts {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan|  plan.map_expressions(|expr| {
            expr.transform_up(|expr| match expr {
                e if !e.children_resolved() => Ok(Transformed::no(e)),
                Expr::BinaryOperator(BinaryOperator{left, op, right}) if left.data_type() != right.data_type() => {
                    match find_tightest_common_type(left.data_type().clone(), right.data_type().clone()) {
                        Some(common_type) => match op {
                            Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo  => {
                                if common_type.is_numeric_type() {
                                    let new_left = if left.data_type() == &common_type {
                                        left
                                    } else {
                                        Box::new(left.cast(common_type.clone()))
                                    };
                                    let new_right = if right.data_type() == &common_type {
                                        right
                                    } else {
                                        Box::new(right.cast(common_type.clone()))
                                    };
                                    Ok(Transformed::yes(Expr::BinaryOperator(BinaryOperator{left: new_left, op: op.clone(), right: new_right})))
                                } else {
                                    Ok(Transformed::no(Expr::BinaryOperator(BinaryOperator{left, op, right})))
                                }
                            },
                            Operator::Eq | Operator::NotEq | Operator::Lt |Operator::LtEq | Operator::Gt | Operator::GtEq => {
                                if common_type.is_numeric_type() || common_type == DataType::String {
                                    let new_left = if left.data_type() == &common_type {
                                        left
                                    } else {
                                        Box::new(left.cast(common_type.clone()))
                                    };
                                    let new_right = if right.data_type() == &common_type {
                                        right
                                    } else {
                                        Box::new(right.cast(common_type.clone()))
                                    };
                                    Ok(Transformed::yes(Expr::BinaryOperator(BinaryOperator{left: new_left, op: op.clone(), right: new_right})))
                                } else {
                                    Ok(Transformed::no(Expr::BinaryOperator(BinaryOperator{left, op, right})))
                                }
                            },
                            Operator::And | Operator::Or => {
                                if common_type == DataType::Boolean {
                                    let new_left = if left.data_type() == &common_type {
                                        left
                                    } else {
                                        Box::new(left.cast(common_type.clone()))
                                    };
                                    let new_right = if right.data_type() == &common_type {
                                        right
                                    } else {
                                        Box::new(right.cast(common_type.clone()))
                                    };
                                    Ok(Transformed::yes(Expr::BinaryOperator(BinaryOperator{left: new_left, op: op.clone(), right: new_right})))
                                } else {
                                    Ok(Transformed::no(Expr::BinaryOperator(BinaryOperator{left, op, right})))
                                }
                            }
                        }
                        None => Ok(Transformed::no(Expr::BinaryOperator(BinaryOperator{left, op, right})))
                    }
                },
                e => Ok(Transformed::no(e))
            })
        }))
    }

    fn name(&self) -> &str {
        "ImplicitTypeCasts"
    }
}

#[derive(Debug)]
pub struct InConversion;

impl AnalyzerRule for InConversion {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan|  plan.map_expressions(|expr| {
            expr.transform_up(|expr| match expr.clone() {
                e if !e.children_resolved() => Ok(Transformed::no(e)),
                Expr::In(In{value, list}) if list.iter().any(|e| e.data_type() != value.data_type()) => {
                    match find_wider_common_type(vec![value.data_type().clone()].into_iter().chain(list.iter().map(|e| e.data_type().clone())).collect()) {
                        Some(common_type) => {
                            Ok(Transformed::yes(Expr::In(In{
                                value: Box::new(cast_if_not_same_type(*value, &common_type)),
                                list: list.into_iter().map(|e|cast_if_not_same_type(e, &common_type)).collect()
                            })))
                        },
                        None => Ok(Transformed::no(expr))
                    }
                },
                _ => Ok(Transformed::no(expr))
            })
        }))
    }

    fn name(&self) -> &str {
        "InConversion"
    }
}

#[derive(Debug)]
pub struct IfCoercion;

impl AnalyzerRule for IfCoercion {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan|  plan.map_expressions(|expr| {
            expr.transform_up(|expr| match expr.clone() {
                e if !e.children_resolved() => Ok(Transformed::no(e)),
                Expr::ScalarFunction(func) => {
                    let any = func.as_any();
                    if let Some(If{predicate, true_value, false_value}) = any.downcast_ref::<If>() {
                        let predicate = predicate.clone();
                        let true_value = true_value.clone();
                        let false_value = false_value.clone();
                        if true_value.data_type() != false_value.data_type() {
                            if let Some(common_type) = find_wider_type_for_two(true_value.data_type().clone(), false_value.data_type().clone()) {
                                return  Ok(Transformed::yes(Expr::ScalarFunction(Box::new(
                                    If::new(
                                        predicate,
                                        Box::new(cast_if_not_same_type(*true_value, &common_type)),
                                        Box::new(cast_if_not_same_type(*false_value, &common_type))
                                    )
                                ))));
                            }
                        } else if predicate.data_type() == DataType::null_type() {
                            return Ok(Transformed::yes(Expr::ScalarFunction(Box::new(
                                If::new(
                                    Box::new(Expr::lit(Value::Null, DataType::Boolean)),
                                    true_value,
                                    false_value
                                )
                            ))));
                        }
                    }
                    Ok(Transformed::no(expr))
                }
                _ => Ok(Transformed::no(expr))
            })
        }))
    }

    fn name(&self) -> &str {
        "IfCoercion"
    }
}

fn cast_if_not_same_type(expr:  Expr, dt:  &DataType) -> Expr {
    if expr.data_type() == dt {
        expr
    } else {
        expr.cast(dt.clone())
    }
}

fn find_tightest_common_type(type1:  DataType, type2:  DataType) -> Option<DataType> {
    match (type1, type2) {
        (t1, t2) if t1 == t2 => Some(t1),
        (t1, DataType::Null) => Some(t1),
        (DataType::Null, t2) => Some(t2),
        (t1, t2) if t1.is_numeric_type() && t2.is_numeric_type() =>
            Some(NUMERIC_PRECEDENCE.iter().rfind(|t| *t == &t1 || *t == &t2).unwrap().clone()),
        _ => None
    }

}

static NUMERIC_PRECEDENCE: [DataType; 4] = [DataType::Int, DataType::Long, DataType::Float, DataType::Double];

fn find_wider_common_type(types: Vec<DataType>) -> Option<DataType> {
    let (string_types, non_string_types): (Vec<_>, Vec<_>) = types.into_iter().partition(|t| has_string_type(t));
    string_types.into_iter().chain(non_string_types.into_iter()).fold(Some(DataType::Null), |r, c| {
        match r {
            Some(d) => find_wider_type_for_two(d, c),
            _ => None
        }
    })
}

fn find_wider_type_for_two(type1:  DataType, type2:  DataType) -> Option<DataType> {
    find_tightest_common_type(type1.clone(), type2.clone()).or_else(|| string_promotion(&type1, &type2))
}

fn string_promotion(data_type1: &DataType, data_type2: &DataType) -> Option<DataType> {
    match (data_type1, data_type2) {
        (DataType::String, t2) if t2.is_atomic_type() && (t2 != &DataType::Binary && t2 != &DataType::Boolean)  => Some(DataType::String),
        (t1, DataType::String) if t1.is_atomic_type() && (t1 != &DataType::Binary && t1 != &DataType::Boolean) => Some(DataType::String),
        _ => None
    }
}

fn has_string_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::String => true,
        DataType::Array(data_type) => has_string_type(data_type),
        _ => false
    }
}

