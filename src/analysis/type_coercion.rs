use itertools::Itertools;
use crate::analysis::AnalyzerRule;
use crate::expr::{BinaryOperator, Expr};
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

fn find_tightest_common_type(type1:  DataType, type2:  DataType) -> Option< DataType> {
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
