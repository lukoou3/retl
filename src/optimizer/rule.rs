use std::collections::HashSet;
use std::fmt::Debug;
use log::{debug, info};
use crate::Result;
use crate::data::{empty_row, Value};
use crate::expr::{Cast, Expr, If, In, InSet, Literal};
use crate::logical_plan::LogicalPlan;
use crate::physical_expr::create_physical_expr;
use crate::tree_node::{Transformed, TreeNode};
use crate::types::DataType;

pub trait OptimizerRule: Debug {
    /// Rewrite `plan`
    fn optimize(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;
}

#[derive(Debug)]
pub struct ConstantFolding;

impl OptimizerRule for ConstantFolding {
    fn optimize(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr {
            e @ Expr::Literal(_) => Ok(Transformed::no(e)),
            e if e.foldable() => {
                create_physical_expr(&e).map(|phy_expr| {
                    let value = phy_expr.eval(empty_row());
                    let data_type = phy_expr.data_type();
                    let new_expr = Expr::lit(value, data_type);
                    debug!("fold {:?} -> {:?}", e, new_expr);
                    Transformed::yes(new_expr)
                })
            },
            _ => Ok(Transformed::no(expr))
        })
    }

    fn name(&self) -> &str {
        "ConstantFolding"
    }
}

#[derive(Debug)]
pub struct SimplifyCasts;

impl OptimizerRule for SimplifyCasts {
    fn optimize(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr {
            Expr::Cast(Cast{child, data_type}) if child.data_type() == &data_type =>
                Ok(Transformed::yes(*child)),
            _ => Ok(Transformed::no(expr))
        })
    }

    fn name(&self) -> &str {
        "SimplifyCasts"
    }
}

#[derive(Debug)]
pub struct OptimizeIn;

impl OptimizerRule for OptimizeIn {
    fn optimize(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr {
            Expr::In(In{value, list}) if list.is_empty() => {
                let e = Expr::ScalarFunction(Box::new(If::new(
                    Box::new(value.is_not_null()),
                    Box::new(Expr::boolean_lit(false)),
                    Box::new(Expr::Literal(Literal::new(Value::Null, DataType::Boolean))),
                )));
                Ok(Transformed::yes(e))
            },
            Expr::In(In{value, list}) if list.iter().all(|e| e.is_literal()) => {
                let data_type = value.data_type().clone();
                let new_list = list.iter().map(|e| e.clone().literal_value()).collect::<HashSet<_>>()
                    .into_iter().map(|v| Expr::lit(v, data_type.clone())).collect::<Vec<_>>();
                if new_list.len() == 1 {
                    let e = (*value).eq(new_list.into_iter().next().unwrap());
                    return Ok(Transformed::yes(e));
                } else if new_list.len() > 10 {
                    let e = Expr::ScalarFunction(Box::new(InSet::new(value, new_list.into_iter().collect())));
                    return Ok(Transformed::yes(e));
                } else if new_list.len() < list.len() {
                    return Ok(Transformed::yes(Expr::In(In::new(value, new_list))))
                } else {
                    return Ok(Transformed::no(Expr::In(In::new(value, list))))
                }
            },
            _ => Ok(Transformed::no(expr))
        })
    }

    fn name(&self) -> &str {
        "OptimizeIn"
    }
}

