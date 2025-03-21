use std::fmt::Debug;
use crate::data::empty_row;
use crate::expr::{Cast, Expr};
use crate::logical_plan::LogicalPlan;
use crate::physical_expr::create_physical_expr;
use crate::tree_node::{Transformed, TreeNode};

pub trait OptimizerRule: Debug {
    /// Rewrite `plan`
    fn optimize(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;
}

#[derive(Debug)]
pub struct ConstantFolding;

impl OptimizerRule for ConstantFolding {
    fn optimize(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan|  plan.map_expressions(|expr| {
            expr.transform_up(|expr| match expr {
                e @ Expr::Literal(_) => Ok(Transformed::no(e)),
                e if e.foldable() => {
                    create_physical_expr(&e).map(|phy_expr| {
                        let value = phy_expr.eval(empty_row());
                        let data_type = phy_expr.data_type();
                        let new_expr = Expr::lit(value, data_type);
                        println!("fold {:?} -> {:?}", e, new_expr);
                        Transformed::yes(new_expr)
                    })
                },
                _ => Ok(Transformed::no(expr))
            })
        }))
    }

    fn name(&self) -> &str {
        "ConstantFolding"
    }
}

#[derive(Debug)]
pub struct SimplifyCasts;

impl OptimizerRule for SimplifyCasts {
    fn optimize(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan|  plan.map_expressions(|expr| {
            expr.transform_up(|expr| match expr {
                Expr::Cast(Cast{child, data_type}) if child.data_type() == &data_type =>
                    Ok(Transformed::yes(*child)),
                _ => Ok(Transformed::no(expr))
            })
        }))
    }

    fn name(&self) -> &str {
        "SimplifyCasts"
    }
}

