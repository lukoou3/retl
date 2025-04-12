use std::collections::HashMap;
use crate::Result;
use crate::analysis::{type_coercion_rules, AnalyzerRule, GlobalAggregates, ResolveFunctions, ResolveReferences, ResolveRelations};
use crate::expr::Expr;
use crate::logical_plan::{Aggregate, LogicalPlan, RelationPlaceholder};
use crate::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use crate::types::DataType;

pub struct Analyzer {
    rules: Vec<Box<dyn AnalyzerRule>>,
}

impl Analyzer {
    pub fn new(temp_views: HashMap<String, RelationPlaceholder>) -> Self {
        let mut rules: Vec<Box<dyn AnalyzerRule>> = vec![
            Box::new(ResolveRelations::new(temp_views)),
            Box::new(ResolveReferences),
            Box::new(ResolveFunctions),
            Box::new(GlobalAggregates),
        ];
        for r in type_coercion_rules() {
            rules.push(r);
        }
        Self { rules }
    }

    pub fn analyze(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut new_plan = plan;
        let mut changed = false;
        for i in 1..=10 {
            changed = false;
            for rule in &self.rules {
                let t = rule.analyze(new_plan)?;
                new_plan = t.data;
                // println!("{} apply {} change:{} after: {:?}", i, rule.name(), t.transformed, new_plan);
                if t.transformed {
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        Self::check_analysis(new_plan)
    }

    fn check_analysis(plan: LogicalPlan) -> Result<LogicalPlan> {
        let plan = plan.transform_up(|plan| {
            match plan {
                LogicalPlan::UnresolvedRelation(t) => {
                    Err(format!("Table or view not found:{}", t))
                },
                p => {
                    p.map_expressions(|expr| {
                        expr.transform_up(|e| {
                            match e.check_input_data_types() {
                                Ok(_) => Ok(Transformed::no(e)),
                                Err(s) => Err(format!("cannot resolve {:?} due to data type mismatch: {}", e, s))
                            }
                        })
                    })
                },
            }
        })?.data;
        let plan = plan.transform_up(|plan| {
            match plan {
                LogicalPlan::Filter(f) if !f.condition.data_type().is_boolean_type() => {
                    Err(format!("filter expression '{:?}' of type {} is not a boolean.", f.condition, f.condition.data_type()))
                },
                LogicalPlan::Aggregate(agg) => {
                    Self::check_aggregate(&agg)?;
                    Ok(Transformed::no(LogicalPlan::Aggregate(agg)))
                },
                p => Ok(Transformed::no(p)),
            }
        })?.data;
        Ok(plan.transform_up(|p| {
            if !p.resolved() {
                return Err(format!("unresolved operator {:?}", p))
            } else {
                Ok(Transformed::no(p))
            }
        })?.data)
    }

    fn check_aggregate(aggregate: &Aggregate) -> Result<()> {
        for expr in &aggregate.grouping_exprs {
            Self::check_valid_group_expr(expr)?;
        }
        for expr in &aggregate.aggregate_exprs {
            Self::check_valid_agg_expr(expr, &aggregate.grouping_exprs)?;
        }
        Ok(())
    }

    fn check_valid_group_expr(expr: &Expr) -> Result<()> {
        expr.apply(|e| {
            if matches!(e, Expr::DeclarativeAggFunction(_)){
                return Err(format!("aggregate functions are not allowed in GROUP BY, but found {:?}", e));
            }
            if !e.data_type().is_numeric_type() && e.data_type() != DataType::string_type()  {
                return Err(format!("grouping expressions must be orderable, but found {:?}", e));
            }
            Ok(TreeNodeRecursion::Continue)
        }).map(|_| ())
    }

    fn check_valid_agg_expr(expr: &Expr, grouping_exprs: &Vec<Expr>, ) -> Result<()> {
        match expr {
            Expr::DeclarativeAggFunction(f) => {
                for x in f.args() {
                    if matches!(x, Expr::DeclarativeAggFunction(_)) {
                        return Err("It is not allowed to use an aggregate function in the argument of another aggregate function.".to_string());
                    }
                }
            },
            e @ Expr::AttributeReference(_) if !grouping_exprs.contains(e) => {
                return Err(format!("expression '{:?}' is neither present in the group by, nor is it an aggregate function.", e));
            },
            e if grouping_exprs.contains(e) => (),
            e => {
                for child in e.children() {
                    Self::check_valid_agg_expr(child, grouping_exprs)?;
                }
            }
        }

        Ok(())
    }
}