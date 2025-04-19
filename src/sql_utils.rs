use std::collections::HashMap;
use std::sync::Arc;
use crate::{parser, Result};
use crate::analysis::Analyzer;
use crate::expr::{Alias, Expr};
use crate::logical_plan::{Expression, Filter, LogicalPlan, RelationPlaceholder};
use crate::optimizer::Optimizer;
use crate::types::Schema;

pub fn sql_plan(sql: &str, schema: &Schema) -> Result<LogicalPlan> {
    let mut temp_views = HashMap::new();
    temp_views.insert("tbl".to_string(), RelationPlaceholder::new("tbl".to_string(), schema.to_attributes()));
    let plan = parser::parse_query(sql)?;
    let plan = Analyzer::new(temp_views).analyze(plan)?;
    //println!("plan:\n{:?}", plan);
    let optimized_plan = Optimizer::new().optimize(plan)?;
    //println!("optimized_plan:\n{:?}", optimized_plan);
    Ok(optimized_plan)
}

pub fn parse_filter(condition: &str, schema: &Schema) -> Result<Filter> {
    let expr = parser::parse_expr(condition)?;
    let plan = LogicalPlan::Filter(Filter::new(expr, Arc::new(LogicalPlan::RelationPlaceholder(RelationPlaceholder::new("tbl".to_string(), schema.to_attributes())))));
    let plan = Analyzer::new(HashMap::new()).analyze(plan)?;
    let optimized_plan = Optimizer::new().optimize(plan)?;
    if let LogicalPlan::Filter(filter) = optimized_plan {
        Ok(filter)
    } else {
        Err("Invalid filter plan".into())
    }
}

pub fn parse_expr(sql: &str, schema: &Schema) -> Result<Expression> {
    let e = parser::parse_expr(sql)?;
    let expr = match e {
        Expr::Alias(_) | Expr::AttributeReference(_) | Expr::UnresolvedAttribute(_) => e,
        e => Expr::Alias(Alias::new(e, "v")),
    };
    let plan = LogicalPlan::Expression(Expression::new(expr, Arc::new(LogicalPlan::RelationPlaceholder(RelationPlaceholder::new("tbl".to_string(), schema.to_attributes())))));
    let plan = Analyzer::new(HashMap::new()).analyze(plan)?;
    let optimized_plan = Optimizer::new().optimize(plan)?;
    if let LogicalPlan::Expression(expr) = optimized_plan {
        Ok(expr)
    } else {
        Err("Invalid expr plan".into())
    }
}