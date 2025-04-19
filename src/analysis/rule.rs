use std::collections::HashMap;
use crate::Result;
use std::fmt::Debug;
use crate::analysis::lookup_function;
use crate::expr::*;
use crate::logical_plan::{Aggregate, LogicalPlan, Project, RelationPlaceholder};
use crate::tree_node::{Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion};
use crate::types::DataType;

pub trait AnalyzerRule: Debug {
    /// Rewrite `plan`
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>>;

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str;
}


#[derive(Debug)]
pub struct ResolveRelations {
    pub temp_views: HashMap<String, RelationPlaceholder>,
}

impl ResolveRelations {
    pub fn new(temp_views: HashMap<String, RelationPlaceholder>) -> Self {
        Self { temp_views }
    }
}

impl AnalyzerRule for ResolveRelations {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match &plan {
            LogicalPlan::UnresolvedRelation(ident) => {
                match self.temp_views.get(ident) {
                    Some(r) => Ok(Transformed::yes(LogicalPlan::RelationPlaceholder(r.clone()))),
                    None => Ok(Transformed::no(plan)),
                }
            },
            _ => Ok(Transformed::no(plan)),
        })
    }

    fn name(&self) -> &str {
        "ResolveRelations"
    }
}

#[derive(Debug)]
pub struct ResolveReferences;

impl ResolveReferences {
    pub fn resolve_expr(&self, expr: Expr, attr_dict: &HashMap<String, AttributeReference>) ->  Result<Transformed<Expr>> {
        expr.transform_up(|expr| {
            match &expr {
                Expr::UnresolvedAttribute(name) => {
                    match attr_dict.get(name) {
                        Some(a) => Ok(Transformed::yes(Expr::AttributeReference(AttributeReference::new_with_expr_id(
                            name.clone(), a.data_type.clone(), a.expr_id)))),
                        None =>  Ok(Transformed::no(expr)),
                    }
                },
                Expr::UnresolvedExtractValue(UnresolvedExtractValue{child, extraction}) if child.resolved() => {
                    match child.data_type() {
                        DataType::Array(_) => Ok(Transformed::yes(Expr::ScalarFunction(Box::new(GetArrayItem::new(child.clone(), extraction.clone()))))),
                        _ => {
                            Err(format!("Can't extract value from {:?}, {:?}", child, extraction))
                        }
                    }
                },
                e if e.resolved() => Ok(Transformed::no(expr)),
                e => Ok(Transformed::no(expr)),
            }
        })
    }
}

impl AnalyzerRule for ResolveReferences {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match plan {
            p if !p.children_resolved() => Ok(Transformed::no(p)),
            p => {
                //println!("");
                //println!("plan:{:?}", p);
                let attributes = p.child_attributes();
                //println!("attributes:{:?}", attributes);
                //println!("");
                let attr_dict:HashMap<String, AttributeReference> = attributes.into_iter().map(|attr| (attr.name.clone(), attr)).collect();
                let transformed = p.map_expressions(|expr| {
                    self.resolve_expr(expr, &attr_dict)
                })?;
                Ok(transformed)
            }
        })
    }

    fn name(&self) -> &str {
        "ResolveReferences"
    }
}

#[derive(Debug)]
pub struct ResolveFunctions;

impl AnalyzerRule for ResolveFunctions {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match plan {
            p if !p.children_resolved() => Ok(Transformed::no(p)),
            p => {
                let transformed = p.map_expressions(|expr| {
                    expr.transform_up(|expr| {
                        match &expr {
                            Expr::UnresolvedFunction(UnresolvedFunction{name, arguments}) => {
                                match lookup_function(name, arguments.clone()) {
                                    Ok(e) => Ok(Transformed::yes(e)),
                                    Err(e) => Err(e)
                                }
                            },
                            e if e.resolved() => Ok(Transformed::no(expr)),
                            e => Ok(Transformed::no(expr)),
                        }
                    })
                })?;
                Ok(transformed)
            }
        })
    }

    fn name(&self) -> &str {
        "ResolveFunctions"
    }
}

#[derive(Debug)]
pub struct GlobalAggregates;

impl GlobalAggregates {
    pub fn contains_aggregates(expr: &Expr) -> bool {
        let mut contains = false;
        expr.apply(|expr| {
            match expr {
                Expr::DeclarativeAggFunction(_) | Expr::TypedAggFunction(_) => {
                    contains = true;
                    Ok(TreeNodeRecursion::Stop)
                },
                _ => Ok(TreeNodeRecursion::Continue),
            }
        }).unwrap();
        contains
    }


}

impl AnalyzerRule for GlobalAggregates {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match &plan {
            LogicalPlan::Project(Project{project_list,child})
                if project_list.into_iter().any(|e| Self::contains_aggregates(e)) => {
                Ok(Transformed::yes(LogicalPlan::Aggregate(Aggregate::new(vec![], project_list.clone(), child.clone()))))
            },
            _ => Ok(Transformed::no(plan)),
        })
    }

    fn name(&self) -> &str {
        "GlobalAggregates"
    }
}
