use std::collections::HashMap;
use crate::Result;
use std::fmt::Debug;
use crate::expr::{AttributeReference, Expr};
use crate::logical_plan::{LogicalPlan, RelationPlaceholder};
use crate::tree_node::{Transformed, TreeNode};

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

