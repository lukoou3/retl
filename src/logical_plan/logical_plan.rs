use std::collections::HashMap;
use std::sync::Arc;
use std::vec;
use crate::Result;
use crate::expr::{Alias, AttributeReference, Expr};
use crate::tree_node::{Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion};
use crate::types::DataType;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub enum LogicalPlan {
    UnresolvedRelation(String),
    RelationPlaceholder(RelationPlaceholder),
    Project(Project),
    Filter(Filter),
    Expression(Expression),
    Aggregate(Aggregate),
}

impl LogicalPlan {
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::UnresolvedRelation(_)
             | LogicalPlan::RelationPlaceholder(_) => vec![],
            LogicalPlan::Project(Project{child, ..})
             | LogicalPlan::Filter(Filter{child, ..})
             | LogicalPlan::Expression(Expression{child, ..})
             | LogicalPlan::Aggregate(Aggregate{child, ..})=> vec![child.as_ref()],
        }
    }

    pub fn expressions(&self) -> Vec<&Expr> {
        match self {
            LogicalPlan::UnresolvedRelation(_)
             | LogicalPlan::RelationPlaceholder(_) => vec![],
            LogicalPlan::Project(Project{project_list, ..}) => project_list.iter().collect(),
            LogicalPlan::Filter(Filter{condition, ..}) => vec![condition],
            LogicalPlan::Expression(Expression{expr, ..}) => vec![expr],
            LogicalPlan::Aggregate(Aggregate{grouping_exprs, aggregate_exprs, ..}) => {
                grouping_exprs.iter().chain(aggregate_exprs.iter()).collect()
            },
        }
    }

    pub fn resolved(&self) -> bool {
        match self {
            LogicalPlan::UnresolvedRelation(_) => false,
            _ => self.expressions().iter().all(|e| e.resolved()) && self.children_resolved(),
        }
    }

    pub fn children_resolved(&self) -> bool {
        self.children().iter().all(|c| c.resolved())
    }

    pub fn output(&self) -> Vec<AttributeReference> {
        match self {
            LogicalPlan::UnresolvedRelation(_) => vec![],
            LogicalPlan::RelationPlaceholder(RelationPlaceholder{output, ..}) => output.clone(),
            LogicalPlan::Project(Project{project_list, ..}) => {
                project_list.iter().map(|e| {
                    match e {
                        Expr::Alias(Alias {child, name, expr_id}) =>
                            AttributeReference::new_with_expr_id(name, child.data_type().clone(), *expr_id),
                        Expr::AttributeReference(a) => a.clone(),
                        Expr::UnresolvedAttribute(a) => AttributeReference::new_with_expr_id(a.clone(), DataType::Int, 0),
                        _ => panic!("{}", format!("{:?} is not allowed in project list", e)),
                    }
                }).collect()
            },
            LogicalPlan::Filter(Filter{child, ..}) => child.output(),
            LogicalPlan::Expression(Expression{expr, ..}) => match expr {
                Expr::Alias(Alias {child, name, expr_id}) =>
                    vec![AttributeReference::new_with_expr_id(name, child.data_type().clone(), *expr_id)],
                Expr::AttributeReference(a) => vec![a.clone()],
                Expr::UnresolvedAttribute(a) => vec![AttributeReference::new_with_expr_id(a.clone(), DataType::Int, 0)],
                e => panic!("{:?} is not allowed in expr", e),
            },
            LogicalPlan::Aggregate(Aggregate{aggregate_exprs, ..}) => {
                aggregate_exprs.iter().map(|e| {
                    match e {
                        Expr::Alias(Alias {child, name, expr_id}) =>
                            AttributeReference::new_with_expr_id(name, child.data_type().clone(), *expr_id),
                        Expr::AttributeReference(a) => a.clone(),
                        Expr::UnresolvedAttribute(a) => AttributeReference::new_with_expr_id(a.clone(), DataType::Int, 0),
                        _ => panic!("{}", format!("{:?} is not allowed in aggregate exprs list", e)),
                    }
                }).collect()
            },
        }
    }

    pub fn child_attributes(&self) -> Vec<AttributeReference> {
        self.children().into_iter().flat_map(|p| p.output().into_iter()).collect()
    }
}

impl<'a> TreeNodeContainer<'a, Self> for LogicalPlan {
    fn apply_elements<F: FnMut(&'a Self) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        f(self)
    }

    fn map_elements<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        f(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct RelationPlaceholder {
    pub name: String,
    pub output: Vec<AttributeReference>,
}

impl RelationPlaceholder {
    pub fn new(name: String, output: Vec<AttributeReference>) -> Self {
        Self { name, output }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct Project {
    pub project_list: Vec<Expr>,
    pub child: Arc<LogicalPlan>,
}

impl Project {
    pub fn new(project_list: Vec<Expr>, child: Arc<LogicalPlan>) -> Self {
        for expr in &project_list {
            match expr {
                Expr::Alias(_) | Expr::AttributeReference(_) | Expr::UnresolvedAttribute(_) => (),
                e => panic!("{}", format!("{:?} is not allowed in project list", expr)),
            }
        }
        Self { project_list, child }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct Filter {
    pub condition: Expr,
    pub child: Arc<LogicalPlan>,
}

impl Filter {
    pub fn new(condition: Expr, child: Arc<LogicalPlan>) -> Self {
        Self { condition, child }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct Expression {
    pub expr: Expr,
    pub child: Arc<LogicalPlan>,
}

impl Expression {
    pub fn new(expr: Expr, child: Arc<LogicalPlan>) -> Self {
        match expr {
            Expr::Alias(_) | Expr::AttributeReference(_) | Expr::UnresolvedAttribute(_) => (),
            e => panic!("{:?} is not allowed in expr", e),
        }
        Self { expr, child }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct Aggregate {
    pub grouping_exprs: Vec<Expr>,
    pub aggregate_exprs: Vec<Expr>,
    pub child: Arc<LogicalPlan>,
}

impl Aggregate {
    pub fn new(grouping_exprs: Vec<Expr>, aggregate_exprs: Vec<Expr>, child: Arc<LogicalPlan>) -> Self {
        for expr in &aggregate_exprs {
            match expr {
                Expr::Alias(_) | Expr::AttributeReference(_) | Expr::UnresolvedAttribute(_) => (),
                e => panic!("{}", format!("{:?} is not allowed in aggregate exprs", e)),
            }
        }
        Self { grouping_exprs, aggregate_exprs, child }
    }

    // groupingExpressions, aggregateExpressions, resultExpressions, child
    pub fn extract_exprs(&self) -> (Vec<Expr>, Vec<Expr>, Vec<Expr>, Arc<LogicalPlan>) {
        let mut equivalent_exprs = HashMap::new();
        let mut agg_exprs = Vec::with_capacity(self.aggregate_exprs.len());
        for expr in &self.aggregate_exprs {
            expr.apply(|e| {
                if let Expr::DeclarativeAggFunction(f) = e {
                    f.agg_buffer_attributes();
                    f.input_agg_buffer_attributes();
                    f.result_attribute();
                    if !equivalent_exprs.contains_key(e) {
                        equivalent_exprs.insert(e.clone(), f.result_attribute());
                        agg_exprs.push(e.clone());
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            }).unwrap();
        }

        let mut named_group_exprs = Vec::with_capacity(self.grouping_exprs.len());
        for expr in &self.grouping_exprs {
            match expr {
                Expr::AttributeReference(_) | Expr::Alias(_) =>
                    named_group_exprs.push((expr.clone(), expr.clone())),
                _ => {
                    let with_alias = Expr::Alias(Alias::new(expr.clone(), "expr"));
                    named_group_exprs.push((expr.clone(), with_alias));
                }
            }
        }
        let group_expr_map = named_group_exprs.clone().into_iter().collect::<HashMap<_, _>>();
        let mut rewritten_result_exprs = Vec::with_capacity(self.aggregate_exprs.len());
        for expr in self.aggregate_exprs.clone() {
            let ep = expr.transform_down(|e| match e {
                e @ Expr::DeclarativeAggFunction(_) => {
                    let attr = Expr::AttributeReference(equivalent_exprs.get(&e).unwrap().clone());
                    Ok(Transformed::yes(attr))
                },
                e if ! e.foldable() => {
                    for (k, v) in group_expr_map.iter() {
                        if k.eq(&e) {
                            return Ok(Transformed::yes(Expr::AttributeReference(v.to_attribute().unwrap())));
                        }
                    }
                    Ok(Transformed::no(e))
                },
                e => {
                    Ok(Transformed::no(e))
                },
            }).unwrap().data;
            rewritten_result_exprs.push(ep);
        }

        (
            named_group_exprs.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
            agg_exprs,
            rewritten_result_exprs,
            self.child.clone(),
        )
    }
}

mod tests {
    use crate::sql_utils;
    use crate::types::{Field, Schema};
    use super::*;

    #[test]
    fn test_project() {
        let sql = "select cate, sum(in_bytes) in_bytes, sum(out_bytes) out_bytes from tbl group by cate";
        //let sql = "select substr(cate, 1, 10) cate, sum(in_bytes) in_bytes, sum(out_bytes) out_bytes, sum(in_bytes) + sum(out_bytes) bytes from tbl group by substr(cate, 1, 10)";
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int),
            Field::new("cate", DataType::String),
            Field::new("text", DataType::String),
            Field::new("in_bytes", DataType::Long),
            Field::new("out_bytes", DataType::Long),
        ]);
        let optimized_plan = sql_utils::sql_plan(sql, &schema).unwrap();
        println!("plan:{:#?}", optimized_plan);
        if let LogicalPlan::Aggregate(agg) = optimized_plan {
            let (group_exprs, agg_exprs, result_exprs, child) = agg.extract_exprs();
            println!("group_exprs:{:#?}", group_exprs);
            println!("agg_exprs:{:#?}", agg_exprs);
            println!("result_exprs:{:#?}", result_exprs);
            println!("child:{:#?}", child);
        }
    }
}