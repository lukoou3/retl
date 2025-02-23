use std::sync::Arc;
use crate::Result;
use crate::expr::{Alias, AttributeReference, Expr};
use crate::tree_node::{Transformed, TreeNodeContainer, TreeNodeRecursion};
use crate::types::DataType;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub enum LogicalPlan {
    UnresolvedRelation(String),
    RelationPlaceholder(RelationPlaceholder),
    Project(Project),
    Filter(Filter),
}

impl LogicalPlan {
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::UnresolvedRelation(_)
             | LogicalPlan::RelationPlaceholder(_) => vec![],
            LogicalPlan::Project(Project{child, ..})
             | LogicalPlan::Filter(Filter{child, ..}) => vec![child.as_ref()],
        }
    }

    pub fn expressions(&self) -> Vec<&Expr> {
        match self {
            LogicalPlan::UnresolvedRelation(_)
             | LogicalPlan::RelationPlaceholder(_) => vec![],
            LogicalPlan::Project(Project{project_list, ..}) => project_list.iter().collect(),
            LogicalPlan::Filter(Filter{condition, ..}) => vec![condition],
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