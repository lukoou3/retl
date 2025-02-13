use std::sync::Arc;
use crate::expr::{AttributeReference, Expr};

#[derive(Clone,Debug)]
pub enum LogicalPlan {
    UnresolvedRelation(String),
    RelationPlaceholder(RelationPlaceholder),
    Project(Project),
    Filter(Filter),
}

#[derive(Clone, Debug)]
pub struct RelationPlaceholder {
    pub name: String,
    pub output: Vec<AttributeReference>,
}

#[derive(Clone, Debug)]
pub struct Project {
    pub project_list: Vec<Expr>,
    pub child: Arc<LogicalPlan>,
}

#[derive(Clone, Debug)]
pub struct Filter {
    pub condition: Expr,
    pub child: Arc<LogicalPlan>,
}