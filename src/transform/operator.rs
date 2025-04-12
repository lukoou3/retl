use std::fmt::Debug;
use std::sync::Arc;
use crate::Result;
use crate::data::{GenericRow, Row};
use crate::execution::Collector;
use crate::expr::BoundReference;
use crate::logical_plan::{Filter, LogicalPlan, Project};
use crate::physical_expr::{create_physical_expr, PhysicalExpr};

pub trait ProcessOperator: Debug {
    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector) -> Result<u64>;
}

#[derive(Debug)]
struct OutOperator;

impl ProcessOperator for OutOperator {
    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector) -> Result<u64> {
        out.collect(row)?;
        Ok(1)
    }
}

#[derive(Debug)]
pub struct FilterOperator {
    predicate: Arc<dyn PhysicalExpr>,
    next: Box<dyn ProcessOperator>,
}

impl FilterOperator {
    pub fn new(predicate: Arc<dyn PhysicalExpr>, next: Box<dyn ProcessOperator>) -> Self {
        Self {predicate, next}
    }
}

impl ProcessOperator for FilterOperator {
    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector) -> Result<u64> {
        let value = self.predicate.eval(row);
        if !value.is_null() && value.get_boolean() {
            self.next.process(row, out)
        } else {
            Ok(0)
        }
    }
}

#[derive(Debug)]
pub struct ProjectOperator {
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    row: GenericRow,
    next: Box<dyn ProcessOperator>,
}

impl ProjectOperator {
    pub fn new(exprs: Vec<Arc<dyn PhysicalExpr>>, next: Box<dyn ProcessOperator>) -> Self {
        let row = GenericRow::new_with_size(exprs.len());
        Self {exprs, row, next}
    }
}

impl ProcessOperator for ProjectOperator {
    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector) -> Result<u64> {
        // self.row.fill_null();
        for (i, expr) in self.exprs.iter().enumerate() {
            self.row.update(i, expr.eval(row));
        }
        self.next.process(&self.row, out)
    }
}

pub fn get_process_operator_chain(plan: LogicalPlan) -> Result<Box<dyn ProcessOperator>> {
    get_process_operator_chain_inner(plan, Box::new(OutOperator))
}

fn get_process_operator_chain_inner(plan: LogicalPlan, out_operator: Box<dyn ProcessOperator>) -> Result<Box<dyn ProcessOperator>> {
    let mut operator = out_operator;
    let mut child_plan = plan;
    loop {
        match child_plan {
            LogicalPlan::Filter(Filter{condition, child}) => {
                let predicate = BoundReference::bind_reference(condition.clone(), child.output())?;
                let predicate = create_physical_expr(&predicate)?;
                operator = Box::new(FilterOperator::new(predicate, operator));
                child_plan = child.as_ref().clone();
            },
            LogicalPlan::Project(Project{project_list, child}) => {
                let input = child.output();
                let exprs = BoundReference::bind_references(project_list, input)?;
                let exprs: Result<Vec<Arc<dyn PhysicalExpr>>, String> = exprs.iter().map(|expr| create_physical_expr(expr)).collect();
                operator = Box::new(ProjectOperator::new(exprs?, operator));
                child_plan = child.as_ref().clone();
            },
            LogicalPlan::RelationPlaceholder(_) => {
                return Ok(operator);
            },
            _ => return Err(format!("not support plan: {:?}", child_plan)),
        }
    }
}
