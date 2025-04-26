use std::sync::Arc;
use crate::batch::{DataFrame, MapDataFrame, MemoryDataFrame, ProjectMapFunction};
use crate::{sql_utils, Result};
use crate::data::GenericRow;
use crate::expr::BoundReference;
use crate::logical_plan::{LogicalPlan, Project};
use crate::physical_expr::{create_physical_expr, PhysicalExpr};
use crate::types::Schema;

pub struct BatchSession;

impl BatchSession {
    pub fn new() -> Self {
        BatchSession
    }

    pub fn sql(&self, sql: &str) -> Result<Box<dyn DataFrame>> {
        let plan = sql_utils::sql_plan(sql, &Schema::new(vec![]))?;
        self.plan_to_df(plan)
    }

    fn plan_to_df(&self, plan: LogicalPlan) -> Result<Box<dyn DataFrame>> {
        let schema = Schema::from_attributes(plan.output());
        match plan {
            LogicalPlan::Project(Project{project_list, child}) => {
                let prev= self.plan_to_df(child.as_ref().clone())?;
                let input = child.output();
                let exprs = BoundReference::bind_references(project_list, input)?;
                let exprs: Result<Vec<Arc<dyn PhysicalExpr>>, String> = exprs.iter().map(|expr| create_physical_expr(expr)).collect();
                Ok(Box::new(MapDataFrame::new(schema, prev, Box::new(ProjectMapFunction::new(exprs?)))))
            },
            LogicalPlan::OneRowRelation => {
                Ok(Box::new(MemoryDataFrame::new(schema, vec![GenericRow::new(vec![])])))
            },
            p => Err(format!("not support plan: {:?}", p))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_batch_sql() -> Result<()> {
        let session = BatchSession::new();
        let mut df = session.sql("select 1 as a, 'b' as b, upper('abc_d') c")?;
        for row in df.compute() {
            println!("row:{:?}", row);
        }
        let rows = df.collect();
        println!("rows:{:?}", rows);
        df.show();
        Ok(())
    }
}