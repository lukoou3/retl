use crate::Result;
use crate::logical_plan::LogicalPlan;
use crate::optimizer::rule::*;

mod rule;

pub struct Optimizer {
    rules: Vec<Box<dyn OptimizerRule>>,
}

impl Optimizer {
    pub fn new() -> Self {
        let rules: Vec<Box<dyn OptimizerRule>> = vec![
            Box::new(ConstantFolding),
            Box::new(SimplifyCasts),
        ];
        Self { rules }
    }

    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut new_plan = plan;
        let mut changed = false;
        for i in 1..=10 {
            changed = false;
            for rule in &self.rules {
                let t = rule.optimize(new_plan)?;
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
        Ok(new_plan)
    }
}