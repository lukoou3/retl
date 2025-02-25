use std::collections::HashMap;
use crate::Result;
use crate::analysis::{AnalyzerRule, ImplicitTypeCasts, ResolveFunctions, ResolveReferences, ResolveRelations};
use crate::logical_plan::{LogicalPlan, RelationPlaceholder};
use crate::tree_node::{Transformed, TreeNode};

pub struct Analyzer {
    rules: Vec<Box<dyn AnalyzerRule>>,
}

impl Analyzer {
    pub fn new(temp_views: HashMap<String, RelationPlaceholder>) -> Self {
        let rules: Vec<Box<dyn AnalyzerRule>> = vec![
            Box::new(ResolveRelations::new(temp_views)),
            Box::new(ResolveReferences),
            Box::new(ResolveFunctions),
            Box::new(ImplicitTypeCasts),
        ];
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
                println!("{} apply {} change:{} after: {:?}", i, rule.name(), t.transformed, new_plan);
                if t.transformed {
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        self.check_analysis(new_plan)
    }

    fn check_analysis(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
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
        Ok(plan.transform_up(|p| {
            if !p.resolved() {
                return Err(format!("unresolved operator {:?}", p))
            } else {
                Ok(Transformed::no(p))
            }
        })?.data)
    }
}