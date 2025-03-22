use crate::expr::Expr;
use crate::Result;
use crate::logical_plan::{Expression, Filter, LogicalPlan, Project};
use crate::tree_node::{Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion};

impl TreeNode for LogicalPlan {
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        for x in self.children() {
            if f(x)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        Ok(match self {
            LogicalPlan::UnresolvedRelation(_) | LogicalPlan::RelationPlaceholder(_) => {
                Transformed::no(self)
            }
            LogicalPlan::Project(Project {
                project_list,
                child,
            }) => child.map_elements(f)?.update_data(|child| {
                LogicalPlan::Project(Project::new(project_list, child,))
            }),
            LogicalPlan::Filter(Filter { condition, child }) =>
                child.map_elements(f)?
                .update_data(|child| {
                    LogicalPlan::Filter(Filter { condition, child })
                }),
            LogicalPlan::Expression(Expression { expr, child }) =>
                child.map_elements(f)?
                .update_data(|child| {
                    LogicalPlan::Expression(Expression {expr, child, })
                }),
        })
    }
}

impl LogicalPlan {
    /// Rewrites all expressions in the current `LogicalPlan` node using `f`.
    ///
    /// Returns the current node.
    ///
    /// # Notes
    /// * Similar to [`TreeNode::map_children`] but for this node's expressions.
    /// * Visits only the top level expressions (Does not recurse into each expression)
    pub fn map_expressions<F: FnMut(Expr) -> Result<Transformed<Expr>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        match self {
            LogicalPlan::UnresolvedRelation(_) | LogicalPlan::RelationPlaceholder(_) =>
                Ok(Transformed::no(self)),
            LogicalPlan::Project(Project { project_list, child, }) =>
                Ok(project_list.map_elements(f)?
                    .update_data(|project_list|
                    LogicalPlan::Project(Project {project_list, child, }))
                ),
            LogicalPlan::Filter(Filter { condition, child }) =>
                Ok(f(condition)?
                    .update_data(|condition|
                    LogicalPlan::Filter(Filter {condition, child, }))
                ),
            LogicalPlan::Expression(Expression { expr, child }) =>
                Ok(f(expr)?
                    .update_data(|expr|
                    LogicalPlan::Expression(Expression {expr, child, }))
                ),
        }
    }
}