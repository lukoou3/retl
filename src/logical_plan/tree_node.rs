use crate::expr::Expr;
use crate::Result;
use crate::logical_plan::{Aggregate, Expression, Filter, Generate, LogicalPlan, Project, SubqueryAlias};
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
            LogicalPlan::UnresolvedRelation(_) | LogicalPlan::OneRowRelation | LogicalPlan::RelationPlaceholder(_) => {
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
            LogicalPlan::SubqueryAlias(SubqueryAlias { identifier, child }) =>
                child.map_elements(f)?
                .update_data(|child| {
                    LogicalPlan::SubqueryAlias(SubqueryAlias {identifier, child})
                }),
            LogicalPlan::Expression(Expression { expr, child }) =>
                child.map_elements(f)?
                .update_data(|child| {
                    LogicalPlan::Expression(Expression {expr, child, })
                }),
            LogicalPlan::Aggregate(Aggregate {grouping_exprs, aggregate_exprs, child}) =>
                child.map_elements(f)?
                .update_data(|child| {
                    LogicalPlan::Aggregate(Aggregate {grouping_exprs, aggregate_exprs, child})
                }),
            LogicalPlan::Generate(Generate{generator, unrequired_child_index, outer, qualifier, generator_output, child}) =>
                child.map_elements(f)?
                .update_data(|child| {
                    LogicalPlan::Generate(Generate{generator, unrequired_child_index, outer, qualifier, generator_output, child})
                })

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
            LogicalPlan::UnresolvedRelation(_) | LogicalPlan::OneRowRelation | LogicalPlan::RelationPlaceholder(_)
             | LogicalPlan::SubqueryAlias(_) =>
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
            LogicalPlan::Aggregate(Aggregate {grouping_exprs, aggregate_exprs, child}) =>
                Ok((grouping_exprs, aggregate_exprs).map_elements(f)?
                    .update_data(|(grouping_exprs, aggregate_exprs)|
                    LogicalPlan::Aggregate(Aggregate {grouping_exprs, aggregate_exprs, child})
                    )
                ),
            LogicalPlan::Generate(Generate{generator, unrequired_child_index, outer, qualifier, generator_output, child}) =>
                Ok(f(generator)? .update_data(|generator|
                        LogicalPlan::Generate(Generate{generator, unrequired_child_index, outer, qualifier, generator_output, child})
                    )
                ),
        }
    }

    pub fn transform_up_expressions<F: FnMut(Expr) -> Result<Transformed<Expr>> + Copy>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        self.transform_up(|plan|  plan.map_expressions(|expr| expr.transform_up(f)))
    }
}