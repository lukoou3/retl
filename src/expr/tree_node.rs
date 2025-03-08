use crate::Result;
use crate::expr::*;
use crate::tree_node::{Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion};

impl TreeNode for Expr {
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
        mut f: F,
    ) -> Result<Transformed<Self>> {
        Ok(match self {
            Expr::UnresolvedAttribute(_)
            | Expr::BoundReference(_)
            | Expr::AttributeReference(_)
            | Expr::Literal(_) => Transformed::no(self),
            Expr::Alias(Alias {
                child,
                name,
                expr_id,
            }) => f(*child)?
                .update_data(|child| Expr::Alias(Alias::new_with_expr_id(child, name, expr_id))),
            Expr::Cast(Cast { child, data_type }) => f(*child)?.update_data(|e| e.cast(data_type)),
            Expr::Not(child) => f(*child)?.update_data(|e| e.not()),
            Expr::BinaryOperator(BinaryOperator { left, op, right }) => (left, right)
                .map_elements(f)?
                .update_data(|(new_left, new_right)| {
                    Expr::BinaryOperator(BinaryOperator::new(new_left, op, new_right))
                }),
            Expr::Like(Like { expr, pattern }) => {
                (expr, pattern)
                    .map_elements(f)?
                    .update_data(|(new_expr, new_pattern)| {
                        Expr::Like(Like::new(new_expr, new_pattern))
                    })
            }
            Expr::RLike(Like { expr, pattern }) => {
                (expr, pattern)
                    .map_elements(f)?
                    .update_data(|(new_expr, new_pattern)| {
                        Expr::RLike(Like::new(new_expr, new_pattern))
                    })
            }
            Expr::UnresolvedFunction(UnresolvedFunction { name, arguments }) => {
                arguments.map_elements(f)?.update_data(|arguments| {
                    Expr::UnresolvedFunction(UnresolvedFunction{name, arguments})
                })
            }
            Expr::ScalarFunction(func) => {
                let args = func
                    .args()
                    .into_iter()
                    .map(|x| x.clone())
                    .collect::<Vec<_>>();
                args.map_elements(f)?
                    .update_data(|args| Expr::ScalarFunction(func.rewrite_args(args)))
            }
        })
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
struct MyNode {
    no: i32,
    children: Vec<MyNode>,
}

impl<'a> TreeNodeContainer<'a, Self> for MyNode {
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

impl TreeNode for MyNode {
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        // 必须引入TreeNodeContainer接口才能调用apply_elements方法，感觉和scala的隐士转换有点类似
        self.children.apply_elements(f)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.children.map_elements(f)?.map_data(|new_children| {
            Ok(MyNode {
                no: self.no,
                children: new_children,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Value;
    use crate::types::DataType;
    use std::sync::Arc;

    ///         1
    ///         2
    ///     3       4
    ///   5   6   7   8
    fn build_tree() -> MyNode {
        let node5 = MyNode {
            no: 5,
            children: vec![],
        };
        let node6 = MyNode {
            no: 6,
            children: vec![],
        };
        let node7 = MyNode {
            no: 7,
            children: vec![],
        };
        let node8 = MyNode {
            no: 8,
            children: vec![],
        };
        let node3 = MyNode {
            no: 3,
            children: vec![node5, node6],
        };
        let node4 = MyNode {
            no: 4,
            children: vec![node7, node8],
        };
        let node2 = MyNode {
            no: 2,
            children: vec![node3, node4],
        };
        let node1 = MyNode {
            no: 1,
            children: vec![node2],
        };
        node1
    }

    #[test]
    fn test_apply() {
        let node = build_tree();
        println!("{:#?}", node);
        node.apply(|node| {
            println!("accessed node: {}", node.no);
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("error");
        println!("{}", "#".repeat(20));
        node.apply(|node| {
            println!("accessed node: {}", node.no);
            if node.no == 3 {
                Ok(TreeNodeRecursion::Jump)
            } else if node.no == 7 {
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .expect("error");
    }

    #[test]
    fn test_transform_up() {
        let node = build_tree();
        println!("{:?}", node);
        // .transform的参数是self，会转移所有权
        let rst1 = node
            .clone()
            .transform_up(|mut node| {
                let old_no = node.no;
                node.no = old_no * 10;
                println!("transformed node: {} -> {}", old_no, node.no);
                Ok(Transformed::new(node, true, TreeNodeRecursion::Continue))
            })
            .unwrap()
            .data;
        println!("{:?}", rst1);
        let rst2 = node
            .clone()
            .transform_up(|mut node| {
                let old_no = node.no;
                node.no = old_no * 10;
                println!("transformed node: {} -> {}", old_no, node.no);
                if old_no == 2 {
                    Ok(Transformed::new(node, true, TreeNodeRecursion::Stop))
                } else if old_no == 6 {
                    Ok(Transformed::new(node, true, TreeNodeRecursion::Jump))
                } else {
                    Ok(Transformed::new(node, true, TreeNodeRecursion::Continue))
                }
            })
            .unwrap()
            .data;
        println!("{:?}", rst2);
    }

    #[test]
    fn test_apply_expr() {
        let col1 = Expr::col(0, DataType::Int);
        let literal = Expr::lit(Value::Int(10), DataType::Int);
        let add = col1 + literal;
        let expr = add.like(Expr::lit(
            Value::String(Arc::new("a".to_string())),
            DataType::String,
        ));
        println!("{:#?}", expr);
        expr.apply(|e| {
            println!("accessed expr: {:?}", e);
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("error");
        println!("{}", "#".repeat(20));
        expr.apply(|e| {
            println!("accessed expr: {:?}", e);
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("error");
    }
}
