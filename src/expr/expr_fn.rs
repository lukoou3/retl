use crate::{Operator};
use crate::expr::{Expr, BinaryOperator};

/// Return a new expression `left <op> right`
pub fn binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryOperator(BinaryOperator::new(Box::new(left), op, Box::new(right)))
}

