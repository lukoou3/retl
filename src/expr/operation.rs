use std::ops;
use crate::{Operator};
use crate::expr::{Expr, binary_expr};


/// Support `<expr> + <expr>` fluent style
impl ops::Add for Expr {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Plus, rhs)
    }
}

/// Support `<expr> - <expr>` fluent style
impl ops::Sub for Expr {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Minus, rhs)
    }
}

/// Support `<expr> * <expr>` fluent style
impl ops::Mul for Expr {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Multiply, rhs)
    }
}

/// Support `<expr> / <expr>` fluent style
impl ops::Div for Expr {
    type Output = Self;

    fn div(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Divide, rhs)
    }
}

/// Support `<expr> % <expr>` fluent style
impl ops::Rem for Expr {
    type Output = Self;

    fn rem(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Modulo, rhs)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_operators() {

    }
}