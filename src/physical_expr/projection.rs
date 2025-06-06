use crate::Result;
use crate::data::{Row, GenericRow, JoinedRow};
use crate::expr::{AttributeReference, BoundReference, Expr};
use crate::physical_expr::{create_physical_expr, PhysicalExpr};

# [derive(Debug)]
pub struct Projection {
    exprs: Vec<(usize, Box<dyn PhysicalExpr>)>,
}

impl Projection {
    pub fn new(expressions: Vec<Expr>) -> Result<Self> {
        let exprs: Result<Vec<(usize, Box<dyn PhysicalExpr>)>, String> = expressions.iter().enumerate()
            .filter(|(_, expr)| !matches!(expr, Expr::NoOp))
            .map(|(i, expr)| create_physical_expr(expr).map(|expr| (i, expr))).collect();
        let exprs = exprs?;
        Ok(Self {exprs})
    }

    pub fn new_with_input_attrs(expressions: Vec<Expr>, input: Vec<AttributeReference>) -> Result<Self> {
        let expressions = BoundReference::bind_references(expressions, input)?;
        Self::new(expressions)
    }

    pub fn apply(&self, input: &dyn Row) -> GenericRow {
        let mut row = GenericRow::new_with_size(self.exprs.len());
        for (i, expr) in self.exprs.iter() {
            row.update(*i, expr.eval(input));
        }
        row
    }

    pub fn apply_targert(&self, row: &mut GenericRow, input: &dyn Row)  {
        for (i, expr) in self.exprs.iter() {
            row.update(*i, expr.eval(input));
        }
    }
}

# [derive(Debug)]
pub struct MutableProjection {
    exprs: Vec<(usize, Box<dyn PhysicalExpr>)>,
    row: GenericRow,
}

impl MutableProjection {
    pub fn new(expressions: Vec<Expr>) -> Result<Self> {
        let row = GenericRow::new_with_size(expressions.len());
        let exprs: Result<Vec<(usize, Box<dyn PhysicalExpr>)>, String> = expressions.iter().enumerate()
            .filter(|(_, expr)| !matches!(expr, Expr::NoOp))
            .map(|(i, expr)| create_physical_expr(expr).map(|expr| (i, expr))).collect();
        let exprs = exprs?;
        Ok(Self {exprs, row})
    }

    pub fn new_with_input_attrs(expressions: Vec<Expr>, input: Vec<AttributeReference>) -> Result<Self> {
        let expressions = BoundReference::bind_references(expressions, input)?;
        Self::new(expressions)
    }

    pub fn targert(&mut self, target: GenericRow) {
        self.row = target;
    }

    #[inline]
    pub fn result(&self) -> &GenericRow {
        &self.row
    }

    pub fn apply(&mut self, input: &dyn Row) -> &GenericRow {
        for (i, expr) in self.exprs.iter() {
            self.row.update(*i, expr.eval(input));
        }
        &self.row
    }
}

# [derive(Debug)]
pub struct MutableProjectionForAgg {
    exprs: Vec<(usize, Box<dyn PhysicalExpr>)>,
    row: GenericRow,
}

impl MutableProjectionForAgg {
    pub fn new(expressions: Vec<Expr>) -> Result<Self> {
        let row = GenericRow::new_with_size(expressions.len());
        let exprs: Result<Vec<(usize, Box<dyn PhysicalExpr>)>, String> = expressions.iter().enumerate()
            .filter(|(_, expr)| !matches!(expr, Expr::NoOp))
            .map(|(i, expr)| create_physical_expr(expr).map(|expr| (i, expr))).collect();
        let exprs = exprs?;
        Ok(Self {exprs, row})
    }

    pub fn new_with_input_attrs(expressions: Vec<Expr>, input: Vec<AttributeReference>) -> Result<Self> {
        let expressions = BoundReference::bind_references(expressions, input)?;
        Self::new(expressions)
    }

    pub fn targert(&mut self, target: GenericRow) {
        self.row = target;
    }

    #[inline]
    pub fn result(&self) -> &GenericRow {
        &self.row
    }

    pub fn apply(&mut self, input: &dyn Row) -> &GenericRow {
        for (i, expr) in self.exprs.iter() {
            let joiner = JoinedRow::new(&self.row, input) ;
            self.row.update(*i, expr.eval(&joiner));
        }
        &self.row
    }
}





