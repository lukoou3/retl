use std::any::Any;
use std::cmp::{Ordering, PartialEq};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use itertools::Itertools;
use crate::{Operator, Result};
use crate::data::Value;
use crate::expr::{binary_expr, Coalesce, Greatest, Least};
use crate::expr::aggregate::{DeclarativeAggFunction, TypedAggFunction};
use crate::physical_expr::{self as phy, can_cast, PhysicalExpr};
use crate::tree_node::{Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion};
use crate::types::{AbstractDataType, DataType};

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum Expr {
    UnresolvedAttribute(String),
    UnresolvedExtractValue(UnresolvedExtractValue),
    NoOp,
    BoundReference(BoundReference),
    AttributeReference(AttributeReference),
    Alias(Alias),
    Cast(Cast),
    Literal(Literal),
    UnresolvedFunction(UnresolvedFunction),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    BinaryOperator(BinaryOperator),
    Like(Like),
    RLike(Like),
    In(In),
    ScalarFunction(Box<dyn ScalarFunction>),
    DeclarativeAggFunction(Box<dyn DeclarativeAggFunction>),
    TypedAggFunction(Box<dyn TypedAggFunction>),
}

impl Expr {
    pub fn foldable(&self) -> bool {
        match self {
            Expr::UnresolvedAttribute(_) | Expr::UnresolvedExtractValue(_) | Expr::UnresolvedFunction(_) | Expr::BoundReference(_) => false,
            // We should never fold named expressions in order to not remove the alias.
            Expr::AttributeReference(_) | Expr::Alias(_)  => false,
            Expr::Literal(_)  => true,
            Expr::ScalarFunction(f) => f.foldable(),
            Expr::DeclarativeAggFunction(_) => false,
            _ => self.children().iter().all(|c| c.foldable()),
        }
    }


    pub fn data_type(&self) -> &DataType {
        match self {
            Expr::UnresolvedAttribute(_) | Expr::UnresolvedExtractValue(_) | Expr::UnresolvedFunction(_)  =>
                panic!("UnresolvedExpr:{:?}", self),
            Expr::NoOp => DataType::null_type(),
            Expr::BoundReference(b) => &b.data_type,
            Expr::AttributeReference(a) => &a.data_type,
            Expr::Alias(e) => e.child.data_type(),
            Expr::Literal(l) => &l.data_type,
            Expr::Cast(c) => &c.data_type,
            Expr::Not(_) | Expr::IsNull(_) | Expr::IsNotNull(_) => DataType::boolean_type(),
            Expr::BinaryOperator(BinaryOperator{left, op, right:_ }) =>  match op {
                Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo =>
                    left.data_type(),
                Operator::Eq | Operator::NotEq | Operator::Lt |Operator::LtEq | Operator::Gt | Operator::GtEq =>
                    DataType::boolean_type(),
                Operator::And | Operator::Or =>
                    DataType::boolean_type(),
            },
            Expr::Like(_) => DataType::boolean_type(),
            Expr::RLike(_) => DataType::boolean_type(),
            Expr::In(_) => DataType::boolean_type(),
            Expr::ScalarFunction(f) => f.data_type(),
            Expr::DeclarativeAggFunction(f) => f.data_type(),
            Expr::TypedAggFunction(f) => f.data_type(),
        }
    }

    pub fn resolved(&self) -> bool {
        match self {
            Expr::UnresolvedAttribute(_) | Expr::UnresolvedFunction(_) =>
                false,
            _ => self.children_resolved() && self.check_input_data_types().is_ok()
        }
    }

    pub fn children_resolved(&self) -> bool {
        self.children().iter().all(|c| c.resolved())
    }

    pub fn check_input_data_types(&self) -> Result<()> {
        match self {
            Expr::UnresolvedAttribute(_)
             | Expr::UnresolvedExtractValue(_)
             | Expr::UnresolvedFunction(_)
             | Expr::NoOp
             | Expr::BoundReference(_)
             | Expr::AttributeReference(_)
             | Expr::Literal(_)
             | Expr::Alias(_) =>
                Ok(()),
            Expr::Cast(Cast{child, data_type}) =>{
                let from = child.data_type();
                if can_cast(from, data_type) {
                    Ok(())
                } else {
                    Err(format!("cannot cast {} to {}", from, data_type))
                }
            },
            Expr::Not(child) => {
                if child.data_type() != DataType::boolean_type() {
                    Err(format!("{:?} requires boolean type, not {}", self, child.data_type()))
                } else {
                    Ok(())
                }
            },
            Expr::IsNull(_) | Expr::IsNotNull(_) => Ok(()),
            Expr::BinaryOperator(BinaryOperator{left, op, right}) => {
                if left.data_type() != right.data_type() {
                    return Err(format!("differing types in {:?}", self));
                }
                match op {
                    Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo => {
                        if !left.data_type().is_numeric_type() {
                            Err(format!("{:?} requires numeric type, not {}", self, left.data_type()))
                        } else if *op == Operator::Divide && left.data_type() != DataType::long_type() && left.data_type() != DataType::double_type() {
                            Err(format!("{:?} requires long/double type, not {}", self, left.data_type()))
                        } else {
                            Ok(())
                        }
                    },
                    Operator::Eq | Operator::NotEq | Operator::Lt |Operator::LtEq | Operator::Gt | Operator::GtEq =>
                        if !left.data_type().is_orderable()  {
                            Err(format!("{:?} requires orderable type, not {}", self, left.data_type()))
                        } else {
                            Ok(())
                        },
                    Operator::And | Operator::Or =>
                        if left.data_type() != DataType::boolean_type() {
                            Err(format!("{:?} requires boolean type, not {}", self, left.data_type()))
                        } else {
                            Ok(())
                        },
                }
            },
            Expr::Like(Like{expr, pattern})
             | Expr::RLike(Like{expr, pattern}) => {
                if expr.data_type() != DataType::string_type(){
                    Err(format!("{:?} requires string type, not {}", self, expr.data_type()))
                } else if pattern.data_type() != DataType::string_type() {
                    Err(format!("{:?} requires string type, not {}", self, pattern.data_type()))
                } else {
                    Ok(())
                }
            },
            Expr::In(In{value, list}) => {
                if list.iter().any(|e| value.data_type() != e.data_type()) {
                    Err(format!("{:?} requires same type", self))
                } else {
                    Ok(())
                }
            },
            Expr::ScalarFunction(f) => {
                f.check_input_data_types()
            },
            Expr::DeclarativeAggFunction(f) => {
                f.check_input_data_types()
            },
            Expr::TypedAggFunction(f) => {
                f.check_input_data_types()
            },
        }
    }

    pub fn children(&self) -> Vec<&Expr> {
        match self {
            Expr::UnresolvedAttribute(_)
            | Expr::BoundReference(_)
            | Expr::AttributeReference(_)
            | Expr::NoOp
            | Expr::Literal(_) => Vec::new(),
            Expr::UnresolvedExtractValue(UnresolvedExtractValue{child, extraction}) =>
                vec![child, extraction],
            Expr::Alias(Alias{ child, ..})
            | Expr::Cast(Cast{ child, ..})
            | Expr::Not(child)
            | Expr::IsNull(child) | Expr::IsNotNull(child) =>
                vec![child],
            Expr::BinaryOperator(BinaryOperator { left, right, .. }) =>
                vec![left, right],
            Expr::Like(Like{expr, pattern})
            | Expr::RLike(Like{expr, pattern}) =>
                vec![expr, pattern],
            Expr::In(In{value, list}) =>
                vec![value.as_ref()].into_iter().chain(list.iter()).collect(),
            Expr::ScalarFunction(f) => f.args(),
            Expr::DeclarativeAggFunction(f) => f.args(),
            Expr::TypedAggFunction(f) => f.args(),
            Expr::UnresolvedFunction(UnresolvedFunction{name: _, arguments}) =>
                arguments.iter().map(|a| a).collect(),
        }
    }

    pub fn to_attribute(&self) -> Result<AttributeReference> {
        match self {
            Expr::Alias(Alias {child, name, expr_id}) =>
                Ok(AttributeReference::new_with_expr_id(name, child.data_type().clone(), *expr_id)),
            Expr::AttributeReference(a) => Ok(a.clone()),
            _ => Err(format!("cannot convert {:?} to AttributeReference", self)),
        }
    }
    
    pub fn alias(self, name: impl Into<String>) -> Expr {
        Expr::Alias(Alias::new(self, name.into()))
    }

    pub fn cast(self, data_type: DataType) -> Expr {
        if self.data_type() == &data_type {
            return self;
        }
        Expr::Cast(Cast::new(self, data_type))
    }

    pub fn not(self) -> Expr {
        Expr::Not(Box::new(self))
    }

    pub fn is_null(self) -> Expr {
        Expr::IsNull(Box::new(self))
    }

    pub fn is_not_null(self) -> Expr {
        Expr::IsNotNull(Box::new(self))
    }

    pub fn col(ordinal: usize, data_type: DataType) -> Expr {
        Expr::BoundReference(BoundReference::new(ordinal, data_type))
    }

    pub fn lit(value: Value, data_type: DataType) -> Expr {
        Expr::Literal(Literal::new(value, data_type))
    }

    pub fn int_lit(v: i32) -> Expr {
        Expr::Literal(Literal::new(Value::Int(v), DataType::Int))
    }

    pub fn long_lit(v: i64) -> Expr {
        Expr::Literal(Literal::new(Value::Long(v), DataType::Long))
    }

    pub fn double_lit(v: f64) -> Expr {
        Expr::Literal(Literal::new(Value::Double(v), DataType::Double))
    }

    pub fn string_lit(s:impl  Into<String>) -> Expr {
        Expr::Literal(Literal::new(Value::string(s), DataType::String))
    }

    pub fn boolean_lit(v: bool) -> Expr {
        Expr::Literal(Literal::new(Value::Boolean(v), DataType::Boolean))
    }

    pub fn null_lit() -> Expr {
        Expr::Literal(Literal::new(Value::Null, DataType::Null))
    }

    pub fn and(self, other: Expr) -> Expr {
        binary_expr(self, Operator::And, other)
    }

    pub fn or(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Or, other)
    }

    /// Return `self == other`
    pub fn eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Eq, other)
    }

    pub fn gt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Gt, other)
    }
    pub fn ge(self, other: Expr) -> Expr {
        binary_expr(self, Operator::GtEq, other)
    }

    pub fn lt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Lt, other)
    }

    pub fn le(self, other: Expr) -> Expr {
        binary_expr(self, Operator::LtEq, other)
    }

    /// Return `self LIKE other`
    pub fn like(self, other: Expr) -> Expr {
        Expr::Like(Like::new(
            Box::new(self),
            Box::new(other),
        ))
    }

    pub fn coalesce(self, other: Expr) -> Expr {
        Expr::ScalarFunction(Box::new(Coalesce::new(vec![self, other])))
    }

    pub fn greatest(self, other: Expr) -> Expr {
        Expr::ScalarFunction(Box::new(Greatest::new(vec![self, other])))
    }

    pub fn least(self, other: Expr) -> Expr {
        Expr::ScalarFunction(Box::new(Least::new(vec![self, other])))
    }
}

pub fn coalesce2(arg1: Expr, arg2: Expr) -> Expr {
    Expr::ScalarFunction(Box::new(Coalesce::new(vec![arg1, arg2])))
}

impl<'a> TreeNodeContainer<'a, Self> for Expr {
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

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct UnresolvedExtractValue {
    pub child: Box<Expr>,
    pub extraction: Box<Expr>,
}

impl UnresolvedExtractValue {
    pub fn new(child: Box<Expr>, extraction: Box<Expr>) -> Self {
        Self { child, extraction, }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct BoundReference {
    pub ordinal: usize,
    pub data_type: DataType,
}

impl BoundReference {
    pub fn new(ordinal: usize, data_type: DataType) -> Self {
        Self { ordinal, data_type }
    }

    pub fn bind_reference(expr: Expr, input: Vec<AttributeReference>) -> Result<Expr> {
        let expr_id_to_ordinal: HashMap<u32, usize> = input.iter().enumerate().map(|(i, x)| (x.expr_id, i)).collect();
        let new_expr= expr.transform_up(|expr| {
            if let Expr::AttributeReference(AttributeReference{data_type, expr_id, ..}) = &expr {
                if let Some(ordinal) = expr_id_to_ordinal.get(expr_id){
                    return Ok(Transformed::yes(Expr::BoundReference(BoundReference::new(*ordinal, data_type.clone()))));
                } else { return Err(format!("not found {:?} in {:?}", expr, input)) }
            } else {
                Ok(Transformed::no(expr))
            }
        })?.data;
        Ok(new_expr)
    }

    pub fn bind_references(exprs: Vec<Expr>, input: Vec<AttributeReference>) -> Result<Vec<Expr>> {
        let expr_id_to_ordinal: HashMap<u32, usize> = input.iter().enumerate().map(|(i, x)| (x.expr_id, i)).collect();
        let mut new_exprs = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let e = expr.transform_up(|expr| {
                if let Expr::AttributeReference(AttributeReference{data_type, expr_id, ..}) = &expr {
                    if let Some(ordinal) = expr_id_to_ordinal.get(expr_id){
                        return Ok(Transformed::yes(Expr::BoundReference(BoundReference::new(*ordinal, data_type.clone()))));
                    } else { return Err(format!("not found {:?} in {:?}", expr, input)) }
                } else {
                    Ok(Transformed::no(expr))
                }
            })?.data;
            new_exprs.push(e);
        }
        Ok(new_exprs)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Alias {
    pub child: Box<Expr>,
    pub name: String,
    pub expr_id: u32,
}

impl Alias {
    pub fn new(
        expr: Expr,
        name: impl Into<String>,
    ) -> Self {
        Self {
            child: Box::new(expr),
            name: name.into(),
            expr_id: ExprIdGenerator::get_next_expr_id(),
        }
    }

    pub fn new_with_expr_id(expr: Expr, name: impl Into<String>, expr_id: u32, )-> Self{
        Self {
            child: Box::new(expr),
            name: name.into(),
            expr_id,
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Cast {
    pub child: Box<Expr>,
    pub data_type: DataType,
}

impl Cast {
    pub fn new(expr: Expr, data_type: DataType) -> Self {
        Self{child: Box::new(expr), data_type}
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct AttributeReference {
    pub name: String,
    pub data_type: DataType,
    pub expr_id: u32,
}

impl AttributeReference {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        let expr_id = ExprIdGenerator::get_next_expr_id();
        AttributeReference{name: name.into(), data_type, expr_id}
    }

    pub fn new_with_expr_id(name: impl Into<String>, data_type: DataType, expr_id: u32) -> Self {
        AttributeReference{name: name.into(), data_type, expr_id}
    }

    pub fn with_expr_id(&self, expr_id: u32) -> Self {
        AttributeReference{ name: self.name.clone(), data_type: self.data_type.clone(), expr_id: self.expr_id }
    }

    pub fn new_instance(&self) -> Self {
        AttributeReference{ name: self.name.clone(), data_type: self.data_type.clone(), expr_id: ExprIdGenerator::get_next_expr_id() }
    }
}

struct ExprIdGenerator {
    counter: std::sync::atomic::AtomicU32,
}

impl ExprIdGenerator {
    fn get_next_expr_id() -> u32 {
        static INSTANCE: ExprIdGenerator = ExprIdGenerator {
            counter: std::sync::atomic::AtomicU32::new(0),
        };
        INSTANCE.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Literal {
    pub value: Value,
    pub data_type: DataType,
}

impl Literal {
    pub fn new(value: Value, data_type: DataType) -> Self {
        Self { value, data_type }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct UnresolvedFunction {
    pub name: String,
    pub arguments: Vec<Expr>,
}

/// Binary operator
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct BinaryOperator {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// The comparison operator
    pub op: Operator,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl BinaryOperator {
    /// Create a new binary expression
    pub fn new(left: Box<Expr>, op: Operator, right: Box<Expr>) -> Self {
        Self { left, op, right }
    }
}

/*#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct BinaryExpr {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl BinaryExpr {
    pub fn new(left: Box<Expr>, right: Box<Expr>) -> Self {
        Self { left, right }
    }
}*/

/// LIKE expression
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct Like {
    pub expr: Box<Expr>,
    pub pattern: Box<Expr>,
}

impl Like {
    pub fn new(expr: Box<Expr>, pattern: Box<Expr>) -> Self {
        Self { expr, pattern }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct In {
    pub value: Box<Expr>,
    pub list: Vec<Expr>,
}

impl In {
    pub fn new(value: Box<Expr>, list: Vec<Expr>) -> Self {
        Self { value, list }
    }
}

pub trait ScalarFunction: Debug + Send + Sync + CreateScalarFunction + ExtendScalarFunction {
    fn name(&self) -> &str;
    fn foldable(&self) -> bool {
        self.args().iter().all(|arg| arg.foldable())
    }
    fn data_type(&self) -> &DataType;
    fn args(&self) -> Vec<&Expr>;
    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        None
    }
    fn check_input_data_types(&self) -> Result<()> {
        match self.expects_input_types() {
            None => {
                Ok(())
            },
            Some(input_types) => {
                let mut mismatches = Vec::new();
                for (i, (tp, input_type)) in self.args().into_iter().zip(input_types.iter()).enumerate() {
                    if !input_type.accepts_type(tp.data_type()) {
                        mismatches.push(format!("{} argument {} requires {:?}, but get {}", self.name(), i + 1, input_type, tp.data_type()));
                    }
                }
                if mismatches.is_empty() {
                    Ok(())
                } else {
                    Err(mismatches.into_iter().join(" "))
                }
            },
        }

    }
    
    fn create_physical_expr(&self) -> Result<Arc<dyn PhysicalExpr>>;
}

pub trait CreateScalarFunction {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> where Self: Sized;

    fn create_function_expr(args: Vec<Expr>) -> Result<Expr> where Self: Sized {
        Ok(Expr::ScalarFunction(Self::from_args(args)?))
    }
}

pub trait ExtendScalarFunction {
    fn clone_box(&self) -> Box<dyn ScalarFunction>;
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction>;
}

impl<T: ScalarFunction + Clone + 'static> ExtendScalarFunction for T {
    fn clone_box(&self) -> Box<dyn ScalarFunction> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn ScalarFunction> {
        Self::from_args(args).unwrap()
    }
}

impl Clone for Box<dyn ScalarFunction> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl PartialEq for Box<dyn ScalarFunction> {
    fn eq(&self, other: &Self) -> bool {
        if self.as_any().type_id() != other.as_any().type_id() {
            return false;
        }
        let args1 = self.args();
        let args2 = other.args();
        if args1.len() != args2.len() {
            return false;
        };
        args1.iter().zip(args2.iter()).all(|(a, b)| a == b)
    }
}

impl Eq for Box<dyn ScalarFunction> {}

impl PartialOrd for Box<dyn ScalarFunction> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let args1 = self.args();
        let args2 = other.args();
        if args1.len() != args2.len() {
            return None;
        };
        for i in 0..args1.len() {
            match args1[i].partial_cmp(args2[i]) {
                None => return None, // 某个元素无法比较
                Some(Ordering::Equal) => continue, // 继续比较下一个元素
                Some(ord) => return Some(ord), // 返回当前元素的比较结果
            }
        }
        // 所有元素都相等
        Some(Ordering::Equal)
    }
}

impl Hash for Box<dyn ScalarFunction> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for x in self.args() {
            x.hash(state);
        }
    }
}

pub fn create_physical_expr(
    e: &Expr,
) -> Result<Arc<dyn PhysicalExpr>> {
    match e {
        Expr::BoundReference(BoundReference{ordinal, data_type}) =>
            Ok(Arc::new(phy::BoundReference::new(*ordinal, data_type.clone()))),
        Expr::Alias(Alias{child, ..}) =>
            create_physical_expr(child),
        Expr::Literal(Literal{value, data_type}) =>
            Ok(Arc::new(phy::Literal::new(value.clone(), data_type.clone()))),
        Expr::Cast(Cast{child, data_type}) =>
            Ok(Arc::new(phy::Cast::new(create_physical_expr(child)?, data_type.clone()))),
        Expr::Not(child) =>
            Ok(Arc::new(phy::Not::new(create_physical_expr(child)?))),
        Expr::IsNull(child) =>
            Ok(Arc::new(phy::IsNull::new(create_physical_expr(child)?))),
        Expr::IsNotNull(child) =>
            Ok(Arc::new(phy::IsNotNull::new(create_physical_expr(child)?))),
        Expr::BinaryOperator(BinaryOperator{left, op, right}) => match op {
            Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo => {
                let l = create_physical_expr(left)?;
                let r = create_physical_expr(right)?;
                Ok(Arc::new(phy::BinaryArithmetic::new(l, op.clone(), r)))
            }
            Operator::Eq | Operator::NotEq | Operator::Lt |Operator::LtEq | Operator::Gt | Operator::GtEq =>
                Ok(Arc::new(phy::BinaryComparison::new(create_physical_expr(left)?, op.clone(), create_physical_expr(right)?))),
            Operator::And =>
                Ok(Arc::new(phy::And::new(create_physical_expr(left)?, create_physical_expr(right)?))),
            Operator::Or =>
                Ok(Arc::new(phy::Or::new(create_physical_expr(left)?, create_physical_expr(right)?))),
        },
        Expr::Like(Like{expr, pattern}) =>
            Ok(Arc::new(phy::Like::new(create_physical_expr(expr)?, create_physical_expr(pattern)?))),
        Expr::RLike(Like{expr, pattern}) =>
            Ok(Arc::new(phy::RLike::new(create_physical_expr(expr)?, create_physical_expr(pattern)?))),
        Expr::In(In{value, list}) => {
            let value = create_physical_expr(value)?;
            let list = list.into_iter().map(|child| create_physical_expr(child)).collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(phy::In::new(value, list)))
        },
        Expr::ScalarFunction(func) => func.create_physical_expr(),
        _ => Err(format!("Not implemented:{:?}", e)),
    }

}

#[macro_export]
macro_rules! match_downcast {
    ($func:expr, $($type:ident { $($field:ident),* } => $block:block),* $(,)? _ => $else_block:block) => {{
        $(
            if $func.as_any().downcast_ref::<$type>().is_some() {
                let f = $func.into_any().downcast::<$type>().unwrap();
                let $type { $($field),* } = *f; // 解构 Box<T> 到指定字段
                $block
            } else
        )*
        $else_block
    }};
}

#[macro_export]
macro_rules! match_downcast_ref {
    ($func:expr, $($type:ident { $($field:ident),* } => $block:block),* $(,)? _ => $else_block:block) => {{
        $(
            if let Some($type { $($field),* }) = $func.as_any().downcast_ref::<$type>() {
                $block
            } else
        )*
        $else_block
    }};
}