use crate::analysis::AnalyzerRule;
use crate::data::Value;
use crate::expr::{BinaryOperator, In, Expr, If, CaseWhen, Coalesce, Least, Greatest, ScalarFunction};
use crate::logical_plan::LogicalPlan;
use crate::{match_downcast, match_downcast_ref, Operator};
use crate::tree_node::{Transformed, TreeNode};
use crate::types::{AbstractDataType, DataType};

pub fn type_coercion_rules() -> Vec<Box<dyn AnalyzerRule>> {
    vec![
        Box::new(InConversion),
        Box::new(PromoteStrings),
        Box::new(FunctionArgumentConversion),
        Box::new(CaseWhenCoercion),
        Box::new(IfCoercion),
        Box::new(ImplicitTypeCasts),
    ]
}

#[derive(Debug)]
pub struct ImplicitTypeCasts;

impl AnalyzerRule for ImplicitTypeCasts {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr {
            e if !e.children_resolved() => Ok(Transformed::no(e)),
            Expr::BinaryOperator(BinaryOperator{left, op, right}) if !matches!(op, Operator::BitShiftLeft | Operator::BitShiftRight | Operator::BitShiftRightUnsigned) && left.data_type() != right.data_type() => {
                match find_tightest_common_type(left.data_type().clone(), right.data_type().clone()) {
                    Some(common_type) => match op {
                        Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo
                          | Operator::BitAnd | Operator::BitOr | Operator::BitXor => {
                            if common_type.is_numeric_type() {
                                let new_left = if left.data_type() == &common_type {
                                    left
                                } else {
                                    Box::new(left.cast(common_type.clone()))
                                };
                                let new_right = if right.data_type() == &common_type {
                                    right
                                } else {
                                    Box::new(right.cast(common_type.clone()))
                                };
                                Ok(Transformed::yes(Expr::BinaryOperator(BinaryOperator{left: new_left, op: op.clone(), right: new_right})))
                            } else {
                                Ok(Transformed::no(Expr::BinaryOperator(BinaryOperator{left, op, right})))
                            }
                        },
                        Operator::Eq | Operator::NotEq | Operator::Lt |Operator::LtEq | Operator::Gt | Operator::GtEq => {
                            if common_type.is_numeric_type() || common_type == DataType::String {
                                let new_left = if left.data_type() == &common_type {
                                    left
                                } else {
                                    Box::new(left.cast(common_type.clone()))
                                };
                                let new_right = if right.data_type() == &common_type {
                                    right
                                } else {
                                    Box::new(right.cast(common_type.clone()))
                                };
                                Ok(Transformed::yes(Expr::BinaryOperator(BinaryOperator{left: new_left, op: op.clone(), right: new_right})))
                            } else {
                                Ok(Transformed::no(Expr::BinaryOperator(BinaryOperator{left, op, right})))
                            }
                        },
                        Operator::And | Operator::Or => {
                            if common_type == DataType::Boolean {
                                let new_left = if left.data_type() == &common_type {
                                    left
                                } else {
                                    Box::new(left.cast(common_type.clone()))
                                };
                                let new_right = if right.data_type() == &common_type {
                                    right
                                } else {
                                    Box::new(right.cast(common_type.clone()))
                                };
                                Ok(Transformed::yes(Expr::BinaryOperator(BinaryOperator{left: new_left, op: op.clone(), right: new_right})))
                            } else {
                                Ok(Transformed::no(Expr::BinaryOperator(BinaryOperator{left, op, right})))
                            }
                        },
                        Operator::BitShiftLeft | Operator::BitShiftRight | Operator::BitShiftRightUnsigned => Ok(Transformed::no(Expr::BinaryOperator(BinaryOperator{left, op, right}))),
                    }
                    None => Ok(Transformed::no(Expr::BinaryOperator(BinaryOperator{left, op, right})))
                }
            },
            Expr::ScalarFunction(func) => {
                if let Some(input_types) = func.expects_input_types()  {
                    if func.args().into_iter().zip(input_types.clone().into_iter()).any(|(arg, input_type)| !input_type.accepts_type(arg.data_type())  ) {
                        let mut args = Vec::with_capacity(func.args().len());
                        let mut change = false;
                        for (arg, input_type) in func.args().into_iter().zip(input_types.into_iter()) {
                            if let Some(tp) = implicit_cast(arg.data_type(), input_type) {
                                args.push(cast_if_not_same_type(arg.clone(), &tp));
                                change = true;
                            } else {
                                args.push(arg.clone());
                            }
                        }
                        if change {
                            return Ok(Transformed::yes(Expr::ScalarFunction(func.rewrite_args(args))))
                        }
                    }
                }
                Ok(Transformed::no(Expr::ScalarFunction(func)))
            }
            e => Ok(Transformed::no(e))
        })
    }

    fn name(&self) -> &str {
        "ImplicitTypeCasts"
    }
}

#[derive(Debug)]
pub struct PromoteStrings;

impl AnalyzerRule for PromoteStrings {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr.clone() {
            e if !e.children_resolved() => Ok(Transformed::no(e)),
            Expr::BinaryOperator(BinaryOperator{left, op, right})
            if matches!(op, Operator::Eq | Operator::NotEq | Operator::Lt |Operator::LtEq | Operator::Gt | Operator::GtEq)
            && find_common_type_for_binary_comparison(left.data_type(), right.data_type()).is_some() => {
                let common_type = find_common_type_for_binary_comparison(left.data_type(), right.data_type()).unwrap();
                Ok(Transformed::yes(Expr::BinaryOperator(BinaryOperator{
                    left: Box::new(cast_if_not_same_type(*left, &common_type)),
                    op,
                    right: Box::new(cast_if_not_same_type(*right, &common_type))
                })))
            },
            _ => Ok(Transformed::no(expr))
        })
    }

    fn name(&self) -> &str {
        "PromoteStrings"
    }
}

#[derive(Debug)]
pub struct InConversion;

impl AnalyzerRule for InConversion {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr.clone() {
            e if !e.children_resolved() => Ok(Transformed::no(e)),
            Expr::In(In{value, list}) if list.iter().any(|e| e.data_type() != value.data_type()) => {
                match find_wider_common_type(vec![value.data_type().clone()].into_iter().chain(list.iter().map(|e| e.data_type().clone())).collect()) {
                    Some(common_type) => {
                        Ok(Transformed::yes(Expr::In(In{
                            value: Box::new(cast_if_not_same_type(*value, &common_type)),
                            list: list.into_iter().map(|e|cast_if_not_same_type(e, &common_type)).collect()
                        })))
                    },
                    None => Ok(Transformed::no(expr))
                }
            },
            _ => Ok(Transformed::no(expr))
        })
    }

    fn name(&self) -> &str {
        "InConversion"
    }
}

#[derive(Debug)]
pub struct CaseWhenCoercion;

impl AnalyzerRule for CaseWhenCoercion {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr {
            e if !e.children_resolved() => Ok(Transformed::no(e)),
            Expr::ScalarFunction(func) => {
                let any = func.as_any();
                if let Some(CaseWhen{branches, else_value}) = any.downcast_ref::<CaseWhen>() {
                    if ! branches.into_iter().all(|(cond, value)| cond.data_type() == value.data_type()) {
                        let mut types = Vec::with_capacity(branches.len() + 1);
                        for (_, e) in branches {
                            types.push(e.data_type().clone());
                        }
                        types.push(else_value.data_type().clone());
                        if let Some(common_type) = find_wider_common_type(types) {
                            return Ok(Transformed::yes(Expr::ScalarFunction(Box::new(CaseWhen::new(
                                branches.into_iter().map(|(cond, value)| (cond.clone(), cast_if_not_same_type(value.clone(), &common_type))).collect(),
                                Box::new(cast_if_not_same_type(*else_value.clone(), &common_type))
                            )))))
                        }
                    }
                }
                Ok(Transformed::no(Expr::ScalarFunction(func)))
            }
            e => Ok(Transformed::no(e))
        })
    }

    fn name(&self) -> &str {
        "IfCoercion"
    }
}

#[derive(Debug)]
pub struct IfCoercion;

impl AnalyzerRule for IfCoercion {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr {
            e if !e.children_resolved() => Ok(Transformed::no(e)),
            Expr::ScalarFunction(func) => {
                let any = func.as_any();
                if let Some(If{predicate, true_value, false_value}) = any.downcast_ref::<If>() {
                    let predicate = predicate.clone();
                    let true_value = true_value.clone();
                    let false_value = false_value.clone();
                    if true_value.data_type() != false_value.data_type() {
                        if let Some(common_type) = find_wider_type_for_two(true_value.data_type().clone(), false_value.data_type().clone()) {
                            return  Ok(Transformed::yes(Expr::ScalarFunction(Box::new(
                                If::new(
                                    predicate,
                                    Box::new(cast_if_not_same_type(*true_value, &common_type)),
                                    Box::new(cast_if_not_same_type(*false_value, &common_type))
                                )
                            ))));
                        }
                    } else if predicate.data_type() == DataType::null_type() {
                        return Ok(Transformed::yes(Expr::ScalarFunction(Box::new(
                            If::new(
                                Box::new(Expr::lit(Value::Null, DataType::Boolean)),
                                true_value,
                                false_value
                            )
                        ))));
                    }
                }
                Ok(Transformed::no(Expr::ScalarFunction(func)))
            }
            e => Ok(Transformed::no(e))
        })
    }

    fn name(&self) -> &str {
        "IfCoercion"
    }
}

#[derive(Debug)]
pub struct IfCoercion2;

impl AnalyzerRule for IfCoercion2 {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr {
            e if !e.children_resolved() => Ok(Transformed::no(e)),
            Expr::ScalarFunction(func) => {
                match_downcast! { func,
                    If { predicate, true_value, false_value } => {
                        Ok(Transformed::no(Expr::ScalarFunction(Box::new(If::new(predicate, true_value, false_value)))))
                    },
                    CaseWhen { branches, else_value } => {
                       Ok(Transformed::no(Expr::ScalarFunction(Box::new(CaseWhen::new(branches, else_value)))))
                    },
                    _ => {
                        Ok(Transformed::no(Expr::ScalarFunction(func)))
                    }
                }
            },
            e => Ok(Transformed::no(e))
        })
    }

    fn name(&self) -> &str {
        "IfCoercion"
    }
}

#[derive(Debug)]
pub struct IfCoercion3;

impl AnalyzerRule for IfCoercion3 {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr {
            e if !e.children_resolved() => Ok(Transformed::no(e)),
            Expr::ScalarFunction(func) => {
                match_downcast_ref! { func,
                    If { predicate, true_value, false_value } => {
                        Ok(Transformed::no(Expr::ScalarFunction(Box::new(If::new(predicate.clone(), true_value.clone(), false_value.clone())))))
                    },
                    CaseWhen{ branches, else_value} => {
                       Ok(Transformed::no(Expr::ScalarFunction(Box::new(CaseWhen::new(branches.clone(), else_value.clone())))))
                    },
                    _ => {
                        Ok(Transformed::no(Expr::ScalarFunction(func)))
                    }
                }
            },
            e => Ok(Transformed::no(e))
        })
    }

    fn name(&self) -> &str {
        "IfCoercion"
    }
}

#[derive(Debug)]
pub struct FunctionArgumentConversion;

impl AnalyzerRule for FunctionArgumentConversion {
    fn analyze(&self, plan: LogicalPlan) -> crate::Result<Transformed<LogicalPlan>> {
        plan.transform_up_expressions(|expr| match expr {
            e if !e.children_resolved() => Ok(Transformed::no(e)),
            Expr::ScalarFunction(func) => {
                let any = func.as_any();
                if let Some(Coalesce{children}) = any.downcast_ref::<Coalesce>() {
                    if ! children.into_iter().all(|e| e.data_type() == children[0].data_type()) {
                        let mut types = Vec::with_capacity(children.len());
                        for e in children {
                            types.push(e.data_type().clone());
                        }
                        if let Some(common_type) = find_wider_common_type(types) {
                            let children = children.into_iter().map(|e|cast_if_not_same_type(e.clone(), &common_type)).collect();
                            return Ok(Transformed::yes(Expr::ScalarFunction(Box::new(Coalesce::new(children)))))
                        }
                    }
                } else if let Some(Least{children}) = any.downcast_ref::<Least>() {
                    if ! children.into_iter().all(|e| e.data_type() == children[0].data_type()) {
                        let mut types = Vec::with_capacity(children.len());
                        for e in children {
                            types.push(e.data_type().clone());
                        }
                        if let Some(common_type) = find_wider_type_without_string_promotion(types) {
                            let children = children.into_iter().map(|e|cast_if_not_same_type(e.clone(), &common_type)).collect();
                            return Ok(Transformed::yes(Expr::ScalarFunction(Box::new(Least::new(children)))))
                        }
                    }
                } else if let Some(Greatest{children}) = any.downcast_ref::<Greatest>() {
                    if ! children.into_iter().all(|e| e.data_type() == children[0].data_type()) {
                        let mut types = Vec::with_capacity(children.len());
                        for e in children {
                            types.push(e.data_type().clone());
                        }
                        if let Some(common_type) = find_wider_type_without_string_promotion(types) {
                            let children = children.into_iter().map(|e|cast_if_not_same_type(e.clone(), &common_type)).collect();
                            return Ok(Transformed::yes(Expr::ScalarFunction(Box::new(Greatest::new(children)))))
                        }
                    }
                }
                Ok(Transformed::no(Expr::ScalarFunction(func)))
            }
            e => Ok(Transformed::no(e))
        })
    }

    fn name(&self) -> &str {
        "FunctionArgumentConversion"
    }
}

fn cast_if_not_same_type(expr:  Expr, dt:  &DataType) -> Expr {
    if expr.data_type() == dt {
        expr
    } else {
        expr.cast(dt.clone())
    }
}

fn find_tightest_common_type(type1:  DataType, type2:  DataType) -> Option<DataType> {
    match (type1, type2) {
        (t1, t2) if t1 == t2 => Some(t1),
        (t1, DataType::Null) => Some(t1),
        (DataType::Null, t2) => Some(t2),
        (t1, t2) if t1.is_numeric_type() && t2.is_numeric_type() =>
            Some(NUMERIC_PRECEDENCE.iter().rfind(|t| *t == &t1 || *t == &t2).unwrap().clone()),
        _ => None
    }

}

static NUMERIC_PRECEDENCE: [DataType; 4] = [DataType::Int, DataType::Long, DataType::Float, DataType::Double];

fn find_common_type_for_binary_comparison(type1:  &DataType, type2:  &DataType) -> Option<DataType> {
    match (type1, type2) {
        (DataType::String, DataType::Null) => Some(DataType::String),
        (DataType::Null, DataType::String) => Some(DataType::String),
        (DataType::String, r) if r.is_atomic_type() && !matches!(r, DataType::String) => Some(DataType::String),
        (l, DataType::String) if l.is_atomic_type() && !matches!(l, DataType::String) => Some(DataType::String),
        _ => None,
    }
}

fn find_wider_common_type(types: Vec<DataType>) -> Option<DataType> {
    let (string_types, non_string_types): (Vec<_>, Vec<_>) = types.into_iter().partition(|t| has_string_type(t));
    string_types.into_iter().chain(non_string_types.into_iter()).fold(Some(DataType::Null), |r, c| {
        match r {
            Some(d) => find_wider_type_for_two(d, c),
            _ => None
        }
    })
}

fn find_wider_type_without_string_promotion(types: Vec<DataType>) -> Option<DataType> {
    types.into_iter().fold(Some(DataType::Null), |r, c| {
        match r {
            Some(d) => find_wider_type_without_string_promotion_for_two(d, c),
            _ => None
        }
    })
}

fn find_wider_type_without_string_promotion_for_two(type1:  DataType, type2:  DataType) -> Option<DataType> {
    find_tightest_common_type(type1, type2)
}


fn find_wider_type_for_two(type1:  DataType, type2:  DataType) -> Option<DataType> {
    find_tightest_common_type(type1.clone(), type2.clone()).or_else(|| string_promotion(&type1, &type2))
}

fn string_promotion(data_type1: &DataType, data_type2: &DataType) -> Option<DataType> {
    match (data_type1, data_type2) {
        (DataType::String, t2) if t2.is_atomic_type() && (t2 != &DataType::Binary && t2 != &DataType::Boolean)  => Some(DataType::String),
        (t1, DataType::String) if t1.is_atomic_type() && (t1 != &DataType::Binary && t1 != &DataType::Boolean) => Some(DataType::String),
        _ => None
    }
}

fn has_string_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::String => true,
        DataType::Array(data_type) => has_string_type(data_type),
        _ => false
    }
}

fn implicit_cast(in_type:  &DataType, expected_type: AbstractDataType) -> Option<DataType> {
    match (in_type, expected_type) {
        (in_type, expected_type) if expected_type.accepts_type(in_type) => Some(in_type.clone()),
        (DataType::Null, expected_type) => Some(expected_type.default_concrete_type()),
        // If the function accepts any numeric type and the input is a string, we follow the hive
        // convention and cast that input into a double
        (DataType::String, AbstractDataType::Numeric) => Some(DataType::Double),
        // Implicit cast among numeric types. When we reach here, input type is not acceptable.
        // For any other numeric types, implicitly cast to each other, e.g. long -> int, int -> long
        (in_type, expected_type) if in_type.is_numeric_type() && expected_type.is_numeric_type() =>
            Some(expected_type.default_concrete_type()),
        // string类型可以隐士转换成这么多类型
        // Implicit cast from/to string
        (DataType::String, expected_type) if expected_type.is_numeric_type() => Some(expected_type.default_concrete_type()),
        (DataType::String, AbstractDataType::Type(DataType::Timestamp)) => Some(DataType::Timestamp),
        // Cast any atomic type to string.
        (in_type, AbstractDataType::Type(DataType::String)) if in_type.is_atomic_type() => Some(DataType::String),
        (in_type, AbstractDataType::Collection(dts)) =>
            dts.into_iter().find_map(|dt| implicit_cast(in_type, dt)),
        _ => None
    }
}

