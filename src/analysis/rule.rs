use std::collections::HashMap;
use crate::Result;
use std::fmt::Debug;
use crate::datetime_utils::NORM_DATETIME_FMT;
use crate::expr::*;
use crate::logical_plan::{LogicalPlan, RelationPlaceholder};
use crate::tree_node::{Transformed, TreeNode};
use crate::types::DataType;

pub trait AnalyzerRule: Debug {
    /// Rewrite `plan`
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>>;

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str;
}


#[derive(Debug)]
pub struct ResolveRelations {
    pub temp_views: HashMap<String, RelationPlaceholder>,
}

impl ResolveRelations {
    pub fn new(temp_views: HashMap<String, RelationPlaceholder>) -> Self {
        Self { temp_views }
    }
}

impl AnalyzerRule for ResolveRelations {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match &plan {
            LogicalPlan::UnresolvedRelation(ident) => {
                match self.temp_views.get(ident) {
                    Some(r) => Ok(Transformed::yes(LogicalPlan::RelationPlaceholder(r.clone()))),
                    None => Ok(Transformed::no(plan)),
                }
            },
            _ => Ok(Transformed::no(plan)),
        })
    }

    fn name(&self) -> &str {
        "ResolveRelations"
    }
}

#[derive(Debug)]
pub struct ResolveReferences;

impl ResolveReferences {
    pub fn resolve_expr(&self, expr: Expr, attr_dict: &HashMap<String, AttributeReference>) ->  Result<Transformed<Expr>> {
        expr.transform_up(|expr| {
            match &expr {
                Expr::UnresolvedAttribute(name) => {
                    match attr_dict.get(name) {
                        Some(a) => Ok(Transformed::yes(Expr::AttributeReference(AttributeReference::new_with_expr_id(
                            name.clone(), a.data_type.clone(), a.expr_id)))),
                        None =>  Ok(Transformed::no(expr)),
                    }
                },
                Expr::UnresolvedExtractValue(UnresolvedExtractValue{child, extraction}) if child.resolved() => {
                    match child.data_type() {
                        DataType::Array(_) => Ok(Transformed::yes(Expr::ScalarFunction(Box::new(GetArrayItem::new(child.clone(), extraction.clone()))))),
                        _ => {
                            Err(format!("Can't extract value from {:?}, {:?}", child, extraction))
                        }
                    }
                },
                e if e.resolved() => Ok(Transformed::no(expr)),
                e => Ok(Transformed::no(expr)),
            }
        })
    }
}

impl AnalyzerRule for ResolveReferences {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match plan {
            p if !p.children_resolved() => Ok(Transformed::no(p)),
            p => {
                //println!("");
                //println!("plan:{:?}", p);
                let attributes = p.child_attributes();
                //println!("attributes:{:?}", attributes);
                //println!("");
                let attr_dict:HashMap<String, AttributeReference> = attributes.into_iter().map(|attr| (attr.name.clone(), attr)).collect();
                let transformed = p.map_expressions(|expr| {
                    self.resolve_expr(expr, &attr_dict)
                })?;
                Ok(transformed)
            }
        })
    }

    fn name(&self) -> &str {
        "ResolveReferences"
    }
}

#[derive(Debug)]
pub struct ResolveFunctions;

impl AnalyzerRule for ResolveFunctions {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match plan {
            p if !p.children_resolved() => Ok(Transformed::no(p)),
            p => {
                let transformed = p.map_expressions(|expr| {
                    expr.transform_up(|expr| {
                        match &expr {
                            Expr::UnresolvedFunction(UnresolvedFunction{name, arguments}) => {
                                match name.to_lowercase().as_str() {
                                    "length" => {
                                        Ok(Transformed::yes(Expr::ScalarFunction(Box::new(Length::new(Box::new(arguments[0].clone()))))))
                                    },
                                    "substring" | "substr" => {
                                        Ok(Transformed::yes(Expr::ScalarFunction(Box::new(Substring::new(
                                            Box::new(arguments[0].clone()), Box::new(arguments[1].clone()), Box::new(arguments[2].clone()))))))
                                    },
                                    "concat" => {
                                        let args = arguments.into_iter().map(|arg| arg.clone()).collect();
                                        Ok(Transformed::yes(Expr::ScalarFunction(Box::new(Concat::new(args)))))
                                    },
                                    "split" => {
                                        if arguments.len() != 2 {
                                            return Err(format!("{} args not match: {:?}", name, arguments));
                                        }
                                        Ok(Transformed::yes(Expr::ScalarFunction(Box::new(StringSplit::new(
                                            Box::new(arguments[0].clone()), Box::new(arguments[1].clone()))))))
                                    },
                                    "split_part" => {
                                        if arguments.len() != 3 {
                                            return Err(format!("{} args not match: {:?}", name, arguments));
                                        }
                                        Ok(Transformed::yes(Expr::ScalarFunction(Box::new(SplitPart::new(
                                            Box::new(arguments[0].clone()), Box::new(arguments[1].clone()), Box::new(arguments[2].clone()))))))
                                    },
                                    "current_timestamp" | "now" => {
                                        if arguments.is_empty() {
                                            Ok(Transformed::yes(Expr::ScalarFunction(Box::new(CurrentTimestamp))))
                                        } else {
                                            return Err(format!("{} args not match: {:?}", name, arguments));
                                        }
                                    },
                                    "from_unixtime" => {
                                        if arguments.len() == 1 {
                                            Ok(Transformed::yes(Expr::ScalarFunction(Box::new(FromUnixTime::new(
                                                Box::new(arguments[0].clone()), Box::new(Expr::string_lit(NORM_DATETIME_FMT)))))))
                                        } else if arguments.len() == 2 {
                                            Ok(Transformed::yes(Expr::ScalarFunction(Box::new(FromUnixTime::new(
                                                Box::new(arguments[0].clone()), Box::new(arguments[1].clone()))))))
                                        } else {
                                            return Err(format!("{} args not match: {:?}", name, arguments));
                                        }
                                    },
                                    "unix_timestamp" => {
                                        if arguments.len() == 0 {
                                            Ok(Transformed::yes(Expr::ScalarFunction(Box::new(ToUnixTimestamp::new(
                                                Box::new(Expr::ScalarFunction(Box::new(CurrentTimestamp))), Box::new(Expr::string_lit(NORM_DATETIME_FMT)))))))
                                        } else if arguments.len() == 1 {
                                            Ok(Transformed::yes(Expr::ScalarFunction(Box::new(ToUnixTimestamp::new(
                                                Box::new(arguments[0].clone()), Box::new(Expr::string_lit(NORM_DATETIME_FMT)))))))
                                        }
                                        else if arguments.len() == 2 {
                                            Ok(Transformed::yes(Expr::ScalarFunction(Box::new(ToUnixTimestamp::new(
                                                Box::new(arguments[0].clone()), Box::new(arguments[1].clone()))))))
                                        } else {
                                            return Err(format!("{} args not match: {:?}", name, arguments));
                                        }
                                    },
                                    "to_unix_timestamp" => {
                                        if arguments.len() == 1 {
                                            Ok(Transformed::yes(Expr::ScalarFunction(Box::new(ToUnixTimestamp::new(
                                                Box::new(arguments[0].clone()), Box::new(Expr::string_lit(NORM_DATETIME_FMT)))))))
                                        } else if arguments.len() == 2 {
                                            Ok(Transformed::yes(Expr::ScalarFunction(Box::new(ToUnixTimestamp::new(
                                                Box::new(arguments[0].clone()), Box::new(arguments[1].clone()))))))
                                        } else {
                                            return Err(format!("{} args not match: {:?}", name, arguments));
                                        }
                                    },
                                    "if" => {
                                        Ok(Transformed::yes(Expr::ScalarFunction(Box::new(If::new(
                                            Box::new(arguments[0].clone()), Box::new(arguments[1].clone()), Box::new(arguments[2].clone()))))))
                                    },
                                    "nvl" => {
                                        if arguments.len() == 2 {
                                            Ok(Transformed::yes(Expr::ScalarFunction(Box::new(Coalesce::new(arguments.clone())))))
                                        } else {
                                            return Err(format!("{} args not match: {:?}", name, arguments));
                                        }
                                    },
                                    "coalesce" => {
                                        if arguments.len() >= 1 {
                                            Ok(Transformed::yes(Expr::ScalarFunction(Box::new(Coalesce::new(arguments.clone())))))
                                        } else {
                                            return Err(format!("{} args not match: {:?}", name, arguments));
                                        }
                                    },
                                    _ => Err(format!("UnresolvedFunction: {}", name))
                                }

                            },
                            e if e.resolved() => Ok(Transformed::no(expr)),
                            e => Ok(Transformed::no(expr)),
                        }
                    })
                })?;
                Ok(transformed)
            }
        })
    }

    fn name(&self) -> &str {
        "ResolveFunctions"
    }
}