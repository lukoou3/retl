use std::collections::HashMap;
use crate::Result;
use std::fmt::Debug;
use itertools::Itertools;
use crate::analysis::lookup_function;
use crate::expr::*;
use crate::logical_plan::{Aggregate, Generate, LogicalPlan, Project, RelationPlaceholder};
use crate::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
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
    fn match_qualifier_parts(&self,name_parts: &Vec<String>, dict: &HashMap<String, Vec<AttributeReference>>, qualified: &HashMap<(String, String), Vec<AttributeReference>>) -> (Vec<AttributeReference>, Vec<String>) {
        if name_parts.len() >= 2 {
            let name = &name_parts[1];
            let key = (name_parts[0].to_lowercase(), name.to_lowercase());
            if let Some(a) = qualified.get(&key) {
                return (a.iter().map(|a| a.with_name(name.clone())).collect(), name_parts[2..].iter().map(|a| a.clone()).collect());
            }
        }
        let name = &name_parts[0];
        let key = name.to_lowercase();
        let nested_fields = &name_parts[1..];
        match dict.get(&key) {
            Some(a) => {
                (a.iter().map(|a| a.with_name(name.clone())).collect(), nested_fields.iter().map(|a| a.clone()).collect())
            },
            None => (Vec::new(), Vec::new()),
        }
    }

    pub fn resolve_expr(&self, expr: Expr, dict: &HashMap<String, Vec<AttributeReference>>, qualified: &HashMap<(String, String), Vec<AttributeReference>>) ->  Result<Transformed<Expr>> {
        expr.transform_up(|expr| {
            match expr {
                Expr::UnresolvedAttribute(name_parts) => {
                    let (candidates, nested_fields) = self.match_qualifier_parts(&name_parts, dict, qualified);
                    if candidates.len() > 1 {
                        let reference_names = candidates.into_iter().map(|a| {
                            if a.qualifier.is_empty() {
                                a.name.clone()
                            } else {
                                a.qualifier.iter().chain(vec![&a.name].into_iter()).join(".")
                            }
                        }).join(", ");
                        Err(format!("Reference '{}' is ambiguous, could be: {}", name_parts.iter().join("."), reference_names))
                    } else if candidates.len() == 0 {
                        Ok(Transformed::no(Expr::UnresolvedAttribute(name_parts)))
                    } else{
                        let mut e = Expr::AttributeReference(candidates.into_iter().next().unwrap());
                        if nested_fields.is_empty() {
                            Ok(Transformed::yes(e))
                        } else {
                            for nested_field in &nested_fields {
                                e = extract_value(e, Expr::string_lit(nested_field))?;
                            }
                            Ok(Transformed::yes(e.alias(nested_fields.last().unwrap().clone())))
                        }
                    }
                },
                Expr::UnresolvedExtractValue(UnresolvedExtractValue{child, extraction}) if child.resolved() => {
                    extract_value(*child, *extraction).map(|e| Transformed::yes(e))
                },
                e if e.resolved() => Ok(Transformed::no(e)),
                e => Ok(Transformed::no(e)),
            }
        })
    }
}

fn extract_value(child: Expr, extraction: Expr) -> Result<Expr> {
    match child.data_type() {
        DataType::Array(_) => Ok(Expr::ScalarFunction(Box::new(GetArrayItem::new(Box::new(child), Box::new(extraction))))),
        DataType::Struct(fields) => match &extraction {
            Expr::Literal(Literal{value, data_type}) if data_type == DataType::string_type() && !value.is_null() => {
                let name = value.get_string();
                let idx = fields.0.iter().position(|f| f.name == name);
                if let Some(ordinal) = idx {
                    let f = GetStructField::new(Box::new(child), Box::new(Expr::int_lit(ordinal as i32)))?;
                    Ok(Expr::ScalarFunction(Box::new(f)))
                } else {
                    Err(format!("Can't find field {} in {}", name, child.data_type()))
                }
            },
            _ => {
                Err(format!("Field name should be String Literal, but it's {:?}", extraction))
            }
        },
        _ => {
            Err(format!("Can't extract value from {:?}, {:?}", child, extraction))
        }
    }
}

impl AnalyzerRule for ResolveReferences {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match plan {
            p if !p.children_resolved() => Ok(Transformed::no(p)),
            LogicalPlan::Generate(g) if g.generator.resolved() => Ok(Transformed::no(LogicalPlan::Generate(g))),
            LogicalPlan::Generate(Generate{generator, unrequired_child_index, outer, qualifier, generator_output, child}) => {
                let attributes = child.output();
                let mut dict = HashMap::new();
                let mut qualified = HashMap::new();
                for attr in attributes {
                    if attr.qualifier.len() > 0 {
                        qualified.entry((attr.qualifier.last().unwrap().to_lowercase(), attr.name.to_lowercase())).or_insert_with(Vec::new).push(attr.clone());
                    }
                    dict.entry(attr.name.to_lowercase()).or_insert_with(Vec::new).push(attr);
                }
                let g = self.resolve_expr(generator, &dict, &qualified)?;
                Ok(g.update_data(|generator| LogicalPlan::Generate(Generate{generator, unrequired_child_index, outer, qualifier, generator_output, child})))
            },
            p => {
                //println!("");
                //println!("plan:{:?}", p);
                let attributes = p.child_attributes();
                //println!("attributes:{:?}", attributes);
                let mut dict = HashMap::new();
                let mut qualified = HashMap::new();
                for attr in attributes {
                    if attr.qualifier.len() > 0 {
                        qualified.entry((attr.qualifier.last().unwrap().to_lowercase(), attr.name.to_lowercase())).or_insert_with(Vec::new).push(attr.clone());
                    }
                    dict.entry(attr.name.to_lowercase()).or_insert_with(Vec::new).push(attr);
                }
                //println!("dict:{:?}", dict);
                //println!("qualified:{:?}", qualified);
                let transformed = p.map_expressions(|expr| {
                    self.resolve_expr(expr, &dict, &qualified)
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
pub struct ResolveGenerate;

impl AnalyzerRule for ResolveGenerate {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match plan {
            LogicalPlan::Generate(g) if !g.child.resolved() || !g.generator.resolved() =>
                Ok(Transformed::no(LogicalPlan::Generate(g))),
            LogicalPlan::Generate(mut generate) if !generate.resolved() => {
                if let Expr::Generator(g) = &generate.generator {
                    let element_attrs = g.element_schema().to_attributes();
                    let mut names = Vec::new();
                    for e in &generate.generator_output {
                        if let Expr::UnresolvedAttribute(name_parts) = e {
                            names.push(name_parts.iter().join("."));
                        } else {
                            return Err(format!("generator output is not unresolvedAttribute {:?}", e));
                        }
                    }
                    let generator_output = if names.len() == element_attrs.len() {
                        names.into_iter().zip(element_attrs.into_iter()).map(|(name, attr)| Expr::AttributeReference(attr.with_name(name))).collect()
                    } else if names.len() == 0 {
                        element_attrs.into_iter().map(|attr| Expr::AttributeReference(attr)).collect()
                    } else {
                        return Err(format!("The number of aliases supplied in the AS clause does not match the number of columns output by the UDTF expected {} aliases but got {}",
                                           element_attrs.len(), names.iter().join(", ")));
                    };
                    generate.generator_output = generator_output;
                    Ok(Transformed::yes(LogicalPlan::Generate(generate)))
                } else {
                    return Ok(Transformed::no(LogicalPlan::Generate(generate)));
                }
            },
            p => Ok(Transformed::no(p)),
        })
    }

    fn name(&self) -> &str {
        "ResolveGenerate"
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
                                match lookup_function(name, arguments.clone()) {
                                    Ok(e) => Ok(Transformed::yes(e)),
                                    Err(e) => Err(e)
                                }
                            },
                            Expr::UnresolvedGenerator(UnresolvedGenerator{name, arguments}) => {
                                match lookup_function(name, arguments.clone()) {
                                    Ok(e) => match &e {
                                        Expr::Generator(_) => Ok(Transformed::yes(e)),
                                        _ => Err(format!("{}  is expected to be a generator. However, it is {:?}", name, e))
                                    },
                                    Err(e) => Err(e)
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

#[derive(Debug)]
pub struct ResolveAliases;

impl ResolveAliases {
    fn has_unresolved_alias(exprs: &Vec<Expr>) -> bool {
        exprs.iter().any(|expr| match expr {
            Expr::UnresolvedAlias(_) => true,
            _ => false
        })
    }

    fn assign_aliases(exprs: Vec<Expr>) -> Vec<Expr> {
        exprs.into_iter().map(|e|  e.transform_up(|expr| {
            let e= match expr {
                Expr::UnresolvedAlias(u) => match u.as_ref() {
                    Expr::Alias(_) | Expr::UnresolvedAlias(_) | Expr::AttributeReference(_) | Expr::UnresolvedAttribute(_) => *u,
                    e if !e.resolved() => Expr::UnresolvedAlias(u),
                    Expr::ScalarFunction(func) if func.as_any().downcast_ref::<GetStructField>().is_some() => {
                        let any = func.as_any();
                        let f = any.downcast_ref::<GetStructField>().unwrap();
                        let name = f.field_name().to_string();
                        u.alias(name)
                    },
                    Expr::Cast(Cast{child, ..}) => match child.as_ref() {
                        Expr::Alias(Alias{name, ..}) => {
                            let name = name.clone();
                            u.alias(name)
                        },
                        Expr::AttributeReference(AttributeReference{name, ..}) => {
                            let name = name.clone();
                            u.alias(name)
                        },
                        Expr::ScalarFunction(func) if func.as_any().downcast_ref::<GetStructField>().is_some() => {
                            let any = func.as_any();
                            let f = any.downcast_ref::<GetStructField>().unwrap();
                            let name = f.field_name().to_string();
                            u.alias(name)
                        },
                        _ => {
                            let sql = u.sql();
                            u.alias(sql)
                        },
                    },
                    _ => {
                        let sql = u.sql();
                        u.alias(sql)
                    },
                },
                e => e,
            };
            Ok(Transformed::yes(e))
        }).unwrap().data).collect()
    }
 }

impl AnalyzerRule for ResolveAliases {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match plan {
            LogicalPlan::Project(Project{project_list,child})
                if child.resolved() && Self::has_unresolved_alias(&project_list)=> {
                Ok(Transformed::yes(LogicalPlan::Project(Project{
                    project_list: Self::assign_aliases(project_list),
                    child
                })))
            },
            LogicalPlan::Aggregate(Aggregate{grouping_exprs, aggregate_exprs, child})
                if child.resolved() && Self::has_unresolved_alias(&aggregate_exprs) => {
                Ok(Transformed::yes(LogicalPlan::Aggregate(Aggregate{
                    grouping_exprs,
                    aggregate_exprs: Self::assign_aliases(aggregate_exprs),
                    child
                })))
            }
            p => Ok(Transformed::no(p)),
        })
    }

    fn name(&self) -> &str {
        "ResolveAliases"
    }
}


#[derive(Debug)]
pub struct GlobalAggregates;

impl GlobalAggregates {
    pub fn contains_aggregates(expr: &Expr) -> bool {
        let mut contains = false;
        expr.apply(|expr| {
            match expr {
                Expr::DeclarativeAggFunction(_) | Expr::TypedAggFunction(_) => {
                    contains = true;
                    Ok(TreeNodeRecursion::Stop)
                },
                _ => Ok(TreeNodeRecursion::Continue),
            }
        }).unwrap();
        contains
    }


}

impl AnalyzerRule for GlobalAggregates {
    fn analyze(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match &plan {
            LogicalPlan::Project(Project{project_list,child})
                if project_list.into_iter().any(|e| Self::contains_aggregates(e)) => {
                Ok(Transformed::yes(LogicalPlan::Aggregate(Aggregate::new(vec![], project_list.clone(), child.clone()))))
            },
            _ => Ok(Transformed::no(plan)),
        })
    }

    fn name(&self) -> &str {
        "GlobalAggregates"
    }
}
