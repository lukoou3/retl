use std::any::Any;
use std::cell::RefCell;
use std::ops::DerefMut;
use std::sync::Arc;
use jsonpath_rust::JsonPath;
use jsonpath_rust::parser::model::JpQuery;
use jsonpath_rust::parser::parse_json_path;
use jsonpath_rust::query::js_path_process;
use serde::Deserializer;
use serde_json::Value as JValue;
use crate::codecs::json::RowVisitor;
use crate::data::{empty_row, Row, Value};
use crate::physical_expr::{Literal, PhysicalExpr};
use crate::types::{DataType, Schema};

#[derive(Debug)]
pub struct GetJsonObject {
    json: Box<dyn PhysicalExpr>,
    path: Box<dyn PhysicalExpr>,
    jp_query: JpQuery,
    path_foldable: bool,
}

impl GetJsonObject {
    pub fn new(json: Box<dyn PhysicalExpr>, path: Box<dyn PhysicalExpr>) -> Self {
        let (jp_query, path_foldable) = if let Some(literal) = path.as_any().downcast_ref::<Literal>() {
            let value = literal.eval(empty_row());
            if value.is_null() {
                (JpQuery::new(Vec::new()), true)
            } else {
                match parse_json_path(value.get_string()) {
                    Ok(json_paths) => (json_paths, true),
                    Err(_) => (JpQuery::new(Vec::new()), true)
                }
            }
        } else {
            (JpQuery::new(Vec::new()), false)
        };
        Self {json, path, jp_query, path_foldable}
    }

    fn eval_json_path(json: Value, jp_query: &JpQuery) -> Value {
        match serde_json::from_str::<JValue>(json.get_string()) {
            Ok(value) => {
                match js_path_process(&jp_query, &value) {
                    Ok(datas) => {
                        let v= datas.into_iter() .map(|r| r.val()).collect::<Vec<_>>();
                        if v.is_empty() {
                            Value::empty_string()
                        } else if v.len() == 1 {
                            match v[0] {
                                JValue::Null => Value::Null,
                                JValue::String(s) => Value::String(Arc::new(s.clone())),
                                v => match serde_json::to_string(v) {
                                    Ok(s) => Value::String(Arc::new(s)),
                                    Err(_) => Value::empty_string()
                                },
                            }

                        } else {
                            match serde_json::to_string(&v) {
                                Ok(s) => Value::String(Arc::new(s)),
                                Err(_) => Value::empty_string()
                            }
                        }
                    },
                    Err(_) => Value::empty_string()
                }
            },
            Err(_) => Value::empty_string()
        }
    }
}

impl PhysicalExpr for GetJsonObject {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let json = self.json.eval(input);
        if json.is_null() {
            return Value::Null;
        }
        let path = self.path.eval(input);
        if path.is_null() {
            return Value::Null
        }
        if self.path_foldable {
            if self.jp_query.segments.is_empty() {
                Value::empty_string()
            } else {
                Self::eval_json_path(json, &self.jp_query)
            }
        } else {
            match parse_json_path(path.get_string()) {
                Ok(jp_query) => Self::eval_json_path(json, &jp_query),
                Err(_) => Value::empty_string(),
            }
        }
    }
}

#[derive(Debug)]
pub struct GetJsonInt {
    json: Box<dyn PhysicalExpr>,
    path: Box<dyn PhysicalExpr>,
    jp_query: JpQuery,
    path_foldable: bool,
}

impl GetJsonInt {
    pub fn new(json: Box<dyn PhysicalExpr>, path: Box<dyn PhysicalExpr>) -> Self {
        let (jp_query, path_foldable) = if let Some(literal) = path.as_any().downcast_ref::<Literal>() {
            let value = literal.eval(empty_row());
            if value.is_null() {
                (JpQuery::new(Vec::new()), true)
            } else {
                match parse_json_path(value.get_string()) {
                    Ok(json_paths) => (json_paths, true),
                    Err(_) => (JpQuery::new(Vec::new()), true)
                }
            }
        } else {
            (JpQuery::new(Vec::new()), false)
        };
        Self {json, path, jp_query, path_foldable}
    }

    fn eval_json_path(json: Value, jp_query: &JpQuery) -> Value {
        match serde_json::from_str::<JValue>(json.get_string()) {
            Ok(value) => {
                match js_path_process(&jp_query, &value) {
                    Ok(datas) => {
                        let v= datas.into_iter() .map(|r| r.val()).collect::<Vec<_>>();
                        if v.is_empty() {
                            Value::Null
                        } else if v.len() == 1 {
                            match v[0] {
                                JValue::Null => Value::Null,
                                JValue::Number(n) => if n.is_f64() {
                                    Value::Long(n.as_f64().unwrap() as i64)
                                } else {
                                    Value::Long(n.as_i64().unwrap())
                                },
                                JValue::String(s) => {
                                    match s.parse::<i64>() {
                                        Ok(n) => Value::Long(n),
                                        Err(_) => Value::Null
                                    }
                                },
                                v => Value::Null,
                            }
                        } else {
                            Value::Null
                        }
                    },
                    Err(_) => Value::Null
                }
            },
            Err(_) => Value::Null
        }
    }
}

impl PhysicalExpr for GetJsonInt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Long
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let json = self.json.eval(input);
        if json.is_null() {
            return Value::Null;
        }
        let path = self.path.eval(input);
        if path.is_null() {
            return Value::Null
        }
        if self.path_foldable {
            if self.jp_query.segments.is_empty() {
                Value::Null
            } else {
                Self::eval_json_path(json, &self.jp_query)
            }
        } else {
            match parse_json_path(path.get_string()) {
                Ok(jp_query) => Self::eval_json_path(json, &jp_query),
                Err(_) => Value::Null,
            }
        }
    }
}

#[derive(Debug)]
pub struct JsonToStructs {
    json: Box<dyn PhysicalExpr>,
    schema: Schema,
    row_visitor: RefCell<RowVisitor>,
}

impl JsonToStructs {
    pub fn new(json: Box<dyn PhysicalExpr>, schema: Schema) -> Self {
        let row_visitor = RefCell::new(RowVisitor::new(schema.fields.clone()));
        Self {json, schema, row_visitor}
    }
}

impl PhysicalExpr for JsonToStructs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        self.schema.to_struct_type()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let json = self.json.eval(input);
        match json {
            Value::String(s) => {
                let mut row_visitor_ref = self.row_visitor.borrow_mut();
                let row_visitor = row_visitor_ref.deref_mut();
                row_visitor.row.fill_null();
                let mut de = serde_json::Deserializer::from_str(s.as_str());
                match de.deserialize_map(&mut *row_visitor) {
                    Ok(_) => Value::Struct(Arc::new(row_visitor.row.clone())),
                    Err(_) => Value::Null,
                }
            },
            Value::Binary(b) => {
                let mut row_visitor_ref = self.row_visitor.borrow_mut();
                let row_visitor = row_visitor_ref.deref_mut();
                row_visitor.row.fill_null();
                let mut de = serde_json::Deserializer::from_slice(b.as_slice());
                match de.deserialize_map(&mut *row_visitor) {
                    Ok(_) => Value::Struct(Arc::new(row_visitor.row.clone())),
                    Err(_) => Value::Null,
                }
            },
            _ => Value::Null,
        }
    }
}

/*#[derive(Debug, Clone)]
enum PathComponent {
    ObjectKey(String),
    ArrayIndex(usize),
}

fn parse_json_path1(s: &str) -> Result<Vec<PathComponent>> {
    if !s.starts_with('$') {
        return Err("Path must start with '$'".into());
    }

    let parts = s.split('.');
    let mut components = Vec::new();

    for part in parts {
        if part.contains('[') && part.ends_with(']') {
            let bracket_pos = part.find('[').ok_or("Invalid array index".to_string())?;
            let key = &part[..bracket_pos];
            let index_str = &part[bracket_pos + 1..part.len() - 1];
            let index = index_str.parse::<usize>().map_err(|_| "Invalid array index".to_string())?;
            if !key.is_empty() {
                components.push(PathComponent::ObjectKey(key.to_string()));
            }
            components.push(PathComponent::ArrayIndex(index));
        } else {
            components.push(PathComponent::ObjectKey(part.to_string()));
        }
    }

    Ok(components)
}*/