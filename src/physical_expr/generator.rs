use std::fmt::Debug;
use crate::data::{GenericRow, Row, Value};
use crate::physical_expr::PhysicalExpr;

pub trait PhysicalGenerator: Debug {
    fn generate(&mut self, input: &dyn Row) -> &[GenericRow];
}

#[derive(Debug)]
pub struct Explode {
    pub child: Box<dyn PhysicalExpr>,
    pub rows: Vec<GenericRow>,
}

impl Explode {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        let rows = Vec::new();
        Explode { child, rows}
    }
}

impl PhysicalGenerator for Explode {
    fn generate(&mut self, input: &dyn Row) -> &[GenericRow]{
        let value = self.child.eval(input);
        if value.is_null() {
            return &self.rows[..0];
        }
        let array = value.get_array();
        if self.rows.len() >= array.len() {
            if self.rows.len() > 100 && array.len() <= 100 {
                self.rows.truncate(100);
            }
        } else {
            for _ in self.rows.len()..array.len() {
                self.rows.push(GenericRow::new_with_size(1));
            }
        }

        for (i, value) in array.iter().enumerate() {
            self.rows[i].update(0, value.clone());
        }

        &self.rows[ ..array.len()]
    }
}

#[derive(Debug)]
pub struct PathFileUnroll {
    pub path: Box<dyn PhysicalExpr>,
    pub file: Box<dyn PhysicalExpr>,
    pub sep: char,
    pub rows: Vec<GenericRow>,
}

impl PathFileUnroll {
    pub fn new(path: Box<dyn PhysicalExpr>, file: Box<dyn PhysicalExpr>, sep: char,) -> Self {
        let rows = Vec::new();
        Self { path, file, sep, rows}
    }
}

impl PhysicalGenerator for PathFileUnroll {
    fn generate(&mut self, input: &dyn Row) -> &[GenericRow]{
        let sep = self.sep;
        let p = self.path.eval(input);
        if p.is_null() {
            return &self.rows[..0];
        }
        let p = p.get_string();
        let path = if p.ends_with(sep) {
            &p[..p.len() - 1]
        } else {
            p
        };
        if p.is_empty() {
            return &self.rows[..0];
        }
        let file = self.file.eval(input);
        let has_file = !file.is_null();
        let (has_file, file) = if file.is_null(){
            (false, "")
        } else {
            let file = file.get_string();
            (!file.is_empty(), file)
        };
        let path_contains_file = has_file && path.ends_with(file);

        if self.rows.len() > 100 {
            self.rows = Vec::new();
        } else {
            self.rows.clear();
        }

        // 拆分路径并打印子路径
        for (i, c) in path.char_indices() {
            if c == sep && i > 0 {
                self.rows.push(GenericRow::new(vec![Value::string(&path[..i]), Value::null()]));
            }
        }

        if !has_file {
            self.rows.push(GenericRow::new(vec![Value::string(path), Value::null()]));
        } else {
            if path_contains_file {
                self.rows.push(GenericRow::new(vec![Value::string(path), Value::string(file)]));
            } else{
                self.rows.push(GenericRow::new(vec![Value::string(path), Value::null()]));
                // 输出path + file
                self.rows.push(GenericRow::new(vec![Value::string(format!("{}{}{}", path, sep, file)), Value::string(file)]));
            }
        }

        &self.rows[..]
    }
}

