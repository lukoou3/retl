use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;
use itertools::Itertools;
use crate::Result;
use crate::expr::{create_physical_expr, Expr, Literal};
use crate::physical_expr::{self as phy, PhysicalGenerator};
use crate::types::{AbstractDataType, DataType, Field, Schema};

pub trait Generator: Debug + Send + Sync + CreateGenerator + ExtendGenerator {
    fn name(&self) -> &str;

    fn element_schema(&self) -> Schema;

    // DataType::Array(Box::new(self.element_schema().to_struct_type()))
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

    fn physical_generator(&self) -> Result<Box<dyn PhysicalGenerator>>;
}

pub trait CreateGenerator {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn Generator>> where Self: Sized;

    fn create_function_expr(args: Vec<Expr>) -> Result<Expr> where Self: Sized {
        Ok(Expr::Generator(Self::from_args(args)?))
    }
}

pub trait ExtendGenerator {
    fn clone_box(&self) -> Box<dyn Generator>;
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn Generator>;
}

impl<T: Generator + Clone + 'static> ExtendGenerator for T {
    fn clone_box(&self) -> Box<dyn Generator> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn Generator> {
        Self::from_args(args).unwrap()
    }
}

impl Clone for Box<dyn Generator> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl PartialEq for Box<dyn Generator> {
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

impl Eq for Box<dyn Generator> {}

impl PartialOrd for Box<dyn Generator> {
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

impl Hash for Box<dyn Generator> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for x in self.args() {
            x.hash(state);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Explode {
    pub child: Box<Expr>,
    pub element_schema: Schema,
    pub data_type: DataType,
}

impl Explode {
    pub fn new(child: Box<Expr>) -> Self {
        let tp = if child.resolved() {
            match child.data_type() {
                DataType::Array(t) => t.as_ref().clone(),
                _ => DataType::Null
            }
        } else {
            DataType::Null
        };
        let fields = vec![Field::new("item", tp.clone())];
        let element_schema = Schema::new(fields);
        let data_type = DataType::Array(Box::new(element_schema.to_struct_type()));
        Self { child, element_schema, data_type }
    }
}

impl CreateGenerator for Explode {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn Generator>> {
        if args.len() != 1 {
            return Err(format!("requires 1 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(child))))
    }
}

impl Generator for Explode {
    fn name(&self) -> &str {
        "explode"
    }

    fn element_schema(&self) -> Schema {
        self.element_schema.clone()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child]
    }

    fn physical_generator(&self) -> Result<Box<dyn PhysicalGenerator>> {
        let child = create_physical_expr(self.child.as_ref())?;
        Ok(Box::new(phy::Explode::new(child)))
    }
}

#[derive(Debug, Clone)]
pub struct PathFileUnroll {
    pub path: Box<Expr>,
    pub file: Box<Expr>,
    pub sep: Box<Expr>,
    pub element_schema: Schema,
    pub data_type: DataType,
}

impl PathFileUnroll {
    pub fn new(path: Box<Expr>, file: Box<Expr>, sep: Box<Expr>) -> Self {
        let fields = vec![Field::new("path", DataType::String), Field::new("file", DataType::String)];
        let element_schema = Schema::new(fields);
        let data_type = DataType::Array(Box::new(element_schema.to_struct_type()));
        Self { path, file, sep, element_schema, data_type }
    }
}

impl CreateGenerator for PathFileUnroll {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn Generator>> {
        if args.len() != 3 {
            return Err(format!("requires 3 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let path = iter.next().unwrap();
        let file = iter.next().unwrap();
        let sep = iter.next().unwrap();
        match sep {
            Expr::Literal(Literal{value, data_type}) if data_type == DataType::String => {
                let sep = value.get_string();
                if sep.chars().count() == 1 {
                    Ok(Box::new(Self::new(Box::new(path), Box::new(file), Box::new(Expr::Literal(Literal{value, data_type})))))
                } else {
                    Err("The third argument should be a String literal and contains one char".to_string())
                }
            },
            _ => Err("The third argument should be a String literal and contains one char".to_string())
        }
    }
}

impl Generator for PathFileUnroll {
    fn name(&self) -> &str {
        "path_file_unroll"
    }

    fn element_schema(&self) -> Schema {
        self.element_schema.clone()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.path, &self.file, &self.sep]
    }

    fn physical_generator(&self) -> Result<Box<dyn PhysicalGenerator>> {
        let path = create_physical_expr(self.path.as_ref())?;
        let file = create_physical_expr(self.file.as_ref())?;
        if let Expr::Literal(Literal{value, ..}) = self.sep.as_ref() {
            Ok(Box::new(phy::PathFileUnroll::new(path, file, value.get_string().chars().next().unwrap())))
        } else {
            Err("The third argument should be a String literal and contains one char".to_string())
        }
    }
}