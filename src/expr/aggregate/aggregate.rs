use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::mem;
use std::sync::Mutex;
use itertools::Itertools;
use crate::Result;
use crate::data::{GenericRow, Row, Value};
use crate::expr::{AttributeReference, Expr};
use crate::types::{AbstractDataType, DataType};

pub trait DeclarativeAggFunction: Debug + Send + Sync + CreateDeclarativeAggFunction + ExtendDeclarativeAggFunction {
    fn name(&self) -> &str;
    fn data_type(&self) -> &DataType;
    fn agg_buffer_attributes(&self) -> Vec<AttributeReference>;
    fn input_agg_buffer_attributes(&self) -> Vec<AttributeReference>;
    fn result_attribute(&self) -> AttributeReference;
    fn initial_values(&self) -> Vec<Expr>;
    fn update_expressions(&self) -> Vec<Expr>;
    fn merge_expressions(&self) -> Vec<Expr>;
    fn evaluate_expression(&self) -> Expr;
    fn args(&self) -> Vec<&Expr>;

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        None
    }
    fn check_input_data_types(&self) ->Result<()> {
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

    fn sql(&self) -> String {
        format!("{}({})", self.name(), self.args().into_iter().map(|arg| arg.sql()).join(", "))
    }
}

pub trait CreateDeclarativeAggFunction {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn DeclarativeAggFunction>> where Self: Sized;
    fn create_function_expr(args: Vec<Expr>) -> Result<Expr> where Self: Sized {
        Ok(Expr::DeclarativeAggFunction(Self::from_args(args)?))
    }
}

pub trait ExtendDeclarativeAggFunction {
    fn clone_box(&self) -> Box<dyn DeclarativeAggFunction>;
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn DeclarativeAggFunction>;
}

impl<T: DeclarativeAggFunction + Clone + 'static> ExtendDeclarativeAggFunction for T {
    fn clone_box(&self) -> Box<dyn DeclarativeAggFunction> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn DeclarativeAggFunction> {
        Self::from_args(args).unwrap()
    }
}

impl Clone for Box<dyn DeclarativeAggFunction> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl PartialEq for Box<dyn DeclarativeAggFunction> {
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

impl Eq for Box<dyn DeclarativeAggFunction> {}

impl PartialOrd for Box<dyn DeclarativeAggFunction> {
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

impl Hash for Box<dyn DeclarativeAggFunction> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for x in self.args() {
            x.hash(state);
        }
    }
}

pub struct TypedAggAttr {
    data_type: DataType,
    buf: Mutex<Option<AttributeReference>>,
    input_agg_attrs: Mutex<Vec<AttributeReference>>,
    result_attr: Mutex<Option<AttributeReference>>,
}

impl TypedAggAttr {
    pub fn new(data_type: DataType) -> Self {
        TypedAggAttr {
            data_type,
            buf: Mutex::new(None),
            input_agg_attrs: Mutex::new(vec![]),
            result_attr: Mutex::new(None),
        }
    }

    pub fn buf_attr(&self) -> AttributeReference {
        let mut buf_guard = self.buf.lock().unwrap();
        if buf_guard.is_none() {
            *buf_guard = Some(AttributeReference::new("buf", self.data_type.clone()));
        }
        buf_guard.as_ref().unwrap().clone()
    }

    pub fn input_agg_attrs(&self) -> Vec<AttributeReference> {
        let mut input_agg_attrs_guard = self.input_agg_attrs.lock().unwrap();
        if input_agg_attrs_guard.is_empty() {
            *input_agg_attrs_guard = vec![self.buf_attr().new_instance()];
        }
        input_agg_attrs_guard.clone()
    }

    pub fn result_attr(&self) -> AttributeReference {
        let mut result_attr_attr_guard = self.result_attr.lock().unwrap();
        if result_attr_attr_guard.is_none() {
            *result_attr_attr_guard = Some(AttributeReference::new("buf", self.data_type.clone()));
        }
        result_attr_attr_guard.as_ref().unwrap().clone()
    }
}

impl Debug for TypedAggAttr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedAggFunction")
            .field("data_type", &self.data_type)
            .field("buf", &self.buf.lock().unwrap())
            .field("input_agg_attrs", &self.input_agg_attrs.lock().unwrap())
            .field("result_attr", &self.result_attr.lock().unwrap())
            .finish()
    }
}

impl Clone for TypedAggAttr {
    fn clone(&self) -> Self {
        TypedAggAttr {
            data_type: self.data_type.clone(),
            buf: Mutex::new(self.buf.lock().unwrap().clone()),
            input_agg_attrs: Mutex::new(self.input_agg_attrs.lock().unwrap().clone()),
            result_attr: Mutex::new(self.result_attr.lock().unwrap().clone()),
        }
    }
}

pub trait TypedAggFunction: Debug + Send + Sync + CreateTypedAggFunction + ExtendTypedAggFunction {
    fn name(&self) -> &str;
    fn data_type(&self) -> &DataType;
    fn with_new_mutable_agg_buffer_offset(&self, offset: usize) -> Box<dyn TypedAggFunction>;
    fn agg_attr(&self) -> &TypedAggAttr;
    fn agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        vec![self.agg_attr().buf_attr()]
    }

    fn input_agg_buffer_attributes(&self) -> Vec<AttributeReference> {
        self.agg_attr().input_agg_attrs()
    }

    fn result_attribute(&self) -> AttributeReference {
        self.agg_attr().result_attr()
    }
    fn physical_function(&self) -> Result<Box<dyn PhysicalTypedAggFunction>>;

    fn args(&self) -> Vec<&Expr>;

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        None
    }
    fn check_input_data_types(&self) -> crate::Result<()> {
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

    fn sql(&self) -> String {
        format!("{}({})", self.name(), self.args().into_iter().map(|arg| arg.sql()).join(", "))
    }
}

pub trait CreateTypedAggFunction {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn TypedAggFunction>> where Self: Sized;
    fn create_function_expr(args: Vec<Expr>) -> Result<Expr> where Self: Sized {
        Ok(Expr::TypedAggFunction(Self::from_args(args)?))
    }
}

pub trait ExtendTypedAggFunction {
    fn clone_box(&self) -> Box<dyn TypedAggFunction>;
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn TypedAggFunction>;
}

impl<T: TypedAggFunction + Clone + 'static> ExtendTypedAggFunction for T {
    fn clone_box(&self) -> Box<dyn TypedAggFunction> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn rewrite_args(&self, args: Vec<Expr>) -> Box<dyn TypedAggFunction> {
        Self::from_args(args).unwrap()
    }
}

impl Clone for Box<dyn TypedAggFunction> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

impl PartialEq for Box<dyn TypedAggFunction> {
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

impl Eq for Box<dyn TypedAggFunction> {}

impl PartialOrd for Box<dyn TypedAggFunction> {
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

impl Hash for Box<dyn TypedAggFunction> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for x in self.args() {
            x.hash(state);
        }
    }
}

pub trait PhysicalTypedAggFunction {
    fn data_type(&self) -> &DataType;
    fn mutable_agg_buffer_offset(&self) -> usize;
    fn input_agg_buffer_offset(&self) -> usize;
    fn create_agg_buffer(&self) -> Value;
    fn update_value(&self, buffer: &mut Value, input: &dyn Row);
    fn merge_value(&self, buffer: &mut Value, input: Value);
    fn eval_value(&self, buffer: Value) -> Value;

    fn initialize(&self, buffer: &mut GenericRow) {
        buffer.update(self.mutable_agg_buffer_offset(), self.create_agg_buffer());
    }
    fn update(&self, buffer: &mut GenericRow, input: &dyn Row){
        self.update_value(buffer.get_mut(self.mutable_agg_buffer_offset()), input);
    }
    fn merge(&self, buffer: &mut GenericRow, input: &dyn Row){
    }
    fn eval(&self, buffer: &mut GenericRow) -> Value {
        self.eval_value(mem::replace(buffer.get_mut(self.mutable_agg_buffer_offset()), Value::Null))
    }
}
