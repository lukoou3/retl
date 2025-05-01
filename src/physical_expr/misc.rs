use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::encrypt::{aes_decrypt, aes_encrypt};
use crate::physical_expr::{PhysicalExpr, TernaryExpr};
use crate::types::DataType;

#[derive(Debug)]
pub struct AesEncrypt {
    input: Box<dyn PhysicalExpr>,
    key: Box<dyn PhysicalExpr>,
    iv: Box<dyn PhysicalExpr>,
}

impl AesEncrypt {
    pub fn new(input: Box<dyn PhysicalExpr>, key: Box<dyn PhysicalExpr>, iv: Box<dyn PhysicalExpr>) -> Self {
        Self { input, key, iv }
    }
}

impl TernaryExpr for AesEncrypt {
    fn child1(&self) -> &dyn PhysicalExpr {
        self.input.as_ref()
    }
    fn child2(&self) -> &dyn PhysicalExpr {
        self.key.as_ref()
    }
    fn child3(&self) -> &dyn PhysicalExpr {
        self.iv.as_ref()
    }
    fn null_safe_eval(&self, input: Value, key: Value, iv: Value) -> Value {
        let input = input.get_binary();
        let key = key.get_binary();
        let iv = iv.get_binary();
        match aes_encrypt(input.as_slice(), key.as_slice(), iv.as_slice()) {
            Ok(v) => Value::Binary(Arc::new(v)),
            Err(_) => Value::Null,
        }
    }
}

impl PhysicalExpr for AesEncrypt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Binary
    }

    fn eval(&self, input: &dyn Row) -> Value {
        TernaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct AesDecrypt {
    input: Box<dyn PhysicalExpr>,
    key: Box<dyn PhysicalExpr>,
    iv: Box<dyn PhysicalExpr>,
}

impl AesDecrypt {
    pub fn new(input: Box<dyn PhysicalExpr>, key: Box<dyn PhysicalExpr>, iv: Box<dyn PhysicalExpr>) -> Self {
        Self { input, key, iv }
    }
}

impl TernaryExpr for AesDecrypt {
    fn child1(&self) -> &dyn PhysicalExpr {
        self.input.as_ref()
    }
    fn child2(&self) -> &dyn PhysicalExpr {
        self.key.as_ref()
    }
    fn child3(&self) -> &dyn PhysicalExpr {
        self.iv.as_ref()
    }
    fn null_safe_eval(&self, input: Value, key: Value, iv: Value) -> Value {
        let input = input.get_binary();
        let key = key.get_binary();
        let iv = iv.get_binary();
        match aes_decrypt(input.as_slice(), key.as_slice(), iv.as_slice()) {
            Ok(v) => Value::Binary(Arc::new(v)),
            Err(_) => Value::Null,
        }
    }
}

impl PhysicalExpr for AesDecrypt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Binary
    }

    fn eval(&self, input: &dyn Row) -> Value {
        TernaryExpr::eval(self, input)
    }
}