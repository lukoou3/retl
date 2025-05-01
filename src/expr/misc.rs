use crate::Result;
use crate::expr::{create_physical_expr, CreateScalarFunction, Expr, ScalarFunction};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::{AbstractDataType, DataType};

#[derive(Debug, Clone)]
pub struct AesEncrypt {
    pub input: Box<Expr>,
    pub key: Box<Expr>,
    pub iv: Box<Expr>,
}

impl AesEncrypt {
    pub fn new(input: Box<Expr>, key: Box<Expr>, iv: Box<Expr>) -> AesEncrypt {
        AesEncrypt {input, key, iv}
    }
}

impl CreateScalarFunction for AesEncrypt {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 3 {
            return Err(format!("requires 3 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let input = iter.next().unwrap();
        let key = iter.next().unwrap();
        let iv = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(input), Box::new(key), Box::new(iv))))
    }
}

impl ScalarFunction for AesEncrypt {
    fn name(&self) -> &str {
        "aes_encrypt"
    }

    fn data_type(&self) -> &DataType {
        DataType::binary_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.input, &self.key, &self.iv]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Type(DataType::Binary), AbstractDataType::Type(DataType::Binary), AbstractDataType::Type(DataType::Binary)])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{input, key, iv} = self;
        Ok(Box::new(phy::AesEncrypt::new(create_physical_expr(input)?, create_physical_expr(key)?, create_physical_expr(iv)?)))
    }
}

#[derive(Debug, Clone)]
pub struct AesDecrypt {
    pub input: Box<Expr>,
    pub key: Box<Expr>,
    pub iv: Box<Expr>,
}

impl AesDecrypt {
    pub fn new(input: Box<Expr>, key: Box<Expr>, iv: Box<Expr>) -> AesDecrypt {
        AesDecrypt {input, key, iv}
    }
}

impl CreateScalarFunction for AesDecrypt {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 3 {
            return Err(format!("requires 3 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let input = iter.next().unwrap();
        let key = iter.next().unwrap();
        let iv = iter.next().unwrap();
        Ok(Box::new(Self::new(Box::new(input), Box::new(key), Box::new(iv))))
    }
}

impl ScalarFunction for AesDecrypt {
    fn name(&self) -> &str {
        "aes_decrypt"
    }

    fn data_type(&self) -> &DataType {
        DataType::binary_type()
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.input, &self.key, &self.iv]
    }

    fn expects_input_types(&self) -> Option<Vec<AbstractDataType>> {
        Some(vec![AbstractDataType::Type(DataType::Binary), AbstractDataType::Type(DataType::Binary), AbstractDataType::Type(DataType::Binary)])
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        let Self{input, key, iv} = self;
        Ok(Box::new(phy::AesDecrypt::new(create_physical_expr(input)?, create_physical_expr(key)?, create_physical_expr(iv)?)))
    }
}