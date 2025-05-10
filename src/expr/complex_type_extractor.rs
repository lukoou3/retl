use crate::Result;
use crate::expr::{CreateScalarFunction, Expr, ScalarFunction, create_physical_expr, Literal};
use crate::physical_expr::{self as phy, PhysicalExpr};
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct GetStructField {
    pub child: Box<Expr>,
    pub ordinal: Box<Expr>,
    pub _ordinal: usize,
}

impl GetStructField {
    pub fn new(child: Box<Expr>, ordinal: Box<Expr>) -> Result<Self> {
        let _ordinal = match ordinal.as_ref() {
            Expr::Literal(Literal{value, data_type}) if data_type == DataType::int_type() => {
                value.get_int() as usize
            },
            _ => return Err(format!("requires ordinal argument to be int literal, found:{:?}", ordinal)),
        };
        Ok(Self { child, ordinal, _ordinal,  })
    }

    pub fn field_name(&self) -> &str {
        match self.child.data_type() {
            DataType::Struct(f) => &f.0[self._ordinal].name,
            _ => ""
        }
    }
}

impl CreateScalarFunction for GetStructField {
    fn from_args(args: Vec<Expr>) -> Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires 2 argument, found:{}", args.len()));
        }
        let mut iter = args.into_iter();
        let child = iter.next().unwrap();
        let ordinal =iter.next().unwrap();
        Ok(Box::new(GetStructField::new(Box::new(child), Box::new(ordinal),)?))
    }
}

impl ScalarFunction for GetStructField {
    fn name(&self) -> &str {
        "get_struct_field"
    }

    fn data_type(&self) -> &DataType {
        match self.child.data_type() {
            DataType::Struct(f) => &f.0[self._ordinal].data_type,
            _ => DataType::null_type()
        }
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child, &self.ordinal]
    }

    fn check_input_data_types(&self) -> Result<()> {
        if !matches!(self.child.data_type(), DataType::Struct(_)) {
            Err(format!("first arg requires struct type, not {}", self.child.data_type()))
        } else {
            Ok(())
        }
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        Ok(Box::new(phy::GetStructField::new(
            create_physical_expr(&self.child)?,
            self._ordinal,
            self.data_type().clone(),
        )))
    }
}

#[derive(Debug, Clone)]
pub struct GetArrayItem {
    pub child: Box<Expr>,
    pub ordinal: Box<Expr>,
}

impl GetArrayItem {
    pub fn new(child: Box<Expr>, ordinal: Box<Expr>) -> GetArrayItem {
        GetArrayItem { child, ordinal }
    }
}

impl CreateScalarFunction for GetArrayItem {
    fn from_args(args: Vec<Expr>) -> crate::Result<Box<dyn ScalarFunction>> {
        if args.len() != 2 {
            return Err(format!("requires 2 argument, found:{}", args.len()));
        }

        let child = args[0].clone();
        let ordinal = args[1].clone();

        Ok(Box::new(GetArrayItem::new(
            Box::new(child),
            Box::new(ordinal),
        )))
    }
}

impl ScalarFunction for GetArrayItem {
    fn name(&self) -> &str {
        "get_array_item"
    }

    fn data_type(&self) -> &DataType {
        if let DataType::Array(data_type) = self.child.data_type() {
            data_type.as_ref()
        } else {
            panic!("GetArrayItem child must be array")
        }
    }

    fn args(&self) -> Vec<&Expr> {
        vec![&self.child, &self.ordinal]
    }

    fn check_input_data_types(&self) -> Result<()> {
        if !matches!(self.child.data_type(), DataType::Array(_)) {
            Err(format!("first arg requires array type, not {}", self.child.data_type()))
        } else {
            Ok(())
        }
    }

    fn create_physical_expr(&self) -> Result<Box<dyn PhysicalExpr>> {
        Ok(Box::new(phy::GetArrayItem::new(
            create_physical_expr(&self.child)?,
            create_physical_expr(&self.ordinal)?,
            self.data_type().clone(),
        )))
    }
}
