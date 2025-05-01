use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use crate::Result;
use crate::expr::*;
use crate::expr::aggregate::*;
use crate::types::DataType;

type FunctionBuilder = dyn Fn(Vec<Expr>) -> Result<Expr> + Send + Sync;
struct FunctionRegistry {
    expressions: HashMap<String, Box<FunctionBuilder>>
}

impl FunctionRegistry {
    pub fn lookup_function(&self, name: &str, args: Vec<Expr>) -> Result<Expr> {
        let builder = self.expressions.get(name);
        match builder {
            Some(builder) => match builder(args) {
                Ok(expr) => Ok(expr),
                Err(e) => Err(format!("invalid arguments for function {}: {}.", name, e))
            },
            None => Err(format!("undefined function {}", name))
        }
    }

    pub fn register_function(&mut self, name: &str, builder: Box<FunctionBuilder>) -> Result<()> {
        self.expressions.insert(name.to_string(), builder);
        Ok(())
    }
}

static FUNCTION_REGISTRY: LazyLock<Mutex<FunctionRegistry>> = LazyLock::new(|| {
    Mutex::new(builtin_function_registry())
});

pub fn register_function(name: &str, builder: Box<FunctionBuilder>)-> Result<()>  {
    let mut registry = FUNCTION_REGISTRY.lock().unwrap();
    registry.register_function(name, builder)
}

pub fn lookup_function(name: &str, args: Vec<Expr>) -> Result<Expr> {
    let registry = FUNCTION_REGISTRY.lock().unwrap();
    registry.lookup_function(name, args)
}

macro_rules! init_expressions {
    ($($($names:literal)|+ => $ty:ident),* $(,)?) => {
        {
            let mut expressions: HashMap<String, Box<FunctionBuilder>> = HashMap::new();
            $(
                $(
                    expressions.insert(
                        $names.to_string(),
                        Box::new(|args| $ty::create_function_expr(args))
                    );
                )+
            )*
            expressions
        }
    };
}

fn builtin_function_registry() -> FunctionRegistry {
    let expressions = init_expressions!(
        "if" => If,
        "nvl" => Nvl,
        "coalesce" => Coalesce,
        "greatest" => Greatest,
        "least" => Least,
        // string functions
        "length" | "char_length" | "character_length" => Length,
        "substring" | "substr" => Substring,
        "concat_ws" => ConcatWs,
        "concat" => Concat,
        "split" => StringSplit,
        "split_part" => SplitPart,
        "replace" => StringReplace,
        "regexp_replace" => RegExpReplace,
        "regexp_extract" => RegExpExtract,
        "trim" => StringTrim,
        "lower" | "lcase" => Lower,
        "upper" | "ucase" => Upper,
        "get_json_object" | "get_json_string" => GetJsonObject,
        "to_base64" => ToBase64,
        "from_base64" => FromBase64,
        "hex" => Hex,
        "unhex" => Unhex,
        // datetime functions
        "current_timestamp" | "now" => CurrentTimestamp,
        "from_unixtime" => FromUnixTime,
        "from_unixtime_millis" => FromUnixTimeMillis,
        "unix_timestamp" => UnixTimestamp,
        "unix_timestamp_millis" => UnixTimestampMillis,
        "to_unix_timestamp" => ToUnixTimestamp,
        "to_unix_timestamp_millis" => ToUnixTimestampMillis,
        "date_trunc" => TruncTimestamp,
        "date_floor" | "time_floor" => TimestampFloor,
        // math functions
        "pow" | "power" => Pow,
        "round" => Round,
        "floor" => Floor,
        "ceil" => Ceil,
        "bin" => Bin,
        // misc functions
        "aes_encrypt" => AesEncrypt,
        "aes_decrypt" => AesDecrypt,
        // aggregate functions
        "sum" => Sum,
        "count" => Count,
        "avg" | "mean" => Average,
        "min" => Min,
        "max" => Max,
        "first" => First,
        "last" => Last,
        "collect_set" => CollectSet,
        "collect_list" => CollectList,
        // generator
        "explode" => Explode,
        "path_file_unroll" => PathFileUnroll,
        // cast aliases
        "int" => CastInt,
        "long" => CastLong,
        "float" => CastFloat,
        "double" => CastDouble,
        "string" => CastString,
        "boolean" => CastBoolean,
        "timestamp" => CastTimestamp,
    );
    FunctionRegistry { expressions }
}


macro_rules! impl_cast_expr {
    ($name:ident, $target_type:expr) => {
        struct $name;

        impl $name {
            fn create_function_expr(args: Vec<Expr>) -> Result<Expr> {
                if args.len() != 1 {
                    return Err(format!("requires 1 argument, found:{}", args.len()));
                }
                let child = args.into_iter().next().unwrap();
                Ok(child.cast($target_type))
            }
        }
    };
}

impl_cast_expr!(CastInt, DataType::Int);
impl_cast_expr!(CastLong, DataType::Long);
impl_cast_expr!(CastFloat, DataType::Float);
impl_cast_expr!(CastDouble, DataType::Double);
impl_cast_expr!(CastString, DataType::String);
impl_cast_expr!(CastBoolean, DataType::Boolean);
impl_cast_expr!(CastTimestamp, DataType::Timestamp);


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup_function() {
        let registry = FUNCTION_REGISTRY.lock().unwrap();
        for (k, _v) in registry.expressions.iter() {
            println!("{}", k)
        }
    }
}