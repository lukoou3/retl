use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use crate::Result;
use crate::expr::*;
use crate::expr::aggregate::*;

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
        "current_timestamp" | "now" => CurrentTimestamp,
        "from_unixtime" => FromUnixTime,
        "unix_timestamp" => UnixTimestamp,
        "to_unix_timestamp" => ToUnixTimestamp,
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
    );
    FunctionRegistry { expressions }
}

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