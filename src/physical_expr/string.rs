use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use base64::{DecodeError, Engine};
use base64::engine::general_purpose::STANDARD;
use crate::data::{Row, Value};
use crate::physical_expr::{BinaryExpr, PhysicalExpr, TernaryExpr, UnaryExpr};
use crate::types::DataType;

#[derive(Debug)]
pub struct Length {
    pub child: Box<dyn PhysicalExpr>,
}

impl Length {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        Length { child }
    }
}

impl UnaryExpr for Length {
    fn child(&self) -> &dyn PhysicalExpr {
        self.child.as_ref()
    }

    fn null_safe_eval(&self, value: Value) -> Value {
        match self.child.data_type() {
            DataType::String => Value::Int(value.get_string().chars().count() as i32),
            DataType::Binary => Value::Int(value.get_binary().len() as i32),
            _ => Value::Null
        }
    }
}

impl PhysicalExpr for Length {
    fn as_any(&self) -> &dyn Any{
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Int
    }

    fn eval(&self, input: &dyn Row) -> Value {
        UnaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct ConcatWs {
    sep: Box<dyn PhysicalExpr>,
    str_args: Vec<Box<dyn PhysicalExpr>>,
}

impl ConcatWs {
    pub fn new(sep: Box<dyn PhysicalExpr>, str_args: Vec<Box<dyn PhysicalExpr>>) -> Self {
        Self { sep, str_args }
    }
}

impl PhysicalExpr for ConcatWs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let sep = self.sep.eval(input);
        if sep.is_null() {
            return Value::Null;
        }
        let sep = sep.get_string();
        if self.str_args.is_empty() {
            return  Value::empty_string();
        }
        let mut first = true;
        let mut rst = String::with_capacity(self.str_args.len() + sep.len() * self.str_args.len());
        for arg in self.str_args.iter() {
            let value = arg.eval(input);
            if value.is_null() {
                continue;
            }
            if first {
                first = false;
                rst.push_str(value.get_string());
            } else {
                rst.push_str(sep);
                rst.push_str(value.get_string());
            }
        }
        Value::String(Arc::new(rst))
    }
}

#[derive(Debug)]
pub struct Substring {
    str: Box<dyn PhysicalExpr>,
    pos: Box<dyn PhysicalExpr>,
    len: Box<dyn PhysicalExpr>,
}

impl Substring {
    pub fn new(str: Box<dyn PhysicalExpr>, pos: Box<dyn PhysicalExpr>, len: Box<dyn PhysicalExpr>) -> Self {
        Self {str, pos, len}
    }
}

impl TernaryExpr for Substring {
    fn child1(&self) -> &dyn PhysicalExpr {
        self.str.as_ref()
    }
    fn child2(&self) -> &dyn PhysicalExpr {
        self.pos.as_ref()
    }
    fn child3(&self) -> &dyn PhysicalExpr {
        self.len.as_ref()
    }
    fn null_safe_eval(&self, str: Value, pos: Value, len: Value) -> Value {
        let str = str.get_string();
        let start = pos.get_int();
        let count = len.get_int();
        let (start, end) = get_true_start_end(
            str,
            start,
            Some(count),
            false,
        ); // start, end is byte-based
        let substr = &str[start..end];
        Value::string(substr)
    }
}

impl PhysicalExpr for Substring {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        TernaryExpr::eval(self, input)
    }

}

#[derive(Debug)]
pub struct StringSplit {
    str: Box<dyn PhysicalExpr>,
    delimiter: Box<dyn PhysicalExpr>,
}

impl StringSplit {
    pub fn new(str: Box<dyn PhysicalExpr>, delimiter: Box<dyn PhysicalExpr>) -> Self {
        Self {str, delimiter}
    }
}

impl BinaryExpr for StringSplit {
    fn left(&self) -> &dyn PhysicalExpr {
        self.str.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.delimiter.as_ref()
    }

    fn null_safe_eval(&self, str: Value, delimiter: Value) -> Value {
        let str = str.get_string();
        let delimiter = delimiter.get_string();
        let split_string: Vec<_> = str.split(delimiter).map(|s| Value::String(Arc::new(s.to_string()))).collect();
        Value::Array(Arc::new(split_string))
    }
}

impl PhysicalExpr for StringSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self) -> DataType {
        DataType::string_array_type().clone()
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct SplitPart {
    str: Box<dyn PhysicalExpr>,
    delimiter: Box<dyn PhysicalExpr>,
    part: Box<dyn PhysicalExpr>,
}

impl SplitPart {
    pub fn new(str: Box<dyn PhysicalExpr>, delimiter: Box<dyn PhysicalExpr>, part: Box<dyn PhysicalExpr>) -> Self {
        Self {str, delimiter, part}
    }
}

impl TernaryExpr for SplitPart {
    fn child1(&self) -> &dyn PhysicalExpr {
        self.str.as_ref()
    }

    fn child2(&self) -> &dyn PhysicalExpr {
        self.delimiter.as_ref()
    }

    fn child3(&self) -> &dyn PhysicalExpr {
        self.part.as_ref()
    }

    fn null_safe_eval(&self, str: Value, delimiter: Value, part: Value) -> Value {
        let str = str.get_string();
        let delimiter = delimiter.get_string();
        let part = part.get_int();
        let split_string: Vec<_> = str.split(delimiter).collect();
        let len = split_string.len();

        let index = match part.cmp(&0) {
            std::cmp::Ordering::Less => len as i32 + part,
            std::cmp::Ordering::Equal => return Value::Null,
            std::cmp::Ordering::Greater => part - 1,
        } as usize;

        if index < len {
            Value::String(Arc::new(split_string[index].to_string()))
        } else {
            Value::empty_string()
        }
    }
}

impl PhysicalExpr for SplitPart {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        TernaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct StringReplace {
    str: Box<dyn PhysicalExpr>,
    search: Box<dyn PhysicalExpr>,
    replace: Box<dyn PhysicalExpr>,
}

impl StringReplace {
    pub fn new(str: Box<dyn PhysicalExpr>, search: Box<dyn PhysicalExpr>, replace: Box<dyn PhysicalExpr>) -> Self {
        Self {str, search, replace}
    }
}

impl TernaryExpr for StringReplace {
    fn child1(&self) -> &dyn PhysicalExpr {
        self.str.as_ref()
    }
    fn child2(&self) -> &dyn PhysicalExpr {
        self.search.as_ref()
    }
    fn child3(&self) -> &dyn PhysicalExpr {
        self.replace.as_ref()
    }
    fn null_safe_eval(&self, str: Value, search: Value, replace: Value) -> Value {
        let s = str.get_string();
        let search = search.get_string();
        let replace = replace.get_string();
        if s.contains(search) {
            Value::string(s.replace(search, replace))
        } else {
            str
        }
    }
}

impl PhysicalExpr for StringReplace {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        TernaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct StringTrim {
    src_str: Box<dyn PhysicalExpr>,
    trim_str: Box<dyn PhysicalExpr>,
}

impl StringTrim {
    pub fn new(src_str: Box<dyn PhysicalExpr>, trim_str: Box<dyn PhysicalExpr>) -> Self {
        Self {src_str, trim_str}
    }
}

impl BinaryExpr for StringTrim {
    fn left(&self) -> &dyn PhysicalExpr {
        self.src_str.as_ref()
    }

    fn right(&self) -> &dyn PhysicalExpr {
        self.trim_str.as_ref()
    }

    fn null_safe_eval(&self, src_str: Value, trim_str: Value) -> Value {
        let src = src_str.get_string();
        let trim = trim_str.get_string();
        if trim.is_empty() {
            src_str
        } else {
            let trimmed = src.trim_matches(|c| trim.contains(c));
            if trimmed.len() == src.len() {
                src_str
            } else {
                Value::string(trimmed)
            }
        }
    }
}


impl PhysicalExpr for StringTrim {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        BinaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct Lower {
    child: Box<dyn PhysicalExpr>,
}

impl Lower {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        Self {child}
    }
}

impl UnaryExpr for Lower {
    fn child(&self) -> &dyn PhysicalExpr {
        self.child.as_ref()
    }

    fn null_safe_eval(&self, value: Value) -> Value {
        let value = value.get_string();
        Value::string(value.to_lowercase())
    }
}

impl PhysicalExpr for Lower {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        UnaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct Upper {
    child: Box<dyn PhysicalExpr>,
}

impl Upper {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        Self {child}
    }
}

impl UnaryExpr for Upper {
    fn child(&self) -> &dyn PhysicalExpr {
        self.child.as_ref()
    }

    fn null_safe_eval(&self, value: Value) -> Value {
        let value = value.get_string();
        Value::string(value.to_uppercase())
    }
}


impl PhysicalExpr for Upper {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        UnaryExpr::eval(self, input)
    }
}

#[derive(Debug)]
pub struct ToBase64 {
    child: Box<dyn PhysicalExpr>,
}

impl ToBase64 {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        Self {child}
    }
}

impl PhysicalExpr for ToBase64 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child.eval(input);
        match value {
            Value::Binary(b) => {
                let encoded = STANDARD.encode(b.as_slice());
                Value::String(Arc::new(encoded))
            },
            Value::String(s) => {
                let decoded = STANDARD.decode(s.as_bytes()).unwrap();
                Value::Binary(Arc::new(decoded))
            },
            _ => Value::Null,
        }
    }
}

#[derive(Debug)]
pub struct FromBase64 {
    child: Box<dyn PhysicalExpr>,
}

impl FromBase64 {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        Self {child}
    }
}

impl PhysicalExpr for FromBase64 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Binary
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child.eval(input);
        match value {
            Value::Binary(b) => {
                match STANDARD.decode(b.as_slice()) {
                    Ok(b) => Value::Binary(Arc::new(b)),
                    Err(_) => Value::Null,
                }
            },
            Value::String(s) => {
                match STANDARD.decode(s.as_bytes()) {
                    Ok(b) => Value::Binary(Arc::new(b)),
                    Err(_) => Value::Null,
                }
            },
            _ => Value::Null,
        }
    }
}

#[derive(Debug)]
pub struct Hex {
    child: Box<dyn PhysicalExpr>,
}

impl Hex {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        Self {child}
    }
}

impl PhysicalExpr for Hex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::String
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child.eval(input);
        match value {
            Value::Binary(b) => {
                let encoded = hex::encode(b.as_slice());
                Value::String(Arc::new(encoded))
            },
            Value::String(s) => {
                let encoded = hex::encode(s.as_bytes());
                Value::String(Arc::new(encoded))
            },
            _ => Value::Null,
        }
    }
}

#[derive(Debug)]
pub struct Unhex {
    child: Box<dyn PhysicalExpr>,
}

impl Unhex {
    pub fn new(child: Box<dyn PhysicalExpr>) -> Self {
        Self {child}
    }
}

impl PhysicalExpr for Unhex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Binary
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let value = self.child.eval(input);
        match value {
            Value::Binary(b) => {
                match hex::decode(b.as_slice()) {
                    Ok(b) => Value::Binary(Arc::new(b)),
                    Err(_) => Value::Null,
                }
            },
            Value::String(s) => {
                match hex::decode(s.as_bytes()) {
                    Ok(b) => Value::Binary(Arc::new(b)),
                    Err(_) => Value::Null,
                }
            },
            _ => Value::Null,
        }
    }
}

// Convert the given `start` and `count` to valid byte indices within `input` string
//
// Input `start` and `count` are equivalent to PostgreSQL's `substr(s, start, count)`
// `start` is 1-based, if `count` is not provided count to the end of the string
// Input indices are character-based, and return values are byte indices
// The input bounds can be outside string bounds, this function will return
// the intersection between input bounds and valid string bounds
// `input_ascii_only` is used to optimize this function if `input` is ASCII-only
//
// * Example
// 'Hi🌏' in-mem (`[]` for one char, `x` for one byte): [x][x][xxxx]
// `get_true_start_end('Hi🌏', 1, None) -> (0, 6)`
// `get_true_start_end('Hi🌏', 1, 1) -> (0, 1)`
// `get_true_start_end('Hi🌏', -10, 2) -> (0, 0)`
fn get_true_start_end(
    input: &str,
    start: i32,
    count: Option<i32>,
    is_input_ascii_only: bool,
) -> (usize, usize) {
    let start = start.checked_sub(1).unwrap_or(start);

    let end = match count {
        Some(count) => start + count as i32,
        None => input.len() as i32,
    };
    let count_to_end = count.is_some();

    let start = start.clamp(0, input.len() as i32) as usize;
    let end = end.clamp(0, input.len() as i32) as usize;
    let count = end - start;

    // If input is ASCII-only, byte-based indices equals to char-based indices
    if is_input_ascii_only {
        return (start, end);
    }

    // Otherwise, calculate byte indices from char indices
    // Note this decoding is relatively expensive for this simple `substr` function,,
    // so the implementation attempts to decode in one pass (and caused the complexity)
    let (mut st, mut ed) = (input.len(), input.len());
    let mut start_counting = false;
    let mut cnt = 0;
    for (char_cnt, (byte_cnt, _)) in input.char_indices().enumerate() {
        if char_cnt == start {
            st = byte_cnt;
            if count_to_end {
                start_counting = true;
            } else {
                break;
            }
        }
        if start_counting {
            if cnt == count {
                ed = byte_cnt;
                break;
            }
            cnt += 1;
        }
    }
    (st, ed)
}