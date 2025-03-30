use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::physical_expr::{PhysicalExpr};
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct Length {
    pub child: Arc<dyn PhysicalExpr>,
}

impl Length {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Length { child }
    }
}

impl PartialEq for Length{
    fn eq(&self, other: &Length) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for Length{}

impl Hash for Length{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
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
        let value = self.child.eval(input);
        if value.is_null() {
            return Value::Null;
        }
        match self.child.data_type() {
            DataType::String => Value::Int(value.get_string().chars().count() as i32),
            DataType::Binary => Value::Int(value.get_binary().len() as i32),
            _ => Value::Null
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcatWs {
    sep: Arc<dyn PhysicalExpr>,
    str_args: Vec<Arc<dyn PhysicalExpr>>,
}

impl ConcatWs {
    pub fn new(sep: Arc<dyn PhysicalExpr>, str_args: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self { sep, str_args }
    }
}

impl PartialEq for ConcatWs {
    fn eq(&self, other: &Self) -> bool {
        self.sep.eq(&other.sep)
            && self.str_args.eq(&other.str_args)
    }
}

impl Eq for ConcatWs{}

impl Hash for ConcatWs{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sep.hash(state);
        self.str_args.hash(state);
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

#[derive(Debug, Clone)]
pub struct Substring {
    str: Arc<dyn PhysicalExpr>,
    pos: Arc<dyn PhysicalExpr>,
    len: Arc<dyn PhysicalExpr>,
}

impl Substring {
    pub fn new(str: Arc<dyn PhysicalExpr>, pos: Arc<dyn PhysicalExpr>, len: Arc<dyn PhysicalExpr>) -> Self {
        Self {str, pos, len}
    }
}

impl PartialEq for Substring {
    fn eq(&self, other: &Self) -> bool {
        self.str.eq(&other.str)
            && self.pos.eq(&other.pos)
            && self.len.eq(&other.len)
    }
}

impl Eq for Substring{}

impl Hash for Substring{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.str.hash(state);
        self.pos.hash(state);
        self.len.hash(state);
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
        let str = self.str.eval(input);
        if str.is_null() {
            return Value::Null;
        }
        let pos = self.pos.eval(input);
        if pos.is_null() {
            return Value::Null;
        }
        let len = self.len.eval(input);
        if len.is_null() {
            return Value::Null;
        }

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

#[derive(Debug, Clone)]
pub struct StringSplit {
    str: Arc<dyn PhysicalExpr>,
    delimiter: Arc<dyn PhysicalExpr>,
}

impl StringSplit {
    pub fn new(str: Arc<dyn PhysicalExpr>, delimiter: Arc<dyn PhysicalExpr>) -> Self {
        Self {str, delimiter}
    }
}

impl PartialEq for StringSplit {
    fn eq(&self, other: &Self) -> bool {
        self.str.eq(&other.str)
            && self.delimiter.eq(&other.delimiter)
    }
}

impl Eq for StringSplit{}

impl Hash for StringSplit{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.str.hash(state);
        self.delimiter.hash(state);
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
        let str = self.str.eval(input);
        if str.is_null() {
            return Value::Null;
        }
        let delimiter = self.delimiter.eval(input);
        if delimiter.is_null() {
            return Value::Null;
        }
        let str = str.get_string();
        let delimiter = delimiter.get_string();
        let split_string: Vec<_> = str.split(delimiter).map(|s| Value::String(Arc::new(s.to_string()))).collect();
        Value::Array(Arc::new(split_string))
    }
}

#[derive(Debug, Clone)]
pub struct SplitPart {
    str: Arc<dyn PhysicalExpr>,
    delimiter: Arc<dyn PhysicalExpr>,
    part: Arc<dyn PhysicalExpr>,
}

impl SplitPart {
    pub fn new(str: Arc<dyn PhysicalExpr>, delimiter: Arc<dyn PhysicalExpr>, part: Arc<dyn PhysicalExpr>) -> Self {
        Self {str, delimiter, part}
    }
}

impl PartialEq for SplitPart {
    fn eq(&self, other: &Self) -> bool {
        self.str.eq(&other.str)
            && self.delimiter.eq(&other.delimiter)
            && self.part.eq(&other.part)
    }
}

impl Eq for SplitPart{}

impl Hash for SplitPart{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.str.hash(state);
        self.delimiter.hash(state);
        self.part.hash(state);
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
        let str = self.str.eval(input);
        if str.is_null() {
            return Value::Null;
        }
        let delimiter = self.delimiter.eval(input);
        if delimiter.is_null() {
            return Value::Null;
        }
        let part = self.part.eval(input);
        if part.is_null() {
            return Value::Null;
        }
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

#[derive(Debug, Clone)]
pub struct StringReplace {
    str: Arc<dyn PhysicalExpr>,
    search: Arc<dyn PhysicalExpr>,
    replace: Arc<dyn PhysicalExpr>,
}

impl StringReplace {
    pub fn new(str: Arc<dyn PhysicalExpr>, search: Arc<dyn PhysicalExpr>, replace: Arc<dyn PhysicalExpr>) -> Self {
        Self {str, search, replace}
    }
}

impl PartialEq for StringReplace {
    fn eq(&self, other: &Self) -> bool {
        self.str.eq(&other.str)
            && self.search.eq(&other.search)
            && self.replace.eq(&other.replace)
    }
}

impl Eq for StringReplace{}

impl Hash for StringReplace{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.str.hash(state);
        self.search.hash(state);
        self.replace.hash(state);
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
        let str = self.str.eval(input);
        if str.is_null() {
            return Value::Null;
        }
        let search = self.search.eval(input);
        if search.is_null() {
            return Value::Null;
        }
        let replace = self.replace.eval(input);
        if replace.is_null() {
            return Value::Null;
        }
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

#[derive(Debug, Clone)]
pub struct StringTrim {
    src_str: Arc<dyn PhysicalExpr>,
    trim_str: Arc<dyn PhysicalExpr>,
}

impl StringTrim {
    pub fn new(src_str: Arc<dyn PhysicalExpr>, trim_str: Arc<dyn PhysicalExpr>) -> Self {
        Self {src_str, trim_str}
    }
}

impl PartialEq for StringTrim {
    fn eq(&self, other: &Self) -> bool {
        self.src_str.eq(&other.src_str)
            && self.trim_str.eq(&other.trim_str)
    }
}

impl Eq for StringTrim{}

impl Hash for StringTrim{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.src_str.hash(state);
        self.trim_str.hash(state);
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
        let src_str = self.src_str.eval(input);
        if src_str.is_null() {
            return Value::Null;
        }
        let trim_str = self.trim_str.eval(input);
        if trim_str.is_null() {
            return Value::Null;
        }
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

#[derive(Debug, Clone)]
pub struct Lower {
    child: Arc<dyn PhysicalExpr>,
}

impl Lower {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self {child}
    }
}

impl PartialEq for Lower {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for Lower{}

impl Hash for Lower{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
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
        let child = self.child.eval(input);
        if child.is_null() {
            return Value::Null;
        }
        let child = child.get_string();
        Value::string(child.to_lowercase())
    }
}

#[derive(Debug, Clone)]
pub struct Upper {
    child: Arc<dyn PhysicalExpr>,
}

impl Upper {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self {child}
    }
}

impl PartialEq for Upper {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for Upper{}

impl Hash for Upper{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
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
        let child = self.child.eval(input);
        if child.is_null() {
            return Value::Null;
        }
        let child = child.get_string();
        Value::string(child.to_uppercase())
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