use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;
use crate::data::{Row, Value};
use crate::physical_expr::{BinaryArithmetic, PhysicalExpr};
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