use std::any::Any;
use std::borrow::Cow;
use std::fmt::{Debug};
use std::hash::Hash;
use std::iter::zip;
use std::sync::Arc;
use log::error;
use memchr::memchr3;
use regex::{Error, Regex, RegexBuilder};
use crate::Result;
use crate::data::{empty_row, Row, Value};
use crate::physical_expr::{Literal, PhysicalExpr, TernaryExpr};
use crate::types::DataType;

#[derive(Debug)]
pub struct RegExpExtract {
    subject: Box<dyn PhysicalExpr>,
    regexp: Box<dyn PhysicalExpr>,
    idx: Box<dyn PhysicalExpr>,
    regexp_static: Option<Regex>,
}

impl RegExpExtract {
    pub fn new(subject: Box<dyn PhysicalExpr>, regexp: Box<dyn PhysicalExpr>, idx: Box<dyn PhysicalExpr>) -> RegExpExtract {
        let regexp_static = if let Some(literal) = regexp.as_any().downcast_ref::<Literal>() {
            let value = literal.eval(empty_row());
            if value.is_null() {
                None
            } else {
                match Regex::new(value.get_string()) {
                    Ok(r) => Some(r),
                    Err(e) => {
                        error!("Failed to compile regexp: {:?}", e);
                        None
                    }
                }
            }
        } else {
            None
        };
        RegExpExtract { subject, regexp, idx, regexp_static, }
    }
}

impl TernaryExpr for RegExpExtract {
    fn child1(&self) -> &dyn PhysicalExpr {
        self.subject.as_ref()
    }

    fn child2(&self) -> &dyn PhysicalExpr {
        self.regexp.as_ref()
    }

    fn child3(&self) -> &dyn PhysicalExpr {
        self.idx.as_ref()
    }

    fn null_safe_eval(&self, subject: Value, regexp: Value, idx: Value) -> Value {
        let source = subject.get_string();
        let idx = idx.get_int();
        if idx < 0 {
            return Value::Null;
        }
        let idx = idx as usize;
        if let Some(regexp) = &self.regexp_static {
            match regexp.captures(source) {
                Some(captures) => {
                    match captures.get(idx) {
                        Some(m) => Value::String(Arc::new(m.as_str().to_string())),
                        None => Value::Null,
                    }
                },
                None => Value::empty_string(),
            }
        } else {
            match Regex::new(regexp.get_string()) {
                Ok(regexp) => match regexp.captures(source) {
                    Some(captures) => {
                        match captures.get(idx) {
                            Some(m) => Value::String(Arc::new(m.as_str().to_string())),
                            None => Value::Null,
                        }
                    },
                    None => Value::empty_string(),
                },
                Err(e) => {
                    error!("Failed to compile regexp: {:?}", e);
                    Value::Null
                }
            }
        }
    }
}


impl PhysicalExpr for RegExpExtract {
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
pub struct RegExpReplace {
    subject: Box<dyn PhysicalExpr>,
    regexp: Box<dyn PhysicalExpr>,
    rep: Box<dyn PhysicalExpr>,
    regexp_static: Option<Regex>,
}

impl RegExpReplace {
    pub fn new(subject: Box<dyn PhysicalExpr>, regexp: Box<dyn PhysicalExpr>, rep: Box<dyn PhysicalExpr>) -> RegExpReplace {
        let regexp_static = if let Some(literal) = regexp.as_any().downcast_ref::<Literal>() {
            let value = literal.eval(empty_row());
            if value.is_null() {
                None
            } else {
                match Regex::new(value.get_string()) {
                    Ok(r) => Some(r),
                    Err(e) => {
                        error!("Failed to compile regexp: {:?}", e);
                        None
                    }
                }
            }
        } else {
            None
        };
        RegExpReplace { subject, regexp, rep, regexp_static, }
    }
}

impl TernaryExpr for RegExpReplace {
    fn child1(&self) -> &dyn PhysicalExpr {
        self.subject.as_ref()
    }

    fn child2(&self) -> &dyn PhysicalExpr {
        self.regexp.as_ref()
    }

    fn child3(&self) -> &dyn PhysicalExpr {
        self.rep.as_ref()
    }

    fn null_safe_eval(&self, subject: Value, regexp: Value, rep: Value) -> Value {
        let source = subject.get_string();
        let replacement = rep.get_string();
        if let Some(regexp) = &self.regexp_static {
            match regexp.replace_all(source, replacement) {
                Cow::Borrowed(s) => subject,
                Cow::Owned(s) => Value::String(Arc::new(s)),
            }
        } else {
            match Regex::new(regexp.get_string()) {
                Ok(regexp) => match regexp.replace_all(source, replacement) {
                    Cow::Borrowed(s) => subject,
                    Cow::Owned(s) => Value::String(Arc::new(s)),
                },
                Err(e) => {
                    error!("Failed to compile regexp: {:?}", e);
                    Value::Null
                }
            }
        }
    }
}


impl PhysicalExpr for RegExpReplace {
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

// Like expression
#[derive(Debug)]
pub struct Like {
    expr: Box<dyn PhysicalExpr>,
    pattern: Box<dyn PhysicalExpr>,
    predicate: Option<Predicate>,
}

impl Like {
    pub fn new(expr: Box<dyn PhysicalExpr>, pattern: Box<dyn PhysicalExpr>) -> Like {
        if let Some(literal) = pattern.as_any().downcast_ref::<Literal>() {
            if let Ok(predicate) = Predicate::like(literal.eval(empty_row()).get_string()){
                Like { expr, pattern, predicate: Some(predicate) }
            } else {
                Like { expr, pattern, predicate: None }
            }
        } else {
            Like { expr, pattern, predicate: None }
        }
    }
}

impl PhysicalExpr for Like {
    fn as_any(&self) -> &dyn Any{
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let left_value = self.expr.eval(input);
        if left_value.is_null() {
            return Value::Null;
        }
        if let Some(predicate) = & self.predicate {
            Value::Boolean(predicate.eval(left_value.get_string()))
        } else {
            let right_value = self.pattern.eval(input);
            if right_value.is_null() {
                return Value::Null;
            }
            if let Ok(predicate)  = Predicate::like(right_value.get_string()) {
                Value::Boolean(predicate.eval(left_value.get_string()))
            } else {
                Value::Null
            }
        }
    }
}

#[derive(Debug)]
pub struct RLike {
    expr: Box<dyn PhysicalExpr>,
    pattern: Box<dyn PhysicalExpr>,
    regex: Option<Regex>,
}

impl RLike {
    pub fn new(expr: Box<dyn PhysicalExpr>, pattern: Box<dyn PhysicalExpr>) -> Self {
        if let Some(literal) = pattern.as_any().downcast_ref::<Literal>() {
            if let Ok(regex) = Regex::new(literal.eval(empty_row()).get_string()) {
                Self { expr, pattern, regex: Some(regex) }
            } else {
                Self { expr, pattern: Box::new(Literal::new(Value::Null, DataType::String)), regex: None }
            }
        } else {
            Self { expr, pattern, regex: None }
        }
    }
}

impl PhysicalExpr for RLike {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, input: &dyn Row) -> Value {
        let left_value = self.expr.eval(input);
        if left_value.is_null() {
            return Value::Null;
        }

        if let Some(regex) = & self.regex {
            Value::Boolean(regex.is_match(&left_value.get_string()))
        } else {
            let right_value = self.pattern.eval(input);
            if right_value.is_null() {
                return Value::Null;
            }
            if let Ok(regex) = Regex::new(right_value.get_string()) {
                Value::Boolean(regex.is_match(&left_value.get_string()))
            } else {
                Value::Null
            }
        }
    }
}

/// A string based predicate
#[derive(Debug)]
pub(crate) enum Predicate {
    Eq(Box<str>),
    Contains(Box<str>),
    StartsWith(Box<str>),
    EndsWith(Box<str>),

    /// Equality ignoring ASCII case
    IEqAscii(Box<str>),
    /// Starts with ignoring ASCII case
    IStartsWithAscii(Box<str>),
    /// Ends with ignoring ASCII case
    IEndsWithAscii(Box<str>),

    Regex(Regex)
}

/*impl Debug for Predicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Predicate::Eq(s) => write!(f, "Eq({:?})", s),
            Predicate::Contains(_) => write!(f, "Contains"),
            Predicate::StartsWith(s) => write!(f, "StartsWith({:?})", s),
            Predicate::EndsWith(s) => write!(f, "EndsWith({:?})", s),
            Predicate::IEqAscii(s) => write!(f, "IEqAscii({:?})", s),
            Predicate::IStartsWithAscii(s) => write!(f, "IStartsWithAscii({:?})", s),
            Predicate::IEndsWithAscii(s) => write!(f, "IEndsWithAscii({:?})", s),
            Predicate::Regex(regex) => write!(f, "Regex({:?})", regex),
        }
    }
}*/

impl Predicate {
    /// Create a predicate for the given like pattern
    pub(crate) fn like(pattern: &str) -> Result<Self> {
        if !contains_like_pattern(pattern) {
            Ok(Self::Eq(Box::from(pattern)))
        } else if pattern.ends_with('%') && !contains_like_pattern(&pattern[..pattern.len() - 1]) {
            Ok(Self::StartsWith(Box::from(&pattern[..pattern.len() - 1])))
        } else if pattern.starts_with('%') && !contains_like_pattern(&pattern[1..]) {
            Ok(Self::EndsWith(Box::from(&pattern[1..])))
        } else if pattern.starts_with('%')
            && pattern.ends_with('%')
            && !contains_like_pattern(&pattern[1..pattern.len() - 1])
        {
            Ok(Self::contains(&pattern[1..pattern.len() - 1]))
        } else {
            Ok(Self::Regex(regex_like(pattern, false)?))
        }
    }

    pub(crate) fn contains(needle: &str) -> Self {
        Self::Contains(Box::from(needle))
    }

    /// Create a predicate for the given ilike pattern
    pub(crate) fn ilike(pattern: &str, is_ascii: bool) -> Result<Self> {
        if is_ascii && pattern.is_ascii() {
            if !contains_like_pattern(pattern) {
                return Ok(Self::IEqAscii(Box::from(pattern)));
            } else if pattern.ends_with('%')
                && !pattern.ends_with("\\%")
                && !contains_like_pattern(&pattern[..pattern.len() - 1])
            {
                return Ok(Self::IStartsWithAscii(Box::from(&pattern[..pattern.len() - 1])));
            } else if pattern.starts_with('%') && !contains_like_pattern(&pattern[1..]) {
                return Ok(Self::IEndsWithAscii(Box::from(&pattern[1..])));
            }
        }
        Ok(Self::Regex(regex_like(pattern, true)?))
    }

    /// Evaluate this predicate against the given haystack
    pub(crate) fn eval(&self, haystack: &str) -> bool {
        match self {
            Predicate::Eq(v) => v.as_ref() == haystack,
            Predicate::IEqAscii(v) => haystack.eq_ignore_ascii_case(v),
            Predicate::Contains(finder) => haystack.contains(finder.as_ref()),
            Predicate::StartsWith(v) => starts_with(haystack, v, equals_kernel),
            Predicate::IStartsWithAscii(v) => {
                starts_with(haystack, v, equals_ignore_ascii_case_kernel)
            }
            Predicate::EndsWith(v) => ends_with(haystack, v, equals_kernel),
            Predicate::IEndsWithAscii(v) => ends_with(haystack, v, equals_ignore_ascii_case_kernel),
            Predicate::Regex(v) => v.is_match(haystack),
        }
    }
}

fn contains_like_pattern(pattern: &str) -> bool {
    memchr3(b'%', b'_', b'\\', pattern.as_bytes()).is_some()
}

fn equals_bytes(lhs: &[u8], rhs: &[u8], byte_eq_kernel: impl Fn((&u8, &u8)) -> bool) -> bool {
    lhs.len() == rhs.len() && zip(lhs, rhs).all(byte_eq_kernel)
}

/// This is faster than `str::starts_with` for small strings.
/// See <https://github.com/apache/arrow-rs/issues/6107> for more details.
fn starts_with(haystack: &str, needle: &str, byte_eq_kernel: impl Fn((&u8, &u8)) -> bool) -> bool {
    if needle.len() > haystack.len() {
        false
    } else {
        zip(haystack.as_bytes(), needle.as_bytes()).all(byte_eq_kernel)
    }
}
/// This is faster than `str::ends_with` for small strings.
/// See <https://github.com/apache/arrow-rs/issues/6107> for more details.
fn ends_with(haystack: &str, needle: &str, byte_eq_kernel: impl Fn((&u8, &u8)) -> bool) -> bool {
    if needle.len() > haystack.len() {
        false
    } else {
        zip(
            haystack.as_bytes().iter().rev(),
            needle.as_bytes().iter().rev(),
        )
            .all(byte_eq_kernel)
    }
}

fn equals_kernel((n, h): (&u8, &u8)) -> bool {
    n == h
}

fn equals_ignore_ascii_case_kernel((n, h): (&u8, &u8)) -> bool {
    n.eq_ignore_ascii_case(h)
}

/// Transforms a like `pattern` to a regex compatible pattern. To achieve that, it does:
///
/// 1. Replace `LIKE` multi-character wildcards `%` => `.*` (unless they're at the start or end of the pattern,
///    where the regex is just truncated - e.g. `%foo%` => `foo` rather than `^.*foo.*$`)
/// 2. Replace `LIKE` single-character wildcards `_` => `.`
/// 3. Escape regex meta characters to match them and not be evaluated as regex special chars. e.g. `.` => `\\.`
/// 4. Replace escaped `LIKE` wildcards removing the escape characters to be able to match it as a regex. e.g. `\\%` => `%`
fn regex_like(pattern: &str, case_insensitive: bool) -> Result<Regex> {
    let mut result = String::with_capacity(pattern.len() * 2);
    let mut chars_iter = pattern.chars().peekable();
    match chars_iter.peek() {
        // if the pattern starts with `%`, we avoid starting the regex with a slow but meaningless `^.*`
        Some('%') => {
            chars_iter.next();
        }
        _ => result.push('^'),
    };

    while let Some(c) = chars_iter.next() {
        match c {
            '\\' => {
                match chars_iter.peek() {
                    Some(&next) => {
                        if regex_syntax::is_meta_character(next) {
                            result.push('\\');
                        }
                        result.push(next);
                        // Skipping the next char as it is already appended
                        chars_iter.next();
                    }
                    None => {
                        // Trailing backslash in the pattern. E.g. PostgreSQL and Trino treat it as an error, but e.g. Snowflake treats it as a literal backslash
                        result.push('\\');
                        result.push('\\');
                    }
                }
            }
            '%' => result.push_str(".*"),
            '_' => result.push('.'),
            c => {
                if regex_syntax::is_meta_character(c) {
                    result.push('\\');
                }
                result.push(c);
            }
        }
    }
    // instead of ending the regex with `.*$` and making it needlessly slow, we just end the regex
    if result.ends_with(".*") {
        result.pop();
        result.pop();
    } else {
        result.push('$');
    }
    RegexBuilder::new(&result)
        .case_insensitive(case_insensitive)
        .dot_matches_new_line(true)
        .build()
        .map_err(|e| format!("Unable to build regex from LIKE pattern: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regex_like() {
        let test_cases = [
            // %..%
            (r"%foobar%", r"foobar"),
            // ..%..
            (r"foo%bar", r"^foo.*bar$"),
            // .._..
            (r"foo_bar", r"^foo.bar$"),
            // escaped wildcards
            (r"\%\_", r"^%_$"),
            // escaped non-wildcard
            (r"\a", r"^a$"),
            // escaped escape and wildcard
            (r"\\%", r"^\\"),
            // escaped escape and non-wildcard
            (r"\\a", r"^\\a$"),
            // regex meta character
            (r".", r"^\.$"),
            (r"$", r"^\$$"),
            (r"\\", r"^\\$"),
        ];

        for (like_pattern, expected_regexp) in test_cases {
            let r = regex_like(like_pattern, false).unwrap();
            assert_eq!(r.to_string(), expected_regexp);
        }
    }

    #[test]
    fn test_contains() {
        assert!(Predicate::contains("hay").eval("haystack"));
        assert!(Predicate::contains("haystack").eval("haystack"));
        assert!(Predicate::contains("h").eval("haystack"));
        assert!(Predicate::contains("k").eval("haystack"));
        assert!(Predicate::contains("stack").eval("haystack"));
        assert!(Predicate::contains("sta").eval("haystack"));
        assert!(Predicate::contains("stack").eval("hay£stack"));
        assert!(Predicate::contains("y£s").eval("hay£stack"));
        assert!(Predicate::contains("£").eval("hay£stack"));
        assert!(Predicate::contains("a").eval("a"));
        // not matching
        assert!(!Predicate::contains("hy").eval("haystack"));
        assert!(!Predicate::contains("stackx").eval("haystack"));
        assert!(!Predicate::contains("x").eval("haystack"));
        assert!(!Predicate::contains("haystack haystack").eval("haystack"));
    }

    #[test]
    fn test_starts_with() {
        assert!(Predicate::StartsWith(Box::from("hay")).eval("haystack"));
        assert!(Predicate::StartsWith(Box::from("h£ay")).eval("h£aystack"));
        assert!(Predicate::StartsWith(Box::from("haystack")).eval("haystack"));
        assert!(Predicate::StartsWith(Box::from("ha")).eval("haystack"));
        assert!(Predicate::StartsWith(Box::from("h")).eval("haystack"));
        assert!(Predicate::StartsWith(Box::from("")).eval("haystack"));

        assert!(!Predicate::StartsWith(Box::from("stack")).eval("haystack"));
        assert!(!Predicate::StartsWith(Box::from("haystacks")).eval("haystack"));
        assert!(!Predicate::StartsWith(Box::from("HAY")).eval("haystack"));
        assert!(!Predicate::StartsWith(Box::from("h£ay")).eval("haystack"));
        assert!(!Predicate::StartsWith(Box::from("hay")).eval("h£aystack"));
    }

    #[test]
    fn test_ends_with() {
        assert!(Predicate::EndsWith(Box::from("stack")).eval("haystack"));
        assert!(Predicate::EndsWith(Box::from("st£ack")).eval("hayst£ack"));
        assert!(Predicate::EndsWith(Box::from("haystack")).eval("haystack"));
        assert!(Predicate::EndsWith(Box::from("ck")).eval("haystack"));
        assert!(Predicate::EndsWith(Box::from("k")).eval("haystack"));
        assert!(Predicate::EndsWith(Box::from("")).eval("haystack"));

        assert!(!Predicate::EndsWith(Box::from("hay")).eval("haystack"));
        assert!(!Predicate::EndsWith(Box::from("STACK")).eval("haystack"));
        assert!(!Predicate::EndsWith(Box::from("haystacks")).eval("haystack"));
        assert!(!Predicate::EndsWith(Box::from("xhaystack")).eval("haystack"));
        assert!(!Predicate::EndsWith(Box::from("st£ack")).eval("haystack"));
        assert!(!Predicate::EndsWith(Box::from("stack")).eval("hayst£ack"));
    }

    #[test]
    fn test_istarts_with() {
        assert!(Predicate::IStartsWithAscii(Box::from("hay")).eval("haystack"));
        assert!(Predicate::IStartsWithAscii(Box::from("hay")).eval("HAYSTACK"));
        assert!(Predicate::IStartsWithAscii(Box::from("HAY")).eval("haystack"));
        assert!(Predicate::IStartsWithAscii(Box::from("HaY")).eval("haystack"));
        assert!(Predicate::IStartsWithAscii(Box::from("hay")).eval("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii(Box::from("HAY")).eval("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii(Box::from("haystack")).eval("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii(Box::from("HaYsTaCk")).eval("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii(Box::from("")).eval("HaYsTaCk"));

        assert!(!Predicate::IStartsWithAscii(Box::from("stack")).eval("haystack"));
        assert!(!Predicate::IStartsWithAscii(Box::from("haystacks")).eval("haystack"));
        assert!(!Predicate::IStartsWithAscii(Box::from("h.ay")).eval("haystack"));
        assert!(!Predicate::IStartsWithAscii(Box::from("hay")).eval("h£aystack"));
    }

    #[test]
    fn test_iends_with() {
        assert!(Predicate::IEndsWithAscii(Box::from("stack")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Box::from("STACK")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Box::from("StAcK")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Box::from("stack")).eval("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii(Box::from("STACK")).eval("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii(Box::from("StAcK")).eval("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii(Box::from("stack")).eval("HAYsTaCk"));
        assert!(Predicate::IEndsWithAscii(Box::from("STACK")).eval("HAYsTaCk"));
        assert!(Predicate::IEndsWithAscii(Box::from("StAcK")).eval("HAYsTaCk"));
        assert!(Predicate::IEndsWithAscii(Box::from("haystack")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Box::from("HAYSTACK")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Box::from("haystack")).eval("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii(Box::from("ck")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Box::from("cK")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Box::from("ck")).eval("haystacK"));
        assert!(Predicate::IEndsWithAscii(Box::from("")).eval("haystack"));

        assert!(!Predicate::IEndsWithAscii(Box::from("hay")).eval("haystack"));
        assert!(!Predicate::IEndsWithAscii(Box::from("stac")).eval("HAYSTACK"));
        assert!(!Predicate::IEndsWithAscii(Box::from("haystacks")).eval("haystack"));
        assert!(!Predicate::IEndsWithAscii(Box::from("stack")).eval("haystac£k"));
        assert!(!Predicate::IEndsWithAscii(Box::from("xhaystack")).eval("haystack"));
    }

    #[test]
    fn test_like() {
        let p = Predicate::like("%ab%").unwrap();
        println!("{:?}", p);
        assert!(p.eval("abc"));
        assert!(p.eval("_abc"));
        assert!(!p.eval("_aac"));
        let p = Predicate::like("%ab").unwrap();
        println!("{:?}", p);
        let p = Predicate::like("ab%").unwrap();
        println!("{:?}", p);
    }
}