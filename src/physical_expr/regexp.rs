use std::any::Any;
use std::fmt::{Debug};
use std::hash::Hash;
use std::iter::zip;
use std::sync::Arc;
use memchr::memchr3;
use regex::{Regex, RegexBuilder};
use crate::Result;
use crate::data::{empty_row, Row, Value};
use crate::physical_expr::{Literal, PhysicalExpr};
use crate::types::DataType;

// Like expression
#[derive(Debug, Clone)]
pub struct Like {
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
    predicate: Option<Predicate>,
}

impl Like {
    pub fn new(expr: Arc<dyn PhysicalExpr>, pattern: Arc<dyn PhysicalExpr>) -> Like {
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

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for Like {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.pattern.eq(&other.pattern)
    }
}

impl Eq for Like {}

impl Hash for Like {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.pattern.hash(state);
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

#[derive(Debug, Clone)]
pub struct RLike {
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
    regex: Option<Regex>,
}

impl RLike {
    pub fn new(expr: Arc<dyn PhysicalExpr>, pattern: Arc<dyn PhysicalExpr>) -> Self {
        if let Some(literal) = pattern.as_any().downcast_ref::<Literal>() {
            if let Ok(regex) = Regex::new(literal.eval(empty_row()).get_string()) {
                Self { expr, pattern, regex: Some(regex) }
            } else {
                Self { expr, pattern: Arc::new(Literal::new(Value::Null, DataType::String)), regex: None }
            }
        } else {
            Self { expr, pattern, regex: None }
        }
    }
}

impl PartialEq for RLike {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && self.pattern.eq(&other.pattern)
    }
}

impl Eq for RLike {}

impl Hash for RLike {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.pattern.hash(state);
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
#[derive(Debug, Clone)]
pub(crate) enum Predicate {
    Eq(Arc<str>),
    Contains(Arc<str>),
    StartsWith(Arc<str>),
    EndsWith(Arc<str>),

    /// Equality ignoring ASCII case
    IEqAscii(Arc<str>),
    /// Starts with ignoring ASCII case
    IStartsWithAscii(Arc<str>),
    /// Ends with ignoring ASCII case
    IEndsWithAscii(Arc<str>),

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
            Ok(Self::Eq(Arc::from(pattern)))
        } else if pattern.ends_with('%') && !contains_like_pattern(&pattern[..pattern.len() - 1]) {
            Ok(Self::StartsWith(Arc::from(&pattern[..pattern.len() - 1])))
        } else if pattern.starts_with('%') && !contains_like_pattern(&pattern[1..]) {
            Ok(Self::EndsWith(Arc::from(&pattern[1..])))
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
        Self::Contains(Arc::from(needle))
    }

    /// Create a predicate for the given ilike pattern
    pub(crate) fn ilike(pattern: &str, is_ascii: bool) -> Result<Self> {
        if is_ascii && pattern.is_ascii() {
            if !contains_like_pattern(pattern) {
                return Ok(Self::IEqAscii(Arc::from(pattern)));
            } else if pattern.ends_with('%')
                && !pattern.ends_with("\\%")
                && !contains_like_pattern(&pattern[..pattern.len() - 1])
            {
                return Ok(Self::IStartsWithAscii(Arc::from(&pattern[..pattern.len() - 1])));
            } else if pattern.starts_with('%') && !contains_like_pattern(&pattern[1..]) {
                return Ok(Self::IEndsWithAscii(Arc::from(&pattern[1..])));
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
        assert!(Predicate::StartsWith(Arc::from("hay")).eval("haystack"));
        assert!(Predicate::StartsWith(Arc::from("h£ay")).eval("h£aystack"));
        assert!(Predicate::StartsWith(Arc::from("haystack")).eval("haystack"));
        assert!(Predicate::StartsWith(Arc::from("ha")).eval("haystack"));
        assert!(Predicate::StartsWith(Arc::from("h")).eval("haystack"));
        assert!(Predicate::StartsWith(Arc::from("")).eval("haystack"));

        assert!(!Predicate::StartsWith(Arc::from("stack")).eval("haystack"));
        assert!(!Predicate::StartsWith(Arc::from("haystacks")).eval("haystack"));
        assert!(!Predicate::StartsWith(Arc::from("HAY")).eval("haystack"));
        assert!(!Predicate::StartsWith(Arc::from("h£ay")).eval("haystack"));
        assert!(!Predicate::StartsWith(Arc::from("hay")).eval("h£aystack"));
    }

    #[test]
    fn test_ends_with() {
        assert!(Predicate::EndsWith(Arc::from("stack")).eval("haystack"));
        assert!(Predicate::EndsWith(Arc::from("st£ack")).eval("hayst£ack"));
        assert!(Predicate::EndsWith(Arc::from("haystack")).eval("haystack"));
        assert!(Predicate::EndsWith(Arc::from("ck")).eval("haystack"));
        assert!(Predicate::EndsWith(Arc::from("k")).eval("haystack"));
        assert!(Predicate::EndsWith(Arc::from("")).eval("haystack"));

        assert!(!Predicate::EndsWith(Arc::from("hay")).eval("haystack"));
        assert!(!Predicate::EndsWith(Arc::from("STACK")).eval("haystack"));
        assert!(!Predicate::EndsWith(Arc::from("haystacks")).eval("haystack"));
        assert!(!Predicate::EndsWith(Arc::from("xhaystack")).eval("haystack"));
        assert!(!Predicate::EndsWith(Arc::from("st£ack")).eval("haystack"));
        assert!(!Predicate::EndsWith(Arc::from("stack")).eval("hayst£ack"));
    }

    #[test]
    fn test_istarts_with() {
        assert!(Predicate::IStartsWithAscii(Arc::from("hay")).eval("haystack"));
        assert!(Predicate::IStartsWithAscii(Arc::from("hay")).eval("HAYSTACK"));
        assert!(Predicate::IStartsWithAscii(Arc::from("HAY")).eval("haystack"));
        assert!(Predicate::IStartsWithAscii(Arc::from("HaY")).eval("haystack"));
        assert!(Predicate::IStartsWithAscii(Arc::from("hay")).eval("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii(Arc::from("HAY")).eval("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii(Arc::from("haystack")).eval("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii(Arc::from("HaYsTaCk")).eval("HaYsTaCk"));
        assert!(Predicate::IStartsWithAscii(Arc::from("")).eval("HaYsTaCk"));

        assert!(!Predicate::IStartsWithAscii(Arc::from("stack")).eval("haystack"));
        assert!(!Predicate::IStartsWithAscii(Arc::from("haystacks")).eval("haystack"));
        assert!(!Predicate::IStartsWithAscii(Arc::from("h.ay")).eval("haystack"));
        assert!(!Predicate::IStartsWithAscii(Arc::from("hay")).eval("h£aystack"));
    }

    #[test]
    fn test_iends_with() {
        assert!(Predicate::IEndsWithAscii(Arc::from("stack")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Arc::from("STACK")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Arc::from("StAcK")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Arc::from("stack")).eval("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii(Arc::from("STACK")).eval("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii(Arc::from("StAcK")).eval("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii(Arc::from("stack")).eval("HAYsTaCk"));
        assert!(Predicate::IEndsWithAscii(Arc::from("STACK")).eval("HAYsTaCk"));
        assert!(Predicate::IEndsWithAscii(Arc::from("StAcK")).eval("HAYsTaCk"));
        assert!(Predicate::IEndsWithAscii(Arc::from("haystack")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Arc::from("HAYSTACK")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Arc::from("haystack")).eval("HAYSTACK"));
        assert!(Predicate::IEndsWithAscii(Arc::from("ck")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Arc::from("cK")).eval("haystack"));
        assert!(Predicate::IEndsWithAscii(Arc::from("ck")).eval("haystacK"));
        assert!(Predicate::IEndsWithAscii(Arc::from("")).eval("haystack"));

        assert!(!Predicate::IEndsWithAscii(Arc::from("hay")).eval("haystack"));
        assert!(!Predicate::IEndsWithAscii(Arc::from("stac")).eval("HAYSTACK"));
        assert!(!Predicate::IEndsWithAscii(Arc::from("haystacks")).eval("haystack"));
        assert!(!Predicate::IEndsWithAscii(Arc::from("stack")).eval("haystac£k"));
        assert!(!Predicate::IEndsWithAscii(Arc::from("xhaystack")).eval("haystack"));
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