use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};
use std::fmt::{Debug, Display, Formatter};

// static EMPTY_BINARY:Arc<Vec<u8>> = Arc::new(Vec::new());
static EMPTY_BINARY: LazyLock<Arc<Vec<u8>>> = LazyLock::new(|| Arc::new(Vec::new()));
static EMPTY_ROW: LazyLock<Arc<dyn Row>> = LazyLock::new(|| Arc::new(GenericRow::new(Vec::new())));
static EMPTY_VALUES: LazyLock<Arc<Vec<Value>>> = LazyLock::new(|| Arc::new(Vec::new()));

//Float wrapper over f32/f64. Just because we cannot build std::hash::Hash for floats directly we have to do it through type wrapper
struct Fl<T>(T);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl std::hash::Hash for Fl<$t> {
            #[inline]
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                state.write(&<$i>::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
            }
        })+
    };
}

hash_float_value!((f64, u64), (f32, u32));

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(Arc<String>),
    Boolean(bool),
    Binary(Arc<Vec<u8>>),
    Struct(Arc<dyn Row>),
    Array(Arc<Vec<Value>>),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Int(v) => write!(f, "{v}"),
            Value::Long(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Double(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "'{v}'"),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::Binary(v) => {
                write!(f, "[")?;
                let mut first = true;
                for x in v.as_ref() {
                    if !first {
                        write!(f, ", ")?;
                    }
                    first = false;
                    write!(f, "{x}")?;
                }
                write!(f, "]")
            },
            Value::Struct(v) => write!(f, "{v}"),
            Value::Array(v) => {
                write!(f, "[")?;
                let mut first = true;
                for x in v.as_ref() {
                    if !first {
                        write!(f, ", ")?;
                    }
                    first = false;
                    write!(f, "{x}")?;
                }
                write!(f, "]")
            }
        }
    }
}

impl Value {
    pub fn string(s: impl Into<String>) -> Self {
        Value::String(Arc::new(s.into()))
    }
    pub fn int(i: i32) -> Self {
        Value::Int(i)
    }

    pub fn long(l: i64) -> Self {
        Value::Long(l)
    }

    pub fn float(f: f32) -> Self {
        Value::Float(f)
    }

    pub fn double(d: f64) -> Self {
        Value::Double(d)
    }

    pub fn boolean(b: bool) -> Self {
        Value::Boolean(b)
    }

    pub fn null() -> Self {
        Value::Null
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        if let Value::Null = self {
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn is_true(&self) -> bool {
        match self {
            Value::Boolean(true) => true,
            _ => false
        }
    }

    #[inline]
    pub fn is_false(&self) -> bool {
        match self {
            Value::Boolean(false) => true,
            _ => false
        }
    }

    pub fn get_int(&self) -> i32 {
        if let Value::Int(v) = self {
            *v
        } else {
            panic!("{:?} is not an Int", self)
        }
    }

    pub fn get_long(&self) -> i64 {
        if let Value::Long(v) = self {
            *v
        } else {
            panic!("{:?} is not an long", self)
        }
    }

    pub fn get_float(&self) -> f32 {
        if let Value::Float(v) = self {
            *v
        } else {
            panic!("{:?} is not a float", self)
        }
    }

    pub fn get_double(&self) -> f64 {
        if let Value::Double(v) = self {
            *v
        } else {
            panic!("{:?} is not a double", self)
        }
    }

    pub fn get_string(&self) -> &str {
        if let Value::String(v) = self {
            v.as_str()
        } else {
            panic!("{:?} is not a string", self)
        }
    }

    pub fn get_boolean(&self) -> bool {
        if let Value::Boolean(v) = self {
            *v
        } else {
            panic!("{:?} is not a boolean", self)
        }
    }

    pub fn get_binary(&self) -> Arc<Vec<u8>> {
        if let Value::Binary(v) = self {
            v.clone()
        } else {
            panic!("{:?} is not a binary", self)
        }
    }

    pub fn get_struct(&self) -> Arc<dyn Row> {
        if let Value::Struct(v) = self {
            v.clone()
        } else {
            panic!("{:?} is not a struct", self)
        }
    }

    pub fn get_array(&self) -> Arc<Vec<Value>> {
        if let Value::Array(v) = self {
            v.clone()
        } else {
            panic!("{:?} is not a array", self)
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use Value::*;
        match self {
            Null => 0.hash(state),
            Int(v) => v.hash(state),
            Long(v) => v.hash(state),
            Float(v) => Fl(*v).hash(state),
            Double(v) => Fl(*v).hash(state),
            String(v) => v.hash(state),
            Boolean(v) => v.hash(state),
            Binary(v) => v.hash(state),
            Struct(v) => v.hash(state),
            Array(v) => v.hash(state),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Int(v1), Int(v2)) => v1.eq(v2),
            (Int(_), _) => false,
            (Long(v1), Long(v2)) => v1.eq(v2),
            (Long(_), _) => false,
            (Float(v1), Float(v2)) => v1.eq(v2),
            (Float(_), _) => false,
            (Double(v1), Double(v2)) => v1.eq(v2),
            (Double(_), _) => false,
            (String(v1), String(v2)) => v1.eq(v2),
            (String(_), _) => false,
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Binary(v1), Binary(v2)) => v1.eq(v2),
            (Binary(_), _) => false,
            (Struct(v1), Struct(v2)) => v1.eq(v2),
            (Struct(_), _) => false,
            (Array(v1), Array(v2)) => v1.eq(v2),
            (Array(_), _) => false,
            (Null, Null) => true,
            (Null, _) => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use Value::*;
        match (self, other) {
            (Int(v1), Int(v2)) => v1.partial_cmp(v2),
            (Int(_), _) => None,
            (Long(v1), Long(v2)) => v1.partial_cmp(v2),
            (Long(_), _) => None,
            (Float(v1), Float(v2)) => Some(v1.total_cmp(v2)),
            (Float(_), _) => None,
            (Double(v1), Double(v2)) => Some(v1.total_cmp(v2)),
            (Double(_), _) => None,
            (String(v1), String(v2)) => v1.partial_cmp(v2),
            (String(_), _) => None,
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
            (Boolean(_), _) => None,
            (Binary(v1), Binary(v2)) => v1.partial_cmp(v2),
            (Binary(_), _) => None,
            (Struct(v1), Struct(v2)) => v1.partial_cmp(v2),
            (Struct(_), _) => None,
            (Array(v1), Array(v2)) => v1.partial_cmp(v2),
            (Array(_), _) => None,
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => None,
        }
    }
}

pub trait Row: Debug + Display + Send + Sync {
    fn size(&self) -> usize;
    fn len(&self) -> usize;
    fn is_null(&self, i: usize) -> bool;
    fn get(&self, i: usize) -> &Value;
    fn set_null_at(&mut self, i: usize);
    fn update(&mut self, i: usize, value: Value);
    fn get_int(&self, i: usize) -> i32;
    fn get_long(&self, i: usize) -> i64;
    fn get_float(&self, i: usize) -> f32;
    fn get_double(&self, i: usize) -> f64;
    fn get_string(&self, i: usize) -> &str;
    fn get_boolean(&self, i: usize) -> bool;
    fn get_binary(&self, i: usize) -> Arc<Vec<u8>>;
    fn get_struct(&self, i: usize) -> Arc<dyn Row>;
    fn get_array(&self, i: usize) -> Arc<Vec<Value>>;

    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let len = self.len();
        for i in 0..len {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", self.get(i))?;
        }
        write!(f, "]")
    }
}

pub fn empty_row() -> &'static dyn Row {
    EMPTY_ROW.as_ref()
}

impl PartialEq for dyn Row {
    fn eq(&self, other: &Self) -> bool {
        let len = self.len();
        if len != other.len() {
            return false;
        }
        for i in 0..len {
            if self.get(i) != other.get(i) {
                return false;
            }
        }
        true
    }
}

impl PartialOrd for dyn Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let len = self.len();
        if len != other.len() {
            return None;
        }
        for i in 0..len {
            match self.get(i).partial_cmp(other.get(i)) {
                None => return None, // 某个元素无法比较
                Some(Ordering::Equal) => continue, // 继续比较下一个元素
                Some(ord) => return Some(ord), // 返回当前元素的比较结果
            }
        }
        // 所有元素都相等
        Some(Ordering::Equal)
    }
}

impl Hash for dyn Row {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let len = self.len();
        for i in 0..len {
            self.get(i).hash(state);
        }
    }
}

#[derive(Clone, Debug)]
pub struct GenericRow {
    values: Vec<Value>,
}

impl GenericRow {
    pub fn new(values: Vec<Value>) -> GenericRow {
        GenericRow { values }
    }

    pub fn new_with_size(size: usize) -> GenericRow {
        let mut values = Vec::with_capacity(size);
        values.resize(size, Value::Null);
        GenericRow { values }
    }

    pub fn fill_null(&mut self) {
        self.values.fill(Value::Null);
    }
}

impl Display for GenericRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let len = self.len();
        for i in 0..len {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", self.get(i))?;
        }
        write!(f, "]")
    }
}

impl Row for GenericRow {
    fn size(&self) -> usize {
        self.values.len()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn is_null(&self, i: usize) -> bool {
        let value = &self.values[i];
        if let Value::Null = value {
            true
        } else {
            false
        }
    }

    fn get(&self, i: usize) -> &Value {
        &self.values[i]
    }

    fn set_null_at(&mut self, i: usize) {
        self.values[i] = Value::Null;
    }

    fn update(&mut self, i: usize, value: Value) {
        self.values[i] = value;
    }

    fn get_int(&self, i: usize) -> i32 {
        let value = &self.values[i];
        if let Value::Int(v) = value {
            *v
        } else {
            0
        }
    }

    fn get_long(&self, i: usize) -> i64 {
        let value = &self.values[i];
        if let Value::Long(v) = value {
            *v
        } else {
            0
        }
    }

    fn get_float(&self, i: usize) -> f32 {
        let value = &self.values[i];
        if let Value::Float(v) = value {
            *v
        } else {
            0f32
        }
    }

    fn get_double(&self, i: usize) -> f64 {
        let value = &self.values[i];
        if let Value::Double(v) = value {
            *v
        } else {
            0f64
        }
    }

    fn get_string(&self, i: usize) -> &str {
        let value = &self.values[i];
        if let Value::String(v) = value {
            v
        } else {
            ""
        }
    }

    fn get_boolean(&self, i: usize) -> bool {
        let value = &self.values[i];
        if let Value::Boolean(v) = value {
            *v
        } else {
            false
        }
    }

    fn get_binary(&self, i: usize) -> Arc<Vec<u8>> {
        let value = &self.values[i];
        if let Value::Binary(v) = value {
            v.clone()
        } else {
            EMPTY_BINARY.clone()
        }
    }

    fn get_struct(&self, i: usize) -> Arc<dyn Row> {
        let value = &self.values[i];
        if let Value::Struct(v) = value {
            v.clone()
        } else {
            EMPTY_ROW.clone()
        }
    }

    fn get_array(&self, i: usize) -> Arc<Vec<Value>> {
        let value = &self.values[i];
        if let Value::Array(v) = value {
            v.clone()
        } else {
            EMPTY_VALUES.clone()
        }
    }
}

impl<'de> serde::de::Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        struct ValueVisitor;

        impl<'de> serde::de::Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("any valid JSON value")
            }

            #[inline]
            fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
                Ok(Value::Boolean(value))
            }

            #[inline]
            fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
                Ok(Value::long(value))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
                Ok(Value::long(value as i64))
            }

            #[inline]
            fn visit_f64<E>(self, value: f64) -> Result<Value, E> {
                Ok(Value::double(value))
            }

        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_values(){
        let v1 = Value::Int(1);
        let v2 = Value::Long(2);
        let v3 = Value::Float(3.14);
        let v4 = Value::Double(4.2);
        let v5 = Value::Boolean(true);
        let v6 = Value::String(Arc::new(String::from("hello")));
        let v7 = Value::Boolean(false);
        let v8 = Value::Null;
        let v9 = Value::Binary(Arc::new(vec![1, 2, 3, 4]));
        let v10 = Value::Array(Arc::new(vec![Value::Int(1), Value::Int(2)]));
        let v11 = Value::Struct(Arc::new(GenericRow::new(vec![Value::Int(3), Value::String(Arc::new(String::from("hello")))])));
        let values = vec![v1.clone(), v2.clone(), v3.clone(), v4.clone(), v5.clone(),
                          v6.clone(), v7.clone(), v8.clone(), v9.clone(), v10.clone(), v11.clone()];
        println!("{:?}", values);
        for v in &values {
            println!("{:?}", v);
        }
        for v in values.iter() {
            println!("{:?}", v);
        }
        println!("{:?}", values);
    }

    #[test]
    fn test_get_value() {
        let row = GenericRow::new(vec![
            Value::Int(42),
            Value::String(Arc::new("Alice".to_string())),
            Value::Boolean(false),
            Value::Null,
        ]);

        println!("i64 size:{}", size_of::<i64>());
        println!("String size:{}", size_of::<String>());
        println!("Arc<String> size:{}", size_of::<Arc<String>>());
        println!("Value size:{}", size_of::<Value>());
        println!("GenericRow size:{}", size_of::<GenericRow>());

        println!("{:?}", row);
        println!("ID: {}", row.get_int(0));
        println!("Name: {}", row.get_string(1));
        println!("Active: {}", row.get_boolean(2));
        println!("cnt: {}", row.get_int(3));
        println!("row: {:?}", row);
        println!("row: {}", row);

        // 判断字段是否为 null
        println!("{}", row.is_null(0));
        println!("{}", row.is_null(1));
        println!("{}", row.is_null(2));
        println!("{}", row.is_null(3));
    }

}