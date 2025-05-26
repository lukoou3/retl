use crate::Result;
use crate::data::{GenericRow, Row, Value};
use crate::execution::Collector;
use crate::transform::vrl::VrlValue;
use crate::types::Schema;
use log::warn;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::mem;
use vrl::compiler::state::RuntimeState;
use vrl::compiler::{
    CompilationResult, CompileConfig, Context, ExpressionError, Program, Resolved, TargetValue,
    TimeZone, TypeState,
};
use vrl::diagnostic::Formatter;
use vrl::prelude::{KeyString, NotNan};
use vrl::value::Secrets;
use crate::datetime_utils::{from_timestamp_micros_utc, NORM_DATETIME_FMT};
use crate::transform::vrl::convert::{create_vrl_to_value, VrlValueToValue};

pub trait Pipeline: Debug {
    fn process(&mut self, input_value: VrlValue, out: &mut dyn Collector) -> Result<()>;
    fn set_next(&mut self, next: Box<dyn Pipeline>);

    fn type_state(&self) -> TypeState;
}

#[derive(Debug)]
pub struct NoOpPipeline;

impl Pipeline for NoOpPipeline {
    fn process(&mut self, _input_value: VrlValue, _out: &mut dyn Collector) -> Result<()> {
        Ok(())
    }

    fn set_next(&mut self, _: Box<dyn Pipeline>) {
        panic!("NoOpPipeline cannot have next pipeline")
    }

    fn type_state(&self) -> TypeState {
        TypeState::default()
    }
}

#[derive(Debug)]
pub struct OutPipeline {
    row: GenericRow,
    converts: HashMap<KeyString, (usize, Box<dyn VrlValueToValue>)>,
}

impl OutPipeline {
    pub fn new(schema: &Schema) -> Result<Self> {
        let row = GenericRow::new_with_size(schema.field_types().len());
        let mut converts = HashMap::new();
        for (i, field) in schema.fields.iter().enumerate() {
            let key = KeyString::from(field.name.as_str());
            let convert = create_vrl_to_value(field.data_type.clone())?;
            converts.insert(key, (i, convert));
        }
        Ok(Self { row, converts })
    }
}

impl Pipeline for OutPipeline {
    fn process(&mut self, input_value: VrlValue, out: &mut dyn Collector) -> Result<()> {
        self.row.fill_null();
        match input_value {
            VrlValue::Object(map) => {
                for (k, v) in map {
                    if let Some((i, convert)) = self.converts.get(&k) {
                        let value = convert.to_value(v);
                        self.row.update(*i, value);
                    }
                }
            },
            _ => (),
        }

        out.collect(&self.row)
    }

    fn set_next(&mut self, _: Box<dyn Pipeline>) {
        panic!("NoOpPipeline cannot have next pipeline")
    }

    fn type_state(&self) -> TypeState {
        TypeState::default()
    }
}

#[derive(Debug)]
pub struct RemapPipeline {
    program: Program,
    type_state: TypeState,
    target: TargetValue,
    state: RuntimeState,
    timezone: TimeZone,
    next: Box<dyn Pipeline>,
}

impl RemapPipeline {
    pub fn new(source: &str, state: TypeState, next: Box<dyn Pipeline>) -> Result<Self> {
        let functions = vrl::stdlib::all();
        let CompilationResult {
            program,
            warnings,
            config: _,
        } = vrl::compiler::compile_with_state(
            source,
            &functions,
            &state,
            CompileConfig::default(),
        )
        .map_err(|diagnostics| Formatter::new(source, diagnostics).colored().to_string())?;
        if !warnings.is_empty() {
            let warnings = Formatter::new(source, warnings).colored().to_string();
            warn!("VRL compilation warning:{}", warnings);
        }
        let type_state = program.final_type_info().state;
        let mut target = TargetValue {
            value: VrlValue::Null,
            metadata: VrlValue::Object(BTreeMap::new()),
            secrets: Secrets::default(),
        };
        let state = RuntimeState::default();
        let timezone = TimeZone::default();
        Ok(Self {
            program,
            type_state,
            target,
            state,
            timezone,
            next,
        })
    }
}

impl Pipeline for RemapPipeline {
    fn process(&mut self, input_value: VrlValue, out: &mut dyn Collector) -> Result<()> {
        self.target.value = input_value;
        self.state.clear();
        let mut ctx = Context::new(&mut self.target, &mut self.state, &self.timezone);
        match self.program.resolve(&mut ctx) {
            Ok(_) | Err(ExpressionError::Return { .. }) => {
                let input_value = mem::replace(&mut self.target.value, VrlValue::Null);
                self.next.process(input_value, out)
            }
            Err(e) => {
                warn!("Remap VRL error: {}", e);
                Ok(())
            }
        }
    }

    fn set_next(&mut self, next: Box<dyn Pipeline>) {
        self.next = next;
    }

    fn type_state(&self) -> TypeState {
        self.type_state.clone()
    }
}

#[derive(Debug)]
pub struct FilterPipeline {
    program: Program,
    type_state: TypeState,
    target: TargetValue,
    state: RuntimeState,
    timezone: TimeZone,
    next: Box<dyn Pipeline>,
}

impl FilterPipeline {
    pub fn new(source: &str, state: TypeState, next: Box<dyn Pipeline>) -> Result<Self> {
        let functions = vrl::stdlib::all();
        let CompilationResult {
            program,
            warnings,
            config: _,
        } = vrl::compiler::compile_with_state(
            source,
            &functions,
            &state,
            CompileConfig::default(),
        )
        .map_err(|diagnostics| Formatter::new(source, diagnostics).colored().to_string())?;
        if !program.final_type_info().result.is_boolean() {
            return Err("VRL conditions must return a boolean.".into());
        }
        if !warnings.is_empty() {
            let warnings = Formatter::new(source, warnings).colored().to_string();
            warn!("VRL compilation warning:{}", warnings);
        }
        let type_state = program.final_type_info().state;
        let mut target = TargetValue {
            value: VrlValue::Null,
            metadata: VrlValue::Object(BTreeMap::new()),
            secrets: Secrets::default(),
        };
        let state = RuntimeState::default();
        let timezone = TimeZone::default();
        Ok(Self {
            program,
            type_state,
            target,
            state,
            timezone,
            next,
        })
    }
}

impl Pipeline for FilterPipeline {
    fn process(&mut self, input_value: VrlValue, out: &mut dyn Collector) -> Result<()> {
        self.target.value = input_value;
        self.state.clear();
        let mut ctx = Context::new(&mut self.target, &mut self.state, &self.timezone);
        match self.program.resolve(&mut ctx) {
            Ok(value) | Err(ExpressionError::Return { value, .. }) => match value {
                VrlValue::Boolean(boolean) => {
                    if boolean {
                        let input_value = mem::replace(&mut self.target.value, VrlValue::Null);
                        self.next.process(input_value, out)
                    } else {
                        Ok(())
                    }
                }
                _ => {
                    warn!("Filter VRL returned non-boolean value.");
                    Ok(())
                }
            },
            Err(e) => {
                warn!("Filter VRL error: {}", e);
                Ok(())
            }
        }
    }

    fn set_next(&mut self, next: Box<dyn Pipeline>) {
        self.next = next;
    }

    fn type_state(&self) -> TypeState {
        self.type_state.clone()
    }
}
