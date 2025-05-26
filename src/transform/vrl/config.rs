use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use vrl::compiler::{TimeZone, TypeState};
use vrl::prelude::KeyString;
use crate::Result;
use crate::config::{TaskContext, TransformConfig, TransformProvider};
use crate::parser::parse_schema;
use crate::transform::Transform;
use crate::transform::vrl::convert::create_value_to_vrl;
use crate::transform::vrl::pipeline::{FilterPipeline, NoOpPipeline, OutPipeline, Pipeline, RemapPipeline};
use crate::transform::vrl::transform::VrlTransform;
use crate::types::Schema;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VrlTransformConfig {
    pub input_columns: Vec<String>,
    pub out_schema: String,
    pub timezone: Option<TimeZone>,
    #[serde(default)]
    pub drop_on_error: bool,
    #[serde(default)]
    pub drop_on_abort: bool,
    pub pipelines: Vec<PipelineConfig>,
}

#[typetag::serde(name = "vrl")]
impl TransformConfig for VrlTransformConfig {
    fn build(&self, schema: Schema) -> Result<Box<dyn TransformProvider>> {
        let mut input_fields = Vec::with_capacity(self.input_columns.len());
        for col in &self.input_columns {
            if let Some(field) = schema.get_filed_by_name(col) {
                input_fields.push(field.clone());
            } else {
                return Err(format!("input column {} not found in schema", col));
            }
        }
        let input_schema = Schema::new(input_fields);
        let schema = parse_schema(&self.out_schema)?;
        let pipeline_configs = self.pipelines.clone();
        Ok(Box::new(VrlTransformProvider {
            input_schema,
            schema,
            timezone: self.timezone.clone(),
            drop_on_error: self.drop_on_error,
            drop_on_abort: self.drop_on_abort,
            pipeline_configs,
        }))
    }
}

#[derive(Clone)]
pub struct VrlTransformProvider {
    input_schema: Schema,
    schema: Schema,
    timezone: Option<TimeZone>,
    drop_on_error: bool,
    drop_on_abort: bool,
    pipeline_configs: Vec<PipelineConfig>,
}

impl TransformProvider for VrlTransformProvider {
    fn create_transform(&self, task_context: TaskContext) -> Result<Box<dyn Transform>> {
        let mut converts = Vec::with_capacity(self.input_schema.fields.len());
        for (i, field) in self.input_schema.fields.iter().enumerate() {
            let key = KeyString::from(field.name.as_ref());
            let convert = create_value_to_vrl(field.data_type.clone())?;
            converts.push((i, key, convert));
        }
        let schema = self.schema.clone();
        let mut pipelines = Vec::with_capacity(self.pipeline_configs.len());
        let mut type_state = TypeState::default();
        for c in &self.pipeline_configs {
            let p = c.build(type_state, Box::new(NoOpPipeline))?;
            type_state = p.type_state();
            pipelines.push(p);
        }
        let mut pipeline:Box<dyn Pipeline> = Box::new(OutPipeline::new(&self.schema)?);
        for mut p in pipelines.into_iter().rev() {
            p.set_next(pipeline);
            pipeline = p;
        }
        Ok(Box::new(VrlTransform::new(
            task_context, schema, self.drop_on_error, self.drop_on_abort,
            pipeline, converts,
        )))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PipelineConfig {
    Remap(RemapConfig),
    Filter(FilterConfig),
}

impl PipelineConfig {
    fn build(&self, state: TypeState, next: Box<dyn Pipeline>) -> Result<Box<dyn Pipeline>> {
        match self {
            PipelineConfig::Remap(c) => c.build(state, next),
            PipelineConfig::Filter(c) => c.build(state, next),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemapConfig {
    pub source: Option<String>,
    pub file: Option<PathBuf>,
}

impl RemapConfig {
    fn build(&self, state: TypeState, next: Box<dyn Pipeline>) -> Result<Box<dyn Pipeline>> {
        let source = if let Some(source) = &self.source {
            source.clone()
        } else if let Some(file) = &self.file {
            std::fs::read_to_string(file).map_err(|_| "read remap file error")?
        } else {
            return Err("remap config must have source or file".into());
        };
        Ok(Box::new(RemapPipeline::new(&source, state, next)?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterConfig {
    pub source: String,
}

impl FilterConfig {
    fn build(&self, state: TypeState, next: Box<dyn Pipeline>) -> Result<Box<dyn Pipeline>> {
        Ok(Box::new(FilterPipeline::new(&self.source, state, next)?))
    }
}