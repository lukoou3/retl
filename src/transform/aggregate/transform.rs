use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::BuildHasherDefault;
use std::mem;
use std::sync::Arc;
use ahash::{AHasher};
use crate::config::TaskContext;
use crate::Result;
use crate::data::{GenericRow, JoinedRow, Object, Row};
use crate::datetime_utils::current_timestamp_millis;
use crate::execution::{Collector, TimeService};
use crate::expr::{AttributeReference, BoundReference, Expr};
use crate::expr::aggregate::PhysicalTypedAggFunction;
use crate::physical_expr::{create_physical_expr, MutableProjection, PhysicalExpr, Projection};
use crate::transform::{Transform, ProcessOperator, OutOperator};
use crate::types::Schema;

struct PreProcessCollector<'a> {
    transform: &'a mut TaskAggregateTransform,
    out: &'a mut dyn Collector,
    time_service: &'a mut TimeService,
}

impl<'a> Debug for PreProcessCollector<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PreProcessCollector")
    }
}

impl<'a> Collector for PreProcessCollector<'a> {
    fn collect(&mut self, row: &dyn Row) -> Result<()> {
        self.transform.post_process(row, self.out, self.time_service)
    }

    fn check_timer(&mut self, time: u64) -> Result<()> {
        Ok(())
    }
}

pub struct TaskAggregateTransform {
    task_context: TaskContext,
    schema: Schema,
    no_pre: bool,
    pre_process: Box<dyn ProcessOperator>,
    agg_func: RowAggregateFunction,
    rst_func: RowResultFunction,
    key_selector: RowKeySelector,
    buffers: HashMap<GenericRow, GenericRow,BuildHasherDefault<AHasher>>,
    max_rows: usize,
    interval_ms: u64,
    trigger_time_ms: u64,
}

impl TaskAggregateTransform {
    pub fn new(task_context: TaskContext, schema: Schema, no_pre: bool, pre_process: Box<dyn ProcessOperator>, agg_exprs: Vec<Expr>, group_exprs: Vec<Expr>,  result_exprs: Vec<Expr>,
               input_attrs: Vec<AttributeReference>, max_rows: usize, interval_ms: u64) -> Result<Self> {
        let mut agg_attrs = Vec::with_capacity(agg_exprs.len());
        let mut final_agg_attrs = Vec::with_capacity(agg_exprs.len());
        for expr in &agg_exprs {
            match expr {
                Expr::DeclarativeAggFunction(f) => {
                    for attr in f.agg_buffer_attributes() {
                        agg_attrs.push(attr);
                    }
                    final_agg_attrs.push(f.result_attribute());
                },
                Expr::TypedAggFunction(f) => {
                    for attr in f.agg_buffer_attributes() {
                        agg_attrs.push(attr);
                    }
                    final_agg_attrs.push(f.result_attribute());
                },
                _ => return Err(format!("not support agg expr:{:?}", expr))
            }
        }
        let mut group_attrs = Vec::with_capacity(group_exprs.len());
        for expr in &group_exprs {
            group_attrs.push(expr.to_attribute()?);
        }

        let agg_func = RowAggregateFunction::new(agg_exprs, agg_attrs, input_attrs.clone())?;
        let exprs: Result<Vec<Arc<dyn PhysicalExpr>>, String> = BoundReference::bind_references(group_exprs, input_attrs)?.iter().map(|expr| create_physical_expr(expr)).collect();
        let key_selector = RowKeySelector::new(exprs?);
        let rst_func = RowResultFunction::new(result_exprs, group_attrs.into_iter().chain(final_agg_attrs.into_iter()).collect())?;

        let trigger_time_ms = 0;
        Ok(Self { task_context, schema, no_pre, pre_process, agg_func, rst_func, key_selector, buffers: HashMap::default(), max_rows, interval_ms,trigger_time_ms })
    }
}

impl Debug for TaskAggregateTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryTransform")
            .field("task_context", &self.task_context)
            .field("schema", &self.schema)
            .field("agg_exprs", &self.agg_func.agg_exprs)
            .field("input_attrs", &self.agg_func.input_attrs)
            .finish()
    }
}

impl TaskAggregateTransform {
    fn post_process(&mut self, row: &dyn Row, out: &mut dyn Collector, time_service: &mut TimeService) -> Result<()> {
        let key = self.key_selector.get_key(row);
        // 也可以这样实现
        let buffer = self.buffers.entry(key).or_insert_with(|| self.agg_func.create_aggregation());
        self.agg_func.update(buffer, row);
        /* if let Some(buffer) = self.buffers.get_mut(&key) {
            self.agg_func.update(buffer, row);
        } else {
            let mut buffer = self.agg_func.create_aggregation();
            self.agg_func.update(&mut buffer, row);
            self.buffers.insert(key, buffer);
        }*/
        if self.buffers.len() >= self.max_rows {
            self.flush(out)
        } else {
            if self.trigger_time_ms == 0 {
                self.trigger_time_ms = current_timestamp_millis() / self.interval_ms * self.interval_ms + self.interval_ms;
                time_service.register_timer(self.trigger_time_ms);
            }
            Ok(())
        }
    }
    
    fn flush(&mut self, out: &mut dyn Collector) -> Result<()> {
        for (key, buffer) in &mut self.buffers {
            let value = self.agg_func.eval(buffer);
            let joiner = JoinedRow::new(key, value) ;
            let row= self.rst_func.result_projection.apply(&joiner);
            out.collect(row)?;
        }
        self.buffers.clear();
        Ok(())
    }
}

impl Transform for TaskAggregateTransform {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn process(&mut self, row: &dyn Row, out: &mut dyn Collector, time_service: &mut TimeService) -> Result<()> {
        if self.no_pre {
            self.post_process(row, out, time_service)
        } else {
            let mut pre_process = mem::replace(&mut self.pre_process, Box::new(OutOperator));
            let mut pre_out = PreProcessCollector{transform: self, out: out,time_service:time_service,};
            let rst = pre_process.process(row, &mut pre_out);
            self.pre_process = pre_process;
            rst?;
            Ok(())
        }
    }

    fn on_time(&mut self, time: u64, out: &mut dyn Collector) -> Result<()> {
        self.trigger_time_ms = 0;
        self.flush(out)
    }
}

struct RowResultFunction {
    result_projection: MutableProjection,
}

impl RowResultFunction {
    fn new(result_exprs: Vec<Expr>, input: Vec<AttributeReference>) -> Result<Self> {
        let expressions = BoundReference::bind_references(result_exprs, input)?;
        let result_projection = MutableProjection::new(expressions)?;
        Ok(Self { result_projection })
    }
}

struct RowAggregateFunction {
    agg_exprs: Vec<Expr>,
    agg_attributes: Vec<AttributeReference>,
    input_attrs: Vec<AttributeReference>,
    agg_buffer_len: usize,
    expr_agg_init: Projection,
    typed_functions: Vec<(usize, Box<dyn PhysicalTypedAggFunction>)>,
    process_row: ProcessRow,
    eavl_projection: Projection,
    agg_rst: GenericRow,
    empty_row: GenericRow,
}

impl RowAggregateFunction {
    fn new(agg_exprs: Vec<Expr>, agg_attributes: Vec<AttributeReference>, input_attrs: Vec<AttributeReference>) -> Result<Self> {
        let agg_exprs = Self::initialize_agg_functions(agg_exprs, input_attrs.clone())?;
        let mut agg_buffer_len = 0;
        let mut init_exprs = Vec::new();
        let mut eval_exprs = Vec::with_capacity(agg_exprs.len());
        let mut typed_functions = Vec::new();
        for (i, expr) in agg_exprs.iter().enumerate() {
            match expr {
                Expr::DeclarativeAggFunction(f) => {
                    for e in f.initial_values() {
                        init_exprs.push(e);
                    }
                    eval_exprs.push(f.evaluate_expression());
                    agg_buffer_len += f.agg_buffer_attributes().len();
                },
                Expr::TypedAggFunction(f) => {
                    init_exprs.push(Expr::NoOp);
                    eval_exprs.push(Expr::NoOp);
                    typed_functions.push((i, f.physical_function()?));
                    agg_buffer_len += 1;
                },
                _ => return Err(format!("not support agg expr:{:?}", expr))
            }
        }
        let expr_agg_init = Projection::new(init_exprs)?;
        let process_row = ProcessRow::new(&agg_exprs, agg_attributes.clone(), input_attrs.clone())?;
        let agg_rst = GenericRow::new_with_size(eval_exprs.len());
        let eavl_projection = Projection::new_with_input_attrs(eval_exprs, agg_attributes.clone())?;
        let empty_row = GenericRow::new(Vec::new());
        Ok(Self { agg_exprs, agg_attributes, input_attrs, agg_buffer_len, expr_agg_init, typed_functions, process_row, eavl_projection, agg_rst, empty_row })
    }

    fn initialize_agg_functions(agg_exprs: Vec<Expr>, input_attrs: Vec<AttributeReference>) -> Result<Vec<Expr>> {
        let mut exprs = Vec::with_capacity(agg_exprs.len());
        let mut mutable_buffer_offset = 0;
        for expr in agg_exprs {
            match expr {
                Expr::DeclarativeAggFunction(f) => {
                    mutable_buffer_offset += f.agg_buffer_attributes().len();
                    exprs.push(Expr::DeclarativeAggFunction(f));
                },
                e @ Expr::TypedAggFunction(_) => {
                    let expr = BoundReference::bind_reference(e, input_attrs.clone())?;
                    if let Expr::TypedAggFunction(f) = expr {
                        let func = f.with_new_mutable_agg_buffer_offset(mutable_buffer_offset);
                        exprs.push(Expr::TypedAggFunction(func));
                    } else {
                        return Err(format!("not support agg expr:{:?}", expr))
                    }
                    mutable_buffer_offset += 1;
                },
                _ => return Err(format!("not support agg expr:{:?}", expr))
            }
        }
        Ok(exprs)
    }
}


impl RowAggregateFunction {
    fn create_aggregation(&self) -> GenericRow {
        let mut buffer = GenericRow::new_with_size(self.agg_buffer_len);
        self.expr_agg_init.apply_targert(&mut buffer, &self.empty_row);
        for (_, func) in self.typed_functions.iter() {
            func.initialize(&mut buffer);
        }
        buffer
    }

    fn update(&self, buffer: &mut GenericRow, input: &dyn Row) {
        self.process_row.process(buffer, input);
    }

    fn eval(&mut self, buffer: &mut GenericRow) -> &GenericRow {
        self.eavl_projection.apply_targert(&mut self.agg_rst, buffer);
        for (i, func) in self.typed_functions.iter() {
            self.agg_rst.update(*i, func.eval(buffer));
        }
        &self.agg_rst
    }
}

struct ProcessRow {
    exprs: Vec<(usize, Arc<dyn PhysicalExpr>)>,
    functions: Vec<Box<dyn PhysicalTypedAggFunction>>
}

impl ProcessRow {
    fn new(agg_exprs: &Vec<Expr>, agg_attributes: Vec<AttributeReference>, input_attrs: Vec<AttributeReference>) -> Result<Self> {
        let mut update_exprs = Vec::new();
        let mut functions = Vec::new();
        for expr in agg_exprs {
            match expr {
                Expr::DeclarativeAggFunction(f) => {
                    for e in f.update_expressions() {
                        update_exprs.push(e);
                    }
                },
                Expr::TypedAggFunction(f) => {
                    update_exprs.push(Expr::NoOp);
                    functions.push(f.physical_function()?);
                },
                _ => panic!("not support agg expr:{:?}", expr)
            }
        }
        let input = agg_attributes.into_iter().chain(input_attrs.into_iter()).collect();
        let expressions = BoundReference::bind_references(update_exprs, input)?;
        let exprs: Result<Vec<(usize, Arc<dyn PhysicalExpr>)>, String> = expressions.iter().enumerate()
            .filter(|(_, expr)| !matches!(expr, Expr::NoOp))
            .map(|(i, expr)| create_physical_expr(expr).map(|expr| (i, expr))).collect();
        let exprs = exprs?;
        Ok(Self { exprs, functions})
    }

    fn process(&self, row: &mut GenericRow, input: &dyn Row)  {
        for (i, expr) in self.exprs.iter() {
            let joiner = JoinedRow::new(row, input) ;
            row.update(*i, expr.eval(&joiner));
        }
        for func in self.functions.iter() {
            func.update(row, input);
        }
    }
}

struct RowKeySelector {
    group_exprs: Vec<(usize, Arc<dyn PhysicalExpr>)>,
}

impl RowKeySelector {
    fn new(group_exprs: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        let group_exprs: Vec<(usize, Arc<dyn PhysicalExpr>)> = group_exprs.into_iter().enumerate().map(|(index, expr)| (index, expr)).collect();
        Self {group_exprs}
    }

    fn get_key(&self, row: &dyn Row) -> GenericRow {
        let mut key = GenericRow::new_with_size(self.group_exprs.len());
        for (index, expr) in &self.group_exprs {
            key.update(*index, expr.eval(row));
        }
        key
    }
}