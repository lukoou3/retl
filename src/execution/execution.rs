use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use prometheus::Registry;
use crate::config::{ApplicationConfig, BaseIOMetrics, OperatorConfig, TaskConfig, TaskContext};
use crate::Result;
use crate::connector::Source;
use crate::execution::{Collector, Graph, MultiCollector, Node, SinkCollector, TransformCollector};
use crate::parser::parse_schema;
use crate::types::Schema;

struct SourceOperator {
    source: Box<dyn Source>,
    out: Box<dyn Collector>,
}

impl SourceOperator {
    fn new(source: Box<dyn Source>, out: Box<dyn Collector>) -> SourceOperator {
       SourceOperator{source, out}
    }
}

impl SourceOperator {
    fn open(&mut self) -> Result<()> {
        self.source.open()?;
        self.out.open()
    }

    pub fn run(&mut self, terminated: Arc<AtomicBool>) -> Result<()> {
        self.source.run(self.out.as_mut(), terminated)
    }

    fn close(&mut self) -> Result<()> {
        self.source.close().and(self.out.close())
    }
}

pub fn new_source_operator(id: u16, graph: &Graph, task_config: TaskConfig) -> Result<SourceOperator> {
    let node = graph.node_dict.get(&id).unwrap().as_ref();
    if let Node::Source(source_node) = node {
        let config = &source_node.source_config.inner;
        let schema = parse_schema(&source_node.source_config.schema)?;
        let base_iometrics = Arc::new(BaseIOMetrics::new(&task_config.metrics_registry, format!("source{}_{}", source_node.id, task_config.subtask_index)));
        let task_context = TaskContext::new(task_config.clone(), OperatorConfig::new(source_node.id), base_iometrics);
        let source = config.build(schema)?.create_source(task_context)?;
        let mut outs = Vec::new();
        for ouput_id in source_node.ouput_ids.iter() {
            let next_node = graph.node_dict.get(ouput_id).unwrap().as_ref();
            let out_schema = source.schema().clone();
            let out = if !next_node.is_sink() {
                new_transform_collector(next_node, graph, task_config.clone(), out_schema)?
            } else {
                new_sink_operator(next_node, task_config.clone(), out_schema)?
            };
            outs.push(out);
        };
        let out = if outs.len() == 1 {
            outs.into_iter().next().unwrap()
        } else {
            Box::new(MultiCollector::new(outs))
        };
        Ok(SourceOperator::new(source, out))
    } else {
        Err(format!("not a source node: {:?}", node))
    }
}

pub fn new_transform_collector(node: &Node, graph: &Graph, task_config: TaskConfig, schema: Schema) -> Result<Box<dyn Collector>> {
    if let Node::Transform(transform_node) = node {
        let config = &transform_node.transform_config.inner;
        let base_iometrics = Arc::new(BaseIOMetrics::new(&task_config.metrics_registry, format!("transform{}_{}", transform_node.id, task_config.subtask_index)));
        let task_context = TaskContext::new(task_config.clone(), OperatorConfig::new(transform_node.id), base_iometrics);
        let transform = config.build(schema)?.create_transform(task_context)?;
        let mut outs = Vec::new();
        for ouput_id in transform_node.ouput_ids.iter() {
            let next_node = graph.node_dict.get(ouput_id).unwrap().as_ref();
            let out_schema = transform.schema().clone();
            let out = if !next_node.is_sink() {
                new_transform_collector(next_node, graph, task_config.clone(), out_schema)?
            } else {
                new_sink_operator(next_node, task_config.clone(), out_schema)?
            };
            outs.push(out);
        };
        let out = if outs.len() == 1 {
            outs.into_iter().next().unwrap()
        } else {
            Box::new(MultiCollector::new(outs))
        };
        Ok(Box::new(TransformCollector::new(transform, out)))
    } else {
        Err(format!("not a transform node: {:?}", node))
    }
}

pub fn new_sink_operator(node: &Node, task_config: TaskConfig, schema: Schema) -> Result<Box<dyn Collector>> {
    if let Node::Sink(sink_node) = node {
        let config = &sink_node.sink_config.inner;
        let base_iometrics = Arc::new(BaseIOMetrics::new(&task_config.metrics_registry, format!("sink{}_{}", sink_node.id, task_config.subtask_index)));
        let task_context = TaskContext::new(task_config, OperatorConfig::new(sink_node.id), base_iometrics);
        let sink = config.build(schema)?.create_sink(task_context)?;
        Ok(Box::new(SinkCollector::new(sink)))
    } else {
        Err(format!("not a sink node: {:?}", node))
    }
}

pub fn execution_graph(graph: &Graph, application_config: &ApplicationConfig, registry: Registry, terminated: Arc<AtomicBool>) -> Result<()> {
    let mut handles = Vec::with_capacity(graph.source_ids.len());
    for (i, source_id) in graph.source_ids.iter().enumerate() {
        let source_id = *source_id;
        let graph = graph.clone();
        let task_config = TaskConfig::new(1, 0, registry.clone());
        let terminated = terminated.clone();
        let builder = thread::Builder::new().stack_size(1024 * 512)
            .name(format!("{}-{}/{}", graph.get_node_dispaly_by_id(source_id), 1, 1));
        handles.push(builder.spawn(move || {
            println!("start source: {}", source_id);
            let mut source = new_source_operator(source_id, &graph, task_config)?;
            source.open()?;
            source.run(terminated)?;
            source.close()
        }).map_err(|_| "failed to spawn thread")?);
    }
    for handle in handles {
        handle.join().unwrap()?;
    }
    Ok(())
}
