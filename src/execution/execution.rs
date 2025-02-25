use crate::Result;
use crate::connector::Source;
use crate::execution::{Collector, Graph, Node, SinkCollector, TransformCollector};
use crate::parser::parse_schema;
use crate::types::Schema;

struct SourceOperator {
    source: Box<dyn Source>,
    out: Box<dyn Collector>,
}

impl SourceOperator {
    pub fn run(&mut self) -> Result<()> {
        self.source.open()?;
        self.source.run(self.out.as_mut())
    }
}

pub fn new_source_operator(id: u16, graph: &Graph) -> Result<SourceOperator> {
    let node = graph.node_dict.get(&id).unwrap();
    if let Node::Source(source_node) = node {
        let config = &source_node.source_config.inner;
        let schema = parse_schema(&source_node.source_config.schema)?;
        let source = config.build(schema)?.create_source()?;
        let next_node = graph.node_dict.get(&source_node.ouput_ids[0]).unwrap();
        let out_schema = source.schema().clone();
        let out = if !next_node.is_sink() {
            new_transform_collector(next_node, graph, out_schema)?
        } else {
            new_sink_operator(next_node, out_schema)?
        };
        Ok(SourceOperator{source, out})
    } else {
        Err(format!("not a source node: {:?}", node))
    }
}

pub fn new_transform_collector(node: &Node, graph: &Graph, schema: Schema) -> Result<Box<dyn Collector>> {
    if let Node::Transform(transform_node) = node {
        let config = &transform_node.transform_config.inner;
        let transform = config.build(schema)?.create_transform()?;
        let next_node = graph.node_dict.get(&transform_node.ouput_ids[0]).unwrap();
        let out_schema = transform.schema().clone();
        let out = if !next_node.is_sink() {
            new_transform_collector(next_node, graph, out_schema)?
        } else {
            new_sink_operator(next_node, out_schema)?
        };
        Ok(Box::new(TransformCollector::new(transform, out)))
    } else {
        Err(format!("not a transform node: {:?}", node))
    }
}

pub fn new_sink_operator(node: &Node, schema: Schema) -> Result<Box<dyn Collector>> {
    if let Node::Sink(sink_node) = node {
        let config = &sink_node.sink_config.inner;
        let sink = config.build(schema)?.create_sink()?;
        Ok(Box::new(SinkCollector::new(sink)))
    } else {
        Err(format!("not a sink node: {:?}", node))
    }
}

pub fn execution_graph(graph: &Graph) -> Result<()> {
    let mut source = new_source_operator(graph.source_ids[0], graph)?;
    source.run()
}
