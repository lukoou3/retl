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
    fn open(&mut self) -> Result<()> {
        self.source.open()?;
        self.out.open()
    }

    pub fn run(&mut self) -> Result<()> {
        self.source.run(self.out.as_mut())
    }

    fn close(&mut self) -> Result<()> {
        self.source.close().and(self.out.close())
    }
}

pub fn new_source_operator(id: u16, graph: &Graph) -> Result<SourceOperator> {
    let node = graph.node_dict.get(&id).unwrap();
    if let Node::Source(source_node) = node {
        let config = &source_node.source_config.inner;
        let schema = parse_schema(&source_node.source_config.schema)?;
        let source = config.build(schema)?.create_source()?;
        let mut outs = Vec::new();
        for ouput_id in source_node.ouput_ids.iter() {
            let next_node = graph.node_dict.get(ouput_id).unwrap();
            let out_schema = source.schema().clone();
            let out = if !next_node.is_sink() {
                new_transform_collector(next_node, graph, out_schema)?
            } else {
                new_sink_operator(next_node, out_schema)?
            };
            outs.push(out);
        };
        let out = if outs.len() == 1 {
            outs.into_iter().next().unwrap()
        } else {
            Box::new(MultiCollector::new(outs))
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
        let mut outs = Vec::new();
        for ouput_id in transform_node.ouput_ids.iter() {
            let next_node = graph.node_dict.get(ouput_id).unwrap();
            let out_schema = transform.schema().clone();
            let out = if !next_node.is_sink() {
                new_transform_collector(next_node, graph, out_schema)?
            } else {
                new_sink_operator(next_node, out_schema)?
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
    let mut handles = Vec::with_capacity(graph.source_ids.len());
    for (i, source_id) in graph.source_ids.iter().enumerate() {
        let source_id = *source_id;
        let graph = graph.clone();
        handles.push(std::thread::Builder::new().stack_size(1024 * 1024).name(format!("etl-{}/{}", i + 1, graph.source_ids.len())).spawn(move || {
            println!("start source: {}", source_id);
            let mut source = new_source_operator(source_id, &graph)?;
            source.open()?;
            source.run()?;
            source.close()
        }).map_err(|_| "failed to spawn thread")?);
    }
    for handle in handles {
        handle.join().unwrap()?;
    }
    Ok(())
}
