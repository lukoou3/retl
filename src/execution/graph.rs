use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;
use serde::{Serialize, Serializer};
use crate::config::{SinkConfig, SinkOuter, SourceConfig, SourceOuter, TransformConfig, TransformOuter};
use crate::Result;
use crate::types::Schema;
use crate::config::{AppConfig};
use crate::parser;

#[derive(Debug, Clone)]
pub enum Node {
    Source(SourceNode),
    Transform(TransformNode),
    Sink(SinkNode),
}

impl Node {

    pub fn is_sink(&self) -> bool {
        match self {
            Node::Sink(_) => true,
            _ => false,
        }
    }

    pub fn input_id(&self) -> u16 {
        match self {
            Node::Transform(node) => node.input_id,
            Node::Sink(node) => node.input_id,
            Node::Source(node) => panic!("source node has not input id"),
        }
    }

    pub fn id(&self) -> u16 {
        match self {
            Node::Source(node) => node.id,
            Node::Transform(node) => node.id,
            Node::Sink(node) => node.id,
        }
    }

    pub fn output_ids(&self) -> &Vec<u16> {
        match self {
            Node::Source(node) => &node.ouput_ids,
            Node::Transform(node) => &node.ouput_ids,
            Node::Sink(_) => panic!("sink node has not output ids"),
        }
    }

    pub fn add_output_id(&mut self, output_id: u16) {
        match self {
            Node::Source(node) => node.ouput_ids.push(output_id),
            Node::Transform(node) => node.ouput_ids.push(output_id),
            Node::Sink(_) => panic!("sink node has not output id"),
        }
    }
}

impl serde::ser::Serialize for Node {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        match self {
            Node::Source(node) => node.serialize(serializer),
            Node::Transform(node) => node.serialize(serializer),
            Node::Sink(node) => node.serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceNode {
    pub id: u16,
    pub ouput_ids: Vec<u16>,
    pub schema: Schema,
    pub source_config: SourceOuter,
}

impl SourceNode {
    pub fn new_unparsed(schema: Schema, source_config: SourceOuter) -> Self {
        Self { id:0, ouput_ids: vec![], schema, source_config }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TransformNode {
    pub id: u16,
    pub input_id: u16,
    pub ouput_ids: Vec<u16>,
    pub transform_config: TransformOuter,
}

impl TransformNode {
    pub fn new_unparsed(transform_config: TransformOuter) -> Self {
        Self { id:0, input_id:0, ouput_ids: vec![], transform_config }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SinkNode {
    pub id: u16,
    pub input_id: u16,
    pub sink_config: SinkOuter,
}

impl SinkNode {
    pub fn new_unparsed(sink_config: SinkOuter) -> Self {
        Self { id:0, input_id:0, sink_config }
    }
}

struct NodeIdGenerator {
    id: std::sync::atomic::AtomicU16,
}

impl NodeIdGenerator {
    fn get_next_node_id() -> u16 {
        static INSTANCE: NodeIdGenerator = NodeIdGenerator {
            id: std::sync::atomic::AtomicU16::new(1),
        };
        INSTANCE.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(Debug, Clone)]
pub struct Graph {
    pub source_ids: Vec<u16>,
    pub node_dict: HashMap<u16, Node>,
}

impl Graph {
    pub fn print_node_chains(&self) {
        for id in self.source_ids.iter() {
            println!("source id: {}", id);
            let ids = vec![*id];
            self.print_node_chains_on_sink(ids, id)
        }
    }

    fn print_node_chains_on_sink(&self, mut ids: Vec<u16>, id: & u16){
        let node = self.node_dict.get(id).unwrap();
        if node.is_sink() {
            println!("{:?}", ids);
        } else {
            for id in node.output_ids() {
                let mut ids_clone = ids.clone();
                ids_clone.push(*id);
                self.print_node_chains_on_sink(ids_clone, id);
            }
        }
    }
}

pub struct NodeParser {
    unparsed_output_node_dict: HashMap<String, Rc<RefCell<Node>>>,
    output_node_dict: HashMap<String, Rc<RefCell<Node>>>,
    pub node_dict: HashMap<u16, Rc<RefCell<Node>>>,
    pub source_ids: Vec<u16>,
}

impl NodeParser {
    pub fn new() -> Self {
        Self { unparsed_output_node_dict: HashMap::new(), output_node_dict: HashMap::new(), node_dict: HashMap::new(), source_ids: vec![] }
    }

    pub fn parse_node_graph(&mut self, config: &AppConfig) -> Result<Graph> {
        for source in config.sources.iter() {
            let output = source.outputs[0].clone();
            let schema = parser::parse_schema(&source.schema)?;
            let node = Rc::new(RefCell::new(Node::Source(SourceNode::new_unparsed(schema, source.clone()))));
            self.unparsed_output_node_dict.insert(output, node);
        };
        for transform in config.transforms.iter() {
            let output = transform.outputs[0].clone();
            let node = Rc::new(RefCell::new(Node::Transform(TransformNode::new_unparsed(transform.clone()))));
            self.unparsed_output_node_dict.insert(output, node);
        }

        for sink in config.sinks.iter() {
            let input = &sink.inputs[0];
            let mut in_node = if let Some(node) = self.output_node_dict.get(input) {
                node.clone()
            } else {
                let in_node = self.parse_input_node(input)?;
                //println!("end parse input node: {}", input);
                in_node
            };
            let mut node = SinkNode::new_unparsed(sink.clone());
            node.input_id = in_node.borrow().id();
            node.id = NodeIdGenerator::get_next_node_id();
            in_node.borrow_mut().add_output_id(node.id);
            self.node_dict.insert(node.id,  Rc::new(RefCell::new(Node::Sink(node))));
        }

        let source_ids = self.source_ids.clone();
        let node_dict = self.node_dict.iter().map(|(i, node)| (*i, node.borrow().clone())).collect();
        Ok(Graph{ source_ids, node_dict})
    }

    fn parse_input_node(&mut self, input: &String) -> Result<Rc<RefCell<Node>>> {
        //println!("parse input node: {}", input);
        if let Some(node) = self.unparsed_output_node_dict.get(input) {
            let node = node.clone();
            match &mut *node.borrow_mut() {
                Node::Source(sourde_node) => {
                    sourde_node.id = NodeIdGenerator::get_next_node_id();
                    self.source_ids.push(sourde_node.id);
                    self.output_node_dict.insert(input.clone(), node.clone());
                    self.node_dict.insert(sourde_node.id, node.clone());
                    Ok(node.clone())
                },
                Node::Transform(transform_node) => {
                    let transform_input = &transform_node.transform_config.inputs[0];
                    let input_node = if let Some(node) = self.output_node_dict.get(transform_input) {
                        node.clone()
                    } else {
                        let input_node = self.parse_input_node(transform_input)?;
                        self.output_node_dict.insert(transform_input.clone(), input_node.clone());
                        //println!("end parse input node: {}", transform_input);
                        input_node
                    };
                    transform_node.input_id = input_node.borrow().id();
                    transform_node.id = NodeIdGenerator::get_next_node_id();
                    input_node.borrow_mut().add_output_id(transform_node.id);
                    self.output_node_dict.insert(input.clone(), node.clone());
                    self.node_dict.insert(transform_node.id, node.clone());
                    Ok(node.clone())
                },
                Node::Sink(_) => Err(format!("sink node not need parse input node:{:?}", node)),
            }
        } else {
            Err(format!("input:{} not found", input))
        }
    }

}



#[cfg(test)]
mod tests {
    use crate::config::parse_config;
    use super::*;

    #[test]
    fn test_config()  {
        let config_path = "config/application.yaml";
        let config: AppConfig = parse_config(config_path).unwrap();
        println!("{:#?}", config);
        println!("{}", serde_yaml::to_string(&config).unwrap());
        let mut parser = NodeParser::new();
        let graph = parser.parse_node_graph(&config).unwrap();
        println!("\nsource_ids:{:?}", graph.source_ids);
        graph.print_node_chains();
        //println!("\n{:#?}", parser.node_dict);
        //println!("\n{:#?}", parser.unparsed_output_node_dict);
        // println!("\n{}", serde_json::to_string_pretty(&sink_nodes).unwrap());
    }

}


