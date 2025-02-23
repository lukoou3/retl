use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;
use serde::{Serialize, Serializer};
use crate::config::{SinkConfig, SinkOuter, SourceConfig, SourceOuter, TransformConfig, TransformOuter};
use crate::Result;
use crate::types::Schema;
use crate::config::{AppConfig};
use crate::parser;

#[derive(Debug, Clone)]
pub enum Node {
    Null,
    Source(SourceNode),
    Transform(TransformNode),
    Sink(SinkNode),
}

impl Node {
    pub fn is_null(&self) -> bool {
        match self {
            Node::Null => true,
            _ => false,
        }
    }
}

impl serde::ser::Serialize for Node {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        match self {
            Node::Null => serializer.serialize_none(),
            Node::Source(node) => node.serialize(serializer),
            Node::Transform(node) => node.serialize(serializer),
            Node::Sink(node) => node.serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceNode {
    pub schema: Schema,
    pub source_config: SourceOuter,
}

impl SourceNode {
    pub fn new(schema: Schema, config: SourceOuter) -> Self {
        Self { schema, source_config: config }
    }
}

#[derive(Debug, Clone)]
pub struct TransformNode {
    pub input: Arc<Node>,
    pub transform_config: TransformOuter,
}

impl TransformNode {
    pub fn new(input: Arc<Node>, config: TransformOuter) -> Self {
        Self { input, transform_config: config }
    }
}

impl serde::ser::Serialize for TransformNode {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        use serde::ser::SerializeMap;
        let mut compound = serializer.serialize_map(None)?;
        compound.serialize_key("transform_config")?;
        compound.serialize_value(&self.transform_config)?;
        compound.serialize_key("input")?;
        compound.serialize_value(self.input.as_ref())?;
        compound.end()
    }
}

#[derive(Debug, Clone)]
pub struct SinkNode {
    pub input: Arc<Node>,
    pub sink_config: SinkOuter,
}

impl serde::ser::Serialize for SinkNode {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        use serde::ser::SerializeMap;
        let mut compound = serializer.serialize_map(None)?;
        compound.serialize_key("sink_config")?;
        compound.serialize_value(&self.sink_config)?;
        compound.serialize_key("input")?;
        compound.serialize_value(self.input.as_ref())?;
        compound.end()
    }
}

impl SinkNode {
    pub fn new(input: Arc<Node>, config: SinkOuter) -> Self {
        Self { input, sink_config: config }
    }
}

pub struct NodeParser {
    unparsed_output_node_dict: HashMap<String, Node>,
    output_node_dict: HashMap<String, Arc<Node>>,
}

impl NodeParser {
    pub fn new() -> Self {
        Self { unparsed_output_node_dict: HashMap::new(), output_node_dict: HashMap::new() }
    }

    pub fn parse_sink_nodes(&mut self, config: &AppConfig) -> Result<Vec<SinkNode>> {
        let null_node = Arc::new(Node::Null);
        for source in config.sources.iter() {
            let output = source.outputs[0].clone();
            let schema = parser::parse_schema(&source.schema)?;
            self.unparsed_output_node_dict.insert(output, Node::Source(SourceNode::new(schema, source.clone())));
        };
        for transform in config.transforms.iter() {
            let output = transform.outputs[0].clone();
            self.unparsed_output_node_dict.insert(output, Node::Transform(TransformNode::new(null_node.clone(), transform.clone())));
        }

        let mut sink_nodes = Vec::with_capacity(config.sinks.len());
        for sink in config.sinks.iter() {
            let input = &sink.inputs[0];
            let node =if let Some(node) = self.output_node_dict.get(input) {
                node.clone()
            } else {
                self.parse_input_node(input)?
            };
            sink_nodes.push(SinkNode::new(node, sink.clone()));
        }

        Ok(sink_nodes)
    }

    fn parse_input_node(&mut self, input: &String) -> Result<Arc<Node>> {
        if let Some(node) = self.unparsed_output_node_dict.get(input) {
            let node = node.clone();
            match &node {
                Node::Source(_) => {
                    let sourde_node = Arc::new(node.clone());
                    self.output_node_dict.insert(input.clone(), sourde_node.clone());
                    Ok(sourde_node)
                },
                Node::Transform(transform_node) => {
                    let input = &transform_node.transform_config.inputs[0];
                    let input_node = self.parse_input_node(input)?;
                    let transform_node = Arc::new(Node::Transform(TransformNode::new(input_node, transform_node.transform_config.clone())));
                    self.output_node_dict.insert(input.clone(), transform_node.clone());
                    Ok(transform_node)
                },
                Node::Null | Node::Sink(_) => Err(format!("node not need parse input node:{:?}", node)),
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
        let sink_nodes = NodeParser::new().parse_sink_nodes(&config).unwrap();
        println!("\n{}", serde_yaml::to_string(&sink_nodes).unwrap());
        println!("\n{}", serde_json::to_string_pretty(&sink_nodes).unwrap());
        //println!("\n{:#?}", sink_nodes);
    }

}


