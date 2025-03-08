use log::{error, info};
use flexi_logger::with_thread;
use std::{env, thread};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;
use prometheus::{Encoder, Registry, TextEncoder};
use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag;
use tiny_http::{Request, Response, Server};
use crate::config::{self, AppConfig, WebConfig};
use crate::execution::{self, NodeParser};
fn handle_request(registry: &Registry, mut request: Request) {
    match request.url() {
        "/metrics" => {
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            let mut buffer = vec![];
            encoder.encode(&metric_families, &mut buffer).unwrap();

            let response = Response::from_data(buffer)
                .with_header(tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain; version=0.0.4"[..]).unwrap());
            request.respond(response).unwrap();
        },
        _ => {
            let response = Response::from_string("Hello, World!")
                .with_status_code(200);
            request.respond(response).unwrap();
        }
    }
}
fn start_web(web_config: WebConfig, registry: Registry,  terminated: Arc<AtomicBool>) -> crate::Result<Vec<JoinHandle<()>>> {
    let server = Arc::new(Server::http(format!("0.0.0.0:{}", web_config.port)).map_err(|e| e.to_string())?);
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    for i in 0..web_config.works {
        let registry = registry.clone();
        let server = server.clone();
        let terminated = Arc::clone(&terminated);
        let builder = thread::Builder::new().stack_size(1024 * 256).name(format!("web-{}", i));
        let handle = builder.spawn(move || {
            while !terminated.load(Ordering::Acquire) {
                match server.recv_timeout(Duration::from_millis(100)) {
                    Ok(Some(request)) => handle_request(&registry, request),
                    Ok(None) => continue,
                    Err(_) => continue,
                }
            }
            info!("thread {} exiting", i);
        }).unwrap();
        handles.push(handle);
    }
    Ok(handles)
}

pub fn run_application() -> crate::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let config_path = if args.len() > 0 {
        args[0].as_str()
    } else {
        "config/application.yaml"
    };
    println!("config path: {}", config_path);
    let config: AppConfig = crate::config::parse_config(config_path).unwrap();
    let mut parser = NodeParser::new();
    let graph = parser.parse_node_graph(&config)?;
    graph.print_node_chains();

    let registry = Registry::new();
    let terminated: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    // 捕获 SIGINT (Ctrl+C) 和 SIGTERM (kill) 信号
    flag::register(SIGINT, terminated.clone()).expect("Failed to register SIGINT handler");
    flag::register(SIGTERM, terminated.clone()).expect("Failed to register SIGTERM handler");
    let handles = start_web(config.env.web.clone(), registry.clone(), terminated.clone())?;
    let result = execution::execution_graph(&graph, &config.env.application, registry, terminated.clone());
    info!("execution finish");
    terminated.store(true, Ordering::Release);
    for handle in handles {
        handle.join().unwrap();
    }
    result
}