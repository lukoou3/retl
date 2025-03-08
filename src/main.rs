use log::{error, info};
use flexi_logger::with_thread;
use retl::execution::application;

fn main() {
    flexi_logger::Logger::try_with_str("info")
        .unwrap()
        .format(with_thread)
        .start()
        .unwrap();
    if let Err(e) = application::run_application() {
        error!("execution error {}", e);
    } else {
        info!("execution success")
    }
}
