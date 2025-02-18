use log::{info, error, debug};
use flexi_logger::with_thread;

fn main() {
    flexi_logger::Logger::try_with_str("info")
        .unwrap()
        .format(with_thread)
        .start()
        .unwrap();

    info!("This is an info message.");
    error!("This is an error message.");
    debug!("This is a debug message.");
}