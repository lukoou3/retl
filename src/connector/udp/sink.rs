use std::net::{UdpSocket, SocketAddr};
use std::time::Duration;
use log::error;
use crate::Result;
use crate::codecs::Serializer;
use crate::config::TaskContext;
use crate::connector::Sink;
use crate::data::Row;

#[derive(Debug)]
pub struct UdpSink {
    task_context: TaskContext,
    target_addr: SocketAddr,
    socket: UdpSocket,
    serializer: Box<dyn Serializer>,
}

impl UdpSink {
    pub fn new(
        task_context: TaskContext,
        hostname: String,
        port: u16,
        serializer: Box<dyn Serializer>,
    ) -> Result<Self> {
        let target_addr = format!("{}:{}", hostname, port).parse::<SocketAddr>().map_err(|e| e.to_string())?;
        let socket = UdpSocket::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        socket.set_write_timeout(Some(Duration::from_secs(1))).map_err(|e| e.to_string())?;
        Ok(Self{task_context, target_addr, socket, serializer,})
    }
}

impl Sink for UdpSink {
    fn invoke(&mut self, row: &dyn Row) -> Result<()> {
        let bytes = self.serializer.serialize(row)?;
        match self.socket.send_to(&bytes, self.target_addr) {
            Ok(_sent_len) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                error!("UDP send timed out: {}", e);
                Ok(())
            }
            Err(e) => {
                error!("UDP send error: {}", e);
                Ok(())
            }
        }
    }

}