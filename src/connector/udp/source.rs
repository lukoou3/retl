use std::net::UdpSocket;
use std::time::{Instant, Duration};
use crate::Result;
use crate::codecs::Deserializer;
use crate::config::TaskContext;
use crate::connector::Source;
use crate::execution::{Collector, PollStatus};
use crate::types::Schema;

#[derive(Debug)]
pub struct UdpSource {
    task_context: TaskContext,
    schema: Schema,
    socket: UdpSocket,
    buffer: Vec<u8>,
    deserializer: Box<dyn Deserializer>,
    poll_start: Instant,
}

impl UdpSource {
    pub fn new(
        task_context: TaskContext,
        schema: Schema,
        hostname: String,
        port: u16,
        buffer_size: usize,
        deserializer: Box<dyn Deserializer>,
    ) -> Result<Self> {
        let addr = format!("{}:{}", hostname, port);
        let socket = UdpSocket::bind(&addr).map_err(|e| e.to_string())?;
        // 设置非阻塞模式，recv_from 无数据时返回 WouldBlock
        socket.set_nonblocking(true).map_err(|e| e.to_string())?;
        let buffer = vec![0u8; buffer_size];
        Ok(Self {
            task_context,
            schema,
            socket,
            buffer,
            deserializer,
            poll_start: Instant::now(),
        })
    }
}

impl Source for UdpSource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn poll_next(&mut self, out: &mut dyn Collector) -> Result<PollStatus> {
        let start = self.poll_start;
        loop {
            match self.socket.recv_from(&mut self.buffer) {
                Ok((len, _src_addr)) => {
                    // 重置超时计时
                    self.poll_start = Instant::now();
                    // 空数据包，跳过
                    if len == 0 {
                        return Ok(PollStatus::More);
                    }
                    let data = &self.buffer[..len];
                    let row = self.deserializer.deserialize(data)?;
                    out.collect(row)?;
                    return Ok(PollStatus::More);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if start.elapsed() >= Duration::from_secs(1) {
                        // 超时，重置时间，返回 More 继续轮询
                        self.poll_start = Instant::now();
                        return Ok(PollStatus::More);
                    } else {
                        // 无数据，休眠 10ms，减少 CPU 占用
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                Err(e) => return Err(e.to_string()),
            }
        }
    }

}