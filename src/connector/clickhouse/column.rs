use std::cmp::min;
use std::io;
use std::io::Read;
use std::sync::{Arc, Mutex};
use bytes::BufMut;
use crate::buffer_block::BufferBlock;
use crate::buffer_pool::BufferPool;
use crate::Result;
use crate::connector::clickhouse::{lz4, make_value_converter, ClickHouseType, ClickHouseValue as Value, ClickHouseValue, ToCkValueConverter};
use crate::data::Row;
use crate::types::DataType;

const BLOCK_BUFFER_SIZE: usize = 1024 * 256;

#[derive(Clone, Debug)]
pub struct ColumnDesc {
    name: String,
    data_type: DataType,
    ck_type: ClickHouseType,
}

impl ColumnDesc {
    pub fn new(name: impl Into<String>, data_type: DataType, ck_type: ClickHouseType) -> Self {
        let name = name.into();
        ColumnDesc { name, data_type, ck_type, }
    }
}

#[derive(Clone)]
pub struct ArcBlockReader {
    pub block: Arc<Mutex<Block>>,
}

impl ArcBlockReader {
    pub fn from(block: Arc<Mutex<Block>>) -> Self {
        ArcBlockReader {block}
    }

    pub fn rows(&self) -> usize {
        self.block.lock().unwrap().rows
    }

    pub fn byte_size(&self) -> usize {
        self.block.lock().unwrap().bytes
    }

    pub fn realease_buffer(&mut self) {
        self.block.lock().unwrap().release_buffer();
    }
}

impl Read for ArcBlockReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut block = self.block.lock().unwrap();
        block.read(buf)
    }

}

#[derive(Clone)]
pub struct ArcCompressBlockReader {
    block: Arc<Mutex<Block>>,
    uncompress_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    compress_size: usize,
    read_offset: usize,
}

impl ArcCompressBlockReader {
    pub fn from(block: Arc<Mutex<Block>>) -> Self {
        let compress_buf_size: usize = 1024 * 1024 * 1; // 压缩 缓冲区
        ArcCompressBlockReader {
            block,
            uncompress_buf: vec![0; compress_buf_size],
            compress_buf: vec![0; lz4::max_compressed_size(compress_buf_size)],
            compress_size: 0,
            read_offset: 0,
        }
    }
}

impl Read for ArcCompressBlockReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut block = self.block.lock().unwrap();
        // 如果 compress_buf 已读完或为空，则重新填充
        if self.read_offset >= self.compress_size {
            // 从 block 读取数据到 uncompress_buf
            let bytes_read = block.read(&mut self.uncompress_buf)?;

            // 如果没有读取到数据，表示已到末尾
            if bytes_read == 0 {
                return Ok(0);
            }

            // 清空 compress_buf 并压缩数据
            self.compress_size = lz4::compress_into(&self.uncompress_buf[..bytes_read], &mut self.compress_buf).map_err(|_| io::Error::other("Failed to compress data"))?;
            self.read_offset = 0; // 重置偏移
        }

        // 计算可以从 compress_buf 读取的字节数
        let available = self.compress_size - self.read_offset;
        let to_read = min(buf.len(), available);

        // 将 compress_buf 中的数据拷贝到输出缓冲区
        buf[..to_read].copy_from_slice(
            &self.compress_buf[self.read_offset..self.read_offset + to_read]
        );
        self.read_offset += to_read;

        Ok(to_read)
    }
}

pub struct Block {
    columns: Vec<Column>,
    value_converters: Vec<ValueConverter>,
    rows: usize,
    bytes: usize,
    header_bytes: Vec<u8>,
    read_pos: usize,                // 当前读取的 column 索引
    read_header_offset: usize,
}

impl Block {
    pub fn new( pool: BufferPool, column_descs: Vec<ColumnDesc>) -> Result<Self> {
        let mut columns = Vec::with_capacity(column_descs.len());
        let mut value_converters = Vec::with_capacity(column_descs.len());
        for column_desc in column_descs {
            columns.push(Column::new(pool.clone(), column_desc.name, column_desc.ck_type.clone())?);
            value_converters.push(ValueConverter {
                data: Value::Null,
                converter: make_value_converter(column_desc.data_type, column_desc.ck_type)?,
            });
        }
        Ok(Block {
            columns,
            value_converters,
            rows: 0,
            bytes: 0,
            header_bytes: Vec::new(),
            read_pos: 0,
            read_header_offset: 0,
        })
    }

    pub fn write(&mut self, row: &dyn Row) -> Result<()> {
        for (i, converter) in self.value_converters.iter_mut().enumerate() {
            let value = row.get(i);
            converter.data = converter.converter.convert(value)?;
        }

        let mut bytes = 0;
        for (column, converter) in self.columns.iter_mut().zip(self.value_converters.iter()) {
            bytes += column.write(&converter.data);
        }

        self.rows += 1;
        self.bytes += bytes;

        Ok(())
    }

    #[inline]
    pub fn rows(&self) -> usize {
        self.rows
    }

    #[inline]
    pub fn byte_size(&self) -> usize {
        self.bytes
    }

    pub fn read_reset(&mut self) {
        self.read_pos = 0;
        self.columns.iter_mut().for_each(|column| column.read_reset());
        self.header_bytes.clear();
        self.read_header_offset = 0;
    }

    pub fn release_buffer(&mut self) {
        for column in self.columns.iter_mut() {
            column.release_buffer();
        }
    }
}

impl Read for Block {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.header_bytes.len() == 0 {
            put_unsigned_leb128(&mut self.header_bytes, self.columns.len() as u64);
            put_unsigned_leb128(&mut self.header_bytes, self.rows as u64);
        }

        let mut total_read = 0;

        // 1. 先读取header_bytes
        if self.read_header_offset < self.header_bytes.len() {
            let header_remaining = self.header_bytes.len() - self.read_header_offset;
            let header_to_read = min(buf.len(), header_remaining);
            buf[..header_to_read].copy_from_slice(&self.header_bytes[self.read_header_offset..self.read_header_offset + header_to_read]);
            self.read_header_offset += header_to_read;
            total_read += header_to_read;

            // 如果buf已满，直接返回
            if total_read == buf.len() {
                return Ok(total_read);
            }
        }

        // 2. header读完后，读取columns
        while total_read < buf.len() && self.read_pos < self.columns.len() {
            let column = &mut self.columns[self.read_pos];
            let bytes_read = column.read(&mut buf[total_read..])?;
            total_read += bytes_read;

            if bytes_read == 0 {
                self.read_pos += 1;
            }
        }

        Ok(total_read)
    }
}

struct ValueConverter {
    data: ClickHouseValue,
    converter: Box<dyn ToCkValueConverter>,
}

pub struct Column {
    name: String,
    data: Box<dyn ColumnData>,
    name_bytes: Vec<u8>,
    read_name_offset: usize,
}

impl Column {
    pub fn new(buffer_pool: BufferPool, name: String, ck_type: ClickHouseType) -> Result<Self> {
        let type_name = ck_type.to_string().to_string();
        let mut name_bytes = Vec::with_capacity(name.len() + 1 + type_name.len() + 1);
        put_unsigned_leb128(&mut name_bytes, name.len() as u64);
        name_bytes.extend_from_slice(name.as_bytes());
        put_unsigned_leb128(&mut name_bytes, type_name.len() as u64);
        name_bytes.extend_from_slice(type_name.as_bytes());
        let data = new_column_data(buffer_pool, ck_type)?;
        Ok(Self{name, data, name_bytes, read_name_offset: 0})
    }

    #[inline]
    pub fn write(&mut self, value: &Value) -> usize {
        self.data.write(value)
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // 1. 检查是否还有数据可读, 首先检查 name_bytes 是否已读完
        if self.read_name_offset >= self.name_bytes.len() {
            // name_bytes 已读完，直接从 data 读取
            return self.data.read(buf);
        }

        // 2. 计算还需要读取多少 null 标志
        let remaining_name_bytes = self.name_bytes.len() - self.read_name_offset;
        let name_bytes_to_read = min(remaining_name_bytes, buf.len());

        // 3. 如果 buffer 空间足够，先读取 name_bytes
        if name_bytes_to_read > 0 {
            buf[..name_bytes_to_read].copy_from_slice(&self.name_bytes[self.read_name_offset..self.read_name_offset + name_bytes_to_read]);
            self.read_name_offset += name_bytes_to_read;

            // 如果 buffer 已满，直接返回
            if name_bytes_to_read == buf.len() {
                return Ok(name_bytes_to_read);
            }

            // 4. 如果还有剩余空间，继续从 data 读取
            let data_read = self.data.read(&mut buf[name_bytes_to_read..])?;
            Ok(name_bytes_to_read + data_read)
        } else {
            // 5. name_bytes 已读完，直接从 data 读取
            self.data.read(buf)
        }
    }

    fn read_reset(&mut self) {
        self.read_name_offset = 0;
        self.data.read_reset();
    }

    fn release_buffer(&mut self) {
        self.data.release_buffer();
    }
}

fn new_column_data(buffer_pool: BufferPool,ck_type: ClickHouseType) -> Result<Box<dyn ColumnData>> {
    let data: Box<dyn ColumnData> = match ck_type {
        ClickHouseType::Int32 => Box::new(Int32ColumnData::new(buffer_pool)),
        ClickHouseType::Int64 => Box::new(Int64ColumnData::new(buffer_pool)),
        ClickHouseType::UInt32 => Box::new(UInt32ColumnData::new(buffer_pool)),
        ClickHouseType::UInt64 => Box::new(UInt64ColumnData::new(buffer_pool)),
        ClickHouseType::Float32 => Box::new(Float32ColumnData::new(buffer_pool)),
        ClickHouseType::Float64 => Box::new(Float64ColumnData::new(buffer_pool)),
        ClickHouseType::String => Box::new(StringColumnData::new(buffer_pool)),
        ClickHouseType::DateTime => Box::new(DateTimeColumnData::new(buffer_pool)),
        ClickHouseType::Nullable(tp) => {
            let inner = new_column_data(buffer_pool, *tp)?;
            Box::new(NullableColumnData::new(inner))
        },
        _ => return Err(format!("not support type:{}", ck_type)),
    };
    Ok(data)
}

pub trait ColumnData: Send + 'static {
    fn sql_type(&self) -> ClickHouseType;
    fn write(&mut self, value: &Value) -> usize;
    fn write_default_value(&mut self) -> usize;
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;
    fn read_reset(&mut self);

    fn release_buffer(&mut self);
}

pub struct NullableColumnData {
    read_null_offset: usize,
    nulls: Vec<u8>,
    inner: Box<dyn ColumnData>,
}

impl NullableColumnData {
    fn new(inner: Box<dyn ColumnData>) -> Self {
        NullableColumnData {
            read_null_offset: 0,
            nulls: Vec::new(),
            inner,
        }
    }
}

impl ColumnData for NullableColumnData {
    fn sql_type(&self) -> ClickHouseType {
        ClickHouseType::Nullable(Box::new(self.inner.sql_type()))
    }

    fn write(&mut self, value: &Value) -> usize {
        if value.is_null() {
            self.nulls.push(1);
            self.inner.write_default_value() + 1
        } else {
            self.nulls.push(0);
            self.inner.write(value) + 1
        }
    }

    fn write_default_value(&mut self) -> usize {
        self.nulls.push(1);
        self.inner.write_default_value() + 1
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // 1. 检查是否还有数据可读, 首先检查 nulls 是否已读完
        if self.read_null_offset >= self.nulls.len() {
            // nulls 已读完，直接从 inner 读取
            return self.inner.read(buf);
        }

        // 2. 计算还需要读取多少 null 标志
        let remaining_nulls = self.nulls.len() - self.read_null_offset;
        let nulls_to_read = min(remaining_nulls, buf.len());

        // 3. 如果 buffer 空间足够，先读取 nulls
        if nulls_to_read > 0 {
            buf[..nulls_to_read].copy_from_slice(&self.nulls[self.read_null_offset..self.read_null_offset + nulls_to_read]);
            self.read_null_offset += nulls_to_read;

            // 如果 buffer 已满，直接返回
            if nulls_to_read == buf.len() {
                return Ok(nulls_to_read);
            }

            // 4. 如果还有剩余空间，继续从 inner 读取
            let inner_read = self.inner.read(&mut buf[nulls_to_read..])?;
            Ok(nulls_to_read + inner_read)
        } else {
            // 5. nulls 已读完，直接从 inner 读取
            self.inner.read(buf)
        }
    }

    fn read_reset(&mut self) {
        self.read_null_offset = 0;
        self.inner.read_reset();
    }

    fn release_buffer(&mut self) {
       self.inner.release_buffer();
    }
}

pub struct Int32ColumnData {
    data: BufferBlock,
}

impl Int32ColumnData {
    fn new(buffer_pool: BufferPool) -> Self {
        Self {
            data: BufferBlock::new(buffer_pool, BLOCK_BUFFER_SIZE),
        }
    }
}

impl ColumnData for Int32ColumnData {
    fn sql_type(&self) -> ClickHouseType {
        ClickHouseType::Int32
    }

    fn write(&mut self, value: &Value)  -> usize  {
        match value {
            Value::Int32(v) => self.data.put_i32_le(*v),
            _ => panic!("invalid value for Int32Column: {:?}", value),
        }
        4
    }

    fn write_default_value(&mut self)  -> usize  {
        self.data.put_i32_le(0);
        4
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }

    fn read_reset(&mut self) {
        self.data.read_reset();
    }

    fn release_buffer(&mut self) {
        self.data.release_buffer();
    }
}

pub struct Int64ColumnData {
    data: BufferBlock,
}

impl Int64ColumnData {
    fn new(buffer_pool: BufferPool) -> Self {
        Self {
            data: BufferBlock::new(buffer_pool, BLOCK_BUFFER_SIZE),
        }
    }
}

impl ColumnData for Int64ColumnData {
    fn sql_type(&self) -> ClickHouseType {
        ClickHouseType::Int64
    }

    fn write(&mut self, value: &Value) -> usize {
        match value {
            Value::Int64(v) => self.data.put_i64_le(*v),
            _ => panic!("invalid value for Int64Column: {:?}", value),
        }
        8
    }

    fn write_default_value(&mut self)  -> usize {
        self.data.put_i64_le(0);
        8
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }

    fn read_reset(&mut self) {
        self.data.read_reset();
    }

    fn release_buffer(&mut self) {
        self.data.release_buffer();
    }
}

pub struct UInt32ColumnData {
    data: BufferBlock,
}

impl UInt32ColumnData {
    fn new(buffer_pool: BufferPool) -> Self {
        Self {
            data: BufferBlock::new(buffer_pool, BLOCK_BUFFER_SIZE),
        }
    }
}

impl ColumnData for UInt32ColumnData {
    fn sql_type(&self) -> ClickHouseType {
        ClickHouseType::UInt32
    }

    fn write(&mut self, value: &Value)  -> usize {
        match value {
            Value::UInt32(v) => self.data.put_u32_le(*v),
            _ => panic!("invalid value for UInt32Column: {:?}", value),
        }
        4
    }

    fn write_default_value(&mut self)  -> usize {
        self.data.put_u32_le(0);
        4
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }

    fn read_reset(&mut self) {
        self.data.read_reset();
    }

    fn release_buffer(&mut self) {
        self.data.release_buffer();
    }
}

pub struct UInt64ColumnData {
    data: BufferBlock,
}

impl UInt64ColumnData {
    fn new(buffer_pool: BufferPool) -> Self {
        Self {
            data: BufferBlock::new(buffer_pool, BLOCK_BUFFER_SIZE),
        }
    }
}

impl ColumnData for UInt64ColumnData {
    fn sql_type(&self) -> ClickHouseType {
        ClickHouseType::UInt64
    }

    fn write(&mut self, value: &Value) -> usize {
        match value {
            Value::UInt64(v) => self.data.put_u64_le(*v),
            _ => panic!("invalid value for UInt64Column: {:?}", value),
        }
        8
    }

    fn write_default_value(&mut self)  -> usize {
        self.data.put_u64_le(0);
        8
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }

    fn read_reset(&mut self) {
        self.data.read_reset();
    }

    fn release_buffer(&mut self) {
        self.data.release_buffer();
    }
}

pub struct Float32ColumnData {
    data: BufferBlock,
}

impl Float32ColumnData {
    fn new(buffer_pool: BufferPool) -> Self {
        Self {
            data: BufferBlock::new(buffer_pool, BLOCK_BUFFER_SIZE),
        }
    }
}

impl ColumnData for Float32ColumnData {
    fn sql_type(&self) -> ClickHouseType {
        ClickHouseType::Float32
    }

    fn write(&mut self, value: &Value) -> usize {
        match value {
            Value::Float32(v) => self.data.put_f32_le(*v),
            _ => panic!("invalid value for Float32Column: {:?}", value),
        }
        4
    }

    fn write_default_value(&mut self) -> usize {
        self.data.put_f32_le(0.0);
        4
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }

    fn read_reset(&mut self) {
        self.data.read_reset();
    }

    fn release_buffer(&mut self) {
        self.data.release_buffer();
    }
}

pub struct Float64ColumnData {
    data: BufferBlock,
}

impl Float64ColumnData {
    fn new(buffer_pool: BufferPool) -> Self {
        Self {
            data: BufferBlock::new(buffer_pool, BLOCK_BUFFER_SIZE),
        }
    }
}

impl ColumnData for Float64ColumnData {
    fn sql_type(&self) -> ClickHouseType {
        ClickHouseType::Float64
    }

    fn write(&mut self, value: &Value) -> usize {
        match value {
            Value::Float64(v) => self.data.put_f64_le(*v),
            _ => panic!("invalid value for Float64Column: {:?}", value),
        }
        8
    }

    fn write_default_value(&mut self) -> usize {
        self.data.put_f64_le(0.0);
        8
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }

    fn read_reset(&mut self) {
        self.data.read_reset();
    }

    fn release_buffer(&mut self) {
        self.data.release_buffer();
    }
}

pub struct StringColumnData {
    data: BufferBlock,
}

impl StringColumnData {
    fn new(buffer_pool: BufferPool) -> Self {
        Self {
            data: BufferBlock::new(buffer_pool, BLOCK_BUFFER_SIZE),
        }
    }
}

impl ColumnData for StringColumnData {
    fn sql_type(&self) -> ClickHouseType {
        ClickHouseType::String
    }

    fn write(&mut self, value: &Value) -> usize {
        match value {
            Value::String(s) => {
                let v = s.as_bytes();
                let mut written = v.len();
                written += put_unsigned_leb128_block(&mut self.data, v.len() as u64);
                self.data.extend_from_slice(v);
                written
            },
            _ =>  panic!("invalid value for StringColumn: {:?}", value),
        }
    }

    fn write_default_value(&mut self) -> usize {
        put_unsigned_leb128_block(&mut self.data, 0)
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }

    fn read_reset(&mut self) {
        self.data.read_reset();
    }

    fn release_buffer(&mut self) {
        self.data.release_buffer();
    }
}

pub struct DateTimeColumnData {
    data: BufferBlock,
}

impl DateTimeColumnData {
    fn new(buffer_pool: BufferPool) -> Self {
        Self {
            data: BufferBlock::new(buffer_pool, BLOCK_BUFFER_SIZE),
        }
    }
}

impl ColumnData for DateTimeColumnData {
    fn sql_type(&self) -> ClickHouseType {
        ClickHouseType::DateTime
    }

    fn write(&mut self, value: &Value) -> usize{
        match value {
            Value::DateTime(v) => self.data.put_u32_le(*v),
            _ => panic!("invalid value for UInt32Column: {:?}", value),
        }
        4
    }

    fn write_default_value(&mut self) -> usize {
        self.data.put_u32_le(0);
        4
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.data.read(buf)
    }

    fn read_reset(&mut self) {
        self.data.read_reset();
    }

    fn release_buffer(&mut self) {
        self.data.release_buffer();
    }
}

/*pub fn cant_type_convert(data_type: DataType, ck_type: ClickHouseType) -> bool {
    match (data_type, ck_type) {
        (tp, ClickHouseType::Nullable(nest)) => cant_type_convert(tp, *nest),
        (DataType::Array(ele_tp), ClickHouseType::Array(nest)) => cant_type_convert(*ele_tp, *nest),
        (DataType::Int | DataType::Long,
            ClickHouseType::Int8 | ClickHouseType::Int16 | ClickHouseType::Int32 | ClickHouseType::Int64 |
            ClickHouseType::UInt8 | ClickHouseType::UInt16 | ClickHouseType::UInt32 | ClickHouseType::UInt64) => true,
        (DataType::Int | DataType::Long | DataType::Float | DataType::Double,
            ClickHouseType::Float32 | ClickHouseType::Float64) => true,
        (DataType::Int | DataType::Long | DataType::Float | DataType::Double | DataType::String,
            ClickHouseType::String) => true,
        (DataType::Timestamp, ClickHouseType::DateTime) => true,
        (DataType::Timestamp, ClickHouseType::DateTime64(_)) => true,
        _ => false,
    }
}*/

fn put_unsigned_leb128_block(buffer: &mut BufferBlock, mut value: u64) -> usize {
    let mut written = 0;
    while {
        let mut byte = value as u8 & 0x7f;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buffer.put_u8(byte);
        written += 1;

        value != 0
    } {}
    written
}

fn put_unsigned_leb128(mut buffer: impl BufMut, mut value: u64) {
    while {
        let mut byte = value as u8 & 0x7f;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buffer.put_u8(byte);

        value != 0
    } {}
}

#[cfg(test)]
mod tests {
    use flexi_logger::with_thread;
    use crate::connector::clickhouse::{make_value_converter, ClickHouseValue};
    use crate::data::{GenericRow, Row};
    use crate::data::Value as EtlValue;
    use crate::types::DataType;
    use super::*;

    #[test]
    fn test_column() -> Result<()> {
        let pool = BufferPool::new(1024 * 1024, 10 * 1024 * 1024, 300_000);
        let types = vec![
            ("id", DataType::Long, ClickHouseType::Int64),
            ("datetime", DataType::Timestamp, ClickHouseType::DateTime),
            ("int32", DataType::Int, ClickHouseType::Int32),
            ("int32_nullalbe", DataType::Int, ClickHouseType::Nullable(Box::new(ClickHouseType::Int32))),
            ("str", DataType::String, ClickHouseType::String),
        ];
        let mut converters = Vec::new();
        let mut columns = Vec::new();
        for (name, data_type, ck_type) in &types {
            converters.push(make_value_converter(data_type.clone(), ck_type.clone())?);
            columns.push(Column::new(pool.clone(), name.to_string(), ck_type.clone())?);
        }
        let mut datas = Vec::with_capacity(converters.len());
        datas.resize(converters.len(), ClickHouseValue::Null);

        let len = converters.len();
        for i in 0..5 {
            let row = GenericRow::new(vec![
                EtlValue::Long(i),  EtlValue::Long((10 + i) * 1_000_000), EtlValue::Int(i as i32),
                if i % 2 == 0 {EtlValue::Null} else {EtlValue::Int(i as i32)},
                 EtlValue::String(Arc::new(format!("str{}", i)))
            ]);

            for j in 0..len {
                let v = (&converters[j]).convert(row.get(j))?;
                datas[j] = v;
            }
            for j in 0..len {
                (&mut columns[j]).write(&datas[j]);
            }
        }

        let mut buffer = Vec::new();
        put_unsigned_leb128(&mut buffer, 5); // columns length
        put_unsigned_leb128(&mut buffer, 5); // rowCnt
        let mut chunk = [0u8; 4096]; // 4KB 缓冲区
        for j in 0..len {
            loop {
                match (&mut columns[j]).read(&mut chunk) {
                    Ok(0) => break, // 读取到末尾
                    Ok(n) => buffer.extend_from_slice(&chunk[..n]),
                    Err(e) => return Err(e.to_string()),
                }
            }
        }
        println!("{:x?}", buffer);
        Ok(())
    }

    #[test]
    fn test_block() -> Result<()> {
        let pool = BufferPool::new(1024 * 1024, 10 * 1024 * 1024, 300_000);
        let types = vec![
            ColumnDesc::new("id", DataType::Long, ClickHouseType::Int64),
            ColumnDesc::new("datetime", DataType::Timestamp, ClickHouseType::DateTime),
            ColumnDesc::new("int32", DataType::Int, ClickHouseType::Int32),
            ColumnDesc::new("int32_nullalbe", DataType::Int, ClickHouseType::Nullable(Box::new(ClickHouseType::Int32))),
            ColumnDesc::new("str", DataType::String, ClickHouseType::String),
        ];
        let mut block = Block::new(pool, types)?;
        for i in 0..5 {
            let row = GenericRow::new(vec![
                EtlValue::Long(i), EtlValue::Long((10 + i) * 1_000_000), EtlValue::Int(i as i32),
                if i % 2 == 0 { EtlValue::Null } else { EtlValue::Int(i as i32) },
                EtlValue::String(Arc::new(format!("str{}", i)))
            ]);
            block.write(&row)?;
        }

        let mut buffer = Vec::new();
        let mut chunk = [0u8; 4096]; // 4KB 缓冲区
        loop {
            match block.read(&mut chunk) {
                Ok(0) => break, // 读取到末尾
                Ok(n) => buffer.extend_from_slice(&chunk[..n]),
                Err(e) => return Err(e.to_string()),
            }
        }
        println!("{:x?}", buffer);


        Ok(())
    }

    #[test]
    fn test_block_compress() -> Result<()> {
        flexi_logger::Logger::try_with_str("info")
            .unwrap()
            .start()
            .unwrap();
        let pool = BufferPool::new(1024 * 1024, 10 * 1024 * 1024, 300_000);
        let types = vec![
            ColumnDesc::new("id", DataType::Long, ClickHouseType::Int64),
            ColumnDesc::new("datetime", DataType::Timestamp, ClickHouseType::DateTime),
            ColumnDesc::new("int32", DataType::Int, ClickHouseType::Int32),
            ColumnDesc::new("int32_nullalbe", DataType::Int, ClickHouseType::Nullable(Box::new(ClickHouseType::Int32))),
            ColumnDesc::new("str", DataType::String, ClickHouseType::String),
        ];
        let mut block = Block::new(pool, types)?;
        for i in 0..50000 {
            let row = GenericRow::new(vec![
                EtlValue::Long(i), EtlValue::Long((10 + i) * 1_000_000), EtlValue::Int(i as i32),
                if i % 2 == 0 { EtlValue::Null } else { EtlValue::Int(i as i32) },
                EtlValue::String(Arc::new(format!("str{}", i)))
            ]);
            block.write(&row)?;
        }

        let mut buffer1 = Vec::new();
        block.read_to_end(&mut buffer1).unwrap();
        block.read_reset();
        let buffer2 = lz4::compress(&buffer1).unwrap().to_vec();
        let mut block_reader = ArcCompressBlockReader::from(Arc::new(Mutex::new(block)));
        let mut buffer3 = Vec::new();
        block_reader.read_to_end(&mut buffer3).unwrap();
        //println!("{}: {:x?}", buffer1.len(), buffer1);
        //println!("{}: {:x?}", buffer2.len(), buffer2);
        //println!("{}: {:x?}", buffer3.len(), buffer3);
        println!("{}, {}, {}", buffer1.len(), buffer2.len(), buffer3.len());
        //assert_eq!(buffer2 , buffer3);
        Ok(())
    }
}