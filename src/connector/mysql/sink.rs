use std::fmt::Debug;
use itertools::Itertools;
use log::{error, info};
use mysql::Pool;
use mysql::prelude::Queryable;
use crate::config::TaskContext;
use crate::connector::batch::BatchConfig;
use crate::connector::mysql::config::{MysqlDefaultBatchSettings, MysqlSinkConfig};
use crate::connector::Sink;
use crate::data::{Row, Value};
use crate::datetime_utils::{format_datetime_fafault, from_timestamp_micros_utc};
use crate::types::{DataType, Schema};

pub struct MysqlSink {
    task_context: TaskContext,
    pool: Pool,
    table: String,
    upsert: bool,
    data_types: Vec<(usize, DataType)>,
    insert_sql_prefix: String,
    insert_sql_suffix: String,
    batch_config: BatchConfig<MysqlDefaultBatchSettings>,
    update_sql: String,
    sql_value: String,
    rows: usize,
}

impl MysqlSink {
    pub fn new(task_context: TaskContext, schema: Schema, sink_config: MysqlSinkConfig) -> anyhow::Result<Self> {
        let pool = Pool::new(sink_config.url.as_str())?;
        let columns = schema.fields.iter().map(|f| &f.name).join(",");
        let insert_sql_prefix = format!("INSERT INTO {} ({}) VALUES ", sink_config.table, columns);
        let insert_sql_suffix = if sink_config.upsert {
            format!(" ON DUPLICATE KEY UPDATE {}", schema.fields.iter().map(|f| format!("{}=VALUES({})", &f.name, &f.name)).join(","))
        } else {
            "".to_string()
        };

        Ok(Self{
            task_context,
            pool,
            table: sink_config.table,
            upsert: sink_config.upsert,
            data_types: schema.fields.iter().enumerate().map(|(i, field)| (i, field.data_type.clone())).collect(),
            insert_sql_prefix,
            insert_sql_suffix,
            batch_config: sink_config.batch_config,
            update_sql: String::new(),
            sql_value: String::new(),
            rows: 0,
        })
    }

    fn row_to_sql(&mut self, row: &dyn Row)  {
        self.sql_value.clear();
        self.sql_value.push_str("(");
        for (i, data_type) in self.data_types.iter() {
            if *i > 0 {
                self.sql_value.push_str(",");
            }
           Self::value_to_sql(&mut self.sql_value, row.get(*i), data_type);
        }
        self.sql_value.push_str(")");
    }

    fn value_to_sql(sql_value: &mut String, v: &Value, data_type: &DataType)  {
        if v.is_null() {
            sql_value.push_str("NULL");
            return;
        }

        match data_type {
            DataType::Int => sql_value.push_str(&v.get_int().to_string()),
            DataType::Long => sql_value.push_str(&v.get_long().to_string()),
            DataType::Float => sql_value.push_str(&v.get_float().to_string()),
            DataType::Double => sql_value.push_str(&v.get_double().to_string()),
            DataType::String => {
                // TODO: escape string.
                sql_value.push_str("'");
                sql_value.push_str(&v.get_string().replace("'", "''"));
                sql_value.push_str("'");
            },
            DataType::Boolean => sql_value.push_str(if v.get_boolean() { "1" } else { "0" }),
            DataType::Timestamp => {
                let tm = format_datetime_fafault(from_timestamp_micros_utc(v.get_long()));
                sql_value.push_str("'");
                sql_value.push_str(tm.as_str());
                sql_value.push_str("'");
            },
            _ => sql_value.push_str("NULL")
        }
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        let mut conn = self.pool.get_conn()?;
        //info!("{}", &self.update_sql);
        conn.query_drop(&self.update_sql)?;
        Ok(())
    }
}

impl Debug for MysqlSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlSink")
            .field("task_context", &self.task_context)
            .field("pool", &self.pool)
            .field("table", &self.table)
            .field("upsert", &self.upsert)
            .field("insert_sql_prefix", &self.insert_sql_prefix)
            .field("insert_sql_suffix", &self.insert_sql_suffix)
            .field("batch_config", &self.batch_config)
            .field("update_sql", &self.update_sql)
            .finish()
    }
}

impl Sink for MysqlSink {
    fn open(&mut self) -> crate::Result<()> {
        self.update_sql.push_str(&self.insert_sql_prefix);
        Ok(())
    }

    fn invoke(&mut self, row: &dyn Row) -> crate::Result<()> {
        if self.rows > 0 {
            self.update_sql.push_str(",");
        }
        self.row_to_sql(row);
        self.update_sql.push_str(&self.sql_value);
        self.rows += 1;

        if self.rows >= self.batch_config.max_rows {
            self.update_sql.push_str(&self.insert_sql_suffix);
            info!("flush rows: {}", self.rows);
            if let Err(e) = self.flush() {
                error!("flush error: {}", e);
            } else {
                info!("flush sucess {}", self.rows);
            }
            self.rows = 0;
            self.update_sql.clear();
            self.update_sql.push_str(&self.insert_sql_prefix);
        }

        Ok(())
    }
}

