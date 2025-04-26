mod data_frame;
mod session;

use std::fs;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
pub use data_frame::*;
pub use session::*;

use crate::Result;
pub fn run_sql_command(sql: Option<String>, filename: Option<String>) -> Result<()> {
    match (sql, filename) {
        (Some(_), Some(_)) => {
            Err("sql and filename can not be set at the same time".into())
        },
        (Some(sql), _) => {
            let mut session = BatchSession::new();
            let mut df = session.sql(&sql)?;
            df.show();
            Ok(())
        },
        (_, Some(f)) => {
            let mut session = BatchSession::new();
            let sql = fs::read_to_string(&f).map_err(|e| format!("Failed to read : {}", f))?;
            let mut df = session.sql(&sql)?;
            df.show();
            Ok(())
        },
        _ => run_sql_repl()
    }
}

fn run_sql_repl() -> Result<()> {
    let mut rl = DefaultEditor::new().map_err(|e| format!("Failed to create rustyline editor: {}", e))?;
    let mut session = BatchSession::new();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                let sql = line.trim();
                if sql.is_empty() {
                    continue;
                }
                if sql.eq_ignore_ascii_case("exit") || sql.eq_ignore_ascii_case("quit") {
                    break;
                }
                rl.add_history_entry(sql).unwrap();
                println!("{}", sql);
                match session.sql(&sql) {
                    Ok(mut df) => df.show(),
                    Err(e) => {
                        println!("error: {}", e);
                    }
                }
            },
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
    Ok(())
}