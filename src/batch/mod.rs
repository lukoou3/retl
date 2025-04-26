mod data_frame;
mod session;

use std::fs;
use std::path::PathBuf;
use dirs::home_dir;
use rustyline::config::Configurer;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use rustyline::history::History;
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

const MAX_HISTORY_LINES: usize = 5000;

fn run_sql_repl() -> Result<()> {
    let mut rl = DefaultEditor::new().map_err(|e| format!("Failed to create rustyline editor: {}", e))?;
    let _ = rl.set_max_history_size(MAX_HISTORY_LINES);
    let mut history_file = home_dir().ok_or("Failed to get home directory")?;
    history_file.push(".rsql_history");
    if history_file.exists() {
        let _ = rl.load_history(&history_file);
    }

    let mut session = BatchSession::new();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                let sql = line.trim();
                if sql.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(sql);
                if sql.eq_ignore_ascii_case("exit") || sql.eq_ignore_ascii_case("quit") {
                    break;
                } else if sql.eq_ignore_ascii_case("history") {
                    for (i, entry) in rl.history().iter().enumerate() {
                        println!("{}: {}", i + 1, entry);
                    }
                    continue;
                }

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

    // 保存历史记录
    let _ = rl.save_history(&history_file);

    Ok(())
}