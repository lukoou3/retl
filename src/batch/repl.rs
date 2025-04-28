use std::fs;
use std::path::PathBuf;
use dirs::home_dir;
use rustyline::config::Configurer;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use rustyline::history::History;

use crate::Result;
use crate::batch::BatchSession;

const MAX_HISTORY_LINES: usize = 1000;

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

pub fn run_sql_repl() -> Result<()> {
    let mut rl = DefaultEditor::new().map_err(|e| format!("Failed to create rustyline editor: {}", e))?;
    let _ = rl.set_max_history_size(MAX_HISTORY_LINES);
    let mut history_file = home_dir().ok_or("Failed to get home directory")?;
    history_file.push(".rsql_history");
    if history_file.exists() {
        let _ = rl.load_history(&history_file);
    }

    let mut running = true;
    let mut buffer = String::new();
    let mut session = BatchSession::new();
    while running {
        let readline = rl.readline("sql> ");
        match readline {
            Ok(line) => {
                buffer.push_str(&line);
                buffer.push('\n');
                if !line.contains(';') {
                    continue;
                }
                
                let (complete_statements, remaining) = split_semi_colon(&buffer);
                buffer = remaining; // 更新缓冲区为剩余部分
                // 执行完整语句
                for stmt in complete_statements {
                    let sql = stmt.trim();
                    if sql.is_empty() {
                        continue;
                    }
                    // 执行 SQL 语句
                    let _ = rl.add_history_entry(format!("{};", stmt.trim_start()));
                    if sql.eq_ignore_ascii_case("exit") || sql.eq_ignore_ascii_case("quit") {
                        println!("Bye");
                        running = false;
                        break;
                    } else if sql.eq_ignore_ascii_case("history") {
                        for (i, entry) in rl.history().iter().enumerate() {
                            println!("{}: {}", i + 1, entry);
                        }
                        continue;
                    }
                    println!("{}", sql);
                    match session.sql(&sql) {
                        Ok(mut df) => {
                            df.show();
                            println!("");
                        },
                        Err(e) => {
                            println!("error: {}", e);
                        }
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

/// Splits a SQL string into complete statements (without trailing semicolon) and a remaining part.
/// Ignores semicolons inside quotes, comments, or escaped sequences.
/// Adapted from Spark's `splitSemiColon` (originally from Hive 2.3's CliDriver).
pub fn split_semi_colon(line: &str) -> (Vec<String>, String) {
    let mut inside_single_quote = false;
    let mut inside_double_quote = false;
    let mut inside_simple_comment = false;
    let mut bracketed_comment_level = 0;
    let mut escape = false;
    let mut begin_char_index = 0; // 字符索引
    let mut begin_byte_index = 0; // 对应的字节索引
    let mut leaving_bracketed_comment = false;
    let mut is_statement = false;
    let mut complete_statements: Vec<String> = Vec::new();

    // Helper functions
    fn inside_bracketed_comment(level: i32) -> bool {
        level > 0
    }

    fn inside_comment(simple: bool, bracketed_level: i32) -> bool {
        simple || inside_bracketed_comment(bracketed_level)
    }

    fn statement_in_progress(
        char_index: usize,
        begin_char_index: usize,
        is_statement: bool,
        current_char: char,
        simple_comment: bool,
        bracketed_level: i32,
    ) -> bool {
        is_statement || (!inside_comment(simple_comment, bracketed_level)
            && char_index > begin_char_index
            && !current_char.is_whitespace())
    }

    // 使用 char_indices 遍历字符，获取 (字节索引, 字符)
    let mut char_indices = line.char_indices().peekable();
    let mut current_char_index = 0; // 当前字符索引

    while let Some((byte_index, current_char)) = char_indices.next() {
        // Check if we need to decrement bracketed comment level
        if leaving_bracketed_comment {
            bracketed_comment_level -= 1;
            leaving_bracketed_comment = false;
        }

        if current_char == '\'' && !inside_comment(inside_simple_comment, bracketed_comment_level) {
            // Handle single quote, ignore if escaped or inside double quotes
            if !escape && !inside_double_quote {
                inside_single_quote = !inside_single_quote;
            }
        } else if current_char == '\"' && !inside_comment(inside_simple_comment, bracketed_comment_level) {
            // Handle double quote, ignore if escaped or inside single quotes
            if !escape && !inside_single_quote {
                inside_double_quote = !inside_double_quote;
            }
        } else if current_char == '-' {
            if let Some((_, next_char)) = char_indices.peek() {
                if inside_double_quote || inside_single_quote || inside_comment(inside_simple_comment, bracketed_comment_level) {
                    // Ignore '-' in quotes or comments
                } else if *next_char == '-' {
                    // Start a single-line comment
                    inside_simple_comment = true;
                    char_indices.next(); // 消耗 '-'
                    current_char_index += 1;
                }
            }
        } else if current_char == ';' {
            if inside_single_quote || inside_double_quote || inside_comment(inside_simple_comment, bracketed_comment_level) {
                // Ignore semicolon in quotes or comments
            } else {
                if is_statement {
                    // Split statement, exclude semicolon
                    let end_byte_index = byte_index; // 分号前的字节索引
                    complete_statements.push(line[begin_byte_index..end_byte_index].to_string());
                }
                // 更新开始索引（跳过分号）
                begin_char_index = current_char_index + 1;
                begin_byte_index = byte_index + current_char.len_utf8();
                is_statement = false;
            }
        } else if current_char == '\n' {
            // End single-line comment on newline
            if !escape {
                inside_simple_comment = false;
            }
        } else if current_char == '/' && !inside_simple_comment {
            if let Some((_, next_char)) = char_indices.peek() {
                if inside_single_quote || inside_double_quote {
                    // Ignore '/' in quotes
                } else if inside_bracketed_comment(bracketed_comment_level) && line[..byte_index].ends_with('*') {
                    // End of bracketed comment (*/); defer level decrement
                    leaving_bracketed_comment = true;
                } else if *next_char == '*' {
                    // Start of bracketed comment (/*)
                    bracketed_comment_level += 1;
                    char_indices.next(); // 消耗 '*'
                    current_char_index += 1;
                }
            }
        }

        // Handle escape character
        if escape {
            escape = false;
        } else if current_char == '\\' {
            escape = true;
        }

        // Update statement progress
        is_statement = statement_in_progress(
            current_char_index,
            begin_char_index,
            is_statement,
            current_char,
            inside_simple_comment,
            bracketed_comment_level,
        );

        current_char_index += 1;
    }

    // Handle remaining part
    let remaining = if is_statement && begin_char_index < current_char_index {
        line[begin_byte_index..].to_string()
    } else {
        String::new()
    };

    (complete_statements, remaining)
}
