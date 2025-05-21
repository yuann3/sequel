mod database;
mod parser;
mod record;

use anyhow::{bail, Context, Result};
use database::Database;
use parser::{parse_query, QueryType, WhereCondition};
use record::Value;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        bail!("Usage: {} <database path> <command>", args[0]);
    }

    let db_path = &args[1];
    let command = &args[2];

    if command.starts_with('.') {
        match command.as_str() {
            ".dbinfo" => handle_dbinfo(db_path),
            ".tables" => handle_tables(db_path),
            _ => bail!("Unsupported command: {}", command),
        }
    } else {
        match parse_query(command)? {
            QueryType::Select {
                columns,
                table,
                where_clause,
            } => handle_select(db_path, &columns, &table, where_clause),
            QueryType::SelectCount { table } => handle_count(db_path, &table),
            QueryType::Unknown => bail!("Unknown or unsupported SQL command: {}", command),
        }
    }
}

fn get_table_column_names(sql_create_table: &str) -> Result<Vec<String>> {
    let start_idx = sql_create_table
        .find('(')
        .context("Invalid CREATE TABLE syntax: missing '('")?;
    let end_idx = sql_create_table
        .rfind(')')
        .context("Invalid CREATE TABLE syntax: missing ')'")?;

    if start_idx >= end_idx {
        bail!("Invalid CREATE TABLE syntax: '(' not before ')'");
    }

    let columns_str = &sql_create_table[start_idx + 1..end_idx];
    Ok(columns_str
        .split(',')
        .map(|col_def| {
            col_def
                .trim()
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .collect())
}

fn handle_select(
    db_path: &str,
    requested_column_names: &[String],
    table_name: &str,
    where_clause: Option<WhereCondition>,
) -> Result<()> {
    let mut db = Database::open(db_path)?;
    let schema_entries = db.read_schema()?;

    let table_entry = schema_entries
        .iter()
        .find(|e| e.typ == "table" && e.tbl_name == table_name)
        .context(format!("Table '{}' not found", table_name))?;

    let table_sql = table_entry.sql.as_ref().context(format!(
        "No SQL definition found for table '{}'",
        table_name
    ))?;

    let all_table_column_names = get_table_column_names(table_sql)?;

    let output_column_indices = requested_column_names
        .iter()
        .map(|req_col_name| {
            all_table_column_names
                .iter()
                .position(|name| name.eq_ignore_ascii_case(req_col_name))
                .context(format!(
                    "Column '{}' not found in table '{}'",
                    req_col_name, table_name
                ))
        })
        .collect::<Result<Vec<usize>>>()?;

    let all_records = db.read_table_records(table_entry.rootpage as usize)?;

    let filtered_records = if let Some(condition) = where_clause {
        let condition_column_index = all_table_column_names
            .iter()
            .position(|name| name.eq_ignore_ascii_case(&condition.column))
            .context(format!(
                "WHERE clause column '{}' not found in table '{}'",
                condition.column, table_name
            ))?;

        all_records
            .into_iter()
            .filter(|record| {
                if condition_column_index < record.len() {
                    match &record[condition_column_index] {
                        Value::Text(val) => {
                            if condition.operator == "=" {
                                val == &condition.value
                            } else {
                                false
                            }
                        }
                        Value::Int(val_int) => {
                            if condition.operator == "=" {
                                if let Ok(cond_val_int) = condition.value.parse::<i64>() {
                                    *val_int == cond_val_int
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        }
                        _ => false,
                    }
                } else {
                    false
                }
            })
            .collect()
    } else {
        all_records
    };

    for record in filtered_records {
        let mut values_to_print = Vec::new();
        for &index in &output_column_indices {
            if index < record.len() {
                match &record[index] {
                    Value::Text(value) => values_to_print.push(value.clone()),
                    Value::Int(value) => values_to_print.push(value.to_string()),
                    Value::Float(value) => values_to_print.push(value.to_string()),
                    Value::Blob(_) => values_to_print.push("[BLOB]".to_string()),
                    Value::Null => values_to_print.push("NULL".to_string()),
                }
            } else {
                values_to_print.push("".to_string());
            }
        }
        println!("{}", values_to_print.join("|"));
    }

    Ok(())
}

fn handle_dbinfo(db_path: &str) -> Result<()> {
    let mut db = Database::open(db_path)?;
    println!("database page size: {}", db.page_size());

    let mut num_tables = 0;
    let schema = db.read_schema()?;

    for entry in schema {
        if entry.typ == "table" && !entry.tbl_name.starts_with("sqlite_") {
            num_tables += 1;
        }
    }

    println!("number of tables: {}", num_tables);
    Ok(())
}

fn handle_tables(db_path: &str) -> Result<()> {
    let mut db = Database::open(db_path)?;
    let schema = db.read_schema()?;

    let mut table_names = Vec::new();
    for entry in schema {
        if entry.typ == "table" && !entry.tbl_name.starts_with("sqlite_") {
            table_names.push(entry.tbl_name);
        }
    }

    println!("{}", table_names.join(" "));
    Ok(())
}

fn handle_count(db_path: &str, table_name: &str) -> Result<()> {
    let mut db = Database::open(db_path)?;
    let schema = db.read_schema()?;

    let entry = schema
        .iter()
        .find(|e| e.typ == "table" && e.tbl_name == table_name)
        .context(format!("Table '{}' not found", table_name))?;

    let records = db.read_table_records(entry.rootpage as usize)?;
    println!("{}", records.len());

    // let page_data = db.read_page(entry.rootpage as usize)?;
    // let header_offset = if entry.rootpage == 1 { 100 } else { 0 };
    // let cell_count =
    //     u16::from_be_bytes([page_data[header_offset + 3], page_data[header_offset + 4]]) as usize;
    // println!("{}", cell_count);
    Ok(())
}

// fn get_column_index(
//     db: &mut Database,
//     table_entry: &database::SchemaEntry,
//     target_column: &str,
// ) -> Result<usize> { ... }
