mod database;
mod parser;
mod record;

use anyhow::{bail, Context, Result};
use database::Database;
use parser::{parse_query, QueryType};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        bail!("Usage: {} <database path> <command>", args[0]);
    }

    let db_path = &args[1];
    let command = &args[2];

    if command.starts_with(".") {
        match command.as_str() {
            ".dbinfo" => handle_dbinfo(db_path),
            ".tables" => handle_tables(db_path),
            _ => bail!("Unsupported command: {}", command),
        }
    } else {
        match parse_query(command)? {
            QueryType::Select { column, table } => handle_select(db_path, &column, &table),
            QueryType::SelectCount { table } => handle_count(db_path, &table),
            _ => bail!("Unsupported SQL command: {}", command),
        }
    }
}

fn handle_select(db_path: &str, column_name: &str, table_name: &str) -> Result<()> {
    let mut db = Database::open(db_path)?;

    let schema = db.read_schema()?;

    let table_entry = schema
        .iter()
        .find(|e| e.typ == "table" && e.tbl_name == table_name)
        .context(format!("Table '{}' not found", table_name))?;

    let column_index = get_column_index(&mut db, table_entry, column_name)?;

    let records = db.read_table_records(table_entry.rootpage as usize)?;

    for record in records {
        if column_index < record.len() {
            if let record::Value::Text(value) = &record[column_index] {
                println!("{}", value);
            } else if let record::Value::Int(value) = record[column_index] {
                println!("{}", value);
            }
        }
    }

    Ok(())
}

fn get_column_index(
    db: &mut Database,
    table_entry: &database::SchemaEntry,
    target_column: &str,
) -> Result<usize> {
    let schema = db.read_schema()?;

    let sql_entry = schema
        .iter()
        .find(|e| e.typ == "table" && e.tbl_name == table_entry.tbl_name)
        .context(format!(
            "Table '{}' SQL definition not found",
            table_entry.tbl_name
        ))?;

    let sql = match &sql_entry.sql {
        Some(sql) => sql,
        None => bail!(
            "No SQL definition found for table '{}'",
            table_entry.tbl_name
        ),
    };

    let start_idx = sql.find('(').context("Invalid CREATE TABLE syntax")?;
    let end_idx = sql.rfind(')').context("Invalid CREATE TABLE syntax")?;

    let columns_str = &sql[start_idx + 1..end_idx];
    let column_defs: Vec<&str> = columns_str.split(',').collect();

    for (i, col_def) in column_defs.iter().enumerate() {
        let col_name = col_def.trim().split_whitespace().next().unwrap_or("");
        if col_name == target_column {
            return Ok(i);
        }
    }

    bail!(
        "Column '{}' not found in table '{}'",
        target_column,
        table_entry.tbl_name
    )
}

fn handle_dbinfo(db_path: &str) -> Result<()> {
    let mut db = Database::open(db_path)?;
    println!("database page size: {}", db.page_size());

    let mut num_tables = 0;
    let schema = db.read_schema()?;

    for entry in schema {
        if entry.typ == "table" {
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
        if entry.typ == "table" {
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

    let page_data = db.read_page(entry.rootpage as usize)?;

    let header_offset = if entry.rootpage == 1 { 100 } else { 0 };

    let cell_count =
        u16::from_be_bytes([page_data[header_offset + 3], page_data[header_offset + 4]]) as usize;

    println!("{}", cell_count);
    Ok(())
}
