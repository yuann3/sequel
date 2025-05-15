use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        bail!("Usage: {} <database path> <command>", args[0]);
    }

    let db_path = &args[1];
    let command = &args[2];

    match command.as_str() {
        ".dbinfo" => handle_dbinfo(db_path),
        _ => bail!("Unsupported command: {}", command),
    }
}

fn handle_dbinfo(db_path: &str) -> Result<()> {
    let mut file = File::open(db_path).context("Failed to open database file")?;

    let mut file_header = [0; 100];
    file.read_exact(&mut file_header)
        .context("Failed to read database header")?;

    let page_size = u16::from_be_bytes([file_header[16], file_header[17]]);
    println!("database page size: {}", page_size);

    let mut page_header = [0; 8];
    file.read_exact(&mut page_header)
        .context("Failed to read page header")?;

    let cell_count = u16::from_be_bytes([page_header[3], page_header[4]]);
    println!("number of tables: {}", cell_count);

    Ok(())
}
