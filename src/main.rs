use anyhow::{bail, Context, Result};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        bail!("Usage: {} <database path> <command>", args[0]);
    }

    let db_path = &args[1];
    let command = &args[2];

    if command.to_lowercase().starts_with("select count(*) from") {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.len() < 4 {
            bail!("Invalid SELECT COUNT command format");
        }
        let table_name = parts[parts.len() - 1];
        handle_count(db_path, table_name)
    } else {
        match command.as_str() {
            ".dbinfo" => handle_dbinfo(db_path),
            ".tables" => handle_tables(db_path),
            _ => bail!("Unsupported command: {}", command),
        }
    }
}

fn handle_dbinfo(db_path: &str) -> Result<()> {
    let mut db = Database::open(db_path)?;

    println!("database page size: {}", db.page_size);

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

struct SchemaEntry {
    typ: String,
    tbl_name: String,
    rootpage: u32,
}

struct Database {
    file: File,
    page_size: usize,
}

impl Database {
    fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path).context("Failed to open database file")?;

        let mut header = [0; 100];
        file.read_exact(&mut header)
            .context("Failed to read database header")?;

        let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;

        Ok(Self {
            file,
            page_size: if page_size == 1 { 65536 } else { page_size },
        })
    }

    fn read_schema(&mut self) -> Result<Vec<SchemaEntry>> {
        let mut page_data = vec![0; self.page_size];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut page_data)?;

        let header_offset = 100;
        let cell_count =
            u16::from_be_bytes([page_data[header_offset + 3], page_data[header_offset + 4]]);

        let offsets_array_start = header_offset + 8;
        let mut schema_entries = Vec::new();

        for i in 0..cell_count {
            let offset_pos = offsets_array_start + (i as usize * 2);
            let cell_offset =
                u16::from_be_bytes([page_data[offset_pos], page_data[offset_pos + 1]]) as usize;

            let cell_data = &page_data[cell_offset..];
            let (_, rest, _) = read_varint(cell_data)?;
            let (_, rest, _) = read_varint(rest)?;

            let record = parse_record(rest)?;

            if record.len() >= 5 {
                let typ = if let Value::Text(t) = &record[0] {
                    t.clone()
                } else {
                    continue;
                };
                let tbl_name = if let Value::Text(t) = &record[2] {
                    t.clone()
                } else {
                    continue;
                };
                let rootpage = if let Value::Int(r) = record[3] {
                    r as u32
                } else {
                    continue;
                };

                schema_entries.push(SchemaEntry {
                    typ,
                    tbl_name,
                    rootpage,
                });
            }
        }

        Ok(schema_entries)
    }

    fn read_page(&mut self, page_number: usize) -> Result<Vec<u8>> {
        let mut page_data = vec![0; self.page_size];
        let offset = (page_number - 1) * self.page_size;

        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.read_exact(&mut page_data)?;

        Ok(page_data)
    }
}

fn read_varint(bytes: &[u8]) -> Result<(u64, &[u8], usize)> {
    let mut result = 0;
    let mut bytes_read = 0;

    for i in 0..9 {
        if i >= bytes.len() {
            bail!("Unexpected end of bytes when reading varint");
        }

        bytes_read += 1;
        let byte = bytes[i];

        if i == 8 {
            result = (result << 8) | (byte as u64);
            break;
        }

        result = (result << 7) | ((byte & 0x7F) as u64);

        if byte & 0x80 == 0 {
            break;
        }
    }

    Ok((result, &bytes[bytes_read..], bytes_read))
}

#[allow(dead_code)]
enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

fn parse_record(bytes: &[u8]) -> Result<Vec<Value>> {
    let (header_size, data, _) = read_varint(bytes)?;
    let header_size = header_size as usize;

    let header_end = header_size - 1;
    let header_data = &data[..header_end];

    let mut header_pos = 0;
    let mut column_types = Vec::new();

    while header_pos < header_end {
        let (serial_type, _, bytes_read) = read_varint(&header_data[header_pos..])?;
        header_pos += bytes_read;
        column_types.push(serial_type);
    }

    let mut current_pos = header_end;
    let mut values = Vec::new();

    for &serial_type in &column_types {
        if serial_type == 0 {
            values.push(Value::Null);
            continue;
        }

        let (value, bytes_read) = parse_value(serial_type, &data[current_pos..]);
        values.push(value);
        current_pos += bytes_read;
    }

    Ok(values)
}

fn parse_value(serial_type: u64, bytes: &[u8]) -> (Value, usize) {
    match serial_type {
        0 => (Value::Null, 0),
        1 => {
            let value = bytes[0] as i8 as i64;
            (Value::Int(value), 1)
        }
        2 => {
            let value = i16::from_be_bytes([bytes[0], bytes[1]]) as i64;
            (Value::Int(value), 2)
        }
        3 => {
            let value = ((bytes[0] as i64) << 16) | ((bytes[1] as i64) << 8) | (bytes[2] as i64);
            if value & 0x800000 != 0 {
                (Value::Int(value | 0xFFFFFF000000), 3)
            } else {
                (Value::Int(value), 3)
            }
        }
        4 => {
            let value = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64;
            (Value::Int(value), 4)
        }
        5 => {
            let mut buf = [0; 8];
            buf[3..8].copy_from_slice(&bytes[0..5]);
            let value = i64::from_be_bytes(buf);
            (Value::Int(value >> 24), 5)
        }
        6 => {
            let value = i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            (Value::Int(value), 8)
        }
        7 => {
            let value = f64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            (Value::Float(value), 8)
        }
        8 => (Value::Int(0), 0),
        9 => (Value::Int(1), 0),
        _ => {
            if serial_type % 2 == 0 {
                let len = ((serial_type - 12) / 2) as usize;
                let blob = bytes[..len].to_vec();
                (Value::Blob(blob), len)
            } else {
                let len = ((serial_type - 13) / 2) as usize;
                let text = String::from_utf8_lossy(&bytes[..len]).to_string();
                (Value::Text(text), len)
            }
        }
    }
}
