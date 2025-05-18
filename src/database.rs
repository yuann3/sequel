use crate::record::{parse_record, read_varint, Value};
use anyhow::{Context, Result};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

pub struct SchemaEntry {
    pub typ: String,
    pub tbl_name: String,
    pub rootpage: u32,
    pub sql: Option<String>,
}

pub struct Database {
    file: File,
    page_size: usize,
}

impl Database {
    pub fn open(path: &str) -> Result<Self> {
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

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn read_schema(&mut self) -> Result<Vec<SchemaEntry>> {
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

                let sql = if let Value::Text(s) = &record[4] {
                    Some(s.clone())
                } else {
                    None
                };

                schema_entries.push(SchemaEntry {
                    typ,
                    tbl_name,
                    rootpage,
                    sql,
                });
            }
        }

        Ok(schema_entries)
    }

    pub fn read_page(&mut self, page_number: usize) -> Result<Vec<u8>> {
        let mut page_data = vec![0; self.page_size];
        let offset = (page_number - 1) * self.page_size;

        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.read_exact(&mut page_data)?;

        Ok(page_data)
    }

    pub fn read_table_records(&mut self, page_number: usize) -> Result<Vec<Vec<Value>>> {
        let page_data = self.read_page(page_number)?;

        let header_offset = if page_number == 1 { 100 } else { 0 };

        let cell_count =
            u16::from_be_bytes([page_data[header_offset + 3], page_data[header_offset + 4]])
                as usize;

        let offsets_array_start = header_offset + 8;
        let mut records = Vec::new();

        for i in 0..cell_count {
            let offset_pos = offsets_array_start + (i as usize * 2);
            let cell_offset =
                u16::from_be_bytes([page_data[offset_pos], page_data[offset_pos + 1]]) as usize;

            let cell_data = &page_data[cell_offset..];
            let (_, rest, _) = read_varint(cell_data)?;
            let (_, rest, _) = read_varint(rest)?;

            let record = parse_record(rest)?;
            records.push(record);
        }

        Ok(records)
    }
}
