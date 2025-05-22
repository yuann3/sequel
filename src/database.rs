use crate::record::{parse_record, read_varint, Value};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

#[derive(Debug, PartialEq)]
pub enum BTreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

impl BTreePageType {
    pub fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0x02 => Ok(BTreePageType::InteriorIndex),
            0x05 => Ok(BTreePageType::InteriorTable),
            0x0a => Ok(BTreePageType::LeafIndex),
            0x0d => Ok(BTreePageType::LeafTable),
            _ => bail!("Invalid B-tree page type: {}", byte),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct BTreePageHeader {
    pub page_type: BTreePageType,
    pub first_freeblock: u16,
    pub cell_count: u16,
    pub cell_content_start: u32,
    pub fragmented_free_bytes: u8,
    pub right_most_pointer: Option<u32>,
}

impl BTreePageHeader {
    pub fn parse(data: &[u8], _is_page_one: bool) -> Result<Self> {
        if data.len() < 8 {
            bail!("Page data too short to parse header");
        }

        let page_type = BTreePageType::from_byte(data[0])?;
        let is_interior = matches!(
            page_type,
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable
        );
        if is_interior && data.len() < 12 {
            bail!("Interior page data too short to parse header");
        }

        let first_freeblock = u16::from_be_bytes([data[1], data[2]]);
        let cell_count = u16::from_be_bytes([data[3], data[4]]);
        let cell_content_start = u16::from_be_bytes([data[5], data[6]]);
        let fragmented_free_bytes = data[7];
        let right_most_pointer = if is_interior {
            Some(u32::from_be_bytes([data[8], data[9], data[10], data[11]]))
        } else {
            None
        };

        Ok(BTreePageHeader {
            page_type,
            first_freeblock,
            cell_count,
            cell_content_start: if cell_content_start == 0 {
                65536
            } else {
                cell_content_start as u32
            },
            fragmented_free_bytes,
            right_most_pointer,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct TableBTreeLeafCell {
    pub payload_size: u64,
    pub rowid: u64,
    pub payload: Bytes,
    pub overflow_page: Option<u32>,
}

impl TableBTreeLeafCell {
    pub fn parse(data: &[u8]) -> Result<(Self, usize)> {
        let mut offset = 0;

        let (payload_size, rest, bytes_read) =
            read_varint(data).context("Failed to read payload size varint")?;
        offset += bytes_read;

        let (rowid, rest, bytes_read) = read_varint(rest).context("Failed to read rowid varint")?;
        offset += bytes_read;

        if rest.len() < payload_size as usize {
            bail!(
                "Not enough data for payload: expected {} bytes, got {}",
                payload_size,
                rest.len()
            );
        }
        let payload = Bytes::from(rest[..payload_size as usize].to_vec());
        offset += payload_size as usize;

        let overflow_page = if rest.len() >= payload_size as usize + 4 {
            let overflow_value = u32::from_be_bytes([
                rest[payload_size as usize],
                rest[payload_size as usize + 1],
                rest[payload_size as usize + 2],
                rest[payload_size as usize + 3],
            ]);
            if overflow_value != 0 {
                Some(overflow_value)
            } else {
                None
            }
        } else {
            None
        };
        if overflow_page.is_some() {
            offset += 4;
        }

        Ok((
            TableBTreeLeafCell {
                payload_size,
                rowid,
                payload,
                overflow_page,
            },
            offset,
        ))
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct TableBTreeInteriorCell {
    pub left_child_page: u32,
    pub rowid: u64,
}

impl TableBTreeInteriorCell {
    pub fn parse(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 4 {
            bail!("Not enough data for interior cell left child pointer");
        }

        let left_child_page = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let (rowid, _, bytes_read) =
            read_varint(&data[4..]).context("Failed to read rowid varint")?;

        Ok((
            TableBTreeInteriorCell {
                left_child_page,
                rowid,
            },
            4 + bytes_read,
        ))
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct IndexBTreeLeafCell {
    pub payload_size: u64,
    pub payload: Bytes,
}

impl IndexBTreeLeafCell {
    pub fn parse(data: &[u8]) -> Result<(Self, usize)> {
        let mut offset = 0;

        let (payload_size, rest, bytes_read) =
            read_varint(data).context("Failed to read index leaf cell payload size varint")?;
        offset += bytes_read;

        if rest.len() < payload_size as usize {
            bail!(
                "Not enough data for index leaf cell payload: expected {} bytes, got {}",
                payload_size,
                rest.len()
            );
        }
        let payload = Bytes::from(rest[..payload_size as usize].to_vec());
        offset += payload_size as usize;

        Ok((
            IndexBTreeLeafCell {
                payload_size,
                payload,
            },
            offset,
        ))
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct IndexBTreeInteriorCell {
    pub left_child_page: u32,
    pub payload_size: u64,
    pub payload: Bytes,
}

impl IndexBTreeInteriorCell {
    pub fn parse(data: &[u8]) -> Result<(Self, usize)> {
        let mut offset = 0;

        if data.len() < 4 {
            bail!("Not enough data for index interior cell left child pointer");
        }
        let left_child_page = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        offset += 4;

        let (payload_size, rest, bytes_read) = read_varint(&data[offset..])
            .context("Failed to read index interior cell payload size varint")?;
        offset += bytes_read;

        if rest.len() < payload_size as usize {
            bail!(
                "Not enough data for index interior cell payload: expected {} bytes, got {}",
                payload_size,
                rest.len()
            );
        }
        let payload = Bytes::from(rest[..payload_size as usize].to_vec());
        offset += payload_size as usize;

        Ok((
            IndexBTreeInteriorCell {
                left_child_page,
                payload_size,
                payload,
            },
            offset,
        ))
    }
}

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

    pub fn collect_leaf_pages(&mut self, root_page: u32) -> Result<Vec<u32>> {
        let mut leaf_pages = Vec::new();
        let mut stack = vec![root_page];

        while let Some(page_number) = stack.pop() {
            let page_data = self.read_page(page_number as usize)?;
            let is_page_one = page_number == 1;
            let header_offset = if is_page_one { 100 } else { 0 };
            let header_data = &page_data[header_offset..];
            let header = BTreePageHeader::parse(header_data, is_page_one)?;

            match header.page_type {
                BTreePageType::LeafTable => {
                    leaf_pages.push(page_number);
                }
                BTreePageType::InteriorTable => {
                    let cell_pointers_start = header_offset
                        + if header.right_most_pointer.is_some() {
                            12
                        } else {
                            8
                        };
                    let cell_count = header.cell_count as usize;
                    let mut child_pages = Vec::new();

                    for i in 0..cell_count {
                        let pointer_offset = cell_pointers_start + i * 2;
                        if pointer_offset + 2 > page_data.len() {
                            bail!("Cell pointer offset out of bounds");
                        }
                        let cell_offset = u16::from_be_bytes([
                            page_data[pointer_offset],
                            page_data[pointer_offset + 1],
                        ]) as usize;
                        let cell_data = &page_data[cell_offset..];
                        let (cell, _) = TableBTreeInteriorCell::parse(cell_data)?;
                        child_pages.push(cell.left_child_page);
                    }

                    if let Some(right_most) = header.right_most_pointer {
                        child_pages.push(right_most);
                    }

                    for &child_page in child_pages.iter().rev() {
                        stack.push(child_page);
                    }
                }
                _ => bail!(
                    "Unexpected page type for table B-tree: {:?}",
                    header.page_type
                ),
            }
        }

        Ok(leaf_pages)
    }

    pub fn read_table_records(&mut self, root_page: u32) -> Result<Vec<Vec<Value>>> {
        let leaf_pages = self.collect_leaf_pages(root_page)?;
        let mut all_records = Vec::new();

        for page_number in leaf_pages {
            let page_data = self.read_page(page_number as usize)?;
            let is_page_one = page_number == 1;
            let header_offset = if is_page_one { 100 } else { 0 };
            let header_data = &page_data[header_offset..];
            let header = BTreePageHeader::parse(header_data, is_page_one)?;

            if header.page_type != BTreePageType::LeafTable {
                bail!("Expected leaf table page, got {:?}", header.page_type);
            }

            let cell_pointers_start = header_offset + 8;
            let cell_count = header.cell_count as usize;

            for i in 0..cell_count {
                let pointer_offset = cell_pointers_start + i * 2;
                if pointer_offset + 2 > page_data.len() {
                    bail!("Cell pointer offset out of bounds");
                }
                let cell_offset =
                    u16::from_be_bytes([page_data[pointer_offset], page_data[pointer_offset + 1]])
                        as usize;
                let cell_data = &page_data[cell_offset..];
                let (cell, _) = TableBTreeLeafCell::parse(cell_data)?;

                let mut record = parse_record(&cell.payload)?;
                record.insert(0, Value::Int(cell.rowid as i64));

                all_records.push(record);
            }
        }

        Ok(all_records)
    }

    pub fn collect_index_rowids(
        &mut self,
        index_root_page: u32,
        target_country: &str,
    ) -> Result<Vec<u64>> {
        let mut rowids = Vec::new();
        let mut stack = vec![index_root_page];

        while let Some(page_number) = stack.pop() {
            let page_data = self.read_page(page_number as usize)?;
            let is_page_one = page_number == 1;
            let header_offset = if is_page_one { 100 } else { 0 };
            let header_data = &page_data[header_offset..];
            let header = BTreePageHeader::parse(header_data, is_page_one)?;

            match header.page_type {
                BTreePageType::LeafIndex => {
                    let cell_pointers_start = header_offset + 8;
                    let cell_count = header.cell_count as usize;

                    for i in 0..cell_count {
                        let pointer_offset = cell_pointers_start + i * 2;
                        if pointer_offset + 2 > page_data.len() {
                            bail!("Index leaf cell pointer offset out of bounds");
                        }
                        let cell_offset = u16::from_be_bytes([
                            page_data[pointer_offset],
                            page_data[pointer_offset + 1],
                        ]) as usize;
                        let cell_data = &page_data[cell_offset..];
                        let (cell, _) = IndexBTreeLeafCell::parse(cell_data)?;
                        let record = parse_record(&cell.payload)?;
                        if record.len() >= 2 {
                            if let (Value::Text(country), Value::Int(rowid)) =
                                (&record[0], &record[1])
                            {
                                if country == target_country {
                                    rowids.push(*rowid as u64);
                                }
                            }
                        }
                    }
                }
                BTreePageType::InteriorIndex => {
                    let cell_pointers_start = header_offset + 12;
                    let cell_count = header.cell_count as usize;
                    let mut child_pages = Vec::new();

                    for i in 0..cell_count {
                        let pointer_offset = cell_pointers_start + i * 2;
                        if pointer_offset + 2 > page_data.len() {
                            bail!("Index interior cell pointer offset out of bounds");
                        }
                        let cell_offset = u16::from_be_bytes([
                            page_data[pointer_offset],
                            page_data[pointer_offset + 1],
                        ]) as usize;
                        let cell_data = &page_data[cell_offset..];
                        let (cell, _) = IndexBTreeInteriorCell::parse(cell_data)?;
                        let record = parse_record(&cell.payload)?;
                        if record.len() >= 1 {
                            if let Value::Text(country) = &record[0] {
                                if target_country <= country.as_str() {
                                    child_pages.push(cell.left_child_page);
                                }
                            }
                        }
                    }

                    if let Some(right_most) = header.right_most_pointer {
                        child_pages.push(right_most);
                    }

                    for &child_page in child_pages.iter().rev() {
                        stack.push(child_page);
                    }
                }
                _ => bail!(
                    "Unexpected page type for index B-tree: {:?}",
                    header.page_type
                ),
            }
        }

        rowids.sort();
        Ok(rowids)
    }

    pub fn read_table_records_by_rowids(
        &mut self,
        table_root_page: u32,
        target_rowids: &[u64],
    ) -> Result<Vec<Vec<Value>>> {
        if target_rowids.is_empty() {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        let mut stack = vec![table_root_page];
        let rowid_set: std::collections::HashSet<u64> = target_rowids.iter().copied().collect();

        while let Some(page_number) = stack.pop() {
            let page_data = self.read_page(page_number as usize)?;
            let is_page_one = page_number == 1;
            let header_offset = if is_page_one { 100 } else { 0 };
            let header_data = &page_data[header_offset..];
            let header = BTreePageHeader::parse(header_data, is_page_one)?;

            match header.page_type {
                BTreePageType::LeafTable => {
                    let cell_pointers_start = header_offset + 8;
                    let cell_count = header.cell_count as usize;

                    for i in 0..cell_count {
                        let pointer_offset = cell_pointers_start + i * 2;
                        if pointer_offset + 2 > page_data.len() {
                            bail!("Table leaf cell pointer offset out of bounds");
                        }
                        let cell_offset = u16::from_be_bytes([
                            page_data[pointer_offset],
                            page_data[pointer_offset + 1],
                        ]) as usize;
                        let cell_data = &page_data[cell_offset..];
                        let (cell, _) = TableBTreeLeafCell::parse(cell_data)?;

                        if rowid_set.contains(&cell.rowid) {
                            let mut record = parse_record(&cell.payload)?;
                            record.insert(0, Value::Int(cell.rowid as i64));
                            records.push(record);
                        }
                    }
                }
                BTreePageType::InteriorTable => {
                    let cell_pointers_start = header_offset + 12;
                    let cell_count = header.cell_count as usize;
                    let mut child_pages = Vec::new();

                    let min_target = *target_rowids.iter().min().unwrap_or(&0);

                    for i in 0..cell_count {
                        let pointer_offset = cell_pointers_start + i * 2;
                        if pointer_offset + 2 > page_data.len() {
                            bail!("Table interior cell pointer offset out of bounds");
                        }
                        let cell_offset = u16::from_be_bytes([
                            page_data[pointer_offset],
                            page_data[pointer_offset + 1],
                        ]) as usize;
                        let cell_data = &page_data[cell_offset..];
                        let (cell, _) = TableBTreeInteriorCell::parse(cell_data)?;

                        if cell.rowid >= min_target {
                            child_pages.push(cell.left_child_page);
                        }
                    }

                    if let Some(right_most) = header.right_most_pointer {
                        child_pages.push(right_most);
                    }

                    for &child_page in child_pages.iter().rev() {
                        stack.push(child_page);
                    }
                }
                _ => bail!(
                    "Unexpected page type for table B-tree: {:?}",
                    header.page_type
                ),
            }
        }

        Ok(records)
    }
}
