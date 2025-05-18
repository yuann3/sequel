use anyhow::{bail, Result};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub fn read_varint(bytes: &[u8]) -> Result<(u64, &[u8], usize)> {
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

pub fn parse_record(bytes: &[u8]) -> Result<Vec<Value>> {
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

pub fn parse_value(serial_type: u64, bytes: &[u8]) -> (Value, usize) {
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
