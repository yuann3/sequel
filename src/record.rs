use anyhow::{bail, Context, Result};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

pub fn read_varint(bytes: &[u8]) -> Result<(u64, &[u8], usize)> {
    let mut result: u64 = 0;
    let mut bytes_read: usize = 0;

    if bytes.is_empty() {
        bail!("Cannot read varint from empty slice");
    }

    for i in 0..9 {
        if i >= bytes.len() {
            bail!(
                "Unexpected end of bytes when reading varint (tried to read byte {}, slice len {})",
                i + 1,
                bytes.len()
            );
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

pub fn parse_record(record_payload: &[u8]) -> Result<Vec<Value>> {
    // K: total_header_size, L: bytes_for_k_varint
    // The first varint in record_payload is K.
    // It is followed by K-L bytes which are the serial type definitions.
    // The rest is the body.
    let (k_total_header_size, cursor_after_k_varint, l_bytes_for_k_varint) =
        read_varint(record_payload)
            .context("Failed to read record's total header size (K) varint")?;

    if k_total_header_size < l_bytes_for_k_varint as u64 {
        bail!(
            "Record's total header size K ({}) is less than the size of K's varint L ({}). Invalid record.",
            k_total_header_size, l_bytes_for_k_varint
        );
    }

    // Length of the part of the header that contains the list of serial types (K-L)
    let serial_types_section_len = k_total_header_size as usize - l_bytes_for_k_varint;

    if serial_types_section_len > cursor_after_k_varint.len() {
        bail!(
            "Record's declared serial types section length (K-L = {}) is greater than actual remaining data length ({}) after K-varint.",
            serial_types_section_len,
            cursor_after_k_varint.len()
        );
    }

    let serial_types_data = &cursor_after_k_varint[..serial_types_section_len];
    let mut body_data_cursor = &cursor_after_k_varint[serial_types_section_len..];

    let mut serial_types_scan_pos = 0;
    let mut column_serial_types = Vec::new();

    while serial_types_scan_pos < serial_types_section_len {
        let (serial_type, _, bytes_read_for_st) =
            read_varint(&serial_types_data[serial_types_scan_pos..]).with_context(|| {
                format!(
                    "Failed to read serial type varint from serial types section at offset {}",
                    serial_types_scan_pos
                )
            })?;

        if bytes_read_for_st == 0 {
            bail!("Read 0 bytes for a serial type varint in header (should not happen).");
        }
        serial_types_scan_pos += bytes_read_for_st;
        column_serial_types.push(serial_type);
    }

    let mut values = Vec::new();
    for (idx, &serial_type) in column_serial_types.iter().enumerate() {
        let (value, bytes_consumed_by_value) = parse_value(serial_type, body_data_cursor)
            .with_context(|| {
                format!(
                    "Failed to parse value for column {} (serial type {})",
                    idx, serial_type
                )
            })?;

        values.push(value);
        if bytes_consumed_by_value > body_data_cursor.len() {
            bail!(
                 "Value parser for serial type {} reported consuming {} bytes, but only {} bytes remain in body.",
                 serial_type, bytes_consumed_by_value, body_data_cursor.len()
             );
        }
        body_data_cursor = &body_data_cursor[bytes_consumed_by_value..];
    }

    Ok(values)
}

pub fn parse_value(serial_type: u64, bytes: &[u8]) -> Result<(Value, usize)> {
    match serial_type {
        0 => Ok((Value::Null, 0)),
        1 => {
            // Int8
            if bytes.is_empty() {
                bail!("Not enough data for Int8 (1 byte)");
            }
            Ok((Value::Int(bytes[0] as i8 as i64), 1))
        }
        2 => {
            // Int16
            if bytes.len() < 2 {
                bail!("Not enough data for Int16 (2 bytes)");
            }
            Ok((
                Value::Int(i16::from_be_bytes([bytes[0], bytes[1]]) as i64),
                2,
            ))
        }
        3 => {
            // Int24
            if bytes.len() < 3 {
                bail!("Not enough data for Int24 (3 bytes)");
            }
            let val = ((bytes[0] as i64) << 16) | ((bytes[1] as i64) << 8) | (bytes[2] as i64);
            // Sign extend if the 24th bit (0x800000) is set
            Ok((
                Value::Int(if val & 0x800000 != 0 {
                    val | !0xFFFFFF
                } else {
                    val
                }),
                3,
            ))
        }
        4 => {
            // Int32
            if bytes.len() < 4 {
                bail!("Not enough data for Int32 (4 bytes)");
            }
            Ok((
                Value::Int(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64),
                4,
            ))
        }
        5 => {
            // Int48 (6 bytes)
            if bytes.len() < 6 {
                bail!("Not enough data for Int48 (6 bytes)");
            }
            let mut buf = [0u8; 8]; // Buffer for full i64
                                    // Check sign bit of the 48-bit number (MSB of the first byte)
            if bytes[0] & 0x80 != 0 {
                buf[0] = 0xFF; // Sign extend with 0xFF for negative
                buf[1] = 0xFF;
            } // else positive, buf[0] and buf[1] remain 0x00
            buf[2..8].copy_from_slice(&bytes[0..6]); // Copy the 6 data bytes
            Ok((Value::Int(i64::from_be_bytes(buf)), 6))
        }
        6 => {
            // Int64
            if bytes.len() < 8 {
                bail!("Not enough data for Int64 (8 bytes)");
            }
            Ok((
                Value::Int(i64::from_be_bytes(
                    bytes[0..8]
                        .try_into()
                        .context("Failed to convert slice to 8-byte array for Int64")?,
                )),
                8,
            ))
        }
        7 => {
            // Float64
            if bytes.len() < 8 {
                bail!("Not enough data for Float64 (8 bytes)");
            }
            Ok((
                Value::Float(f64::from_be_bytes(
                    bytes[0..8]
                        .try_into()
                        .context("Failed to convert slice to 8-byte array for Float64")?,
                )),
                8,
            ))
        }
        8 => Ok((Value::Int(0), 0)), // Constant 0
        9 => Ok((Value::Int(1), 0)), // Constant 1
        st if st == 10 || st == 11 => {
            bail!("Reserved serial type {} encountered. These are unused.", st)
        }
        st if st >= 12 => {
            // Blob or Text
            let len = ((st - (if st % 2 == 0 { 12 } else { 13 })) / 2) as usize;
            if bytes.len() < len {
                bail!(
                    "Not enough data for {} (serial type {}): expected {} bytes, got {}",
                    if st % 2 == 0 { "Blob" } else { "Text" },
                    st,
                    len,
                    bytes.len()
                );
            }
            if st % 2 == 0 {
                // Blob
                Ok((Value::Blob(bytes[..len].to_vec()), len))
            } else {
                // Text
                match String::from_utf8(bytes[..len].to_vec()) {
                    Ok(text) => Ok((Value::Text(text), len)),
                    Err(e) => bail!(
                        "Invalid UTF-8 sequence for Text (serial type {}): {}",
                        st,
                        e
                    ),
                }
            }
        }
        _ => bail!("Unknown or unhandled serial type: {}", serial_type),
    }
}
