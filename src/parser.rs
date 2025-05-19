use anyhow::{bail, Result};

#[allow(dead_code)]
#[derive(Debug)]
pub enum QueryType {
    Select { columns: Vec<String>, table: String },
    SelectCount { table: String },
    Unknown,
}

pub fn parse_query(query: &str) -> Result<QueryType> {
    let query = query.trim().to_lowercase();

    if query.starts_with("select") {
        let parts: Vec<&str> = query.split_whitespace().collect();

        if parts.len() >= 4
            && parts[0] == "select"
            && (parts[1] == "count(*)"
                || (parts[1] == "count" && parts[2] == "(*)" && parts[3] == "from"))
        {
            let table_index = if parts[1] == "count(*)" { 3 } else { 4 };
            if parts.len() <= table_index {
                bail!("Missing table name in SELECT COUNT query");
            }
            let table = parts[table_index].to_string();
            return Ok(QueryType::SelectCount { table });
        }

        if parts[0] == "select" {
            let from_pos = query.find(" from ");
            if let Some(pos) = from_pos {
                let columns_part = &query[7..pos].trim();
                let table_part = &query[pos + 6..].trim();

                if table_part.is_empty() {
                    bail!("Missing table name in SELECT query");
                }

                let columns: Vec<String> = columns_part
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                if columns.is_empty() {
                    bail!("No columns specified in SELECT query");
                }

                let table = table_part.to_string();
                return Ok(QueryType::Select { columns, table });
            }
        }

        bail!("Invalid SELECT query format");
    }

    bail!("Unsupported SQL query")
}
