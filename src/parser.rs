use anyhow::{bail, Result};

#[allow(dead_code)]
#[derive(Debug)]
pub enum QueryType {
    Select { column: String, table: String },
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

        if parts.len() >= 4 && parts[0] == "select" && parts[2] == "from" {
            let column = parts[1].to_string();
            let table = parts[3].to_string();
            return Ok(QueryType::Select { column, table });
        }

        bail!("Invalid SELECT query format");
    }

    bail!("Unsupported SQL query")
}

// pub fn parse_select_query(query: &str) -> Result<(String, String)> {
//     match parse_query(query)? {
//         QueryType::Select { column, table } => Ok((column, table)),
//         _ => bail!("Not a simple SELECT query"),
//     }
// }
