use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub struct WhereCondition {
    pub column: String,
    pub operator: String,
    pub value: String,
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum QueryType {
    Select {
        columns: Vec<String>,
        table: String,
        where_clause: Option<WhereCondition>,
    },
    SelectCount {
        table: String,
    },
    Unknown,
}

pub fn parse_query(query: &str) -> Result<QueryType> {
    let query_lower = query.trim().to_lowercase();
    let original_query_trimmed = query.trim();

    if query_lower.starts_with("select") {
        let parts: Vec<&str> = query_lower.split_whitespace().collect();

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

        let select_keyword_len = "select".len();
        let from_keyword = " from ";
        let where_keyword = " where ";

        if let Some(from_pos_lower) = query_lower.find(from_keyword) {
            let from_pos_original = original_query_trimmed
                .to_lowercase()
                .find(from_keyword)
                .unwrap_or(from_pos_lower);

            let columns_part_str =
                original_query_trimmed[select_keyword_len..from_pos_original].trim();

            let remaining_part_str_original =
                original_query_trimmed[from_pos_original + from_keyword.len()..].trim();
            let remaining_part_str_lower = remaining_part_str_original.to_lowercase();

            let columns: Vec<String> = columns_part_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if columns.is_empty() {
                bail!("No columns specified in SELECT query");
            }

            let table_name_str: String;
            let mut where_clause: Option<WhereCondition> = None;

            if let Some(where_pos_lower) = remaining_part_str_lower.find(where_keyword) {
                let where_pos_original = remaining_part_str_original
                    .to_lowercase()
                    .find(where_keyword)
                    .unwrap_or(where_pos_lower);

                table_name_str = remaining_part_str_original[..where_pos_original]
                    .trim()
                    .to_string();
                let condition_str =
                    remaining_part_str_original[where_pos_original + where_keyword.len()..].trim();

                let condition_parts: Vec<&str> =
                    condition_str.splitn(2, '=').map(|s| s.trim()).collect();
                if condition_parts.len() == 2 {
                    let column = condition_parts[0].to_string();
                    let mut value_str = condition_parts[1].to_string();

                    if value_str.starts_with('\'')
                        && value_str.ends_with('\'')
                        && value_str.len() >= 2
                    {
                        value_str = value_str[1..value_str.len() - 1].to_string();
                    } else {
                        // For now, only string literals are supported as per the challenge
                        bail!("WHERE clause value must be a string literal enclosed in single quotes, e.g., 'Yellow'");
                    }

                    where_clause = Some(WhereCondition {
                        column,
                        operator: "=".to_string(),
                        value: value_str,
                    });
                } else {
                    bail!("Invalid WHERE clause format. Expected 'column = \\'value\\''");
                }
            } else {
                table_name_str = remaining_part_str_original.to_string();
            }

            if table_name_str.is_empty() {
                bail!("Missing table name in SELECT query");
            }

            return Ok(QueryType::Select {
                columns,
                table: table_name_str,
                where_clause,
            });
        }
        bail!("Invalid SELECT query format. Missing FROM clause.");
    }

    bail!("Unsupported SQL query: {}", query)
}
