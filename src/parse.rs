use anyhow::Result;
use pg_query::protobuf;

pub fn parse(sql: &str) -> Result<pg_query::ParseResult> {
    pg_query::parse(sql).map_err(|e| anyhow::anyhow!("Parse error: {e}"))
}

pub fn extract_statement_sql(source: &str, stmt: &protobuf::RawStmt) -> String {
    let start = stmt.stmt_location as usize;
    let len = if stmt.stmt_len > 0 {
        stmt.stmt_len as usize
    } else {
        source.len() - start
    };
    let end = (start + len).min(source.len());
    source[start..end].trim().to_string()
}

pub fn format_relation(r: &protobuf::RangeVar) -> String {
    if r.schemaname.is_empty() {
        r.relname.clone()
    } else {
        format!("{}.{}", r.schemaname, r.relname)
    }
}
