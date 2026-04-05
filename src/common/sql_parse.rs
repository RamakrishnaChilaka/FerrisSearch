//! Shared SQL identifier parsing helpers used by both the API layer and the CLI.

/// Remove surrounding quotes from an identifier: `"foo"` → `foo`, `` `foo` `` → `foo`, `'foo'` → `foo`.
pub fn unquote_identifier(s: &str) -> String {
    let s = s.trim().trim_end_matches(';').trim();
    if (s.starts_with('"') && s.ends_with('"'))
        || (s.starts_with('`') && s.ends_with('`'))
        || (s.starts_with('\'') && s.ends_with('\''))
    {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Convert a sqlparser `ObjectName` to a dot-joined unquoted string.
pub fn object_name_to_string(name: &sqlparser::ast::ObjectName) -> Option<String> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| unquote_identifier(&part.to_string()))
        .filter(|part| !part.is_empty())
        .collect();

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    }
}

/// Find the byte offset of a top-level SQL keyword, respecting quotes and
/// parenthesis nesting. Returns `None` if the keyword does not appear at top
/// level with word boundaries.
pub fn find_top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick_quote = false;
    let mut paren_depth = 0usize;

    for (idx, ch) in sql.char_indices() {
        if in_single_quote {
            if ch == '\'' {
                in_single_quote = false;
            }
            continue;
        }
        if in_double_quote {
            if ch == '"' {
                in_double_quote = false;
            }
            continue;
        }
        if in_backtick_quote {
            if ch == '`' {
                in_backtick_quote = false;
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single_quote = true;
                continue;
            }
            '"' => {
                in_double_quote = true;
                continue;
            }
            '`' => {
                in_backtick_quote = true;
                continue;
            }
            '(' => {
                paren_depth += 1;
                continue;
            }
            ')' => {
                paren_depth = paren_depth.saturating_sub(1);
                continue;
            }
            _ => {}
        }

        if paren_depth != 0 || idx + keyword.len() > sql.len() {
            continue;
        }

        if !sql[idx..idx + keyword.len()].eq_ignore_ascii_case(keyword) {
            continue;
        }

        let prev_ok = idx == 0
            || !sql[..idx]
                .chars()
                .next_back()
                .is_some_and(|prev| prev.is_ascii_alphanumeric() || prev == '_');
        let next_ok = sql[idx + keyword.len()..]
            .chars()
            .next()
            .is_some_and(|next| next.is_ascii_whitespace());

        if prev_ok && next_ok {
            return Some(idx);
        }
    }

    None
}

/// Extract a possibly-quoted, possibly-dot-qualified identifier starting at
/// `input`. Stops at the first unquoted non-identifier character.
pub fn extract_qualified_identifier(input: &str) -> Option<String> {
    let mut rest = input.trim_start();
    let mut parts = Vec::new();

    loop {
        if rest.is_empty() {
            break;
        }

        let first = rest.chars().next()?;
        if matches!(first, '"' | '\'' | '`') {
            let end = rest[1..].find(first)? + 1;
            parts.push(rest[1..end].to_string());
            rest = &rest[end + 1..];
        } else {
            let end = rest
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-')
                .unwrap_or(rest.len());
            if end == 0 {
                return None;
            }
            parts.push(rest[..end].to_string());
            rest = &rest[end..];
        }

        rest = rest.trim_start();
        if let Some(next) = rest.strip_prefix('.') {
            rest = next.trim_start();
            continue;
        }
        break;
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    }
}

/// Fallback index extractor for `SELECT … FROM "table"` queries when
/// `sqlparser` cannot parse the SQL (e.g. quoted hyphenated identifiers).
pub fn extract_index_from_sql_fallback(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SELECT"))
    {
        return None;
    }

    let from_pos = find_top_level_keyword(trimmed, "FROM")?;
    let after_from = trimmed.get(from_pos + 4..)?;
    extract_qualified_identifier(after_from).map(|name| unquote_identifier(&name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unquote_double_quotes() {
        assert_eq!(unquote_identifier(r#""foo""#), "foo");
    }

    #[test]
    fn unquote_backticks() {
        assert_eq!(unquote_identifier("`bar`"), "bar");
    }

    #[test]
    fn unquote_single_quotes() {
        assert_eq!(unquote_identifier("'baz'"), "baz");
    }

    #[test]
    fn unquote_plain_identifier() {
        assert_eq!(unquote_identifier("plain"), "plain");
    }

    #[test]
    fn unquote_with_trailing_semicolon() {
        assert_eq!(unquote_identifier(r#""name";"#), "name");
    }

    #[test]
    fn find_top_level_from_simple() {
        let sql = "SELECT a FROM t WHERE x = 1";
        assert_eq!(find_top_level_keyword(sql, "FROM"), Some(9));
    }

    #[test]
    fn find_top_level_from_inside_parens_skipped() {
        let sql = "SELECT a FROM t WHERE x IN (SELECT b FROM u)";
        // Should find the first top-level FROM, not the nested one
        assert_eq!(find_top_level_keyword(sql, "FROM"), Some(9));
    }

    #[test]
    fn find_top_level_from_inside_string_skipped() {
        let sql = r#"SELECT a FROM t WHERE x = 'FROM'"#;
        assert_eq!(find_top_level_keyword(sql, "FROM"), Some(9));
    }

    #[test]
    fn find_top_level_keyword_case_insensitive() {
        let sql = "SELECT a from t";
        assert_eq!(find_top_level_keyword(sql, "FROM"), Some(9));
    }

    #[test]
    fn extract_qualified_quoted_hyphen() {
        assert_eq!(
            extract_qualified_identifier(r#" "my-index" WHERE"#),
            Some("my-index".to_string())
        );
    }

    #[test]
    fn extract_qualified_unquoted_hyphen() {
        assert_eq!(
            extract_qualified_identifier(" my-index WHERE"),
            Some("my-index".to_string())
        );
    }

    #[test]
    fn fallback_count_star_quoted_hyphen() {
        assert_eq!(
            extract_index_from_sql_fallback(r#"SELECT count(*) FROM "benchmark-1gb""#),
            Some("benchmark-1gb".to_string())
        );
    }

    #[test]
    fn fallback_lowercase_from() {
        assert_eq!(
            extract_index_from_sql_fallback(r#"select count(*) from "benchmark-1gb""#),
            Some("benchmark-1gb".to_string())
        );
    }

    #[test]
    fn fallback_rejects_non_select() {
        assert_eq!(extract_index_from_sql_fallback("SHOW TABLES"), None);
    }
}
