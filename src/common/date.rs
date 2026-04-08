//! ISO 8601 date parsing and formatting utilities.
//!
//! Stores dates as epoch milliseconds (i64) — the internal representation
//! used by `FieldType::Date` in Tantivy i64 fast fields.

use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use serde_json::Value;

/// Parse an ISO 8601 date/datetime string into epoch milliseconds (UTC).
///
/// Supported formats:
/// - RFC 3339 / ISO 8601 datetimes with timezone offsets
/// - `YYYY-MM-DDTHH:MM:SS[.fraction]` → treated as UTC
/// - `YYYY-MM-DD` → midnight UTC
///
/// Returns `None` for unparseable or out-of-range values.
pub fn parse_iso8601_to_epoch_millis(s: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.timestamp_millis())
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                .ok()
                .map(|dt| dt.and_utc().timestamp_millis())
        })
        .or_else(|| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|dt| dt.and_utc().timestamp_millis())
        })
}

/// Format epoch milliseconds as an ISO 8601 UTC string.
pub fn epoch_millis_to_iso8601(millis: i64) -> String {
    Utc.timestamp_millis_opt(millis)
        .single()
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::AutoSi, true))
        .unwrap_or_else(|| millis.to_string())
}

pub fn normalize_json_date_value(value: &Value) -> Option<Value> {
    match value {
        Value::String(text) => parse_iso8601_to_epoch_millis(text)
            .map(epoch_millis_to_iso8601)
            .map(Value::String),
        Value::Number(number) => number
            .as_i64()
            .map(epoch_millis_to_iso8601)
            .map(Value::String),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_date_only() {
        assert_eq!(
            parse_iso8601_to_epoch_millis("2025-01-05"),
            Some(1_736_035_200_000)
        );
    }

    #[test]
    fn parse_datetime_no_timezone() {
        assert_eq!(
            parse_iso8601_to_epoch_millis("2025-01-05T08:15:00"),
            Some(1_736_064_900_000)
        );
    }

    #[test]
    fn parse_datetime_utc_z() {
        assert_eq!(
            parse_iso8601_to_epoch_millis("2025-01-05T08:15:00Z"),
            Some(1_736_064_900_000)
        );
    }

    #[test]
    fn parse_datetime_with_millis() {
        assert_eq!(
            parse_iso8601_to_epoch_millis("2025-01-05T08:15:00.123Z"),
            Some(1_736_064_900_123)
        );
    }

    #[test]
    fn parse_datetime_utc_offset() {
        assert_eq!(
            parse_iso8601_to_epoch_millis("2025-01-05T08:15:00+00:00"),
            Some(1_736_064_900_000)
        );
    }

    #[test]
    fn parse_non_utc_offset_and_normalize_to_utc() {
        let millis = parse_iso8601_to_epoch_millis("2025-01-05T08:15:00+05:30").unwrap();
        assert_eq!(epoch_millis_to_iso8601(millis), "2025-01-05T02:45:00Z");
    }

    #[test]
    fn parse_rejects_garbage() {
        assert_eq!(parse_iso8601_to_epoch_millis("not-a-date"), None);
        assert_eq!(parse_iso8601_to_epoch_millis(""), None);
        assert_eq!(parse_iso8601_to_epoch_millis("2025-13-01"), None); // invalid month
        assert_eq!(parse_iso8601_to_epoch_millis("2025-02-29"), None); // 2025 is not leap
    }

    #[test]
    fn parse_leap_year() {
        assert!(parse_iso8601_to_epoch_millis("2024-02-29").is_some()); // 2024 is leap
    }

    #[test]
    fn roundtrip_epoch_zero() {
        let millis = parse_iso8601_to_epoch_millis("1970-01-01T00:00:00Z").unwrap();
        assert_eq!(millis, 0);
        assert_eq!(epoch_millis_to_iso8601(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn roundtrip_recent_date() {
        let input = "2025-01-05T08:15:00Z";
        let millis = parse_iso8601_to_epoch_millis(input).unwrap();
        let output = epoch_millis_to_iso8601(millis);
        assert_eq!(output, input);
    }

    #[test]
    fn roundtrip_with_millis() {
        let millis = parse_iso8601_to_epoch_millis("2025-01-05T08:15:00.123Z").unwrap();
        assert_eq!(epoch_millis_to_iso8601(millis), "2025-01-05T08:15:00.123Z");
    }

    #[test]
    fn format_no_trailing_millis_when_zero() {
        let millis = parse_iso8601_to_epoch_millis("2025-01-05T08:15:00").unwrap();
        let output = epoch_millis_to_iso8601(millis);
        assert_eq!(output, "2025-01-05T08:15:00Z");
    }

    #[test]
    fn parse_negative_epoch() {
        // 1969-12-31 = -86400000 millis
        let millis = parse_iso8601_to_epoch_millis("1969-12-31").unwrap();
        assert_eq!(millis, -86_400_000);
        assert_eq!(epoch_millis_to_iso8601(millis), "1969-12-31T00:00:00Z");
    }

    #[test]
    fn normalize_json_date_value_converts_numbers_and_strings_to_iso() {
        assert_eq!(
            normalize_json_date_value(&Value::from(1_736_064_900_123_i64)),
            Some(Value::String("2025-01-05T08:15:00.123Z".into()))
        );
        assert_eq!(
            normalize_json_date_value(&Value::String("2025-01-05T08:15:00+05:30".into())),
            Some(Value::String("2025-01-05T02:45:00Z".into()))
        );
    }
}
