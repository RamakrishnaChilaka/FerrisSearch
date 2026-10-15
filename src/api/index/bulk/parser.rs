use super::{BulkDoc, parse_bulk_action};
use crate::api::index::bulk::pipeline::BulkError;
use std::io::BufRead;

pub(super) const MAX_LINE_BYTES: usize = 32 * 1024 * 1024;

pub(super) struct BulkDecoder<R> {
    pub reader: R,
    pub bytes_read: u64,
    line: u64,
    items: usize,
    line_limit: usize,
}

impl<R: BufRead> BulkDecoder<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            bytes_read: 0,
            line: 0,
            items: 0,
            line_limit: MAX_LINE_BYTES,
        }
    }

    fn line(&mut self) -> Result<Option<(u64, Option<String>)>, BulkError> {
        loop {
            let mut bytes = Vec::new();
            let mut seen = false;
            let mut oversized = false;
            loop {
                let available = self.reader.fill_buf()?;
                if available.is_empty() {
                    if !seen {
                        return Ok(None);
                    }
                    break;
                }
                seen = true;
                let newline = available.iter().position(|byte| *byte == b'\n');
                let count = newline.map_or(available.len(), |offset| offset + 1);
                let content = newline.unwrap_or(count);
                if !oversized {
                    if content > self.line_limit - bytes.len() {
                        oversized = true;
                        bytes.clear();
                    } else {
                        bytes.extend_from_slice(&available[..content]);
                    }
                }
                self.reader.consume(count);
                self.bytes_read += count as u64;
                if newline.is_some() {
                    break;
                }
            }
            self.line += 1;
            if oversized {
                return Ok(Some((self.line, None)));
            }
            let text = String::from_utf8(bytes).map_err(|error| {
                BulkError::Invalid(format!(
                    "Invalid UTF-8 bulk body at line {}: {error}",
                    self.line
                ))
            })?;
            if !text.trim().is_empty() {
                return Ok(Some((self.line, Some(text))));
            }
        }
    }

    pub fn next_document(&mut self) -> Result<Option<BulkDoc>, BulkError> {
        let Some((line, action)) = self.line()? else {
            return Ok(None);
        };
        let action = action.ok_or_else(|| {
            BulkError::Limit(format!(
                "Bulk action/metadata line [{line}] exceeds {} bytes",
                self.line_limit
            ))
        })?;
        let mut document =
            parse_bulk_action(&action, line, self.items + 1).map_err(BulkError::Invalid)?;
        if document.action != "delete" {
            let (source_line, source) = self.line()?.ok_or_else(|| {
                BulkError::Invalid(format!(
                    "Malformed action/metadata line [{line}]: action [{}] requires a source line",
                    document.action
                ))
            })?;
            match source {
                Some(source) => match serde_json::from_str(&source) {
                    Ok(payload) => document.payload = payload,
                    Err(error) => {
                        document.source_error =
                            Some(format!("Malformed source line [{source_line}]: {error}"));
                    }
                },
                None => {
                    document.source_error = Some(format!(
                        "Bulk source line [{source_line}] exceeds {} bytes",
                        self.line_limit
                    ));
                }
            }
        }
        self.items += 1;
        Ok(Some(document))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    #[test]
    fn incremental_decoder_preserves_crlf_unicode_blank_lines_and_action_boundaries() {
        let input = "\r\n{\"index\":{\"_id\":\"one\"}}\r\n{\"body\":\"snow \u{96ea}\"}\r\n\
                     \r\n{\"delete\":{\"_id\":\"one\"}}\r\n\
                     {\"update\":{\"_id\":\"two\",\"if_seq_no\":0,\"if_primary_term\":2}}\r\n\
                     {\"doc\":{\"value\":7}}";
        for capacity in 1..=input.len() {
            let mut decoder =
                BulkDecoder::new(BufReader::with_capacity(capacity, Cursor::new(input)));
            let index = decoder.next_document().unwrap().unwrap();
            assert_eq!(index.doc_id, "one");
            assert_eq!(index.payload["body"], "snow \u{96ea}");
            assert_eq!(decoder.next_document().unwrap().unwrap().action, "delete");
            let update = decoder.next_document().unwrap().unwrap();
            assert_eq!(update.payload["doc"]["value"], 7);
            assert_eq!(update.condition.expected_version(), (Some(0), Some(2)));
            assert!(decoder.next_document().unwrap().is_none());
            assert_eq!(decoder.bytes_read, input.len() as u64);
        }
    }

    #[test]
    fn incremental_decoder_retains_invalid_and_oversized_source_items() {
        let input = "{\"index\":{\"_id\":\"bad\"}}\nnot-json\n\
                     {\"index\":{\"_id\":\"large\"}}\n{\"body\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"}\n\
                     {\"delete\":{\"_id\":\"last\"}}\n";
        let mut decoder = BulkDecoder::new(BufReader::with_capacity(3, Cursor::new(input)));
        decoder.line_limit = 40;
        let bad = decoder.next_document().unwrap().unwrap();
        assert!(bad.source_error.unwrap().contains("source line [2]"));
        let large = decoder.next_document().unwrap().unwrap();
        assert_eq!(large.doc_id, "large");
        assert!(
            large
                .source_error
                .unwrap()
                .contains("source line [4] exceeds 40")
        );
        assert_eq!(decoder.next_document().unwrap().unwrap().doc_id, "last");
        assert!(decoder.next_document().unwrap().is_none());
    }

    #[test]
    fn incremental_decoder_reports_structural_and_truncated_input_precisely() {
        for (tail, expected) in [
            ("{\"bogus\":{}}\n", "unknown action"),
            ("{\"index\":{},\"delete\":{}}\n", "exactly one action"),
            ("{\"index\":7}\n", "must contain an object"),
            ("{\"update\":{\"_id\":\"x\"}}", "requires a source line"),
        ] {
            let input = format!("{{\"delete\":{{\"_id\":\"ok\"}}}}\n\n{tail}");
            let mut decoder = BulkDecoder::new(Cursor::new(input));
            assert_eq!(decoder.next_document().unwrap().unwrap().doc_id, "ok");
            let error = decoder.next_document().unwrap_err().to_string();
            assert!(error.contains("line [3]"), "{error}");
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn incremental_decoder_rejects_oversized_actions_and_invalid_utf8() {
        let mut decoder = BulkDecoder::new(Cursor::new(b"{\"delete\":{\"_id\":\"large\"}}\n"));
        decoder.line_limit = 8;
        assert!(matches!(decoder.next_document(), Err(BulkError::Limit(_))));
        let mut decoder = BulkDecoder::new(Cursor::new(b"{\"index\":{}}\n\xff\n"));
        assert!(
            decoder
                .next_document()
                .unwrap_err()
                .to_string()
                .contains("Invalid UTF-8 bulk body at line 2")
        );
    }

    #[test]
    fn incremental_decoder_rejects_unsupported_metadata_at_original_item_position() {
        let mut decoder = BulkDecoder::new(Cursor::new(
            "{\"delete\":{\"_id\":\"ok\"}}\n{\"index\":{\"routing\":\"no\"}}\n{}\n",
        ));
        decoder.next_document().unwrap();
        let error = decoder.next_document().unwrap_err().to_string();
        assert!(
            error.contains("line [2]") && error.contains("item [2]"),
            "{error}"
        );
        assert!(error.contains("routing"), "{error}");
    }
}
