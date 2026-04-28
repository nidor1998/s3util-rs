use anyhow::Result;
use aws_sdk_s3::types::Tag;

/// Parse a URL-encoded tagging string (e.g. `"key1=val1&key2=val2"`) into a
/// `Vec<Tag>`. Each `key=value` pair is percent-decoded before being passed to
/// the SDK.
///
/// Shared by `create-bucket` (with `--tagging`), `put-bucket-tagging`, and
/// `put-object-tagging` so the percent-decoding semantics stay consistent
/// across all three commands.
pub fn parse_tagging_to_tags(s: &str) -> Result<Vec<Tag>> {
    if s.is_empty() {
        return Ok(vec![]);
    }
    let mut tags = Vec::new();
    for pair in s.split('&') {
        let mut parts = pair.splitn(2, '=');
        let raw_key = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid tagging pair: {pair}"))?;
        let raw_val = parts.next().unwrap_or("");
        let key = urlencoding::decode(raw_key)
            .map_err(|e| anyhow::anyhow!("invalid percent-encoding in tag key: {e}"))?
            .into_owned();
        let value = urlencoding::decode(raw_val)
            .map_err(|e| anyhow::anyhow!("invalid percent-encoding in tag value: {e}"))?
            .into_owned();
        tags.push(Tag::builder().key(key).value(value).build()?);
    }
    Ok(tags)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(tag: &Tag) -> (&str, &str) {
        (tag.key(), tag.value())
    }

    #[test]
    fn empty_string_returns_empty_vec() {
        let tags = parse_tagging_to_tags("").unwrap();
        assert!(tags.is_empty());
    }

    #[test]
    fn single_pair_parsed() {
        let tags = parse_tagging_to_tags("k=v").unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(pair(&tags[0]), ("k", "v"));
    }

    #[test]
    fn multiple_pairs_parsed() {
        let tags = parse_tagging_to_tags("k1=v1&k2=v2").unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(pair(&tags[0]), ("k1", "v1"));
        assert_eq!(pair(&tags[1]), ("k2", "v2"));
    }

    #[test]
    fn missing_value_yields_empty_string_value() {
        let tags = parse_tagging_to_tags("k").unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(pair(&tags[0]), ("k", ""));
    }

    #[test]
    fn percent_encoded_key_and_value_decoded() {
        let tags = parse_tagging_to_tags("Key%20A=Val%26B").unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(pair(&tags[0]), ("Key A", "Val&B"));
    }

    #[test]
    fn invalid_percent_encoding_in_key_errors() {
        // %FF decodes to byte 0xFF, which is not a valid UTF-8 start byte;
        // the urlencoding crate surfaces this as a FromUtf8Error.
        let err = parse_tagging_to_tags("k%FF=v").unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("invalid percent-encoding in tag key"),
            "actual: {msg}"
        );
    }

    #[test]
    fn invalid_percent_encoding_in_value_errors() {
        let err = parse_tagging_to_tags("k=v%FF").unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("invalid percent-encoding in tag value"),
            "actual: {msg}"
        );
    }
}
