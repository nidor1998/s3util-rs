//! UTC date-time helpers built on the `time` crate.
//!
//! The string formats produced here intentionally match what `chrono`
//! produced before the migration (`+00:00` offset, 0/3/6/9 fractional digits),
//! so JSON output and the `s3sync_origin_last_modified` metadata value stay
//! unchanged.

use std::fmt::Write;

use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

/// Parses an RFC 3339 date-time (any offset) and normalizes it to UTC.
///
/// The date/time separator must be `T`, `t`, or a space (as RFC 3339 allows);
/// `time` itself accepts any byte there.
///
/// A leap second (`:60`) is rejected. `time` would otherwise accept one at the
/// end of a UTC month and silently read it as `23:59:59.999999999`.
///
/// Returns an error (rather than panicking) if the value is out of range after
/// conversion to UTC.
pub fn parse_rfc3339_utc(value: &str) -> Result<OffsetDateTime, String> {
    if !has_rfc3339_separator(value) {
        return Err(format!(
            "invalid RFC 3339 date-time (expected 'T' between date and time): {value}"
        ));
    }
    // RFC 3339 spells the seconds at a fixed position: `YYYY-MM-DDTHH:MM:SS`.
    if value.get(16..20).is_some_and(|s| s.starts_with(":60")) {
        return Err(format!(
            "invalid RFC 3339 date-time (leap seconds (\":60\") are not supported): {value}"
        ));
    }
    let dt = OffsetDateTime::parse(value, &Rfc3339).map_err(|e| e.to_string())?;
    dt.checked_to_offset(UtcOffset::UTC)
        .ok_or_else(|| format!("date-time out of range after conversion to UTC: {value}"))
}

/// Whether byte 10 of `value` (between `YYYY-MM-DD` and the time) is one of
/// the separators RFC 3339 allows: `T`, `t`, or a space.
pub(crate) fn has_rfc3339_separator(value: &str) -> bool {
    matches!(value.as_bytes().get(10), Some(b'T' | b't' | b' '))
}

/// Parses an RFC 3339 date-time for the `Expires` header and normalizes it to UTC.
///
/// The value is sent as an HTTP date, which only supports years `0001..=9999`,
/// so anything outside that range (after conversion to UTC) is rejected here
/// instead of failing the upload later.
pub fn parse_expires(value: &str) -> Result<OffsetDateTime, String> {
    let dt = parse_rfc3339_utc(value)?;
    if !(1..=9999).contains(&dt.year()) {
        return Err(format!(
            "expires must be between 0001-01-01T00:00:00Z and 9999-12-31T23:59:59Z: {value}"
        ));
    }
    Ok(dt)
}

/// Converts an AWS [`DateTime`] to an [`OffsetDateTime`] truncated to
/// millisecond precision.
pub fn to_time_millis(dt: &DateTime) -> anyhow::Result<OffsetDateTime> {
    Ok(DateTime::from_millis(dt.to_millis()?).to_time()?)
}

/// Formats seconds since the Unix epoch as RFC 3339 in UTC with whole seconds,
/// e.g. `2023-01-20T17:56:37+00:00`.
///
/// Same output as [`Rfc3339Ext::to_rfc3339_secs`], but without
/// [`OffsetDateTime`]'s year range limit (`-9999..=9999`): a year past `9999`
/// is written with an explicit sign (`+10000-01-01T00:00:00+00:00`), as
/// `chrono` did. Used where the timestamp comes from the local file system,
/// which can hold such values.
pub fn unix_secs_to_rfc3339_secs(secs: i64) -> String {
    let mut s = unix_secs_date_time(secs);
    s.push_str("+00:00");
    s
}

/// Formats an AWS [`DateTime`] as RFC 3339 in UTC, like
/// [`Rfc3339Ext::to_rfc3339`] (0, 3, 6, or 9 fractional digits).
///
/// Never fails: a value outside [`OffsetDateTime`]'s year range
/// (`-9999..=9999`) is formatted from its Unix seconds and nanoseconds
/// instead, with a signed year (`+10000-01-01T00:00:00+00:00`), as `chrono`
/// did.
pub fn aws_datetime_to_rfc3339(dt: &DateTime) -> String {
    match dt.to_time() {
        Ok(t) => t.to_rfc3339(),
        Err(_) => unix_to_rfc3339(dt.secs(), dt.subsec_nanos()),
    }
}

/// Like [`aws_datetime_to_rfc3339`], but truncated to millisecond precision
/// first (same output as `to_time_millis(dt)?.to_rfc3339()` when that
/// succeeds).
pub fn aws_datetime_to_rfc3339_millis(dt: &DateTime) -> String {
    match to_time_millis(dt) {
        Ok(t) => t.to_rfc3339(),
        Err(_) => {
            let nanos = dt.subsec_nanos();
            unix_to_rfc3339(dt.secs(), nanos - nanos % 1_000_000)
        }
    }
}

/// Formats Unix seconds plus nanoseconds as RFC 3339 in UTC, without
/// [`OffsetDateTime`]'s year range limit.
fn unix_to_rfc3339(secs: i64, nanos: u32) -> String {
    let mut s = unix_secs_date_time(secs);
    push_fraction(&mut s, nanos);
    s.push_str("+00:00");
    s
}

/// `YYYY-MM-DDTHH:MM:SS` (signed year outside `0..=9999`) for Unix seconds,
/// without an offset.
fn unix_secs_date_time(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);

    // Howard Hinnant's `civil_from_days`, in i128 so no input can overflow.
    let z = i128::from(days) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i128::from(month <= 2);

    let mut s = String::with_capacity(35);
    if (0..=9999).contains(&year) {
        let _ = write!(s, "{year:04}");
    } else {
        let _ = write!(s, "{year:+05}");
    }
    let _ = write!(
        s,
        "-{month:02}-{day:02}T{:02}:{:02}:{:02}",
        secs_of_day / 3_600,
        secs_of_day / 60 % 60,
        secs_of_day % 60
    );
    s
}

/// Appends `.fff`, `.ffffff`, or `.fffffffff` (nothing when `nanos` is 0).
fn push_fraction(s: &mut String, nanos: u32) {
    if nanos == 0 {
        return;
    }
    if nanos.is_multiple_of(1_000_000) {
        let _ = write!(s, ".{:03}", nanos / 1_000_000);
    } else if nanos.is_multiple_of(1_000) {
        let _ = write!(s, ".{:06}", nanos / 1_000);
    } else {
        let _ = write!(s, ".{nanos:09}");
    }
}

/// RFC 3339 formatting helpers for [`OffsetDateTime`].
///
/// Negative years are written with an explicit sign and at least four digits
/// (e.g. `-0001`), matching `chrono`. Such output is an extension of RFC 3339,
/// which only allows four-digit non-negative years.
///
/// Values are converted to UTC. If that conversion would go out of range, the
/// value is formatted in its own offset instead of panicking.
pub trait Rfc3339Ext {
    /// Formats as RFC 3339 in UTC with `+00:00` and 0, 3, 6, or 9 fractional digits.
    /// e.g. `2023-01-20T17:56:37.552+00:00`
    fn to_rfc3339(&self) -> String;

    /// Formats as RFC 3339 in UTC with whole seconds only.
    /// e.g. `2023-01-20T17:56:37+00:00`
    fn to_rfc3339_secs(&self) -> String;
}

impl Rfc3339Ext for OffsetDateTime {
    fn to_rfc3339(&self) -> String {
        let dt = utc_or_self(self);
        let mut s = format_date_time_secs(&dt);

        push_fraction(&mut s, dt.nanosecond());

        push_offset(&mut s, dt.offset());
        s
    }

    fn to_rfc3339_secs(&self) -> String {
        let dt = utc_or_self(self);
        let mut s = format_date_time_secs(&dt);
        push_offset(&mut s, dt.offset());
        s
    }
}

fn utc_or_self(dt: &OffsetDateTime) -> OffsetDateTime {
    dt.checked_to_offset(UtcOffset::UTC).unwrap_or(*dt)
}

fn push_offset(s: &mut String, offset: UtcOffset) {
    let (hours, minutes, _) = offset.as_hms();
    let sign = if offset.is_negative() { '-' } else { '+' };
    let _ = write!(
        s,
        "{sign}{:02}:{:02}",
        hours.unsigned_abs(),
        minutes.unsigned_abs()
    );
}

fn format_date_time_secs(dt: &OffsetDateTime) -> String {
    let year = dt.year();
    let mut s = String::with_capacity(35);
    if (0..=9999).contains(&year) {
        let _ = write!(s, "{year:04}");
    } else {
        let _ = write!(s, "{year:+05}");
    }
    let _ = write!(
        s,
        "-{:02}-{:02}T{:02}:{:02}:{:02}",
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    );
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_utc() {
        let dt = parse_rfc3339_utc("2023-01-20T17:56:37.552Z").unwrap();
        assert_eq!(dt.offset(), UtcOffset::UTC);
        assert_eq!(dt.to_rfc3339(), "2023-01-20T17:56:37.552+00:00");
    }

    #[test]
    fn parse_with_offset_normalizes_to_utc() {
        let dt = parse_rfc3339_utc("2023-04-20T21:40:11.111+09:00").unwrap();
        assert_eq!(dt.offset(), UtcOffset::UTC);
        assert_eq!(dt.to_rfc3339(), "2023-04-20T12:40:11.111+00:00");
    }

    #[test]
    fn parse_space_separator() {
        let dt = parse_rfc3339_utc("2023-02-19 12:00:00Z").unwrap();
        assert_eq!(dt.to_rfc3339(), "2023-02-19T12:00:00+00:00");
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_rfc3339_utc("2023-02-19").is_err());
        assert!(parse_rfc3339_utc("not a date").is_err());
        assert!(parse_rfc3339_utc("2023-02-19T12:00:00").is_err());
    }

    #[test]
    fn parse_separator() {
        assert!(parse_rfc3339_utc("2023-02-19t12:00:00Z").is_ok());
        assert!(parse_rfc3339_utc("2023-02-19X12:00:00Z").is_err());
        assert!(parse_rfc3339_utc("2023-02-19_12:00:00Z").is_err());
        assert!(parse_rfc3339_utc("").is_err());
    }

    #[test]
    fn parse_leap_second() {
        // `time` would accept a month-end leap second as 23:59:59.999999999;
        // it must be rejected like any other `:60`, for `--expires` too.
        for v in [
            "2016-12-31T23:59:60Z",
            "2016-12-31T23:59:60.5+09:00",
            "2016-12-31T12:00:60Z",
        ] {
            let err = parse_rfc3339_utc(v).unwrap_err();
            assert!(err.contains("leap seconds"), "{v}: {err}");
            let err = parse_expires(v).unwrap_err();
            assert!(err.contains("leap seconds"), "{v}: {err}");
        }
        // A genuine .999999999 still parses, and minute 60 is not a leap second.
        assert_eq!(
            parse_rfc3339_utc("2016-12-31T23:59:59.999999999Z")
                .unwrap()
                .to_rfc3339(),
            "2016-12-31T23:59:59.999999999+00:00"
        );
        let err = parse_rfc3339_utc("2016-12-31T12:60:00Z").unwrap_err();
        assert!(!err.contains("leap seconds"), "{err}");
    }

    #[test]
    fn unix_secs_to_rfc3339_secs_matches_to_rfc3339_secs() {
        for secs in [
            0,
            -1,
            86_399,
            86_400,
            951_782_400,      // 2000-02-29
            1_674_237_397,    // 2023-01-20T17:56:37Z
            -62_135_596_800,  // 0001-01-01
            -62_167_219_200,  // 0000-01-01
            -62_167_219_201,  // -0001-12-31T23:59:59
            253_402_300_799,  // 9999-12-31T23:59:59
            -377_705_116_800, // -9999-01-01
        ] {
            assert_eq!(
                unix_secs_to_rfc3339_secs(secs),
                OffsetDateTime::from_unix_timestamp(secs)
                    .unwrap()
                    .to_rfc3339_secs(),
                "{secs}"
            );
        }
    }

    #[test]
    fn unix_secs_to_rfc3339_secs_beyond_time_range() {
        assert_eq!(
            unix_secs_to_rfc3339_secs(253_402_300_800),
            "+10000-01-01T00:00:00+00:00"
        );
        assert_eq!(
            unix_secs_to_rfc3339_secs(-377_705_116_801),
            "-10000-12-31T23:59:59+00:00"
        );
        // Must not panic or overflow at the extremes.
        unix_secs_to_rfc3339_secs(i64::MAX);
        unix_secs_to_rfc3339_secs(i64::MIN);
    }

    #[test]
    fn aws_datetime_to_rfc3339_in_range_matches_time() {
        for (secs, nanos) in [
            (0, 0),
            (1_674_237_397, 552_000_000),
            (1_674_237_397, 552_123_000),
            (1_674_237_397, 552_123_456),
            (-62_167_219_201, 1),
            (253_402_300_799, 999_999_999),
        ] {
            let dt = DateTime::from_secs_and_nanos(secs, nanos);
            assert_eq!(
                aws_datetime_to_rfc3339(&dt),
                dt.to_time().unwrap().to_rfc3339(),
                "{secs}.{nanos}"
            );
            assert_eq!(
                aws_datetime_to_rfc3339_millis(&dt),
                to_time_millis(&dt).unwrap().to_rfc3339(),
                "{secs}.{nanos}"
            );
            // The fallback path agrees with `time` wherever both apply.
            assert_eq!(
                unix_to_rfc3339(secs, nanos),
                dt.to_time().unwrap().to_rfc3339(),
                "{secs}.{nanos}"
            );
        }
    }

    #[test]
    fn aws_datetime_to_rfc3339_beyond_time_range() {
        let dt = DateTime::from_secs_and_nanos(253_402_300_800, 552_123_456);
        assert_eq!(
            aws_datetime_to_rfc3339(&dt),
            "+10000-01-01T00:00:00.552123456+00:00"
        );
        assert_eq!(
            aws_datetime_to_rfc3339_millis(&dt),
            "+10000-01-01T00:00:00.552+00:00"
        );
        let dt = DateTime::from_secs(-377_705_116_801);
        assert_eq!(aws_datetime_to_rfc3339(&dt), "-10000-12-31T23:59:59+00:00");
        // Must not panic at the extremes (`to_millis` overflows here).
        aws_datetime_to_rfc3339(&DateTime::from_secs(i64::MAX));
        aws_datetime_to_rfc3339_millis(&DateTime::from_secs(i64::MAX));
        aws_datetime_to_rfc3339_millis(&DateTime::from_secs(i64::MIN));
    }

    #[test]
    fn parse_max_date_with_negative_offset_does_not_panic() {
        // Out of range once converted to UTC (year 10000).
        assert!(parse_rfc3339_utc("9999-12-31T23:59:59-01:00").is_err());
    }

    #[test]
    fn parse_expires_range() {
        assert_eq!(
            parse_expires("9999-12-31T23:59:59Z").unwrap().to_rfc3339(),
            "9999-12-31T23:59:59+00:00"
        );
        assert_eq!(
            parse_expires("0001-01-01T00:00:00Z").unwrap().to_rfc3339(),
            "0001-01-01T00:00:00+00:00"
        );
        assert!(parse_expires("9999-12-31T23:59:59-01:00").is_err());
        assert!(parse_expires("0000-12-31T23:59:59Z").is_err());
        assert!(parse_expires("0001-01-01T00:00:00+01:00").is_err());
        assert!(parse_expires("not a date").is_err());
    }

    #[test]
    fn expires_range_bounds_convert_to_http_date() {
        // Every value `parse_expires` accepts must be sendable as the
        // `Expires` HTTP date, so the upload path never fails on it.
        use aws_smithy_types::date_time::Format;
        for v in ["0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999Z"] {
            let dt = DateTime::from_time(parse_expires(v).unwrap());
            assert!(dt.fmt(Format::HttpDate).is_ok(), "{v}");
        }
    }

    #[test]
    fn to_rfc3339_fraction_digits() {
        let base = OffsetDateTime::from_unix_timestamp(0).unwrap();
        assert_eq!(base.to_rfc3339(), "1970-01-01T00:00:00+00:00");
        assert_eq!(
            base.replace_millisecond(5).unwrap().to_rfc3339(),
            "1970-01-01T00:00:00.005+00:00"
        );
        assert_eq!(
            base.replace_microsecond(5).unwrap().to_rfc3339(),
            "1970-01-01T00:00:00.000005+00:00"
        );
        assert_eq!(
            base.replace_nanosecond(5).unwrap().to_rfc3339(),
            "1970-01-01T00:00:00.000000005+00:00"
        );
    }

    #[test]
    fn to_rfc3339_secs_truncates() {
        let dt = OffsetDateTime::from_unix_timestamp(0)
            .unwrap()
            .replace_millisecond(999)
            .unwrap();
        assert_eq!(dt.to_rfc3339_secs(), "1970-01-01T00:00:00+00:00");
    }

    #[test]
    fn negative_year() {
        let dt = parse_rfc3339_utc("0000-01-01T00:00:00Z").unwrap() - time::Duration::days(1);
        assert_eq!(dt.to_rfc3339(), "-0001-12-31T00:00:00+00:00");
    }

    #[test]
    fn to_rfc3339_out_of_range_utc_does_not_panic() {
        let max =
            time::PrimitiveDateTime::MAX.assume_offset(UtcOffset::from_hms(-1, -30, 0).unwrap());
        assert!(max.checked_to_offset(UtcOffset::UTC).is_none());
        assert_eq!(max.to_rfc3339(), "9999-12-31T23:59:59.999999999-01:30");
        assert_eq!(max.to_rfc3339_secs(), "9999-12-31T23:59:59-01:30");
    }

    #[test]
    fn to_time_millis_truncates_sub_millis() {
        let dt = DateTime::from_secs_and_nanos(1_674_237_397, 552_123_456);
        let t = to_time_millis(&dt).unwrap();
        assert_eq!(t.nanosecond(), 552_000_000);
        assert_eq!(t.to_rfc3339(), "2023-01-20T17:56:37.552+00:00");
    }

    #[test]
    fn to_time_millis_out_of_range_errors() {
        // 10000-01-01T00:00:00Z is beyond `OffsetDateTime`'s range.
        assert!(to_time_millis(&DateTime::from_secs(253_402_300_800)).is_err());
    }

    #[test]
    fn http_date_rejects_year_10000() {
        // Guards against enabling `time`'s `large-dates` feature: Cargo feature
        // unification would turn it on inside aws-smithy-types too, and its
        // HTTP-date formatter (used for `Expires`) relies on years >= 10000
        // being rejected.
        use aws_smithy_types::date_time::Format;
        assert!(
            DateTime::from_secs(253_402_300_800)
                .fmt(Format::HttpDate)
                .is_err()
        );
    }
}
