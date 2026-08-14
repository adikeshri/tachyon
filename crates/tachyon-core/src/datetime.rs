//! Minimal RFC 3339 <-> epoch-milliseconds conversion.
//!
//! `date` fields are stored as `i64` epoch milliseconds so they share the
//! numeric index and sort machinery with `int`. We accept either an integer or
//! an RFC 3339 string on ingest, and always hand back what the user originally
//! sent (the source document is stored verbatim), so this module only needs to
//! be correct, not lossless-round-tripping.
//!
//! Hand-rolled rather than pulling in a date library: the accepted grammar is
//! small and fixed, and this keeps the dependency tree (and the static binary)
//! lean.

/// Days from 1970-01-01 to the given proleptic-Gregorian civil date.
///
/// Howard Hinnant's `days_from_civil`, valid for any year in `i64` range.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m as i64 + 9) % 12; // Mar = 0
    let doy = (153 * mp + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => 0,
    }
}

fn digits(bytes: &[u8], at: usize, n: usize) -> Option<i64> {
    let slice = bytes.get(at..at + n)?;
    let mut acc: i64 = 0;
    for b in slice {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc * 10 + (b - b'0') as i64;
    }
    Some(acc)
}

/// Parse an RFC 3339 timestamp into epoch milliseconds.
///
/// Accepts `YYYY-MM-DDTHH:MM:SS[.fff…][Z|±HH:MM]`. The date-time separator may
/// be `T`, `t`, or a space. Fractional seconds are truncated to milliseconds.
/// A missing offset is treated as UTC.
pub fn parse_rfc3339(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    if !matches!(b[10], b'T' | b't' | b' ') {
        return None;
    }
    if b[13] != b':' || b[16] != b':' {
        return None;
    }

    let year = digits(b, 0, 4)?;
    let month = digits(b, 5, 2)? as u32;
    let day = digits(b, 8, 2)? as u32;
    let hour = digits(b, 11, 2)?;
    let min = digits(b, 14, 2)?;
    let sec = digits(b, 17, 2)?;

    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    // Leap seconds are accepted and clamped, matching RFC 3339 §5.7 leniency.
    if hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    let sec = sec.min(59);

    let mut idx = 19;
    let mut millis_frac: i64 = 0;
    if b.get(idx) == Some(&b'.') || b.get(idx) == Some(&b',') {
        idx += 1;
        let start = idx;
        while idx < b.len() && b[idx].is_ascii_digit() {
            idx += 1;
        }
        if idx == start {
            return None;
        }
        // Take at most 3 digits, right-padding shorter fractions.
        let frac = &b[start..idx.min(start + 3)];
        let mut acc = 0i64;
        for &d in frac {
            acc = acc * 10 + (d - b'0') as i64;
        }
        millis_frac = acc * 10i64.pow(3 - frac.len() as u32);
    }

    let offset_secs: i64 = match b.get(idx) {
        None => 0,
        Some(b'Z') | Some(b'z') if idx + 1 == b.len() => 0,
        Some(sign @ (b'+' | b'-')) => {
            let sign = if *sign == b'-' { -1 } else { 1 };
            if b.len() != idx + 6 || b[idx + 3] != b':' {
                return None;
            }
            let oh = digits(b, idx + 1, 2)?;
            let om = digits(b, idx + 4, 2)?;
            if oh > 23 || om > 59 {
                return None;
            }
            sign * (oh * 3600 + om * 60)
        }
        _ => return None,
    };

    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + hour * 3600 + min * 60 + sec - offset_secs;
    Some(secs * 1000 + millis_frac)
}

/// Current wall-clock time in epoch milliseconds.
pub fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        // Clock before the epoch: only reachable on a badly misconfigured host.
        Err(e) => -(e.duration().as_millis() as i64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch() {
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Some(0));
    }

    #[test]
    fn known_timestamps() {
        assert_eq!(parse_rfc3339("2026-08-13T12:34:56Z"), Some(1_786_624_496_000));
        assert_eq!(parse_rfc3339("2000-01-01T00:00:00Z"), Some(946_684_800_000));
        assert_eq!(parse_rfc3339("1969-12-31T23:59:59Z"), Some(-1000));
    }

    #[test]
    fn fractional_seconds_truncate_to_millis() {
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00.5Z"), Some(500));
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00.25Z"), Some(250));
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00.123456Z"), Some(123));
    }

    #[test]
    fn offsets_are_applied() {
        assert_eq!(parse_rfc3339("1970-01-01T05:30:00+05:30"), Some(0));
        assert_eq!(parse_rfc3339("1969-12-31T19:00:00-05:00"), Some(0));
    }

    #[test]
    fn separators_and_missing_offset() {
        assert_eq!(parse_rfc3339("1970-01-01 00:00:00"), Some(0));
        assert_eq!(parse_rfc3339("1970-01-01t00:00:00z"), Some(0));
    }

    #[test]
    fn leap_days() {
        assert_eq!(parse_rfc3339("2024-02-29T00:00:00Z"), Some(1_709_164_800_000));
        assert_eq!(parse_rfc3339("2023-02-29T00:00:00Z"), None);
        assert_eq!(parse_rfc3339("1900-02-29T00:00:00Z"), None);
        assert!(parse_rfc3339("2000-02-29T00:00:00Z").is_some());
    }

    #[test]
    fn rejects_malformed() {
        for bad in [
            "",
            "2026-08-13",
            "2026-13-01T00:00:00Z",
            "2026-08-00T00:00:00Z",
            "2026-08-13T24:00:00Z",
            "2026-08-13T00:60:00Z",
            "2026-08-13T00:00:00+5:00",
            "2026-08-13T00:00:00ZZ",
            "2026-08-13T00:00:00.Z",
            "20x6-08-13T00:00:00Z",
        ] {
            assert_eq!(parse_rfc3339(bad), None, "should reject {bad:?}");
        }
    }
}
