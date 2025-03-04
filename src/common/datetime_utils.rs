use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

pub const DEFAULT_DATETIME_UTC: DateTime<Utc> = NaiveDateTime::new(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(), NaiveTime::from_hms_opt(0, 0, 0).unwrap()).and_utc();
pub const NORM_DATETIME_FMT: &str = "%Y-%m-%d %H:%M:%S";
pub const NORM_DATETIME_MS_FMT: &str = "%Y-%m-%d %H:%M:%S%.3f";

#[inline]
pub const fn from_timestamp_micros_utc(micros: i64) -> DateTime<Utc> {
    match DateTime::from_timestamp_micros(micros) {
        None => DEFAULT_DATETIME_UTC,
        Some(datetime) => datetime,
    }
}


#[inline]
pub fn format_datetime_fafault<Tz: TimeZone>(datetime: DateTime<Tz>) -> String
where
    Tz::Offset: fmt::Display,
{
    datetime.format(NORM_DATETIME_FMT).to_string()
}

#[inline]
pub fn format_datetime_ms_fafault<Tz: TimeZone>(datetime: DateTime<Tz>) -> String
where
    Tz::Offset: fmt::Display,
{
    datetime.format(NORM_DATETIME_MS_FMT).to_string()
}

#[inline]
pub fn current_timestamp_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("system time before Unix epoch").as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_timestamp_micros_utc() {
        let datetime = from_timestamp_micros_utc(0);
        println!("{}, {}, {}", datetime, format_datetime_fafault(datetime), format_datetime_ms_fafault(datetime));
        let datetime = from_timestamp_micros_utc(60 * 60 * 1000_000);
        println!("{}, {}, {}", datetime, format_datetime_fafault(datetime), format_datetime_ms_fafault(datetime));
    }

}