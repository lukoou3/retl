use chrono::NaiveDate;

pub const DEFAULT_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

pub const fn num_days_to_date(days: i32) -> NaiveDate {
    match NaiveDate::from_num_days_from_ce_opt(days) {
        Some(date) => date,
        None => DEFAULT_DATE,
    }
}

