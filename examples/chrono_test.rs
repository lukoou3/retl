use chrono::{DateTime, Datelike, FixedOffset, Local, NaiveDate, NaiveDateTime, Utc};

fn test_datetime() {
    let utc_datetime: DateTime<Utc> = Utc::now();
    let local_datetime: DateTime<Local> = Local::now();
    println!("UTC datetime: {}, Local datetime: {}", utc_datetime, local_datetime);
    println!("UTC datetime: {:?}, Local datetime: {:?}", utc_datetime, local_datetime);
    println!("UTC datetime: {}, Local datetime: {}", utc_datetime.to_rfc3339(), local_datetime.to_rfc3339());
    println!("UTC datetime: {}, Local datetime: {}", utc_datetime.to_string(), local_datetime.to_string());
    println!("UTC datetime: {}, Local datetime: {}", utc_datetime.format("%Y-%m-%d %H:%M:%S"), local_datetime.format("%Y-%m-%d %H:%M:%S"));
    println!("UTC datetime: {:?}, Local datetime: {:?}", utc_datetime.format("%Y-%m-%d %H:%M:%S"), local_datetime.format("%Y-%m-%d %H:%M:%S"));
}

fn test_fixed_offset() {
    let utc_date_time: DateTime<Utc> = Utc::now();
    let fixed_offset = FixedOffset::east_opt(8 * 3600).unwrap(); // 转为 utc+8 东八区
    let local_date_time = utc_date_time.with_timezone(&fixed_offset);
    println!("UTC datetime: {}, time in UTC+8: {}", utc_date_time, local_date_time)
}

fn test_unix_timestamp() {
    let utc_date_time: DateTime<Utc> = Utc::now();
    let unix_timestamp = utc_date_time.timestamp();
    println!("UTC datetime: {}, unix timestamp: {}", utc_date_time, unix_timestamp);
    let timestamp_millis = utc_date_time.timestamp_millis();
    println!("UTC datetime: {}, unix timestamp millis: {}", utc_date_time, timestamp_millis);
}

fn test_from_unix_timestamp() {
    let utc_date_time: DateTime<Utc> = Utc::now();
    let unix_timestamp = utc_date_time.timestamp();
    let from_unix_timestamp = DateTime::<Utc>::from_timestamp(unix_timestamp, 0).unwrap();
    let timestamp_millis = utc_date_time.timestamp_millis();
    let from_unix_timestamp_millis = DateTime::<Utc>::from_timestamp_millis(timestamp_millis).unwrap();
    println!("UTC datetime: {}, unix timestamp: {}, from unix timestamp: {}", utc_date_time, unix_timestamp, from_unix_timestamp);
    println!("UTC datetime: {}, unix timestamp millis: {}, from unix timestamp millis: {}", utc_date_time, timestamp_millis, from_unix_timestamp_millis);
}

fn test_date_time_format() {
    let utc_datetime: DateTime<Utc> = Utc::now();
    let local_datetime: DateTime<Local> = Local::now();
    println!("UTC datetime: {}, Local datetime: {}", utc_datetime.format("%Y-%m-%d %H:%M:%S"), local_datetime.format("%Y-%m-%d %H:%M:%S"));
    let naive_date_time: NaiveDateTime = NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
    let date_time = DateTime::parse_from_str("2023-01-01 00:00:00 +08:00", "%Y-%m-%d %H:%M:%S %z").unwrap();
    println!("Naive date time: {}, date time: {}", naive_date_time, date_time);
}

fn test_date_time_parse() {
    let formats = vec![
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.3f",
        "%Y-%m-%d %H:%M:%S%.6f",
    ];

    let texts = vec![
        "1970-01-01 01:00:00",
        "1970-01-01 01:00:00.1",
        "1970-01-01 01:00:00.001",
        "1970-01-01 01:00:00.0012",
        "1970-01-01 01:00:00.000001",
        "1970-01-01 01:00:00.000001222",
    ];

    for text in texts {
        println!("尝试解析: {}", text);
        for format in formats.clone() {
            if let Ok(dt) = NaiveDateTime::parse_from_str(text, format) {
                println!("成功解析: {} -> {} -> {}", format, dt, dt.format(format));
            }
        }
    }
}

fn test_naive_date_time() {
    let utc_datetime: DateTime<Utc> = Utc::now();
    let local_datetime: DateTime<Local> = Local::now();
    let utc_naive_date_time: NaiveDateTime = utc_datetime.naive_utc();
    let local_naive_date_time: NaiveDateTime = local_datetime.naive_utc();
    println!("UTC datetime: {}, Local datetime: {}", utc_datetime, local_datetime);
    println!("UTC naive datetime: {}, Local naive datetime: {}", utc_naive_date_time, local_naive_date_time);
}

fn test_naive_date() {
    let naive_date_0 = NaiveDate::from_num_days_from_ce_opt(0).unwrap();
    let naive_date_1 = NaiveDate::from_num_days_from_ce_opt(1).unwrap();
    let naive_date__1 = NaiveDate::from_num_days_from_ce_opt(-1).unwrap();
    println!("Naive date 0: {}, Naive date 1: {}, Naive date -1: {}", naive_date_0, naive_date_1, naive_date__1);
    let days_0 = naive_date_0.num_days_from_ce();
    let days_1 = naive_date_1.num_days_from_ce();
    let days__1 = naive_date__1.num_days_from_ce();
    println!("Naive date 0: {}, Naive date 1: {}, Naive date -1: {}", days_0, days_1, days__1);
}

fn main() {
    //test_unix_timestamp();
    //test_from_unix_timestamp();
    //test_date_time_format();
    test_date_time_parse();
    //test_naive_date_time();
    //test_naive_date();
}