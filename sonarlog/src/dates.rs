// Copied from jobgraph and tweaked.

use chrono::{Duration, DateTime, Utc};
#[cfg(test)]
use chrono::NaiveDate;

pub fn date_range(t1: DateTime<Utc>, t2: DateTime<Utc>) -> Vec<String> {
    let d1 = t1.date_naive();
    let d2 = t2.date_naive();

    let mut date_range = Vec::new();

    let mut current_date = d1;
    while current_date <= d2 {
        date_range.push(current_date.format("%Y-%m-%d").to_string());
        current_date += Duration::days(1);
    }

    date_range
}

#[test]
fn test_date_range() {
    let from = NaiveDate::from_ymd_opt(2023, 05, 30).unwrap().and_hms_opt(5, 20, 33).unwrap();
    let to = NaiveDate::from_ymd_opt(2023, 06, 04).unwrap().and_hms_opt(0, 0, 0).unwrap();
    assert!(date_range(DateTime::from_utc(from, Utc), DateTime::from_utc(to, Utc))
            .eq(&vec!["2023-05-30", "2023-05-31", "2023-06-01", "2023-06-02",
                      "2023-06-03", "2023-06-04"]));
}
