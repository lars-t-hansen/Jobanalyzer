use chrono::NaiveDate;
use chrono::{DateTime, Datelike, Duration, Utc};

/// Returns vector of (year, month, day) with times inclusive between the days of t1 and t2; sub-day
/// information in t1 and t2 is ignored.

pub fn date_range(t1: DateTime<Utc>, t2: DateTime<Utc>) -> Vec<(i32, u32, u32)> {
    // Drop h/m/s
    let d1 = DateTime::<Utc>::from_utc(
        NaiveDate::from_ymd_opt(t1.year(), t1.month(), t1.day())
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Utc,
    );
    let d2 = DateTime::<Utc>::from_utc(
        NaiveDate::from_ymd_opt(t2.year(), t2.month(), t2.day())
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
        Utc,
    );
    let mut date_range = Vec::new();
    let mut current_date = d1;
    while current_date <= d2 {
        date_range.push((
            current_date.year(),
            current_date.month(),
            current_date.day(),
        ));
        current_date += Duration::days(1);
    }

    date_range
}

#[test]
fn test_date_range() {
    let from = NaiveDate::from_ymd_opt(2023, 05, 30)
        .unwrap()
        .and_hms_opt(5, 20, 33)
        .unwrap();
    let to = NaiveDate::from_ymd_opt(2023, 06, 04)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    println!(
        "{:?}",
        date_range(DateTime::from_utc(from, Utc), DateTime::from_utc(to, Utc))
    );
    assert!(
        date_range(DateTime::from_utc(from, Utc), DateTime::from_utc(to, Utc)).eq(&vec![
            (2023, 5, 30),
            (2023, 5, 31),
            (2023, 6, 1),
            (2023, 6, 2),
            (2023, 6, 3),
            (2023, 6, 4)
        ])
    );
}
