// Utilities for handling "system load": sets of log entries with a shared host and timestamp

use anyhow::Result;
use std::cell::RefCell;
use std::collections::HashMap;
use crate::{LogEntry, Timestamp, parse_logfile};

/// Return a map (represented as a vector of pairs) from hostname to a map (again a vector of pairs)
/// from timestamp to a vector of LogEntry records with that timestamp on that host.  The vectors of
/// pairs and timestamps are sorted ascending.  All timestamps in the innermost vector-of-records
/// are the same, but the timestamp is included explicitly anyway.
///
/// The Vec<LogEntry> and Vec<(DateTime, Vec<LogEntry>)> are never empty, but it's possible for the
/// outermost vector to be empty.
///
/// If there's an error from the parser then it is propagated, though not necessarily precisely.

pub fn compute_load<F>(logfiles: &[String], filter: F) -> Result<Vec<(String, Vec<(Timestamp, Vec<LogEntry>)>)>>
where
    // (user, host, jobid, timestamp)
    F: Fn(&str, &str, u32, &Timestamp) -> bool,
{
    // In principle the sonar log is already broken down by hostname so the hashmap and bucketing
    // should not be necessary, but there is utility in being able to catenate log files without any
    // concern about that.  Each record contains timestamp and host name, and is self-contained.
    // The file path is not relevant, even if informative.

    let err = RefCell::<Option<anyhow::Error>>::new(None);
    let mut loadlog = HashMap::<String, Vec<LogEntry>>::new();
    logfiles.iter().for_each(|file| {
        match parse_logfile(file, &filter) {
            Ok(mut log_entries) => {
                for entry in log_entries.drain(0..) {
                    if let Some(loadspec) = loadlog.get_mut(&entry.hostname) {
                        loadspec.push(entry);
                    } else {
                        loadlog.insert(entry.hostname.clone(), vec![entry]);
                    }
                }
            }
            Err(e) => {
                *err.borrow_mut() = Some(e);
            }
        }
    });
    if err.borrow().is_some() {
        return Err(err.into_inner().unwrap());
    }

    let mut by_host = vec![];
    for (host, mut records) in loadlog.drain() {
        records.sort_by_key(|j| j.timestamp);
        let mut by_time = vec![];
        loop {
            if records.len() == 0 {
                break
            }
            let first = records.pop().unwrap();
            let t = first.timestamp;
            let mut bucket = vec![first];
            while records.len() > 0 && records.last().unwrap().timestamp == t {
                bucket.push(records.pop().unwrap());
            }
            by_time.push((t, bucket));
        }

        // TODO: The clone here is highly undesirable
        by_time.sort_by_key(|(timestamp, _)| timestamp.clone());
        if by_time.len() > 0 {
            by_host.push((host, by_time));
        }
    }

    // TODO: The clone here is highly undesirable
    by_host.sort_by_key(|(hostname, _)| hostname.clone());

    Ok(by_host)
}

