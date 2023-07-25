// Utilities for handling "system load": sets of log entries with a shared host and timestamp

use anyhow::Result;
use chrono::prelude::{DateTime,NaiveDate};
use chrono::Utc;
#[cfg(test)]
use chrono::{Datelike,Timelike};
use std::cell::RefCell;
use core::cmp::{min,max};
use std::collections::HashMap;
use crate::{LogEntry, parse_logfile};

/// Return a map (represented as a vector of pairs) from hostname to a map (again a vector of pairs)
/// from timestamp to a vector of LogEntry records with that timestamp on that host.  The vectors of
/// pairs and timestamps are sorted ascending.  All timestamps in the innermost vector-of-records
/// are the same, but the timestamp is included explicitly anyway.
///
/// If there's an error from the parser then it is propagated, though not necessarily precisely.

pub fn compute_load(logfiles: &[String], filter: F) -> Result<Vec<(String, Vec<(DateTime<Utc>, Vec<LogEntry>)>)>>
where
    // (user, host, jobid, timestamp)
    F: Fn(&str, &str, u32, &DateTime<Utc>) -> bool,
{
    // In principle the sonar log is already broken down by hostname so the hashmap and bucketing
    // should not be necessary, but there is utility in being able to catenate log files without any
    // concern about that.  Each record contains timestamp and host name, and is self-contained.
    // The file path is not relevant, even if informative.

    let err = RefCell::<Option<anyhow::Error>>::new(None);
    let mut loadlog = HashMap::<String, Vec<LogEntry>>::new();
    logfiles.iter().for_each(|file| {
        match sonarlog::parse_logfile(file, filter) {
            Ok(mut log_entries) => {
                for entry in log_entries.drain(0..) {
                    if let Some(loadspec) = loadlog.get_mut(&entry.hostname) {
                        loadspec.push(entry);
                    } else {
                        loadlog.insert(entry.hostname, vec![entry]);
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

    // Sort each host's records by ascending time
    loadlog.iter_mut().for_each(|(_k, &mut ref mut loadspec)| {
        loadspec.sort_by_key(|j| j.timestamp);
    });

    // Bucket the records with the same timestamp
    ...;

    // Create the final value
    ...;
}

