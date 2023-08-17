/// Utilities for handling "system load": sets of log entries with a shared host and timestamp
use crate::{postprocess_log, read_logfiles, LogEntry, Timestamp};

use anyhow::Result;
use std::collections::HashMap;

/// Return a map (represented as a vector of pairs) from hostname to a map (again a vector of pairs)
/// from timestamp to a vector of LogEntry records with that timestamp on that host.  The vectors of
/// pairs and timestamps are sorted ascending.  All timestamps in the innermost vector-of-records
/// are the same, but the timestamp is included explicitly anyway.
///
/// The Vec<LogEntry> and Vec<(DateTime, Box<Vec<LogEntry>>)> are never empty, but it's possible for the
/// outermost vector to be empty.
///
/// If there's an error from the parser then it is propagated, though not necessarily precisely.

pub fn compute_load<F>(
    logfiles: &[String],
    filter: F,
) -> Result<Vec<(String, Vec<(Timestamp, Vec<Box<LogEntry>>)>)>>
where
    F: Fn(&LogEntry) -> bool,
{
    let (mut entries, _earliest, _latest, _num_records) = read_logfiles(logfiles)?;
    entries = postprocess_log(entries, filter);

    // TODO: The entries are sorted by hostname and time in `postprocess_log` (and this is part of
    // the contract), so this bucketing is no longer necessary.  This is a vestige of an older
    // design.

    let mut loadlog = HashMap::<String, Vec<Box<LogEntry>>>::new();
    while let Some(entry) = entries.pop() {
        if let Some(loadspec) = loadlog.get_mut(&entry.hostname) {
            loadspec.push(entry);
        } else {
            loadlog.insert(entry.hostname.clone(), vec![entry]);
        }
    }

    let mut by_host = vec![];
    for (host, mut records) in loadlog.drain() {
        records.sort_by_key(|j| j.timestamp);
        let mut by_time = vec![];
        // TODO: This is a "while pop is something" loop
        loop {
            if records.len() == 0 {
                break;
            }
            let first = records.pop().unwrap();
            let t = first.timestamp;
            let mut bucket = vec![first];
            while records.len() > 0 && records.last().unwrap().timestamp == t {
                bucket.push(records.pop().unwrap());
            }
            by_time.push((t, bucket));
        }

        by_time.sort_by(|(timestamp_a, _), (timestamp_b, _)| timestamp_a.cmp(timestamp_b));
        if by_time.len() > 0 {
            by_host.push((host, by_time));
        }
    }

    by_host.sort_by(|(hostname_a, _), (hostname_b, _)| hostname_a.cmp(hostname_b));

    Ok(by_host)
}
