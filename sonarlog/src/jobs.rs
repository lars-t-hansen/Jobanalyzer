// Utilities for handling "jobs": sets of log entries with a shared job ID

use anyhow::Result;
use chrono::prelude::{DateTime,NaiveDate};
use chrono::Utc;
use std::cell::RefCell;
use core::cmp::{min,max};
use std::collections::HashMap;
use crate::{Aggregate, LogEntry, parse_logfile, LIVE_AT_START, LIVE_AT_END};

/// Given a list of file names of log files, read all the logs and return a hashmap that maps the
/// Job ID to a sorted vector of the job records for the Job ID, along with the count of unfiltered
/// records and the earliest and latest timestamp seen across all logs before filtering.

pub fn compute_jobs<F>(logfiles: Vec<String>, filter: F) -> Result<(HashMap<u32, Vec<LogEntry>>, usize, DateTime<Utc>, DateTime<Utc>)>
where
    // (user, host, jobid, timestamp)
    F: Fn(&str, &str, u32, &DateTime<Utc>) -> bool,
{

    // Read the files, filter the records, build up a set of candidate log records.

    let record_counter = RefCell::new(0usize);
    let new_filter = |user:&str, host:&str, job: u32, t:&DateTime<Utc>| {
        *record_counter.borrow_mut() += 1;
        filter(user, host, job, t)
    };

    let mut joblog = HashMap::<u32, Vec<LogEntry>>::new();
    logfiles.iter().for_each(|file| {
        match parse_logfile(file, &new_filter) {
            Ok(mut log_entries) => {
                for entry in log_entries.drain(0..) {
                    if let Some(job) = joblog.get_mut(&entry.job_id) {
                        job.push(entry);
                    } else {
                        joblog.insert(entry.job_id, vec![entry]);
                    }
                }
            }
            Err(_e) => {
                // FIXME: Record this somehow
            }
        }
    });

    // The `joblog` is a map from job ID to a vector of all job records with that job ID. Sort each
    // vector by ascending timestamp to get an idea of the duration of the job.
    //
    // TODO: We currenly only care about the max and min timestamps per job, so optimize later if
    // that doesn't change.
    //
    // (I have no idea what `&mut ref mut` means.)

    joblog.iter_mut().for_each(|(_k, &mut ref mut job)| {
        job.sort_by_key(|j| j.timestamp);
    });

    // Compute the earliest and latest times observed across all the logs
    //
    // FIXME: This is wrong!  It considers only included records, thus leading to incorrect marks being computed.
    // To do better, the log reader must compute these values, or we compute it in the filter function.

    let (earliest, latest) = {
        let max_start = epoch();
        let min_start = now();
        joblog.iter().fold((min_start, max_start),
                           |(earliest, latest), (_k, r)| (min(earliest, r[0].timestamp), max(latest, r[r.len()-1].timestamp)))
    };

    let num_records = *record_counter.borrow();
    Ok((joblog, num_records, earliest, latest))
}

/// Given a list of log entries for a job, sorted ascending by timestamp, and the earliest and
/// latest timestamps from all records read, return an Aggregate for the job.

pub fn aggregate_job(job: &[LogEntry], earliest: DateTime<Utc>, latest: DateTime<Utc>) -> Aggregate {
    let first = job[0].timestamp;
    let last = job[job.len()-1].timestamp;
    let duration = (last - first).num_seconds();
    let minutes = duration / 60;
    let mut classification = 0;
    if first == earliest {
        classification |= LIVE_AT_START;
    }
    if last == latest {
        classification |= LIVE_AT_END;
    }
    Aggregate {
        first,
        last,
        duration: duration,                     // total number of seconds
        minutes: minutes % 60,                  // fractional hours
        hours: (minutes / 60) % 24,             // fractional days
        days: minutes / (60 * 24),              // full days
        uses_gpu: job.iter().any(|jr| jr.gpu_mask != 0),
        avg_cpu: (job.iter().fold(0.0, |acc, jr| acc + jr.cpu_pct) / (job.len() as f64) * 100.0).ceil(),
        peak_cpu: (job.iter().map(|jr| jr.cpu_pct).reduce(f64::max).unwrap() * 100.0).ceil(),
        avg_gpu: (job.iter().fold(0.0, |acc, jr| acc + jr.gpu_pct) / (job.len() as f64) * 100.0).ceil(),
        peak_gpu: (job.iter().map(|jr| jr.gpu_pct).reduce(f64::max).unwrap() * 100.0).ceil(),
        avg_mem_gb: (job.iter().fold(0.0, |acc, jr| acc + jr.mem_gb) /  (job.len() as f64)).ceil(),
        peak_mem_gb: (job.iter().map(|jr| jr.mem_gb).reduce(f64::max).unwrap()).ceil(),
        avg_vmem_pct: (job.iter().fold(0.0, |acc, jr| acc + jr.gpu_mem_pct) /  (job.len() as f64) * 100.0).ceil(),
        peak_vmem_pct: (job.iter().map(|jr| jr.gpu_mem_pct).reduce(f64::max).unwrap() * 100.0).ceil(),
        selected: true,
        classification,
    }
}

fn epoch() -> DateTime<Utc> {
    // FIXME, but this is currently good enough for all our uses
    DateTime::from_utc(NaiveDate::from_ymd_opt(2000,1,1).unwrap().and_hms_opt(0,0,0).unwrap(), Utc)
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

