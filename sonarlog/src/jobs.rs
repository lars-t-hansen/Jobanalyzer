// Utilities for handling "jobs": sets of log entries with a shared job ID

use anyhow::Result;
use chrono::prelude::{DateTime,NaiveDate};
use chrono::Utc;
#[cfg(test)]
use chrono::{Datelike,Timelike};
use std::cell::RefCell;
use core::cmp::{min,max};
use std::collections::HashMap;
use crate::{JobAggregate, LogEntry, parse_logfile, LIVE_AT_START, LIVE_AT_END};

/// Given a list of file names of log files, read all the logs and return a hashmap that maps the
/// Job ID to a sorted vector of the job records for the Job ID, along with the count of unfiltered
/// records and the earliest and latest timestamp seen across all logs before filtering.
///
/// This propagates I/O errors, though not necessarily precisely.

pub fn compute_jobs<F>(logfiles: &[String], filter: F) -> Result<(HashMap<u32, Vec<LogEntry>>, usize, DateTime<Utc>, DateTime<Utc>)>
where
    // (user, host, jobid, timestamp)
    F: Fn(&str, &str, u32, &DateTime<Utc>) -> bool,
{

    // Read the files, filter the records, build up a set of candidate log records.
    //
    // `earliest` and `latest` are computed here so that they are computed on the basis of all
    // records seen, not just the records retained after filtering.  Doing so prevents us from
    // misclassifying a job as alive at start or end of log when it is simply alive at start or end
    // of filtered records.

    let record_counter = RefCell::new(0usize);
    let earliest = RefCell::new(now());
    let latest = RefCell::new(epoch());
    let new_filter = |user:&str, host:&str, job: u32, t:&DateTime<Utc>| {
        *record_counter.borrow_mut() += 1;
        let mut e = earliest.borrow_mut();
        *e = min(*e, *t);
        let mut l = latest.borrow_mut();
        *l = max(*l, *t);
        filter(user, host, job, t)
    };

    // TODO: The thing with `err` is a bit of a mess, a standard loop with an early return would
    // likely be easier to understand.

    let err = RefCell::<Option<anyhow::Error>>::new(None);
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
            Err(e) => {
                *err.borrow_mut() = Some(e);
            }
        }
    });
    if err.borrow().is_some() {
        return Err(err.into_inner().unwrap());
    }

    // The `joblog` is a map from job ID to a vector of all job records with that job ID. Sort each
    // vector by ascending timestamp to get an idea of the duration of the job.
    //
    // (I have no idea what `&mut ref mut` means.)

    joblog.iter_mut().for_each(|(_k, &mut ref mut job)| {
        job.sort_by_key(|j| j.timestamp);
    });

    let num_records = *record_counter.borrow();
    let earliest = *earliest.borrow();
    let latest = *latest.borrow();
    Ok((joblog, num_records, earliest, latest))
}

/// Given a list of log entries for a job, sorted ascending by timestamp, and the earliest and
/// latest timestamps from all records read, return a JobAggregate for the job.

pub fn aggregate_job(job: &[LogEntry], earliest: DateTime<Utc>, latest: DateTime<Utc>) -> JobAggregate {
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
    JobAggregate {
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
    // TODO: should do better, but this is currently good enough for all our uses.
    DateTime::from_utc(NaiveDate::from_ymd_opt(2000,1,1).unwrap().and_hms_opt(0,0,0).unwrap(), Utc)
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

#[test]
fn test_compute_jobs1() {
    let filter = |_user:&str, _host:&str, _job: u32, _t:&DateTime<Utc>| {
        true
    };
    assert!(compute_jobs(&vec![
        "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
        "../sonar_test_data0/2023/07/01/ml3.hpc.uio.no.csv".to_string(), // Not found
        "../sonar_test_data0/2023/06/02/ml8.hpc.uio.no.csv".to_string()],
                         &filter).is_err());
}

#[test]
fn test_compute_jobs2() {
    // Filter by time so that we can test computation of earliest and latest
    let filter = |_user:&str, _host:&str, _job: u32, t:&DateTime<Utc>| {
        t.hour() >= 6 && t.hour() <= 18
    };
    let (_jobs, numrec, earliest, latest) = compute_jobs(&vec![
        "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
        "../sonar_test_data0/2023/06/01/ml8.hpc.uio.no.csv".to_string()],
                         &filter).unwrap();

    // total number of records read
    assert!(numrec == 1440+1440);

    // first record of first file:
    // 2023-06-23T05:05:01.224181967+00:00,ml8.hpc.uio.no,192,einarvid,2381069,mongod,1.6,3608300,0,0,0,0
    assert!(earliest.year() == 2023 && earliest.month() == 6 && earliest.day() == 23 &&
            earliest.hour() == 5 && earliest.minute() == 5 && earliest.second() == 1);

    // last record of last file:
    // 2023-06-24T22:05:02.092905606+00:00,ml8.hpc.uio.no,192,zabbix,4093,zabbix_agentd,4.6,2664,0,0,0,0
    assert!(latest.year() == 2023 && latest.month() == 6 && latest.day() == 24 &&
            latest.hour() == 22 && latest.minute() == 5 && latest.second() == 2);
}

#[test]
fn test_compute_jobs3() {
    // job 2447150 crosses files

    // Filter by job ID, we just want the one job
    let filter = |_user:&str, _host:&str, job: u32, _t:&DateTime<Utc>| {
        job == 2447150
    };
    let (jobs, _numrec, earliest, latest) = compute_jobs(&vec![
        "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
        "../sonar_test_data0/2023/06/01/ml8.hpc.uio.no.csv".to_string()],
                         &filter).unwrap();

    assert!(jobs.len() == 1);
    let job = jobs.get(&2447150).unwrap();

    // First record
    // 2023-06-23T12:25:01.486240376+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,173,18813976,1000,0,0,833536
    //
    // Last record
    // 2023-06-24T09:00:01.386294752+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,161,13077760,1000,0,0,833536

    let start = job[0].timestamp;
    let end = job[job.len()-1].timestamp;
    assert!(start.year() == 2023 && start.month() == 6 && start.day() == 23 &&
            start.hour() == 12 && start.minute() == 25 && start.second() == 1);
    assert!(end.year() == 2023 && end.month() == 6 && end.day() == 24 &&
            end.hour() == 9 && end.minute() == 0 && end.second() == 1);

    let agg = aggregate_job(job, earliest, latest);
    assert!(agg.classification == 0);
    assert!(agg.first == start);
    assert!(agg.last == end);
    assert!(agg.duration == (end - start).num_seconds());
    assert!(agg.days == 0);
    assert!(agg.hours == 20);
    assert!(agg.minutes == 34);
    assert!(agg.uses_gpu);
    assert!(agg.selected);
    // TODO: Really more here
}
