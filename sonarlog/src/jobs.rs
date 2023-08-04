/// Utilities for handling "jobs": sets of log entries with a shared job ID

use crate::{LogEntry, Timestamp, epoch, now, parse_logfile};

use anyhow::Result;
use std::cell::RefCell;
use core::cmp::{min,max};
use std::collections::HashMap;

#[cfg(test)]
use chrono::{Datelike,Timelike};

/// A datum representing a complex key in the jobs map, using just the job ID or the (job ID, host
/// name).

#[derive(Hash, PartialEq, Eq, Debug)]
pub struct JobKey {
    job_id: u32,
    host: Option<String>,
}

impl JobKey {
    /// Create a JobKey from a LogEntry.  If `by_host` is true then jobs are host-specific and
    /// different hosts give rise to unequal keys, otherwise jobs can cross hosts and equal job IDs
    /// from different hosts give rise to equal keys.

    pub fn from_entry(by_host: bool, entry: &LogEntry) -> JobKey {
        JobKey {
            job_id: entry.job_id,
            host: if by_host { Some(entry.hostname.clone()) } else { None }
        }
    }

    /// Create a JobKey from a host name and a job ID.  If `by_host` is true then jobs are
    /// host-specific and different hosts give rise to unequal keys, otherwise jobs can cross hosts
    /// and equal job IDs from different hosts give rise to equal keys.  In the latter case, `host`
    /// is ignored and can be anything.

    pub fn from_parts(by_host: bool, host: &str, job_id: u32) -> JobKey {
        JobKey {
            job_id,
            host: if by_host { Some(host.to_string()) } else { None }
        }
    }
}

/// Given a list of file names of log files, read all the logs and return a hashmap that maps the
/// JobKey to a sorted vector of the job records for the JobKey, along with the count of unfiltered
/// records and the earliest and latest timestamp seen across all logs before filtering.
///
/// If `merge_across_hosts` is true then we ignore the host names in the records when we create
/// jobs; jobs can span hosts.
///
/// This propagates I/O errors, though not necessarily precisely.

pub fn compute_jobs<F>(
    logfiles: &[String],
    filter: F,
    merge_across_hosts: bool) -> Result<(HashMap<JobKey, Vec<LogEntry>>, usize, Timestamp, Timestamp)>
where
    // (user, host, jobid, timestamp)
    F: Fn(&str, &str, u32, &Timestamp) -> bool,
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
    let new_filter = |user:&str, host:&str, job: u32, t:&Timestamp| {
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
    let mut joblog = HashMap::<JobKey, Vec<LogEntry>>::new();
    logfiles.iter().for_each(|file| {
        match parse_logfile(file, &new_filter) {
            Ok(mut log_entries) => {
                for entry in log_entries.drain(0..) {
                    if let Some(job) = joblog.get_mut(&JobKey::from_entry(!merge_across_hosts, &entry)) {
                        job.push(entry);
                    } else {
                        joblog.insert(JobKey::from_entry(!merge_across_hosts, &entry), vec![entry]);
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

#[test]
fn test_compute_jobs1() {
    let filter = |_user:&str, _host:&str, _job: u32, _t:&Timestamp| {
        true
    };
    assert!(compute_jobs(&vec![
        "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
        "../sonar_test_data0/2023/07/01/ml3.hpc.uio.no.csv".to_string(), // Not found
        "../sonar_test_data0/2023/06/02/ml8.hpc.uio.no.csv".to_string()],
                         &filter, false).is_err());
}

#[test]
fn test_compute_jobs2() {
    // Filter by time so that we can test computation of earliest and latest
    let filter = |_user:&str, _host:&str, _job: u32, t:&Timestamp| {
        t.hour() >= 6 && t.hour() <= 18
    };
    let (_jobs, numrec, earliest, latest) = compute_jobs(&vec![
        "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
        "../sonar_test_data0/2023/06/01/ml8.hpc.uio.no.csv".to_string()],
                         &filter, false).unwrap();

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
    // job 2447150 crosses files (but not hosts)

    // Filter by job ID, we just want the one job
    let filter = |_user:&str, _host:&str, job: u32, _t:&Timestamp| {
        job == 2447150
    };
    let (jobs, _numrec, _earliest, _latest) = compute_jobs(&vec![
        "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
        "../sonar_test_data0/2023/06/01/ml8.hpc.uio.no.csv".to_string()],
                         &filter, /* merge_across_hosts= */ false).unwrap();

    assert!(jobs.len() == 1);
    let job = jobs.get(&JobKey::from_parts(/* by_host= */ true, "ml8.hpc.uio.no", 2447150)).unwrap();

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
}

#[test]
fn test_compute_jobs4() {
    // job 2447150 crosses files and hosts

    // Filter by job ID, we just want the one job
    let filter = |_user:&str, _host:&str, job: u32, _t:&Timestamp| {
        job == 2447150
    };
    let (jobs, _numrec, _earliest, _latest) = compute_jobs(&vec![
        "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
        "../sonar_test_data0/2023/05/31/ml1.hpc.uio.no.csv".to_string()],
                         &filter, /* merge_across_hosts= */ true).unwrap();

    assert!(jobs.len() == 1);
    let job = jobs.get(&JobKey::from_parts(/* by_host= */ false, "", 2447150)).unwrap();

    // First record is in the ml1 file
    // 2023-06-23T12:24:01.486240376+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,173,18813976,1000,0,0,833536
    //
    // Last record is in the ml8 file
    // 2023-06-24T01:41:01.411339362+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,160,18981724,1000,0,0,833536

    let start = job[0].timestamp;
    let end = job[job.len()-1].timestamp;
    assert!(start.year() == 2023 && start.month() == 6 && start.day() == 23 &&
            start.hour() == 12 && start.minute() == 24 && start.second() == 1);
    assert!(end.year() == 2023 && end.month() == 6 && end.day() == 24 &&
    end.hour() == 1 && end.minute() == 41 && end.second() == 1);
}
