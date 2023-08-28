/// Utilities for handling "jobs": sets of log entries with a shared job ID
use crate::{postprocess_log, read_logfiles, LogEntry, System, Timestamp};

use anyhow::Result;
use std::collections::HashMap;

#[cfg(test)]
use chrono::{Datelike, Timelike};

pub type JobKey = (String, u32);

/// Given a list of file names of log files, read all the logs and return a hashmap that maps the
/// JobKey to a sorted vector of the job records for the JobKey, along with the count of unfiltered
/// records and the earliest and latest timestamp seen across all logs before filtering.
///
/// The JobKey must distinguish by host name and job ID; the client must perform cross-host merging,
/// if any.
///
/// This propagates I/O errors, though not necessarily precisely.

pub fn compute_jobs<F>(
    logfiles: &[String],
    filter: F,
    configs: &Option<HashMap<String, System>>,
) -> Result<(HashMap::<JobKey, Vec<Box<LogEntry>>>, usize, Timestamp, Timestamp)>
where
    F: Fn(&LogEntry) -> bool,
{
    let (mut entries, earliest, latest, num_records) = read_logfiles(logfiles)?;
    entries = postprocess_log(entries, filter, configs);

    let mut joblog = HashMap::<JobKey, Vec<Box<LogEntry>>>::new();

    while let Some(entry) = entries.pop() {
        let key = (entry.hostname.clone(), entry.job_id);
        if let Some(job) = joblog.get_mut(&key) {
            job.push(entry);
        } else {
            joblog.insert(key, vec![entry]);
        }
    }

    // The `joblog` is a map from job ID to a vector of all job records with that job ID. Sort each
    // vector by ascending timestamp to get an idea of the duration of the job.
    //
    // (I have no idea what `&mut ref mut` means.)

    joblog.iter_mut().for_each(|(_k, &mut ref mut job)| {
        job.sort_by_key(|j| j.timestamp);
    });
 
    Ok((joblog, num_records, earliest, latest))
}

#[cfg(untagged_sonar_data)]
#[test]
fn test_compute_jobs1a() {
    let filter = |_e: &LogEntry| true;
    assert!(compute_jobs(
        &vec![
            "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
            "../sonar_test_data0/2023/07/01/ml3.hpc.uio.no.csv".to_string(), // Not found
            "../sonar_test_data0/2023/06/02/ml8.hpc.uio.no.csv".to_string()
        ],
        &filter,
        &None
    )
    .is_err());
}

#[test]
fn test_compute_jobs1b() {
    let filter = |_e: &LogEntry| true;
    assert!(compute_jobs(
        &vec![
            "../sonar_test_data0/2023/08/15/ml8.hpc.uio.no.csv".to_string(),
            "../sonar_test_data0/2023/08/15/ml1.hpc.uio.no.csv".to_string(), // Not found
            "../sonar_test_data0/2023/08/15/ml3.hpc.uio.no.csv".to_string()
        ],
        &filter,
        &None
    )
    .is_err());
}

#[cfg(untagged_sonar_data)]
#[test]
fn test_compute_jobs2a() {
    // Filter by time so that we can test computation of earliest and latest
    let filter = |e: &LogEntry| e.timestamp.hour() >= 6 && e.timestamp.hour() <= 18;
    let (_jobs, numrec, earliest, latest) = compute_jobs(
        &vec![
            "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
            "../sonar_test_data0/2023/06/01/ml8.hpc.uio.no.csv".to_string(),
        ],
        &filter,
        &None
    )
    .unwrap();

    // total number of records read
    assert!(numrec == 1440 + 1440);

    // first record of first file:
    // 2023-06-23T05:05:01.224181967+00:00,ml8.hpc.uio.no,192,einarvid,2381069,mongod,1.6,3608300,0,0,0,0
    assert!(
        earliest.year() == 2023
            && earliest.month() == 6
            && earliest.day() == 23
            && earliest.hour() == 5
            && earliest.minute() == 5
            && earliest.second() == 1
    );

    // last record of last file:
    // 2023-06-24T22:05:02.092905606+00:00,ml8.hpc.uio.no,192,zabbix,4093,zabbix_agentd,4.6,2664,0,0,0,0
    assert!(
        latest.year() == 2023
            && latest.month() == 6
            && latest.day() == 24
            && latest.hour() == 22
            && latest.minute() == 5
            && latest.second() == 2
    );
}

#[test]
fn test_compute_jobs2b() {
    // Filter by time so that we can test computation of earliest and latest.  Note this should not
    // affect this test.  In fact, we could exclude every record here.
    let filter = |e: &LogEntry| e.timestamp.hour() >= 13 && e.timestamp.hour() <= 15;
    let (_jobs, numrec, earliest, latest) = compute_jobs(
        &vec![
            "../sonar_test_data0/2023/08/15/ml8.hpc.uio.no.csv".to_string(),
            "../sonar_test_data0/2023/08/15/ml3.hpc.uio.no.csv".to_string(),
        ],
        &filter,
        &None
    )
    .unwrap();

    // total number of records read
    assert!(numrec == 108 + 33);

    // first record of first file:
    // v=0.7.0,time=2023-08-15T12:46:53+02:00,host=ml8.hpc.uio.no,cores=192,user=joachipo,job=3321033,pid=0,cmd=python,cpu%=3074.4,cpukib=304326252,gpus=0,gpu%=3033.6,gpumem%=44,gpukib=5441536,cputime_sec=40655,rolledup=28
    assert!(
        earliest.year() == 2023
            && earliest.month() == 8
            && earliest.day() == 15
            && earliest.hour() == 10 // UTC
            && earliest.minute() == 46
            && earliest.second() == 53
    );

    // last record of last file:
    // v=0.7.0,time=2023-08-15T13:05:01+02:00,host=ml3.hpc.uio.no,cores=56,user=lamonsta,job=25997,pid=25997,cmd=bash,cpu%=50.1,cpukib=3744,gpus=none,gpu%=0,gpumem%=0,gpukib=0,cputime_sec=1539221
    assert!(
        latest.year() == 2023
            && latest.month() == 8
            && latest.day() == 15
            && latest.hour() == 11 // UTC
            && latest.minute() == 5
            && latest.second() == 1
    );
}

#[cfg(untagged_sonar_data)]
#[test]
fn test_compute_jobs3() {
    // job 2447150 crosses files (but not hosts)

    // Filter by job ID, we just want the one job
    let filter = |e:&LogEntry| e.job_id == 2447150;
    let (jobs, _numrec, _earliest, _latest) = compute_jobs(
        &vec![
            "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
            "../sonar_test_data0/2023/06/01/ml8.hpc.uio.no.csv".to_string(),
        ],
        &filter,
        &None,
    )
    .unwrap();

    assert!(jobs.len() == 1);
    let job = jobs
        .get(&JobKey::from_parts(
            /* by_host= */ true,
            "ml8.hpc.uio.no",
            2447150,
        ))
        .unwrap();

    // First record
    // 2023-06-23T12:25:01.486240376+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,173,18813976,1000,0,0,833536
    //
    // Last record
    // 2023-06-24T09:00:01.386294752+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,161,13077760,1000,0,0,833536

    let start = job[0].timestamp;
    let end = job[job.len() - 1].timestamp;
    assert!(
        start.year() == 2023
            && start.month() == 6
            && start.day() == 23
            && start.hour() == 12
            && start.minute() == 25
            && start.second() == 1
    );
    assert!(
        end.year() == 2023
            && end.month() == 6
            && end.day() == 24
            && end.hour() == 9
            && end.minute() == 0
            && end.second() == 1
    );
}

#[cfg(untagged_sonar_data)]
#[test]
fn test_compute_jobs4() {
    // job 2447150 crosses files and hosts

    // Filter by job ID, we just want the one job
    let filter = |e:&LogEntry| e.job_id == 2447150;
    let (jobs, _numrec, _earliest, _latest) = compute_jobs(
        &vec![
            "../sonar_test_data0/2023/05/31/ml8.hpc.uio.no.csv".to_string(),
            "../sonar_test_data0/2023/05/31/ml1.hpc.uio.no.csv".to_string(),
        ],
        &filter,
        &None,
    )
    .unwrap();

    assert!(jobs.len() == 1);
    let job = jobs
        .get(&JobKey::from_parts(/* by_host= */ false, "", 2447150))
        .unwrap();

    // First record is in the ml1 file
    // 2023-06-23T12:24:01.486240376+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,173,18813976,1000,0,0,833536
    //
    // Last record is in the ml8 file
    // 2023-06-24T01:41:01.411339362+00:00,ml8.hpc.uio.no,192,larsbent,2447150,python,160,18981724,1000,0,0,833536

    let start = job[0].timestamp;
    let end = job[job.len() - 1].timestamp;
    assert!(
        start.year() == 2023
            && start.month() == 6
            && start.day() == 23
            && start.hour() == 12
            && start.minute() == 24
            && start.second() == 1
    );
    assert!(
        end.year() == 2023
            && end.month() == 6
            && end.day() == 24
            && end.hour() == 1
            && end.minute() == 41
            && end.second() == 1
    );
}
