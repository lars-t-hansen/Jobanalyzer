// Process sonar log files and list jobs, with optional filtering and details.
// (WIP)

use anyhow::Result;
use chrono::prelude::DateTime;
use chrono::Utc;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

// List jobs for user in sonar logs.
//
// lsjobs [user [from [to]]]
// where
//   user is a user name, "-" for "everyone", default is the current user
//   from and to are iso dates yyyy-mm-dd
//   default "from" is 24 hours ago
//   default "to" is now
//
// Ergo "lsjobs" lists my jobs for the last 24 hours.
//
// By default the log files are found in a directory named by environment
// variable SONAR_ROOT, otherwise in $HOME/sonar_logs.
//
// Log file structure is as for jobgraph: ...
//
//
// Output:
// The basic listing is
//
//  job-id  user  start-time end-time avg-cpu% avg-mem avg-gpu% avg-gpu-mem% command
//
// where user is shown only if the requested user was "-".  This is sorted by increasing
// start-time (i think).

// For jobgraph, the log format is this:
//    let file_name = format!("{}/{}/{}/{}/{}.csv", data_path, year, month, day, hostname);
// where we loop across dates and host names, and data_path defaults to /cluster/shared/sonar/data,
// akin to our SONAR_ROOT.
//
// Host names are a complication, plus host names are redundantly coded into the sonar output.  This
// allows log files to be catenated though, maybe just as well.

fn main() {
    // Parse date range
    // Find all files that overlap the date range
    // Create user filter
    // For each file
    //   entries <- Read the file
    //   join the full list
    // v2:
    //   sort by user and job id - there could be several entries with the same
    //     user and job id but different host names
    //   merge jobs across hosts that have the same user and job id
    //     start date <- earliest among them
    //     end date <- latest among them
    //     command <- ??? should be the same ???
    //     other fields are just summed?  probably
    // sort by start date
    // create listing
    let logfiles = vec![
        //"/itf-fi-ml/home/larstha/sonar/ml8.hpc.uio.no.log".to_string(),
        "ml8.hpc.uio.no.log".to_string(),
        ];
    let from = None;
    let to = None;
    let users = None;

    let mut joblog = HashMap::<u32, Vec<LogEntry>>::new();
    logfiles.iter().for_each(|file| {
        match parse_log(file, users, from, to) {
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
                eprintln!("ERROR: {:?}", e);
                return;
            }
        }
    });

    // OK, now we have collected all records for each job into a vector. Sort the vector by
    // ascending timestamp to get an idea for the duration of the job.
    //
    // (I have no idea what `&mut ref mut` means.)
    joblog.iter_mut().for_each(|(_k, &mut ref mut job)| {
        job.sort_by_key(|j| j.timestamp);
    });

    // Get the vectors of jobs back into a vector
    let mut jobvec = joblog.drain().map(|(_, val)| val).collect::<Vec<Vec<LogEntry>>>();

    // And sort ascending by lowest timestamp
    jobvec.sort_by(|a, b| a[0].timestamp.cmp(&b[0].timestamp));

    // Now print.
    //
    // Unix user names are max 8 chars.
    // Linux pids are max 7 decimal digits.
    // We don't care about seconds or fractions of a second in the timestamp, nor timezone.
    //
    // NOTE, these are samples, so anything that's alive when the logger first starts running will
    // have the same "start" time and anything that's alive when the log is observed with have the
    // same "stop" time.  The logger and this listing only have value if the logger is run
    // continually after boot.

    let tfmt = "%Y-%m-%d_%H:%M";
    jobvec.iter().for_each(|job| {
        println!("{:7} {:8} {} {} {}", job[0].job_id, job[0].user, job[0].timestamp.format(tfmt), job[job.len()-1].timestamp.format(tfmt), job[0].command);
    });
}

#[derive(Debug)]
struct LogEntry {
    timestamp: DateTime<Utc>,
    hostname: String,
    num_cores: u32,
    user: String,
    job_id: u32,
    command: String,
    cpu_pct: f64,
    mem_gb: f64,
    gpu_mask: usize,
    gpu_pct: f64,
    gpu_mem_pct: f64,
    gpu_mem_gb: f64,
}

// Read entries from the log and parse them, keeping the ones for the user (or for all the users if
// users==None) in the date range (if supplied).

fn parse_log(
    file_name: &str,
    users: Option<&HashSet<String>>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> Result<Vec<LogEntry>> {
    #[derive(Debug, Deserialize)]
    struct LogRecord {
        timestamp: String,
        hostname: String,
        num_cores: u32,
        user: String,
        job_id: u32,
        command: String,
        cpu_percentage: f64,
        mem_kb: u64,
        gpu_mask: String,
        gpu_percentage: f64,
        gpu_mem_percentage: f64,
        gpu_mem_kb: u64,
    }

    let mut results = vec![];
    let error_message = "INTERNAL ERROR in lsjobs".to_string();
    if std::path::Path::new(&file_name).exists() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(file_name)
            .expect(&error_message);

        for record in reader.deserialize() {
            let record: LogRecord = record.expect(&error_message);
            if users.is_none() || users.unwrap().contains(&record.user) {
                let timestamp : DateTime<Utc> =
                    DateTime::parse_from_rfc3339(&record.timestamp).expect(&error_message).into();
                if (from.is_none() || from.unwrap() <= timestamp) &&
                    (to.is_none() || timestamp <= to.unwrap()) {
                        let gpu_mask =
                            usize::from_str_radix(&record.gpu_mask, 2).expect(&error_message);
                        results.push(LogEntry {
                            timestamp,
                            hostname: record.hostname,
                            num_cores: record.num_cores,
                            user: record.user,
                            job_id: record.job_id,
                            command: record.command,
                            cpu_pct: record.cpu_percentage / 100.0,
                            mem_gb: (record.mem_kb as f64) / (1024.0 * 1024.0),
                            gpu_mask,
                            gpu_pct: record.gpu_percentage / 100.0,
                            gpu_mem_pct: record.gpu_mem_percentage / 100.0,
                            gpu_mem_gb: (record.gpu_mem_kb as f64) / (1024.0 * 1024.0),
                        });
                    }
            }
        }
    }
    Ok(results)
}
