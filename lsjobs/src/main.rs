// Process Sonar log files and list jobs, with optional filtering and details.
// See MANUAL.md for a manual, or run with --help for brief help.

// TODO
//
// This merges jobs across nodes / hosts and should show the correct total utilization, but does not
// show the node names.
//
// Memory consumption is not shown, but we have the data.
//
// Default users to exclude could be read from a config file (as could other things be), but then
// the config file would have to be somewhere (maybe in /etc).

mod logfile;

use core::cmp::{min,max};
use std::collections::{HashSet,HashMap};
use chrono::prelude::{DateTime,NaiveDate};
use chrono::Utc;

// Task list:
// - figure out command line args and parse those into suitable variables, and implement
//   functionality we might be missing
//
// - figure out file listing and file filtering
//
// - maybe refactor a bit

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

    let data_path = ".";
    let logfiles = vec![
        data_path.to_string() + "/" + "ml8.hpc.uio.no.log",
        ];

    let from: Option<DateTime<Utc>> = None;
    let to: Option<DateTime<Utc>> = None;
    let include_users : HashSet<String> = HashSet::new();
    let mut exclude_users = HashSet::new();
    exclude_users.insert("root");
    exclude_users.insert("zabbix");
    let filter = |user:&str, t:&DateTime<Utc>| {
        ((&include_users).is_empty() || (&include_users).contains(user)) &&
            !(&exclude_users).contains(user) &&
            (from.is_none() || from.unwrap() <= *t) &&
            (to.is_none() || *t <= to.unwrap())
    };
    let mut joblog = HashMap::<u32, Vec<logfile::LogEntry>>::new();
    logfiles.iter().for_each(|file| {
        match logfile::parse(file, &filter) {
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
    // TODO: We currenly only care about the max and min timestamps per job, so optimize later if
    // that doesn't change.
    //
    // (I have no idea what `&mut ref mut` means.)
    joblog.iter_mut().for_each(|(_k, &mut ref mut job)| {
        job.sort_by_key(|j| j.timestamp);
    });

    // Compute the earliest and latest times observed across all the logs
    let (earliest, latest) = {
        let max_start = DateTime::from_utc(NaiveDate::from_ymd_opt(2000,1,1).unwrap().and_hms_opt(0,0,0).unwrap(), Utc);
        let min_start = DateTime::from_utc(NaiveDate::from_ymd_opt(2038,1,1).unwrap().and_hms_opt(0,0,0).unwrap(), Utc);
        joblog.iter().fold((min_start, max_start),
                           |(earliest, latest), (_k, r)| (min(earliest, r[0].timestamp), max(latest, r[r.len()-1].timestamp)))
    };

    // Get the vectors of jobs back into a vector, and filter out jobs observed only once
    let mut jobvec = joblog
        .drain()
        .map(|(_, val)| val)
        .filter(|job| job.len() > 1)
        .collect::<Vec<Vec<logfile::LogEntry>>>();

    // And sort ascending by lowest timestamp
    jobvec.sort_by(|a, b| a[0].timestamp.cmp(&b[0].timestamp));

    // Now print.
    //
    // Unix user names are max 8 chars.
    // Linux pids are max 7 decimal digits.
    // We don't care about seconds in the timestamp, nor timezone.

    println!("{:8} {:8}   {:9}   {:16}   {:16}   {:22}   {:3}  {:11}  {:11}",
             "job#", "user", "time", "start", "end", "command", "ty", "cpu avg/max", "gpu avg/max");
    let tfmt = "%Y-%m-%d %H:%M";
    jobvec.iter().for_each(|job| {
        let first = job[0].timestamp;
        let last = job[job.len()-1].timestamp;
        let duration = (last - first).num_minutes();
        let minutes = duration % 60;                  // fractional hours
        let hours = (duration / 60) % 24;             // fractional days
        let days = duration / (60 * 24);              // full days
        let dur = format!("{:2}d{:2}h{:2}m", days, hours, minutes);
        let uses_gpu = job.iter().any(|jr| jr.gpu_mask != 0);
        let avg_cpu = job.iter().fold(0.0, |acc, jr| acc + jr.cpu_pct) / (job.len() as f64);
        let peak_cpu = job.iter().map(|jr| jr.cpu_pct).reduce(f64::max).unwrap();
        let avg_gpu = job.iter().fold(0.0, |acc, jr| acc + jr.gpu_pct) / (job.len() as f64);
        let peak_gpu = job.iter().map(|jr| jr.gpu_pct).reduce(f64::max).unwrap();
        println!("{:7}{} {:8}   {}   {}   {}   {:22}   {}  {:5.1}/{:5.1}  {:5.1}/{:5.1}",
                 job[0].job_id,
                 if first == earliest && last == latest {
                     "!"
                 } else if first == earliest {
                     "<"
                 } else if last == latest {
                     ">"
                 } else {
                     " "
                 },
                 job[0].user,
                 dur,
                 first.format(tfmt),
                 last.format(tfmt),
                 job[0].command,
                 if uses_gpu { "gpu" } else { "   " },
                 avg_cpu,
                 peak_cpu,
                 avg_gpu,
                 peak_gpu);
    });
}
