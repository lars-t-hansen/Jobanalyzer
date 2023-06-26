// Process Sonar log files and list jobs, with optional filtering and details.
// See MANUAL.md for a manual, or run with --help for brief help.

// TODO
//
// This merges jobs across nodes / hosts and will show the correct total utilization, but does not
// show the node names.
//
// Memory consumption is not shown, but we have the data, and we want them.
//
// The input file enumeration must be implemented.
//
// Apply output filtering:
//  - max number per user from the _last_, not the first, this may mean a prepass
//  - by avg/peak cpu/gpu
//  - by duration
//
// Not sure the `ty` field pays for itself at all.
//
// Maybe move the defaulting of data-path back into main.rs because it's part of command line
// processing, not part of file finding.  Maybe.
//
// Maybe refactor the argument processing into a separate file, it's becoming complex enough.  Wait
// until output filtering logic is in order.

mod logfile;

use chrono::prelude::{DateTime,NaiveDate};
use chrono::Utc;
use clap::Parser;
use core::cmp::{min,max};
use std::collections::{HashSet,HashMap};
use std::env;
use std::num::ParseIntError;
use std::process;
use std::str::FromStr;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Select the root directory for log files [default: $SONAR_ROOT]
    #[arg(long)]
    data_path: Option<String>,

    /// User name(s) to include, comma-separated, "-" for all [default: $LOGNAME]
    #[arg(long, short)]
    user: Option<String>,

    /// User name(s) to exclude, comma-separated [default: none]
    #[arg(long)]
    exclude: Option<String>,

    /// Job number(s) to select, comma-separated [default: all]
    #[arg(long, value_parser = job_numbers)]
    job: Option<Vec<usize>>,
    
    /// Select records by this time and later, format YYYY-MM-DD, `start` for the first record in logs [default: 24h ago]
    #[arg(long, short, value_parser = start_time)]
    from: Option<DateTime<Utc>>,

    /// Select records by this time and earlier, format YYYY-MM-DD, `end` for the last record in logs [default: now]
    #[arg(long, short, value_parser = end_time)]
    to: Option<DateTime<Utc>>,

    /// Select records and logs by these host name(s), comma-separated [default: all]
    #[arg(long)]
    host: Option<String>,

    /// Print at most these many most recent jobs per user [default: all]
    #[arg(long, short)]
    numrecs: Option<usize>,

    /// Print only jobs with this much average CPU use (100=1 full CPU) [default: 0]
    #[arg(long)]
    avgcpu: Option<usize>, 

    /// Print only jobs with this much peak CPU use (100=1 full CPU) [default: 0]
    #[arg(long)]
    maxcpu: Option<usize>, 

    /// Print only jobs with this much average GPU use (100=1 full GPU card) [default: 0]
    #[arg(long)]
    avggpu: Option<usize>, 

    /// Print only jobs with this much peak GPU use (100=1 full GPU card) [default: 0]
    #[arg(long)]
    maxgpu: Option<usize>, 

    /// Print only jobs with at least this much runtime, format `DdHhMm`, all parts optional [default: 0]
    #[arg(long, value_parser = run_time)]
    minrun: Option<Duration>,

    /// Log file names (overrides --data-path)
    #[arg(last = true)]
    logfiles: Vec<String>,
}

// Comma-separated job numbers
fn job_numbers(s: &str) -> Result<Vec<usize>, String> {
    let candidates = s.split(',').map(|x| usize::from_str(x)).collect::<Vec<Result<usize, ParseIntError>>>();
    if candidates.iter().all(|x| x.is_ok()) {
        Ok(candidates.iter().map(|x| *x.as_ref().unwrap()).collect::<Vec<usize>>())
    } else {
        Err("Illegal job numbers: ".to_string() + s)
    }
}

// YYYY-MM-DD, but with a little (too much?) flexibility
fn parse_time(s: &str) -> Result<DateTime<Utc>, String> {
    let parts = s.split('-').map(|x| usize::from_str(x)).collect::<Vec<Result<usize, ParseIntError>>>();
    if !parts.iter().all(|x| x.is_ok()) || parts.len() != 3 {
        return Err(format!("Invalid date syntax: {}", s));
    }
    let vals = parts.iter().map(|x| *x.as_ref().unwrap()).collect::<Vec<usize>>();
    let d = NaiveDate::from_ymd_opt(vals[0] as i32, vals[1] as u32, vals[2] as u32);
    if !d.is_some() {
        return Err(format!("Invalid date: {}", s));
    }
    Ok(DateTime::from_utc(d.unwrap().and_hms_opt(0,0,0).unwrap(), Utc))
}

// YYYY-MM-DD | start
fn start_time(s: &str) -> Result<DateTime<Utc>, String> {
    if s == "start" {
        Ok(epoch())
    } else {
        parse_time(s)
    }
}

// YYYY-MM-DD | end
fn end_time(s: &str) -> Result<DateTime<Utc>, String> {
    if s == "end" {
        Ok(now())
    } else {
        parse_time(s)
    }
}

// This is DdHhMm with all parts optional but at least one part required
fn run_time(s: &str) -> Result<Duration, String> {
    let bad = format!("Bad time duration syntax: {}", s);
    let mut days = 0u64;
    let mut hours = 0u64;
    let mut minutes = 0u64;
    let mut have_days = false;
    let mut have_hours = false;
    let mut have_minutes = false;
    let mut ds = "".to_string();
    for ch in s.chars() {
        if ch.is_digit(10) {
            ds = ds + &ch.to_string();
        } else {
            if ds == "" ||
                (ch != 'd' && ch != 'h' && ch != 'm') ||
                (ch == 'd' && have_days) || (ch == 'h' && have_hours) || (ch == 'm' && have_minutes) {
                    return Err(bad)
                }
            let v = u64::from_str(&ds);
            if !v.is_ok() {
                return Err(bad);
            }
            let val = v.unwrap();
            ds = "".to_string();
            if ch == 'd' {
                have_days = true;
                days = val;
            } else if ch == 'h' {
                have_hours = true;
                hours = val;
            } else if ch == 'm' {
                have_minutes = true;
                minutes = val;
            }
        }
    }
    if ds != "" || (!have_days && !have_hours && !have_minutes) {
        return Err(bad);
    }

    return Ok(Duration::from_secs(days * 3600 * 24 + hours * 3600 + minutes * 60))
}

fn main() {
    let cli = Cli::parse();

    // Convert the input filtering options to a useful form.

    let from = if let Some(x) = cli.from { x } else { one_day_ago() };
    let to = if let Some(x) = cli.to { x } else { now() };
    if from > to {
        fail("The --from time is greater than the --to time");
    }

    let include_hosts = if let Some(hosts) = cli.host {
        hosts.split(',').map(|x| x.to_string()).collect::<HashSet<String>>()
    } else {
        HashSet::new()
    };

    let include_jobs = if let Some(jobs) = cli.job {
        jobs.iter().map(|x| *x).collect::<HashSet<usize>>()
    } else {
        HashSet::new()
    };

    let include_users = if let Some(users) = cli.user {
        if users == "-" {
            HashSet::new()
        } else {
            users.split(',').map(|x| x.to_string()).collect::<HashSet<String>>()
        }
    } else {
        let mut users = HashSet::new();
        if let Ok(u) = env::var("LOGNAME") {
            users.insert(u);
        };
        users
    };

    let mut exclude_users = if let Some(excl) = cli.exclude {
        excl.split(',').map(|x| x.to_string()).collect::<HashSet<String>>()
    } else {
        HashSet::new()
    };
    exclude_users.insert("root".to_string());
    exclude_users.insert("zabbix".to_string());

    // The input filter.

    let filter = |user:&str, host:&str, job: u32, t:&DateTime<Utc>| {
        ((&include_users).is_empty() || (&include_users).contains(user)) &&
        ((&include_hosts).is_empty() || (&include_hosts).contains(host)) &&
        ((&include_jobs).is_empty() || (&include_jobs).contains(&(job as usize))) &&
            !(&exclude_users).contains(user) &&
            from <= *t &&
            *t <= to
    };

    // Logfiles, filtered by host and time range.

    let maybe_logfiles = logfile::find_logfiles(cli.logfiles, cli.data_path, &include_hosts, from, to);
    if let Err(ref msg) = maybe_logfiles {
        fail(&msg);
    }
    let logfiles = maybe_logfiles.unwrap();

    // Read the files, filter the records, build up a set of candidate log records.

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

    println!("{}", joblog.len());

    // OK, now the log is a map from job ID to a vector of all records for the job with that
    // ID. Sort each vector by ascending timestamp to get an idea of the duration of the job.
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
        let max_start = epoch();
        let min_start = now();
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
             "job#", "user", "time", "start?", "end?", "command", "ty", "cpu avg/max", "gpu avg/max");
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

fn epoch() -> DateTime<Utc> {
    // FIXME, but this is currently good enough for all our uses
    DateTime::from_utc(NaiveDate::from_ymd_opt(2000,1,1).unwrap().and_hms_opt(0,0,0).unwrap(), Utc)
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

fn one_day_ago() -> DateTime<Utc> {
    now() - chrono::Duration::days(1)
}

fn fail(msg: &str) {
    eprintln!("ERROR: {}", msg);
    process::exit(1);
}
