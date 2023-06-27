// Process Sonar log files and list jobs, with optional filtering and details.
// See MANUAL.md for a manual, or run with --help for brief help.

// TODO - High pri
//
// The input file enumeration must be implemented.
//
// Now we can ask for "at least this much cpu/gpu" but we can't ask for "no more than this much
// cpu/gpu".  It would be great to ask for at least "no gpu" or "very little gpu" in some way.  The
// switches probably want to be renamed.  Consider "maxgpu" which is really a /floor/ for the peak
// gpu.  Maybe --min-peak-gpu would be good.  Then we could have eg --max-peak-gpu=0 to list jobs
// that don't use GPU at all.  We already have this for --minrun.
//
//
// TODO - Normal pri
//
// There's a fairly benign bug below in how earliest and latest are computed.
//
// Could add aggregation filtering to show jobs in the four categories corresponding to the "!",
// "<", ">", and " " marks.
//
// Having the absence of --user mean "only $LOGNAME" is a footgun, maybe - it's right for a use case
// where somebody is looking at her own jobs though.
//
// This merges jobs across nodes / hosts and will show the correct total utilization, but does not
// show the node names.
//
// Maybe refactor the argument processing into a separate file, it's becoming complex enough.  Wait
// until output filtering logic is in order.
//
// Not sure if it's the right default to filter jobs observed only once, and if it is the right
// default, then we should have a switch to control this, eg, -o 0 to show all jobs (`-o 2` is the
// default), -o 5 to show jobs observed at least five times.  This is partly redundant with running
// time I guess.  A job observed only once will have running time zero.  The long name for this
// would be --min-observations.
//
// We allow for at most a two-digit number of days of running time in the output but in practice
// we're going to see some three-digit number of days, make room for that.
//
// Selftest cases esp for the argument parsers and filterers.
//
// Performance and memory use will become an issue with a large number of records?  Probably want to
// profile before we hack too much, but there are obvious inefficiencies in representations and the
// number of passes across data structures, and probably in the number of copies made (and thus the
// amount of memory allocation).

mod logfile;
mod dates;

use chrono::prelude::{DateTime,NaiveDate};
use chrono::Utc;
use clap::Parser;
use core::cmp::{min,max};
use std::collections::{HashSet,HashMap};
use std::env;
use std::num::ParseIntError;
use std::process;
use std::str::FromStr;
use std::time;

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
    
    /// Select records by this time and later, format YYYY-MM-DD [default: 24h ago]
    #[arg(long, short, value_parser = parse_time)]
    from: Option<DateTime<Utc>>,

    /// Select records by this time and earlier, format YYYY-MM-DD [default: now]
    #[arg(long, short, value_parser = parse_time)]
    to: Option<DateTime<Utc>>,

    /// Select records and logs by these host name(s), comma-separated [default: all]
    #[arg(long)]
    host: Option<String>,

    /// Print at most these many most recent jobs per user [default: all]
    #[arg(long, short)]
    numrecs: Option<usize>,

    /// Print only jobs with at least this much average CPU use (100=1 full CPU) [default: 0]
    #[arg(long)]
    avgcpu: Option<usize>, 

    /// Print only jobs with at least this much peak CPU use (100=1 full CPU) [default: 0]
    #[arg(long)]
    maxcpu: Option<usize>, 

    /// Print only jobs with at least this much average main memory use (GB) [default: 0]
    #[arg(long)]
    avgmem: Option<usize>, 

    /// Print only jobs with at least this much peak main memory use (GB) [default: 0]
    #[arg(long)]
    maxmem: Option<usize>, 

    /// Print only jobs with at least this much average GPU use (100=1 full GPU card) [default: 0]
    #[arg(long)]
    avggpu: Option<usize>, 

    /// Print only jobs with at least this much peak GPU use (100=1 full GPU card) [default: 0]
    #[arg(long)]
    maxgpu: Option<usize>, 

    /// Print only jobs with at least this much average GPU memory use (100=1 full GPU card) [default: 0]
    #[arg(long)]
    avgvmem: Option<usize>, 

    /// Print only jobs with at least this much peak GPU memory use (100=1 full GPU card) [default: 0]
    #[arg(long)]
    maxvmem: Option<usize>, 

    /// Print only jobs with at least this much runtime, format `DdHhMm`, all parts optional [default: 0]
    #[arg(long, value_parser = run_time)]
    minrun: Option<chrono::Duration>,

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

// This is DdHhMm with all parts optional but at least one part required
fn run_time(s: &str) -> Result<chrono::Duration, String> {
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

    match chrono::Duration::from_std(time::Duration::from_secs(days * 3600 * 24 + hours * 3600 + minutes * 60)) {
        Ok(e) => Ok(e),
        Err(_) => Err("Bad running time".to_string())
    }
}

fn main() {
    let cli = Cli::parse();

    // Figure out the data path from switches and defaults.

    let data_path = if cli.data_path.is_some() {
        cli.data_path
    } else if let Ok(val) = env::var("SONAR_ROOT") {
        Some(val)
    } else if let Ok(val) = env::var("HOME") {
        Some(val + "/sonar_logs")
    } else {
        None
    };

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

    // Convert the aggregation filter options to a useful form.

    let avgcpu = if let Some(n) = cli.avgcpu { n as f64 } else { 0.0 };
    let maxcpu = if let Some(n) = cli.maxcpu { n as f64 } else { 0.0 };
    let avgmem = if let Some(n) = cli.avgmem { n } else { 0 };
    let maxmem = if let Some(n) = cli.maxmem { n } else { 0 };
    let avggpu = if let Some(n) = cli.avggpu { n as f64 } else { 0.0 };
    let maxgpu = if let Some(n) = cli.maxgpu { n as f64 } else { 0.0 };
    let minrun = if let Some(n) = cli.minrun { n.num_seconds() } else { 0 };
    let avgvmem = if let Some(n) = cli.avgvmem { n as f64 } else { 0.0 };
    let maxvmem = if let Some(n) = cli.maxvmem { n as f64 } else { 0.0 };
    // `minsamples` should maybe be an option: the minimum number of observations we have to make of a
    // job to consider it further.
    let minsamples = 2;
    
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

    let maybe_logfiles = logfile::find_logfiles(cli.logfiles, data_path, &include_hosts, from, to);
    if let Err(ref msg) = maybe_logfiles {
        fail(&format!("{}", msg));
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

    // Get the vectors of jobs back into a vector, aggregate data, and filter the jobs.
    struct Aggregate {
        first: DateTime<Utc>,
        last: DateTime<Utc>,
        duration: i64,
        minutes: i64,
        hours: i64,
        days: i64,
        _uses_gpu: bool,
        avg_cpu: f64,
        peak_cpu: f64,
        avg_gpu: f64,
        peak_gpu: f64,
        avg_mem_gb: f64,
        peak_mem_gb: f64,
        avg_vmem_pct: f64,
        peak_vmem_pct: f64,
        selected: bool,
    }

    let mut jobvec = joblog
        .drain()
        .filter(|(_, job)| job.len() >= minsamples)
        .map(|(_, job)| {
            let first = job[0].timestamp;
            let last = job[job.len()-1].timestamp;
            let duration = (last - first).num_seconds();
            let minutes = duration / 60;
            (Aggregate {
                first,
                last,
                duration: duration,                     // total number of seconds
                minutes: minutes % 60,                  // fractional hours
                hours: (minutes / 60) % 24,             // fractional days
                days: minutes / (60 * 24),              // full days
                _uses_gpu: job.iter().any(|jr| jr.gpu_mask != 0),
                avg_cpu: (job.iter().fold(0.0, |acc, jr| acc + jr.cpu_pct) / (job.len() as f64) * 100.0).round(),
                peak_cpu: (job.iter().map(|jr| jr.cpu_pct).reduce(f64::max).unwrap() * 100.0).round(),
                avg_gpu: (job.iter().fold(0.0, |acc, jr| acc + jr.gpu_pct) / (job.len() as f64) * 100.0).round(),
                peak_gpu: (job.iter().map(|jr| jr.gpu_pct).reduce(f64::max).unwrap() * 100.0).round(),
                avg_mem_gb: (job.iter().fold(0.0, |acc, jr| acc + jr.mem_gb) /  (job.len() as f64)).round(),
                peak_mem_gb: (job.iter().map(|jr| jr.mem_gb).reduce(f64::max).unwrap()).round(),
                avg_vmem_pct: (job.iter().fold(0.0, |acc, jr| acc + jr.gpu_mem_pct) /  (job.len() as f64) * 100.0).round(),
                peak_vmem_pct: (job.iter().map(|jr| jr.gpu_mem_pct).reduce(f64::max).unwrap() * 100.0).round(),
                selected: true,
             },
             job)
        })
        .filter(|(aggregate, _)| {
            aggregate.avg_cpu >= avgcpu &&
                aggregate.peak_cpu >= maxcpu &&
                aggregate.avg_mem_gb >= avgmem as f64 &&
                aggregate.peak_mem_gb >= maxmem as f64 &&
                aggregate.avg_gpu >= avggpu &&
                aggregate.peak_gpu >= maxgpu &&
                aggregate.avg_vmem_pct >= avgvmem &&
                aggregate.peak_vmem_pct >= maxvmem &&
                aggregate.duration >= minrun
        })
        .collect::<Vec<(Aggregate, Vec<logfile::LogEntry>)>>();

    // And sort ascending by lowest beginning timestamp
    jobvec.sort_by(|a, b| a.0.first.cmp(&b.0.first));

    // Select a number of records per user, if applicable.  This means working from the bottom up
    // in the vector and marking the n first per user.  We need a hashmap user -> count.
    if let Some(n) = cli.numrecs {
        let mut counts: HashMap<&str,usize> = HashMap::new();
        jobvec.iter_mut().rev().for_each(|(aggregate, job)| {
            if let Some(c) = counts.get(&(*job[0].user)) {
                if *c < n {
                    counts.insert(&job[0].user, *c+1);
                } else {
                    aggregate.selected = false;
                }
            } else {
                counts.insert(&job[0].user, 1);
            }
        })
    }

    // Now print.
    //
    // Unix user names are max 8 chars.
    // Linux pids are max 7 decimal digits.
    // We don't care about seconds in the timestamp, nor timezone.

    println!("{:8} {:8}   {:9}   {:16}   {:16}   {:9}  {:9}  {:9}  {:9}   {}",
             "job#", "user", "time", "start?", "end?", "cpu", "mem gb", "gpu", "gpu mem", "command", );
    let tfmt = "%Y-%m-%d %H:%M";
    jobvec.iter().for_each(|(aggregate, job)| {
        if aggregate.selected {
            let dur = format!("{:2}d{:2}h{:2}m", aggregate.days, aggregate.hours, aggregate.minutes);
            println!("{:7}{} {:8}   {}   {}   {}   {:4}/{:4}  {:4}/{:4}  {:4}/{:4}  {:4}/{:4}   {:22}",
                     job[0].job_id,
                     if aggregate.first == earliest && aggregate.last == latest {
                         "!"
                     } else if aggregate.first == earliest {
                         "<"
                     } else if aggregate.last == latest {
                         ">"
                     } else {
                         " "
                     },
                     job[0].user,
                     dur,
                     aggregate.first.format(tfmt),
                     aggregate.last.format(tfmt),
                     aggregate.avg_cpu,
                     aggregate.peak_cpu,
                     aggregate.avg_mem_gb,
                     aggregate.peak_mem_gb,
                     aggregate.avg_gpu,
                     aggregate.peak_gpu,
                     aggregate.avg_vmem_pct,
                     aggregate.peak_vmem_pct,
                     job[0].command);
        }
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
